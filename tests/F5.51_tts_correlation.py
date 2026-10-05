"""Correlated text survives queues, provider streaming and interruption unchanged."""

from dataclasses import replace
import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location("tts_correlation_fixtures", Path(__file__).with_name("F5.48_openai_tts_stream_block.py"))
F = importlib.util.module_from_spec(spec)
spec.loader.exec_module(F)
B = F.BLOCK


def request(text="  Exact approved message.\n", call_id="blox-out-"+"a"*32):
    return {"text":text,"call_id":call_id,"message_sha256":hashlib.sha256(text.encode()).hexdigest()}


def activate(context, value, port="synthesis_request"):
    context.input_attribute(port).update(value,content_type="application/json")
    try:
        return B.execute_runtime(context)
    finally:
        context.mark_inputs_consumed()


def commands(client, stream_id):
    client.states("drain")
    return [json.loads(output.value) for result in client.results for output in result.outputs
            if output.port_name == "command_out" and json.loads(output.value)["stream_id"] == stream_id]


def contracts():
    sender = Mock()
    ctx = F.context_for(services={"runtime_listener":sender,"runtime_audio_streams":SimpleNamespace(available=True)})
    before = request()
    for value in (before, json.dumps(before)):
        assert activate(ctx,value).status == "success"
        queued = sender.send.call_args.args[0]
        assert queued["text"] == before["text"] and queued["correlation"] == {key:before[key] for key in ("call_id","message_sha256")}
    before["call_id"] = "changed-after-send"
    assert queued["correlation"]["call_id"] != before["call_id"], "Queued identity must own its immutable snapshot"
    sender.reset_mock()
    for value in (None,[],{},"{","x"*32769,{**request(),"extra":1},{**request(),"text":"different"},
                  {**request(),"call_id":"unsafe/call"},{**request(),"call_id":True},request(" "),
                  {**request(),"text":"x"*4097},{**request(),"text":"\ud800"},
                  '{"text":"a","text":"b","call_id":"x","message_sha256":"x"}'):
        assert activate(ctx,value).status == "failed",repr(value)[:80]
    assert not sender.send.called
    # Two speech inputs together are ambiguous; a priority interrupt still wins.
    ctx.input_attribute("text_in").update("ordinary text")
    assert activate(ctx,request()).status == "failed" and not sender.send.called
    ctx.input_attribute("text_in").update("ordinary text")
    ctx.input_attribute("synthesis_request").update(request())
    assert activate(ctx,'{"action":"interrupt"}',"command_in").status == "success"
    assert sender.send.call_args.args[0] == {"action":"interrupt"}
    sender.reset_mock()
    assert B.execute_runtime(ctx).status == "skipped" and not sender.send.called
    # Existing two-input nodes stay valid; adding a port never mutates their saved model.
    legacy = replace(ctx,input_ports=ctx.input_ports[:2])
    assert B.prepare_runtime(legacy).listen_on_run and len(legacy.input_ports) == 2
    simulation = F.context_for("centralized", services={"runtime_listener":sender})
    assert activate(simulation,request()).status == "skipped" and not sender.send.called


def successive_streams():
    first, second = request(),request("Second approved message", "blox-out-"+"b"*32)
    with F.fake_openai("hold_tail") as api, F.listener() as client:
        one = activate(client.context,first).metadata[B.kind]["stream_id"]
        two = activate(client.context,second).metadata[B.kind]["stream_id"]
        F.until(lambda:len(client.frames)>2,"First stream must start before provider EOF")
        assert commands(client,one)[0] == {"action":"start","stream_id":one,"call_id":first["call_id"],"message_sha256":first["message_sha256"]}
        assert not commands(client,two)
        api.release.set()
        F.until(lambda:len(client.states("completed")) == 2,"Both correlated streams must finish")
        for ident,item in ((one,first),(two,second)):
            lifecycle = commands(client,ident)
            frames = [frame for frame in client.frames if frame["stream_id"] == ident]
            assert lifecycle == [
                {"action":"start","stream_id":ident,"call_id":item["call_id"],"message_sha256":item["message_sha256"]},
                {"action":"stop","stream_id":ident,"call_id":item["call_id"],"frame_count":len(frames),
                 "byte_count":sum(len(frame["payload"]) for frame in frames),"aborted":False}]
        assert [r["body"]["input"] for r in api.requests] == [first["text"],second["text"]]
        assert all("call_id" not in r["body"] and "message_sha256" not in r["body"] for r in api.requests)


def interrupted_stream():
    with F.fake_openai("hold_tail") as api, F.listener() as client:
        old = request()
        ident = activate(client.context,old).metadata[B.kind]["stream_id"]
        F.until(lambda:len(client.frames)>2,"First stream must start")
        activate(client.context,request("Queued obsolete", "old-queued"))
        assert activate(client.context,'{"action":"interrupt"}',"command_in").status == "success"
        F.until(lambda:client.states("interrupted"),"Interrupt must remain usable during correlated TTS")
        end = commands(client,ident)[-1]
        assert end["action"] == "stop" and end["call_id"] == old["call_id"] and end["aborted"]
        api.release.set()
        fresh = request("Fresh after cancellation","new-call")
        new_id = activate(client.context,fresh).metadata[B.kind]["stream_id"]
        F.until(lambda:client.states("completed"),"Fresh correlated TTS must resume")
        assert commands(client,new_id)[0]["call_id"] == "new-call"
        assert [r["body"]["input"] for r in api.requests] == [old["text"],fresh["text"]]


if __name__ == "__main__":
    for test in (contracts, successive_streams, interrupted_stream):
        test()
        print("[ok] "+test.__name__,flush=True)
