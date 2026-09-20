#!/usr/bin/env python3
"""FB1/FB4/FB5: command-only TTS interruption on real local HTTP and graph edges."""
from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
from dataclasses import replace
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch


FIXTURE_PATH = Path(__file__).with_name("F5.48_openai_tts_stream_block.py")
SPEC = importlib.util.spec_from_file_location("tts_interrupt_fixtures", FIXTURE_PATH)
assert SPEC is not None and SPEC.loader is not None
FIXTURES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURES)
BLOCK, KEY, REF = FIXTURES.BLOCK, FIXTURES.KEY, FIXTURES.REF
from block_test_fixtures import instance_scope
INTERRUPT = '{"action":"interrupt"}'


def activate(context, port, value, content_type="text/plain"):
    """Deliver one fresh port value, retaining consumed values as the real worker does."""
    context.input_attribute(port).update(value, content_type=content_type)
    before = time.monotonic()
    result = BLOCK.execute_runtime(context)
    assert time.monotonic() - before < .2, "An activation must not wait for HTTP cancellation."
    context.mark_inputs_consumed()
    return result


def interrupt(client):
    """Issue a separate JSON message without refreshing the previous text input."""
    return activate(client.context, "command_in", INTERRUPT, "application/json")


def lifecycle(client, stream_id):
    """Drain accumulated listener results and return one stream's visible lifecycle commands."""
    client.states("__drain_only__")
    return [json.loads(output.value) for result in client.results for output in result.outputs
            if output.port_name == "command_out" and json.loads(output.value).get("stream_id") == stream_id]


def test_interrupt_contract_and_io_free_simulation():
    """FB1/FB5: independent optional message inputs and compatibility without simulation IO."""
    ports = BLOCK.model["ports"]["inputs"]
    assert [(port["id"], port["name"]) for port in ports] == [(1, "text_in"), (2, "command_in")]
    assert all(port["transport"] == "message" and port["multiplicity"] == "one" for port in ports)
    assert all(not port["required"] and port["execution_requirement"] == "not_required_for_execution"
               for port in ports), "A command cannot wait for a fresh text input."
    assert "application/json" in ports[1]["accepts"]
    assert BLOCK.model["runtime"]["active_execution_policy"] == "on_each_event"
    for mode in ("centralized", "zeromq_active"):
        context = FIXTURES.context_for(mode)
        assert BLOCK.prepare_runtime(context).listen_on_run == (mode == "zeromq_active")
        # Existing Opus nodes remain runnable without silently changing their persisted ports.
        legacy_input = SimpleNamespace(**{**vars(context.input_ports[0]), "required": True,
                                        "execution_requirement": "required_for_execution"})
        legacy = replace(context, input_ports=(legacy_input,))
        assert BLOCK.prepare_runtime(legacy).listen_on_run == (mode == "zeromq_active")
        legacy_audio = deepcopy(context.output_ports[0])
        legacy_audio.audio_stream["codecs"] = ["pcm_s16le"]
        try:
            BLOCK.prepare_runtime(replace(legacy, output_ports=(legacy_audio, context.output_ports[1])))
        except ValueError as error:
            assert "PCM" in str(error) or "Opus" in str(error)
        else:
            raise AssertionError("Legacy PCM audio must not be misrepresented as Opus.")
    resolver, mailbox = Mock(side_effect=AssertionError("No secret needed in simulation")), Mock()
    context = FIXTURES.context_for("centralized", services={"resolve_secret": resolver, "runtime_listener": mailbox})
    with patch.object(BLOCK, "_http_client", side_effect=AssertionError("No network in simulation")):
        assert BLOCK.initialize_runtime(context).status == "success"
        for port, value in (("text_in", "Simulation seulement"), ("command_in", INTERRUPT)):
            result = activate(context, port, value)
            assert result.status == "skipped" and not result.outputs
        for invalid in ("{", '{"action":"stop"}', '{"action":"interrupt","extra":1}'):
            assert activate(context, "command_in", invalid).status == "failed"
    resolver.assert_not_called()
    mailbox.send.assert_not_called()


def test_fresh_inputs_and_invalid_commands():
    """FB1/FB2/FB4: stale text never requeues and stale interruption never cancels a new text."""
    mailbox = Mock()
    context = FIXTURES.context_for(services={"runtime_listener": mailbox,
        "runtime_audio_streams": SimpleNamespace(available=True)})
    assert activate(context, "text_in", "Same text").status == "success"
    assert mailbox.send.call_count == 1
    assert activate(context, "command_in", INTERRUPT, "application/json").status == "success"
    assert mailbox.send.call_count == 2
    assert mailbox.send.call_args.args[0] == {"action": "interrupt"}
    result = BLOCK.execute_runtime(context)
    assert result.status == "skipped" and mailbox.send.call_count == 2
    assert activate(context, "text_in", "Same text").status == "success"
    assert mailbox.send.call_count == 3, "Equal text is a new event; a remembered interrupt is not."
    assert mailbox.send.call_args.args[0]["text"] == "Same text"
    invalid = ("{", "null", "[]", '"interrupt"', "{}", '{"action":"stop"}',
               '{"action":true}', '{"action":"interrupt","extra":1}', "x" * 4097)
    for payload in invalid:
        result = activate(context, "command_in", payload, "application/json")
        assert result.status == "failed" and not result.outputs
        assert mailbox.send.call_count == 3, "A malformed command must neither cancel nor replay old text."
        assert KEY not in str(result)
    # One activation can contain several fresh attributes (manual replay or a grouped input wave).
    context.input_attribute("text_in").update("Must not restart immediately")
    context.input_attribute("command_in").update(INTERRUPT, content_type="application/json")
    assert BLOCK.execute_runtime(context).status == "success"
    context.mark_inputs_consumed()
    assert mailbox.send.call_count == 4 and mailbox.send.call_args.args[0] == {"action": "interrupt"}


def test_input_events_and_interrupt_without_audio():
    """FB1/FB4: event identity beats remembered attributes; an interrupt needs no audio route."""
    mailbox = Mock()
    context = FIXTURES.context_for(services={"runtime_listener": mailbox})
    context.input_attribute("text_in").update("Old value still visible")
    context.input_attribute("command_in").update(INTERRUPT, content_type="application/json")
    context.mark_inputs_consumed()
    context.input_events = (SimpleNamespace(input_port_id=2, value=INTERRUPT),)
    assert BLOCK.execute_runtime(context).status == "success"
    assert mailbox.send.call_args.args[0] == {"action": "interrupt"}
    # The real worker can preserve a consumed value while receiving a new event with equal text.
    context.services["runtime_audio_streams"] = SimpleNamespace(available=True)
    context.input_events = (SimpleNamespace(input_port_id=1, value="Nouvelle livraison"),)
    assert BLOCK.execute_runtime(context).status == "success"
    assert mailbox.send.call_args.args[0]["text"] == "Nouvelle livraison"
    assert mailbox.send.call_count == 2
    # Do not promote an unrelated remembered/updated attribute when this activation has explicit events.
    context.input_attribute("command_in").update(INTERRUPT, content_type="application/json")
    assert BLOCK.execute_runtime(context).status == "success"
    assert mailbox.send.call_args.args[0]["text"] == "Nouvelle livraison"


def test_pending_interrupt_prevents_request_at_drain_boundary():
    """FB4: a full bounded drain pass cannot launch old HTTP while another command is already queued."""
    size = FIXTURES.tts_module.MAX_PENDING_TEXTS
    mailbox = deque(SimpleNamespace(payload={"text": f"Ancienne attente {index}", "stream_id": f"{index:032x}"})
                    for index in range(size))
    seen = SimpleNamespace(received=0, stopped=False, requests=[], interruptions=[])

    def receive_command(timeout_sec=0):
        """Simulate a concurrent producer filling the now-free mailbox slot at the drain boundary."""
        if not mailbox:
            return None
        result = mailbox.popleft()
        seen.received += 1
        if seen.received == size:
            mailbox.append(SimpleNamespace(payload={"action": "interrupt"}))
        assert len(mailbox) <= size
        return result

    def emit_result(result):
        """Stop this isolated loop immediately after its interruption acknowledgment."""
        if result.metadata.get(BLOCK.kind, {}).get("state") == "interrupted":
            seen.interruptions.append(result)
            seen.stopped = True

    async def would_start_http(context, config, text, stream_id):
        """Record the synthesis scheduling boundary without opening a socket or resolving a secret."""
        seen.requests.append(text)
        await asyncio.Event().wait()

    context = SimpleNamespace(stop_requested=lambda: seen.stopped,
                              receive_command=receive_command, emit_result=emit_result)
    with patch.object(BLOCK, "_speak", side_effect=would_start_http):
        asyncio.run(asyncio.wait_for(BLOCK._listen(context, FIXTURES.DEFAULTS), timeout=1))
    assert len(seen.interruptions) == 1
    assert seen.requests == [], "An interrupt already in the mailbox must prevent the first obsolete HTTP request."
    assert seen.interruptions[0].metadata[BLOCK.kind]["discarded_texts"] == size


def test_interrupt_idle_then_resume():
    """FB4: idle/repeated interruption is harmless and keeps the listener alive for new text."""
    with FIXTURES.fake_openai() as api, FIXTURES.listener() as client:
        for count in (1, 2):
            assert interrupt(client).status == "success"
            FIXTURES.until(lambda: len(client.states("interrupted")) == count, "Idle interrupt acknowledgement missing.")
            assert not api.requests and not client.frames and not client.host.failure
            assert client.host._thread.is_alive()
        assert activate(client.context, "text_in", "After the interruptions").status == "success"
        FIXTURES.until(lambda: len(client.states("completed")) == 1, "The listener must accept text after an idle interrupt.")
        assert [item["body"]["input"] for item in api.requests] == ["After the interruptions"]
        assert not client.states("error")


def test_interrupt_in_flight_purges_queue_and_resumes():
    """FB3/FB4: cancel before headers or during Opus streaming, purge old queue, then reuse Run."""
    for mode in ("stall_headers", "hold_tail"):
        with FIXTURES.fake_openai(mode) as api, FIXTURES.listener() as client:
            started = activate(client.context, "text_in", "Old answer")
            assert started.status == "success" and api.first.wait(2)
            old_id = started.metadata[BLOCK.kind]["stream_id"]
            pending_ids = []
            for index in range(3):
                result = activate(client.context, "text_in", f"Ancienne attente {index}")
                assert result.status == "success"
                pending_ids.append(result.metadata[BLOCK.kind]["stream_id"])
            if mode == "hold_tail":
                FIXTURES.until(lambda: len(client.frames) >= 3, "The first response must be streaming before interruption.")
                assert lifecycle(client, old_id)[0] == {"action": "start", "stream_id": old_id}
            before = time.monotonic()
            assert interrupt(client).status == "success"
            FIXTURES.until(lambda: bool(client.states("interrupted")), "Interrupt must be processed while HTTP is blocked.", timeout=1)
            assert time.monotonic() - before < 1
            assert client.host._thread.is_alive() and not client.host.failure
            assert len(api.requests) == 1 and not client.states("completed")
            old_frames = [frame for frame in client.frames if frame["stream_id"] == old_id]
            commands = lifecycle(client, old_id)
            if mode == "hold_tail":
                assert commands[-1] == {"action": "stop", "stream_id": old_id, "aborted": True,
                    "frame_count": len(old_frames), "byte_count": sum(len(frame["payload"]) for frame in old_frames)}
                assert len(commands) == 2
            else:
                assert not commands and not old_frames, "Never invent start/stop without any accepted audio."
            resumed = activate(client.context, "text_in", "New answer")
            assert resumed.status == "success"
            new_id = resumed.metadata[BLOCK.kind]["stream_id"]
            api.release.set()
            FIXTURES.until(lambda: len(client.states("completed")) == 1, "New text must finish on the same listener.")
            assert [item["body"]["input"] for item in api.requests] == ["Old answer", "New answer"]
            assert client.states("completed")[0].metadata[BLOCK.kind]["stream_id"] == new_id
            assert not client.states("error") and not client.host.failure
            assert not any(frame["stream_id"] in pending_ids for frame in client.frames)
            assert len([frame for frame in client.frames if frame["stream_id"] == old_id]) == len(old_frames)
            assert b"".join(frame["payload"] for frame in client.frames if frame["stream_id"] == new_id) == FIXTURES.encoded_opus()
            assert lifecycle(client, new_id)[-1]["aborted"] is False
            assert KEY not in str(client.results)


def command_graph_document():
    """Add an independent, visibly wired JSON-command source to the existing TTS/audio mini-graph."""
    payload = FIXTURES.document()
    command = FIXTURES.TextBlock().build_node_payload(node_id="command")
    command["outputs"][0].update(text=INTERRUPT, emits=["application/json"])
    command["position"] = {"x": 80, "y": 380}
    payload["nodes"].append(command)
    payload["edges"].append({"id": "interrupt", "from": {"node": "command", "port": 1},
                             "to": {"node": "tts", "port": 2}, "kind": "data"})
    return payload


def test_real_graph_command_only_interrupt_and_simulation():
    """FB1/FB4/FB5: real routing delivers an independent interrupt after text was consumed."""
    import zmq
    runtime_document, graph = FIXTURES.compile_runtime_graph_document(
        FIXTURES.GraphDocument.from_payload(command_graph_document()))
    with TemporaryDirectory(prefix="tts-interrupt-graph-") as directory, FIXTURES.fake_openai("hold_tail") as api:
        root = Path(directory)
        wallet = FIXTURES.SecretManager(root / "secrets")
        wallet.initialize("test-only-wallet-password")
        wallet.set_secret(ref=REF, value=KEY)
        engine = FIXTURES.WorkflowOrchestrator(root_dir=root, runs_dir=root / "runs", secret_manager=wallet,
                                             active_worker_host="thread")
        run = engine.prepare_active_run(graph, document=runtime_document,
                                        run_data_scope=instance_scope(root, command_graph_document()))
        assert run.status == "prepared", run.logs
        publisher = zmq.Context.instance().socket(zmq.PUB)
        publisher.setsockopt(zmq.LINGER, 0)
        reader = None
        try:
            manager = engine.runtime_audio_egress(run.run_id)
            ticket = manager.create_session(node_id="player", input_port="audio_in")
            reader = manager.attach(ticket.session_id, ticket.ticket)
            session = engine._active_sessions[run.run_id]
            publisher.connect(session.pub_endpoint)
            time.sleep(.2)  # The extra test publisher must connect; production listeners use their ready gate.
            received = []

            def publish(source_id, value, sequence, content_type="text/plain"):
                """Send through the compiled graph edge, never directly to TTS internals."""
                topic = run.plan.worker_configs[source_id].outputs[0].topic
                envelope = FIXTURES.MessageEnvelope(run_id=run.run_id, source_node_id=source_id,
                    source_port_id=1, payload=value, sequence=sequence, content_type=content_type)
                publisher.send_multipart([topic.encode(), envelope.to_json().encode()])

            def state():
                """Read the TTS business state published by the ordinary worker."""
                return run.results.get("tts", {}).get(BLOCK.kind, {}).get("state")

            def receive_one():
                """Drain one actual egress frame without involving browser playback or real hardware."""
                frame = reader.receive_frame(.05)
                if frame is not None:
                    received.append(frame)
                return bool(received)

            assert not api.requests, "Run must not synthesize without text."
            publish("text", "First answer from the graph", 1)
            FIXTURES.until(receive_one, "Text input must trigger streamed Opus before command input has a value.")
            first_id = received[0].stream_id
            publish("text", "Wait that became obsolete", 2)
            publish("command", INTERRUPT, 1, "application/json")
            FIXTURES.until(lambda: state() == "interrupted", "Command-only graph event must cancel after text consumption.", timeout=2)
            assert len(api.requests) == 1, "Old queued text must not start during interruption."
            aborted = json.loads(run.output_values["tts:2"]["value"])
            assert aborted["action"] == "stop" and aborted["stream_id"] == first_id and aborted["aborted"] is True
            assert run.run_id in engine._active_sessions and not run.cancel_requested
            publish("text", "Answer after the interruption", 3)
            api.release.set()
            FIXTURES.until(lambda: state() == "completed", "The same Run must complete the next synthesis.")
            deadline = time.monotonic() + 2
            expected_total = aborted["byte_count"] + len(FIXTURES.encoded_opus())
            while sum(len(frame.payload) for frame in received) < expected_total and time.monotonic() < deadline:
                receive_one()
            assert [item["body"]["input"] for item in api.requests] == ["First answer from the graph", "Answer after the interruption"]
            old_frames = [frame for frame in received if frame.stream_id == first_id]
            new_frames = [frame for frame in received if frame.stream_id != first_id]
            assert len(old_frames) == aborted["frame_count"]
            assert sum(len(frame.payload) for frame in old_frames) == aborted["byte_count"]
            assert b"".join(frame.payload for frame in new_frames) == FIXTURES.encoded_opus()
            assert len({frame.stream_id for frame in new_frames}) == 1
            assert run.results["tts"][BLOCK.kind]["stream_id"] != first_id
            assert not json.loads(run.output_values["tts:2"]["value"])["aborted"]
            assert KEY not in str(run.logs) and KEY not in str(run.results)
        finally:
            publisher.close(0)
            engine.stop_active_run(run.run_id)
        assert reader is not None and not reader.available
        count = len(api.requests)
        simulation = engine.create_run(graph, document=runtime_document, runtime_mode="centralized", auto_start=False)
        with patch.object(BLOCK, "_http_client", side_effect=AssertionError("No network in simulation")):
            engine._execute_run(simulation)
        assert simulation.status == "success", simulation.logs
        assert len(api.requests) == count and "tts:1" not in simulation.output_values
        assert "tts:2" not in simulation.output_values, "Simulation must not publish source lifecycle commands."


if __name__ == "__main__":
    for test in (test_interrupt_contract_and_io_free_simulation, test_fresh_inputs_and_invalid_commands,
                 test_input_events_and_interrupt_without_audio, test_pending_interrupt_prevents_request_at_drain_boundary,
                 test_interrupt_idle_then_resume, test_interrupt_in_flight_purges_queue_and_resumes,
                 test_real_graph_command_only_interrupt_and_simulation):
        test()
        print(f"[ok] {test.__name__}", flush=True)
