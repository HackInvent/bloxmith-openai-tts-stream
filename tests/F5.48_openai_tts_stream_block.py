#!/usr/bin/env python3
"""FB1–FB6: real HTTP/listener/graph boundaries and modal UX without paid OpenAI calls."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import permutations
import json
from pathlib import Path
import sys
import subprocess
from tempfile import TemporaryDirectory
from threading import Event, Thread
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from blocs.openai_tts_stream import block as tts_module
from blocs.openai_tts_stream.block import OpenAITtsStreamBlock, DEFAULTS, MODEL, _config, _failure
from blocs.openai_tts_stream.ogg_stream import MAX_PAGE_BYTES, OggOpusStream, page_crc
from blocs.audio_play_stream.block import AudioPlayStreamBlock
from blocs.text.block import TextBlock
from blocs.registry import get_block_definition
from bloxsmith_app.block_api import BlockRuntimeContext, BlockRuntimePreparationContext
from bloxsmith_app.active_runtime.listener import RuntimeListenerHost
from bloxsmith_app.block_ui import declared_block_ui_assets
from bloxsmith_app.graph_compile import compile_runtime_graph_document
from bloxsmith_app.graph_document import GraphDocument
from bloxsmith_app.messages import MessageEnvelope
from bloxsmith_app.orchestrator import WorkflowOrchestrator
from bloxsmith_app.secrets import SecretManager
from ui_smoke_common import create_project_api, graph_payload, http_json, project_editor_url, run_playwright_smoke
from block_test_artifacts import artifact_path
from block_test_fixtures import instance_scope

BLOCK = OpenAITtsStreamBlock()
REF = "secret://workspace/test-tts"
KEY = "test-only-TTS-key-never-real"


@lru_cache(maxsize=8)
def encoded_opus(duration=.5, channels=1, page_duration=100000, bitrate="128k"):
    """Generate reusable valid Ogg Opus without a microphone, key or provider call."""
    return subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
        f"sine=frequency=440:duration={duration}", "-ar", "24000", "-ac", str(channels),
        "-c:a", "libopus", "-b:a", bitrate, "-page_duration", str(page_duration), "-f", "ogg", "pipe:1"],
        check=True, capture_output=True, timeout=10).stdout


def until(predicate, message, timeout=6):
    """Poll a test observation with a finite deadline, not a production timing assumption."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.01)
    raise AssertionError(message)


def context_for(mode="zeromq_active", config=None, services=None):
    """Use the manifest ports in a real public block context."""
    return BlockRuntimeContext(run_id="tts-test", node_id="tts", kind=BLOCK.kind,
        title=BLOCK.default_title(), config={**DEFAULTS, "api_key_ref": REF, **(config or {})},
        runtime_mode=mode, input_ports=tuple(SimpleNamespace(**port) for port in BLOCK.model["ports"]["inputs"]),
        output_ports=tuple(SimpleNamespace(**port) for port in BLOCK.model["ports"]["outputs"]),
        services=services or {}, root_dir=ROOT)


@contextmanager
def fake_openai(mode="normal", payload=None):
    """Serve real incremental HTTP locally; only tests can replace the fixed production URL."""
    seen = SimpleNamespace(requests=[], first=Event(), release=Event(), closed=Event())
    class Handler(BaseHTTPRequestHandler):
        """Emit actual Ogg Opus or bounded provider faults without logging credentials."""
        def log_message(self, *args):
            """Silence local server logs, including request metadata."""

        def do_POST(self):
            """Record the request and split Ogg pages at odd HTTP boundaries to exercise reassembly."""
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.requests.append({"body": body, "auth": self.headers.get("Authorization"), "at": time.monotonic()})
            seen.first.set()
            if mode == "stall_headers":
                seen.release.wait(6)
            status = {"unauthorized": 401, "quota": 429, "redirect": 307}.get(mode, 200)
            self.send_response(status)
            self.send_header("Content-Type", "application/json" if mode == "bad_type" or status != 200 else "audio/ogg; codecs=opus")
            if status != 200:
                self.send_header("Location", "https://example.invalid/do-not-follow")
            self.end_headers()
            normal = encoded_opus() if payload is None else payload
            data = (KEY.encode() if status != 200 else b"" if mode == "empty" else
                    normal[:-10] if mode == "truncated" else encoded_opus(1.5) if mode == "too_long" else normal)
            try:
                for offset in range(0, len(data), 1001):
                    self.wfile.write(data[offset:offset + 1001])
                    self.wfile.flush()
                    if mode == "hold_tail" and offset == 4004:
                        seen.release.wait(6)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                seen.closed.set()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = Thread(target=lambda: server.serve_forever(poll_interval=.02), daemon=True)
    thread.start()
    try:
        with patch.object(tts_module, "SPEECH_URL", f"http://127.0.0.1:{server.server_port}/v1/audio/speech"):
            yield seen
    finally:
        seen.release.set()
        server.shutdown()
        server.server_close()
        thread.join(1)
        assert not thread.is_alive()


@contextmanager
def listener(config=None):
    """Use the real supervised mailbox; capture publications for deterministic pacing assertions."""
    frames, results = [], []
    def publish(port, payload, **metadata):
        """Capture immutable frames and their monotonic publication times."""
        frames.append({"port": port, "payload": payload, "at": time.monotonic(), **metadata})
    context = context_for(config=config, services={"resolve_secret": lambda ref: KEY,
        "runtime_audio_streams": SimpleNamespace(available=True, publish_port=publish)})
    gate = Event()
    gate.set()
    host = RuntimeListenerHost(context=BlockRuntimePreparationContext.from_context(context),
        services=context.services, hook=BLOCK.listen_runtime, stop_event=Event(), ready_gate=gate)
    context.services["runtime_listener"] = host.client
    def submit(text):
        """Submit through ordinary execution and prove it never waits for synthesis."""
        context.input_attribute("text_in").update(text)
        before = time.monotonic()
        result = BLOCK.execute_runtime(context)
        if result.status in {"success", "skipped"}:
            context.attributes.mark_inputs_consumed()
        assert time.monotonic() - before < .1
        return result
    def states(state):
        """Drain listener results without hiding prior transitions."""
        results.extend(host.pop_results())
        return [result for result in results if result.metadata.get(BLOCK.kind, {}).get("state") == state]
    host.start()
    assert host.started.wait(1)
    try:
        yield SimpleNamespace(context=context, host=host, frames=frames, results=results, submit=submit, states=states)
    finally:
        host.close()
        assert not host._thread.is_alive(), "Stop must cancel all network/production waits."


def test_contract_and_settings():
    """FB1/FB2/FB5/FB6: fixed ports, wallet-only settings, honest modes and declared surfaces."""
    assert isinstance(get_block_definition(BLOCK.kind), OpenAITtsStreamBlock)
    for mode in ("centralized", "zeromq_active"):
        context = context_for(mode)
        assert BLOCK.prepare_runtime(context).listen_on_run == (mode == "zeromq_active")
        with patch.object(BLOCK, "_http_client", side_effect=AssertionError("No request on Run")):
            context.services["resolve_secret"] = Mock(return_value=KEY)
            assert BLOCK.initialize_runtime(context).status == "success"
            assert context.services["resolve_secret"].call_count == int(mode == "zeromq_active")
            if mode == "centralized":
                result = BLOCK.execute_runtime(context)
                assert result.status == "skipped" and not result.outputs
                assert not context.services["resolve_secret"].called
        assert BLOCK.execute_runtime(replace(context, input_ports=())).status == "failed"
        legacy = replace(context, output_ports=context.output_ports[:1])
        assert "Ports TTS invalides" in BLOCK.execute_runtime(legacy).error
    for bad in ({"api_key_ref": KEY}, {"api_key": KEY}, {"voice": "fake"}, {"speed": "nan"},
                {"speed": True}, {"instructions": []}, {"connect_timeout_sec": 0}, {"max_audio_sec": 601},
                {"read_timeout_sec": "inf"}, {"instructions": "x" * 1025}):
        try:
            _config(bad)
        except ValueError:
            pass
        else:
            raise AssertionError("Invalid config accepted")
    node = BLOCK.build_node_payload(node_id="tts")
    saved = BLOCK.handle_ui_action(node=node, action="save_properties", values={"title": "Ma voix",
        "config": {"api_key_ref": REF, "voice": "cedar", "speed": "1.2"}})
    assert saved["node_patch"]["title"] == "Ma voix" and saved["node_patch"]["config"]["speed"] == 1.2
    for values in ({"title": " "}, {"config": {"api_key_ref": KEY}}, {"config": {"speed": -1}}):
        result = BLOCK.handle_ui_action(node=node, action="save_properties", values=values)
        assert "error" in result and "node_patch" not in result and KEY not in str(result)
    for services in ({}, {"resolve_secret": lambda ref: ""}, {"resolve_secret": Mock(side_effect=Exception(KEY))}):
        result = BLOCK.initialize_runtime(context_for(services=services))
        assert result.status == "failed" and KEY not in str(result)
    assert KEY not in str(_failure(Exception(KEY)))
    node["title"] = '<script>alert("x")</script>'
    node["config"]["instructions"] = '</textarea><script>alert("x")</script>'
    for render in (BLOCK.render_modal, BLOCK.render_inspector_panel, BLOCK.render_node_card):
        html = render(node=node)["html"]
        assert "{{" not in html and "<script>" not in html, html
    for render in (BLOCK.render_modal, BLOCK.render_inspector_panel):
        assert '"action":"interrupt"' in render(node=node)["html"]
        legacy_html = render(node={**node, "inputs": node["inputs"][:1]})["html"]
        assert "n’a pas d’entrée command_in" in legacy_html and "recréez le TTS" in legacy_html
    error_modal = BLOCK.render_modal(node=node, payload={"runtime": {"error": "Test erreur"}})["html"]
    assert 'tts-diagnostics" open' in error_modal and error_modal.count("data-block-modal-error-panel") == 1
    for asset in declared_block_ui_assets(BLOCK.kind, None):
        assert (BLOCK.directory / asset["path"]).is_file()


def test_port_order():
    """FB1/FB5: input/output permutations preserve routing, strict contracts and legacy Opus nodes."""
    for mode in ("centralized", "zeromq_active"):
        base = context_for(mode)
        for inputs in permutations(base.input_ports):
            for outputs in permutations(base.output_ports):
                received = []
                ctx = replace(base, input_ports=inputs, output_ports=outputs, services={
                    "runtime_listener": SimpleNamespace(send=received.append),
                    "runtime_audio_streams": SimpleNamespace(available=True)})
                before = json.dumps([[vars(p) for p in inputs], [vars(p) for p in outputs]], sort_keys=True)
                prepared = BLOCK.prepare_runtime(BlockRuntimePreparationContext.from_context(ctx))
                assert prepared.listen_on_run == (mode == "zeromq_active")
                for name, value in (("text_in", "Bonjour"), ("command_in", '{"action":"interrupt"}')):
                    ctx.input_attribute(name).update(value)
                    result = BLOCK.execute_runtime(ctx)
                    assert result.status == ("success" if mode == "zeromq_active" else "skipped"), result
                    assert not result.outputs
                    ctx.mark_inputs_consumed()
                if mode == "zeromq_active":
                    assert received[0]["text"] == "Bonjour" and received[0]["stream_id"]
                    assert received[1] == {"action": "interrupt"} and len(received) == 2
                else:
                    assert not received
                assert json.dumps([[vars(p) for p in ctx.input_ports], [vars(p) for p in ctx.output_ports]], sort_keys=True) == before
        legacy = replace(context_for(mode), input_ports=base.input_ports[:1], output_ports=base.output_ports[::-1])
        assert BLOCK.prepare_runtime(legacy).listen_on_run == (mode == "zeromq_active")
        assert BLOCK.execute_runtime(legacy).status == "skipped"
        assert len(legacy.input_ports) == 1, "Validation must not migrate legacy nodes."
        invalid_ports = [
            {"input_ports": ()}, {"input_ports": (base.input_ports[1],)},
            {"input_ports": (base.input_ports[0], base.input_ports[0])},
            {"output_ports": base.output_ports[:1]},
            {"output_ports": (base.output_ports[0], base.output_ports[0])},
            {"output_ports": (*base.output_ports, base.output_ports[0])},
        ]
        for field, ports in (("input_ports", base.input_ports), ("output_ports", base.output_ports)):
            for index, port in enumerate(ports):
                for change in ({"id": 99}, {"name": "wrong"},
                               {"transport": "audio_stream" if getattr(port, "transport", "message") == "message" else "message"},
                               {"multiplicity": "many" if port.multiplicity == "one" else "one"}):
                    changed = list(ports)
                    changed[index] = SimpleNamespace(**{**vars(port), **change})
                    invalid_ports.append({field: tuple(reversed(changed))})
        invalid_ports.append({"output_ports": (base.output_ports[1], SimpleNamespace(
            **{**vars(base.output_ports[0]), "audio_stream": {"codecs": ["pcm_s16le"]}}))})
        for changes in invalid_ports:
            ctx = replace(base, **changes)
            try:
                BLOCK.prepare_runtime(ctx)
            except ValueError:
                pass
            else:
                raise AssertionError(f"Invalid ports accepted: {changes}")
            assert BLOCK.execute_runtime(ctx).status == "failed"


def test_streaming_and_order():
    """FB1–FB4: real HTTP Opus before EOF, exact separate start/stop, serial order and pacing."""
    with fake_openai("hold_tail") as api, listener(config={"voice": "cedar", "instructions": "Doucement"}) as client:
        assert client.submit("Bonjour").status == "success"
        assert client.submit("Deuxième phrase").status == "success"
        until(lambda: len(client.frames) >= 3, "Headers and audible Opus must arrive before HTTP EOF.")
        assert not api.closed.is_set() and len(api.requests) == 1
        assert not client.states("completed")
        api.release.set()
        until(lambda: len(client.states("completed")) == 2, "Both utterances must finish serially.")
        assert [item["body"]["input"] for item in api.requests] == ["Bonjour", "Deuxième phrase"]
        assert all(item["auth"] == f"Bearer {KEY}" for item in api.requests)
        body = api.requests[0]["body"]
        assert body == {"model": MODEL, "voice": "cedar", "input": "Bonjour", "speed": 1.0,
                        "response_format": "opus", "stream_format": "audio", "instructions": "Doucement"}
        streams = list(dict.fromkeys(frame["stream_id"] for frame in client.frames))
        assert len(streams) == 2
        for stream_id in streams:
            frames = [frame for frame in client.frames if frame["stream_id"] == stream_id]
            assert b"".join(frame["payload"] for frame in frames) == encoded_opus()
            assert all(frame["port"] == "audio_out" and len(frame["payload"]) <= MAX_PAGE_BYTES and
                       frame["codec"] == "opus" and frame["sample_rate_hz"] == 48000 and
                       frame["channels"] == 1 and frame["correlation_id"] == stream_id for frame in frames)
            commands = [json.loads(o.value) for r in client.results for o in r.outputs if json.loads(o.value)["stream_id"] == stream_id]
            assert commands == [{"action": "start", "stream_id": stream_id},
                {"action": "stop", "stream_id": stream_id, "frame_count": len(frames),
                 "byte_count": len(encoded_opus()), "aborted": False}]
        assert api.requests[1]["at"] - client.frames[0]["at"] >= .48
        assert all(o.port_id == 2 and o.port_name == "command_out" and o.content_type == "application/json"
                   for r in client.results for o in r.outputs)
        assert KEY not in str(client.results)
        assert client.submit("  ").status == "skipped"
        assert client.submit({"text": "not a string"}).status == "failed"
        assert client.submit("x" * 4097).status == "failed"
        # Bad internal commands are rejected without killing the listener or calling the provider.
        client.host.client.send({"text": {}, "stream_id": "a" * 32})
        until(lambda: client.states("error"), "Invalid internal text must produce a safe result.")
        assert not client.host.failure and len(api.requests) == 2


def test_compiled_runtime_metadata():
    """FB2/FB5/FB6: accept compiled/restored metadata without weakening settings or wallet checks."""
    source = GraphDocument.from_payload(document())
    flattened, _ = compile_runtime_graph_document(source)
    metadata = {"position", "runtime_path", "runtime_path_label"}
    # Exercise both a persisted blueprint and the already flattened document in a Run snapshot.
    for document_to_load in (source, GraphDocument.from_payload(flattened.to_dict())):
        runtime_document, graph = compile_runtime_graph_document(document_to_load)
        node = graph.nodes["tts"]
        assert {"runtime_path", "runtime_path_label"} <= set(node.config)
        before = json.dumps(node.config, sort_keys=True)
        for mode in ("centralized", "zeromq_active"):
            context = context_for(mode, config={**node.config, "position": {"x": 10, "y": 20}})
            preparation = BLOCK.prepare_runtime(BlockRuntimePreparationContext.from_context(context))
            assert preparation.listen_on_run == (mode == "zeromq_active")
            assert set(_config(context.config)) == set(DEFAULTS), "Runtime metadata is not speech configuration."
            assert json.dumps(node.config, sort_keys=True) == before, "Do not mutate framework metadata."
        runtime_node = next(item for item in runtime_document.to_dict()["nodes"] if item["id"] == "tts")
        for render in (BLOCK.render_modal, BLOCK.render_inspector_panel, BLOCK.render_node_card):
            assert "{{" not in render(node=runtime_node)["html"]
        result = BLOCK.handle_ui_action(node=runtime_node, action="save_properties", values={"config": {"voice": "cedar"}})
        assert result["node_patch"]["config"]["voice"] == "cedar"
        assert not metadata.intersection(result["node_patch"]["config"])
        for bad in ({"extra_parameter": KEY}, {"api_key": KEY}, {"api_key_ref": KEY}, {"speed": 0}):
            try:
                _config({**node.config, **bad})
            except ValueError as error:
                assert KEY not in str(error)
                if "extra_parameter" in bad:
                    assert "wallet" not in str(error).lower() and "clé" not in str(error).lower()
            else:
                raise AssertionError("Technical metadata must not bypass business/credential validation.")
        for key in metadata:
            result = BLOCK.handle_ui_action(node=runtime_node, action="save_properties", values={"config": {key: "not editable"}})
            assert "error" in result and "node_patch" not in result


def test_ogg_validation_and_timing():
    """FB3: parse arbitrary boundaries, 24 kHz input/48 kHz clock, EOS and malformed containers."""
    def parse(data, chunk_size=1024):
        """Return original validated pages and their final timing, requiring a true EOS."""
        container = OggOpusStream()
        pages = []
        for offset in range(0, len(data), chunk_size):
            pages.extend(container.push(data[offset:offset + chunk_size]))
        container.finish()
        return pages, container

    def changed(page, offset, value):
        """Modify one header field while preserving its CRC, to test semantic validation too."""
        copy = bytearray(page)
        copy[offset:offset + len(value)] = value
        copy[22:26] = page_crc(copy).to_bytes(4, "little")
        return bytes(copy)

    for channels in (1, 2):
        for page_duration in (100000, 1000000):
            data = encoded_opus(1.5, channels, page_duration)
            for size in (1, 17, 1024, 8192):
                pages, container = parse(data, size)
                assert b"".join(pages) == data and container.channels == channels
                assert abs(container.seconds - 1.5) < .00001
                assert all(len(page) <= MAX_PAGE_BYTES for page in pages)
    data = encoded_opus()
    pages, _ = parse(data)
    damaged = bytearray(data)
    damaged[-1] ^= 1
    failures = [b"", data[:-1], b"RIFF" + data[4:], bytes(damaged), data + data,
        b"".join(pages[:2]),
        changed(pages[0], 28, b"Vorbis!!") + b"".join(pages[1:]),
        changed(pages[0], 46, b"\x01") + b"".join(pages[1:]),
        pages[0] + changed(pages[1], 18, (9).to_bytes(4, "little")) + b"".join(pages[2:]),
        b"".join(pages[:-1]) + changed(pages[-1], 5, b"\x00"),
        b"".join(pages[:-1]) + changed(pages[-1], 6, (9999999).to_bytes(8, "little")),
        encoded_opus(6, 1, 10000000, "16k")]
    for index, invalid in enumerate(failures):
        try:
            parse(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Malformed/truncated/chained or excessive Ogg accepted: case {index}.")
    try:
        list(OggOpusStream().push(b"x" * 8193))
    except ValueError:
        pass
    else:
        raise AssertionError("Unbounded HTTP chunk accepted.")


def test_failures_and_stop():
    """FB2/FB3/FB4: provider faults, finite memory/duration, cancellation and mailbox limits."""
    for mode, expected in (("unauthorized", "401"), ("quota", "429"), ("redirect", "307"),
                           ("empty", "vide"), ("truncated", "tronquée"), ("bad_type", "Opus"),
                           ("too_long", "maximale")):
        with fake_openai(mode) as api, listener(config={"max_audio_sec": 1}) as client:
            assert client.submit("Test").status == "success"
            until(lambda: client.states("error"), f"Expected provider failure: {mode}")
            assert expected in client.states("error")[-1].error
            assert len(api.requests) == 1 and not client.states("completed") and KEY not in str(client.results)
            commands = [json.loads(o.value) for r in client.results for o in r.outputs]
            if client.frames:
                assert commands[0]["action"] == "start" and commands[-1]["aborted"] is True
                assert commands[-1]["frame_count"] == len(client.frames)
                assert commands[-1]["byte_count"] == sum(len(f["payload"]) for f in client.frames)
            else:
                assert not commands
    for mode in ("stall_headers", "hold_tail"):
        with fake_openai(mode) as api, listener() as client:
            client.submit("En cours")
            client.submit("Ne doit pas partir après Stop")
            assert api.first.wait(2)
            before = time.monotonic()
            client.host.close(timeout_sec=.8)
            assert time.monotonic() - before < .8 and not client.host._thread.is_alive()
            assert len(api.requests) == 1 and not client.host.failure
    with fake_openai("stall_headers") as api, listener(config={"read_timeout_sec": 1}) as client:
        client.submit("Timeout")
        until(lambda: client.states("error"), "A stalled provider must not wait forever.", timeout=3)
        assert len(api.requests) == 1 and KEY not in str(client.results)
    with fake_openai("hold_tail") as api, listener() as client:
        client.submit("En cours")
        assert api.first.wait(2)
        # The listener now drains while HTTP is active. Both its local FIFO and the
        # framework mailbox remain bounded; which limit is reached first is concurrent.
        rejected = False
        for _ in range(2 * tts_module.MAX_PENDING_TEXTS + 2):
            result = client.submit("En attente")
            rejected = result.status == "failed" or bool(client.states("error"))
            if rejected:
                break
        until(lambda: rejected or client.states("error"), "Excess pending text must be explicitly rejected.")
        assert len(api.requests) == 1


def document():
    """Build the actual Text → TTS → Audio Play chain, without any implicit data/audio channel."""
    nodes = [TextBlock().build_node_payload(node_id="text"),
             BLOCK.build_node_payload(node_id="tts", config_overrides={"api_key_ref": REF}),
             AudioPlayStreamBlock().build_node_payload(node_id="player")]
    nodes[0]["outputs"][0]["text"] = "Bonjour depuis le graphe"
    for index, node in enumerate(nodes):
        node["position"] = {"x": 80 + index * 300, "y": 190}
    return graph_payload("TTS et lecture", nodes, [
        {"id": "text", "from": {"node": "text", "port": 1}, "to": {"node": "tts", "port": 1}, "kind": "data"},
        {"id": "audio", "from": {"node": "tts", "port": 1}, "to": {"node": "player", "port": 1}, "kind": "data"}])


def test_real_graph_modes(*, reordered=False):
    """FB1/FB5: compile and run real routing in both modes, optionally reversing TTS/player ports."""
    import zmq
    payload = document()
    if reordered:
        for node in payload["nodes"]:
            if node["kind"] in {"openai_tts_stream", "audio_play_stream"}:
                node["inputs"].reverse()
                node["outputs"].reverse()
    runtime_document, graph = compile_runtime_graph_document(GraphDocument.from_payload(payload))
    if reordered:
        assert [p.id for p in graph.nodes["tts"].inputs] == [2, 1]
        assert [p.id for p in graph.nodes["tts"].outputs] == [2, 1]
    with TemporaryDirectory(prefix="tts-graph-") as directory, fake_openai() as api:
        root = Path(directory)
        wallet = SecretManager(root / "secrets")
        wallet.initialize("test-only-wallet-password")
        wallet.set_secret(ref=REF, value=KEY)
        engine = WorkflowOrchestrator(root_dir=root, runs_dir=root / "runs", secret_manager=wallet, active_worker_host="thread")
        run = engine.prepare_active_run(graph, document=runtime_document, run_data_scope=instance_scope(root, payload))
        assert run.status == "prepared", run.logs
        publisher = zmq.Context.instance().socket(zmq.PUB)
        publisher.setsockopt(zmq.LINGER, 0)
        try:
            manager = engine.runtime_audio_egress(run.run_id)
            ticket = manager.create_session(node_id="player", input_port="audio_in")
            reader = manager.attach(ticket.session_id, ticket.ticket)
            session = engine._active_sessions[run.run_id]
            publisher.connect(session.pub_endpoint)
            topic = run.plan.worker_configs["text"].outputs[0].topic
            time.sleep(.2)  # Attach the test publisher; production workers use the ready gate.
            assert api.requests == [], "Preparing Run cannot issue paid requests."
            envelope = MessageEnvelope(run_id=run.run_id, source_node_id="text", source_port_id=1,
                payload="Bonjour du port texte", content_type="text/plain", sequence=1)
            publisher.send_multipart([topic.encode(), envelope.to_json().encode()])
            received = []
            until(lambda: bool(api.requests), "Text graph edge must trigger TTS.")
            deadline = time.monotonic() + 4
            while sum(len(frame.payload) for frame in received) < len(encoded_opus()) and time.monotonic() < deadline:
                frame = reader.receive_frame(.1)
                if frame is not None:
                    received.append(frame)
            assert b"".join(frame.payload for frame in received) == encoded_opus(), run.logs
            assert all(frame.codec == "opus" and frame.sample_rate_hz == 48000 for frame in received)
            assert api.requests[0]["body"]["input"] == "Bonjour du port texte"
            assert "tts:1" not in run.output_values, "Opus must never be persisted as a message output."
            assert KEY not in str(run.results) and KEY not in str(run.logs)
        finally:
            publisher.close(0)
            engine.stop_active_run(run.run_id)
        assert not reader.available
        simulation = engine.create_run(graph, document=runtime_document, runtime_mode="centralized", auto_start=False)
        engine._execute_run(simulation)
        assert simulation.status == "success", simulation.logs
        assert len(api.requests) == 1 and "tts:1" not in simulation.output_values


def test_reordered_graph_modes():
    """FB1/FB5: real text/audio routes and simulation survive reversed persisted port lists."""
    test_real_graph_modes(reordered=True)


def test_properties_browser(page, server, blocking_errors):
    """FB6: real shell, discoverable assets, accessible responsive modal and durable atomic edits."""
    project = create_project_api(server, title="TTS UX", document=document())["project"]
    graph_id = project.get("graph_id") or project["project_id"]
    page.goto(project_editor_url(server.base_url, graph_id, workspace_project_id=project["workspace_project_id"]))
    page.locator('.canvas-node[data-node-id="tts"] h3').dblclick()
    modal = page.locator('[data-generic-block-modal-root][data-node-kind="openai_tts_stream"]')
    modal.wait_for()
    assert '{"action":"interrupt"}' in modal.inner_text()
    assert "son déjà envoyé au lecteur n’est pas coupé" in modal.inner_text()
    assert modal.locator('[data-tts-apply]').is_disabled()
    for width, height, label in ((1440, 1000, "desktop"), (390, 740, "mobile"), (320, 568, "small")):
        page.set_viewport_size({"width": width, "height": height})
        bounds = modal.evaluate("""panel => {
          const rect = panel.getBoundingClientRect(), body = panel.querySelector('.tts-body');
          const apply = panel.querySelector('[data-tts-apply]').getBoundingClientRect();
          const close = panel.querySelector('[data-close-block-modal]').getBoundingClientRect();
          return {background: getComputedStyle(panel).backgroundColor, left: rect.left, right: rect.right,
            top: rect.top, bottom: rect.bottom, overflow: body.scrollWidth > body.clientWidth + 1,
            applyVisible: apply.bottom <= innerHeight && apply.top >= 0,
            closeVisible: close.bottom <= innerHeight && close.top >= 0};
        }""")
        assert bounds["background"] == "rgb(255, 255, 255)" and not bounds["overflow"], bounds
        assert bounds["left"] >= 0 and bounds["right"] <= width and bounds["bottom"] <= height, bounds
        assert bounds["applyVisible"] and bounds["closeVisible"], bounds
        assert modal.locator('[data-block-modal-error-panel]').count() == 1
        page.screenshot(path=artifact_path(f"openai-tts-modal-{label}.png"))
    title = modal.locator('[data-tts-title]')
    title.fill("Voix de test")
    assert modal.locator('[data-tts-apply]').is_enabled()
    title.fill("OpenAI TTS Stream")
    assert modal.locator('[data-tts-apply]').is_disabled()
    advanced = modal.locator('.tts-disclosure').first
    advanced.locator('summary').focus()
    page.keyboard.press("Enter")
    timeout = modal.locator('[data-tts-setting="connect_timeout_sec"]')
    timeout.fill("0")
    advanced.locator('summary').click()
    modal.locator('[data-tts-apply]').click()
    assert advanced.evaluate("element => element.open")
    assert "Vérifiez" in modal.locator('[data-tts-feedback]').inner_text()
    timeout.fill("10")
    title.fill("Voix de test")
    modal.locator('[data-tts-setting="voice"]').select_option("cedar")
    modal.locator('[data-tts-setting="instructions"]').fill("Voix calme")
    modal.locator('[data-tts-apply]').click()
    # The inspector can coexist with the modal; never observe its independent feedback.
    modal.locator('[data-tts-feedback]').filter(has_text="prochain Run").wait_for(timeout=5000)
    modal.locator('[data-close-block-modal]').click()
    page.reload()
    page.set_viewport_size({"width": 1440, "height": 1000})
    page.locator('.canvas-node[data-node-id="tts"] h3').dblclick()
    modal.wait_for()
    assert modal.locator('[data-tts-title]').input_value() == "Voix de test"
    assert modal.locator('[data-tts-setting="voice"]').input_value() == "cedar"
    assert modal.locator('[data-tts-setting="instructions"]').input_value() == "Voix calme"
    modal.locator('[data-close-block-modal]').click()
    assert not blocking_errors, blocking_errors
    test_editor_run_preparation(page, server)


def test_editor_run_preparation(page, server):
    """FB2/FB5: the real editor Run button prepares the saved blueprint using a test-only wallet."""
    http_json(server.base_url, "/api/application/secrets/init", method="POST", payload={"password": "test-only-wallet-password"})
    http_json(server.base_url, "/api/application/secrets", method="POST", payload={"ref": REF, "value": KEY})
    page.click("#activeRuntimeModeButton")
    with page.expect_response(lambda response: response.url.endswith("/runs/prepare") and response.request.method == "POST") as response:
        page.click("#loadRunButton")
    run = response.value.json()
    assert response.value.ok and run.get("status") == "prepared", run.get("logs", run.get("error"))
    try:
        assert not any("[prepare-error]" in line for line in run.get("logs", []))
        # Do not click Play or publish text: preparing Run must not call the provider.
        assert not run.get("output_values", {}).get("tts:1")
    finally:
        with page.expect_response(lambda response: response.url.endswith("/stop") and response.request.method == "POST") as response:
            page.click("#stopRunButton")
        assert response.value.ok
    print("[ok] Real editor Run/Stop with compiled TTS configuration", flush=True)


if __name__ == "__main__":
    for test in (test_compiled_runtime_metadata, test_contract_and_settings, test_port_order, test_ogg_validation_and_timing, test_streaming_and_order,
                 test_failures_and_stop, test_real_graph_modes, test_reordered_graph_modes):
        test()
        print(f"[ok] {test.__name__}", flush=True)
    run_playwright_smoke("F5.48_openai_tts_stream", test_properties_browser)
