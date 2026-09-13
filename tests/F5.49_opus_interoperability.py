#!/usr/bin/env python3
"""FB1/FB3/FB5: real TTS→STT/Save/Player routing, Opus decode and IO-free simulation.

Both OpenAI endpoints are local fakes. No user blueprint, microphone or API key is used.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy
import subprocess
import sys
from tempfile import TemporaryDirectory
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from blocs.audio_play_stream.block import AudioPlayStreamBlock
from blocs.display.block import DisplayBlock
from blocs.microphone_stream.block import MicrophoneStreamBlock
from blocs.openai_realtime_stt.block import OpenAIRealtimeSttBlock
from blocs.openai_tts_stream.block import OpenAITtsStreamBlock
from blocs.save_audio.block import SaveAudioBlock
from blocs.text.block import TextBlock
from bloxsmith_app.graph_compile import compile_runtime_graph_document
from bloxsmith_app.graph_document import GraphDocument
from bloxsmith_app.messages import MessageEnvelope
from bloxsmith_app.orchestrator import WorkflowOrchestrator
from bloxsmith_app.secrets import SecretManager
from ui_smoke_common import graph_payload, run_playwright_smoke
from block_test_fixtures import instance_scope

TTS = runpy.run_path(str(ROOT / "blocs/openai_tts_stream/tests/F5.48_openai_tts_stream_block.py"))
STT = runpy.run_path(str(ROOT / "blocs/openai_realtime_stt/tests/F5.46_openai_realtime_stt_block.py"))


def document(source, receiver, *, commanded):
    """Compile the real catalog contracts for every streaming producer/consumer pair."""
    nodes = [source.build_node_payload(node_id="source"), receiver.build_node_payload(node_id="sink")]
    edges = [{"id": "audio", "from": {"node": "source", "port": 1}, "to": {"node": "sink", "port": 1}}]
    if commanded:
        edges.append({"id": "commands", "from": {"node": "source", "port": 2}, "to": {"node": "sink", "port": 2}})
    return graph_payload("Opus compatibility", nodes, edges)


def test_port_matrix():
    """FB1: both producers connect to all three consumers, with no data disguised as audio."""
    for source in (MicrophoneStreamBlock(), OpenAITtsStreamBlock()):
        assert source.model["ports"]["outputs"][0]["audio_stream"]["codecs"] == ["opus"]
        for receiver in (OpenAIRealtimeSttBlock(), SaveAudioBlock(), AudioPlayStreamBlock()):
            payload = document(source, receiver, commanded=receiver.kind != "audio_play_stream")
            _, graph = compile_runtime_graph_document(GraphDocument.from_payload(payload))
            assert graph.nodes["source"].outputs[0].transport == "audio_stream"
            assert graph.nodes["sink"].inputs[0].transport == "audio_stream"
    # Extra receiver formats remain compatible with already supported external sources.
    assert "aac" in OpenAIRealtimeSttBlock().model["ports"]["inputs"][0]["audio_stream"]["codecs"]
    assert "aac" in SaveAudioBlock().model["ports"]["inputs"][0]["audio_stream"]["codecs"]
    assert "pcm_s16le" in AudioPlayStreamBlock().model["ports"]["inputs"][0]["audio_stream"]["codecs"]


def test_tts_fanout(channels):
    """FB3/FB5: a real streamed response feeds recording, transcription and browser egress together."""
    import zmq
    encoded = TTS["encoded_opus"](3.3, channels)
    with TemporaryDirectory(prefix="opus-interop-") as directory, TTS["fake_openai"](payload=encoded) as speech, STT["fake_openai"]() as transcription:
        root = Path(directory)
        wallet = SecretManager(root / "secrets")
        wallet.initialize("test-only-wallet-password")
        wallet.set_secret(ref=TTS["REF"], value=TTS["KEY"])
        nodes = [TextBlock().build_node_payload(node_id="text"),
            OpenAITtsStreamBlock().build_node_payload(node_id="tts", config_overrides={"api_key_ref": TTS["REF"]}),
            OpenAIRealtimeSttBlock().build_node_payload(node_id="stt", config_overrides={"api_key_ref": TTS["REF"], "segment_seconds": 1}),
            SaveAudioBlock().build_node_payload(node_id="save", config_overrides={"output_dir": str(root / "recordings")}),
            AudioPlayStreamBlock().build_node_payload(node_id="player"),
            DisplayBlock().build_node_payload(node_id="partial"), DisplayBlock().build_node_payload(node_id="final")]
        nodes[0]["outputs"][0]["text"] = "Parole de test"
        links = [("text", 1, "tts", 1), ("tts", 1, "stt", 1), ("tts", 2, "stt", 2),
            ("tts", 1, "save", 1), ("tts", 2, "save", 2), ("tts", 1, "player", 1),
            ("stt", 1, "partial", 1), ("stt", 2, "final", 1)]
        payload = graph_payload("TTS Opus fan-out", nodes, [{"id": f"link-{index}",
            "from": {"node": source, "port": sp}, "to": {"node": sink, "port": dp}}
            for index, (source, sp, sink, dp) in enumerate(links)])
        runtime_document, graph = compile_runtime_graph_document(GraphDocument.from_payload(payload))
        engine = WorkflowOrchestrator(root_dir=root, runs_dir=root / "runs", secret_manager=wallet, active_worker_host="thread")
        simulation = engine.create_run(graph, document=runtime_document, runtime_mode="centralized", auto_start=False)
        engine._execute_run(simulation)
        assert simulation.status == "success", simulation.logs
        assert not speech.requests and transcription.opened == 0 and not (root / "recordings").exists()
        assert not any(key.startswith("tts:") for key in simulation.output_values), "Simulation emits neither audio nor commands."

        run = engine.prepare_active_run(graph, document=runtime_document, run_data_scope=instance_scope(root, payload))
        assert run.status == "prepared", run.logs
        publisher = zmq.Context.instance().socket(zmq.PUB)
        publisher.setsockopt(zmq.LINGER, 0)
        received = []
        try:
            manager = engine.runtime_audio_egress(run.run_id)
            ticket = manager.create_session(node_id="player", input_port="audio_in")
            reader = manager.attach(ticket.session_id, ticket.ticket)
            session = engine._active_sessions[run.run_id]
            publisher.connect(session.pub_endpoint)
            topic = run.plan.worker_configs["text"].outputs[0].topic
            time.sleep(.2)
            assert not speech.requests and transcription.opened == 0
            envelope = MessageEnvelope(run_id=run.run_id, source_node_id="text", source_port_id=1,
                payload="Synthèse interopérable", content_type="text/plain", sequence=1)
            publisher.send_multipart([topic.encode(), envelope.to_json().encode()])
            deadline = time.monotonic() + 10
            final_before_stop = False
            while time.monotonic() < deadline and sum(len(frame.payload) for frame in received) < len(encoded):
                frame = reader.receive_frame(.05)
                if frame is not None:
                    received.append(frame)
                command = json.loads(run.output_values.get("tts:2", {}).get("value") or "{}")
                if run.output_values.get("stt:2") and command.get("action") == "start":
                    final_before_stop = True
            assert b"".join(frame.payload for frame in received) == encoded, run.logs
            assert final_before_stop, "Confirmed STT segments must remain available before source stop."
            TTS["until"](lambda: run.results.get("save", {}).get("save_audio", {}).get("state") == "saved", "Save did not finalize from TTS stop.")
            TTS["until"](lambda: run.results.get("stt", {}).get("openai_realtime_stt", {}).get("state") == "completed", "STT did not finish from TTS stop.")
            assert run.output_values["stt:2"]["value"] == "final 4"
            assert run.output_values["stt:1"]["value"].startswith("provisoire")
            TTS["until"](lambda: run.node_statuses["final"] == "success" and run.node_statuses["partial"] == "success",
                "Both Display workers must execute their asynchronously delivered text inputs.")
            assert abs(len(transcription.pcm) - 158400) <= 64, len(transcription.pcm)
            files = list((root / "recordings").glob("*.ogg"))
            assert len(files) == 1 and files[0].read_bytes() == encoded
            assert not list((root / "recordings").glob(".*.part"))
            probe = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "stream=codec_name,sample_rate,channels", "-of", "json", str(files[0])],
                check=True, capture_output=True, timeout=5)
            profile = json.loads(probe.stdout)["streams"][0]
            assert profile == {"codec_name": "opus", "sample_rate": "48000", "channels": channels}
            stop = json.loads(run.output_values["tts:2"]["value"])
            assert stop == {"action": "stop", "stream_id": received[0].stream_id, "frame_count": len(received), "byte_count": len(encoded), "aborted": False}
            assert all(frame.codec == "opus" and frame.channels == channels and frame.sample_rate_hz == 48000 for frame in received)
            assert "tts:1" not in run.output_values
            assert TTS["KEY"] not in str(run.results) + str(run.logs)
        finally:
            publisher.close(0)
            engine.stop_active_run(run.run_id)
        return [{"payload": base64.b64encode(frame.payload).decode("ascii"), "codec": frame.codec,
            "sample_rate_hz": frame.sample_rate_hz, "channels": frame.channels, "sequence": frame.sequence,
            "stream_id": frame.stream_id, "source_id": frame.source_id} for frame in received]


def verify_player(page, server, blocking_errors, captures):
    """Decode the actual TTS egress bytes with native WebCodecs and verify exact trimmed duration."""
    page.goto(server.base_url)
    page.set_content('<button id="activate">Activer le son de test</button>')
    for asset in ("common", "opus_demux", "browser_runtime"):
        page.add_script_tag(path=str(ROOT / f"blocs/audio_play_stream/assets/js/{asset}.js"))
    page.evaluate("""() => { document.querySelector('#activate').onclick = async () => {
      window.testAudio = new AudioContext({ sampleRate: 48000 }); await window.testAudio.resume();
    }; }""")
    page.click("#activate")
    result = page.evaluate("""async captures => {
      const abort = new AbortController();
      const player = CWAudioPlayStream.createPlayer({ node: { config: { max_buffer_sec: 10 } }, audioContext: testAudio, signal: abort.signal });
      try {
        for (const capture of captures) for (const frame of capture) {
          await player.enqueue({ ...frame, payload: Uint8Array.from(atob(frame.payload), c => c.charCodeAt(0)).buffer });
        }
        return player.snapshot();
      } finally { abort.abort(); await testAudio.close(); }
    }""", captures)
    assert result["playedSamples"] == 2 * 3.3 * 48000, result
    assert result["receivedFrames"] == sum(map(len, captures)) and not result["error"], result
    assert not blocking_errors, blocking_errors


if __name__ == "__main__":
    test_port_matrix()
    print("[ok] Opus port compatibility: 2 producers × 3 consumers", flush=True)
    captures = [test_tts_fanout(channels) for channels in (1, 2)]
    print("[ok] TTS Opus → STT / Save / Player, mono/stereo, both modes", flush=True)
    run_playwright_smoke("F5.49_opus_interoperability", lambda page, server, errors: verify_player(page, server, errors, captures))
