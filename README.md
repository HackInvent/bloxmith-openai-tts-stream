# OpenAI TTS Stream

<!-- block-metadata:start -->
[![Block version: 0.1.0](https://img.shields.io/badge/block-0.1.0-blue)](model.json)
[![BloxSmith compatibility: 1.0.9](https://img.shields.io/badge/BloxSmith-1.0.9-brightgreen)](compatibility.json)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Verified BloxSmith versions: **1.0.9** (bundled-block tests; see [test evidence](compatibility.json)).
<!-- block-metadata:end -->


An autonomous `openai_tts_stream` block that turns each text message into AI-generated speech and progressively publishes **Opus in Ogg**, compatible with **Audio Play Stream**, **OpenAI Realtime STT** and **Save Audio**. It never builds a complete audio file or sends audio through a data port.

## Usage

1. Install the block's dependencies into the Python environment running BloxSmith. From this block repository: `python3 -m pip install -r requirements.txt`.
2. Add an OpenAI key to the wallet, unlock it, and enter only its complete secret reference, for example `secret://workspace/openai_tts_api`. Project-scoped references are also accepted. Do not enter the key itself.
3. Connect the text source to `text_in`, then `audio_out` to `Audio Play Stream.audio_in`.
4. Apply settings, start **Active Runtime / Run**, enable browser sound and send text. No command link is needed between TTS and the player. Enable sound before triggering an automatically starting text source.
5. To interrupt synthesis without stopping the Run, send `{"action":"interrupt"}` through `command_in`. This cancels the current request and removes queued text; later text can still be spoken. **Audio already sent to the browser is not stopped by this TTS command.**
6. Global Stop cancels synthesis and discards pending text. Closing properties does not stop synthesis or playback.

For transcription or recording, connect both `audio_out → audio_in` and `command_out → command_in` on the same receiver. Each text automatically produces start, audio, then stop; STT/Save Audio need no extra click. Wait for their finalization before stopping the Run.

### Existing nodes

For an **old PCM TTS node**, stop the Run, recreate the block from the catalog, restore settings and reconnect it. Ports are persisted in the blueprint; updating code does not replace old ports. Preparation reports this requirement, without modifying the blueprint. Reload the application after updating.

An **old Opus node without `command_in`** still works without data interruption. Recreate it while stopped to obtain the new input. Existing nodes never gain a port silently; the modal and inspector explain this limitation.

Text and voice instructions are sent to OpenAI and billed to the API account. Tell users that the voice is **AI-generated**. Opening properties or preparing a Run does not synthesize speech; text must arrive on `text_in`.

## Ports and provider contract

| Port | Transport | Behavior |
| --- | --- | --- |
| `text_in` (1) | message | One source; text up to 4,096 characters. Not required, so commands can run alone. Empty text is ignored; objects are rejected. |
| `command_in` (2) | message | One optional JSON source; only `{"action":"interrupt"}`. Cancels synthesis and clears the local queue without stopping the Run. |
| `audio_out` (1) | audio_stream | One output, multiple consumers; Opus/Ogg, 48 kHz decode clock, mono/stereo according to the actual header. |
| `command_out` (2) | message | Separate start/stop JSON for multiple consumers. Not needed by the player; required by STT and Save Audio. |

Input/output visual order is unrestricted: IDs, not positions, identify ports. Swapping either pair requires no recreation or rewiring. Names, IDs, transports and multiplicities remain fixed; missing, duplicate or incompatible ports are rejected. Validation never reorders a blueprint.

The request uses `POST https://api.openai.com/v1/audio/speech`, fixed model `gpt-4o-mini-tts`, `response_format: opus` and `stream_format: audio`.

The block requires and validates an **Ogg Opus container**, not merely its MIME label. WebM, PCM or unexpected content fails instead of being relabeled. Complete pages are published during download, preserving original bytes without re-encoding. Each text has its own `stream_id`/`correlation_id`. The audio port carries no text, path or implicit command. Published counters mean “sent to the port”, not “heard by a user”.

## Capture commands

```json
{"action":"start","stream_id":"session-unique"}
{"action":"stop","stream_id":"session-unique","frame_count":12,"byte_count":32000,"aborted":false}
```

Start follows validation of the first Ogg header and precedes its audio publication. Stop follows validation of the container end and publication of its last bytes. Counters include **all pages, including headers**.

Audio and data transports are independent: a receiver may see audio before start, or stop before the last frames. It must correlate stream IDs, sequences and totals. Interruption after start sends an aborted stop when possible; errors before start open no remote session. Delivery is not guaranteed after runtime Stop. A normal stop means production ended, **not that a playback queue should be cut off**.

## Queueing and interruption

Requests are sequential with no automatic retry. The listener starts on Run and consumes commands during HTTP waits and audio pacing. It holds one active request and at most 128 pending texts, in addition to the framework mailbox's 128 messages. Overflow is reported explicitly; rejected text is not retained.

Interruption removes earlier texts in local receive order; later messages proceed normally. It does not cancel future results from a still-running Codex or implement conversation-turn filtering. A full framework mailbox can also reject the interrupt command; the block then makes no claim that stopping succeeded.

The inputs are independent (`on_each_event`). Commands work without text and after previous text has been consumed. Only fresh inputs are handled: cached commands do not interrupt later text, and cached text is not synthesized again on a command.

If both inputs are fresh in the same activation, interruption wins and that activation's text is discarded. Manual replay without a new value is ignored. Start/stop belong only to `command_out`; other input actions, malformed JSON and extra fields are rejected. Text-form JSON commands are limited to 4 KiB, as on the Speaker.

`interrupt_requested` means the command was delivered to the listener. `interrupted` confirms processing and reports the number of removed texts; neither claims the browser Speaker stopped.

Interrupting before the first bytes creates no fake remote start/stop pair. After start, the block tries to send `aborted: true` with the counts actually published.

## Pacing and validation

Production follows Opus timing positions at 48 kHz, never compressed-byte counts. Maximum lead is one audio page plus 250 ms, drained between texts; provider page duration influences latency. Network delays reset the clock without catch-up bursts. Stop also cancels network waits.

HTTP errors, empty/truncated responses and exceeded limits produce readable diagnostics. Published audio cannot be recalled after an error. No automatic paid retry is made; other queued texts remain eligible for processing.

Incremental validation checks CRC, ordering/continuity, Opus headers, clock and EOS. Limits are:

- One non-chained Ogg stream; Opus mapping 0, mono/stereo.
- Pages up to 65,307 bytes and five seconds.
- Packets/comments up to 64 KiB.
- HTTP response up to 64 MiB, plus configured duration limits.

A header's informational input rate may be 24 kHz; the Opus transport/decode clock remains 48 kHz.

## Runtime and interoperability

- **Active Runtime (`zeromq_active`)** resolves the secret server-side, listens for text and publishes through `runtime_audio_streams.publish_port`. The Run remains listening until Stop.
- **One Shot Simulation (`centralized`)** returns `skipped`, without secret resolution, OpenAI calls or fake audio. Fresh commands are validated but not executed; invalid JSON is still reported.
- **STT → TTS**: connect `final_out`, not `partial_out`, to avoid speaking every revision. Each message is spoken once without implicit deduplication or cross-message sentence assembly.
- **Save Audio**: one `.ogg` file per text, automatically finalized by TTS stop.
- **OpenAI Realtime STT**: internal conversion to 24 kHz PCM for its API; that PCM is not sent through graph links. Confirmed final segments remain available before stop.
- **Shared format**: Microphone Stream emits Opus/WebM or Opus/Ogg; TTS emits Opus/Ogg. Existing extra consumer formats remain supported (AAC for STT/Save, PCM for the player); TTS no longer emits raw PCM.

## Settings and UI

| Setting | Default | Limit |
| --- | --- | --- |
| Secret reference | Empty | Full wallet reference, never a plaintext key |
| Voice | `marin` | `marin`, `cedar`, `alloy`, `ash`, `ballad`, `coral`, `echo`, `fable`, `nova`, `onyx`, `sage`, `shimmer`, `verse` |
| Speed | 1× | 0.25–4× |
| Voice instructions | Empty | Optional; at most 1,024 characters |
| Connection timeout | 10 s | 1–60 s |
| Network idle timeout | 30 s | 1–120 s without data |
| Maximum audio duration | 180 s | 1–600 s per message |

The modal uses the shell's opaque panel, fixed actions, a scrollable body and labeled fields. Timeouts/diagnostics are collapsible; errors expand diagnostics, and invalid advanced fields are revealed before focus. Modal and inspector share settings through their own JavaScript. Apply becomes available after a change and atomically saves name/configuration for the next Run.

## Architecture and security

Python, HTML, CSS and JavaScript behavior belongs entirely to this directory. Python uses only `bloxsmith_app.block_api` for framework dependencies. No endpoint, shared service, browser codec or orchestrator-specific branch is added.

Parser/compiler metadata `position`, `runtime_path` and `runtime_path_label` is accepted and ignored without changing framework context, including restored Runs. These are not synthesis settings or editable form fields. Other unknown parameters still fail with configuration errors distinct from key/wallet errors; secret and voice validation are unchanged.

`httpx` is server-side only. TLS verification remains enabled, the URL is fixed and redirects are refused. Secrets and raw OpenAI error bodies are excluded from results, block logs, the browser and persisted configuration.

The interrupt input changes neither the framework, Codex nor browser player. Internal `ogg_stream.py` validates and paces containers; it does not decode audio, depend on another block or add a framework service.

## Verification

From the private integration workspace:

```sh
python3 -B tests/run_tests.py openai_tts_stream
```

Captures are stored in ignored results without personal paths. FB1–FB6 cover ports/configuration, safe secret handling/errors, local HTTP streaming mocks, order/pacing/cancellation, real Audio Play mini-graphs in both modes, discovery and forms in the real shell.

Port-order regression covers every input/output permutation, text and interrupt routing, legacy one-input Opus nodes and rejection of genuinely invalid contracts. A compiled TTS → Player graph with reversed ports checks actual audio transport and simulation without changing visual order.

`F5.49_opus_interoperability.py` tests six source/consumer connections, then TTS fan-out to STT/Save/Player in both modes and mono/stereo: byte-identical saved Ogg, FFprobe-verified profiles, provisional/final text before stop, automatic finalization and exact native-WebCodecs decoded duration.

Tests use FFmpeg-generated synthetic audio. FFmpeg is needed by these tests, not by TTS-only execution. Receiver-owned suites also cover WebM.

Preparation regression includes full blueprint compilation and reloading its flattened runtime document, not only hand-built graphs. The editor Run button is tested with a fake wallet, without Play or text. No real key or paid OpenAI request is used.

`F5.50_tts_interrupt_commands.py` covers interruption during a network wait, queue purge/resumption, independent fresh inputs, strict commands, a graph-delivered command after text consumption and no-I/O simulation.

Provider references: [OpenAI text-to-speech guide](https://developers.openai.com/api/docs/guides/text-to-speech) and [Speech API reference](https://developers.openai.com/api/reference/resources/audio/subresources/speech/methods/create).

## Compatibility policy

[compatibility.json](compatibility.json) records HackInvent's verified BloxSmith versions and test evidence. Only the versions listed above have been verified, using the block-owned suites in a **bundled-block test installation**. This is not a certification of managed-package installation, every browser/OS, or live provider availability. Other framework versions are unverified, not necessarily incompatible.

The block-version badge follows `model.json`, not a published Git tag. `unversioned` means that no block release version is declared; no number is inferred from the framework version. The framework still uses `model.json` for its runtime/install contract; the tester-owned JSON does not replace it. Official integration tests run in the private `bloxmith-blocs` workspace. Test helpers and the proprietary framework are not bundled in this public block repository.
