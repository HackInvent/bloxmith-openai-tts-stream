"""Convert text to paced Ogg Opus with explicit lifecycle commands using OpenAI Speech."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Mapping
from contextlib import suppress
from html import escape
import importlib.util
import json
import math
import re
import time
from typing import Any
from uuid import uuid4

from bloxsmith_app.block_api import (
    BlockDefinition, BlockRuntimeContext, BlockRuntimeListenerContext, BlockRuntimeOutput,
    BlockRuntimePreparation, BlockRuntimePreparationContext, BlockRuntimeResult,
    RuntimeListenerError, render_inspector_template, render_node_card_template,
)

from .ogg_stream import OggOpusStream, OpusStreamError, SAMPLE_RATE

MODEL = "gpt-4o-mini-tts"
SPEECH_URL = "https://api.openai.com/v1/audio/speech"
VOICES = ("marin", "cedar", "alloy", "ash", "ballad", "coral", "echo", "fable", "nova", "onyx", "sage", "shimmer", "verse")
HTTP_CHUNK_BYTES = 1024  # Do not hold short compressed utterances until a large HTTP buffer fills.
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_TEXT = 4096
MAX_COMMAND_BYTES = 4096
MAX_PENDING_TEXTS = 128
DEFAULTS = {"api_key_ref": "", "voice": "marin", "instructions": "", "speed": 1,
            "connect_timeout_sec": 10, "read_timeout_sec": 30, "max_audio_sec": 180}
BOUNDS = {"speed": (.25, 4), "connect_timeout_sec": (1, 60), "read_timeout_sec": (1, 120), "max_audio_sec": (1, 600)}
RUNTIME_METADATA_KEYS = frozenset({"position", "runtime_path", "runtime_path_label"})
SECRET_REF = re.compile(r"secret://(?:workspace/[A-Za-z0-9_.-]{1,80}|project/[A-Za-z0-9_.-]{1,80}/[A-Za-z0-9_.-]{1,80})")


class TtsError(ValueError):
    """Known-safe, user-facing diagnostic containing neither secret nor provider body."""


def _config(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return validated TTS settings, ignoring only known framework layout/runtime metadata.

    Raw configuration may be an immutable compiled/restored snapshot. Leave it untouched;
    reject other unknown fields and literal credentials independently of technical metadata.
    """
    if raw is not None and not isinstance(raw, Mapping):
        raise TtsError("Configuration TTS invalide.")
    # Compilation adds node ancestry even without composites; these fields are not speech settings.
    if raw and set(raw) - set(DEFAULTS) - RUNTIME_METADATA_KEYS:
        raise TtsError("Paramètre de configuration TTS non pris en charge.")
    result = {key: (raw or {}).get(key, value) for key, value in DEFAULTS.items()}
    for key, maximum in (("api_key_ref", 200), ("instructions", 1024), ("voice", 20)):
        if not isinstance(result[key], str) or len(result[key]) > maximum:
            raise TtsError(f"Champ {key} invalide : {maximum} caractères maximum.")
        result[key] = result[key].strip()
    if result["api_key_ref"] and not SECRET_REF.fullmatch(result["api_key_ref"]):
        raise TtsError("Utilisez une référence complète du wallet, par exemple secret://workspace/openai_tts_api.")
    if result["voice"] not in VOICES:
        raise TtsError("Voix OpenAI non prise en charge.")
    for key, (minimum, maximum) in BOUNDS.items():
        try:
            value = float(result[key])
        except (ValueError, TypeError, OverflowError):
            raise TtsError(f"{key} doit être un nombre.") from None
        if isinstance(result[key], bool) or not math.isfinite(value) or not minimum <= value <= maximum:
            raise TtsError(f"{key} doit être compris entre {minimum:g} et {maximum:g}.")
        result[key] = value
    return result


def _text(raw: Any) -> str:
    """Accept plain text only; one message is one synthesis, without implicit JSON coercion."""
    if not isinstance(raw, str):
        raise TtsError("text_in attend du texte, pas un objet ou des données audio.")
    if len(raw) > MAX_TEXT:
        raise TtsError("Texte trop long : 4 096 caractères maximum par message.")
    return raw.strip()


def _interrupt(raw: Any) -> dict[str, str]:
    """Validate the explicit interruption command, distinct from producer start/stop metadata."""
    if isinstance(raw, str):
        if len(raw.encode("utf-8")) > MAX_COMMAND_BYTES:
            raise TtsError("Commande trop volumineuse : 4 Kio maximum sur command_in.")
        try:
            raw = json.loads(raw)
        except (ValueError, RecursionError):
            raise TtsError('command_in attend le JSON {"action":"interrupt"}.') from None
    if not isinstance(raw, Mapping) or set(raw) != {"action"} or raw["action"] != "interrupt":
        raise TtsError('command_in attend uniquement {"action":"interrupt"}, pas le stop de fin de flux.')
    return {"action": "interrupt"}


def _fresh_input(context: BlockRuntimeContext, name: str) -> tuple[bool, Any]:
    """Read current deliveries only; remembered text/commands must not replay on the other port."""
    attribute = context.input_attribute(name)
    if attribute is None:
        return False, None
    for event in reversed(context.input_events):
        if event.input_port_id == attribute.port_id:
            return True, event.value
    if context.input_events or attribute.status != "updated":
        return False, None
    return True, attribute.value


def _secret(context: Any, config: dict) -> str:
    """Resolve a server-side wallet secret for this job without retaining it in results/state."""
    if not config["api_key_ref"]:
        raise TtsError("Renseignez la référence du secret OpenAI dans les propriétés du bloc.")
    resolver = context.services.get("resolve_secret")
    if not callable(resolver):
        raise TtsError("Le résolveur de secrets du wallet n’est pas disponible.")
    try:
        value = resolver(config["api_key_ref"])
    except Exception:
        raise TtsError("Clé OpenAI inaccessible : déverrouillez le wallet et vérifiez la référence.") from None
    if not isinstance(value, str) or not value.strip() or len(value) > 4096 or any(c in value for c in "\r\n"):
        raise TtsError("Le secret OpenAI est vide ou invalide.")
    return value.strip()


def _failure(error: Exception, stream_id: str = "") -> BlockRuntimeResult:
    """Return safe diagnostics; raw HTTP/provider/secret exceptions are never exposed."""
    if isinstance(error, (TtsError, OpusStreamError)):
        message = str(error)
    elif isinstance(error, RuntimeListenerError):
        message = "File TTS pleine ou arrêtée : attendez la fin des messages en cours, ou relancez Run."
    elif isinstance(error, TimeoutError):
        message = "Synthèse interrompue : délai maximal dépassé."
    else:
        message = "Synthèse interrompue : erreur de connexion ou de transport audio."
    return BlockRuntimeResult(status="failed", outputs=[], error=message, last_message=message,
                              metadata={"openai_tts_stream": {"state": "error", "stream_id": stream_id}})


# FB1 - Separate text/interrupt inputs, Opus audio output and JSON start/stop output.
# FB2 - Wallet-only credentials, bounded validated settings and safe provider errors.
# FB3 - Stream validated paced Ogg Opus before EOF; one independent stream_id per text.
# FB4 - Bounded serial synthesis; data interrupt cancels/purges without stopping the Run.
# FB5 - Honest IO-free simulation plus active mini-graph through public block services.
# FB6 - Professional block-owned modal/inspector, discovery, assets and end-user documentation.
class OpenAITtsStreamBlock(BlockDefinition):
    """Synthesize text with a public audio output, without any browser or framework business code."""

    kind = "openai_tts_stream"

    def _ports(self, context: BlockRuntimePreparationContext | BlockRuntimeContext) -> None:
        """Validate context ports by stable id, without changing their persisted visual order.

        Names, transports and cardinalities stay fixed; legacy Opus nodes may omit
        command_in. Duplicate ids must fail before indexing can hide a port.
        """
        inputs = {port.id: port for port in context.input_ports}
        outputs = {port.id: port for port in context.output_ports}
        message = "Ports TTS invalides : entrée text_in (1), sorties audio_out Opus (1) et command_out (2), entrée command_in (2) facultative."
        if (len(inputs) != len(context.input_ports) or set(inputs) not in ({1}, {1, 2})
                or len(context.output_ports) != 2 or set(outputs) != {1, 2}):
            raise TtsError(message)
        text, audio, lifecycle = inputs[1], outputs[1], outputs[2]
        if (text.name != "text_in" or getattr(text, "transport", "message") != "message"
                or text.multiplicity != "one" or audio.name != "audio_out"
                or audio.transport != "audio_stream" or audio.multiplicity != "many"
                or lifecycle.name != "command_out" or getattr(lifecycle, "transport", "message") != "message"
                or lifecycle.multiplicity != "many"):
            raise TtsError(message)
        if len(inputs) == 2:
            command = inputs[2]
            if (text.required or command.name != "command_in"
                    or command.required or command.multiplicity != "one"
                    or getattr(command, "transport", "message") != "message"
                    or tuple(command.accepts) != ("application/json",)):
                raise TtsError("text_in et command_in doivent être indépendants et non requis pour l’exécution.")
        profile = audio.audio_stream
        codecs = profile.get("codecs", ()) if isinstance(profile, Mapping) else profile.codecs
        if tuple(codecs) != ("opus",):
            raise TtsError("Ancienne sortie TTS PCM : recréez le bloc pour obtenir audio_out Opus et command_out.")

    def prepare_runtime(self, context: BlockRuntimePreparationContext) -> BlockRuntimePreparation:
        """Validate static contracts without IO, then opt into the generic Run listener."""
        self._ports(context)
        _config(context.config)
        return BlockRuntimePreparation(listen_on_run=context.runtime_mode == "zeromq_active")

    def initialize_runtime(self, context: BlockRuntimeContext) -> BlockRuntimeResult:
        """Verify active credentials/dependency locally; never connect or pay merely on Run."""
        try:
            config = _config(context.config)
            if context.runtime_mode == "zeromq_active":
                if importlib.util.find_spec("httpx") is None:
                    raise TtsError("Installez les dépendances du bloc : blocs/openai_tts_stream/requirements.txt.")
                _secret(context, config)
            return BlockRuntimeResult(last_message="TTS prêt : en attente d’un texte sur text_in.")
        except Exception as error:
            return _failure(error)

    def execute_runtime(self, context: BlockRuntimeContext) -> BlockRuntimeResult:
        """Forward fresh text or a priority interrupt to the listener without waiting for HTTP.

        An interrupt wins if both ports are fresh in one activation. A later text starts normally;
        neither a consumed command nor remembered text is implicitly replayed.
        """
        try:
            self._ports(context)
            _config(context.config)
            has_command, raw_command = _fresh_input(context, "command_in")
            has_text, raw_text = _fresh_input(context, "text_in")
            command = _interrupt(raw_command) if has_command else None
            if context.runtime_mode != "zeromq_active":
                return BlockRuntimeResult(status="skipped", outputs=[], last_message="Simulation : aucun appel OpenAI ni flux audio.",
                                          metadata={self.kind: {"state": "simulation"}})
            if not has_command and not has_text:
                return BlockRuntimeResult(status="skipped", last_message="En attente d’un nouveau texte ou d’une commande.")
            sender = context.services.get("runtime_listener")
            if sender is None:
                raise TtsError("Listener TTS indisponible : Stop puis Run.")
            if has_command:
                sender.send(command)
                return BlockRuntimeResult(last_message="Interruption transmise au TTS ; arrêt en cours.",
                    metadata={self.kind: {"state": "interrupt_requested"}})
            text = _text(raw_text)
            if not text:
                return BlockRuntimeResult(status="skipped", last_message="Texte vide : aucune synthèse.")
            audio = context.services.get("runtime_audio_streams")
            if audio is None or not audio.available:
                raise TtsError("Reliez audio_out à une entrée Opus compatible, par exemple Audio Play Stream.audio_in.")
            stream_id = uuid4().hex
            sender.send({"text": text, "stream_id": stream_id})
            return BlockRuntimeResult(last_message="Texte mis en file de synthèse.",
                                      metadata={self.kind: {"state": "queued", "characters": len(text), "stream_id": stream_id}})
        except Exception as error:
            return _failure(error)

    def listen_runtime(self, context: BlockRuntimeListenerContext) -> None:
        """Own a single cancellable async synthesis task on the framework-supervised listener."""
        asyncio.run(self._listen(context, _config(context.config)))

    async def _listen(self, context: BlockRuntimeListenerContext, config: dict) -> None:
        """Poll commands during HTTP; interrupt cancels active audio and purges earlier pending texts.

        The local FIFO is bounded independently of the framework mailbox. Overflow is reported,
        never silently dropped; commands after an interruption belong to the next synthesis wave.
        """
        active = None
        pending = deque()
        stream_id = ""
        try:
            while not context.stop_requested():
                if active is not None and active.done():
                    try:
                        active.result()
                    except Exception as error:
                        context.emit_result(_failure(error, stream_id))
                    active = None
                # Reading while busy is what lets a data command interrupt a stalled HTTP request.
                # Bound each drain pass so ongoing HTTP and runtime cancellation still get CPU time.
                mailbox_empty = False
                for _ in range(MAX_PENDING_TEXTS):
                    command = context.receive_command(timeout_sec=0)
                    if command is None or context.stop_requested():
                        mailbox_empty = command is None
                        break
                    payload = command.payload
                    if isinstance(payload, Mapping) and dict(payload) == {"action": "interrupt"}:
                        discarded = len(pending)
                        pending.clear()
                        interrupted_stream = stream_id if active is not None else ""
                        if active is not None:
                            active.cancel()
                            with suppress(asyncio.CancelledError, Exception):
                                await active
                            active = None
                        context.emit_result(BlockRuntimeResult(
                            last_message="TTS interrompu ; textes en attente supprimés. Le son déjà transmis reste dans le lecteur.",
                            metadata={self.kind: {"state": "interrupted", "stream_id": interrupted_stream,
                                                 "discarded_texts": discarded}}))
                        continue
                    try:
                        if not isinstance(payload, Mapping) or set(payload) != {"text", "stream_id"}:
                            raise TtsError("Commande TTS interne invalide.")
                        candidate = payload["stream_id"]
                        if not isinstance(candidate, str) or not re.fullmatch(r"[0-9a-f]{32}", candidate):
                            raise TtsError("Identifiant de synthèse invalide.")
                        text = _text(payload["text"])
                        if len(pending) >= MAX_PENDING_TEXTS:
                            raise TtsError("File locale TTS pleine : ce texte n’a pas été conservé. Envoyez interrupt ou attendez.")
                        pending.append((text, candidate))
                    except TtsError as error:
                        context.emit_result(_failure(error))
                # Finish draining accepted commands before opening another paid request: an
                # interrupt just beyond the bounded pass may invalidate all pending texts.
                if active is None and pending and mailbox_empty and not context.stop_requested():
                    text, stream_id = pending.popleft()
                    active = asyncio.create_task(self._speak(context, config, text, stream_id))
                await asyncio.sleep(.02)
        finally:
            if active is not None:
                active.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await active

    def _http_client(self, config: dict) -> Any:
        """Create a cancellable verified-TLS client; the endpoint is fixed and redirects are disabled."""
        import httpx
        return httpx.AsyncClient(timeout=httpx.Timeout(connect=config["connect_timeout_sec"],
            read=config["read_timeout_sec"], write=config["connect_timeout_sec"], pool=config["connect_timeout_sec"]),
            follow_redirects=False)

    async def _speak(self, context: BlockRuntimeListenerContext, config: dict, text: str, stream_id: str) -> None:
        """Stream validated native Ogg Opus pages and separate per-text start/stop commands.

        Pace by the Opus clock, never by compressed byte count. Each publication contains one
        bounded page (at most five seconds); preserve headers, pre-skip and end trimming unchanged.
        Start/stop use ordinary result publication; receivers reconcile independently routed bytes.
        """
        if not text:
            return
        audio = context.services.get("runtime_audio_streams")
        if audio is None or not audio.available:
            raise TtsError("La sortie audio_out n’est pas connectée à un récepteur Opus compatible.")
        key = _secret(context, config)
        body = {"model": MODEL, "voice": config["voice"], "input": text, "speed": config["speed"],
                "response_format": "opus", "stream_format": "audio"}
        if config["instructions"]:
            body["instructions"] = config["instructions"]
        frames, total = 0, 0
        downloaded = 0
        container = OggOpusStream()
        started, stopped = False, False
        first_audio_at = None

        def command(action: str, *, aborted: bool = False) -> None:
            """Publish lifecycle metadata only, with exact counts of accepted audio publications."""
            value = {"action": action, "stream_id": stream_id}
            if action == "stop":
                value.update(frame_count=frames, byte_count=total, aborted=aborted)
            context.emit_result(BlockRuntimeResult(outputs=[BlockRuntimeOutput(
                port_id=2, port_name="command_out", value=json.dumps(value), content_type="application/json")]))

        context.emit_result(BlockRuntimeResult(last_message="Génération de la voix OpenAI…",
            metadata={self.kind: {"state": "generating", "stream_id": stream_id, "voice": config["voice"]}}))
        try:
            async with asyncio.timeout(config["max_audio_sec"] + config["read_timeout_sec"] + config["connect_timeout_sec"]):
                async with self._http_client(config) as http:
                    async with http.stream("POST", SPEECH_URL, json=body,
                            headers={"Authorization": f"Bearer {key}", "Accept": "audio/ogg, audio/opus, application/octet-stream"}) as response:
                        if response.status_code != 200:
                            reason = {401: "Clé OpenAI refusée.", 403: "Accès au modèle TTS refusé.",
                                      429: "Quota ou limite OpenAI atteint.", 400: "OpenAI a refusé le texte ou les réglages de voix."}.get(
                                      response.status_code, "Le service de synthèse OpenAI a refusé la requête.")
                            raise TtsError(f"{reason} HTTP {response.status_code}. Aucun nouvel essai automatique.")
                        mime = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                        if mime not in {"audio/ogg", "audio/opus", "application/ogg", "application/octet-stream"}:
                            raise TtsError("OpenAI n’a pas retourné le format Ogg Opus attendu.")
                        async for chunk in response.aiter_bytes(chunk_size=HTTP_CHUNK_BYTES):
                            if context.stop_requested():
                                raise asyncio.CancelledError
                            downloaded += len(chunk)
                            if downloaded > MAX_RESPONSE_BYTES:
                                raise TtsError("Réponse audio trop volumineuse : synthèse interrompue.")
                            previous_seconds = container.seconds
                            for page in container.push(chunk):
                                if container.seconds > config["max_audio_sec"]:
                                    raise TtsError("Durée audio maximale atteinte : synthèse interrompue.")
                                if container.seconds > previous_seconds:
                                    # A late HTTP page rebases the clock: never flood readers to catch up.
                                    first_audio_at = max(first_audio_at or 0, time.monotonic() - previous_seconds)
                                    due = first_audio_at + previous_seconds - .25
                                    while time.monotonic() < due:
                                        if context.stop_requested():
                                            raise asyncio.CancelledError
                                        await asyncio.sleep(max(0, min(.02, due - time.monotonic())))
                                if not started:
                                    command("start")
                                    started = True
                                audio.publish_port("audio_out", page, codec="opus", sample_rate_hz=SAMPLE_RATE,
                                                   channels=container.channels, stream_id=stream_id, correlation_id=stream_id)
                                frames += 1
                                total += len(page)
                                previous_seconds = container.seconds
                                if frames == 1:
                                    context.emit_result(BlockRuntimeResult(last_message="Voix Opus en cours de diffusion sur audio_out.",
                                        metadata={self.kind: {"state": "streaming", "stream_id": stream_id}}))
                        container.finish()
                        if context.stop_requested():
                            raise asyncio.CancelledError
                        command("stop")
                        stopped = True
                # Drain the production lead between utterances too: repeated short texts must
                # not accumulate an unbounded playback delay despite per-request pacing.
                due = first_audio_at + container.seconds
                while time.monotonic() < due:
                    if context.stop_requested():
                        raise asyncio.CancelledError
                    await asyncio.sleep(max(0, min(.02, due - time.monotonic())))
            if context.stop_requested():
                raise asyncio.CancelledError
            context.emit_result(BlockRuntimeResult(last_message="Synthèse diffusée ; le lecteur termine les derniers échantillons.",
                metadata={self.kind: {"state": "completed", "stream_id": stream_id, "frames_sent": frames,
                                     "bytes_sent": total, "audio_seconds": container.seconds, "codec": "opus", "container": "ogg"}}))
        finally:
            if started and not stopped:
                # Runtime Stop revokes result queues; abort metadata is best-effort, never a fake success.
                with suppress(RuntimeListenerError):
                    command("stop", aborted=True)
            key = ""  # Never retain a credential in block instance state or a result.

    def render_node_card(self, *, node: dict, payload: dict | None = None) -> dict:
        """Render a compact source card with voice and transport, never a credential."""
        config = _config(node.get("config"))
        return render_node_card_template(block=self, node=node, node_classes=["openai-tts-node"], replacements={
            "title": str(node.get("title") or self.default_title()), "voice": config["voice"],
            "configured": "Clé référencée" if config["api_key_ref"] else "Configurer le secret OpenAI"})

    def _settings_html(self, node: dict) -> str:
        """Render grouped, labelled settings with progressive disclosure and clear cost/privacy scope."""
        config = _config(node.get("config"))
        ref = escape(config["api_key_ref"], quote=True)
        instructions = escape(config["instructions"])
        voices = "".join(f'<option value="{voice}"{" selected" if voice == config["voice"] else ""}>{voice.capitalize()}</option>' for voice in VOICES)
        def number(key: str, label: str) -> str:
            """Render a numeric field with its server-enforced bounds and compact default."""
            minimum, maximum = BOUNDS[key]
            return (f'<label>{label}<input type="number" data-tts-setting="{key}" value="{config[key]:g}" '
                    f'min="{minimum:g}" max="{maximum:g}" step="any" required /></label>')
        return (
            '<section class="tts-section"><div class="tts-heading"><h3>Connexion OpenAI</h3>'
            f'<span class="tts-badge">{MODEL}</span></div><label>Référence du secret'
            f'<input type="text" data-tts-setting="api_key_ref" value="{ref}" maxlength="200" spellcheck="false" '
            'autocomplete="off" placeholder="secret://workspace/openai_tts_api" /></label>'
            '<p class="tts-help">Copiez la référence complète depuis Paramètres → Secrets. La clé reste dans le wallet côté serveur.</p>'
            '<p class="tts-notice">Le texte est envoyé à OpenAI et facturé sur votre compte API. Aucun appel n’est fait en ouvrant cette fenêtre.</p></section>'
            '<section class="tts-section"><h3>Voix et interprétation</h3><div class="tts-grid">'
            f'<label>Voix<select data-tts-setting="voice">{voices}</select></label>{number("speed", "Vitesse (×)")}</div>'
            f'<label>Consignes de voix <span class="tts-optional">Facultatif</span><textarea data-tts-setting="instructions" maxlength="1024" '
            f'rows="3" placeholder="Parle en français, avec une voix calme et naturelle.">{instructions}</textarea></label>'
            '<p class="tts-help">Le texte à prononcer arrive sur text_in. Ces consignes règlent le ton et la diction, pas le contenu du message.</p></section>'
            '<details class="tts-disclosure"><summary>Délais et limites</summary><div class="tts-disclosure-body"><div class="tts-grid">'
            f'{number("connect_timeout_sec", "Connexion maximale (s)")}{number("read_timeout_sec", "Attente réseau maximale (s)")}'
            f'{number("max_audio_sec", "Durée audio maximale (s)")}</div></div></details>'
            '<p class="tts-help">Voix générée par IA : informez les personnes qui l’écoutent. Chaque message est traité dans l’ordre ; '
            'branchez une sortie de texte final, pas un texte provisoire qui change à chaque mot.</p>')

    def render_modal(self, *, node: dict, payload: dict | None = None) -> dict:
        """Render fixed actions and diagnostics, explaining whether this node has command_in."""
        template = (self.directory / "block_modal.html").read_text(encoding="utf-8")
        template = template.replace("{{ settings_html }}", self._settings_html(node))
        template = template.replace("{{ command_help }}", self._command_help_html(node))
        if self._runtime_error_text(payload or {}):
            template = template.replace('class="tts-disclosure tts-diagnostics"', 'class="tts-disclosure tts-diagnostics" open')
        return {"html": self._render_generic_modal_template(template=template, node=node, payload=payload or {}),
                "context": {"node_id": str(node.get("id") or ""), "node_kind": self.kind}}

    def _command_help_html(self, node: dict) -> str:
        """Distinguish current command-capable nodes from persisted one-input Opus nodes."""
        if not any(port.get("id") == 2 and port.get("name") == "command_in" for port in node.get("inputs", [])):
            return ('<p class="tts-help">Ce bloc n’a pas d’entrée command_in. Pour l’ajouter, arrêtez le Run, '
                    'recréez le TTS depuis le catalogue et reprenez ses réglages et ses liens. '
                    'La synthèse existante reste utilisable sans interruption data.</p>')
        return ('<p class="tts-help">Sur l’entrée <code>command_in</code> de ce TTS, envoyez '
                '<code>{"action":"interrupt"}</code> pour annuler la synthèse et vider les textes en attente. '
                'Le son déjà envoyé au lecteur n’est pas coupé.</p>')

    def render_inspector_panel(self, *, node: dict, payload: dict | None = None) -> dict:
        """Keep shared settings and node-specific command help in the inspector's standard tabs."""
        template = (self.directory / "inspector_panel.html").read_text(encoding="utf-8")
        return {"html": render_inspector_template(template=template, node={**node, "type": self.kind, "kind": self.kind},
                    payload=payload, replacements={"settings_html": self._settings_html(node),
                        "command_help": self._command_help_html(node)}, show_duplicate=True),
                "context": {"node_id": str(node.get("id") or ""), "full_panel": True}}

    def handle_ui_action(self, *, node: dict, action: str, values: dict, payload: dict | None = None) -> dict:
        """Save the complete form atomically, rejecting raw credentials and invalid hidden fields."""
        try:
            if action == "save_properties":
                if not isinstance(values, dict) or set(values) - {"title", "config"}:
                    raise TtsError("Propriétés TTS invalides.")
                title = values.get("title", node.get("title") or self.default_title())
                if not isinstance(title, str) or not 1 <= len(title.strip()) <= 200:
                    raise TtsError("Le nom doit contenir entre 1 et 200 caractères.")
                patch = values.get("config", {})
                if not isinstance(patch, dict) or set(patch) - set(DEFAULTS):
                    raise TtsError("Réglages TTS invalides.")
                config = _config({**(node.get("config") or {}), **patch})
                return {"node_patch": {"title": title.strip(), "config": config}, "rerender_inspector": False}
            result = super().handle_ui_action(node=node, action=action, values=values, payload=payload)
            patch = result.get("node_patch", {}).get("config")
            if isinstance(patch, dict):
                if set(patch) - set(DEFAULTS):
                    raise TtsError("Réglages TTS invalides.")
                normalized = _config({**(node.get("config") or {}), **patch})
                result["node_patch"]["config"] = {key: normalized[key] for key in patch}
            return result
        except TtsError as error:
            return {"error": str(error)}
