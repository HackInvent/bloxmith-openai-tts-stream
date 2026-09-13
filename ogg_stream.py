"""Bounded Ogg/Opus validation and audio timing for the block's Speech HTTP response.

No decoding or framework routing belongs here. Validated pages retain their original bytes;
granules use the Opus 48 kHz clock, not the informational OpusHead input sample rate.
"""

from collections.abc import Iterator

SAMPLE_RATE = 48000
MAX_PAGE_BYTES = 65307
MAX_PACKET_BYTES = 65536
MAX_PAGE_SECONDS = 5


class OpusStreamError(ValueError):
    """Safe format diagnostic that never includes provider content."""


def _crc_entry(value: int) -> int:
    """Build one entry of the non-reflected Ogg CRC-32 lookup table."""
    value <<= 24
    for _ in range(8):
        value = ((value << 1) ^ (0x04C11DB7 if value & 0x80000000 else 0)) & 0xFFFFFFFF
    return value


_CRC_TABLE = tuple(_crc_entry(value) for value in range(256))


def page_crc(page: bytes) -> int:
    """Compute an Ogg page checksum, treating its stored checksum field as zero."""
    crc = 0
    for index, value in enumerate(page):
        if 22 <= index < 26:
            value = 0
        crc = ((crc << 8) & 0xFFFFFFFF) ^ _CRC_TABLE[(crc >> 24) ^ value]
    return crc


def _packet_samples(packet: bytes) -> int:
    """Read the Opus TOC duration without decoding; reject empty/oversized durations."""
    if not packet:
        raise OpusStreamError("Paquet Opus vide.")
    toc = packet[0]
    config = toc >> 3
    samples = (120 << (config & 3)) if config >= 16 else ((480 << (config & 1)) if config >= 12 else (480, 960, 1920, 2880)[config & 3])
    code = toc & 3
    if code == 3 and len(packet) < 2:
        raise OpusStreamError("Paquet Opus tronqué.")
    frames = (packet[1] & 63) if code == 3 else (1 if code == 0 else 2)
    if not 0 < frames * samples <= 5760:
        raise OpusStreamError("Durée de paquet Opus invalide.")
    return frames * samples


class OggOpusStream:
    """Validate one fresh, non-chained mono/stereo Ogg Opus stream incrementally.

    Call ``push`` with bounded HTTP chunks and fully consume its iterator before the next call.
    Each yielded page exposes cumulative playable ``seconds`` and stable ``channels``. ``finish``
    requires a complete EOS, so transport EOF alone never legitimizes a truncated recording.
    """

    def __init__(self) -> None:
        """Allocate only a partial page/packet and counters, never a complete audio file."""
        self.buffer = bytearray()
        self.packet = bytearray()
        self.serial = None
        self.sequence = 0
        self.packets = 0
        self.channels = 0
        self.pre_skip = 0
        self.samples = 0
        self.granule = 0
        self.seconds = 0.0
        self.ended = False

    def push(self, chunk: bytes) -> Iterator[bytes]:
        """Yield complete validated pages across arbitrary HTTP boundaries, with finite memory."""
        if len(chunk) > 8192 or len(self.buffer) + len(chunk) > MAX_PAGE_BYTES + 8192:
            raise OpusStreamError("Tampon Ogg trop volumineux.")
        self.buffer.extend(chunk)
        while self.buffer:
            if self.ended:
                raise OpusStreamError("Données après la fin du flux Ogg ; flux chaînés non pris en charge.")
            if len(self.buffer) < 27:
                return
            if self.buffer[:4] != b"OggS" or self.buffer[4] != 0:
                raise OpusStreamError("OpenAI n’a pas retourné le conteneur Ogg Opus attendu.")
            header_size = 27 + self.buffer[26]
            if len(self.buffer) < header_size:
                return
            size = header_size + sum(self.buffer[27:header_size])
            if len(self.buffer) < size:
                return
            page = bytes(self.buffer[:size])
            del self.buffer[:size]
            self._page(page, header_size)
            yield page

    def _page(self, page: bytes, header_size: int) -> None:
        """Check order, CRC, headers and sample timing before exposing a page to consumers."""
        flags = page[5]
        serial = int.from_bytes(page[14:18], "little")
        sequence = int.from_bytes(page[18:22], "little")
        granule = int.from_bytes(page[6:14], "little", signed=True)
        if self.serial is None:
            self.serial = serial
        if (flags & ~7 or bool(flags & 2) != (self.sequence == 0) or serial != self.serial
                or sequence != self.sequence or bool(flags & 1) != bool(self.packet)):
            raise OpusStreamError("Pages Ogg manquantes, désordonnées ou profil modifié.")
        if page_crc(page) != int.from_bytes(page[22:26], "little"):
            raise OpusStreamError("Réponse Ogg corrompue : checksum invalide.")
        self.sequence += 1
        offset = header_size
        previous_samples, previous_seconds = self.samples, self.seconds
        for length in page[27:header_size]:
            self.packet.extend(page[offset:offset + length])
            offset += length
            if len(self.packet) > MAX_PACKET_BYTES:
                raise OpusStreamError("Paquet ou en-tête Opus trop volumineux.")
            if length == 255:
                continue
            packet = bytes(self.packet)
            self.packet.clear()
            if self.packets == 0:
                if (len(packet) < 19 or packet[:8] != b"OpusHead" or not 1 <= packet[8] <= 15
                        or packet[9] not in (1, 2) or packet[18] != 0
                        or header_size != 28 or granule != 0):
                    raise OpusStreamError("En-tête Ogg Opus mono/stéréo invalide.")
                self.channels = packet[9]
                self.pre_skip = int.from_bytes(packet[10:12], "little")
            elif self.packets == 1:
                if len(packet) < 16 or not packet.startswith(b"OpusTags") or offset != len(page) or granule != 0:
                    raise OpusStreamError("En-tête de commentaires Opus invalide.")
            else:
                self.samples += _packet_samples(packet)
            self.packets += 1
        if self.packets == 0:
            raise OpusStreamError("En-tête Ogg Opus incomplet.")
        self.ended = bool(flags & 4)
        if self.samples > previous_samples:
            if (granule < self.granule or granule > self.samples
                    or (not self.ended and granule != self.samples)):
                raise OpusStreamError("Horloge du flux Ogg Opus incohérente.")
            self.granule = granule
            self.seconds = max(0, granule - self.pre_skip) / SAMPLE_RATE
            if self.seconds - previous_seconds > MAX_PAGE_SECONDS:
                raise OpusStreamError("Page Ogg trop longue pour une diffusion continue (5 s maximum).")
        elif granule not in (-1, 0):
            raise OpusStreamError("Horloge d’en-tête Ogg invalide.")
        if self.ended and (self.packet or self.samples == 0 or self.seconds <= 0):
            raise OpusStreamError("Fin du flux Ogg vide ou tronquée.")

    def finish(self) -> None:
        """Require actual audio and a complete end page before a successful data stop."""
        if not self.sequence:
            raise OpusStreamError("OpenAI a retourné un flux audio vide ou sans en-tête Ogg complet.")
        if self.buffer or self.packet or not self.ended:
            raise OpusStreamError("Réponse Ogg tronquée : fin du conteneur manquante.")
