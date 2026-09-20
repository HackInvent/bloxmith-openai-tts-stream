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
        raise OpusStreamError("Truncated Opus packet.")
    frames = (packet[1] & 63) if code == 3 else (1 if code == 0 else 2)
    if not 0 < frames * samples <= 5760:
        raise OpusStreamError("Invalid Opus packet duration.")
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
                raise OpusStreamError("Data after the end of the Ogg stream; chained streams are not supported.")
            if len(self.buffer) < 27:
                return
            if self.buffer[:4] != b"OggS" or self.buffer[4] != 0:
                raise OpusStreamError("OpenAI did not return the expected Ogg Opus container.")
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
            raise OpusStreamError("Missing or out-of-order Ogg pages, or a changed profile.")
        if page_crc(page) != int.from_bytes(page[22:26], "little"):
            raise OpusStreamError("Corrupted Ogg response: invalid checksum.")
        self.sequence += 1
        offset = header_size
        previous_samples, previous_seconds = self.samples, self.seconds
        for length in page[27:header_size]:
            self.packet.extend(page[offset:offset + length])
            offset += length
            if len(self.packet) > MAX_PACKET_BYTES:
                raise OpusStreamError("Opus packet or header too large.")
            if length == 255:
                continue
            packet = bytes(self.packet)
            self.packet.clear()
            if self.packets == 0:
                if (len(packet) < 19 or packet[:8] != b"OpusHead" or not 1 <= packet[8] <= 15
                        or packet[9] not in (1, 2) or packet[18] != 0
                        or header_size != 28 or granule != 0):
                    raise OpusStreamError("Invalid mono/stereo Ogg Opus header.")
                self.channels = packet[9]
                self.pre_skip = int.from_bytes(packet[10:12], "little")
            elif self.packets == 1:
                if len(packet) < 16 or not packet.startswith(b"OpusTags") or offset != len(page) or granule != 0:
                    raise OpusStreamError("Invalid Opus comment header.")
            else:
                self.samples += _packet_samples(packet)
            self.packets += 1
        if self.packets == 0:
            raise OpusStreamError("Incomplete Ogg Opus header.")
        self.ended = bool(flags & 4)
        if self.samples > previous_samples:
            if (granule < self.granule or granule > self.samples
                    or (not self.ended and granule != self.samples)):
                raise OpusStreamError("Inconsistent Ogg Opus stream clock.")
            self.granule = granule
            self.seconds = max(0, granule - self.pre_skip) / SAMPLE_RATE
            if self.seconds - previous_seconds > MAX_PAGE_SECONDS:
                raise OpusStreamError("Ogg page too long for continuous streaming (5 s maximum).")
        elif granule not in (-1, 0):
            raise OpusStreamError("Invalid Ogg header clock.")
        if self.ended and (self.packet or self.samples == 0 or self.seconds <= 0):
            raise OpusStreamError("Empty or truncated end of the Ogg stream.")

    def finish(self) -> None:
        """Require actual audio and a complete end page before a successful data stop."""
        if not self.sequence:
            raise OpusStreamError("OpenAI returned an empty audio stream, or one without a complete Ogg header.")
        if self.buffer or self.packet or not self.ended:
            raise OpusStreamError("Truncated Ogg response: the end of the container is missing.")
