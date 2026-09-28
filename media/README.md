# TTS STREAM artwork

This image is a concept illustration of the `openai_tts_stream` BloxSmith block.
The implemented function is: Turns incoming text into a live Opus speech stream and separate command events.

- `cover.png`: selected original square PNG, copied byte for byte.
- `thumbnail.webp`: 320 × 320 WebP export for the README and catalog cards.
- Accent: amber orange.
- Geometry reference: the audio_mixer pilot cover. It is a visual reference only;
  its block-specific content was replaced.
- Creation: built-in image generation tool, 2026-09-28.
- License: [Apache-2.0](../LICENSE), like the rest of this block repository.

The image is not a Studio screenshot or a description of additional controls.
The artwork is documentation; its presence does not register it automatically
with Studio or the website. The block version and runtime contract are unchanged.

Export command (ImageMagick):

```sh
convert media/cover.png -thumbnail 320x320 -strip -quality 88 -define webp:method=6 media/thumbnail.webp
```

## Exact generation prompt

```text
Use case: stylized-concept.
Asset type: one square BloxSmith block cover illustration, to be read as a small catalog thumbnail.
Input image: the audio_mixer pilot cover is a STYLE AND GEOMETRY REFERENCE ONLY. Use its centered graphite and steel industrial cube, exact front/top/right three-quarter perspective, reinforced corners, neat vents and ports, plain near-white background, subtle grounded shadow, upper-left light, scale and generous margins. Replace every subject-specific icon, display and accent from the reference; create a new original cover for openai_tts_stream.
Primary request: visualize the real software block role accurately: Turns incoming text into a live Opus speech stream and separate command events.
Main roof symbol: one large text-line glyph transforming into sound-wave arcs.
Front panel: a prominent upper title plate, and directly below it one text card leads to a continuing audio waveform lane, with a narrow separate command-event lane. Keep the concept legible at 320 pixels. Only a few large functional details; no microtext.
Color palette: one dominant restrained amber orange accent, otherwise charcoal housing and brushed steel structural edges. The palette identifies a family but the symbols must distinguish the block without color.
Style/medium: polished, crisp industrial pixel-art product illustration with stepped edges and deliberate pixel clusters. Restrained glow, tactile surfaces, no photorealistic rendering.
Composition: one isolated complete near-cubical module on a 1:1 square canvas, about 84 percent of frame, no crop, no extra scene or props; match the pilot's geometry and consistent clean framing.
Text (verbatim): "TTS STREAM" exactly once in bold white uppercase pixel lettering on the front title plate. This is the ONLY writing in the picture. Spell it exactly; never add a subtitle, version, badge, copied text, fake small labels, watermark or slogan. The README carries the full technical name.
Accuracy constraints: speaker hardware playback, saved complete audio file, speech-to-text. The picture should communicate function through simple symbols and one primary flow. Do not copy the pilot's icon, title, faders, waveform, code screen or scanning page unless actually relevant. No cats, people, desks, mascots, official third-party logos, detached modules or floating objects.
```
