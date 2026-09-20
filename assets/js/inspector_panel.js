/** Bind this release's TTS settings inspector; no synthesis request is ever issued here. */
import { mountSettings } from "./common.js";

/** Mount the surface through its injected public API and return UI-only cleanup. */
export function mount(root, api) {
  return mountSettings(root, api);
}
