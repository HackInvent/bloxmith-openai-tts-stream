import { withProperties } from "./properties.js";

/** Bind this release's TTS settings modal; no synthesis request is ever issued here. */
import { mountSettings } from "./common.js";

/** Mount the surface through its injected public API and return UI-only cleanup. */
function mountOwned(root, api) {
  return mountSettings(root, api);
}

/** Keep the block behavior and add properties-only accessibility. */
export function mount(root, ...args) {
  return withProperties(mountOwned).call(this, root, ...args);
}
