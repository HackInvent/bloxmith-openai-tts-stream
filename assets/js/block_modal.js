/** Register the autonomous TTS modal with the generic UI facade. */
(function () {
  "use strict";
  const registry = (window.CWBlockUiBlocks = window.CWBlockUiBlocks || {});
  registry.openai_tts_stream = {
    /** Bind settings and return surface-only cleanup. */
    mount(root, api) { return window.CWOpenAITtsStream.mount(root, api); },
  };
})();
