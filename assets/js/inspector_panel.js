/** Register the autonomous TTS inspector with the generic UI facade. */
(function () {
  "use strict";
  const registry = (window.CWBlockUiBlocks = window.CWBlockUiBlocks || {});
  registry.openai_tts_streamInspectorPanel = {
    /** Reuse only this block's settings binder. */
    mount(root, api) { return window.CWOpenAITtsStream.mount(root, api); },
  };
})();
