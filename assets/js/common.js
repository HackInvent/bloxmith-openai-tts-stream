/** Validated, atomic next-Run settings, shared only by the TTS block's own surfaces. */

/** Bind a modal/inspector to the public facade without issuing any synthesis request.
 * @param {HTMLElement} root - This block-owned settings surface.
 * @param {object} api - Generic action facade; secret values never enter the browser.
 * @returns {Function} Detach the surface's listeners.
 */
export function mountSettings(root, api) {
    const fields = Array.from(root.querySelectorAll("[data-tts-setting]"));
    const title = root.querySelector("[data-tts-title]");
    const controls = title ? [title, ...fields] : fields;
    const apply = root.querySelector("[data-tts-apply]");
    const feedback = root.querySelector("[data-tts-feedback]");
    const snapshot = () => ({ ...(title ? { title: title.value } : {}),
      config: Object.fromEntries(fields.map(field => [field.dataset.ttsSetting, field.value])) });
    let saved = JSON.stringify(snapshot());
    let busy = false;
    let disposed = false;
    const changed = () => JSON.stringify(snapshot()) !== saved;
    const announce = (message, error = false) => {
      if (!disposed && feedback) { feedback.textContent = message; feedback.dataset.error = String(error); }
    };
    const refresh = () => {
      if (!disposed && apply) {
        apply.disabled = busy || !changed() || Boolean(api.isReadOnly?.());
        apply.textContent = busy ? "Application…" : "Appliquer";
      }
    };
    const dirty = () => { announce(changed() ? "Modifications non appliquées." : "Aucune modification."); refresh(); };
    /** Reveal invalid advanced fields before focusing them; preserve edits made during save. */
    const save = async () => {
      if (disposed || busy || !changed() || api.isReadOnly?.()) return;
      const invalid = controls.find(field => !field.checkValidity());
      if (invalid) {
        const details = invalid.closest("details");
        if (details) details.open = true;
        invalid.reportValidity(); announce("Vérifiez le champ signalé.", true); return;
      }
      const patch = snapshot();
      busy = true; refresh();
      try {
        const result = await api.applyAction("save_properties", patch);
        if (result?.error) throw new Error(result.error);
        saved = JSON.stringify(patch);
        announce(changed() ? "Enregistré ; des modifications restent à appliquer." : "Appliqué au prochain Run.");
      } catch (error) { announce(error.message || "Échec de l’enregistrement.", true); }
      finally { busy = false; refresh(); }
    };
    for (const field of controls) { field.addEventListener("input", dirty); field.addEventListener("change", dirty); }
    apply?.addEventListener("click", save);
    refresh();
    const observer = new MutationObserver(() => { if (!root.isConnected) cleanup(); });
    observer.observe(document.body, { childList: true, subtree: true });
    /** Handle both explicit unmount and generic shell replacement without retaining detached DOM. */
    function cleanup() {
      if (disposed) return;
      disposed = true;
      observer.disconnect();
      for (const field of controls) { field.removeEventListener("input", dirty); field.removeEventListener("change", dirty); }
      apply?.removeEventListener("click", save);
    }
    return cleanup;
}
