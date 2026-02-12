// 001
// static/kb_usage_time.js
(() => {
  "use strict";

  function clamp(v, min, max) {
    const n = Number(v);
    if (!Number.isFinite(n)) return min;
    return Math.min(max, Math.max(min, n));
  }

  function parseTimeToMin(hhmm) {
    const s = String(hhmm || "").trim();
    if (!s) return null;
    const m = s.match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return null;
    const hh = parseInt(m[1], 10);
    const mm = parseInt(m[2], 10);
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
    if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
    return hh * 60 + mm;
  }

  function initKbDuration() {
    const forms = document.querySelectorAll('form[data-kb-duration-form]');
    if (!forms || !forms.length) return;

    forms.forEach((form) => {
      const start = form.querySelector('input[name="start_time"]');
      const end = form.querySelector('input[name="end_time"]');
      if (!start || !end) return;

      if (form.dataset.kbDurationApplied === "1") return;
      form.dataset.kbDurationApplied = "1";

      const hidden = form.querySelector('input[type="hidden"][name="duration_min"]');
      const label = form.querySelector("[data-kb-duration-label]");

      function render() {
        const sMin = parseTimeToMin(start.value);
        const eMin = parseTimeToMin(end.value);

        let dur = null;
        if (sMin != null && eMin != null) {
          dur = eMin - sMin;
          if (dur < 0) dur = null;
          if (dur != null) dur = clamp(dur, 0, 24 * 60);
        }

        if (label) label.textContent = dur == null ? "" : `${dur}分`;
        if (hidden) hidden.value = dur == null ? "" : String(dur);
      }

      start.addEventListener("input", render);
      end.addEventListener("input", render);
      form.addEventListener("submit", render);
      render();
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    initKbDuration();
  });
})();
