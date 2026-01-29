// 001
// static/kb_sort_filter.js
(() => {
  "use strict";

  const CB_SELECTOR = 'input[type="checkbox"][id="kb_diary_tracked_only"][name="diary_tracked"]';
  const LIST_SELECTOR = "#kb_person_results";
  const CARD_SELECTOR = ".kb-person-result";
  const SLOT_SELECTOR = '.kb-diary-slot[data-kb-diary-slot="1"][data-person-id]';
  const LS_KEY = "kb_ui_diary_tracked_only_v1"; // 状態保存（任意）

  function $(sel, root = document) {
    return root.querySelector(sel);
  }

  function $all(sel, root = document) {
    return Array.from(root.querySelectorAll(sel));
  }

  function loadPref() {
    try { return localStorage.getItem(LS_KEY) === "1"; } catch (_) { return false; }
  }

  function savePref(v) {
    try { localStorage.setItem(LS_KEY, v ? "1" : "0"); } catch (_) {}
  }

  function applyFilter(onlyTracked) {
    const list = $(LIST_SELECTOR);
    if (!list) return;

    const cards = $all(CARD_SELECTOR, list);
    if (!cards.length) return;

    cards.forEach((card) => {
      const hasSlot = !!$(SLOT_SELECTOR, card);
      card.style.display = (!onlyTracked || hasSlot) ? "" : "none";
    });
  }

  function debounce(fn, ms) {
    let t = null;
    return (...args) => {
      if (t) clearTimeout(t);
      t = setTimeout(() => fn(...args), ms);
    };
  }

  document.addEventListener("DOMContentLoaded", () => {
    const cb = $(CB_SELECTOR);
    if (!cb) return;

    // 初期状態：localStorage優先（URLクエリで checked 済みならそのままでもOK）
    if (!cb.checked) cb.checked = loadPref();

    applyFilter(cb.checked);

    cb.addEventListener("change", () => {
      savePref(cb.checked);
      applyFilter(cb.checked);
    });

    // 検索結果が部分更新されたら再適用（あなたのサイトの挙動に強い）
    const reapply = debounce(() => applyFilter(cb.checked), 80);
    const mo = new MutationObserver(() => reapply());
    mo.observe(document.documentElement, { childList: true, subtree: true });
  });
})();
