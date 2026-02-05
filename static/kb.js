// 008
// static/kb.js
(() => {
  "use strict";

  // ============================================================
  // Helpers
  // ============================================================
  function parseNumOrNull(v) {
    if (v == null) return null;
    const s = String(v).trim();
    if (!s) return null;
    const cleaned = s.replace(/[^\d.\-]/g, "");
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }

  function parseYen(v) {
    if (v == null) return 0;
    const s = String(v).trim();
    if (!s) return 0;
    const cleaned = s.replace(/[^\d\-]/g, "");
    const n = parseInt(cleaned || "0", 10);
    return Number.isFinite(n) ? n : 0;
  }

  function formatYen(n) {
    const x = Number(n || 0);
    if (!Number.isFinite(x)) return "0";
    return Math.trunc(x).toLocaleString("ja-JP");
  }

  function escapeHtml(str) {
    return String(str ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

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

  function cupToRank(cup) {
    const c = String(cup || "").trim().toUpperCase();
    if (!c) return null;
    const code = c.charCodeAt(0);
    if (code < 65 || code > 90) return null;
    return (code - 64);
  }

  function normalizeDiaryUrl(u) {
    const s = String(u || "").trim();
    if (!s) return "";
    const noSlash = s.replace(/\/+$/, "");
    if (noSlash.endsWith("/diary")) return noSlash;
    return noSlash + "/diary";
  }

  // ============================================================
  // KB：人物検索結果の並び替え
  // ============================================================
  function initKbPersonSearchSort() {
    const sel = document.getElementById("kb_person_sort");
    const list = document.getElementById("kb_person_results");
    if (!sel || !list) return;

    const getNameKey = (el) => {
      const ds = el?.dataset?.sortName;
      if (ds && String(ds).trim()) return String(ds).trim();
      const a = el.querySelector(".result-id a");
      return (a?.textContent || "").trim();
    };

    const getNumKey = (el, key) => {
      const v = el?.dataset ? el.dataset[key] : "";
      return parseNumOrNull(v);
    };

    function compareNullableNumber(a, b, asc) {
      const aNull = (a == null);
      const bNull = (b == null);
      if (aNull && bNull) return 0;
      if (aNull) return 1;
      if (bNull) return -1;
      return asc ? (a - b) : (b - a);
    }

    function applySort(mode) {
      const items = Array.from(list.querySelectorAll(".kb-person-result"));
      if (!items.length) return;

      const enriched = items.map((el, idx) => ({
        el,
        idx,
        name: getNameKey(el),
        rating: getNumKey(el, "sortRating"),
        cupRank: cupToRank(el?.dataset?.sortCup || ""),
        height: getNumKey(el, "sortHeight"),
        price: getNumKey(el, "sortPrice"),
        age: getNumKey(el, "sortAge"),
      }));

      enriched.sort((A, B) => {
        if (mode === "name") {
          const c = A.name.localeCompare(B.name, "ja", { numeric: true, sensitivity: "base" });
          if (c !== 0) return c;
        } else if (mode === "height") {
          const c = compareNullableNumber(A.height, B.height, true);
          if (c !== 0) return c;
        } else if (mode === "cup") {
          const c = compareNullableNumber(A.cupRank, B.cupRank, false);
          if (c !== 0) return c;
        } else if (mode === "age") {
          const c = compareNullableNumber(A.age, B.age, true);
          if (c !== 0) return c;
        } else if (mode === "price") {
          const c = compareNullableNumber(A.price, B.price, true);
          if (c !== 0) return c;
        } else if (mode === "rating") {
          const c = compareNullableNumber(A.rating, B.rating, false);
          if (c !== 0) return c;
        }

        const cn = A.name.localeCompare(B.name, "ja", { numeric: true, sensitivity: "base" });
        if (cn !== 0) return cn;
        return A.idx - B.idx;
      });

      const frag = document.createDocumentFragment();
      enriched.forEach((x) => frag.appendChild(x.el));
      list.appendChild(frag);
    }

    sel.addEventListener("change", () => applySort(sel.value || "name"));
    applySort(sel.value || "name");
  }

  // ============================================================
  // KB：星評価
  // ============================================================
  function initKbStarRating() {
    const boxes = document.querySelectorAll("[data-star-rating]");
    if (!boxes || !boxes.length) return;

    boxes.forEach((box) => {
      if (box.dataset.kbStarApplied === "1") return;
      box.dataset.kbStarApplied = "1";

      const stars = box.querySelectorAll(".kb-star");
      const input = box.querySelector('input[type="hidden"][name="rating"]');
      const label = box.querySelector("[data-star-label]");

      function render(val) {
        stars.forEach((btn) => {
          const n = parseInt(btn.getAttribute("data-value") || "0", 10);
          btn.textContent = (val && n <= val) ? "★" : "☆";
        });
        if (label) label.textContent = val ? `（${val}/5）` : "";
      }

      stars.forEach((btn) => {
        btn.addEventListener("click", () => {
          const v = parseInt(btn.getAttribute("data-value") || "0", 10);
          const val = (1 <= v && v <= 5) ? v : "";
          if (input) input.value = String(val || "");
          render(val);
        });
      });

      const initVal = input ? parseInt(input.value || "0", 10) : 0;
      render((1 <= initVal && initVal <= 5) ? initVal : 0);
    });
  }

  // ============================================================
  // KB：写メ日記 NEW バッジ
  // ============================================================
  const DIARY_LATEST_API = "/kb/api/diary_latest";
  const DIARY_SEEN_API = "/kb/api/diary_seen";
  const DIARY_REFRESH_INTERVAL_MS = 10 * 60 * 1000;

  // DOM Event bridge
  const EV_FORCE = "kb:diary:force";
  const EV_SIGNAL = "kb:diary:signal";

  function diarySeenKey(personId) {
    return `kb_diary_seen_${String(personId)}`;
  }

  function hideDiaryBadges(personId) {
    const nodes = document.querySelectorAll(
      `[data-kb-diary-new][data-person-id="${CSS.escape(String(personId))}"]`
    );
    nodes.forEach((n) => {
      try { n.remove(); } catch (_) { n.style.display = "none"; }
    });
  }

  function applyDiarySeenFromLocalStorage() {
    const badges = document.querySelectorAll("[data-kb-diary-new][data-person-id]");
    if (!badges.length) return;

    badges.forEach((a) => {
      const pid = a.getAttribute("data-person-id");
      if (!pid) return;

      const stored = (() => {
        try { return localStorage.getItem(diarySeenKey(pid)) || ""; } catch (_) { return ""; }
      })();

      const diaryKey = a.getAttribute("data-diary-key") || "";
      if (diaryKey && stored && stored === diaryKey) hideDiaryBadges(pid);
    });
  }

  async function markDiarySeen(personId, diaryKey) {
    if (diaryKey) {
      try { localStorage.setItem(diarySeenKey(personId), String(diaryKey)); } catch (_) {}
    }

    try {
      await fetch(DIARY_SEEN_API, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: Number(personId) }),
        keepalive: true,
      });
    } catch (_) {}
  }

  function createNewBadge(personId, diaryUrl, diaryKey) {
    const a = document.createElement("a");
    a.href = diaryUrl;
    a.className = "kb-diary-new";
    a.setAttribute("data-kb-diary-new", "1");
    a.setAttribute("data-person-id", String(personId));
    a.setAttribute("data-diary-key", String(diaryKey || ""));
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    a.textContent = "NEW";
    return a;
  }

  function parseTrackedFromSlot(slot) {
    const v = String(slot?.getAttribute?.("data-diary-track") || "1").trim();
    return v === "1";
  }

  function findDiaryRootForMeta(slot) {
    let el = slot;
    for (let i = 0; i < 20; i++) {
      if (!el) break;
      try {
        if (el.querySelector && el.querySelector('[data-kb-diary-meta="1"]')) return el;
      } catch (_) {}
      el = el.parentElement;
    }
    return document;
  }

  function setDiaryMetaUi(root, st) {
    if (!root) return;

    const meta = root.querySelector('[data-kb-diary-meta="1"]');
    if (!meta) return;

    const tracked = !!st?.tracked;
    if (!tracked) {
      meta.style.display = "none";
      return;
    }

    meta.style.display = "";

    const elChecked = root.querySelector("[data-kb-diary-checked]");
    const elLatest = root.querySelector("[data-kb-diary-latest]");

    const cm = st?.checked_ago_min;
    const ld = st?.latest_ago_days;

    if (elChecked) {
      elChecked.textContent = (cm != null && Number.isFinite(Number(cm)))
        ? `最終チェック:${Number(cm)}分前/`
        : "最終チェック:-/";
    }

    if (elLatest) {
      elLatest.textContent = (ld != null && Number.isFinite(Number(ld)))
        ? `最新日記:${Number(ld)}日前(取得)`
        : "最新日記:-";
    }
  }

  async function fetchDiaryLatestAndRender() {
    const slots = Array.from(document.querySelectorAll('[data-kb-diary-slot][data-person-id]'));
    if (!slots.length) return;

    const uniqIds = [];
    const seen = new Set();

    slots.forEach((s) => {
      const pid = parseInt(String(s.getAttribute("data-person-id") || "0"), 10);
      if (!Number.isFinite(pid) || pid <= 0) return;
      if (seen.has(pid)) return;
      seen.add(pid);
      uniqIds.push(pid);
    });

    if (!uniqIds.length) return;

    const qs = new URLSearchParams({ ids: uniqIds.join(",") });
    qs.set("_", String(Date.now()));

    const res = await fetch(`${DIARY_LATEST_API}?${qs.toString()}`, {
      method: "GET",
      credentials: "same-origin",
      headers: { "Accept": "application/json" },
      cache: "no-store",
    }).catch(() => null);

    if (!res || !res.ok) return;

    const json = await res.json().catch(() => null);
    const items = (json && json.ok && Array.isArray(json.items)) ? json.items : [];
    if (!items.length) return;

    const byId = new Map();
    items.forEach((it) => {
      const pid = Number(it?.id || 0);
      if (!Number.isFinite(pid) || pid <= 0) return;
      byId.set(pid, it);
    });

    slots.forEach((slot) => {
      const pid = parseInt(String(slot.getAttribute("data-person-id") || "0"), 10);
      if (!Number.isFinite(pid) || pid <= 0) return;

      const root = findDiaryRootForMeta(slot);
      const st = byId.get(pid);

      if (st) {
        const tracked = (typeof st.tracked === "boolean") ? st.tracked : parseTrackedFromSlot(slot);
        const checkedAgo = (st.checked_ago_min != null) ? Number(st.checked_ago_min) : null;
        const latestAgo = (st.latest_ago_days != null) ? Number(st.latest_ago_days) : null;
        setDiaryMetaUi(root, {
          tracked,
          checked_ago_min: (checkedAgo != null && Number.isFinite(checkedAgo)) ? checkedAgo : null,
          latest_ago_days: (latestAgo != null && Number.isFinite(latestAgo)) ? latestAgo : null,
        });
      } else {
        setDiaryMetaUi(root, { tracked: parseTrackedFromSlot(slot), checked_ago_min: null, latest_ago_days: null });
      }

      const existing = slot.querySelector("[data-kb-diary-new]");
      const isNew = !!st?.is_new;
      const latestTs = (st?.latest_ts != null) ? String(st.latest_ts) : "";
      const openUrl = String(st?.open_url || "").trim();
      const attrUrl = String(slot.getAttribute("data-diary-url") || "").trim();
      const url = normalizeDiaryUrl(openUrl || attrUrl);

      if (!isNew || !latestTs || !url) {
        if (existing) {
          try { existing.remove(); } catch (_) { existing.style.display = "none"; }
        }
        return;
      }

      if (!existing) {
        const badge = createNewBadge(pid, url, latestTs);
        slot.appendChild(badge);
      } else {
        try { existing.setAttribute("data-diary-key", latestTs); } catch (_) {}
      }
    });

    applyDiarySeenFromLocalStorage();
  }

  function initKbDiaryNewBadges() {
    applyDiarySeenFromLocalStorage();

    const slots0 = Array.from(document.querySelectorAll('[data-kb-diary-slot][data-person-id]'));
    slots0.forEach((slot) => {
      try {
        const root = findDiaryRootForMeta(slot);
        setDiaryMetaUi(root, {
          tracked: parseTrackedFromSlot(slot),
          checked_ago_min: null,
          latest_ago_days: null,
        });
      } catch (_) {}
    });

    fetchDiaryLatestAndRender();

    try {
      if (window.__kbDiaryHooksApplied !== "1") {
        window.__kbDiaryHooksApplied = "1";

        // Userscript push完了通知（互換）
        const onPushed = () => { fetchDiaryLatestAndRender(); };
        window.addEventListener("kb-diary-pushed", onPushed, { passive: true });
        document.addEventListener("kb-diary-pushed", onPushed, { passive: true });

        // 新名（任意）
        window.addEventListener("kb:diary:pushed", onPushed, { passive: true });
        document.addEventListener("kb:diary:pushed", onPushed, { passive: true });

        window.kbDiaryRefresh = () => {
          fetchDiaryLatestAndRender();
        };

        const list = document.getElementById("kb_person_results");
        if (list) {
          let t = null;
          const schedule = () => {
            if (t) clearTimeout(t);
            t = setTimeout(() => fetchDiaryLatestAndRender(), 450);
          };

          const mo = new MutationObserver((mutations) => {
            for (const m of mutations) {
              if (m.type !== "childList") continue;

              const nodes = [];
              if (m.addedNodes && m.addedNodes.length) nodes.push(...m.addedNodes);
              if (m.removedNodes && m.removedNodes.length) nodes.push(...m.removedNodes);

              for (const n of nodes) {
                if (!n || n.nodeType !== 1) continue;
                const el = n;
                if (
                  el.matches?.('[data-kb-diary-slot][data-person-id], .kb-person-result') ||
                  el.querySelector?.('[data-kb-diary-slot][data-person-id]')
                ) {
                  schedule();
                  return;
                }
              }
            }
          });

          mo.observe(list, { childList: true, subtree: true });
        }
      }
    } catch (_) {}

    try {
      if (window.__kbDiaryIntervalApplied !== "1") {
        window.__kbDiaryIntervalApplied = "1";
        setInterval(() => fetchDiaryLatestAndRender(), DIARY_REFRESH_INTERVAL_MS);
      }
    } catch (_) {}

    try {
      if (window.__kbDiaryClickApplied !== "1") {
        window.__kbDiaryClickApplied = "1";

        document.addEventListener("click", (e) => {
          const a = e.target?.closest?.("[data-kb-diary-new]");
          if (!a) return;

          const pid = a.getAttribute("data-person-id");
          if (!pid) return;

          const diaryKey = a.getAttribute("data-diary-key") || "";
          hideDiaryBadges(pid);
          markDiarySeen(pid, diaryKey);
        }, true);
      }
    } catch (_) {}
  }

  // ============================================================
  // Force button (DOM CustomEvent only)
  // ============================================================
  function initKbDiaryForceButton() {
    const btn = document.getElementById("kbDiaryBtnForce");
    if (!btn) return;

    if (btn.dataset.kbDiaryBound === "1") return;
    btn.dataset.kbDiaryBound = "1";

    const statusEl = document.getElementById("kbDiaryForceStatus");
    const setStatus = (t) => { if (statusEl) statusEl.textContent = t || ""; };

    // ridで相関させる（同時押し/遅延でも混ざらない）
    let activeRid = "";
    function newRid() {
      return `r${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    // Userscript signal
    try {
      if (window.__kbDiarySignalBound !== "1") {
        window.__kbDiarySignalBound = "1";

        const onSignal = (ev) => {
          const d = ev?.detail || {};
          const stage = String(d.stage || "");
          const rid = String(d.rid || "");

          if (!stage) return;
          if (activeRid && rid && rid !== activeRid) return; // 別リクエストの信号は無視

          if (stage === "force_received") setStatus("受信…");
          else if (stage === "force_debounced") setStatus("連打防止");
          else if (stage === "force_accept") setStatus("取得開始…");
          else if (stage === "gm_unavailable") setStatus("GM非対応ブラウザ");
          else if (stage === "run_blocked_running") setStatus("実行中");
          else if (stage === "run_blocked_epoch") setStatus("世代切替");
          else if (stage === "run_abort_no_slots") setStatus("slot=0");
          else if (stage === "run_start") setStatus("取得中…");
          else if (stage === "push_start") setStatus("push開始…");
          else if (stage === "push_fetch_start") setStatus("push送信中…");
          else if (stage === "push_fetch_done") {
            const ok = !!d.ok;
            const st = Number(d.status || 0);
            setStatus(ok ? `push完了(${st})` : `push失敗(${st})`);
          } else if (stage === "push_fetch_error") {
            setStatus("push例外");
          } else if (stage === "done") {
            const ok = !!d.ok;
            const st = Number(d.status || 0);
            setStatus(ok ? `完了(${st})` : "失敗");
          }
        };

        window.addEventListener(EV_SIGNAL, onSignal, { passive: true });
        document.addEventListener(EV_SIGNAL, onSignal, { passive: true });
      }
    } catch (_) {}

    let running = false;
    let lastRunAt = 0;
    const COOLDOWN_MS = 15 * 1000;
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

    async function forceFetchAndRefresh() {
      const now = Date.now();
      if (running) return;
      if (lastRunAt && (now - lastRunAt) < COOLDOWN_MS) return;

      running = true;
      lastRunAt = now;

      try {
        activeRid = newRid();
        setStatus("送信…");

        // DOMイベントで依頼（sandbox跨ぎ）
        try {
          const detail = {
            rid: activeRid,
            origin: "button",
            force: true,
            scope: "all",
            reason: "user_click",
          };
          const ev = new CustomEvent(EV_FORCE, { detail });
          document.dispatchEvent(ev);
          window.dispatchEvent(ev);
        } catch (_) {
          setStatus("Userscript未注入");
          return;
        }

        // push→反映→latest までの遅延を吸収して複数回更新
        const delays = [800, 1800, 3500, 6000];
        for (const d of delays) {
          await sleep(d);
          try { window.kbDiaryRefresh?.(); } catch (_) {}
        }
      } finally {
        running = false;
      }
    }

    btn.addEventListener("click", () => {
      forceFetchAndRefresh().catch(() => {});
    });
  }



  // ============================================================
  // ★追加：KB パニックボタン（チェックONで有効化）
  // ============================================================
  function initKbPanicCheck() {
    const chk = document.getElementById("kb_panic_check");
    const btn = document.getElementById("kb_panic_btn");
    if (!chk || !btn) return;

    if (chk.dataset.kbPanicApplied === "1") return;
    chk.dataset.kbPanicApplied = "1";

    const sync = () => { btn.disabled = !chk.checked; };
    chk.addEventListener("change", sync);
    sync();
  }

  // ============================================================
  // ★追加：KB バックアップ生成＆コピー＋インポート確認
  // ============================================================
  function initKbBackupUi() {
    const btnGen = document.getElementById("kb_backup_generate");
    const btnCopy = document.getElementById("kb_backup_copy");
    const ta = document.getElementById("kb_backup_text");
    const msg = document.getElementById("kb_backup_msg");

    const importChk = document.getElementById("kb_import_check");
    const importBtn = document.getElementById("kb_import_btn");

    if (importChk && importBtn) {
      if (importChk.dataset.kbImportApplied !== "1") {
        importChk.dataset.kbImportApplied = "1";
        const sync = () => { importBtn.disabled = !importChk.checked; };
        importChk.addEventListener("change", sync);
        sync();
      }
    }

    if (!btnGen || !btnCopy || !ta) return;

    if (btnGen.dataset.kbBackupApplied === "1") return;
    btnGen.dataset.kbBackupApplied = "1";

    const setMsg = (t) => { if (msg) msg.textContent = t || ""; };

    btnGen.addEventListener("click", async function () {
      setMsg("");
      btnGen.disabled = true;
      btnCopy.disabled = true;
      const orig = btnGen.textContent;
      btnGen.textContent = "生成中...";

      try {
        const res = await fetch("/kb/export", { headers: { "Accept": "application/json" } });
        if (!res.ok) throw new Error("export_failed");
        const data = await res.json();
        const text = JSON.stringify(data, null, 2);
        ta.value = text;
        ta.scrollTop = 0;
        btnCopy.disabled = !text;
        setMsg("生成しました。Joplinに貼り付けて保存してください。");
      } catch (e) {
        ta.value = "";
        btnCopy.disabled = true;
        setMsg("バックアップの取得に失敗しました。別タブで /kb/export を開いてコピーしてください。");
      } finally {
        btnGen.disabled = false;
        btnGen.textContent = orig;
      }
    });

    btnCopy.addEventListener("click", async function () {
      const text = ta.value || "";
      if (!text) return;

      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(text);
          setMsg("コピーしました。");
          return;
        }
      } catch (e) {}

      try {
        ta.focus();
        ta.select();
        const ok = document.execCommand("copy");
        setMsg(ok ? "コピーしました。" : "コピーに失敗しました。手動で選択してコピーしてください。");
      } catch (e) {
        setMsg("コピーに失敗しました。手動で選択してコピーしてください。");
      }
    });
  }

  // ============================================================
  // KB：料金項目（行追加＆合計＆hidden JSON）＋ テンプレ（DB + 端末ローカルの並び/使用回数）
  // ============================================================
  function getStoreIdFromPage() {
    const el = document.querySelector("[data-kb-store-id]");
    if (el) {
      const raw = el.getAttribute("data-kb-store-id");
      const v = parseInt(String(raw || ""), 10);
      if (Number.isFinite(v) && v > 0) return v;
    }
    try {
      const m = location.pathname.match(/\/kb\/store\/(\d+)/);
      if (m) {
        const v = parseInt(m[1], 10);
        if (Number.isFinite(v) && v > 0) return v;
      }
    } catch (e) {}
    return 0;
  }

  async function apiGetTemplates(storeId) {
    if (!storeId) return [];
    const url = "/kb/api/price_templates?store_id=" + encodeURIComponent(String(storeId));
    const res = await fetch(url, { method: "GET", credentials: "same-origin" });
    if (!res.ok) return [];
    const data = await res.json().catch(() => null);
    if (!data || !data.ok || !Array.isArray(data.items)) return [];
    return data.items;
  }

  async function apiSaveTemplate(storeId, name, items) {
    const payload = { store_id: storeId, name, items };
    const res = await fetch("/kb/api/price_templates/save", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const t = await res.text().catch(() => "");
      throw new Error(`save_failed: HTTP ${res.status} ${t}`);
    }
    const data = await res.json().catch(() => null);
    if (!data || !data.ok) throw new Error("save_failed");
    return data;
  }

  async function apiDeleteTemplate(id) {
    const payload = { id };
    const res = await fetch("/kb/api/price_templates/delete", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error("delete_failed");
    const data = await res.json().catch(() => null);
    if (!data || !data.ok) throw new Error("delete_failed");
    return data;
  }

  function tplPrefsKey(storeId) {
    return `kb_tpl_prefs_store_${String(storeId)}`;
  }

  function loadTplPrefs(storeId) {
    const def = { sortMode: "usage", manualOrder: [], useCounts: {} };
    try {
      const raw = localStorage.getItem(tplPrefsKey(storeId));
      if (!raw) return def;
      const p = JSON.parse(raw);
      if (!p || typeof p !== "object") return def;
      return {
        sortMode: (p.sortMode === "name" || p.sortMode === "manual" || p.sortMode === "usage") ? p.sortMode : "usage",
        manualOrder: Array.isArray(p.manualOrder) ? p.manualOrder.map((x) => String(x)) : [],
        useCounts: (p.useCounts && typeof p.useCounts === "object") ? p.useCounts : {},
      };
    } catch (e) {
      return def;
    }
  }

  function saveTplPrefs(storeId, prefs) {
    try {
      localStorage.setItem(tplPrefsKey(storeId), JSON.stringify(prefs || {}));
    } catch (e) {}
  }

  function getUseCount(prefs, id) {
    const k = String(id);
    const v = prefs?.useCounts?.[k];
    const n = parseInt(String(v ?? "0"), 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  function incUseCount(storeId, id) {
    const prefs = loadTplPrefs(storeId);
    const k = String(id);
    const cur = getUseCount(prefs, k);
    prefs.useCounts[k] = cur + 1;
    saveTplPrefs(storeId, prefs);
  }

  function sortTemplatesForUi(storeId, list) {
    const prefs = loadTplPrefs(storeId);
    const items = Array.isArray(list) ? list.slice() : [];

    const byName = (a, b) =>
      String(a?.name || "").localeCompare(String(b?.name || ""), "ja", { numeric: true, sensitivity: "base" });

    if (prefs.sortMode === "name") {
      items.sort(byName);
      return items;
    }

    if (prefs.sortMode === "manual") {
      const order = prefs.manualOrder || [];
      const pos = new Map(order.map((id, i) => [String(id), i]));
      items.sort((a, b) => {
        const pa = pos.has(String(a?.id)) ? pos.get(String(a?.id)) : 1e9;
        const pb = pos.has(String(b?.id)) ? pos.get(String(b?.id)) : 1e9;
        if (pa !== pb) return pa - pb;
        return byName(a, b);
      });
      return items;
    }

    items.sort((a, b) => {
      const ua = getUseCount(prefs, a?.id);
      const ub = getUseCount(prefs, b?.id);
      if (ua !== ub) return ub - ua;
      return byName(a, b);
    });
    return items;
  }

  function broadcastTemplatesUpdated(storeId) {
    const evt = new CustomEvent("kb-price-templates-updated", { detail: { storeId } });
    document.dispatchEvent(evt);
  }

  function ensureOneRow(body) {
    if (!body) return;
    const rows = body.querySelectorAll("tr");
    if (rows && rows.length) return;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="text" data-price-label placeholder="例：オプション"></td>
      <td><input type="text" data-price-amount placeholder="例：3,000"></td>
      <td><button type="button" data-price-remove>削除</button></td>
    `;
    body.appendChild(tr);
  }

  function addRowToBody(body, label = "", amount = "") {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="text" data-price-label value="${escapeHtml(String(label || ""))}" placeholder="例：オプション"></td>
      <td><input type="text" data-price-amount value="${escapeHtml(String(amount ?? ""))}" placeholder="例：3,000"></td>
      <td><button type="button" data-price-remove>削除</button></td>
    `;
    body.appendChild(tr);
  }

  function appendTemplateItemsToBody(body, items) {
    if (!body) return;
    const rows = Array.from(body.querySelectorAll("tr"));
    let rowIdx = 0;

    const findNextEmptyRow = () => {
      for (; rowIdx < rows.length; rowIdx++) {
        const tr = rows[rowIdx];
        const lab = (tr.querySelector("[data-price-label]")?.value || "").trim();
        const amt = (tr.querySelector("[data-price-amount]")?.value || "").trim();
        if (!lab && !amt) return tr;
      }
      return null;
    };

    (items || []).forEach((it) => {
      const label = String(it?.label ?? "").trim();
      const amount = it?.amount ?? 0;

      const empty = findNextEmptyRow();
      if (empty) {
        empty.querySelector("[data-price-label]").value = label;
        empty.querySelector("[data-price-amount]").value = amount ? formatYen(parseYen(amount)) : "";
      } else {
        addRowToBody(body, label, amount ? formatYen(parseYen(amount)) : "");
      }
    });

    ensureOneRow(body);
  }

  function writeRowsReplace(body, items) {
    if (!body) return;
    body.innerHTML = "";
    (items || []).forEach((it) => {
      addRowToBody(body, String(it?.label ?? ""), it?.amount ? formatYen(parseYen(it?.amount)) : "");
    });
    ensureOneRow(body);
  }

  function collectItemsFromBody(body) {
    const rows = body ? Array.from(body.querySelectorAll("tr")) : [];
    const items = [];
    let total = 0;

    rows.forEach((tr) => {
      const label = (tr.querySelector("[data-price-label]")?.value || "").trim();
      const amtRaw = tr.querySelector("[data-price-amount]")?.value || "";
      const amt = parseYen(amtRaw);

      if (!label && amt === 0) return;

      items.push({ label, amount: amt });
      total += amt;
    });

    return { items, total };
  }

  function initKbPriceItems() {
    const roots = document.querySelectorAll("[data-price-items]");
    if (!roots || !roots.length) return;

    const storeId = getStoreIdFromPage();

    function ensureTemplateModal() {
      let modal = document.getElementById("kbPriceTemplateModal");
      if (modal) return modal;

      modal = document.createElement("div");
      modal.id = "kbPriceTemplateModal";
      modal.style.position = "fixed";
      modal.style.left = "0";
      modal.style.top = "0";
      modal.style.right = "0";
      modal.style.bottom = "0";
      modal.style.background = "rgba(0,0,0,0.5)";
      modal.style.zIndex = "5000";
      modal.style.overflowY = "scroll";
      modal.style.display = "none";

      modal.innerHTML = `
        <div style="max-width: 980px; margin: 5vh auto; background: #fff; border-radius: 12px; padding: 14px;">
          <div style="display:flex; justify-content: space-between; align-items:center; gap:10px;">
            <strong>料金明細テンプレ 管理(店舗ごと/DB保存)</strong>
            <button type="button" data-role="close" class="btn-secondary">閉じる</button>
          </div>

          <div style="margin-top:10px; display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
            <div class="muted">並び：</div>
            <label style="display:flex; align-items:center; gap:6px;">
              <input type="radio" name="kb_tpl_sortmode" value="usage"> よく使う順
            </label>
            <label style="display:flex; align-items:center; gap:6px;">
              <input type="radio" name="kb_tpl_sortmode" value="name"> 名前順
            </label>
            <label style="display:flex; align-items:center; gap:6px;">
              <input type="radio" name="kb_tpl_sortmode" value="manual"> 手動
            </label>
            <span class="muted">(手動は↑↓で並び替え)</span>
          </div>

          <hr style="margin:12px 0;">

          <div style="display:flex; gap:12px; flex-wrap:wrap;">
            <div style="flex: 0 0 320px; min-width:280px; border:1px solid #eee; border-radius:10px; padding:10px;">
              <div class="muted" style="margin-bottom:6px;">テンプレ一覧</div>
              <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:8px;">
                <button type="button" data-role="new" class="btn-secondary">新規</button>
                <button type="button" data-role="duplicate" class="btn-secondary">複製</button>
                <button type="button" data-role="rename" class="btn-secondary">リネーム</button>
                <button type="button" data-role="delete" class="btn-secondary">削除</button>
              </div>

              <div style="max-height: 52vh; overflow:auto; border:1px solid #f0f0f0; border-radius:10px;">
                <ul data-role="list" style="list-style:none; padding:0; margin:0;"></ul>
              </div>

              <div class="muted" style="margin-top:8px;">
                ※「よく使う順」「手動の並び」はこの端末のlocalStorageに保存されます(DBには影響しません)
              </div>
            </div>

            <div style="flex:1; min-width:320px; border:1px solid #eee; border-radius:10px; padding:10px;">
              <div style="display:flex; gap:10px; flex-wrap:wrap; align-items:last baseline;">
                <div style="flex:1; min-width:220px;">
                  <div class="muted" style="margin-bottom:4px;">テンプレ名</div>
                  <input type="text" data-role="name" class="kb-input-sm" style="width:100%;" placeholder="例：基本＋延長">
                </div>
                <div style="display:flex; gap:8px; flex-wrap:wrap;">
                  <button type="button" data-role="save" class="btn-primary">保存</button>
                  <button type="button" data-role="save_as_new" class="btn-secondary">別名で保存</button>
                </div>
              </div>

              <div style="margin-top:10px;">
                <div class="muted" style="margin-bottom:6px;">明細（項目 / 金額）</div>

                <table class="kb-price-table" style="width:100%;">
                  <thead>
                    <tr>
                      <th style="width:60%;">項目</th>
                      <th style="width:30%;">金額</th>
                      <th style="width:10%;"></th>
                    </tr>
                  </thead>
                  <tbody data-role="items"></tbody>
                </table>

                <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; align-items:center;">
                  <button type="button" data-role="add_item" class="btn-secondary">行追加</button>
                  <span class="muted">合計：</span>
                  <strong data-role="sum">0</strong><span class="muted">円</span>
                </div>
              </div>

              <hr style="margin:12px 0;">

              <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <button type="button" data-role="export" class="btn-secondary">エクスポート(JSON)</button>
                <button type="button" data-role="import" class="btn-secondary">インポート(JSON)</button>
              </div>

              <div class="muted" style="margin-top:10px;">
                ※エクスポートはこの店舗のテンプレ一覧をJSON化します。インポートは「追加」か「置換（全削除→再作成）」を選べます。
              </div>
            </div>
          </div>
        </div>
      `;

      document.body.appendChild(modal);

      modal.addEventListener("click", (e) => {
        if (e.target === modal) modal.style.display = "none";
      });

      modal.querySelector('[data-role="close"]').addEventListener("click", () => {
        modal.style.display = "none";
      });

      return modal;
    }

    function liTemplateRow(tpl, selectedId, useCount) {
      const active = String(tpl?.id) === String(selectedId);
      const name = escapeHtml(String(tpl?.name || ""));
      const badge = useCount ? ` <span class="muted">（使用:${useCount}）</span>` : "";
      return `
        <li data-id="${escapeHtml(String(tpl?.id))}"
            style="display:flex; align-items:center; gap:8px; padding:8px 10px; border-bottom:1px solid #f2f2f2; ${active ? "background:#f7f7ff;" : ""}">
          <button type="button" data-act="pick" class="btn-secondary" style="padding:4px 8px;">選択</button>
          <div style="flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">
            <strong>${name}</strong>${badge}
          </div>
          <div style="display:flex; gap:6px;">
            <button type="button" data-act="up" class="btn-secondary" style="padding:4px 8px;">↑</button>
            <button type="button" data-act="down" class="btn-secondary" style="padding:4px 8px;">↓</button>
          </div>
        </li>
      `;
    }

    function modalItemsRow(label = "", amount = "") {
      return `
        <tr>
          <td><input type="text" data-role="item_label" value="${escapeHtml(String(label || ""))}" placeholder="例：指名"></td>
          <td><input type="text" data-role="item_amount" value="${escapeHtml(String(amount || ""))}" placeholder="例：3,000"></td>
          <td><button type="button" data-role="item_remove">削除</button></td>
        </tr>
      `;
    }

    function modalCollectItems(modal) {
      const body = modal.querySelector('[data-role="items"]');
      const rows = Array.from(body.querySelectorAll("tr"));
      const items = [];
      let sum = 0;
      rows.forEach((tr) => {
        const label = (tr.querySelector('[data-role="item_label"]')?.value || "").trim();
        const amtRaw = tr.querySelector('[data-role="item_amount"]')?.value || "";
        const amt = parseYen(amtRaw);
        if (!label && amt === 0) return;
        items.push({ label, amount: amt });
        sum += amt;
      });
      return { items, sum };
    }

    function modalRenderSum(modal) {
      const { sum } = modalCollectItems(modal);
      const el = modal.querySelector('[data-role="sum"]');
      if (el) el.textContent = formatYen(sum);
    }

    function modalEnsureOneItemRow(modal) {
      const body = modal.querySelector('[data-role="items"]');
      if (!body) return;
      if (body.querySelectorAll("tr").length) return;
      body.insertAdjacentHTML("beforeend", modalItemsRow("", ""));
      modalRenderSum(modal);
    }

    async function openTemplateModal(initialTemplateId) {
      if (!storeId) {
        alert("store_id が取得できませんでした。ページ側に data-kb-store-id が必要です。");
        return;
      }

      const modal = ensureTemplateModal();
      modal.style.display = "block";

      const ul = modal.querySelector('[data-role="list"]');
      const inName = modal.querySelector('[data-role="name"]');
      const itemsTbody = modal.querySelector('[data-role="items"]');

      let templatesRaw = await apiGetTemplates(storeId);
      let selectedId = initialTemplateId ? String(initialTemplateId) : (templatesRaw[0]?.id != null ? String(templatesRaw[0].id) : "");

      function getPrefs() {
        return loadTplPrefs(storeId);
      }

      function setSortMode(mode) {
        const prefs = getPrefs();
        prefs.sortMode = mode;
        saveTplPrefs(storeId, prefs);
      }

      function syncSortRadios() {
        const prefs = getPrefs();
        const radios = modal.querySelectorAll('input[name="kb_tpl_sortmode"]');
        radios.forEach((r) => {
          r.checked = (r.value === prefs.sortMode);
        });
      }

      function sortedTemplates() {
        return sortTemplatesForUi(storeId, templatesRaw);
      }

      function renderList() {
        const prefs = getPrefs();
        const list = sortedTemplates();
        ul.innerHTML = list.map((t) => liTemplateRow(t, selectedId, getUseCount(prefs, t.id))).join("");
      }

      function findById(id) {
        return templatesRaw.find((t) => String(t.id) === String(id)) || null;
      }

      function loadToEditor(id) {
        const t = findById(id) || templatesRaw[0] || null;
        if (!t) {
          selectedId = "";
          inName.value = "";
          itemsTbody.innerHTML = "";
          modalEnsureOneItemRow(modal);
          return;
        }
        selectedId = String(t.id);
        inName.value = String(t.name || "");

        itemsTbody.innerHTML = "";
        const items = Array.isArray(t.items) ? t.items : [];
        if (items.length === 0) {
          itemsTbody.insertAdjacentHTML("beforeend", modalItemsRow("", ""));
        } else {
          items.forEach((it) => {
            const label = String(it?.label ?? "");
            const amount = it?.amount ? formatYen(parseYen(it.amount)) : "";
            itemsTbody.insertAdjacentHTML("beforeend", modalItemsRow(label, amount));
          });
        }
        modalRenderSum(modal);
        renderList();
      }

      async function reloadAll(keepId) {
        templatesRaw = await apiGetTemplates(storeId);
        if (keepId && findById(keepId)) selectedId = String(keepId);
        else selectedId = templatesRaw[0]?.id != null ? String(templatesRaw[0].id) : "";
        syncSortRadios();
        renderList();
        loadToEditor(selectedId);
        broadcastTemplatesUpdated(storeId);
      }

      const radios = modal.querySelectorAll('input[name="kb_tpl_sortmode"]');
      radios.forEach((r) => {
        r.onchange = () => {
          setSortMode(r.value);
          renderList();
          broadcastTemplatesUpdated(storeId);
        };
      });

      ul.onclick = async (e) => {
        const btn = e.target?.closest?.("button");
        const li = e.target?.closest?.("li[data-id]");
        if (!btn || !li) return;

        const id = String(li.getAttribute("data-id") || "");
        const act = btn.getAttribute("data-act");

        if (act === "pick") {
          loadToEditor(id);
          return;
        }

        if (act === "up" || act === "down") {
          const prefs = getPrefs();
          const allIds = sortedTemplates().map((t) => String(t.id));
          let order = allIds.slice();
          const idx = order.indexOf(String(id));
          if (idx < 0) return;

          const to = act === "up" ? idx - 1 : idx + 1;
          if (to < 0 || to >= order.length) return;

          const tmp = order[idx];
          order[idx] = order[to];
          order[to] = tmp;

          prefs.manualOrder = order;
          prefs.sortMode = "manual";
          saveTplPrefs(storeId, prefs);

          syncSortRadios();
          renderList();
          broadcastTemplatesUpdated(storeId);
          return;
        }
      };

      modal.addEventListener("input", (e) => {
        const t = e.target;
        if (!t) return;
        if (t.matches('[data-role="item_label"]') || t.matches('[data-role="item_amount"]')) {
          modalRenderSum(modal);
        }
      });

      modal.addEventListener("blur", (e) => {
        const t = e.target;
        if (!t || !t.matches || !t.matches('[data-role="item_amount"]')) return;
        const raw = (t.value || "").trim();
        if (!raw) return;
        const n = parseYen(raw);
        t.value = n ? formatYen(n) : "";
        modalRenderSum(modal);
      }, true);

      modal.addEventListener("click", async (e) => {
        const t = e.target;
        if (!t || !t.getAttribute) return;

        if (t.getAttribute("data-role") === "add_item") {
          e.preventDefault();
          itemsTbody.insertAdjacentHTML("beforeend", modalItemsRow("", ""));
          modalRenderSum(modal);
          return;
        }

        if (t.getAttribute("data-role") === "item_remove") {
          e.preventDefault();
          const tr = t.closest("tr");
          if (tr) tr.remove();
          modalEnsureOneItemRow(modal);
          modalRenderSum(modal);
          return;
        }

        if (t.getAttribute("data-role") === "new") {
          e.preventDefault();
          selectedId = "";
          inName.value = "新規テンプレ";
          itemsTbody.innerHTML = "";
          itemsTbody.insertAdjacentHTML("beforeend", modalItemsRow("", ""));
          modalRenderSum(modal);
          renderList();
          return;
        }

        if (t.getAttribute("data-role") === "duplicate") {
          e.preventDefault();
          const cur = selectedId ? findById(selectedId) : null;
          if (!cur) return;
          const newName = prompt("複製名を入力してください。", String(cur.name || "") + "（複製）");
          if (newName == null) return;
          const name = String(newName).trim();
          if (!name) return;

          try {
            const items = Array.isArray(cur.items) ? cur.items : [];
            await apiSaveTemplate(storeId, name, items);
            await reloadAll();
            alert("複製しました。");
          } catch (err) {
            alert("複製に失敗しました。");
          }
          return;
        }

        if (t.getAttribute("data-role") === "rename") {
          e.preventDefault();
          const cur = selectedId ? findById(selectedId) : null;
          if (!cur) return;
          const ans = prompt("新しいテンプレ名を入力してください。", String(cur.name || ""));
          if (ans == null) return;
          const name = String(ans).trim();
          if (!name) return;

          try {
            const items = Array.isArray(cur.items) ? cur.items : [];
            const res = await apiSaveTemplate(storeId, name, items);
            const newId = res?.id != null ? String(res.id) : "";
            if (!newId) {
              alert("リネームに失敗しました（新ID取得不可）。旧テンプレは残っています。");
              await reloadAll(selectedId);
              return;
            }

            const ok = confirm("旧テンプレを削除してリネームを完了しますか？（キャンセルすると両方残ります）");
            if (ok) {
              await apiDeleteTemplate(cur.id);
              await reloadAll(newId);
              alert("リネームしました。");
            } else {
              await reloadAll(newId);
              alert("新しい名前のテンプレを作成しました（旧も残しています）。");
            }
          } catch (err) {
            alert("リネームに失敗しました。旧テンプレは残っています。");
          }
          return;
        }

        if (t.getAttribute("data-role") === "delete") {
          e.preventDefault();
          const cur = selectedId ? findById(selectedId) : null;
          if (!cur) return;
          if (!confirm(`「${String(cur.name || "")}」を削除しますか？`)) return;

          try {
            await apiDeleteTemplate(cur.id);
            await reloadAll();
            alert("削除しました。");
          } catch (err) {
            alert("削除に失敗しました。");
          }
          return;
        }

        if (t.getAttribute("data-role") === "save") {
          e.preventDefault();
          const name = String(inName.value || "").trim();
          if (!name) {
            alert("テンプレ名を入力してください。");
            return;
          }
          const { items } = modalCollectItems(modal);
          if (!items.length) {
            alert("明細が空です。");
            return;
          }

          const cur = selectedId ? findById(selectedId) : null;

          try {
            const res = await apiSaveTemplate(storeId, name, items);
            const newId = res?.id != null ? String(res.id) : "";

            if (cur && newId) {
              const ok = confirm("旧テンプレを削除して上書きを完了しますか？（キャンセルすると両方残ります）");
              if (ok) {
                await apiDeleteTemplate(cur.id);
                await reloadAll(newId);
                alert("保存しました（上書き完了）。");
              } else {
                await reloadAll(newId);
                alert("保存しました（旧も残しています）。");
              }
            } else {
              await reloadAll(newId || undefined);
              alert("保存しました。");
            }
          } catch (err) {
            alert("保存に失敗しました。");
          }
          return;
        }

        if (t.getAttribute("data-role") === "save_as_new") {
          e.preventDefault();
          const name = prompt("新しいテンプレ名を入力してください。", String(inName.value || "").trim());
          if (name == null) return;
          const nm = String(name).trim();
          if (!nm) return;

          const { items } = modalCollectItems(modal);
          if (!items.length) {
            alert("明細が空です。");
            return;
          }
          try {
            const res = await apiSaveTemplate(storeId, nm, items);
            const newId = res?.id != null ? String(res.id) : "";
            await reloadAll(newId || undefined);
            alert("別名で保存しました。");
          } catch (err) {
            alert("保存に失敗しました。");
          }
          return;
        }

        if (t.getAttribute("data-role") === "export") {
          e.preventDefault();
          try {
            const list = await apiGetTemplates(storeId);
            const prefs = loadTplPrefs(storeId);
            const payload = {
              version: 1,
              store_id: storeId,
              exported_at: new Date().toISOString(),
              templates: list.map((x) => ({
                name: String(x?.name || ""),
                items: Array.isArray(x?.items) ? x.items.map((it) => ({
                  label: String(it?.label ?? "").trim(),
                  amount: parseYen(it?.amount ?? 0),
                })) : [],
              })),
              local_prefs: prefs,
            };
            const json = JSON.stringify(payload, null, 2);

            if (navigator.clipboard?.writeText) {
              await navigator.clipboard.writeText(json);
              alert("JSONをクリップボードにコピーしました。");
            } else {
              prompt("コピーできない環境です。以下を手動でコピーしてください。", json);
            }
          } catch (err) {
            alert("エクスポートに失敗しました。");
          }
          return;
        }

        if (t.getAttribute("data-role") === "import") {
          e.preventDefault();
          const raw = prompt("JSONを貼り付けてください。");
          if (!raw) return;

          let parsed = null;
          try {
            parsed = JSON.parse(raw);
          } catch (e) {
            alert("JSONが不正です。");
            return;
          }

          const templates = parsed?.templates;
          if (!Array.isArray(templates)) {
            alert("形式が不正です（templates配列がありません）。");
            return;
          }

          const mode = confirm("置換しますか？\nOK: 既存テンプレを全削除→インポート\nキャンセル: 既存に追加");
          try {
            if (mode) {
              const cur = await apiGetTemplates(storeId);
              for (const t0 of cur) {
                try { await apiDeleteTemplate(t0.id); } catch (e) {}
              }
            }

            for (const t1 of templates) {
              const name = String(t1?.name || "").trim();
              const items = Array.isArray(t1?.items) ? t1.items.map((it) => ({
                label: String(it?.label ?? "").trim(),
                amount: parseYen(it?.amount ?? 0),
              })) : [];
              if (!name || !items.length) continue;
              await apiSaveTemplate(storeId, name, items);
            }

            if (parsed?.local_prefs && typeof parsed.local_prefs === "object") {
              const p = parsed.local_prefs;
              const prefs = loadTplPrefs(storeId);
              if (p.sortMode === "usage" || p.sortMode === "name" || p.sortMode === "manual") prefs.sortMode = p.sortMode;
              if (Array.isArray(p.manualOrder)) prefs.manualOrder = p.manualOrder.map((x) => String(x));
              if (p.useCounts && typeof p.useCounts === "object") prefs.useCounts = p.useCounts;
              saveTplPrefs(storeId, prefs);
            }

            await reloadAll();
            alert("インポートしました。");
          } catch (err) {
            alert("インポートに失敗しました。");
          }
          return;
        }
      });

      syncSortRadios();
      renderList();
      loadToEditor(selectedId);
      modalEnsureOneItemRow(modal);
    }

    async function fillTemplateSelect(root, keepValue) {
      const sel = root.querySelector("[data-price-template]");
      if (!sel) return;

      const current = keepValue ? String(sel.value || "") : "";
      const listRaw = await apiGetTemplates(storeId);
      const list = sortTemplatesForUi(storeId, listRaw);

      sel.innerHTML = "";
      const opt0 = document.createElement("option");
      opt0.value = "";
      opt0.textContent = "（選択）";
      sel.appendChild(opt0);

      list.forEach((t) => {
        const opt = document.createElement("option");
        opt.value = String(t.id);
        opt.textContent = String(t.name || "");
        sel.appendChild(opt);
      });

      if (current && Array.from(sel.options).some((o) => o.value === current)) {
        sel.value = current;
      } else {
        sel.value = "";
      }
    }

    async function getTemplateById(id) {
      const list = await apiGetTemplates(storeId);
      return list.find((t) => String(t.id) === String(id)) || null;
    }

    roots.forEach((root) => {
      if (root.dataset.kbPriceApplied === "1") return;
      root.dataset.kbPriceApplied = "1";

      const body = root.querySelector("[data-price-items-body]");
      const elTotal = root.querySelector("[data-price-total]");
      const hidden = root.querySelector('input[type="hidden"][name="price_items_json"]');

      function collectAndSync() {
        const { items, total } = collectItemsFromBody(body);
        if (elTotal) elTotal.textContent = formatYen(total);
        if (hidden) hidden.value = JSON.stringify(items);
      }

      function addRow() {
        if (!body) return;
        addRowToBody(body, "", "");
        collectAndSync();
      }

      async function applyTemplateAppend() {
        const sel = root.querySelector("[data-price-template]");
        const id = String(sel?.value || "");
        if (!id) return;

        const t = await getTemplateById(id);
        if (!t) return;

        appendTemplateItemsToBody(body, Array.isArray(t.items) ? t.items : []);
        collectAndSync();

        incUseCount(storeId, id);
        broadcastTemplatesUpdated(storeId);
      }

      async function applyTemplateReplace() {
        const sel = root.querySelector("[data-price-template]");
        const id = String(sel?.value || "");
        if (!id) return;

        const t = await getTemplateById(id);
        if (!t) return;

        writeRowsReplace(body, Array.isArray(t.items) ? t.items : []);
        collectAndSync();

        incUseCount(storeId, id);
        broadcastTemplatesUpdated(storeId);
      }

      function clearTemplate() {
        writeRowsReplace(body, []);
        collectAndSync();
        const sel = root.querySelector("[data-price-template]");
        if (sel) sel.value = "";
      }

      async function saveCurrentAsTemplate() {
        if (!storeId) {
          alert("store_id が取得できませんでした。ページ側に data-kb-store-id が必要です。");
          return;
        }

        const name = prompt("テンプレ名を入力してください（この店舗専用）");
        if (!name) return;

        const { items } = collectItemsFromBody(body);
        if (!items.length) {
          alert("明細が空です。テンプレに保存する内容を入れてください。");
          return;
        }

        try {
          await apiSaveTemplate(storeId, String(name).trim() || "テンプレ", items);
          await fillTemplateSelect(root, false);
          broadcastTemplatesUpdated(storeId);
          alert("テンプレとして保存しました。");
        } catch (e) {
          alert("保存に失敗しました。");
        }
      }

      root.addEventListener("input", (e) => {
        const t = e.target;
        if (!t) return;
        if (t.matches("[data-price-label]") || t.matches("[data-price-amount]")) {
          collectAndSync();
        }
      });

      root.addEventListener("blur", (e) => {
        const t = e.target;
        if (!t || !t.matches || !t.matches("[data-price-amount]")) return;

        const raw = (t.value || "").trim();
        if (!raw) return;

        const n = parseYen(raw);
        t.value = n ? formatYen(n) : "";
        collectAndSync();
      }, true);

      root.addEventListener("click", async (e) => {
        const t = e.target;
        if (!t) return;

        if (t.matches("[data-price-remove]")) {
          e.preventDefault();
          const tr = t.closest("tr");
          if (tr && body) tr.remove();
          ensureOneRow(body);
          collectAndSync();
          return;
        }

        if (t.matches("[data-price-add]")) {
          e.preventDefault();
          addRow();
          return;
        }

        if (t.matches("[data-price-template-apply]")) {
          e.preventDefault();
          if (e.altKey) await applyTemplateReplace();
          else await applyTemplateAppend();
          return;
        }

        if (t.matches("[data-price-template-clear]")) {
          e.preventDefault();
          clearTemplate();
          return;
        }

        if (t.matches("[data-price-template-manage]")) {
          e.preventDefault();
          const sel = root.querySelector("[data-price-template]");
          await openTemplateModal(String(sel?.value || ""));
          return;
        }

        if (t.matches("[data-price-template-save-current]")) {
          e.preventDefault();
          await saveCurrentAsTemplate();
          return;
        }
      });

      document.addEventListener("kb-price-templates-updated", async (evt) => {
        const sid = evt?.detail?.storeId;
        if (!sid || String(sid) !== String(storeId)) return;
        await fillTemplateSelect(root, true);
      });

      const form = root.closest("form");
      if (form) {
        form.addEventListener("submit", () => {
          collectAndSync();
        });
      }

      (async () => {
        ensureOneRow(body);
        collectAndSync();
        if (storeId) {
          await fillTemplateSelect(root, false);
        }
      })();
    });
  }

  // ============================================================
  // KB：利用時間（開始/終了 → ○○分）
  // ============================================================
  function initKbDuration() {
    const forms = document.querySelectorAll("form");
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

        if (label) {
          label.textContent = (dur == null) ? "" : `${dur}分`;
        }
        if (hidden) {
          hidden.value = (dur == null) ? "" : String(dur);
        }
      }

      start.addEventListener("input", render);
      end.addEventListener("input", render);
      form.addEventListener("submit", render);
      render();
    });
  }

  // ============================================================
  // 起動
  // ============================================================
  document.addEventListener("DOMContentLoaded", () => {
    initKbPersonSearchSort();
    initKbStarRating();
    initKbDiaryNewBadges();
    initKbDiaryForceButton();
    initKbPriceItems();
    initKbPanicCheck();
    initKbBackupUi();
    initKbDuration();
  });
})();
