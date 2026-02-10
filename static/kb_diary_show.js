// 002
// static/kb_diary_show.js
(() => {
  "use strict";

  // ============================================================
  // KB：写メ日記 NEW バッジ + メタ表示 + Forceボタン
  // ============================================================

  const DIARY_LATEST_API = "/kb/api/diary_latest";
  const DIARY_SEEN_API = "/kb/api/diary_seen";

  // DOM Event bridge
  const EV_FORCE = "kb:diary:force";
  const EV_SIGNAL = "kb:diary:signal";
  const EV_PUSHED2 = "kb:diary:pushed"; // ★統一

  // push成功後：数秒後に「1回だけ」最新取得を走らせる
  const PUSH_REFRESH_DELAY_MS = 2800;

  // Force押下→Userscript受信のACK待ち
  const FORCE_ACK_TIMEOUT_MS = 1500;

  function normalizeDiaryUrl(u) {
    const s = String(u || "").trim();
    if (!s) return "";
    const noSlash = s.replace(/\/+$/, "");
    if (noSlash.endsWith("/diary")) return noSlash;
    return noSlash + "/diary";
  }

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
      // ★smartソート用：カード側に最新日記TSを反映（ms epoch）
      const card = slot.closest(".kb-person-result");
      if (card) card.dataset.diaryLatestTs = latestTs || "";
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
    // ★smartの見た目を即反映（kb.js側に再ソートを依頼）
    document.dispatchEvent(new CustomEvent("kb:personResults:rerunSort"));
  }

  // ============================================================
  // push後：数秒後に「一度だけ」refresh
  // ============================================================
  let pushTimer = null;
  function scheduleRefreshOnceAfterPush() {
    if (pushTimer != null) return;
    pushTimer = setTimeout(() => {
      pushTimer = null;
      fetchDiaryLatestAndRender().catch(() => {});
    }, PUSH_REFRESH_DELAY_MS);
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

    fetchDiaryLatestAndRender().catch(() => {});

    try {
      if (window.__kbDiaryHooksApplied !== "1") {
        window.__kbDiaryHooksApplied = "1";

        const onPushed = () => { scheduleRefreshOnceAfterPush(); };

        // ★統一：kb:diary:pushed のみ listen
        document.addEventListener(EV_PUSHED2, onPushed, { passive: true });
        window.addEventListener(EV_PUSHED2, onPushed, { passive: true });

        // 外から呼べる手動更新（forceボタンやデバッグ用）
        window.kbDiaryRefresh = () => {
          fetchDiaryLatestAndRender().catch(() => {});
        };

        // 一覧DOMが動く時の追随
        const list = document.getElementById("kb_person_results");
        if (list) {
          let t = null;
          const schedule = () => {
            if (t) clearTimeout(t);
            t = setTimeout(() => fetchDiaryLatestAndRender().catch(() => {}), 450);
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
      if (window.__kbDiaryClickApplied !== "1") {
        window.__kbDiaryClickApplied = "1";

        document.addEventListener("click", (e) => {
          const a = e.target?.closest?.("[data-kb-diary-new]");
          if (!a) return;

          const pid = a.getAttribute("data-person-id");
          if (!pid) return;

          const diaryKey = a.getAttribute("data-diary-key") || "";
          hideDiaryBadges(pid);
          markDiarySeen(pid, diaryKey).catch(() => {});
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

    let activeRid = "";
    let ackTimer = null;

    function newRid() {
      return `r${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    function clearAckTimer() {
      if (ackTimer) {
        clearTimeout(ackTimer);
        ackTimer = null;
      }
    }

    // Userscript signal listener
    try {
      if (window.__kbDiarySignalBound !== "1") {
        window.__kbDiarySignalBound = "1";

        const onSignal = (ev) => {
          const d = ev?.detail || {};
          const stage = String(d.stage || "");
          const rid = String(d.rid || "");

          if (!stage) return;
          if (activeRid && rid && rid !== activeRid) return;

          // 受信できた時点で「注入されてる」確度が上がる
          if (stage === "force_received") { clearAckTimer(); setStatus("受信…"); }
          else if (stage === "force_debounced") { clearAckTimer(); setStatus("連打防止"); }
          else if (stage === "force_accept") { clearAckTimer(); setStatus("取得開始…"); }
          else if (stage === "gm_unavailable") { clearAckTimer(); setStatus("GM非対応ブラウザ"); }
          else if (stage === "run_blocked_running") { clearAckTimer(); setStatus("実行中"); }
          else if (stage === "run_blocked_epoch") { clearAckTimer(); setStatus("世代切替"); }
          else if (stage === "run_abort_no_slots") { clearAckTimer(); setStatus("slot=0"); }
          else if (stage === "run_start") { clearAckTimer(); setStatus("取得中…"); }
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

        document.addEventListener(EV_SIGNAL, onSignal, { passive: true });
        window.addEventListener(EV_SIGNAL, onSignal, { passive: true });
      }
    } catch (_) {}

    let running = false;
    let lastRunAt = 0;
    const COOLDOWN_MS = 15 * 1000;

    async function forceFetch() {
      const now = Date.now();
      if (running) return;
      if (lastRunAt && (now - lastRunAt) < COOLDOWN_MS) return;

      running = true;
      lastRunAt = now;

      try {
        activeRid = newRid();
        setStatus("送信…");

        // ACK待ちタイムアウト：注入されてない/ガードでreturn等を可視化
        clearAckTimer();
        ackTimer = setTimeout(() => {
          ackTimer = null;
          setStatus("Userscript未注入/合言葉未許可の可能性");
        }, FORCE_ACK_TIMEOUT_MS);

        // ★documentへ送るのが本命
        const detail = {
          rid: activeRid,
          origin: "button",
          force: true,
          scope: "all",
          reason: "user_click",
        };
        document.dispatchEvent(new CustomEvent(EV_FORCE, { detail }));
      } catch (_) {
        clearAckTimer();
        setStatus("送信失敗");
      } finally {
        running = false;
      }
    }

    btn.addEventListener("click", () => {
      forceFetch();
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    initKbDiaryNewBadges();
    initKbDiaryForceButton();
  });
})();
