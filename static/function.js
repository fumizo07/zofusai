// static/function.js
(() => {
  "use strict";

  // ★ 読み替わり確認用（DevTools Console で見える）
  const FUNCTION_JS_VERSION = "2025-12-19.v2";
  window.__PS_FUNCTION_JS_VERSION = FUNCTION_JS_VERSION;
  console.log("[PersonalSearch] function.js loaded:", FUNCTION_JS_VERSION);

  // ============================================================
  // ユーティリティ
  // ============================================================
  function escapeHtml(s) {
    return (s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function clamp(v, min, max) {
    return Math.max(min, Math.min(max, v));
  }

  // 店舗名末尾の「数字」「丸数字①②③…」「絵文字」などを落とす
  function normalizeStoreTitle(raw) {
    let s = (raw || "").trim();
    if (!s) return s;

    s = s.replace(/\s*\d+\s*$/g, "");
    s = s.replace(/[\s\u2460-\u2473\u24EA\u3251-\u325F\u32B1-\u32BF]+$/gu, "");
    try {
      s = s.replace(/[\s\p{Extended_Pictographic}\uFE0F\u200D]+$/gu, "");
    } catch (_) {}
    return s.trim();
  }

  function openGoogleSearch(query) {
    const url = "https://www.google.com/search?q=" + encodeURIComponent(query);
    window.open(url, "_blank", "noopener");
  }

  // ============================================================
  // 店舗ページ検索 / 名前で店舗ページ検索
  // ============================================================
  function initStoreSearchHandlers() {
    document.addEventListener("click", (e) => {
      const target = e.target;

      const storeBox = target.closest ? target.closest(".store-search") : null;
      if (storeBox) {
        const clickable =
          target.closest(".store-search-link") ||
          target.closest("a") ||
          target.closest("button");
        if (!clickable) return;

        const storeRaw = (storeBox.dataset.storeTitle || "").trim();
        const store = normalizeStoreTitle(storeRaw);
        if (!store) return;

        let site = clickable.getAttribute("data-site");
        if (!site) {
          const label = (clickable.textContent || "").trim();
          if (label.includes("シティヘブン")) site = "city";
          else if (label.includes("デリヘルタウン")) site = "dto";
          else if (label.toLowerCase().includes("google")) site = "google";
        }
        if (!site) return;

        if (clickable.tagName === "A") e.preventDefault();

        let query = "";
        if (site === "city") query = "site:cityheaven.net " + store;
        else if (site === "dto") query = "site:dto.jp " + store;
        else query = store;

        openGoogleSearch(query);
        return;
      }

      const nameBox = target.closest ? target.closest(".name-store-search") : null;
      if (nameBox) {
        const btn = target.closest
          ? (target.closest(".name-search-btn") || target.closest("button"))
          : null;
        if (!btn) return;

        let site = btn.getAttribute("data-site");
        if (!site) {
          const label = (btn.textContent || "").trim();
          if (label.includes("シティヘブン")) site = "city";
          else if (label.includes("デリヘルタウン")) site = "dto";
          else if (label.toLowerCase().includes("google")) site = "google";
        }
        if (!site) return;

        const storeRaw = (nameBox.dataset.storeTitle || "").trim();
        const store = normalizeStoreTitle(storeRaw);
        if (!store) return;

        const input = nameBox.querySelector('input[name="name_keyword"]');
        const name = (input?.value || "").trim();
        if (!name) {
          alert("名前を入力してください。");
          return;
        }

        let query = "";
        if (site === "city") query = "site:cityheaven.net " + store + " " + name;
        else if (site === "dto") query = "site:dto.jp " + store + " " + name;
        else query = store + " " + name;

        openGoogleSearch(query);
      }
    });
  }

  // ============================================================
  // 「もっと読む」
  // ============================================================
  function initReadMore() {
    const maxLines = 3;
    const contextLines = document.querySelectorAll(".context-line");
    if (!contextLines || !contextLines.length) return;

    contextLines.forEach((line) => {
      if (line.dataset.readMoreApplied === "1") return;
      line.dataset.readMoreApplied = "1";

      const style = window.getComputedStyle(line);
      let lineHeight = parseFloat(style.lineHeight);
      if (Number.isNaN(lineHeight)) {
        const fontSize = parseFloat(style.fontSize) || 14;
        lineHeight = fontSize * 1.5;
      }

      const maxHeight = lineHeight * maxLines;
      if (line.scrollHeight > maxHeight + 2) {
        line.classList.add("context-collapsed");

        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "read-more-btn";
        btn.textContent = "もっと読む";

        btn.addEventListener("click", () => {
          if (line.classList.contains("context-collapsed")) {
            line.classList.remove("context-collapsed");
            btn.textContent = "閉じる";
          } else {
            line.classList.add("context-collapsed");
            btn.textContent = "もっと読む";
          }
        });

        line.insertAdjacentElement("afterend", btn);
      }
    });
  }

  // ============================================================
  // ハンバーガー
  // ============================================================
  function initHamburger() {
    const btn = document.getElementById("hamburgerButton");
    const overlay = document.getElementById("quickMenuOverlay");
    if (!btn || !overlay) return;

    function openMenu() {
      overlay.classList.add("open");
      btn.classList.add("open");
      document.body.classList.add("quick-menu-open");
    }

    function closeMenu() {
      overlay.classList.remove("open");
      btn.classList.remove("open");
      document.body.classList.remove("quick-menu-open");
    }

    btn.addEventListener("click", () => {
      if (overlay.classList.contains("open")) closeMenu();
      else openMenu();
    });

    overlay.addEventListener("click", (e) => {
      if (e.target === overlay) closeMenu();
    });

    const closeBtn = overlay.querySelector(".quick-menu-close");
    if (closeBtn) closeBtn.addEventListener("click", closeMenu);

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && overlay.classList.contains("open")) closeMenu();
    });
  }

  // ============================================================
  // ツールチッププレビュー
  // ============================================================
  const API_ENDPOINT = "/api/post_preview";
  const HOVER_OPEN_DELAY_MS = 80;
  const MAX_RANGE_EXPAND = 30;

  let zIndexSeed = 2000;
  let currentAnchorEl = null;
  let currentKey = "";
  let openTimer = null;
  let abortCtl = null;
  const cache = new Map(); // key -> { ok, posted_at, body } or { ok:false, message }

  function buildKey(threadUrl, postNo) {
    return `${threadUrl}||${postNo}`;
  }

  function parseBakusaiRridHref(href) {
    if (!href) return null;
    const m = href.match(/^(.*?\/)rrid=(\d+)\/?$/);
    if (!m) return null;

    const threadUrl = m[1];
    const postNo = parseInt(m[2], 10);
    if (!Number.isFinite(postNo) || postNo <= 0) return null;

    const openUrl = `${threadUrl}rrid=${postNo}/`;
    return { threadUrl, postNo, openUrl };
  }

  // 本文中の参照（>>n, >>n-m）を「プレビューできるリンク(#n表示)」に変換
  function linkifyAnchorsToPreviewLinks(text, threadUrl) {
    const safe = escapeHtml(text ?? "");
    const rangeRe = /(?:&gt;&gt;|＞＞)\s*(\d+)\s*-\s*(\d+)/g;
    const singleRe = /(?:&gt;&gt;|＞＞)\s*(\d+)/g;

    let out = safe;

    out = out.replace(rangeRe, (all, aRaw, bRaw) => {
      const a = parseInt(aRaw, 10);
      const b = parseInt(bRaw, 10);
      if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return all;

      const start = Math.min(a, b);
      const end = Math.max(a, b);
      const count = end - start + 1;
      if (count > MAX_RANGE_EXPAND) return all;

      const links = [];
      for (let n = start; n <= end; n++) {
        const openUrl = `${threadUrl}rrid=${n}/`;
        links.push(
          `<a href="${escapeHtml(openUrl)}" class="post-preview-link" data-thread-url="${escapeHtml(threadUrl)}" data-post-no="${n}">#${n}</a>`
        );
      }
      return links.join(" ");
    });

    out = out.replace(singleRe, (all, nRaw) => {
      const n = parseInt(nRaw, 10);
      if (!Number.isFinite(n) || n <= 0) return all;
      const openUrl = `${threadUrl}rrid=${n}/`;
      return `<a href="${escapeHtml(openUrl)}" class="post-preview-link" data-thread-url="${escapeHtml(threadUrl)}" data-post-no="${n}">#${n}</a>`;
    });

    return out.replace(/\r?\n/g, "<br>");
  }

  // Tooltip DOM（1つだけ）
  const tooltip = document.createElement("div");
  tooltip.className = "post-preview-tooltip";
  tooltip.setAttribute("aria-hidden", "true");
  tooltip.innerHTML = `
    <div class="post-preview-tooltip-inner" role="dialog" aria-live="polite">
      <div class="post-preview-tooltip-header">
        <div class="post-preview-tooltip-title" data-role="title"></div>
        <div class="post-preview-tooltip-actions">
          <a class="post-preview-tooltip-open" data-role="open" href="#" target="_blank" rel="noopener noreferrer">開く</a>
          <button type="button" class="post-preview-tooltip-close" data-role="close" aria-label="閉じる">×</button>
        </div>
      </div>
      <div class="post-preview-tooltip-body" data-role="body"></div>
      <div class="post-preview-tooltip-foot" data-role="foot"></div>
    </div>
  `;
  document.body.appendChild(tooltip);

  const elTitle = tooltip.querySelector('[data-role="title"]');
  const elOpen = tooltip.querySelector('[data-role="open"]');
  const elBody = tooltip.querySelector('[data-role="body"]');
  const elFoot = tooltip.querySelector('[data-role="foot"]');
  const elClose = tooltip.querySelector('[data-role="close"]');

  function isTooltipOpen() {
    return tooltip.classList.contains("open");
  }

  function bringToFront() {
    zIndexSeed += 1;
    tooltip.style.zIndex = String(zIndexSeed);
  }

  // ★ ヘッダーは必ず #n（ここで >> を絶対に使わない）
  function setTooltipContentLoading(threadUrl, postNo, openUrl) {
    elTitle.textContent = `#${postNo} ／ 読み込み中…`;
    elOpen.href = openUrl || "#";
    elBody.innerHTML = `<div class="post-preview-tooltip-loading">読み込み中…</div>`;
    elFoot.textContent = "";
    tooltip.dataset.threadUrl = threadUrl;
  }

  function setTooltipContentError(threadUrl, postNo, openUrl, message) {
    elTitle.textContent = `#${postNo} ／ 取得できませんでした`;
    elOpen.href = openUrl || "#";
    elBody.innerHTML = `<div class="post-preview-tooltip-error">${escapeHtml(message)}</div>`;
    elFoot.textContent = "";
    tooltip.dataset.threadUrl = threadUrl;
  }

  function setTooltipContentOk(threadUrl, postNo, openUrl, postedAt, bodyText) {
    const posted = postedAt ? `／ ${postedAt}` : "";
    elTitle.textContent = `#${postNo} ${posted}`; // ← ここも # 固定
    elOpen.href = openUrl || "#";
    elBody.innerHTML = linkifyAnchorsToPreviewLinks(bodyText ?? "", threadUrl);
    elFoot.textContent = "";
    tooltip.dataset.threadUrl = threadUrl;
  }

  function positionTooltipNear(anchorEl) {
    const rect = anchorEl.getBoundingClientRect();
    tooltip.style.left = "0px";
    tooltip.style.top = "0px";

    const tipRect = tooltip.getBoundingClientRect();
    const margin = 10;
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    let left = clamp(rect.left, margin, vw - tipRect.width - margin);

    const belowTop = rect.bottom + 8;
    const aboveTop = rect.top - tipRect.height - 8;
    let top = belowTop;
    if (belowTop + tipRect.height + margin > vh && aboveTop > margin) {
      top = aboveTop;
    } else {
      top = clamp(top, margin, vh - tipRect.height - margin);
    }

    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  }

  function openTooltip(anchorEl, threadUrl, postNo, openUrl) {
    bringToFront();

    currentAnchorEl = anchorEl;
    const key = buildKey(threadUrl, postNo);
    currentKey = key;

    tooltip.classList.add("open");
    tooltip.setAttribute("aria-hidden", "false");

    positionTooltipNear(anchorEl);
    setTooltipContentLoading(threadUrl, postNo, openUrl);

    if (cache.has(key)) {
      const cached = cache.get(key);
      if (cached && cached.ok) {
        setTooltipContentOk(threadUrl, postNo, openUrl, cached.posted_at, cached.body);
      } else {
        setTooltipContentError(threadUrl, postNo, openUrl, cached?.message || "not_found");
      }
      return;
    }

    if (abortCtl) {
      try { abortCtl.abort(); } catch (_) {}
    }
    abortCtl = new AbortController();

    const qs = new URLSearchParams({ thread_url: threadUrl, post_no: String(postNo) });
    fetch(`${API_ENDPOINT}?${qs.toString()}`, { signal: abortCtl.signal })
      .then(async (res) => {
        if (!res.ok) {
          let msg = `HTTP ${res.status}`;
          try {
            const j = await res.json();
            if (j && j.error) msg = j.error;
          } catch (_) {}
          throw new Error(msg);
        }
        return res.json();
      })
      .then((data) => {
        if (currentKey !== key) return;

        if (data && data.ok) {
          cache.set(key, { ok: true, posted_at: data.posted_at || "", body: data.body || "" });
          setTooltipContentOk(threadUrl, postNo, openUrl, data.posted_at || "", data.body || "");
        } else {
          const msg = (data && data.error) ? data.error : "unknown_error";
          cache.set(key, { ok: false, message: msg });
          setTooltipContentError(threadUrl, postNo, openUrl, msg);
        }
      })
      .catch((err) => {
        if (currentKey !== key) return;
        if (err && err.name === "AbortError") return;
        cache.set(key, { ok: false, message: err?.message || "fetch_error" });
        setTooltipContentError(threadUrl, postNo, openUrl, err?.message || "fetch_error");
      });
  }

  function closeTooltip() {
    if (openTimer) {
      clearTimeout(openTimer);
      openTimer = null;
    }
    currentAnchorEl = null;
    currentKey = "";
    tooltip.classList.remove("open");
    tooltip.setAttribute("aria-hidden", "true");
  }

  function findPreviewTargetFromElement(el) {
    if (!el) return null;

    // ツールチップ内ヘッダー操作は “絶対に” プレビュー対象外（「開く」「×」を邪魔しない）
    if (el.closest && el.closest(".post-preview-tooltip-actions")) return null;
    if (el.getAttribute && (el.getAttribute("data-role") === "open" || el.getAttribute("data-role") === "close")) {
      return null;
    }

    // ツールチップ内で生成したリンク（#n）
    const dt = el.getAttribute("data-thread-url");
    const pn = el.getAttribute("data-post-no");
    if (dt && pn && /^\d+$/.test(pn)) {
      const postNo = parseInt(pn, 10);
      const threadUrl = dt;
      const openUrl = `${threadUrl}rrid=${postNo}/`;
      return { threadUrl, postNo, openUrl };
    }

    // 既存 rrid= リンク
    const href = el.getAttribute("href") || "";
    const parsed = parseBakusaiRridHref(href);
    if (parsed) return parsed;

    return null;
  }

  function scheduleOpen(el, target) {
    if (openTimer) clearTimeout(openTimer);
    openTimer = setTimeout(() => {
      openTooltip(el, target.threadUrl, target.postNo, target.openUrl);
    }, HOVER_OPEN_DELAY_MS);
  }

  // hover で開く（閉じない：× or 外側クリックまで維持）
  document.addEventListener("mouseover", (e) => {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;
    if (!a) return;
    if (a.closest && a.closest(".post-preview-tooltip-actions")) return;

    const target = findPreviewTargetFromElement(a);
    if (!target) return;
    scheduleOpen(a, target);
  });

  // クリックで開く（閉じない）
  document.addEventListener("click", (e) => {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;

    // 外側クリックで閉じる
    if (!a) {
      if (isTooltipOpen() && !tooltip.contains(e.target)) {
        closeTooltip();
      }
      return;
    }

    // ツールチップ内の「開く」「×」は通常動作（邪魔しない）
    if (a.closest && a.closest(".post-preview-tooltip-actions")) return;

    const target = findPreviewTargetFromElement(a);
    if (!target) return;

    e.preventDefault();
    e.stopPropagation();

    const key = buildKey(target.threadUrl, target.postNo);
    if (isTooltipOpen() && currentKey === key) {
      bringToFront();
      return;
    }
    openTooltip(a, target.threadUrl, target.postNo, target.openUrl);
  }, { passive: false });

  // ツールチップ内クリックは外側判定に流さない
  tooltip.addEventListener("click", (e) => {
    e.stopPropagation();
  });

  // ×で閉じる（ここが効かない場合は CSS の pointer-events が犯人）
  elClose.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    closeTooltip();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && isTooltipOpen()) closeTooltip();
  });

  window.addEventListener("scroll", () => {
    if (isTooltipOpen() && currentAnchorEl) positionTooltipNear(currentAnchorEl);
  }, { passive: true });

  window.addEventListener("resize", () => {
    if (isTooltipOpen() && currentAnchorEl) positionTooltipNear(currentAnchorEl);
  });

  document.addEventListener("DOMContentLoaded", () => {
    initHamburger();
    initReadMore();
    initStoreSearchHandlers();
  });
})();
