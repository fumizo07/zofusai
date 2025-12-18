// static/preview_tooltip.js
(() => {
  "use strict";

  // ----------------------------
  // 設定
  // ----------------------------
  const API_ENDPOINT = "/api/post_preview";
  const CLOSE_DELAY_MS = 180;
  const HOVER_OPEN_DELAY_MS = 80;
  const MAX_RANGE_EXPAND = 30; // >>15-999 みたいなのを無限展開しないため

  // ----------------------------
  // ユーティリティ
  // ----------------------------
  function escapeHtml(s) {
    return (s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  // href から (thread_url, post_no, open_url) を推定
  // 例: https://bakusai.com/.../tid=12984894/rrid=15/
  function parseBakusaiRridHref(href) {
    if (!href) return null;
    const m = href.match(/^(.*?\/)rrid=(\d+)\/?$/);
    if (!m) return null;

    const threadUrl = m[1]; // rrid= を除いたベース
    const postNo = parseInt(m[2], 10);
    if (!Number.isFinite(postNo) || postNo <= 0) return null;

    const openUrl = `${threadUrl}rrid=${postNo}/`;
    return { threadUrl, postNo, openUrl };
  }

  // テキスト中のアンカー（>>15, ＞＞15, >>15-17, ＞＞15-17）をリンク化（#15 表示）
  function linkifyAnchorsToPreviewLinks(text, threadUrl) {
    const safe = escapeHtml(text ?? "");

    // range も単発もまとめて処理したいので、まず range を処理 → 次に単発
    // ただし range の方が広いので先に。
    const rangeRe = /(?:&gt;&gt;|＞＞)\s*(\d+)\s*-\s*(\d+)/g;
    const singleRe = /(?:&gt;&gt;|＞＞)\s*(\d+)/g;

    let out = safe;

    // range 展開（上限付き）
    out = out.replace(rangeRe, (_, aRaw, bRaw) => {
      const a = parseInt(aRaw, 10);
      const b = parseInt(bRaw, 10);
      if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return _;

      const start = Math.min(a, b);
      const end = Math.max(a, b);
      const count = end - start + 1;
      if (count > MAX_RANGE_EXPAND) {
        // 多すぎるなら展開せず文字として残す（表示は # にはしない：曖昧さ回避）
        return _;
      }

      const links = [];
      for (let n = start; n <= end; n++) {
        const openUrl = `${threadUrl}rrid=${n}/`;
        links.push(
          `<a href="${escapeHtml(openUrl)}" class="post-preview-link" data-thread-url="${escapeHtml(threadUrl)}" data-post-no="${n}">#${n}</a>`
        );
      }
      return links.join(" ");
    });

    // 単発（range 内で生成した #リンクは対象外なので、>> だけを置換）
    out = out.replace(singleRe, (_, nRaw) => {
      const n = parseInt(nRaw, 10);
      if (!Number.isFinite(n) || n <= 0) return _;
      const openUrl = `${threadUrl}rrid=${n}/`;
      return `<a href="${escapeHtml(openUrl)}" class="post-preview-link" data-thread-url="${escapeHtml(threadUrl)}" data-post-no="${n}">#${n}</a>`;
    });

    // 改行を <br> に
    return out.replace(/\r?\n/g, "<br>");
  }

  function clamp(v, min, max) {
    return Math.max(min, Math.min(max, v));
  }

  // ----------------------------
  // Tooltip DOM
  // ----------------------------
  const tooltip = document.createElement("div");
  tooltip.className = "post-preview-tooltip";
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

  // ----------------------------
  // 状態
  // ----------------------------
  let currentAnchorEl = null;
  let currentKey = "";
  let openTimer = null;
  let closeTimer = null;
  let abortCtl = null;

  const cache = new Map(); // key -> { ok, posted_at, body, ... } / or error obj

  function buildKey(threadUrl, postNo) {
    return `${threadUrl}||${postNo}`;
  }

  function isTooltipOpen() {
    return tooltip.classList.contains("open");
  }

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
    elTitle.textContent = `#${postNo} ${posted}`;
    elOpen.href = openUrl || "#";

    // 本文をリンク化（ツールチップ内アンカーもさらにツールチップ化）
    const html = linkifyAnchorsToPreviewLinks(bodyText ?? "", threadUrl);
    elBody.innerHTML = html;

    elFoot.textContent = "※ツールチップ内の #アンカーもそのままプレビューできます。";
    tooltip.dataset.threadUrl = threadUrl;
  }

  function positionTooltipNear(anchorEl) {
    const rect = anchorEl.getBoundingClientRect();

    // 一旦表示してサイズ取得（display: none だと取れないため）
    tooltip.style.left = "0px";
    tooltip.style.top = "0px";

    const tipRect = tooltip.getBoundingClientRect();

    const margin = 10;
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    // 右寄せ気味 / 画面外に出ないように
    let left = rect.left;
    left = clamp(left, margin, vw - tipRect.width - margin);

    // 下に出すか上に出すか（下が足りなければ上）
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
    // 直前のクローズ予約を殺す
    if (closeTimer) {
      clearTimeout(closeTimer);
      closeTimer = null;
    }

    currentAnchorEl = anchorEl;
    const key = buildKey(threadUrl, postNo);
    currentKey = key;

    tooltip.classList.add("open");
    tooltip.setAttribute("aria-hidden", "false");

    positionTooltipNear(anchorEl);
    setTooltipContentLoading(threadUrl, postNo, openUrl);

    // キャッシュがあれば即表示
    if (cache.has(key)) {
      const cached = cache.get(key);
      if (cached && cached.ok) {
        setTooltipContentOk(threadUrl, postNo, openUrl, cached.posted_at, cached.body);
      } else {
        setTooltipContentError(threadUrl, postNo, openUrl, cached?.message || "not_found");
      }
      return;
    }

    // 進行中 fetch を中止
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
        // 途中で別のアンカーに移動していたら捨てる
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

  function scheduleCloseTooltip() {
    if (closeTimer) clearTimeout(closeTimer);
    closeTimer = setTimeout(() => {
      // ツールチップ or 現在アンカーにホバー中なら閉じない
      const hoveringTooltip = tooltip.matches(":hover");
      const hoveringAnchor = currentAnchorEl && currentAnchorEl.matches(":hover");
      if (hoveringTooltip || hoveringAnchor) {
        scheduleCloseTooltip();
        return;
      }
      closeTooltip();
    }, CLOSE_DELAY_MS);
  }

  function closeTooltip() {
    if (openTimer) {
      clearTimeout(openTimer);
      openTimer = null;
    }
    if (closeTimer) {
      clearTimeout(closeTimer);
      closeTimer = null;
    }
    currentAnchorEl = null;
    currentKey = "";
    tooltip.classList.remove("open");
    tooltip.setAttribute("aria-hidden", "true");
  }

  // ----------------------------
  // 対象リンク判定
  // ----------------------------
  function normalizeAnchorDisplayText(el) {
    // 表示が >>15 なら #15 に統一（ページ本体も含めて）
    const t = (el.textContent || "").trim();
    const m = t.match(/^(?:>>|＞＞)\s*(\d+)$/);
    if (m) {
      el.textContent = `#${m[1]}`;
    }
  }

  function findPreviewTargetFromElement(el) {
    if (!el) return null;

    // ツールチップ内で生成したリンク（data-thread-url/data-post-no）
    const dt = el.getAttribute("data-thread-url");
    const pn = el.getAttribute("data-post-no");
    if (dt && pn && /^\d+$/.test(pn)) {
      const postNo = parseInt(pn, 10);
      const threadUrl = dt;
      const openUrl = `${threadUrl}rrid=${postNo}/`;
      return { threadUrl, postNo, openUrl };
    }

    // 既存の highlight_with_links が作った rrid= 付きリンクを拾う（href解析）
    const href = el.getAttribute("href") || "";
    const parsed = parseBakusaiRridHref(href);
    if (parsed) return parsed;

    return null;
  }

  // ----------------------------
  // イベント（デリゲーション）
  // ----------------------------
  function onAnchorEnter(el, target) {
    // hover で開く（誤爆防止に少しだけ遅延）
    if (openTimer) clearTimeout(openTimer);
    openTimer = setTimeout(() => {
      openTooltip(el, target.threadUrl, target.postNo, target.openUrl);
    }, HOVER_OPEN_DELAY_MS);
  }

  function onAnchorLeave() {
    if (openTimer) {
      clearTimeout(openTimer);
      openTimer = null;
    }
    scheduleCloseTooltip();
  }

  document.addEventListener("mouseover", (e) => {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;
    if (!a) return;

    normalizeAnchorDisplayText(a);

    const target = findPreviewTargetFromElement(a);
    if (!target) return;

    onAnchorEnter(a, target);
  });

  document.addEventListener("mouseout", (e) => {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;
    if (!a) return;

    const target = findPreviewTargetFromElement(a);
    if (!target) return;

    onAnchorLeave();
  });

  // タップ（モバイル）: クリックでトグル
  document.addEventListener("click", (e) => {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;
    if (!a) {
      // 外側クリックで閉じる（ツールチップ内クリックは除外）
      if (isTooltipOpen() && !tooltip.contains(e.target)) {
        closeTooltip();
      }
      return;
    }

    const target = findPreviewTargetFromElement(a);
    if (!target) return;

    // 通常遷移は止める（「開く」リンクがあるので）
    e.preventDefault();
    e.stopPropagation();

    normalizeAnchorDisplayText(a);

    // 同じ場所なら閉じる
    const key = buildKey(target.threadUrl, target.postNo);
    if (isTooltipOpen() && currentKey === key) {
      closeTooltip();
      return;
    }

    openTooltip(a, target.threadUrl, target.postNo, target.openUrl);
  }, { passive: false });

  // ツールチップ内に入っている間は消さない
  tooltip.addEventListener("mouseenter", () => {
    if (closeTimer) {
      clearTimeout(closeTimer);
      closeTimer = null;
    }
  });
  tooltip.addEventListener("mouseleave", () => {
    scheduleCloseTooltip();
  });

  // 閉じるボタン
  elClose.addEventListener("click", (e) => {
    e.preventDefault();
    closeTooltip();
  });

  // Esc で閉じる（PC）
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && isTooltipOpen()) {
      closeTooltip();
    }
  });

  // 初回：ページ内の >>n 表示を #n に揃える（既存リンクも）
  document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("a").forEach(normalizeAnchorDisplayText);
  });

  // スクロール/リサイズ時は位置を追随（表示中のみ）
  window.addEventListener("scroll", () => {
    if (isTooltipOpen() && currentAnchorEl) positionTooltipNear(currentAnchorEl);
  }, { passive: true });

  window.addEventListener("resize", () => {
    if (isTooltipOpen() && currentAnchorEl) positionTooltipNear(currentAnchorEl);
  });

})();
