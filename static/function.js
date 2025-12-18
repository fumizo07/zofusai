// static/function.js
(() => {
  "use strict";

  // ============================================================
  // 共通ユーティリティ
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

    // 通常の数字末尾
    s = s.replace(/\s*\d+\s*$/g, "");

    // 丸数字などの数字記号末尾（①〜⑳、⓪、㉑〜㉟、㊱〜㊿ なども含めて広めに）
    s = s.replace(/[\s\u2460-\u2473\u24EA\u3251-\u325F\u32B1-\u32BF]+$/gu, "");

    // 絵文字末尾（対応ブラウザのみ）
    try {
      s = s.replace(/[\s\p{Extended_Pictographic}\uFE0F\u200D]+$/gu, "");
    } catch (e) {
      // 古い環境では無視
    }

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
    // クリック委譲で十分（各ページで要素があれば動く）
    document.addEventListener("click", (e) => {
      const target = e.target;

      // ----------------------------
      // 1) スレタイ近くの「店舗ページ検索」
      // ----------------------------
      const storeBox = target.closest ? target.closest(".store-search") : null;
      if (storeBox) {
        // クリック対象がリンク/ボタンで、それっぽいものだけ拾う
        const clickable =
          target.closest(".store-search-link") ||
          target.closest("a") ||
          target.closest("button");

        if (!clickable) return;

        // store_title は data-store-title が理想。無い場合は諦める（誤爆防止）
        const storeRaw = (storeBox.dataset.storeTitle || "").trim();
        const store = normalizeStoreTitle(storeRaw);
        if (!store) return;

        // data-site があればそれを優先
        let site = clickable.getAttribute("data-site");

        // data-site が無いテンプレにも耐える（リンク文言で推定）
        if (!site) {
          const label = (clickable.textContent || "").trim();
          if (label.includes("シティヘブン")) site = "city";
          else if (label.includes("デリヘルタウン")) site = "dto";
          else if (label.toLowerCase().includes("google")) site = "google";
        }

        if (!site) return;

        // a の場合は href 遷移を止める（JSで統一クエリを作る）
        if (clickable.tagName === "A") {
          e.preventDefault();
        }

        let query = "";
        if (site === "city") query = "site:cityheaven.net " + store;
        else if (site === "dto") query = "site:dto.jp " + store;
        else query = store;

        openGoogleSearch(query);
        return;
      }

      // ----------------------------
      // 2) 各レスの「名前で店舗ページ検索」
      // ----------------------------
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
  // 「もっと読む」折りたたみ（.context-line があるページだけ動く）
  // ============================================================
  function initReadMore() {
    const maxLines = 3;
    const contextLines = document.querySelectorAll(".context-line");
    if (!contextLines || !contextLines.length) return;

    contextLines.forEach(function (line) {
      // 二重付与防止（既にボタンが付いてる場合）
      if (line.dataset.readMoreApplied === "1") return;
      line.dataset.readMoreApplied = "1";

      const style = window.getComputedStyle(line);
      let lineHeight = parseFloat(style.lineHeight);

      if (Number.isNaN(lineHeight)) {
        const fontSize = parseFloat(style.fontSize) || 14;
        lineHeight = fontSize * 1.5;
      }

      const maxHeight = lineHeight * maxLines;

      // 3行より長い場合だけ対象
      if (line.scrollHeight > maxHeight + 2) {
        line.classList.add("context-collapsed");

        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "read-more-btn";
        btn.textContent = "もっと読む";

        btn.addEventListener("click", function () {
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
  // ハンバーガーメニュー開閉（要素があるページだけ動く）
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

    btn.addEventListener("click", function () {
      if (overlay.classList.contains("open")) closeMenu();
      else openMenu();
    });

    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) closeMenu();
    });

    const closeBtn = overlay.querySelector(".quick-menu-close");
    if (closeBtn) closeBtn.addEventListener("click", closeMenu);

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && overlay.classList.contains("open")) closeMenu();
    });
  }

  // ============================================================
  // アンカー先ツールチッププレビュー（あなたの既存実装を統合）
  // ============================================================
  const API_ENDPOINT = "/api/post_preview";
  const CLOSE_DELAY_MS = 180;
  const HOVER_OPEN_DELAY_MS = 80;
  const MAX_RANGE_EXPAND = 30;

  // href から (thread_url, post_no, open_url) を推定
  // 例: https://bakusai.com/.../tid=12984894/rrid=15/
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

  // テキスト中のアンカー（>>15, ＞＞15, >>15-17, ＞＞15-17）をリンク化（#15 表示）
  function linkifyAnchorsToPreviewLinks(text, threadUrl) {
    const safe = escapeHtml(text ?? "");

    const rangeRe = /(?:&gt;&gt;|＞＞)\s*(\d+)\s*-\s*(\d+)/g;
    const singleRe = /(?:&gt;&gt;|＞＞)\s*(\d+)/g;

    let out = safe;

    out = out.replace(rangeRe, (_, aRaw, bRaw) => {
      const a = parseInt(aRaw, 10);
      const b = parseInt(bRaw, 10);
      if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return _;

      const start = Math.min(a, b);
      const end = Math.max(a, b);
      const count = end - start + 1;
      if (count > MAX_RANGE_EXPAND) return _;

      const links = [];
      for (let n = start; n <= end; n++) {
        const openUrl = `${threadUrl}rrid=${n}/`;
        links.push(
          `<a href="${escapeHtml(openUrl)}" class="post-preview-link" data-thread-url="${escapeHtml(threadUrl)}" data-post-no="${n}">#${n}</a>`
        );
      }
      return links.join(" ");
    });

    out = out.replace(singleRe, (_, nRaw) => {
      const n = parseInt(nRaw, 10);
      if (!Number.isFinite(n) || n <= 0) return _;
      const openUrl = `${threadUrl}rrid=${n}/`;
      return `<a href="${escapeHtml(openUrl)}" class="post-preview-link" data-thread-url="${escapeHtml(threadUrl)}" data-post-no="${n}">#${n}</a>`;
    });

    return out.replace(/\r?\n/g, "<br>");
  }

  // Tooltip DOM（1回だけ作る）
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

  let currentAnchorEl = null;
  let currentKey = "";
  let openTimer = null;
  let closeTimer = null;
  let abortCtl = null;

  const cache = new Map(); // key -> { ok, posted_at, body } or { ok:false, message }

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

    // 本文のアンカーもリンク化（ツールチップ内も再ツールチップ可能）
    elBody.innerHTML = linkifyAnchorsToPreviewLinks(bodyText ?? "", threadUrl);

    elFoot.textContent = "※ツールチップ内の #アンカーもそのままプレビューできます。";
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

    let left = rect.left;
    left = clamp(left, margin, vw - tipRect.width - margin);

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

  function scheduleCloseTooltip() {
    if (closeTimer) clearTimeout(closeTimer);
    closeTimer = setTimeout(() => {
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

  // 表示が >>15 なら #15 に統一
  function normalizeAnchorDisplayText(el) {
    const t = (el.textContent || "").trim();
    const m = t.match(/^(?:>>|＞＞)\s*(\d+)$/);
    if (m) {
      el.textContent = `#${m[1]}`;
    }
  }

  function findPreviewTargetFromElement(el) {
    if (!el) return null;

    // ツールチップ内で生成したリンク
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

  function onAnchorEnter(el, target) {
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

  // モバイル：タップでトグル
  document.addEventListener("click", (e) => {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;

    if (!a) {
      if (isTooltipOpen() && !tooltip.contains(e.target)) {
        closeTooltip();
      }
      return;
    }

    const target = findPreviewTargetFromElement(a);
    if (!target) return; // プレビュー対象でなければ通常挙動に任せる

    e.preventDefault();
    e.stopPropagation();

    normalizeAnchorDisplayText(a);

    const key = buildKey(target.threadUrl, target.postNo);
    if (isTooltipOpen() && currentKey === key) {
      closeTooltip();
      return;
    }

    openTooltip(a, target.threadUrl, target.postNo, target.openUrl);
  }, { passive: false });

  // ツールチップ内にいる間は閉じない
  tooltip.addEventListener("mouseenter", () => {
    if (closeTimer) {
      clearTimeout(closeTimer);
      closeTimer = null;
    }
  });
  tooltip.addEventListener("mouseleave", () => {
    scheduleCloseTooltip();
  });

  elClose.addEventListener("click", (e) => {
    e.preventDefault();
    closeTooltip();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && isTooltipOpen()) {
      closeTooltip();
    }
  });

  window.addEventListener("scroll", () => {
    if (isTooltipOpen() && currentAnchorEl) positionTooltipNear(currentAnchorEl);
  }, { passive: true });

  window.addEventListener("resize", () => {
    if (isTooltipOpen() && currentAnchorEl) positionTooltipNear(currentAnchorEl);
  });

  // ============================================================
  // 起動（ページごとに要素があるものだけ動く）
  // ============================================================
  document.addEventListener("DOMContentLoaded", () => {
    // 既存リンクの >>n 表示を #n に揃える
    document.querySelectorAll("a").forEach(normalizeAnchorDisplayText);

    initHamburger();
    initReadMore();
    initStoreSearchHandlers();
  });
})();
