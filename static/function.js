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
    document.addEventListener("click", (e) => {
      const target = e.target;

      // 1) スレタイ近くの「店舗ページ検索」
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

      // 2) 各レスの「名前で店舗ページ検索」
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
  // アンカー先ツールチッププレビュー
  // 要件:
  // ① ツールチップは × or 外側クリック/タップまで消えない
  //    さらにツールチップ内アンカーから開いた場合は“積み上げ”て前面へ
  // ② 「開く」ボタンはクリックを奪わず、そのまま新規タブで開ける
  // ============================================================
  const API_ENDPOINT = "/api/post_preview";
  const MAX_RANGE_EXPAND = 30;

  // 共有キャッシュ（全ツールチップで共通）
  const cache = new Map(); // key -> { ok, posted_at, body } or { ok:false, message }

  // ツールチップを積むスタック
  const tooltipStack = [];
  const BASE_Z_INDEX = 2000;

  function buildKey(threadUrl, postNo) {
    return `${threadUrl}||${postNo}`;
  }

  // href から (thread_url, post_no, open_url) を推定
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

  // 表示が >>15 なら #15 に統一
  function normalizeAnchorDisplayText(el) {
    const t = (el.textContent || "").trim();
    const m = t.match(/^(?:>>|＞＞)\s*(\d+)$/);
    if (m) el.textContent = `#${m[1]}`;
  }

  // 「開く」リンクはプレビュー対象にしない
  function isOpenLink(el) {
    if (!el) return false;
    if (el.classList && el.classList.contains("post-preview-tooltip-open")) return true;
    const role = el.getAttribute ? el.getAttribute("data-role") : "";
    if (role === "open") return true;
    return false;
  }

  function findPreviewTargetFromElement(el) {
    if (!el) return null;

    // 「開く」リンクは通常遷移させたいので除外
    if (isOpenLink(el)) return null;

    // ツールチップ内で生成したリンク（data-thread-url/data-post-no）
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

  function renumberZIndex() {
    tooltipStack.forEach((t, i) => {
      t.el.style.zIndex = String(BASE_Z_INDEX + i);
    });
  }

  function createTooltip() {
    const el = document.createElement("div");
    el.className = "post-preview-tooltip";
    el.setAttribute("aria-hidden", "true");
    el.innerHTML = `
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
    document.body.appendChild(el);

    const t = {
      el,
      elTitle: el.querySelector('[data-role="title"]'),
      elOpen: el.querySelector('[data-role="open"]'),
      elBody: el.querySelector('[data-role="body"]'),
      elFoot: el.querySelector('[data-role="foot"]'),
      elClose: el.querySelector('[data-role="close"]'),
      currentKey: "",
      currentAnchorEl: null,
      abortCtl: null,
    };

    // ×で閉じる（このツールチップだけ）
    t.elClose.addEventListener("click", (e) => {
      e.preventDefault();
      closeSpecificTooltip(t);
    });

    // クリックで前面へ（積み上げの“触ったら一番上”感）
    el.addEventListener("mousedown", () => {
      bringTooltipToFront(t);
    });

    // 初期Z
    tooltipStack.push(t);
    renumberZIndex();
    bringTooltipToFront(t);

    return t;
  }

  function bringTooltipToFront(t) {
    const idx = tooltipStack.indexOf(t);
    if (idx < 0) return;
    tooltipStack.splice(idx, 1);
    tooltipStack.push(t);
    renumberZIndex();
  }

  function closeSpecificTooltip(t) {
    // fetch中止
    if (t.abortCtl) {
      try { t.abortCtl.abort(); } catch (_) {}
      t.abortCtl = null;
    }

    const idx = tooltipStack.indexOf(t);
    if (idx >= 0) tooltipStack.splice(idx, 1);

    try {
      t.el.remove();
    } catch (_) {}

    renumberZIndex();
  }

  function closeTopTooltip() {
    const top = tooltipStack.length ? tooltipStack[tooltipStack.length - 1] : null;
    if (top) closeSpecificTooltip(top);
  }

  function positionTooltipNear(tipEl, anchorEl) {
    const rect = anchorEl.getBoundingClientRect();

    tipEl.style.left = "0px";
    tipEl.style.top = "0px";

    const tipRect = tipEl.getBoundingClientRect();
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

    tipEl.style.left = `${left}px`;
    tipEl.style.top = `${top}px`;
  }

  function setTooltipContentLoading(t, threadUrl, postNo, openUrl) {
    t.elTitle.textContent = `#${postNo} ／ 読み込み中…`;
    t.elOpen.href = openUrl || "#";
    t.elBody.innerHTML = `<div class="post-preview-tooltip-loading">読み込み中…</div>`;
    t.elFoot.textContent = "";
    t.el.dataset.threadUrl = threadUrl;
  }

  function setTooltipContentError(t, threadUrl, postNo, openUrl, message) {
    t.elTitle.textContent = `#${postNo} ／ 取得できませんでした`;
    t.elOpen.href = openUrl || "#";
    t.elBody.innerHTML = `<div class="post-preview-tooltip-error">${escapeHtml(message)}</div>`;
    t.elFoot.textContent = "";
    t.el.dataset.threadUrl = threadUrl;
  }

  function setTooltipContentOk(t, threadUrl, postNo, openUrl, postedAt, bodyText) {
    const posted = postedAt ? `／ ${postedAt}` : "";
    t.elTitle.textContent = `#${postNo} ${posted}`;
    t.elOpen.href = openUrl || "#";
    t.elBody.innerHTML = linkifyAnchorsToPreviewLinks(bodyText ?? "", threadUrl);
    t.elFoot.textContent = "※ツールチップ内の #アンカーもそのままプレビューできます。";
    t.el.dataset.threadUrl = threadUrl;
  }

  function openTooltip(anchorEl, target) {
    const t = createTooltip();
    bringTooltipToFront(t);

    t.currentAnchorEl = anchorEl;
    t.currentKey = buildKey(target.threadUrl, target.postNo);

    t.el.classList.add("open");
    t.el.setAttribute("aria-hidden", "false");

    positionTooltipNear(t.el, anchorEl);
    setTooltipContentLoading(t, target.threadUrl, target.postNo, target.openUrl);

    // キャッシュがあれば即表示
    if (cache.has(t.currentKey)) {
      const cached = cache.get(t.currentKey);
      if (cached && cached.ok) {
        setTooltipContentOk(t, target.threadUrl, target.postNo, target.openUrl, cached.posted_at, cached.body);
      } else {
        setTooltipContentError(t, target.threadUrl, target.postNo, target.openUrl, cached?.message || "not_found");
      }
      return;
    }

    // fetch開始（このツールチップ専用のAbortController）
    if (t.abortCtl) {
      try { t.abortCtl.abort(); } catch (_) {}
    }
    t.abortCtl = new AbortController();

    const qs = new URLSearchParams({
      thread_url: target.threadUrl,
      post_no: String(target.postNo),
    });

    fetch(`${API_ENDPOINT}?${qs.toString()}`, { signal: t.abortCtl.signal })
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
        // このツールチップが既に閉じられていたら何もしない
        if (!tooltipStack.includes(t)) return;

        if (data && data.ok) {
          cache.set(t.currentKey, { ok: true, posted_at: data.posted_at || "", body: data.body || "" });
          setTooltipContentOk(t, target.threadUrl, target.postNo, target.openUrl, data.posted_at || "", data.body || "");
        } else {
          const msg = (data && data.error) ? data.error : "unknown_error";
          cache.set(t.currentKey, { ok: false, message: msg });
          setTooltipContentError(t, target.threadUrl, target.postNo, target.openUrl, msg);
        }
      })
      .catch((err) => {
        if (!tooltipStack.includes(t)) return;
        if (err && err.name === "AbortError") return;
        cache.set(t.currentKey, { ok: false, message: err?.message || "fetch_error" });
        setTooltipContentError(t, target.threadUrl, target.postNo, target.openUrl, err?.message || "fetch_error");
      });
  }

  // ============================================================
  // イベント
  // - プレビュー対象リンクのクリックで開く（積み上げ）
  // - ツールチップは自動では閉じない
  // - 外側クリックで「一番上」だけ閉じる
  // - 「開く」リンクは奪わない（通常どおり新規タブへ）
  // ============================================================

  document.addEventListener("click", (e) => {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;

    // 1) プレビュー対象リンクならツールチップを“追加で”開く（既存は消さない）
    if (a) {
      // 「開く」リンクは通常挙動（ここで止めない）
      if (isOpenLink(a)) return;

      normalizeAnchorDisplayText(a);

      const target = findPreviewTargetFromElement(a);
      if (target) {
        e.preventDefault();
        e.stopPropagation();
        openTooltip(a, target);
        return;
      }
    }

    // 2) ツールチップの中をクリックした場合は何もしない（閉じない）
    if (e.target && e.target.closest && e.target.closest(".post-preview-tooltip")) {
      return;
    }

    // 3) それ以外の“外側クリック”で、トップだけ閉じる
    closeTopTooltip();
  }, { passive: false });

  // Esc でトップだけ閉じる
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeTopTooltip();
  });

  // スクロール/リサイズ時は「トップだけ」位置追随（最前面を操作してる想定）
  window.addEventListener("scroll", () => {
    const top = tooltipStack.length ? tooltipStack[tooltipStack.length - 1] : null;
    if (top && top.currentAnchorEl) positionTooltipNear(top.el, top.currentAnchorEl);
  }, { passive: true });

  window.addEventListener("resize", () => {
    const top = tooltipStack.length ? tooltipStack[tooltipStack.length - 1] : null;
    if (top && top.currentAnchorEl) positionTooltipNear(top.el, top.currentAnchorEl);
  });

  // ============================================================
  // 起動（ページごとに要素があるものだけ動く）
  // ============================================================
  document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("a").forEach(normalizeAnchorDisplayText);

    initHamburger();
    initReadMore();
    initStoreSearchHandlers();
  });
})();
