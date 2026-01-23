// 004
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

  function parseTimeToMin(hhmm) {
    const s = String(hhmm || "").trim();
    if (!s) return null;
    const m = s.match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return null;
    const hh = parseInt(m[1], 10);
    const mm = parseInt(m[2], 10);
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
    if (hh < 0 || hh > 23) return null;
    if (mm < 0 || mm > 59) return null;
    return hh * 60 + mm;
  }

  // 金額：入力は「1,234円」「¥1,234」「１２３４」などを許容 → 0以上の整数にする
  function parseYen(v) {
    const s = String(v ?? "").trim();
    if (!s) return 0;
    // 数字とマイナス以外を捨てる（カンマ、円、空白など）
    const cleaned = s.replace(/[^\d-]/g, "");
    if (!cleaned) return 0;
    const n = parseInt(cleaned, 10);
    if (!Number.isFinite(n) || n < 0) return 0;
    return n;
  }

  // 金額：表示用（カンマ付け）
  function formatYen(n) {
    const x = Number(n || 0);
    if (!Number.isFinite(x)) return "0";
    return Math.trunc(x).toLocaleString("ja-JP");
  }

  // 数値（空や不正は null）
  function parseNumOrNull(v) {
    const s = String(v ?? "").trim();
    if (!s) return null;
    const n = Number(s);
    if (!Number.isFinite(n)) return null;
    return n;
  }

  // カップ文字 → ソート用順位（大きいほど上）
  // 例: AAA < AA < A < B < ... < K ...
  function cupToRank(raw) {
    const s = String(raw ?? "").trim().toUpperCase();
    if (!s) return null;

    if (s.startsWith("AAA")) return -1;
    if (s.startsWith("AA")) return 0;

    const m = s.match(/[A-Z]/);
    if (!m) return null;

    const code = m[0].charCodeAt(0); // 'A' = 65
    const rank = code - 64; // A=1, B=2...
    if (!Number.isFinite(rank)) return null;
    return rank;
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
  // KB：人物検索結果の並び替え（再読み込みなし）
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
        } else if (mode === "rating") {
          const c = compareNullableNumber(A.rating, B.rating, false); // 高い順
          if (c !== 0) return c;
        } else if (mode === "cup") {
          const c = compareNullableNumber(A.cupRank, B.cupRank, false); // 大きい順
          if (c !== 0) return c;
        } else if (mode === "height") {
          const c = compareNullableNumber(A.height, B.height, false); // 高い順
          if (c !== 0) return c;
        } else if (mode === "price") {
          const c = compareNullableNumber(A.price, B.price, true); // 安い順
          if (c !== 0) return c;
        } else if (mode === "age") {
          const c = compareNullableNumber(A.age, B.age, true); // 若い順
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

    sel.addEventListener("change", () => {
      applySort(sel.value || "name");
    });

    applySort(sel.value || "name");
  }

  // ============================================================
  // KB：星評価（☆☆☆☆☆ → クリックで ★★★☆☆）
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
        if (label) {
          label.textContent = val ? `（${val}/5）` : "";
        }
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
  // KB：料金項目（行追加＆合計＆hidden JSON）
  // ============================================================
  function initKbPriceItems() {
    const roots = document.querySelectorAll("[data-price-items]");
    if (!roots || !roots.length) return;

    roots.forEach((root) => {
      if (root.dataset.kbPriceApplied === "1") return;
      root.dataset.kbPriceApplied = "1";

      const body = root.querySelector("[data-price-items-body]");
      const elTotal = root.querySelector("[data-price-total]");
      const hidden = root.querySelector('input[type="hidden"][name="price_items_json"]');

      function collect() {
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

        if (elTotal) elTotal.textContent = formatYen(total);
        if (hidden) hidden.value = JSON.stringify(items);
      }

      function addRow() {
        if (!body) return;
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><input type="text" data-price-label placeholder="例：オプション"></td>
          <td><input type="text" data-price-amount placeholder="例：3,000"></td>
          <td><button type="button" data-price-remove>削除</button></td>
        `;
        body.appendChild(tr);
        collect();
      }

      root.addEventListener("input", (e) => {
        const t = e.target;
        if (!t) return;
        if (t.matches("[data-price-label]") || t.matches("[data-price-amount]")) {
          collect();
        }
      });

      root.addEventListener("blur", (e) => {
        const t = e.target;
        if (!t || !t.matches || !t.matches("[data-price-amount]")) return;

        const raw = (t.value || "").trim();
        if (!raw) return;

        const n = parseYen(raw);
        t.value = formatYen(n);
        collect();
      }, true);

      root.addEventListener("click", (e) => {
        const t = e.target;
        if (!t) return;

        if (t.matches("[data-price-remove]")) {
          e.preventDefault();
          const tr = t.closest("tr");
          if (tr && body) tr.remove();
          collect();
          return;
        }

        if (t.matches("[data-price-add]")) {
          e.preventDefault();
          addRow();
          return;
        }
      });

      const form = root.closest("form");
      if (form) {
        form.addEventListener("submit", () => {
          collect();
        });
      }

      collect();
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

      const hidden = form.querySelector('input[name="duration_min"]');
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
  // アンカー先ツールチッププレビュー（手動クローズ + スタック）
  // （あなたの既存実装：そのまま）
  // ============================================================
  const API_ENDPOINT = "/api/post_preview";
  const MAX_RANGE_EXPAND = 30;

  const cache = new Map();

  const tooltipStack = [];
  const BASE_Z_INDEX = 2000;

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

  function isOpenLink(el) {
    if (!el) return false;
    if (el.classList && el.classList.contains("post-preview-tooltip-open")) return true;
    const role = el.getAttribute ? el.getAttribute("data-role") : "";
    if (role === "open") return true;
    return false;
  }

  function findPreviewTargetFromElement(el) {
    if (!el) return null;

    if (isOpenLink(el)) return null;

    const dt = el.getAttribute("data-thread-url");
    const pn = el.getAttribute("data-post-no");
    if (dt && pn && /^\d+$/.test(pn)) {
      const postNo = parseInt(pn, 10);
      const threadUrl = dt;
      const openUrl = `${threadUrl}rrid=${postNo}/`;
      return { threadUrl, postNo, openUrl };
    }

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

  function bringTooltipToFront(t) {
    const idx = tooltipStack.indexOf(t);
    if (idx < 0) return;
    tooltipStack.splice(idx, 1);
    tooltipStack.push(t);
    renumberZIndex();
  }

  function closeSpecificTooltip(t) {
    if (t.abortCtl) {
      try { t.abortCtl.abort(); } catch (_) {}
      t.abortCtl = null;
    }

    const idx = tooltipStack.indexOf(t);
    if (idx >= 0) tooltipStack.splice(idx, 1);

    try { t.el.remove(); } catch (_) {}

    renumberZIndex();
  }

  function closeTopTooltip() {
    const top = tooltipStack.length ? tooltipStack[tooltipStack.length - 1] : null;
    if (top) closeSpecificTooltip(top);
  }

  function linkifyAnchorsToPreviewLinks(text, threadUrl) {
    const safe = escapeHtml(text ?? "");

    const rangeRe = /(?:&gt;&gt;|＞＞)\s*(\d+)\s*-\s*(\d+)/g;
    const singleRe = /(?:&gt;&gt;|＞＞)\s*(\d+)/g;

    let out = safe;

    out = out.replace(rangeRe, (whole, aRaw, bRaw) => {
      const a = parseInt(aRaw, 10);
      const b = parseInt(bRaw, 10);
      if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return whole;

      const start = Math.min(a, b);
      const end = Math.max(a, b);
      const count = end - start + 1;
      if (count > MAX_RANGE_EXPAND) return whole;

      const links = [];
      for (let n = start; n <= end; n++) {
        const openUrl = `${threadUrl}rrid=${n}/`;
        links.push(
          `<a href="${escapeHtml(openUrl)}" class="post-preview-link" data-thread-url="${escapeHtml(threadUrl)}" data-post-no="${n}">&gt;&gt;${n}</a>`
        );
      }
      return links.join(" ");
    });

    out = out.replace(singleRe, (whole, nRaw) => {
      const n = parseInt(nRaw, 10);
      if (!Number.isFinite(n) || n <= 0) return whole;
      const openUrl = `${threadUrl}rrid=${n}/`;
      return `<a href="${escapeHtml(openUrl)}" class="post-preview-link" data-thread-url="${escapeHtml(threadUrl)}" data-post-no="${n}">&gt;&gt;${n}</a>`;
    });

    return out.replace(/\r?\n/g, "<br>");
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

    el.addEventListener("mousedown", () => bringTooltipToFront(t));

    t.elClose.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      closeSpecificTooltip(t);
    });

    tooltipStack.push(t);
    renumberZIndex();
    bringTooltipToFront(t);

    return t;
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
    t.elFoot.textContent = "※ツールチップ内のアンカーもそのままプレビューできます。";
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

    if (cache.has(t.currentKey)) {
      const cached = cache.get(t.currentKey);
      if (cached && cached.ok) {
        setTooltipContentOk(t, target.threadUrl, target.postNo, target.openUrl, cached.posted_at, cached.body);
      } else {
        setTooltipContentError(t, target.threadUrl, target.postNo, target.openUrl, cached?.message || "not_found");
      }
      return;
    }

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

        const msg = err?.message || "fetch_error";
        cache.set(t.currentKey, { ok: false, message: msg });
        setTooltipContentError(t, target.threadUrl, target.postNo, target.openUrl, msg);
      });
  }

  function isClickInsideAnyTooltip(node) {
    if (!node || !node.closest) return false;
    return !!node.closest(".post-preview-tooltip");
  }

  document.addEventListener("click", (e) => {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;

    if (a && isOpenLink(a)) {
      return;
    }

    if (a) {
      const target = findPreviewTargetFromElement(a);
      if (target) {
        e.preventDefault();
        e.stopPropagation();
        if (e.stopImmediatePropagation) e.stopImmediatePropagation();
        openTooltip(a, target);
        return;
      }
    }

    if (isClickInsideAnyTooltip(e.target)) {
      return;
    }

    if (tooltipStack.length) {
      closeTopTooltip();
    }
  }, true);

  function stopHoverIfPreviewTarget(e) {
    const a = e.target && e.target.closest ? e.target.closest("a") : null;
    if (!a) return;
    const target = findPreviewTargetFromElement(a);
    if (!target) return;
    e.stopPropagation();
    if (e.stopImmediatePropagation) e.stopImmediatePropagation();
  }
  document.addEventListener("mouseover", stopHoverIfPreviewTarget, true);
  document.addEventListener("mouseout", stopHoverIfPreviewTarget, true);

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeTopTooltip();
  });

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
    initHamburger();
    initReadMore();
    initStoreSearchHandlers();

    // KB
    initKbPersonSearchSort();
    initKbStarRating();
    initKbPriceItems();
    initKbDuration();
  });
})();
