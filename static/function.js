// 007
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
  function cupToRank(raw) {
    const s = String(raw ?? "").trim().toUpperCase();
    if (!s) return null;

    if (s.startsWith("AAA")) return -1;
    if (s.startsWith("AA")) return 0;

    const m = s.match(/[A-Z]/);
    if (!m) return null;

    const code = m[0].charCodeAt(0);
    const rank = code - 64; // A=1, B=2...
    if (!Number.isFinite(rank)) return null;
    return rank;
  }

  // 店舗名末尾の「数字」「丸数字①②③…」「絵文字」などを落とす
  function normalizeStoreTitle(raw) {
    let s = (raw || "").trim();
    if (!s) return s;

    s = s.replace(/\s*\d+\s*$/g, "");
    s = s.replace(/[\s\u2460-\u2473\u24EA\u3251-\u325F\u32B1-\u32BF]+$/gu, "");

    try {
      s = s.replace(/[\s\p{Extended_Pictographic}\uFE0F\u200D]+$/gu, "");
    } catch (e) {}

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
    // 期待：[{id, name, items:[{label,amount}]}]
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
    return data; // 期待: {ok:true, id:...}
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
    // よく使う順を選びやすいように、適用したらusageに寄せる（好みがあればここは消せます）
    if (prefs.sortMode === "usage") {
      saveTplPrefs(storeId, prefs);
    } else {
      // sortModeは変えない
      saveTplPrefs(storeId, prefs);
    }
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

    // usage（デフォ）
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

    // ----------------------------
    // 共通：管理モーダル（1個だけ）
    // ----------------------------
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
      modal.style.display = "none";

      modal.innerHTML = `
        <div style="max-width: 980px; margin: 5vh auto; background: #fff; border-radius: 12px; padding: 14px;">
          <div style="display:flex; justify-content: space-between; align-items:center; gap:10px;">
            <strong>料金明細テンプレ 管理（店舗ごと / DB保存）</strong>
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
            <span class="muted">（手動は↑↓で並び替え）</span>
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
                ※「よく使う順」「手動の並び」はこの端末のlocalStorageに保存されます（DBには影響しません）
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
                <button type="button" data-role="export" class="btn-secondary">エクスポート（JSON）</button>
                <button type="button" data-role="import" class="btn-secondary">インポート（JSON）</button>
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

      // radio change
      const radios = modal.querySelectorAll('input[name="kb_tpl_sortmode"]');
      radios.forEach((r) => {
        r.onchange = () => {
          setSortMode(r.value);
          renderList();
          broadcastTemplatesUpdated(storeId);
        };
      });

      // list click (delegate)
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

        // 手動並び替え（↑↓）：manualOrder を更新（sortModeをmanualに寄せる）
        if (act === "up" || act === "down") {
          const prefs = getPrefs();
          const allIds = sortedTemplates().map((t) => String(t.id));
          // manualOrder を「今の並び」をベースに作り直してから動かす
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

      // items events
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
          // 新規はエディタだけ初期化（保存で作成）
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

          // サーバーに update が無い前提で「安全リネーム」：
          // 1) 新規作成（同items, 新name）
          // 2) 成功後に旧を削除（失敗したら旧は残る）
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

          // 既存選択中なら「上書き相当」：安全に 1)新規作成→2)旧削除（任意）
          // ※update API ができたらここを真正のupdateに置換できます
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
              // 端末ローカルの並び/使用も入れておく（移行時に便利）
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

            // local_prefs も入っていれば取り込む（任意）
            if (parsed?.local_prefs && typeof parsed.local_prefs === "object") {
              const p = parsed.local_prefs;
              const prefs = loadTplPrefs(storeId);
              // 乱暴に上書きしないで、必要そうなものだけ
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

      // 初期
      syncSortRadios();
      renderList();
      loadToEditor(selectedId);
      modalEnsureOneItemRow(modal);
    }

    // ----------------------------
    // 各price box へバインド
    // ----------------------------
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

      // 入力変化で合計更新
      root.addEventListener("input", (e) => {
        const t = e.target;
        if (!t) return;
        if (t.matches("[data-price-label]") || t.matches("[data-price-amount]")) {
          collectAndSync();
        }
      });

      // 金額blurで整形
      root.addEventListener("blur", (e) => {
        const t = e.target;
        if (!t || !t.matches || !t.matches("[data-price-amount]")) return;

        const raw = (t.value || "").trim();
        if (!raw) return;

        const n = parseYen(raw);
        t.value = n ? formatYen(n) : "";
        collectAndSync();
      }, true);

      // ボタン類
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
          // デフォは「追加適用」：基本 → 指名 → 追加…が自然に積める
          // 置換したい時だけ Alt+クリック（または Option+クリック）
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

      // テンプレ更新イベントが来たらselectを更新
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

      // 初期化
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
  // 起動
  // ============================================================
  document.addEventListener("DOMContentLoaded", () => {
    initHamburger();
    initReadMore();
    initStoreSearchHandlers();

    // KB
    initKbPersonSearchSort();
    initKbStarRating();
    initKbPriceItems();   // ← テンプレ(並び/使用/移行)込み
    initKbDuration();
  });
})();
