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
  // data-* 読み取りの“ゆるい互換”ヘルパ
  // - data-person-id / data-person_id / dataset.personId など混在しても拾う
  // ============================================================
  function getDataAny(el, keys) {
    if (!el) return "";
    for (const k of keys) {
      // 1) dataset（camel / snake を両方）
      try {
        if (el.dataset) {
          if (el.dataset[k] != null && String(el.dataset[k]).trim() !== "") return String(el.dataset[k]).trim();
        }
      } catch (_) {}

      // 2) data-camelCase -> data-camel-case
      const kebab = "data-" + String(k).replace(/([A-Z])/g, "-$1").toLowerCase();
      const v1 = el.getAttribute ? el.getAttribute(kebab) : null;
      if (v1 != null && String(v1).trim() !== "") return String(v1).trim();

      // 3) data-camelCase -> data_camel_case（underscore版）
      const snake = "data-" + String(k).replace(/([A-Z])/g, "_$1").toLowerCase();
      const v2 = el.getAttribute ? el.getAttribute(snake) : null;
      if (v2 != null && String(v2).trim() !== "") return String(v2).trim();
    }
    return "";
  }

  function ensureDataset(el, key, val) {
    if (!el || !key) return;
    if (val == null) return;
    const s = String(val).trim();
    if (!s) return;
    try {
      if (!el.dataset[key]) el.dataset[key] = s;
    } catch (_) {}
  }

  function normalizeName(s) {
    return (s || "").replace(/\s+/g, "").toLowerCase();
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

      // 初期値があれば反映
      const initVal = input ? parseInt(input.value || "0", 10) : 0;
      render((1 <= initVal && initVal <= 5) ? initVal : 0);
    });
  }

  // ============================================================
  // KB：料金項目（行追加＆合計＆hidden JSON）
  // - テンプレが無くても personページの訪問フォームに自動挿入する
  // ============================================================
  function initKbPriceItems() {
    // 1) “明細テンプレが無い”場合に自動生成（KB人物ページ想定）
    ensureKbPriceItemsAutoInsert();

    // 2) 通常の [data-price-items] を初期化
    const roots = document.querySelectorAll("[data-price-items]");
    if (!roots || !roots.length) return;

    roots.forEach((root) => applyKbPriceItemsRoot(root));
  }

  function ensureKbPriceItemsAutoInsert() {
    // KB人物ページだけを対象にする（誤爆防止）
    const path = String(location.pathname || "");
    const isKbPerson = /^\/kb\/person\/\d+/.test(path);
    if (!isKbPerson) return;

    const forms = Array.from(document.querySelectorAll("form"));
    if (!forms.length) return;

    forms.forEach((form) => {
      if (!form || form.dataset.kbPriceAutoInserted === "1") return;

      // 訪問ログっぽいフォーム判定
      const hasVisited = !!form.querySelector('input[name="visited_at"]');
      const hasStart = !!form.querySelector('input[name="start_time"]');
      const hasEnd = !!form.querySelector('input[name="end_time"]');
      const hasMemo = !!form.querySelector('textarea[name="memo"], input[name="memo"]');
      if (!(hasVisited && hasStart && hasEnd && hasMemo)) return;

      // 既にUIがあれば何もしない
      if (form.querySelector("[data-price-items]")) return;

      // action でさらに絞る（無くてもOK）
      const action = String(form.getAttribute("action") || "").toLowerCase();
      const looksVisit =
        action.includes("/kb/person/") && action.endsWith("/visit");
      const looksVisitUpdate =
        action.includes("/kb/visit/") && action.endsWith("/update");
      // actionが空なら“構造で判定”のみでOK
      if (action && !(looksVisit || looksVisitUpdate)) return;

      // 明細UIを作る
      const wrap = document.createElement("div");
      wrap.className = "kb-price-items";
      wrap.setAttribute("data-price-items", "");
      wrap.innerHTML = `
        <div class="kb-price-items-head">明細</div>
        <table class="kb-price-items-table">
          <thead>
            <tr>
              <th>項目</th>
              <th>金額</th>
              <th></th>
            </tr>
          </thead>
          <tbody data-price-items-body></tbody>
        </table>
        <div class="kb-price-items-foot">
          <button type="button" data-price-add>行追加</button>
          <span class="muted">合計：<span data-price-total>0</span> 円</span>
        </div>
      `;

      // hidden input を確実に用意（既存があれば移動）
      let hidden = form.querySelector('input[type="hidden"][name="price_items_json"]');
      if (!hidden) {
        hidden = document.createElement("input");
        hidden.type = "hidden";
        hidden.name = "price_items_json";
        hidden.value = "";
      } else {
        // 既存をwrap内で拾えるようにするため、後でwrapに移動する
        try { hidden.parentElement && hidden.parentElement.removeChild(hidden); } catch (_) {}
      }
      wrap.appendChild(hidden);

      // memo欄の少し手前に入れる（自然な位置）
      const memoEl = form.querySelector('textarea[name="memo"], input[name="memo"]');
      if (memoEl && memoEl.insertAdjacentElement) {
        memoEl.insertAdjacentElement("beforebegin", wrap);
      } else {
        form.appendChild(wrap);
      }

      form.dataset.kbPriceAutoInserted = "1";

      // 追加したwrapは直ちに初期化
      applyKbPriceItemsRoot(wrap);
    });
  }

  function applyKbPriceItemsRoot(root) {
    if (!root) return;
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

        items.push({ label, amount: amt }); // 数値だけ保存（カンマなし）
        total += amt;
      });

      if (elTotal) elTotal.textContent = formatYen(total); // 表示だけカンマ
      if (hidden) hidden.value = JSON.stringify(items);
    }

    function addRow(presetLabel = "", presetAmount = "") {
      if (!body) return;
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><input type="text" data-price-label placeholder="例：オプション"></td>
        <td><input type="text" data-price-amount placeholder="例：3,000"></td>
        <td><button type="button" data-price-remove>削除</button></td>
      `;
      const inLabel = tr.querySelector("[data-price-label]");
      const inAmt = tr.querySelector("[data-price-amount]");
      if (inLabel) inLabel.value = String(presetLabel || "");
      if (inAmt) inAmt.value = (presetAmount === 0 || presetAmount) ? String(presetAmount) : "";
      body.appendChild(tr);
      collect();
    }

    function hydrateFromHidden() {
      if (!hidden) return;
      const raw = String(hidden.value || "").trim();
      if (!raw) {
        collect();
        return;
      }
      try {
        const data = JSON.parse(raw);
        if (!Array.isArray(data)) {
          collect();
          return;
        }
        // 既存行を消してから復元
        try {
          if (body) body.innerHTML = "";
        } catch (_) {}
        data.forEach((it) => {
          if (!it || typeof it !== "object") return;
          const label = String(it.label || "");
          const amt = (it.amount == null) ? "" : String(it.amount);
          addRow(label, amt);
        });
        collect();
      } catch (_) {
        collect();
      }
    }

    // 入力変化で再計算（合計の表示更新）
    root.addEventListener("input", (e) => {
      const t = e.target;
      if (!t) return;
      if (t.matches("[data-price-label]") || t.matches("[data-price-amount]")) {
        collect();
      }
    });

    // 金額欄は「フォーカス外れたら」見た目だけカンマ整形（内部は常に数値でJSON化）
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

    // 送信直前に確実にJSON化
    const form = root.closest("form");
    if (form) {
      form.addEventListener("submit", () => {
        collect();
      });
    }

    // 初回：hiddenが既に入っていれば復元
    hydrateFromHidden();
  }

  // ============================================================
  // KB：利用時間（開始/終了 → ○○分）
  // ============================================================
  function initKbDuration() {
    // KBページ以外でも安全に動くように「要素があれば」方式
    const forms = document.querySelectorAll("form");
    if (!forms || !forms.length) return;

    forms.forEach((form) => {
      // 同一フォームに start/end が無ければ無視
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

          // ★日跨ぎ対応（バックエンド _calc_duration と合わせる）
          if (dur < 0) dur += 24 * 60;

          dur = clamp(dur, 0, 24 * 60);
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

      // submit前にも確実に反映
      form.addEventListener("submit", render);

      // 初期表示
      render();
    });
  }

  // ============================================================
  // KB：パニックボタン（チェックで有効化）
  // ============================================================
  function initKbPanicButton() {
    const chk = document.getElementById("kb_panic_check");
    const btn = document.getElementById("kb_panic_btn");
    if (!chk || !btn) return;

    const sync = () => { btn.disabled = !chk.checked; };
    chk.addEventListener("change", sync);
    sync();
  }

  // ============================================================
  // KB：バックアップ/インポート UI（テンプレに無くてもJSで挿入）
  // ============================================================
  function initKbBackupImportUi() {
    const path = String(location.pathname || "");
    const isKbIndexLike = (path === "/kb" || path === "/kb/search");
    if (!isKbIndexLike) return;

    // 既にあるなら何もしない
    if (document.querySelector("[data-kb-backup-ui]")) return;

    const main = document.querySelector("main");
    if (!main) return;

    const card = document.createElement("section");
    card.className = "card";
    card.setAttribute("data-kb-backup-ui", "1");
    card.innerHTML = `
      <div class="section-title">バックアップ / インポート</div>
      <div class="kb-backup-actions" style="display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
        <a class="btn" href="/kb/export" target="_blank" rel="noopener">バックアップ（JSON）</a>
        <span class="muted">※別タブにJSONが出ます（保存してください）</span>
      </div>

      <div style="margin-top:14px;">
        <form method="post" action="/kb/import">
          <input type="hidden" name="mode" value="replace">
          <div class="muted" style="margin-bottom:6px;">インポート（全置換）</div>
          <textarea name="payload_json" rows="8" style="width:100%; box-sizing:border-box;" placeholder="ここに /kb/export のJSONを貼り付け"></textarea>
          <div style="margin-top:8px; display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
            <label><input type="checkbox" name="confirm_check" value="1"> すべて置き換える（確認）</label>
            <button type="submit" class="btn">インポート実行</button>
          </div>
          <div class="muted" style="margin-top:6px;">※確認チェックなしだとエラーになります</div>
        </form>
      </div>
    `;

    // mainの末尾に追加
    main.appendChild(card);
  }

  // ============================================================
  // KB：重複検知 / 並び替え / 星フィルタ / 写メ日記NEWチェック（本物）
  // - テンプレ不足があっても “可能な限り自己修復” する
  // ============================================================
  function initKbIndexEnhancements() {
    // containerが無くても items から推定して動かす
    let container = document.getElementById("kb_person_results");

    // items候補（KB一覧の行）
    let items = Array.from(document.querySelectorAll(".kb-person-result"));
    if (!items.length) {
      // fallback: data-person-id を持つ行を拾う
      items = Array.from(document.querySelectorAll("[data-person-id],[data-person_id]"))
        .filter((el) => {
          const pid = getDataAny(el, ["personId", "person_id"]);
          const name = getDataAny(el, ["name"]);
          return !!pid && !!name;
        });
      // それっぽいならクラス付け（他の処理が動くように）
      items.forEach((el) => {
        try {
          if (!el.classList.contains("kb-person-result")) el.classList.add("kb-person-result");
        } catch (_) {}
      });
    }

    if (!items.length) return;

    if (!container) {
      // 最初の行の親を仮コンテナにする（並び替え/クリック委譲用）
      container = items[0].parentElement || null;
    }
    if (!container) return;

    const sortKeyEl = document.getElementById("kb_sort_key");
    const sortDirEl = document.getElementById("kb_sort_dir");
    const starOnlyEl = document.getElementById("kb_star_only");
    const diaryCheckEl = document.getElementById("kb_diary_check");
    const diaryNoteEl = document.getElementById("kb_diary_note");

    const LS_SORT_KEY = "kb_sort_key";
    const LS_SORT_DIR = "kb_sort_dir";
    const LS_STAR_ONLY = "kb_star_only";
    const LS_DIARY_CHECK = "kb_diary_check";

    // ★ NEWの「消し込み」はブラウザに保存（DBいじらずに“本物”にする）
    const LS_DIARY_SEEN_PREFIX = "kb_diary_seen_ts_"; // + person_id -> epoch(ms)

    function safeLsGet(key) {
      try { return localStorage.getItem(key); } catch (e) { return null; }
    }
    function safeLsSet(key, val) {
      try { localStorage.setItem(key, val); } catch (e) {}
    }
    function getSeenTsMs(personId) {
      const v = safeLsGet(LS_DIARY_SEEN_PREFIX + String(personId));
      const n = parseInt(v || "0", 10);
      return Number.isFinite(n) ? n : 0;
    }
    function setSeenTsMs(personId, tsMs) {
      const n = Number(tsMs || 0);
      if (!Number.isFinite(n) || n <= 0) return;
      safeLsSet(LS_DIARY_SEEN_PREFIX + String(personId), String(Math.trunc(n)));
    }

    // data属性の揺れを吸収しつつ、必要なら dataset に“正規キー”を補う
    function normalizeRowData(el) {
      const pid = getDataAny(el, ["personId", "person_id"]);
      const sid = getDataAny(el, ["storeId", "store_id"]);
      const nm = getDataAny(el, ["name"]);
      const ar = getDataAny(el, ["avgRating", "avg_rating"]);
      const aa = getDataAny(el, ["avgAmount", "avg_amount"]);
      const ht = getDataAny(el, ["height", "heightCm", "height_cm"]);
      const cp = getDataAny(el, ["cup"]);
      const lv = getDataAny(el, ["lastVisit", "last_visit"]);

      ensureDataset(el, "personId", pid);
      ensureDataset(el, "storeId", sid);
      ensureDataset(el, "name", nm);
      ensureDataset(el, "avgRating", ar);
      ensureDataset(el, "avgAmount", aa);
      ensureDataset(el, "height", ht);
      ensureDataset(el, "cup", cp);
      ensureDataset(el, "lastVisit", lv);
    }

    function findPersonLink(el) {
      if (!el) return null;
      // できれば人物ページへのリンクの直後にバッジを置く
      const a1 = el.querySelector('a[href^="/kb/person/"],a[href*="/kb/person/"]');
      if (a1) return a1;
      const a2 = el.querySelector("a");
      return a2 || null;
    }

    function ensureBadge(el, role, text, className) {
      if (!el) return null;
      let b =
        el.querySelector(`[data-role="${role}"]`) ||
        (className ? el.querySelector("." + className) : null) ||
        null;

      if (b && !b.getAttribute("data-role")) {
        try { b.setAttribute("data-role", role); } catch (_) {}
      }

      if (!b) {
        b = document.createElement("span");
        b.className = className || "";
        b.textContent = text;
        b.setAttribute("data-role", role);
        // JSが制御する前提：初期は隠しておく（テンプレが出しっぱなしでも後で矯正する）
        b.hidden = true;

        const a = findPersonLink(el);
        if (a && a.insertAdjacentElement) {
          a.insertAdjacentElement("afterend", b);
        } else {
          el.appendChild(b);
        }
      }

      // “重複?”はまず必ず隠す（常時表示バグ潰し）
      if (role === "dup") {
        b.hidden = true;
      }

      return b;
    }

    items.forEach((el, idx) => {
      normalizeRowData(el);
      el.dataset.origIndex = String(idx);

      // テンプレに無くてもバッジを作る（kb_store でも NEW を出すため）
      ensureBadge(el, "diary-new", "NEW", "kb-diary-new");
      ensureBadge(el, "dup", "重複?", "kb-dup");
    });

    function cupRank(cup) {
      const c = (cup || "").toUpperCase().trim();
      if (!c) return null;
      const m = c.match(/[A-Z]/);
      if (!m) return null;
      const code = m[0].charCodeAt(0);
      if (code < 65 || code > 90) return null;
      return (code - 64); // A=1
    }

    function parseNumOrNull(v) {
      if (v === null || v === undefined) return null;
      const s = String(v).trim();
      if (!s) return null;
      const n = Number(s.replace(/,/g, ""));
      return Number.isFinite(n) ? n : null;
    }

    function parseTimeOrNull(v) {
      const s = String(v || "").trim();
      if (!s) return null;
      const t = Date.parse(s);
      return Number.isFinite(t) ? t : null;
    }

    function compareNullLast(va, vb, dir) {
      const aNull = (va === null || va === undefined);
      const bNull = (vb === null || vb === undefined);
      if (aNull && bNull) return 0;
      if (aNull) return 1;   // 常に最後
      if (bNull) return -1;  // 常に最後
      if (va < vb) return -1 * dir;
      if (va > vb) return 1 * dir;
      return 0;
    }

    // ② 重複検知（表示中の一覧だけ）
    function markDuplicates() {
      // まず全部隠す（テンプレが出しっぱなしでも矯正）
      items.forEach((el) => {
        const badge = el.querySelector('[data-role="dup"]');
        if (badge) badge.hidden = true;
      });

      const map = new Map(); // key -> [elements]
      items.forEach((el) => {
        const name = normalizeName(el.dataset.name || "");
        const storeId = String(el.dataset.storeId || "");
        if (!name) return;
        const key = storeId + "|" + name;
        const arr = map.get(key) || [];
        arr.push(el);
        map.set(key, arr);
      });

      map.forEach((arr) => {
        if (arr.length <= 1) return;
        const ids = arr.map(e => e.dataset.personId).join(", ");
        arr.forEach((el) => {
          const badge = el.querySelector('[data-role="dup"]');
          if (!badge) return;
          badge.hidden = false;
          badge.title = "同一店舗内で同名が複数: " + ids;
        });
      });
    }

    function applyStarFilter() {
      const only = !!starOnlyEl?.checked;
      items.forEach((el) => {
        const hasRating = String(el.dataset.avgRating || "").trim() !== "";
        el.classList.toggle("kb-hidden", only && !hasRating);
      });
    }

    function sortItems() {
      if (!sortKeyEl) return;

      const key = sortKeyEl.value || "none";
      const dir = (sortDirEl?.dataset?.dir || "desc") === "asc" ? 1 : -1;

      const visible = items.slice().filter(el => !el.classList.contains("kb-hidden"));
      const hidden = items.slice().filter(el => el.classList.contains("kb-hidden"));

      function valueOf(el) {
        if (key === "avg_rating") return parseNumOrNull(el.dataset.avgRating);
        if (key === "avg_amount") return parseNumOrNull(el.dataset.avgAmount);
        if (key === "height") return parseNumOrNull(el.dataset.height);
        if (key === "cup") return cupRank(el.dataset.cup);
        if (key === "name") return normalizeName(el.dataset.name);
        if (key === "last_visit") return parseTimeOrNull(el.dataset.lastVisit);
        return parseNumOrNull(el.dataset.origIndex) ?? 0;
      }

      visible.sort((a, b) => {
        const va = valueOf(a);
        const vb = valueOf(b);

        if (typeof va === "string" || typeof vb === "string") {
          const sa = String(va ?? "");
          const sb = String(vb ?? "");
          const c = sa.localeCompare(sb, "ja");
          if (c !== 0) return c * dir;
          const ia = parseNumOrNull(a.dataset.origIndex) ?? 0;
          const ib = parseNumOrNull(b.dataset.origIndex) ?? 0;
          return ia - ib;
        }

        const c = compareNullLast(va, vb, dir);
        if (c !== 0) return c;

        const ia = parseNumOrNull(a.dataset.origIndex) ?? 0;
        const ib = parseNumOrNull(b.dataset.origIndex) ?? 0;
        return ia - ib;
      });

      const frag = document.createDocumentFragment();
      visible.forEach(el => frag.appendChild(el));
      hidden.forEach(el => frag.appendChild(el));
      container.appendChild(frag);
    }

    let diaryReqSeq = 0;
    let diaryBusy = false;
    let diaryTimer = null;

    function stopDiaryTimer() {
      if (diaryTimer) {
        clearTimeout(diaryTimer);
        diaryTimer = null;
      }
    }

    function isDiaryEnabled() {
      // UIが無い場合でも「動く」ことを優先してON扱い
      if (!diaryCheckEl) return true;
      return !!diaryCheckEl.checked;
    }

    function scheduleDiaryNext() {
      stopDiaryTimer();
      if (!isDiaryEnabled()) return;

      // ブロック回避：15分 + ランダムゆらぎ（0〜120秒）
      const base = 15 * 60 * 1000;
      const jitter = Math.floor(Math.random() * 120 * 1000);
      diaryTimer = setTimeout(() => {
        if (document.visibilityState !== "visible") {
          scheduleDiaryNext();
          return;
        }
        updateDiaryBadges();
        scheduleDiaryNext();
      }, base + jitter);
    }

    async function updateDiaryBadges() {
      const seq = ++diaryReqSeq;

      if (!isDiaryEnabled()) {
        // OFFなら全部隠す
        items.forEach((el) => {
          const b = el.querySelector('[data-role="diary-new"]');
          if (b) b.hidden = true;
        });
        if (diaryNoteEl) diaryNoteEl.hidden = true;
        stopDiaryTimer();
        return;
      }

      if (diaryBusy) return;
      diaryBusy = true;

      // 表示中のみ、最大30件だけチェック（ブロック対策）
      const targets = items.filter(el => !el.classList.contains("kb-hidden")).slice(0, 30);
      const ids = targets.map(el => el.dataset.personId).filter(Boolean);

      if (!ids.length) {
        diaryBusy = false;
        return;
      }

      if (diaryNoteEl) {
        diaryNoteEl.hidden = false;
        diaryNoteEl.textContent = "写メ日記: 更新チェック中…（最大" + ids.length + "件）";
      }

      try {
        const url = "/kb/api/diary_latest?ids=" + encodeURIComponent(ids.join(","));
        const res = await fetch(url, { method: "GET" });
        if (!res.ok) throw new Error("status " + res.status);

        const data = await res.json();

        // 古い応答は捨てる
        if (seq !== diaryReqSeq) {
          diaryBusy = false;
          return;
        }

        const mapLatest = new Map();
        const mapOpen = new Map();
        (data.items || []).forEach((it) => {
          const id = String(it.id || "");
          const latest = Number(it.latest_ts || 0);
          if (id) mapLatest.set(id, Number.isFinite(latest) ? latest : 0);
          const ou = String(it.open_url || "");
          if (id) mapOpen.set(id, ou);
        });

        targets.forEach((el) => {
          const pid = String(el.dataset.personId || "");
          const latestTs = mapLatest.get(pid) || 0;
          const seenTs = getSeenTsMs(pid);

          const b = el.querySelector('[data-role="diary-new"]') || ensureBadge(el, "diary-new", "NEW", "kb-diary-new");
          if (!b) return;

          // badgeに情報を埋める（クリック時に使う）
          b.dataset.latestTs = latestTs ? String(Math.trunc(latestTs)) : "";
          b.dataset.openUrl = mapOpen.get(pid) || "";

          const isNew = (latestTs > 0 && latestTs > seenTs);
          b.hidden = !isNew;

          if (!b.hidden) {
            b.title = "写メ日記: NEW（クリックで開いて消えます）";
          }
        });

        if (diaryNoteEl) {
          diaryNoteEl.textContent = "写メ日記: チェック完了（" + ids.length + "件）";
          setTimeout(() => { diaryNoteEl.hidden = true; }, 2500);
        }
      } catch (e) {
        // APIが未実装/停止でも静かに劣化
        if (seq !== diaryReqSeq) {
          diaryBusy = false;
          return;
        }
        if (diaryNoteEl) {
          diaryNoteEl.hidden = false;
          diaryNoteEl.textContent = "写メ日記: 取得失敗（URL未設定/外部取得失敗の可能性）";
        }
      } finally {
        diaryBusy = false;
      }
    }

    function syncSortDirButton() {
      if (!sortDirEl) return;
      const dir = sortDirEl.dataset.dir || "desc";
      sortDirEl.textContent = (dir === "asc") ? "▲" : "▼";
    }

    function applyAll() {
      applyStarFilter();
      sortItems();
      updateDiaryBadges();
    }

    // 初期：復元（UIがある場合のみ）
    try {
      if (sortKeyEl) sortKeyEl.value = localStorage.getItem(LS_SORT_KEY) || "none";
      if (sortDirEl) sortDirEl.dataset.dir = localStorage.getItem(LS_SORT_DIR) || "desc";
      if (starOnlyEl) starOnlyEl.checked = (localStorage.getItem(LS_STAR_ONLY) === "1");
      if (diaryCheckEl) {
        // 未保存なら「ON寄り」で起動（“動いてないように見える”対策）
        const v = localStorage.getItem(LS_DIARY_CHECK);
        diaryCheckEl.checked = (v == null) ? true : (v === "1");
      }
    } catch (e) {}

    markDuplicates();
    syncSortDirButton();
    applyAll();
    scheduleDiaryNext();

    sortKeyEl?.addEventListener("change", () => {
      try { localStorage.setItem(LS_SORT_KEY, sortKeyEl.value); } catch (e) {}
      applyAll();
    });

    sortDirEl?.addEventListener("click", () => {
      if (!sortDirEl) return;
      const cur = sortDirEl.dataset.dir || "desc";
      sortDirEl.dataset.dir = (cur === "asc") ? "desc" : "asc";
      try { localStorage.setItem(LS_SORT_DIR, sortDirEl.dataset.dir); } catch (e) {}
      syncSortDirButton();
      applyAll();
    });

    starOnlyEl?.addEventListener("change", () => {
      try { localStorage.setItem(LS_STAR_ONLY, starOnlyEl.checked ? "1" : "0"); } catch (e) {}
      applyAll();
    });

    diaryCheckEl?.addEventListener("change", () => {
      try { localStorage.setItem(LS_DIARY_CHECK, diaryCheckEl.checked ? "1" : "0"); } catch (e) {}
      updateDiaryBadges();
      scheduleDiaryNext();
    });

    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") {
        updateDiaryBadges();
        scheduleDiaryNext();
      }
    });
  }

  // ============================================================
  // KB：NEWバッジクリック（テンプレが違っても動くように document で拾う）
  // - クリックで “見た” を保存 → NEWを消す → 日記を開く
  // ============================================================
  function initKbDiaryNewClickGlobal() {
    const LS_DIARY_SEEN_PREFIX = "kb_diary_seen_ts_";

    function safeLsSet(key, val) {
      try { localStorage.setItem(key, val); } catch (e) {}
    }
    function setSeenTsMs(personId, tsMs) {
      const n = Number(tsMs || 0);
      if (!Number.isFinite(n) || n <= 0) return;
      safeLsSet(LS_DIARY_SEEN_PREFIX + String(personId), String(Math.trunc(n)));
    }

    function getPersonIdFromRow(row) {
      if (!row) return "";
      const pid = getDataAny(row, ["personId", "person_id"]);
      if (pid) return pid;
      // dataset正規化済みならこちら
      return String(row.dataset?.personId || "").trim();
    }

    async function fetchDiaryLatestOne(pid) {
      const url = "/kb/api/diary_latest?ids=" + encodeURIComponent(String(pid));
      const res = await fetch(url, { method: "GET" });
      if (!res.ok) throw new Error("status " + res.status);
      const data = await res.json();
      const it = (data.items && data.items[0]) ? data.items[0] : null;
      if (!it) return { latestTs: 0, openUrl: "" };
      const latestTs = Number(it.latest_ts || 0);
      const openUrl = String(it.open_url || "");
      return { latestTs: Number.isFinite(latestTs) ? latestTs : 0, openUrl };
    }

    document.addEventListener("click", (e) => {
      const t = e.target;
      if (!t || !t.closest) return;

      // data-role="diary-new" が理想だが、classだけでも拾う（互換）
      const badge =
        t.closest('[data-role="diary-new"]') ||
        t.closest(".kb-diary-new") ||
        t.closest(".kb-new");
      if (!badge) return;

      // hidden属性が使われていれば “出てる時だけ” 反応
      if (badge.hidden === true) return;

      const row = badge.closest(".kb-person-result") || badge.closest("[data-person-id]") || badge.closest("[data-person_id]");
      const pid = getPersonIdFromRow(row);
      if (!pid) return;

      e.preventDefault();
      e.stopPropagation();
      if (e.stopImmediatePropagation) e.stopImmediatePropagation();

      // まずは即座に消す（“クリックしても消えない”対策）
      try { badge.hidden = true; } catch (_) {}

      // タブをユーザー操作の瞬間に確保（後でURLを入れる）
      let win = null;
      try { win = window.open("about:blank", "_blank", "noopener"); } catch (_) { win = null; }

      const latestTs = parseInt(String(badge.dataset?.latestTs || "0"), 10);
      const openUrl = String(badge.dataset?.openUrl || "");

      // latest/openが揃っていれば即処理
      if (Number.isFinite(latestTs) && latestTs > 0) {
        setSeenTsMs(pid, latestTs);
      } else {
        // 無ければサーバに単発問い合わせ（テンプレ/初期化不足でも成立させる）
        fetchDiaryLatestOne(pid)
          .then(({ latestTs: lt, openUrl: ou }) => {
            if (lt > 0) setSeenTsMs(pid, lt);
            const url2 = ou || openUrl || "";
            if (url2) {
              if (win && !win.closed) {
                try { win.location.href = url2; } catch (_) {}
              } else {
                // ポップアップブロック時の保険：同一タブ遷移
                try { window.location.href = url2; } catch (_) {}
              }
            } else {
              // それでも無いなら google に逃がす（最後の保険）
              const name = String(row?.dataset?.name || "").trim();
              const q = (name ? (name + " 写メ日記") : "写メ日記");
              const g = "https://www.google.com/search?q=" + encodeURIComponent(q);
              if (win && !win.closed) {
                try { win.location.href = g; } catch (_) {}
              } else {
                try { window.location.href = g; } catch (_) {}
              }
            }
          })
          .catch(() => {
            // 失敗時も google へ（保険）
            const name = String(row?.dataset?.name || "").trim();
            const q = (name ? (name + " 写メ日記") : "写メ日記");
            const g = "https://www.google.com/search?q=" + encodeURIComponent(q);
            if (win && !win.closed) {
              try { win.location.href = g; } catch (_) {}
            } else {
              try { window.location.href = g; } catch (_) {}
            }
          });
        return;
      }

      // openUrl で開く（無いなら google）
      const url = openUrl || "";
      if (url) {
        if (win && !win.closed) {
          try { win.location.href = url; } catch (_) {}
        } else {
          try { window.location.href = url; } catch (_) {}
        }
      } else {
        const name = String(row?.dataset?.name || "").trim();
        const q = (name ? (name + " 写メ日記") : "写メ日記");
        const g = "https://www.google.com/search?q=" + encodeURIComponent(q);
        if (win && !win.closed) {
          try { win.location.href = g; } catch (_) {}
        } else {
          try { window.location.href = g; } catch (_) {}
        }
      }
    }, true);
  }

  // ============================================================
  // アンカー先ツールチッププレビュー（手動クローズ + スタック）
  // （あなたの既存実装：そのまま）
  // ============================================================
  const API_ENDPOINT = "/api/post_preview";
  const MAX_RANGE_EXPAND = 30;

  const cache = new Map(); // key -> { ok, posted_at, body } or { ok:false, message }

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
    initKbStarRating();
    initKbPriceItems();
    initKbDuration();
    initKbPanicButton();
    initKbBackupImportUi();
    initKbIndexEnhancements();
    initKbDiaryNewClickGlobal();
  });
})();
