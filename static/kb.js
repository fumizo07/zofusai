// 011
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

  function cupToRank(cup) {
    const c = String(cup || "").trim().toUpperCase();
    if (!c) return null;
    const code = c.charCodeAt(0);
    if (code < 65 || code > 90) return null;
    return code - 64;
  }

  // ============================================================
  // KB：人物検索結果の並び替え + フィルタ（repeat/NG）
  // ============================================================
  function initKbPersonSearchSort() {
    const sel = document.getElementById("kb_person_sort");
    const list = document.getElementById("kb_person_results");
    if (!sel || !list) return;

    const repeatSel = document.getElementById("kb_repeat_filter");
    const hideNgChk = document.getElementById("kb_hide_ng");

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
      const aNull = a == null;
      const bNull = b == null;
      if (aNull && bNull) return 0;
      if (aNull) return 1; // nullは最後
      if (bNull) return -1;
      return asc ? a - b : b - a;
    }

    function getRepeat(el) {
      return String(el?.dataset?.repeat || "").trim().toLowerCase(); // yes/hold/no/""
    }

    function getNextActionRank(el) {
      const a = String(el?.dataset?.nextAction || "").trim();
      if (!a) return 99;
      if (a === "調べる") return 0;
      if (a === "予約する") return 1;
      if (a === "再訪検討") return 2;
      if (a === "見送り") return 3;
      if (a === "メモだけ") return 4;
      return 9;
    }
    
    function applyFilter(items) {
      const repeatVal = String(repeatSel?.value || "").trim().toLowerCase(); // yes/hold/no/""
      const hideNg = !!hideNgChk?.checked;

      // repeat=no を明示してる時は NG を隠すと矛盾するので無効化
      const effectiveHideNg = hideNg && repeatVal !== "no";

      items.forEach((el) => {
        const ri = getRepeat(el);
        let hide = false;

        // NG非表示（ただし repeat=no の時は無効）
        if (effectiveHideNg && ri === "no") hide = true;

        // repeat絞り込み
        if (!hide && repeatVal) {
          if (ri !== repeatVal) hide = true;
        }

        el.classList.toggle("kb-hidden", hide);
      });
    }

    function applySort(mode) {
      const items = Array.from(list.querySelectorAll(".kb-person-result"));
      if (!items.length) return;

      // まずフィルタを適用（表示/非表示）
      applyFilter(items);

      const enriched = items.map((el, idx) => ({
        el,
        idx,
        visible: !el.classList.contains("kb-hidden"),
        name: getNameKey(el),
        rating: getNumKey(el, "sortRating"),
        cupRank: cupToRank(el?.dataset?.sortCup || ""),
        height: getNumKey(el, "sortHeight"),
        price: getNumKey(el, "sortPrice"),
        age: getNumKey(el, "sortAge"),
        cand: getNumKey(el, "sortCand"), // 1..5 / null
        nextActionRank: getNextActionRank(el),
        lastVisitTs: getNumKey(el, "lastVisitTs"),
        diaryLatestTs: getNumKey(el, "diaryLatestTs"),
      }));

      enriched.sort((A, B) => {
        // 非表示のものは最後へ（順序の安定のため）
        if (A.visible !== B.visible) return A.visible ? -1 : 1;

        if (mode === "smart") {
          // 1) 候補が上（candがある人）
          const aIsCand = A.cand != null;
          const bIsCand = B.cand != null;
          if (aIsCand !== bIsCand) return aIsCand ? -1 : 1;

          // 2) 候補内は cand(1→5) → 日記最新(新しい→古い) → 最終訪問(古い→新しい)
          if (aIsCand && bIsCand) {
            const c1 = compareNullableNumber(A.cand, B.cand, true);
            if (c1 !== 0) return c1;

            const cAct = compareNullableNumber(A.nextActionRank, B.nextActionRank, true);
            if (cAct !== 0) return cAct;

            const c2 = compareNullableNumber(A.diaryLatestTs, B.diaryLatestTs, false);
            if (c2 !== 0) return c2;

            const c3 = compareNullableNumber(A.lastVisitTs, B.lastVisitTs, true);
            if (c3 !== 0) return c3;
          } else {
            const cAct = compareNullableNumber(A.nextActionRank, B.nextActionRank, true);
            if (cAct !== 0) return cAct;
            
            // 3) 候補外は 日記最新(新しい→古い) → 最終訪問(古い→新しい)
            const c2 = compareNullableNumber(A.diaryLatestTs, B.diaryLatestTs, false);
            if (c2 !== 0) return c2;

            const c3 = compareNullableNumber(A.lastVisitTs, B.lastVisitTs, true);
            if (c3 !== 0) return c3;
          }
        } else if (mode === "candidate") {
          const c = compareNullableNumber(A.cand, B.cand, true); // 1→5
          if (c !== 0) return c;
        } else if (mode === "name") {
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

    const rerun = () => applySort(sel.value || "name");
    document.addEventListener("kb:personResults:rerunSort", rerun);

    sel.addEventListener("change", rerun);
    if (repeatSel) repeatSel.addEventListener("change", rerun);
    if (hideNgChk) hideNgChk.addEventListener("change", rerun);

    rerun();
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
          btn.textContent = val && n <= val ? "★" : "☆";
        });
        if (label) label.textContent = val ? `（${val}/5）` : "";
      }

      stars.forEach((btn) => {
        btn.addEventListener("click", () => {
          const v = parseInt(btn.getAttribute("data-value") || "0", 10);
          const val = 1 <= v && v <= 5 ? v : "";
          if (input) input.value = String(val || "");
          render(val);
        });
      });

      const initVal = input ? parseInt(input.value || "0", 10) : 0;
      render(1 <= initVal && initVal <= 5 ? initVal : 0);
    });
  }

  // ============================================================
  // KB パニックボタン（チェックONで有効化）
  // ============================================================
  function initKbPanicCheck() {
    const chk = document.getElementById("kb_panic_check");
    const btn = document.getElementById("kb_panic_btn");
    if (!chk || !btn) return;

    if (chk.dataset.kbPanicApplied === "1") return;
    chk.dataset.kbPanicApplied = "1";

    const sync = () => {
      btn.disabled = !chk.checked;
    };
    chk.addEventListener("change", sync);
    sync();
  }

  // ============================================================
  // KB バックアップ生成＆コピー＋インポート確認
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
        const sync = () => {
          importBtn.disabled = !importChk.checked;
        };
        importChk.addEventListener("change", sync);
        sync();
      }
    }

    if (!btnGen || !btnCopy || !ta) return;

    if (btnGen.dataset.kbBackupApplied === "1") return;
    btnGen.dataset.kbBackupApplied = "1";

    const setMsg = (t) => {
      if (msg) msg.textContent = t || "";
    };

    btnGen.addEventListener("click", async function () {
      setMsg("");
      btnGen.disabled = true;
      btnCopy.disabled = true;
      const orig = btnGen.textContent;
      btnGen.textContent = "生成中...";

      try {
        const res = await fetch("/kb/export", { headers: { Accept: "application/json" } });
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
  // KB 店舗名編集+保存
  // ============================================================
  function initKbStoreInlineEdit() {
  // 編集ボタンのクリックを委譲で拾う（一覧は繰り返し要素なのでこれが安全）
  document.addEventListener("click", (ev) => {
    const btn = ev.target?.closest?.("[data-kb-store-edit-toggle='1']");
    if (btn) {
      const storeId = String(btn.dataset.storeId || "");
      const form = document.querySelector(`[data-kb-store-edit-form='1'][data-store-id='${storeId}']`);
      if (form) form.classList.toggle("kb-hidden");
      return;
    }

    const cancel = ev.target?.closest?.("[data-kb-store-edit-cancel='1']");
    if (cancel) {
      const form = cancel.closest("form");
      if (form) form.classList.add("kb-hidden");
      return;
    }
    });
  }

  // ============================================================
  // KB 人物一覧で編集
  // ============================================================
  function initKbQuickEdit() {
  document.querySelectorAll("form.kb-quick-edit select").forEach((sel) => {
    sel.addEventListener("change", () => {
      const form = sel.closest("form.kb-quick-edit");
      if (form) form.submit();
    });
  });
  }

  // ============================================================
  // 起動
  // ============================================================
  document.addEventListener("DOMContentLoaded", () => {
    initKbPersonSearchSort();
    initKbStarRating();
    // ★日記系は kb_diary_show.js 側で起動します
    // ★料金項目/テンプレは kb_price_temp.js 側で起動します（kb_person.html だけ_footer.htmlで読み込む想定）
    // ★利用時間は kb_usage_time.js 側で起動します（kb_person.html だけ_footer.htmlで読み込む想定）
    initKbStoreInlineEdit();
    initKbQuickEdit();
    initKbPanicCheck();
    initKbBackupUi();
  });
})();
