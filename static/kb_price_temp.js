// 001
// static/kb_price.js
(() => {
  "use strict";

  // ============================================================
  // Helpers（kb.js から独立させるため最低限だけ複製）
  // ============================================================
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
        sortMode: p.sortMode === "name" || p.sortMode === "manual" || p.sortMode === "usage" ? p.sortMode : "usage",
        manualOrder: Array.isArray(p.manualOrder) ? p.manualOrder.map((x) => String(x)) : [],
        useCounts: p.useCounts && typeof p.useCounts === "object" ? p.useCounts : {},
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
        const amtRaw = tr.querySelector('[data-role="item_amount]')?.value || "";
        const amt = parseYen(amtRaw);
        if (!label && amt === 0) return;
        items.push({ label, amount: amt });
        sum += amt;
      });
      return { items, sum };
    }

    function modalCollectItemsSafe(modal) {
      // ↑の selector typo を避けるため、確実な実装を使う
      const body = modal.querySelector('[data-role="items"]');
      const rows = body ? Array.from(body.querySelectorAll("tr")) : [];
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
      const { sum } = modalCollectItemsSafe(modal);
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
      let selectedId = initialTemplateId
        ? String(initialTemplateId)
        : templatesRaw[0]?.id != null
          ? String(templatesRaw[0].id)
          : "";

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
          r.checked = r.value === prefs.sortMode;
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

      modal.addEventListener(
        "input",
        (e) => {
          const t = e.target;
          if (!t) return;
          if (t.matches('[data-role="item_label"]') || t.matches('[data-role="item_amount"]')) {
            modalRenderSum(modal);
          }
        },
        true
      );

      modal.addEventListener(
        "blur",
        (e) => {
          const t = e.target;
          if (!t || !t.matches || !t.matches('[data-role="item_amount"]')) return;
          const raw = (t.value || "").trim();
          if (!raw) return;
          const n = parseYen(raw);
          t.value = n ? formatYen(n) : "";
          modalRenderSum(modal);
        },
        true
      );

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
          const { items } = modalCollectItemsSafe(modal);
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

          const { items } = modalCollectItemsSafe(modal);
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
                items: Array.isArray(x?.items)
                  ? x.items.map((it) => ({
                      label: String(it?.label ?? "").trim(),
                      amount: parseYen(it?.amount ?? 0),
                    }))
                  : [],
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
                try {
                  await apiDeleteTemplate(t0.id);
                } catch (e) {}
              }
            }

            for (const t1 of templates) {
              const name = String(t1?.name || "").trim();
              const items = Array.isArray(t1?.items)
                ? t1.items.map((it) => ({
                    label: String(it?.label ?? "").trim(),
                    amount: parseYen(it?.amount ?? 0),
                  }))
                : [];
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

      root.addEventListener(
        "blur",
        (e) => {
          const t = e.target;
          if (!t || !t.matches || !t.matches("[data-price-amount]")) return;

          const raw = (t.value || "").trim();
          if (!raw) return;

          const n = parseYen(raw);
          t.value = n ? formatYen(n) : "";
          collectAndSync();
        },
        true
      );

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
  // 起動（このファイルを読み込んだページだけで動く）
  // ============================================================
  document.addEventListener("DOMContentLoaded", () => {
    initKbPriceItems();
  });
})();
