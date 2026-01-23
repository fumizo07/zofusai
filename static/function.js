// 001
// static/kb_price_templates.js
(() => {
  "use strict";

  function normInt(s) {
    const raw = String(s ?? "").replace(/[,_\s\u3000]/g, "").replace(/[¥￥円]/g, "");
    const m = raw.match(/-?\d+/);
    if (!m) return 0;
    const v = parseInt(m[0], 10);
    return Number.isFinite(v) && v > 0 ? v : 0;
  }

  function ensureOneRow(tbody) {
    if (!tbody) return;
    if (tbody.querySelectorAll("tr").length > 0) return;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="text" data-price-label placeholder="例：オプション"></td>
      <td><input type="text" data-price-amount placeholder="例：3,000"></td>
      <td><button type="button" data-price-remove>削除</button></td>
    `;
    tbody.appendChild(tr);
  }

  function readItems(box) {
    const tbody = box.querySelector("[data-price-items-body]");
    const rows = tbody ? Array.from(tbody.querySelectorAll("tr")) : [];
    const items = [];
    let total = 0;

    for (const tr of rows) {
      const labelEl = tr.querySelector("[data-price-label]");
      const amtEl = tr.querySelector("[data-price-amount]");
      const label = String(labelEl?.value ?? "").trim();
      const amount = normInt(amtEl?.value ?? "");
      if (!label && amount === 0) continue;
      items.push({ label, amount });
      total += amount;
    }
    return { items, total };
  }

  function writeRows(box, items) {
    const tbody = box.querySelector("[data-price-items-body]");
    if (!tbody) return;

    tbody.innerHTML = "";
    for (const it of (items || [])) {
      const tr = document.createElement("tr");
      const label = String(it?.label ?? "");
      const amount = String(it?.amount ?? 0);
      tr.innerHTML = `
        <td><input type="text" data-price-label value="${escapeHtmlAttr(label)}" placeholder="例：オプション"></td>
        <td><input type="text" data-price-amount value="${escapeHtmlAttr(amount)}" placeholder="例：3,000"></td>
        <td><button type="button" data-price-remove>削除</button></td>
      `;
      tbody.appendChild(tr);
    }
    ensureOneRow(tbody);
  }

  function syncBox(box) {
    const hidden = box.querySelector('input[name="price_items_json"]');
    const totalEl = box.querySelector("[data-price-total]");
    const { items, total } = readItems(box);

    if (hidden) {
      try {
        hidden.value = JSON.stringify(items);
      } catch {
        hidden.value = "[]";
      }
    }
    if (totalEl) totalEl.textContent = String(total);
  }

  function escapeHtmlAttr(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function onAdd(box) {
    const tbody = box.querySelector("[data-price-items-body]");
    if (!tbody) return;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="text" data-price-label placeholder="例：オプション"></td>
      <td><input type="text" data-price-amount placeholder="例：3,000"></td>
      <td><button type="button" data-price-remove>削除</button></td>
    `;
    tbody.appendChild(tr);
    syncBox(box);
  }

  function onRemove(box, btn) {
    const tr = btn.closest("tr");
    if (tr) tr.remove();
    const tbody = box.querySelector("[data-price-items-body]");
    ensureOneRow(tbody);
    syncBox(box);
  }

  function applyTemplate(box) {
    const sel = box.querySelector("[data-price-template]");
    const raw = String(sel?.value ?? "").trim();
    if (!raw) return;

    let items = [];
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) items = parsed;
    } catch {
      items = [];
    }

    writeRows(box, items);
    syncBox(box);
  }

  function clearTemplate(box) {
    writeRows(box, []);
    syncBox(box);
  }

  function bindBox(box) {
    if (!box || box.dataset.priceTemplatesBound === "1") return;
    box.dataset.priceTemplatesBound = "1";

    // 初期同期（既存明細が描画されている場合に合計とhiddenを揃える）
    syncBox(box);

    box.addEventListener("input", (e) => {
      const t = e.target;
      if (!t) return;
      if (t.matches("[data-price-label], [data-price-amount]")) {
        syncBox(box);
      }
    });

    box.addEventListener("click", (e) => {
      const t = e.target;
      if (!t) return;

      if (t.matches("[data-price-add]")) {
        e.preventDefault();
        onAdd(box);
        return;
      }
      if (t.matches("[data-price-remove]")) {
        e.preventDefault();
        onRemove(box, t);
        return;
      }
      if (t.matches("[data-price-template-apply]")) {
        e.preventDefault();
        applyTemplate(box);
        return;
      }
      if (t.matches("[data-price-template-clear]")) {
        e.preventDefault();
        clearTemplate(box);
        return;
      }
    });

    // submit直前にも同期（保険）
    const form = box.closest("form");
    if (form) {
      form.addEventListener("submit", () => syncBox(box));
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("[data-price-items]").forEach(bindBox);
  });
})();
