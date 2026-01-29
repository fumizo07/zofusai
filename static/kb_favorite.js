// 002
// static/kb_favorite.js
(() => {
  "use strict";

  const API_FAVORITE = "/kb/api/person_favorite";
  const CSRF_INIT_API = "/kb/api/csrf_init";

  const CSRF_COOKIE_NAME = "kb_csrf";
  const CSRF_HEADER_NAME = "X-KB-CSRF";

  // ★ フィルタの「強制非表示」属性（他JSが display を戻しても負けない）
  const HIDE_ATTR = "data-kb-fav-hide"; // "1" なら非表示

  function injectHideCssOnce() {
    if (document.getElementById("kb_fav_css")) return;
    const style = document.createElement("style");
    style.id = "kb_fav_css";
    style.textContent = `
      .kb-person-result[${HIDE_ATTR}="1"] { display: none !important; }
    `;
    document.head.appendChild(style);
  }

  function getCookie(name) {
    try {
      const m = document.cookie.match(
        new RegExp(
          "(^|;\\s*)" +
            name.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&") +
            "=([^;]*)"
        )
      );
      return m ? decodeURIComponent(m[2] || "") : "";
    } catch (_) {
      return "";
    }
  }

  async function ensureCsrfToken() {
    let token = getCookie(CSRF_COOKIE_NAME);
    if (token) return token;

    try {
      await fetch(CSRF_INIT_API, { method: "GET", credentials: "same-origin" });
    } catch (_) {}

    token = getCookie(CSRF_COOKIE_NAME);
    return token || "";
  }

  function setStar(el, fav) {
    if (!el) return;
    const on = !!fav;
    el.textContent = on ? "★" : "☆";
    el.setAttribute("aria-pressed", on ? "true" : "false");
    el.dataset.fav = on ? "1" : "0";
  }

  function setCardFavStateFromChild(btn) {
    const card = btn.closest(".kb-person-result");
    if (!card) return;
    const fav =
      btn.dataset.fav === "1" ||
      btn.getAttribute("aria-pressed") === "true" ||
      btn.textContent === "★";
    card.setAttribute("data-fav", fav ? "1" : "0");
  }

  function updateAllStars(personId, fav) {
    const pid = String(personId);
    const nodes = document.querySelectorAll(
      `[data-kb-fav-toggle="1"][data-person-id="${CSS.escape(pid)}"]`
    );
    nodes.forEach((n) => {
      setStar(n, fav);
      setCardFavStateFromChild(n);
    });

    const cards = document.querySelectorAll(
      `.kb-person-result[data-person-id="${CSS.escape(pid)}"]`
    );
    cards.forEach((c) => c.setAttribute("data-fav", fav ? "1" : "0"));
  }

  // ---- 「お気に入りだけ」フィルタ（CSS強制非表示方式）
  function applyFavOnlyFilter() {
    const chk = document.getElementById("kb_fav_only");
    if (!chk) return;

    const onlyFav = !!chk.checked;
    const list = document.getElementById("kb_person_results");
    if (!list) return;

    const items = list.querySelectorAll(".kb-person-result");
    items.forEach((it) => {
      const isFav = it.getAttribute("data-fav") === "1";
      const hide = onlyFav && !isFav;
      if (hide) it.setAttribute(HIDE_ATTR, "1");
      else it.setAttribute(HIDE_ATTR, "0");
    });
  }

  // いろんな「後から表示を戻す奴」に負けないための再適用（軽いデバウンス）
  function makeDebounced(fn, waitMs) {
    let t = null;
    return () => {
      if (t) clearTimeout(t);
      t = setTimeout(() => {
        t = null;
        fn();
      }, waitMs);
    };
  }
  const applyFavOnlyFilterDebounced = makeDebounced(applyFavOnlyFilter, 50);

  async function postFavorite(personId, favorite) {
    const token = await ensureCsrfToken();

    const res = await fetch(API_FAVORITE, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { [CSRF_HEADER_NAME]: token } : {}),
      },
      body: JSON.stringify({ id: Number(personId), favorite: !!favorite }),
      keepalive: true,
    });

    if (!res.ok) {
      const t = await res.text().catch(() => "");
      throw new Error(`favorite_failed: HTTP ${res.status} ${t}`);
    }

    const json = await res.json().catch(() => null);
    if (json && json.ok === false) {
      throw new Error(`favorite_failed: ${json.error || "unknown"}`);
    }
    return json;
  }

  function initFavoriteUi() {
    injectHideCssOnce();

    // 初期：ボタン表示を dataset/aria に揃える
    const btns = document.querySelectorAll(
      `[data-kb-fav-toggle="1"][data-person-id]`
    );
    btns.forEach((b) => {
      const fav =
        b.getAttribute("aria-pressed") === "true" ||
        b.dataset.fav === "1" ||
        String(b.textContent || "").trim() === "★";
      setStar(b, fav);
      setCardFavStateFromChild(b);
    });

    // 初期フィルタ
    applyFavOnlyFilter();

    // フィルタ変更
    const chk = document.getElementById("kb_fav_only");
    if (chk && chk.dataset.kbFavOnlyApplied !== "1") {
      chk.dataset.kbFavOnlyApplied = "1";
      chk.addEventListener("change", () => {
        applyFavOnlyFilter();
        // 念のため、後から別JSが触っても戻るように少し後でも再適用
        setTimeout(applyFavOnlyFilterDebounced, 150);
        setTimeout(applyFavOnlyFilterDebounced, 800);
      });
    }

    // 並び替えが走った後に表示を戻される可能性が高いので、sort変更後も再適用
    const sortSel = document.getElementById("kb_person_sort");
    if (sortSel && sortSel.dataset.kbFavSortHook !== "1") {
      sortSel.dataset.kbFavSortHook = "1";
      sortSel.addEventListener("change", () => {
        // 並び替え側のDOM操作が終わったタイミングで再適用
        setTimeout(applyFavOnlyFilterDebounced, 0);
        setTimeout(applyFavOnlyFilterDebounced, 120);
        setTimeout(applyFavOnlyFilterDebounced, 500);
      });
    }

    // DOMが動いたら再適用（並び替え・日記更新などでHTMLが書き換わる想定）
    const list = document.getElementById("kb_person_results");
    if (list && !list.dataset.kbFavObserver) {
      list.dataset.kbFavObserver = "1";
      const obs = new MutationObserver(() => {
        applyFavOnlyFilterDebounced();
      });
      obs.observe(list, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["style", "class", "data-fav", "data-person-id"],
      });
    }

    // クリックでトグル（イベントデリゲーション）
    if (document.body.dataset.kbFavApplied === "1") return;
    document.body.dataset.kbFavApplied = "1";

    document.addEventListener(
      "click",
      async (e) => {
        const btn = e.target?.closest?.(
          `[data-kb-fav-toggle="1"][data-person-id]`
        );
        if (!btn) return;

        e.preventDefault();

        const pid = btn.getAttribute("data-person-id");
        if (!pid) return;

        const beforeFav =
          btn.getAttribute("aria-pressed") === "true" || btn.textContent === "★";
        const nextFav = !beforeFav;

        if (btn.dataset.busy === "1") return;
        btn.dataset.busy = "1";

        // 見た目先行
        updateAllStars(pid, nextFav);
        applyFavOnlyFilterDebounced();

        try {
          await postFavorite(pid, nextFav);
          // 成功：このまま
          applyFavOnlyFilterDebounced();
        } catch (_) {
          // 失敗：戻す
          updateAllStars(pid, beforeFav);
          applyFavOnlyFilterDebounced();
          alert(
            "お気に入りの保存に失敗しました（通信/CSRF/ログイン状態を確認してください）。"
          );
        } finally {
          btn.dataset.busy = "0";
        }
      },
      true
    );
  }

  document.addEventListener("DOMContentLoaded", initFavoriteUi);
})();
