// 001
// static/kb_favorite.js
(() => {
  "use strict";

  const API_FAVORITE = "/kb/api/person_favorite";
  const CSRF_INIT_API = "/kb/api/csrf_init";

  const CSRF_COOKIE_NAME = "kb_csrf";
  const CSRF_HEADER_NAME = "X-KB-CSRF";

  function getCookie(name) {
    try {
      const m = document.cookie.match(new RegExp("(^|;\\s*)" + name.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&") + "=([^;]*)"));
      return m ? decodeURIComponent(m[2] || "") : "";
    } catch (_) {
      return "";
    }
  }

  async function ensureCsrfToken() {
    let token = getCookie(CSRF_COOKIE_NAME);
    if (token) return token;

    // cookieが無ければ発行しに行く（Userscript不要・通常ブラウザ用）
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
    const fav = (btn.dataset.fav === "1") || (btn.getAttribute("aria-pressed") === "true") || (btn.textContent === "★");
    card.setAttribute("data-fav", fav ? "1" : "0");
  }

  function updateAllStars(personId, fav) {
    const pid = String(personId);
    const nodes = document.querySelectorAll(`[data-kb-fav-toggle="1"][data-person-id="${CSS.escape(pid)}"]`);
    nodes.forEach((n) => {
      setStar(n, fav);
      setCardFavStateFromChild(n);
    });

    // card側だけ先にあるケースに備えて直接も更新
    const cards = document.querySelectorAll(`.kb-person-result[data-person-id="${CSS.escape(pid)}"]`);
    cards.forEach((c) => c.setAttribute("data-fav", fav ? "1" : "0"));
  }

  function applyFavOnlyFilter() {
    const chk = document.getElementById("kb_fav_only");
    if (!chk) return;

    const onlyFav = !!chk.checked;
    const list = document.getElementById("kb_person_results");
    if (!list) return;

    const items = list.querySelectorAll(".kb-person-result");
    items.forEach((it) => {
      const isFav = (it.getAttribute("data-fav") === "1");
      it.style.display = (!onlyFav || isFav) ? "" : "none";
    });
  }

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

    // API仕様は「今のでOK」とのことなので、ここでは成功/失敗だけ見ます
    if (!res.ok) {
      const t = await res.text().catch(() => "");
      throw new Error(`favorite_failed: HTTP ${res.status} ${t}`);
    }

    const json = await res.json().catch(() => null);
    // ok: true を返す想定
    if (json && json.ok === false) {
      throw new Error(`favorite_failed: ${json.error || "unknown"}`);
    }
    return json;
  }

  function initFavoriteUi() {
    // 初期：ボタン表示を dataset/aria に揃える
    const btns = document.querySelectorAll(`[data-kb-fav-toggle="1"][data-person-id]`);
    btns.forEach((b) => {
      const fav =
        (b.getAttribute("aria-pressed") === "true") ||
        (b.dataset.fav === "1") ||
        (String(b.textContent || "").trim() === "★");
      setStar(b, fav);
      setCardFavStateFromChild(b);
    });

    // フィルタ初期反映
    applyFavOnlyFilter();

    // フィルタ変更
    const chk = document.getElementById("kb_fav_only");
    if (chk && chk.dataset.kbFavOnlyApplied !== "1") {
      chk.dataset.kbFavOnlyApplied = "1";
      chk.addEventListener("change", applyFavOnlyFilter);
    }

    // クリックでトグル（イベントデリゲーション）
    if (document.body.dataset.kbFavApplied === "1") return;
    document.body.dataset.kbFavApplied = "1";

    document.addEventListener(
      "click",
      async (e) => {
        const btn = e.target?.closest?.(`[data-kb-fav-toggle="1"][data-person-id]`);
        if (!btn) return;

        e.preventDefault();

        const pid = btn.getAttribute("data-person-id");
        if (!pid) return;

        const beforeFav = (btn.getAttribute("aria-pressed") === "true") || (btn.textContent === "★");
        const nextFav = !beforeFav;

        // 連打防止
        if (btn.dataset.busy === "1") return;
        btn.dataset.busy = "1";

        // 体感を良くするため先に見た目だけ反映（失敗したら戻す）
        updateAllStars(pid, nextFav);
        applyFavOnlyFilter();

        try {
          await postFavorite(pid, nextFav);
          // 成功：このまま確定
        } catch (err) {
          // 失敗：戻す
          updateAllStars(pid, beforeFav);
          applyFavOnlyFilter();
          alert("お気に入りの保存に失敗しました（通信/CSRF/ログイン状態を確認してください）。");
        } finally {
          btn.dataset.busy = "0";
        }
      },
      true
    );
  }

  document.addEventListener("DOMContentLoaded", initFavoriteUi);
})();
