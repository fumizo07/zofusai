# main.py
import os
import secrets

from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import PlainTextResponse
from fastapi.templating import Jinja2Templates

from app_lifecycle import register_startup
from routers.internal_search import router as internal_router
from routers.admin import router as admin_router
from routers.threads import router as threads_router
from routers.external_search import router as external_router

# ★ツールチップ用（既存）
from preview_api import preview_api
# ★投稿編集（既存）
from post_edit import post_edit_router

from models import ThreadPost, ThreadMeta, CachedThread, CachedPost, ExternalSearchHistory, KBRegion, KBStore, KBPerson, KBVisit

# =========================
# BASIC 認証
# =========================
security = HTTPBasic()
BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER") or ""
BASIC_AUTH_PASS = os.getenv("BASIC_AUTH_PASS") or ""
BASIC_ENABLED = bool(BASIC_AUTH_USER and BASIC_AUTH_PASS)


def verify_basic(credentials: HTTPBasicCredentials = Depends(security)):
    if not BASIC_ENABLED:
        return
    correct_username = secrets.compare_digest(credentials.username, BASIC_AUTH_USER)
    correct_password = secrets.compare_digest(credentials.password, BASIC_AUTH_PASS)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )


# =========================
# FastAPI 初期化
# =========================
app = FastAPI(
    dependencies=[Depends(verify_basic)],
    docs_url=None,
    redoc_url=None,
)

# static / templates
app.mount("/static", StaticFiles(directory="static"), name="static")

# router 登録
app.include_router(preview_api)
app.include_router(post_edit_router)

app.include_router(internal_router)
app.include_router(admin_router)
app.include_router(threads_router)
app.include_router(external_router)

# startup（DB schema補助・バックフィル）
register_startup(app)


# =========================
# robots.txt でクロール拒否
# =========================
@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"



# =========================
# KB（知った情報を整理する）
# =========================
from sqlalchemy import desc


@app.get("/kb", response_class=HTMLResponse)
def kb_index(request: Request, db: Session = Depends(get_db)):
    regions = db.query(KBRegion).order_by(KBRegion.name.asc()).all()

    # region -> stores
    store_rows = (
        db.query(KBStore, KBRegion)
        .join(KBRegion, KBRegion.id == KBStore.region_id)
        .order_by(KBRegion.name.asc(), KBStore.name.asc())
        .all()
    )

    stores_by_region = defaultdict(list)
    for s, r in store_rows:
        stores_by_region[r.id].append(s)

    # store -> persons count
    counts = dict(
        db.query(KBPerson.store_id, func.count(KBPerson.id))
        .group_by(KBPerson.store_id)
        .all()
    )

    return templates.TemplateResponse(
        "kb_index.html",
        {
            "request": request,
            "regions": regions,
            "stores_by_region": stores_by_region,
            "person_counts": counts,
        },
    )


@app.post("/kb/region")
def kb_add_region(request: Request, name: str = Form(""), db: Session = Depends(get_db)):
    name = (name or "").strip()
    back_url = request.headers.get("referer") or "/kb"
    if not name:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        exists = db.query(KBRegion).filter(KBRegion.name == name).first()
        if not exists:
            db.add(KBRegion(name=name))
            db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url="/kb", status_code=303)


@app.post("/kb/store")
def kb_add_store(
    request: Request,
    region_id: int = Form(...),
    name: str = Form(""),
    db: Session = Depends(get_db),
):
    name = (name or "").strip()
    back_url = request.headers.get("referer") or "/kb"
    if not name:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        exists = (
            db.query(KBStore)
            .filter(KBStore.region_id == int(region_id), KBStore.name == name)
            .first()
        )
        if not exists:
            db.add(KBStore(region_id=int(region_id), name=name))
            db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url="/kb", status_code=303)


@app.get("/kb/store/{store_id}", response_class=HTMLResponse)
def kb_store_page(request: Request, store_id: int, db: Session = Depends(get_db)):
    store = db.query(KBStore).filter(KBStore.id == int(store_id)).first()
    if not store:
        return RedirectResponse(url="/kb", status_code=303)

    region = db.query(KBRegion).filter(KBRegion.id == store.region_id).first()

    persons = (
        db.query(KBPerson)
        .filter(KBPerson.store_id == store.id)
        .order_by(KBPerson.name.asc())
        .all()
    )

    return templates.TemplateResponse(
        "kb_store.html",
        {
            "request": request,
            "region": region,
            "store": store,
            "persons": persons,
        },
    )


@app.post("/kb/person")
def kb_add_person(
    request: Request,
    store_id: int = Form(...),
    name: str = Form(""),
    db: Session = Depends(get_db),
):
    name = (name or "").strip()
    back_url = request.headers.get("referer") or "/kb"
    if not name:
        return RedirectResponse(url=back_url, status_code=303)

    try:
        exists = (
            db.query(KBPerson)
            .filter(KBPerson.store_id == int(store_id), KBPerson.name == name)
            .first()
        )
        if exists:
            return RedirectResponse(url=f"/kb/person/{exists.id}", status_code=303)

        p = KBPerson(store_id=int(store_id), name=name)
        db.add(p)
        db.commit()
        db.refresh(p)
        return RedirectResponse(url=f"/kb/person/{p.id}", status_code=303)
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@app.get("/kb/person/{person_id}", response_class=HTMLResponse)
def kb_person_page(request: Request, person_id: int, db: Session = Depends(get_db)):
    person = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not person:
        return RedirectResponse(url="/kb", status_code=303)

    store = db.query(KBStore).filter(KBStore.id == person.store_id).first()
    region = db.query(KBRegion).filter(KBRegion.id == store.region_id).first() if store else None

    visits = (
        db.query(KBVisit)
        .filter(KBVisit.person_id == person.id)
        .order_by(desc(KBVisit.visited_at).nullslast(), desc(KBVisit.id))
        .all()
    )

    # 平均評価（null除外）
    rating_avg = (
        db.query(func.avg(KBVisit.rating))
        .filter(KBVisit.person_id == person.id, KBVisit.rating.isnot(None))
        .scalar()
    )

    return templates.TemplateResponse(
        "kb_person.html",
        {
            "request": request,
            "region": region,
            "store": store,
            "person": person,
            "visits": visits,
            "rating_avg": rating_avg,
        },
    )


@app.post("/kb/person/{person_id}/update")
def kb_update_person(
    request: Request,
    person_id: int,
    name: str = Form(""),
    height_cm: str = Form(""),
    bust_cm: str = Form(""),
    waist_cm: str = Form(""),
    hip_cm: str = Form(""),
    tags: str = Form(""),
    memo: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/person/{person_id}"
    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url="/kb", status_code=303)

    def _to_int(x):
        x = (x or "").strip()
        if not x:
            return None
        try:
            return int(x)
        except Exception:
            return None

    try:
        p.name = (name or "").strip() or p.name
        p.height_cm = _to_int(height_cm)
        p.bust_cm = _to_int(bust_cm)
        p.waist_cm = _to_int(waist_cm)
        p.hip_cm = _to_int(hip_cm)
        p.tags = (tags or "").strip() or None
        p.memo = (memo or "").strip() or None
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@app.post("/kb/person/{person_id}/visit")
def kb_add_visit(
    request: Request,
    person_id: int,
    visited_at: str = Form(""),
    rating: str = Form(""),
    memo: str = Form(""),
    price_items_json: str = Form(""),
    db: Session = Depends(get_db),
):
    back_url = request.headers.get("referer") or f"/kb/person/{person_id}"
    p = db.query(KBPerson).filter(KBPerson.id == int(person_id)).first()
    if not p:
        return RedirectResponse(url="/kb", status_code=303)

    # visited_at は "YYYY-MM-DD" を想定（無ければnull）
    dt = None
    va = (visited_at or "").strip()
    if va:
        try:
            dt = datetime.strptime(va, "%Y-%m-%d")
        except Exception:
            dt = None

    # rating 1-5
    r = None
    try:
        rr = int((rating or "").strip() or "0")
        if 1 <= rr <= 5:
            r = rr
    except Exception:
        r = None

    # price items
    items = []
    total = 0
    raw = (price_items_json or "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                for it in data:
                    if not isinstance(it, dict):
                        continue
                    label = str(it.get("label", "")).strip()
                    amt = it.get("amount", 0)
                    try:
                        amt_i = int(amt)
                    except Exception:
                        amt_i = 0
                    if not label and amt_i == 0:
                        continue
                    items.append({"label": label, "amount": amt_i})
                    total += amt_i
        except Exception:
            items = []
            total = 0

    try:
        v = KBVisit(
            person_id=p.id,
            visited_at=dt,
            rating=r,
            memo=(memo or "").strip() or None,
            price_items=items or None,
            total_yen=total,
        )
        db.add(v)
        db.commit()
    except Exception:
        db.rollback()

    return RedirectResponse(url=back_url, status_code=303)


@app.post("/kb/visit/{visit_id}/delete")
def kb_delete_visit(request: Request, visit_id: int, db: Session = Depends(get_db)):
    back_url = request.headers.get("referer") or "/kb"
    try:
        db.query(KBVisit).filter(KBVisit.id == int(visit_id)).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()
    return RedirectResponse(url=back_url, status_code=303)

