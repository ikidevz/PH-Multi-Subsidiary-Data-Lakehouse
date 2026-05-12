

from fastapi import FastAPI

from database import init_db
from routers.deps import SUB_ID, DEPT_TYPE, gen
from routers import finance, tax, hr, ops, sales, procurement, legal, it_audit

app = FastAPI(title="PH Multi-Subsidiary Dept Service - CRUD Enabled")


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup_event():
    init_db()


# ── Health & Info ─────────────────────────────────────────────────────────────

@app.get("/health", tags=["Info"])
def health():
    return {
        "status": "ok",
        "subsidiary_id": SUB_ID,
        "dept": DEPT_TYPE,
        "mode": "crud-enabled",
        "database": "postgresql",
    }


@app.get("/schema", tags=["Info"])
def schema():
    return gen.schema()


@app.get("/metrics", tags=["Info"])
def metrics():
    return gen.metrics()


@app.post("/reset", tags=["Info"])
def reset():
    gen.reset()
    return {"status": "reset", "subsidiary_id": SUB_ID, "dept": DEPT_TYPE}


@app.post("/generate", tags=["Info"])
def generate(days: int = 30):
    return {"generated": gen.generate_all(days=days), "subsidiary_id": SUB_ID, "dept": DEPT_TYPE}


@app.get("/records", tags=["Info"])
def records(days: int = 30, category: str | None = None):
    return gen.records(days=days, category=category)


# ── Department Routers ────────────────────────────────────────────────────────

app.include_router(finance.router)
app.include_router(tax.router)
app.include_router(hr.router)
app.include_router(ops.router)
app.include_router(sales.router)
app.include_router(procurement.router)
app.include_router(legal.router)
app.include_router(it_audit.router)
