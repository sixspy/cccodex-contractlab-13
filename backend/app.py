
import hashlib, secrets, sqlite3, threading
from datetime import date, datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 拒收同败：同载荷并发拒收排队用的全局锁（单机库，进程内串行化即可）
_REJECT_LOCK = threading.RLock()
# 测试闸门：让并发用例能稳定复现“后到排队”
_test_reject_gate = None
# 故障注入钩子（测试用）：在拒收事务内指定语句前抛错，验证整笔回滚
_fail_before_sql: str | None = None
_fail_with: Exception | None = None

def _maybe_fail(sql: str):
    if _fail_before_sql and sql.lstrip().upper().startswith(_fail_before_sql):
        raise _fail_with

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def payload_key(subject: str, payload_fields: str) -> str:
    """同一载荷的稳定指纹：主题 + 排序后的字段集合。"""
    fields = ",".join(sorted(x.strip() for x in payload_fields.split(",") if x.strip()))
    return hashlib.sha256(f"{subject}|{fields}".encode("utf-8")).hexdigest()[:16]

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        -- 拒收原因流水：与拒收批次同成同败（甲路径，同一事务）
        CREATE TABLE IF NOT EXISTS rejections(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL UNIQUE,
          subject TEXT NOT NULL,
          payload_key TEXT NOT NULL,
          reason TEXT NOT NULL,
          review_state TEXT NOT NULL DEFAULT '待复核',
          created_at TEXT NOT NULL
        );
        -- 同载荷并发拒收：后到者只登记拒收争用，不再落拒收
        CREATE TABLE IF NOT EXISTS reject_contention(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          payload_key TEXT NOT NULL,
          subject TEXT NOT NULL,
          existing_batch_id INTEGER NOT NULL,
          created_at TEXT NOT NULL
        );
        -- 拒收同败簿：甲/乙路径当天选定后禁止改口
        CREATE TABLE IF NOT EXISTS reject_policy_ledger(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          path TEXT NOT NULL,
          locked_day TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        -- 拒收偏了：偏离同败簿的半截拒收全部拦在这本账上
        CREATE TABLE IF NOT EXISTS reject_deviations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          ref_id INTEGER,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        # 既有库轻量迁移：批次表补载荷指纹列，争用判据不依赖流水表是否完好
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(ingress_batches)").fetchall()}
        if "payload_key" not in cols:
            conn.execute("ALTER TABLE ingress_batches ADD COLUMN payload_key TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reject_key ON ingress_batches(payload_key, result)")
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))
        # 沿用既有库文件：为历史拒收批次补原因流水与载荷指纹，避免老数据天然分裂
        legacy = conn.execute("""
            SELECT i.id, i.subject, i.payload_fields, i.detail, i.created_at
            FROM ingress_batches i
            LEFT JOIN rejections r ON r.batch_id = i.id
            WHERE i.result='拒收' AND r.id IS NULL""").fetchall()
        for row in legacy:
            key = payload_key(row["subject"], row["payload_fields"] or "")
            conn.execute("UPDATE ingress_batches SET payload_key=? WHERE id=?", (key, row["id"]))
            conn.execute("""INSERT INTO rejections(batch_id, subject, payload_key, reason, review_state, created_at)
                            VALUES (?,?,?,?,?,?)""",
                         (row["id"], row["subject"], key, row["detail"] or "历史拒收补登", "待复核", row["created_at"]))
        # 同败簿默认钉死甲路径（拒收批次与原因流水同一事务，失败整笔回滚）
        if conn.execute("SELECT COUNT(*) c FROM reject_policy_ledger").fetchone()["c"] == 0:
            conn.execute("INSERT INTO reject_policy_ledger(path, locked_day, created_at) VALUES (?,?,?)",
                         ("甲", date.today().isoformat(), datetime.now().isoformat(timespec="seconds")))

def _record_deviation(conn, kind: str, ref_id, detail: str):
    conn.execute("INSERT INTO reject_deviations(kind, ref_id, detail, created_at) VALUES (?,?,?,?)",
                 (kind, ref_id, detail, datetime.now().isoformat(timespec="seconds")))

def _audit_rejections(conn):
    """扫描半截拒收：有批次无流水 / 有流水无批次，逐项记拒收偏了。"""
    found = []
    no_flow = conn.execute("""
        SELECT i.id FROM ingress_batches i
        LEFT JOIN rejections r ON r.batch_id=i.id
        WHERE i.result='拒收' AND r.id IS NULL""").fetchall()
    for row in no_flow:
        _record_deviation(conn, "有批次无流水", row["id"], f"拒收批次 #{row['id']} 缺原因流水")
        found.append({"kind": "有批次无流水", "ref_id": row["id"]})
    no_batch = conn.execute("""
        SELECT r.id, r.batch_id FROM rejections r
        LEFT JOIN ingress_batches i ON i.id=r.batch_id
        WHERE i.id IS NULL OR i.result<>'拒收'""").fetchall()
    for row in no_batch:
        _record_deviation(conn, "有流水无批次", row["id"], f"原因流水 #{row['id']} 挂空批次 {row['batch_id']}")
        found.append({"kind": "有流水无批次", "ref_id": row["id"]})
    return found

@app.on_event("startup")
def _s():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    now = datetime.now().isoformat(timespec="seconds")
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        if missing:
            reason = "缺字段:" + ",".join(missing)
            if ctr["mode"] == "兼容":
                cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "缓冲", reason, now))
                c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (cur.lastrowid, "缺字段进双写缓冲"))
                return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
            return _reject_atomic(c, body, ctr["version"], reason, now, missing)
        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
        return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid}

def _reject_atomic(c, body: IngressIn, version: str, reason: str, now: str, missing):
    """甲路径：拒收批次与原因流水同一事务，失败整笔回滚；同载荷后到只记争用。"""
    key = payload_key(body.subject, body.payload_fields)
    with _REJECT_LOCK:
        gate = _test_reject_gate
        if gate is not None:
            entered, release = gate
            entered.set()
            release.wait(5)
        try:
            c.execute("BEGIN IMMEDIATE")
            # 争用判据以批次表载荷指纹为准：即便原因流水被破坏，同载荷后到仍只登记争用
            dup = c.execute("""SELECT id FROM ingress_batches
                               WHERE payload_key=? AND result='拒收'
                               ORDER BY id LIMIT 1""", (key,)).fetchone()
            if dup:
                # 同载荷两名值班同时抢拒收：后到排队，只登记拒收争用
                c.execute("INSERT INTO reject_contention(payload_key, subject, existing_batch_id, created_at) VALUES (?,?,?,?)",
                          (key, body.subject, dup["id"], now))
                c.commit()
                return {"result": "拒收争用", "version": version, "missing": missing,
                        "batch_id": dup["id"], "contention": True}
            _maybe_fail("INSERT INTO INGRESS_BATCHES")
            cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,payload_key) VALUES (?,?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, version, "拒收", reason, now, key))
            bid = cur.lastrowid
            # 原因流水与批次同一事务：任一写失败都由外层回滚，杜绝半截拒收
            _maybe_fail("INSERT INTO REJECTIONS")
            c.execute("""INSERT INTO rejections(batch_id, subject, payload_key, reason, review_state, created_at)
                         VALUES (?,?,?,?,?,?)""",
                      (bid, body.subject, key, reason, "待复核", now))
            c.commit()
            return {"result": "拒收", "version": version, "missing": missing, "batch_id": bid}
        except Exception:
            c.rollback()
            raise HTTPException(500, "拒收同败失败，整笔已回滚")

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}

# ---------- 拒收同败：详情 / 复核队列 / 争用 / 策略附页 / 审计 ----------

@app.get("/api/rejections")
def rejections(authorization: str | None = Header(default=None)):
    """拒收详情：与入站列表一一对齐，页序按批次 id 升序可核。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT r.*, i.result AS batch_result, i.contract_version, i.detail AS batch_detail
                            FROM rejections r JOIN ingress_batches i ON i.id=r.batch_id
                            ORDER BY r.batch_id ASC""").fetchall()
        return {"items": [dict(r) for r in rows]}

@app.get("/api/rejections/review-queue")
def review_queue(authorization: str | None = Header(default=None)):
    """复核队列：同败可查，拒收件只在待复核/已复核之间流转，永不标通过。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT r.*, i.result AS batch_result FROM rejections r
                            JOIN ingress_batches i ON i.id=r.batch_id
                            ORDER BY r.batch_id ASC""").fetchall()
        return {"items": [dict(r) for r in rows]}

class ReviewIn(BaseModel):
    verdict: str

@app.post("/api/rejections/{batch_id}/review")
def review_one(batch_id: int, body: ReviewIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    if body.verdict == "通过":
        raise HTTPException(400, "拒收件复核不得改判通过")
    with connect() as c:
        row = c.execute("SELECT id FROM rejections WHERE batch_id=?", (batch_id,)).fetchone()
        if not row:
            raise HTTPException(404, "拒收流水不存在")
        c.execute("UPDATE rejections SET review_state='已复核' WHERE batch_id=?", (batch_id,))
    return {"ok": True, "batch_id": batch_id, "review_state": "已复核"}

@app.get("/api/reject-contention")
def contention(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM reject_contention ORDER BY id ASC").fetchall()
        return {"items": [dict(r) for r in rows]}

class PolicyIn(BaseModel):
    path: str  # 甲：同一事务；乙：草稿确认后原子补原因（草稿不得标通过）

@app.post("/api/reject-policy")
def set_policy(body: PolicyIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    if body.path not in ("甲", "乙"):
        raise HTTPException(400, "路径只许填 甲 或 乙")
    today = date.today().isoformat()
    with connect() as c:
        row = c.execute("SELECT * FROM reject_policy_ledger ORDER BY id DESC LIMIT 1").fetchone()
        if row and row["locked_day"] == today:
            if row["path"] != body.path:
                # 偏离先落账提交，再拦请求，避免被异常回滚带走
                _record_deviation(c, "同败簿改口", row["id"], f"当天试图由 {row['path']} 改为 {body.path}，已拦")
                c.commit()
                raise HTTPException(409, "拒收同败簿当天禁止改口")
            return {"ok": True, "path": body.path, "locked_day": today, "idempotent": True}
        c.execute("INSERT INTO reject_policy_ledger(path, locked_day, created_at) VALUES (?,?,?)",
                  (body.path, today, datetime.now().isoformat(timespec="seconds")))
    return {"ok": True, "path": body.path, "locked_day": today}

@app.get("/api/reject-policy")
def get_policy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM reject_policy_ledger ORDER BY id DESC LIMIT 1").fetchone()
        today = date.today().isoformat()
        return {"path": row["path"], "locked_day": row["locked_day"],
                "locked": row["locked_day"] == today}

@app.get("/api/reject-policy/appendix")
def policy_appendix(authorization: str | None = Header(default=None)):
    """策略附页：同败簿路径 + 每笔拒收同败记录，页序与拒收详情一致。"""
    require_user(authorization)
    with connect() as c:
        ledger = c.execute("SELECT * FROM reject_policy_ledger ORDER BY id DESC LIMIT 1").fetchone()
        rows = c.execute("""SELECT r.batch_id, r.subject, r.payload_key, r.reason, r.review_state, r.created_at
                            FROM rejections r JOIN ingress_batches i ON i.id=r.batch_id
                            ORDER BY r.batch_id ASC""").fetchall()
        return {"policy": dict(ledger), "items": [dict(r) for r in rows]}

@app.get("/api/reject-audit")
def reject_audit(authorization: str | None = Header(default=None)):
    """对账账册的拒收专页：扫出半截拒收并记拒收偏了。"""
    require_user(authorization)
    with connect() as c:
        found = _audit_rejections(c)
        rows = c.execute("SELECT * FROM reject_deviations ORDER BY id DESC").fetchall()
        return {"deviations": [dict(r) for r in rows], "live_findings": found,
                "clean": len(found) == 0}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        rej_rows = c.execute("SELECT COUNT(*) c FROM rejections").fetchone()["c"]
        cont_rows = c.execute("SELECT COUNT(*) c FROM reject_contention").fetchone()["c"]
        live = _audit_rejections(c)
    return {"ingress_total": total, "passed": passed, "buffered": buffered, "rejected": rejected,
            "buffer_rows": buf_rows, "aligned": buffered == buf_rows,
            "reject_rows": rej_rows, "reject_aligned": rejected == rej_rows and len(live) == 0,
            "reject_contention": cont_rows}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
