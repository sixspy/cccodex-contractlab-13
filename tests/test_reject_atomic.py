"""拒收同败失败测：拒收批次与拒收原因流水同成同败，不得出现半截拒收。

红线流程：本文件在修复前必须全部失败（红），修复后转绿。
禁止只改拒收文案让测试通过——所有断言都落库核验。
"""
import threading

import pytest
from fastapi.testclient import TestClient

import app
from app import app as fastapi_app, init_db, connect


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr(app, "DB_PATH", tmp_path / "reject.db")
    init_db()
    yield


def _client():
    c = TestClient(fastapi_app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def _reject(h, payload="order_id", subject="订单入站"):
    return TestClient(fastapi_app).post(
        "/api/ingress/validate", headers=h,
        json={"subject": subject, "payload_fields": payload})


# ---------- 同败 1：拒收必须同时有批次和原因流水，且为同一事务 ----------

def test_reject_writes_batch_and_reason_flow_together():
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id"})
    assert r.status_code == 200 and r.json()["result"] == "拒收"
    bid = r.json()["batch_id"]
    with connect() as conn:
        batch = conn.execute("SELECT result FROM ingress_batches WHERE id=?", (bid,)).fetchone()
        flow = conn.execute("SELECT * FROM rejections WHERE batch_id=?", (bid,)).fetchone()
    assert batch is not None and batch["result"] == "拒收"
    assert flow is not None, "拒收原因流水必须与批次同笔落库"
    assert flow["reason"], "原因流水不得为空"


def test_half_reject_rolls_back_when_flow_insert_fails(monkeypatch):
    """流水写入故障 → 整笔回滚：禁止有批次无流水。"""
    c, h = _client()
    monkeypatch.setattr(app, "_fail_before_sql", "INSERT INTO REJECTIONS")
    monkeypatch.setattr(app, "_fail_with", app.sqlite3.OperationalError("模拟原因流水落库失败"))
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id"})
    assert r.status_code == 500
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM rejections").fetchone()["c"] == 0


def test_half_reject_rolls_back_when_batch_insert_fails(monkeypatch):
    """批次写入故障 → 整笔回滚：禁止有流水无批次。"""
    c, h = _client()
    monkeypatch.setattr(app, "_fail_before_sql", "INSERT INTO INGRESS_BATCHES")
    monkeypatch.setattr(app, "_fail_with", app.sqlite3.OperationalError("模拟拒收批次落库失败"))
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id"})
    assert r.status_code == 500
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM rejections").fetchone()["c"] == 0


def test_reject_must_not_advance_pass_counter():
    """拒收不得推进通过计数。"""
    c, h = _client()
    for pf in ("order_id", "amount", "currency"):  # 不同载荷，避免争用登记
        r = c.post("/api/ingress/validate", headers=h,
                   json={"subject": "订单入站", "payload_fields": pf})
        assert r.json()["result"] == "拒收"
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["passed"] == 0
    assert recon["rejected"] == 3
    assert recon["reject_aligned"] is True


# ---------- 同败 2：同载荷并发拒收排队，后到只登记拒收争用 ----------

def test_same_payload_concurrent_reject_queues_and_records_contention():
    c, h = _client()
    entered = threading.Event()
    release = threading.Event()
    app._test_reject_gate = (entered, release)
    results = []

    def worker():
        r = _reject(h)
        results.append((r.status_code, r.json()))

    t1 = threading.Thread(target=worker)
    t1.start()
    assert entered.wait(2), "先到请求未进入拒收临界区"
    t2 = threading.Thread(target=worker)
    t2.start()  # 同载荷后到，须排队等锁，不得插单
    release.set()
    t1.join(5)
    t2.join(5)
    app._test_reject_gate = None

    assert len(results) == 2
    with connect() as conn:
        n_batch = conn.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        n_flow = conn.execute("SELECT COUNT(*) c FROM rejections").fetchone()["c"]
        n_cont = conn.execute("SELECT COUNT(*) c FROM reject_contention").fetchone()["c"]
        keys = {r["payload_key"] for r in conn.execute("SELECT DISTINCT payload_key FROM reject_contention")}
    assert n_batch == 1 and n_flow == 1, "同载荷只许一笔拒收，批次与流水各一"
    assert n_cont == 1, "后到只登记拒收争用，不得再落拒收"
    assert len(keys) == 1


def test_different_payloads_do_not_contend():
    c, h = _client()
    r1 = c.post("/api/ingress/validate", headers=h,
                json={"subject": "订单入站", "payload_fields": "order_id"})
    r2 = c.post("/api/ingress/validate", headers=h,
                json={"subject": "订单入站", "payload_fields": "amount"})
    assert r1.json()["result"] == "拒收" and r2.json()["result"] == "拒收"
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM reject_contention").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) c FROM rejections").fetchone()["c"] == 2


# ---------- 同败 3：拒收同败簿（甲/乙路径选定后当天禁止改口） ----------

def test_policy_ledger_locks_path_for_the_day():
    c, h = _client()
    r1 = c.post("/api/reject-policy", headers=h, json={"path": "甲"})
    assert r1.status_code == 200 and r1.json()["path"] == "甲"
    # 同路径当天重复确认属幂等，不新增账簿行
    r1b = c.post("/api/reject-policy", headers=h, json={"path": "甲"})
    assert r1b.status_code == 200
    # 当天改口（甲→乙）必须拦下
    r2 = c.post("/api/reject-policy", headers=h, json={"path": "乙"})
    assert r2.status_code == 409  # 当天禁止改口
    ledger = c.get("/api/reject-policy", headers=h).json()
    assert ledger["path"] == "甲" and ledger["locked"] is True
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM reject_policy_ledger").fetchone()["c"] == 1
        assert any(r["kind"] == "同败簿改口" for r in conn.execute("SELECT kind FROM reject_deviations"))


def test_reject_never_lands_in_pass_or_draft_state():
    """甲路径下拒收只落拒收态；草稿不得标通过。"""
    c, h = _client()
    c.post("/api/reject-policy", headers=h, json={"path": "甲"})
    c.post("/api/ingress/validate", headers=h,
           json={"subject": "订单入站", "payload_fields": "order_id"})
    with connect() as conn:
        states = {r["result"] for r in conn.execute("SELECT DISTINCT result FROM ingress_batches")}
        review_states = {r["review_state"] for r in conn.execute("SELECT DISTINCT review_state FROM rejections")}
    assert "通过" not in states
    assert "草稿" not in review_states and "通过" not in review_states


# ---------- 同败 4：五处可查 + 页序 + 偏离拦截 ----------

def test_reject_details_align_with_ingress_list_and_are_ordered():
    """拒收详情与入站列表对得齐，拒收页序可核（升序页码即 id 升序）。"""
    c, h = _client()
    ids = []
    for pf in ("order_id", "amount", "currency"):
        r = c.post("/api/ingress/validate", headers=h,
                   json={"subject": "订单入站", "payload_fields": pf})
        ids.append(r.json()["batch_id"])
    ingress = {b["id"] for b in c.get("/api/ingress", headers=h).json()["items"] if b["result"] == "拒收"}
    details = c.get("/api/rejections", headers=h).json()["items"]
    detail_bids = [d["batch_id"] for d in details]
    assert set(detail_bids) == set(ids) == ingress
    assert detail_bids == sorted(detail_bids), "拒收页序寅可核：按批次 id 升序"
    for d in details:
        assert d["reason"] and d["payload_key"]


def test_review_queue_and_appendix_show_same_failure_records():
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id"})
    bid = r.json()["batch_id"]
    queue = c.get("/api/rejections/review-queue", headers=h).json()["items"]
    appendix = c.get("/api/reject-policy/appendix", headers=h).json()["items"]
    assert {q["batch_id"] for q in queue} == {bid}
    assert {a["batch_id"] for a in appendix} == {bid}
    # 复核动作不得把拒收改成通过
    rr = c.post(f"/api/rejections/{bid}/review", headers=h, json={"verdict": "通过"})
    assert rr.status_code in (400, 409)


def test_audit_flags_split_rejections_as_deviation(tmp_path, monkeypatch):
    """审计：半截拒收（有流水无批次 / 有批次无流水）须拦下并记拒收偏了。"""
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id"})
    bid = r.json()["batch_id"]
    # 制造有批次无流水
    with connect() as conn:
        conn.execute("DELETE FROM rejections WHERE batch_id=?", (bid,))
    audit = c.get("/api/reject-audit", headers=h).json()
    kinds = {d["kind"] for d in audit["deviations"]}
    assert "有批次无流水" in kinds
    with connect() as conn:
        logged = conn.execute("SELECT kind FROM reject_deviations").fetchall()
    assert any(row["kind"] == "有批次无流水" for row in logged)
