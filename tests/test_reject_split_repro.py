"""拒收分裂复现：分别投入“有流水无批次”与“有批次无流水”。

本文件在修复前仅断言分裂事实确实能被制造出来（见底）；
修复后追加断言：新的同败审计能把这两种半截拒收全部拦下并记拒收偏了。
"""
import sqlite3

from fastapi.testclient import TestClient

import app
from app import app as fastapi_app, init_db, connect


def _client():
    c = TestClient(fastapi_app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def _has_table(name: str) -> bool:
    with connect() as conn:
        return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _raw(sql, params=()):
    conn = sqlite3.connect(app.DB_PATH)  # 动态取路径，跟随 monkeypatch
    try:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def test_repro_flow_without_batch(tmp_path, monkeypatch):
    """有流水无批次：拒收原因流水挂在一个不存在的批次上。"""
    monkeypatch.setattr(app, "DB_PATH", tmp_path / "split.db")
    init_db()
    c, h = _client()
    # 先正常走一笔拒收，拿到批次后删掉批次，制造“只剩流水”的分裂
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "order_id"})
    assert r.status_code == 200 and r.json()["result"] == "拒收"
    bid = r.json()["batch_id"]

    if _has_table("rejections"):
        _raw("DELETE FROM rejections WHERE batch_id=?", (bid,))
        _raw("INSERT INTO rejections(batch_id, subject, payload_key, reason, review_state, created_at) "
             "SELECT id, subject, '', detail, '待复核', created_at FROM ingress_batches WHERE id=?", (bid,))
    else:
        _raw("INSERT INTO buffers(batch_id, reason) VALUES (?,?)", (bid, "拒收原因流水-无批次"))
    _raw("DELETE FROM ingress_batches WHERE id=?", (bid,))

    flow_table = "rejections" if _has_table("rejections") else "buffers"
    with connect() as conn:
        flow_n = conn.execute(f"SELECT COUNT(*) c FROM {flow_table}").fetchone()["c"]
        batch_n = conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
    assert flow_n == 1 and batch_n == 0  # 分裂见底：有流水无批次


def test_repro_batch_without_flow(tmp_path, monkeypatch):
    """有批次无流水：拒收批次在，拒收原因流水被抹掉。"""
    monkeypatch.setattr(app, "DB_PATH", tmp_path / "split2.db")
    init_db()
    c, h = _client()
    r = c.post("/api/ingress/validate", headers=h,
               json={"subject": "订单入站", "payload_fields": "amount"})
    assert r.status_code == 200 and r.json()["result"] == "拒收"
    bid = r.json()["batch_id"]

    flow_table = "rejections" if _has_table("rejections") else "buffers"
    _raw(f"DELETE FROM {flow_table} WHERE batch_id=?", (bid,))

    with connect() as conn:
        batch = conn.execute("SELECT result FROM ingress_batches WHERE id=?", (bid,)).fetchone()
        flow_n = conn.execute(f"SELECT COUNT(*) c FROM {flow_table} WHERE batch_id=?", (bid,)).fetchone()["c"]
    assert batch["result"] == "拒收" and flow_n == 0  # 分裂见底：有批次无流水
