"""知识链接领域服务：管理实体与外部知识的链接关系。

缺陷修复：此前校验回执只写展示字段「验证状态」，规范状态 status 不流转，
导致详情页、批量校验、聚合视图三方结论对不上。现以生命周期事件台账为
唯一事实来源：验证回执 / 失效请求 / 移除请求按「提交 → 确认」两段式入账，
由已确认事件推导实际有效区间；聚合视图消费核对数据集并把采纳结论回写
链接验证台账，保证各入口看到同一份结论。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


LINK_TYPES = ["wiki", "dbpedia", "knowledge_panel", "authority_page", "linkedin"]

# 生命周期事件类型
EVENT_VALIDATION = "验证回执"
EVENT_INVALIDATION = "失效请求"
EVENT_REMOVAL = "移除请求"

# 事件状态
EVENT_PENDING = "待确认"
EVENT_CONFIRMED = "已确认"

# 生命周期结论（与 STATUS_ORDER 对齐，回写台账时直接采纳）
CONCLUSION_PENDING = "待验证"
CONCLUSION_VALID = "已验证"
CONCLUSION_INVALID = "已失效"
CONCLUSION_REMOVED = "已移除"

# 历史记录里「仅写有效标记」的取值
LEGACY_VALID_MARKS = {"有效", "已验证", "验证通过"}
LEGACY_INVALID_MARKS = {"已失效", "无效", "失效"}

# 事件台账的互斥锁：并发提交/确认时保证去重与序号分配的原子性
LIFECYCLE_LOCK = threading.RLock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class KnowledgeLinkService(BaseModuleService):
    MODULE = "knowledge_link"
    ENTITY = "链接记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["链接ID", "源实体", "外部URI", "链接类型", "外部来源", "验证状态", "上次验证"]
    REQUIRED_FIELDS = ["链接ID", "源实体", "外部URI"]
    STATUS_ORDER = ["待验证", "已验证", "已失效", "已移除"]
    ACTION_RULES = {"验证链接": "待验证", "标记失效": "已验证", "移除链接": "已验证"}
    NEGATIVE_ACTIONS = []

    # ---------- 生命周期事件台账 ----------

    def _state(self) -> dict[str, Any]:
        state = store.module_state(self.MODULE)
        state.setdefault("seq", 0)
        state.setdefault("events", [])
        return state

    def _tick(self) -> int:
        state = self._state()
        state["seq"] = int(state.get("seq", 0)) + 1
        return state["seq"]

    def _events(self, link_id: int | None = None) -> list[dict[str, Any]]:
        events = self._state()["events"]
        if link_id is None:
            return list(events)
        return [e for e in events if e["link_id"] == int(link_id)]

    def _register_event(
        self, link_id: int, kind: str, receipt_id: str | None = None, note: str = ""
    ) -> dict[str, Any]:
        seq = self._tick()
        event = {
            "event_id": f"ev-{seq}",
            "link_id": int(link_id),
            "kind": kind,
            "receipt_id": receipt_id,
            "state": EVENT_PENDING,
            "submitted_seq": seq,
            "submitted_at": _now_iso(),
            "confirmed_seq": None,
            "confirmed_at": None,
            "note": note,
            "disposition": None,
        }
        self._state()["events"].append(event)
        return event

    # ---------- 生命周期推导 ----------

    def _legacy_conclusion(
        self,
        row: dict[str, Any],
        intervals: list[dict[str, Any]],
        evidence: list[str],
    ) -> str:
        """兼容历史仅写有效标记的记录：无生命周期事件时以现有标记为证据。"""
        status = str(row.get("status") or "")
        mark = str(row.get("验证状态") or "")
        if status == CONCLUSION_REMOVED:
            evidence.append("台账状态为已移除（历史记录，无生命周期事件）")
            return CONCLUSION_REMOVED
        if status == CONCLUSION_INVALID:
            evidence.append("台账状态为已失效（历史记录，无生命周期事件）")
            return CONCLUSION_INVALID
        if status == CONCLUSION_VALID or mark in LEGACY_VALID_MARKS:
            intervals.append({
                "valid_from": row.get("上次验证") or None,
                "valid_from_seq": None,
                "valid_until": None,
                "valid_until_seq": None,
                "source": "历史有效标记",
                "source_event": None,
                "receipt_id": None,
                "ended_by": None,
            })
            evidence.append("兼容历史仅写有效标记的记录：按现有标记判定有效")
            return CONCLUSION_VALID
        evidence.append("无已确认事件且现有标记未表明有效，维持待验证")
        return CONCLUSION_PENDING

    def _derive_lifecycle(self, row: dict[str, Any]) -> dict[str, Any]:
        """按已确认事件推导实际生命周期结论与有效区间。

        确认顺序决定区间：验证回执确认开启区间；失效/移除确认结束当前开放
        区间并进入终态——此后到达的回执一律被覆盖，不能令链接重新生效。
        """
        link_id = int(row.get("id", 0))
        events = self._events(link_id)
        confirmed = sorted(
            (e for e in events if e["state"] == EVENT_CONFIRMED),
            key=lambda e: e["confirmed_seq"],
        )
        pending_ops = [e for e in events if e["state"] == EVENT_PENDING]

        intervals: list[dict[str, Any]] = []
        open_interval: dict[str, Any] | None = None
        invalidated = False
        removed = False
        for event in confirmed:
            kind = event["kind"]
            if kind == EVENT_VALIDATION:
                if invalidated or removed:
                    continue  # 失效/移除处置已确认，回执不再生效
                if open_interval is None:
                    open_interval = {
                        "valid_from": event["confirmed_at"],
                        "valid_from_seq": event["confirmed_seq"],
                        "valid_until": None,
                        "valid_until_seq": None,
                        "source": EVENT_VALIDATION,
                        "source_event": event["event_id"],
                        "receipt_id": event.get("receipt_id"),
                        "ended_by": None,
                    }
                    intervals.append(open_interval)
                # 已处于有效区间：重复确认不叠加
            else:
                if open_interval is not None:
                    open_interval["valid_until"] = event["confirmed_at"]
                    open_interval["valid_until_seq"] = event["confirmed_seq"]
                    open_interval["ended_by"] = event["event_id"]
                    open_interval = None
                invalidated = True
                if kind == EVENT_REMOVAL:
                    removed = True

        evidence: list[str] = []
        if removed:
            conclusion = CONCLUSION_REMOVED
            evidence.append("移除请求已确认，链接已移除")
        elif invalidated:
            conclusion = CONCLUSION_INVALID
            evidence.append("失效请求已确认，有效区间已终结")
        elif open_interval is not None:
            conclusion = CONCLUSION_VALID
            evidence.append(f"验证回执 {open_interval['source_event']} 确认后进入有效区间")
        elif not confirmed:
            conclusion = self._legacy_conclusion(row, intervals, evidence)
        else:
            conclusion = CONCLUSION_PENDING
            evidence.append("无开放有效区间，维持待验证")

        blocked_by = [
            f"{e['kind']} {e['event_id']} 待确认" for e in pending_ops
        ]
        if conclusion == CONCLUSION_INVALID:
            blocked_by.append("失效处置已确认")
        elif conclusion == CONCLUSION_REMOVED:
            blocked_by.append("移除处置已确认")
        elif conclusion == CONCLUSION_PENDING and not pending_ops:
            blocked_by.append("无已确认的验证回执")

        return {
            "link_id": link_id,
            "conclusion": conclusion,
            "current_valid": conclusion == CONCLUSION_VALID,
            "intervals": intervals,
            "open_interval": open_interval,
            "pending_ops": [
                {
                    "event_id": e["event_id"],
                    "kind": e["kind"],
                    "receipt_id": e.get("receipt_id"),
                    "submitted_at": e["submitted_at"],
                    "submitted_seq": e["submitted_seq"],
                }
                for e in pending_ops
            ],
            "blocked_by": blocked_by,
            "evidence": evidence,
        }

    @staticmethod
    def _mark_consistent(row: dict[str, Any], conclusion: str) -> bool:
        """对照原验证标记与实际生命周期结论是否一致。"""
        mark = str(row.get("验证状态") or "")
        if mark in LEGACY_VALID_MARKS:
            return conclusion == CONCLUSION_VALID
        if mark in LEGACY_INVALID_MARKS:
            return conclusion == CONCLUSION_INVALID
        return True  # 无明确标记时不构成冲突

    # ---------- 核对数据集与台账回写 ----------

    def build_validity_check_dataset(self) -> dict[str, Any]:
        """生成知识链接有效性核对数据集：逐条对照现有标记与实际生命周期结论。

        有效链接清单与汇总条数同源于 records，天然保持一致。
        """
        with LIFECYCLE_LOCK:
            records = []
            for row in store.rows(self.MODULE):
                lifecycle = self._derive_lifecycle(row)
                conclusion = lifecycle["conclusion"]
                records.append({
                    "link_id": lifecycle["link_id"],
                    "链接ID": row.get("链接ID"),
                    "源实体": row.get("源实体"),
                    "台账状态": row.get("status"),
                    "原验证标记": row.get("验证状态"),
                    "实际结论": conclusion,
                    "当前是否有效": lifecycle["current_valid"],
                    "有效区间": lifecycle["intervals"],
                    "待确认操作": lifecycle["pending_ops"],
                    "阻止进入当轮数据": lifecycle["blocked_by"],
                    "台账与结论一致": row.get("status") == conclusion,
                    "标记与结论一致": self._mark_consistent(row, conclusion),
                    "结论依据": lifecycle["evidence"],
                })
            valid_links = [r["link_id"] for r in records if r["当前是否有效"]]
            summary = {
                "total": len(records),
                "valid": len(valid_links),
                "invalid": sum(1 for r in records if r["实际结论"] == CONCLUSION_INVALID),
                "pending": sum(1 for r in records if r["实际结论"] == CONCLUSION_PENDING),
                "removed": sum(1 for r in records if r["实际结论"] == CONCLUSION_REMOVED),
            }
            return {
                "dataset": "知识链接有效性核对数据集",
                "module": self.MODULE,
                "as_of_seq": self._state().get("seq", 0),
                "generated_at": _now_iso(),
                "records": records,
                "valid_links": valid_links,  # 有效链接清单
                "summary": summary,          # 汇总与清单同源，条数必然一致
            }

    def _writeback_row(self, row: dict[str, Any], lifecycle: dict[str, Any]) -> None:
        """把单条链接的生命周期结论回写到链接验证台账。"""
        conclusion = lifecycle["conclusion"]
        row["status"] = conclusion
        row["pending"] = conclusion == CONCLUSION_PENDING
        if conclusion == CONCLUSION_VALID and lifecycle["intervals"]:
            latest = lifecycle["intervals"][-1]
            if latest.get("valid_from"):
                row["上次验证"] = latest["valid_from"]

    def apply_validity_conclusions(
        self, dataset: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """把核对数据集的采纳结论回写链接验证台账（规范状态 status）。"""
        with LIFECYCLE_LOCK:
            dataset = dataset or self.build_validity_check_dataset()
            changes = []
            for record in dataset["records"]:
                row = store.find(self.MODULE, int(record["link_id"]))
                if row is None:
                    continue
                before = row.get("status")
                self._writeback_row(row, self._derive_lifecycle(row))
                if before != row.get("status"):
                    changes.append({
                        "link_id": record["link_id"],
                        "from": before,
                        "to": row.get("status"),
                    })
            return {"ok": True, "written_back": len(changes), "changes": changes}

    # ---------- 操作提交（未确认） ----------

    def submit_validation_receipt(
        self, link_id: int, receipt_id: str | None = None
    ) -> dict[str, Any]:
        """登记验证成功回执（提交，未确认）。

        同一回执编号并发提交只算一次；已完成失效/移除处置的链接不再受理。
        """
        with LIFECYCLE_LOCK:
            row = store.find(self.MODULE, int(link_id))
            if row is None:
                return {"ok": False, "reason": f"链接记录 {link_id} 不存在或已归档"}
            if receipt_id:
                for event in self._events(int(link_id)):
                    if event["kind"] == EVENT_VALIDATION and event.get("receipt_id") == receipt_id:
                        return {
                            "ok": True,
                            "deduplicated": True,
                            "event": event,
                            "message": "同一验证回执已登记，并发提交只算一次",
                            "当前结论": self._derive_lifecycle(row)["conclusion"],
                        }
            lifecycle = self._derive_lifecycle(row)
            if lifecycle["conclusion"] in (CONCLUSION_INVALID, CONCLUSION_REMOVED):
                return {
                    "ok": False,
                    "rejected": True,
                    "reason": f"链接已完成{lifecycle['conclusion']}处置，验证回执不再生效",
                    "当前结论": lifecycle["conclusion"],
                }
            receipt_id = receipt_id or f"rcpt-{link_id}-{self._state().get('seq', 0) + 1}"
            event = self._register_event(int(link_id), EVENT_VALIDATION, receipt_id=receipt_id)
            return {"ok": True, "deduplicated": False, "event": event, "message": "验证回执已提交，待确认"}

    def submit_invalidation_request(self, link_id: int, reason: str = "") -> dict[str, Any]:
        """登记标记失效请求（提交，未确认）；同一链接的在途请求只登记一次。"""
        with LIFECYCLE_LOCK:
            row = store.find(self.MODULE, int(link_id))
            if row is None:
                return {"ok": False, "reason": f"链接记录 {link_id} 不存在或已归档"}
            lifecycle = self._derive_lifecycle(row)
            if lifecycle["conclusion"] == CONCLUSION_REMOVED:
                return {
                    "ok": False,
                    "rejected": True,
                    "reason": "链接已移除，失效请求不再受理",
                    "当前结论": CONCLUSION_REMOVED,
                }
            if lifecycle["conclusion"] == CONCLUSION_INVALID:
                return {
                    "ok": True,
                    "deduplicated": True,
                    "event": None,
                    "message": "失效处置已确认，无需重复提交",
                    "当前结论": CONCLUSION_INVALID,
                }
            for event in self._events(int(link_id)):
                if event["kind"] == EVENT_INVALIDATION and event["state"] == EVENT_PENDING:
                    return {"ok": True, "deduplicated": True, "event": event,
                            "message": "失效请求已在途，只登记一次"}
            event = self._register_event(int(link_id), EVENT_INVALIDATION, note=reason)
            return {"ok": True, "deduplicated": False, "event": event, "message": "失效请求已提交，待确认"}

    def submit_removal_request(self, link_id: int, reason: str = "") -> dict[str, Any]:
        """登记移除请求（提交，未确认）。"""
        with LIFECYCLE_LOCK:
            row = store.find(self.MODULE, int(link_id))
            if row is None:
                return {"ok": False, "reason": f"链接记录 {link_id} 不存在或已归档"}
            lifecycle = self._derive_lifecycle(row)
            if lifecycle["conclusion"] == CONCLUSION_REMOVED:
                return {"ok": True, "deduplicated": True, "event": None,
                        "message": "链接已移除", "当前结论": CONCLUSION_REMOVED}
            for event in self._events(int(link_id)):
                if event["kind"] == EVENT_REMOVAL and event["state"] == EVENT_PENDING:
                    return {"ok": True, "deduplicated": True, "event": event,
                            "message": "移除请求已在途，只登记一次"}
            event = self._register_event(int(link_id), EVENT_REMOVAL, note=reason)
            return {"ok": True, "deduplicated": False, "event": event, "message": "移除请求已提交，待确认"}

    # ---------- 操作确认 ----------

    def confirm_link_event(self, link_id: int, event_id: str) -> dict[str, Any]:
        """确认一条已提交的生命周期事件，并按时间证据核对它开启/结束的区间。"""
        with LIFECYCLE_LOCK:
            row = store.find(self.MODULE, int(link_id))
            if row is None:
                return {"ok": False, "reason": f"链接记录 {link_id} 不存在或已归档"}
            event = next(
                (e for e in self._events(int(link_id)) if e["event_id"] == event_id), None
            )
            if event is None:
                return {"ok": False, "reason": f"事件 {event_id} 不属于链接 {link_id}"}
            if event["state"] == EVENT_CONFIRMED:
                return {
                    "ok": True,
                    "deduplicated": True,
                    "event": event,
                    "message": "事件已确认过，重复确认只算一次",
                    "当前结论": self._derive_lifecycle(row)["conclusion"],
                }

            seq = self._tick()
            event["state"] = EVENT_CONFIRMED
            event["confirmed_seq"] = seq
            event["confirmed_at"] = _now_iso()
            after = self._derive_lifecycle(row)

            if event["kind"] == EVENT_VALIDATION:
                opened = after["open_interval"]
                if opened and opened.get("source_event") == event["event_id"]:
                    event["disposition"] = {
                        "结果": "开启有效区间",
                        "valid_from": opened["valid_from"],
                    }
                elif after["conclusion"] in (CONCLUSION_INVALID, CONCLUSION_REMOVED):
                    event["disposition"] = {"结果": "失效/移除处置已确认，回执被覆盖不再生效"}
                else:
                    event["disposition"] = {"结果": "链接已处于有效区间，重复确认不叠加"}
            else:
                ended = next(
                    (i for i in after["intervals"] if i.get("ended_by") == event["event_id"]),
                    None,
                )
                if ended is not None:
                    event["disposition"] = {
                        "结果": "结束有效区间",
                        "valid_from": ended["valid_from"],
                        "valid_until": ended["valid_until"],
                    }
                else:
                    event["disposition"] = {
                        "结果": "无开放区间可结束",
                        "说明": "确认时链接不在有效区间内",
                    }

            # 确认即回写台账规范状态，保证详情页与聚合视图一致
            self._writeback_row(row, after)
            return {
                "ok": True,
                "deduplicated": False,
                "event": event,
                "当前结论": after["conclusion"],
                "message": f"{event['kind']}已确认",
            }

    def confirm_all_pending(self, link_id: int) -> dict[str, Any]:
        """按提交顺序确认该链接全部在途事件（交错到达后的集中确认）。"""
        with LIFECYCLE_LOCK:
            pending = sorted(
                (e for e in self._events(int(link_id)) if e["state"] == EVENT_PENDING),
                key=lambda e: e["submitted_seq"],
            )
            results = [self.confirm_link_event(link_id, e["event_id"]) for e in pending]
            row = store.find(self.MODULE, int(link_id))
            conclusion = self._derive_lifecycle(row)["conclusion"] if row else None
            return {"ok": True, "confirmed": len(results), "results": results,
                    "当前结论": conclusion}

    # ---------- 通用动作入口（同步提交并确认） ----------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        row = store.find(self.MODULE, entry_id)
        if row is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        source = self.ACTION_RULES.get(action)
        if source is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        lifecycle = self._derive_lifecycle(row)
        if lifecycle["conclusion"] != source:
            return None, (
                f"链接实际结论为「{lifecycle['conclusion']}」，"
                f"不满足动作「{action}」的前置状态「{source}」"
            )
        if action == "验证链接":
            submitted = self.submit_validation_receipt(entry_id)
        elif action == "标记失效":
            submitted = self.submit_invalidation_request(entry_id)
        else:
            submitted = self.submit_removal_request(entry_id)
        if not submitted.get("ok"):
            return None, submitted.get("reason") or submitted.get("message", "提交失败")
        event = submitted.get("event")
        if event is None:  # 幂等情形：处置已完成
            return row, submitted.get("message", f"{self.ENTITY}已{action}")
        confirmed = self.confirm_link_event(entry_id, event["event_id"])
        if not confirmed.get("ok"):
            return None, confirmed.get("reason", "确认失败")
        return row, f"{self.ENTITY}已{action}，实际结论：{confirmed['当前结论']}"

    # ---------- 详情回查 ----------

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """详情回查：附带原验证标记与实际生命周期结论的对照。"""
        row = store.find(self.MODULE, entry_id)
        if row is None:
            return None
        lifecycle = self._derive_lifecycle(row)
        return {
            **row,
            "有效性核对": {
                "原验证标记": row.get("验证状态"),
                "台账状态": row.get("status"),
                "实际结论": lifecycle["conclusion"],
                "当前是否有效": lifecycle["current_valid"],
                "有效区间": lifecycle["intervals"],
                "待确认操作": lifecycle["pending_ops"],
                "标记与结论一致": self._mark_consistent(row, lifecycle["conclusion"]),
                "结论依据": lifecycle["evidence"],
            },
        }

    # ---------- 跨模块消费 ----------

    def consume_relations(self, relation_graph_edges: list[dict[str, Any]]) -> dict[str, Any]:
        """消费关系图谱的已验证边，为实体建立外部知识链接。"""
        if not relation_graph_edges:
            return {"ok": True, "link_count": 0, "links": []}

        links = []
        for edge in relation_graph_edges:
            entities = [edge.get("source_entity", ""), edge.get("target_entity", "")]
            for entity in entities:
                if not entity:
                    continue
                ehash = int(hashlib.md5(entity.encode()).hexdigest()[:4], 16)
                link_type = LINK_TYPES[ehash % len(LINK_TYPES)]
                link_id = hashlib.sha256(
                    f"{entity}|{link_type}|ext".encode()
                ).hexdigest()[:12]
                external_uri = f"https://{link_type}.example.com/entity/{entity}"
                links.append({
                    "链接ID": link_id,
                    "源实体": entity,
                    "外部URI": external_uri,
                    "链接类型": link_type,
                    "外部来源": "relation_graph_pipeline",
                    "验证状态": "自动生成",
                    "上次验证": "2026-11-19T10:00:00Z",
                })
        return {
            "ok": True,
            "input_edges": len(relation_graph_edges),
            "link_count": len(links),
            "links": links,
        }

    def build_aggregate_input(self) -> dict[str, Any]:
        """构建聚合视图的输入数据：消费核对数据集中的实际有效区间。"""
        dataset = self.build_validity_check_dataset()
        link_type_dist: dict[str, int] = {}
        for row in store.rows(self.MODULE):
            ltype = row.get("链接类型", "未知")
            link_type_dist[ltype] = link_type_dist.get(ltype, 0) + 1
        summary = dataset["summary"]
        return {
            "total_links": summary["total"],
            "verified_links": summary["valid"],
            "invalid_links": summary["invalid"],
            "pending_links": summary["pending"],
            "valid_links": list(dataset["valid_links"]),
            "source_dataset": dataset["dataset"],
            "as_of_seq": dataset["as_of_seq"],
            "link_type_distribution": link_type_dist,
        }

    def validate_link_batch(self) -> dict[str, Any]:
        """批量验证待验证链接：回执确认后规范状态同步流转，重复运行不再认成新工作。"""
        with LIFECYCLE_LOCK:
            validated: list[int] = []
            skipped: list[dict[str, Any]] = []
            for row in store.rows(self.MODULE):
                lifecycle = self._derive_lifecycle(row)
                conclusion = lifecycle["conclusion"]
                if conclusion != CONCLUSION_PENDING:
                    skipped.append({"link_id": row.get("id"), "conclusion": conclusion})
                    continue
                if lifecycle["pending_ops"]:
                    skipped.append({
                        "link_id": row.get("id"),
                        "conclusion": conclusion,
                        "reason": "存在待确认操作，等待确认后再校验",
                    })
                    continue
                receipt_id = f"batch-validation-{row.get('id')}"
                submitted = self.submit_validation_receipt(int(row["id"]), receipt_id=receipt_id)
                if not submitted.get("ok"):
                    skipped.append({"link_id": row.get("id"), "reason": submitted.get("reason")})
                    continue
                confirmed = self.confirm_link_event(int(row["id"]), submitted["event"]["event_id"])
                if confirmed.get("ok"):
                    validated.append(int(row["id"]))
            remaining = sum(
                1
                for row in store.rows(self.MODULE)
                if self._derive_lifecycle(row)["conclusion"] == CONCLUSION_PENDING
            )
            return {
                "ok": True,
                "validated_count": len(validated),
                "validated_links": validated,
                "skipped": skipped,
                "pending_count": remaining,
            }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["link_stats"] = self.build_aggregate_input()
        base["unconfirmed_ops"] = sum(
            1 for e in self._events() if e["state"] == EVENT_PENDING
        )
        return base
