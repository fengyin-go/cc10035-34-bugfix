"""知识链接有效性核对场景验证：覆盖校验→聚合数据流上的状态一致性。

场景对应缺陷修复要求：
1. 批量校验回执与详情页、聚合输入结论一致，重复运行不认成新工作；
2. 历史仅写有效标记的记录被核对数据集识别并回写台账；
3. 验证回执与标记失效交错到达：确认前聚合指出阻塞项，确认顺序决定有效区间，
   失效确认后重传同一回执不令链接重新生效；
4. 并发提交同一验证回执只算一次；
5. 当次质量汇总与有效链接清单条数始终一致。

直接运行：.venv/bin/python tests/test_knowledge_link_validity.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.seed import SEED_ROWS  # noqa: E402
from app.services.aggregate_view import AggregateViewService  # noqa: E402
from app.services.knowledge_link import KnowledgeLinkService  # noqa: E402
from app.store import store  # noqa: E402

MODULE = "knowledge_link"


def reset_store() -> None:
    """每个场景前还原种子数据并清空生命周期事件台账。"""
    store.rows(MODULE)[:] = [dict(r) for r in SEED_ROWS[MODULE]]
    store.module_state(MODULE).clear()
    store.module_state("aggregate_view").clear()


def detail_of(round_result: dict, link_id: int) -> dict:
    return next(d for d in round_result["details"] if d["link_id"] == link_id)


def record_of(dataset: dict, link_id: int) -> dict:
    return next(r for r in dataset["records"] if r["link_id"] == link_id)


def assert_round_consistent(round_result: dict) -> None:
    """当次质量汇总与有效链接清单条数一致，且与核对数据集同源。"""
    summary = round_result["quality_summary"]
    assert summary["valid"] == len(round_result["valid_links"]), (
        f"汇总 valid={summary['valid']} 与清单 {round_result['valid_links']} 条数不一致"
    )
    assert summary["valid_list_count"] == len(round_result["valid_links"])
    assert round_result["summary_list_consistent"] is True
    dataset = KnowledgeLinkService().build_validity_check_dataset()
    assert summary["valid"] == dataset["summary"]["valid"] == len(dataset["valid_links"])


def test_batch_validation_receipt_matches_aggregation() -> None:
    """成组校验后：详情页、聚合输入同步生效；再次运行不认成新工作。"""
    reset_store()
    kls = KnowledgeLinkService()
    avs = AggregateViewService()

    before = kls.build_aggregate_input()
    assert before["verified_links"] == 1  # 种子数据仅 id=2 已验证

    result = kls.validate_link_batch()
    assert result["validated_count"] == 1 and result["validated_links"] == [1]

    # 回执与详情页一致：规范状态已流转，不再是待验证
    entry = kls.get_entry(1)
    assert entry["status"] == "已验证"
    assert entry["有效性核对"]["实际结论"] == "已验证"
    assert entry["有效性核对"]["当前是否有效"] is True

    # 聚合输入不再丢失这条有效链接
    after = kls.build_aggregate_input()
    assert after["verified_links"] == 2
    assert 1 in after["valid_links"]

    # 再次运行不认成新工作
    rerun = kls.validate_link_batch()
    assert rerun["validated_count"] == 0
    assert rerun["pending_count"] == 0

    round_result = avs.aggregate_valid_links()
    assert 1 in round_result["valid_links"]
    assert_round_consistent(round_result)
    print("✔ 批量校验回执与聚合输入一致，重复运行不认成新工作")


def test_legacy_mark_only_record_reconciled() -> None:
    """历史仅写有效标记的记录：核对数据集识别偏差，聚合采纳后回写台账。"""
    reset_store()
    kls = KnowledgeLinkService()
    avs = AggregateViewService()

    # 模拟修复前的历史写入：只写有效标记，规范状态未流转
    row = store.find(MODULE, 1)
    row["验证状态"] = "有效"
    row["上次验证"] = "2026-11-19T10:00:00Z"
    assert row["status"] == "待验证"

    dataset = kls.build_validity_check_dataset()
    rec = record_of(dataset, 1)
    assert rec["实际结论"] == "已验证"          # 现有标记作为有效证据
    assert rec["当前是否有效"] is True
    assert rec["台账与结论一致"] is False        # 状态偏差被核对出来
    assert rec["标记与结论一致"] is True
    assert rec["有效区间"][-1]["source"] == "历史有效标记"
    assert rec["有效区间"][-1]["valid_from"] == "2026-11-19T10:00:00Z"

    # 页面回查：对照原标记与实际生命周期结论
    view = kls.get_entry(1)
    assert view["有效性核对"]["原验证标记"] == "有效"
    assert view["有效性核对"]["台账状态"] == "待验证"
    assert view["有效性核对"]["实际结论"] == "已验证"

    # 聚合视图采纳结论并回写链接验证台账
    round_result = avs.aggregate_valid_links()
    assert 1 in round_result["valid_links"]
    changed = {c["link_id"] for c in round_result["ledger_writeback"]["changes"]}
    assert 1 in changed
    assert store.find(MODULE, 1)["status"] == "已验证"
    assert_round_consistent(round_result)

    # 回写后详情页与聚合结论一致
    view_after = kls.get_entry(1)
    assert view_after["status"] == "已验证"
    assert view_after["有效性核对"]["台账状态"] == "已验证"
    print("✔ 历史仅写有效标记的记录被核对、回写，页面回查可对照原标记与实际结论")


def test_interleaved_receipt_and_invalidation() -> None:
    """验证回执与失效请求交错到达：确认前聚合指出阻塞；确认顺序决定区间；
    失效确认后重传同一回执不令链接重新生效。"""
    reset_store()
    kls = KnowledgeLinkService()
    avs = AggregateViewService()

    entry, missing = kls.create_entry(
        {"链接ID": "L-100", "源实体": "实体A", "外部URI": "https://example.com/a"}
    )
    assert not missing
    link_id = int(entry["id"])

    # 两项操作同时到达（均未确认）
    r1 = kls.submit_validation_receipt(link_id, receipt_id="RCPT-100")
    assert r1["ok"] and not r1["deduplicated"]
    x1 = kls.submit_invalidation_request(link_id, reason="巡检发现目标页下线")
    assert x1["ok"] and not x1["deduplicated"]

    # 两项都确认之前，值班员执行一轮有效链接汇总
    round1 = avs.aggregate_valid_links()
    d1 = detail_of(round1, link_id)
    assert d1["usable"] is False
    assert d1["valid_since"] is None
    assert any("验证回执" in b and "待确认" in b for b in d1["blocked_by"])
    assert any("失效请求" in b and "待确认" in b for b in d1["blocked_by"])
    assert_round_consistent(round1)

    # 先确认验证回执 → 链接进入有效区间
    c1 = kls.confirm_link_event(link_id, r1["event"]["event_id"])
    assert c1["当前结论"] == "已验证"
    assert c1["event"]["disposition"]["结果"] == "开启有效区间"
    round2 = avs.aggregate_valid_links()
    d2 = detail_of(round2, link_id)
    assert d2["usable"] is True
    assert d2["valid_since_event"] == r1["event"]["event_id"]
    # 汇总明细与链接时间记录一致
    rec2 = record_of(kls.build_validity_check_dataset(), link_id)
    assert d2["valid_since"] == rec2["有效区间"][-1]["valid_from"] == c1["event"]["confirmed_at"]
    assert_round_consistent(round2)

    # 再确认失效请求 → 按时间证据核对并结束该区间
    c2 = kls.confirm_link_event(link_id, x1["event"]["event_id"])
    assert c2["当前结论"] == "已失效"
    disp = c2["event"]["disposition"]
    assert disp["结果"] == "结束有效区间"
    assert disp["valid_from"] == c1["event"]["confirmed_at"]
    assert disp["valid_until"] == c2["event"]["confirmed_at"]
    round3 = avs.aggregate_valid_links()
    d3 = detail_of(round3, link_id)
    assert d3["usable"] is False
    assert any("失效处置已确认" in b for b in d3["blocked_by"])
    assert_round_consistent(round3)

    # 失效确认后重传同一验证回执 → 幂等去重，链接不重新生效
    again = kls.submit_validation_receipt(link_id, receipt_id="RCPT-100")
    assert again["ok"] and again["deduplicated"]
    assert again["当前结论"] == "已失效"
    # 新回执同样被拒绝
    fresh = kls.submit_validation_receipt(link_id, receipt_id="RCPT-101")
    assert not fresh["ok"] and fresh["rejected"]

    # 核对详情和再次汇总：结论仍是已失效，区间已闭合
    view = kls.get_entry(link_id)
    assert view["status"] == "已失效"
    assert view["有效性核对"]["实际结论"] == "已失效"
    assert view["有效性核对"]["当前是否有效"] is False
    round4 = avs.aggregate_valid_links()
    assert detail_of(round4, link_id)["usable"] is False
    assert_round_consistent(round4)
    rec4 = record_of(kls.build_validity_check_dataset(), link_id)
    assert rec4["有效区间"][-1]["valid_until"] == c2["event"]["confirmed_at"]
    print("✔ 交错到达场景：确认前聚合指出阻塞项，失效确认后重传回执不重新生效")


def test_concurrent_duplicate_receipt_counts_once() -> None:
    """并发提交同一验证回执只登记一次、只确认一次、只开一个区间。"""
    reset_store()
    kls = KnowledgeLinkService()

    first = kls.submit_validation_receipt(1, receipt_id="RCPT-DUP")
    second = kls.submit_validation_receipt(1, receipt_id="RCPT-DUP")
    assert first["ok"] and second["ok"] and second["deduplicated"]
    assert first["event"]["event_id"] == second["event"]["event_id"]
    events = [e for e in kls._events(1) if e["kind"] == "验证回执"]
    assert len(events) == 1

    kls.confirm_link_event(1, first["event"]["event_id"])
    again = kls.confirm_link_event(1, first["event"]["event_id"])
    assert again["deduplicated"]
    rec = record_of(kls.build_validity_check_dataset(), 1)
    assert len(rec["有效区间"]) == 1
    assert rec["当前是否有效"] is True
    print("✔ 并发提交同一验证回执只算一次")


def test_invalidation_confirmed_before_receipt_blocks_validity() -> None:
    """失效先确认（无开放区间可结束），后确认的回执被覆盖，不重新生效。"""
    reset_store()
    kls = KnowledgeLinkService()

    r = kls.submit_validation_receipt(1, receipt_id="RCPT-LATE")
    x = kls.submit_invalidation_request(1)
    cx = kls.confirm_link_event(1, x["event"]["event_id"])
    assert cx["event"]["disposition"]["结果"] == "无开放区间可结束"
    assert cx["当前结论"] == "已失效"

    cr = kls.confirm_link_event(1, r["event"]["event_id"])
    assert cr["当前结论"] == "已失效"
    assert "覆盖" in cr["event"]["disposition"]["结果"]
    rec = record_of(kls.build_validity_check_dataset(), 1)
    assert rec["当前是否有效"] is False
    assert rec["有效区间"] == []
    print("✔ 失效先确认时，后到的验证回执被覆盖，链接不重新生效")


def test_stale_valid_mark_does_not_revive_invalidated_link() -> None:
    """残留有效标记与失效台账冲突：失效结论优先，回执不再生效。"""
    reset_store()
    kls = KnowledgeLinkService()

    row = store.find(MODULE, 3)  # 种子中已失效
    row["验证状态"] = "有效"      # 历史残留的有效标记
    dataset = kls.build_validity_check_dataset()
    rec = record_of(dataset, 3)
    assert rec["实际结论"] == "已失效"
    assert rec["当前是否有效"] is False
    assert rec["标记与结论一致"] is False  # 对照出标记与结论冲突

    res = kls.submit_validation_receipt(3, receipt_id="RCPT-3")
    assert not res["ok"] and res["rejected"]
    print("✔ 残留有效标记不会令已失效链接重新生效")


def test_run_action_transitions_canonical_status() -> None:
    """工作台动作走同一事件台账：状态流转受实际结论约束。"""
    reset_store()
    kls = KnowledgeLinkService()

    entry, _ = kls.run_action(1, "验证链接")
    assert entry is not None and entry["status"] == "已验证"
    # 已验证链接不允许重复验证
    entry2, message2 = kls.run_action(1, "验证链接")
    assert entry2 is None and "已验证" in message2
    # 标记失效：按时间证据结束当前区间
    entry3, _ = kls.run_action(1, "标记失效")
    assert entry3 is not None and entry3["status"] == "已失效"
    # 已失效链接不能通过验证动作重新生效
    entry4, _ = kls.run_action(1, "验证链接")
    assert entry4 is None
    # 已失效可移除
    entry5, _ = kls.run_action(1, "移除链接")
    assert entry5 is None  # 前置状态为已验证，已失效不满足
    print("✔ 工作台动作流转规范状态，前置状态按实际结论核对")


def test_removal_closes_interval_and_is_terminal() -> None:
    """移除关闭有效区间且为终态。"""
    reset_store()
    kls = KnowledgeLinkService()
    kls.run_action(1, "验证链接")
    entry, _ = kls.run_action(1, "移除链接")
    assert entry is not None and entry["status"] == "已移除"
    res = kls.submit_validation_receipt(1, receipt_id="RCPT-AFTER-REMOVAL")
    assert not res["ok"] and res["rejected"]
    rec = record_of(kls.build_validity_check_dataset(), 1)
    assert rec["实际结论"] == "已移除"
    assert rec["有效区间"][-1]["valid_until"] is not None
    print("✔ 移除关闭有效区间且为终态")


def test_module_state_does_not_pollute_overview() -> None:
    """生命周期事件台账不进入业务模块统计。"""
    reset_store()
    kls = KnowledgeLinkService()
    kls.submit_validation_receipt(1, receipt_id="RCPT-X")
    overview = store.overview()
    assert len(overview["modules"]) == 18
    assert MODULE in store.module_names()
    print("✔ 事件台账不污染运营概览")


def main() -> int:
    scenarios = [
        test_batch_validation_receipt_matches_aggregation,
        test_legacy_mark_only_record_reconciled,
        test_interleaved_receipt_and_invalidation,
        test_concurrent_duplicate_receipt_counts_once,
        test_invalidation_confirmed_before_receipt_blocks_validity,
        test_stale_valid_mark_does_not_revive_invalidated_link,
        test_run_action_transitions_canonical_status,
        test_removal_closes_interval_and_is_terminal,
        test_module_state_does_not_pollute_overview,
    ]
    failed = 0
    for scenario in scenarios:
        try:
            scenario()
        except AssertionError as exc:
            failed += 1
            print(f"✘ {scenario.__name__}: {exc}")
    if failed:
        print(f"\n{failed} 个场景未通过")
        return 1
    print(f"\n全部 {len(scenarios)} 个场景通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
