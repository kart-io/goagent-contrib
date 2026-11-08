#!/usr/bin/env python3
"""运行离线退款宿主的黑盒业务案例；使用专用数据库，无第三方 Python 依赖。"""

import argparse
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import urllib.parse
import uuid
from pathlib import Path


def check(condition, message):
    if not condition:
        raise AssertionError(message)


class Cases:
    def __init__(self, url, container):
        self.url = url
        self.container = container
        self.token = os.environ.get("GOAGENT_DEMO_TOKEN", "local-demo")
        self.requests = []
        self.results = []

    def call(self, method, path, body=None, expected=200, key=None, authorized=True, raw=None):
        headers = {"Content-Type": "application/json"}
        if authorized:
            headers["Authorization"] = "Bearer " + self.token
        if method == "POST" and key is not False:
            headers["Idempotency-Key"] = key or str(uuid.uuid4())
        request = urllib.request.Request(
            self.url + path,
            data=raw if raw is not None else (None if body is None else json.dumps(body, ensure_ascii=False).encode()),
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read()
        result = json.loads(raw)
        self.requests.append({"method": method, "path": path, "status": status})
        check(status == expected, f"{method} {path} 预期 HTTP {expected}，实际 {status}：{result}")
        return result

    def wait(self, path, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.call("GET", path)
            if predicate(value):
                return value
            time.sleep(0.02)
        raise AssertionError(f"等待超时：{path}；最后结果：{value}")

    def sql(self, statement):
        completed = subprocess.run(
            ["docker", "exec", self.container, "psql", "-XAt", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", statement],
            text=True, capture_output=True, timeout=15, check=True,
        )
        return completed.stdout.strip()

    @staticmethod
    def resource_id(value):
        # 数据库诊断仅接受服务端 UUID，拒绝把业务输入插入 SQL。
        return str(uuid.UUID(value))

    def refunds(self, task_id):
        task_id = self.resource_id(task_id)
        return int(self.sql(f"SELECT count(*) FROM demo_management_refunds WHERE tenant_id='demo' AND task_id='{task_id}'"))

    def event_state(self, session_id):
        session_id = self.resource_id(session_id)
        value = self.sql(
            "SELECT count(*)::text||','||md5(COALESCE(string_agg(seq::text||':'||event::text,'|' ORDER BY seq),'')) "
            "FROM goagent_session_event WHERE session_id="
            f"(SELECT data->>'log_id' FROM mg_sessions WHERE tenant_id='demo' AND id='{session_id}')"
        )
        count, fingerprint = value.split(",")
        return int(count), fingerprint

    def session(self, agent_id, version=None):
        body = {"agent_id": agent_id}
        if version is not None:
            body["version"] = version
        value = self.call("POST", "/v1/sessions", body, expected=202)
        return self.wait("/v1/sessions/" + value["id"], lambda item: item["status"] == "ready")

    def submit(self, session_id, text, key=None):
        return self.call("POST", "/v1/tasks", {"session_id": session_id, "input": text}, expected=202, key=key)

    def pending(self, task):
        task_id = task["id"]
        current = self.wait("/v1/tasks/" + task_id, lambda item: item["state"] == "waiting_approval")
        page = self.wait("/v1/approvals?task_id=" + task_id + "&status=pending", lambda item: len(item["items"]) == 1)
        check(self.refunds(task_id) == 0, "审批前已产生退款")
        return current, page["items"][0]

    def decide(self, approval, decision, reason, key=None):
        return self.call("POST", "/v1/approvals/" + approval["id"] + "/decision", {
            "expected_revision": approval["revision"], "decision": decision, "reason": reason,
        }, key=key)

    def finish(self, task_id, state="succeeded"):
        return self.wait("/v1/tasks/" + task_id, lambda item: item["state"] == state)

    def record(self, name, **evidence):
        self.results.append({"案例": name, "结果": "通过", **evidence})
        print("通过：" + name, flush=True)

    def run(self):
        self.call("GET", "/v1/agents", expected=401, authorized=False)
        self.record("未认证请求被拒绝", HTTP=401)
        agent = self.call("POST", "/v1/agents", {"name": "HTTP案例退款客服"}, expected=201)
        aid = agent["id"]
        self.call("POST", f"/v1/agents/{aid}/versions", {
            "version": "v1", "runner_kind": "refund", "implementation_version": "demo-v1", "config": {},
        }, expected=201)
        agent = self.call("POST", f"/v1/agents/{aid}/publish", {"version": "v1", "expected_revision": agent["revision"]})

        # 同一业务请求重投，既不能多建 Task，也不能用普通 resume 绕过审批。
        source = self.session(aid)
        key = str(uuid.uuid4())
        first = self.submit(source["id"], "请退还50元", key)
        duplicate = self.submit(source["id"], "请退还50元", key)
        check(first["id"] == duplicate["id"], "幂等重投创建了第二个任务")
        self.call("POST", "/v1/tasks", {"session_id": source["id"], "input": "不同业务内容"}, expected=409, key=key)
        waiting, approval = self.pending(first)
        rejected = self.call("POST", f"/v1/tasks/{first['id']}/resume", {
            "expected_revision": waiting["revision"], "wait_id": approval["wait"]["id"],
            "input": {"approval_id": approval["id"], "decision": "approve", "intent_hash": "伪造"},
        }, expected=409)
        check(rejected["error"]["code"] == "approval_decision_required", "普通恢复没有明确要求审批决定")
        self.record("重复提交与审批绕过保护", Task=first["id"], 重投复用任务=True, 同键不同内容HTTP=409, 绕过审批HTTP=409, 审批前退款=0)

        decision_key = str(uuid.uuid4())
        decided = self.decide(approval, "approve", "核验通过", decision_key)
        repeated = self.decide(approval, "approve", "核验通过", decision_key)
        check(decided["id"] == repeated["id"] and decided["decided_at"] == repeated["decided_at"], "重复决定没有复用原响应")
        approved_task = self.finish(first["id"])
        self.call("POST", f"/v1/approvals/{approval['id']}/decision", {
            "expected_revision": approval["revision"], "decision": "reject", "reason": "迟到拒绝",
        }, expected=409)
        check(self.refunds(first["id"]) == 1, "批准或重复批准导致退款次数异常")
        check("退款凭证已生成" in approved_task["result"], "批准后的业务答复不正确")
        recorded = self.call("GET", "/v1/approvals/" + approval["id"])
        check(recorded["actor"]["subject"] == "案例审批人" and recorded["reason"] == "核验通过", "处理人与理由没有保留")
        self.record("批准与重复审批只退款一次", Task=first["id"], 退款次数=1, 决定复用=True, 迟到反向决定HTTP=409, 处理人=recorded["actor"]["subject"])

        refusal_session = self.session(aid)
        refusal = self.submit(refusal_session["id"], "请审批这笔退款")
        _, refusal_approval = self.pending(refusal)
        self.decide(refusal_approval, "reject", "不符合退款规则")
        refused_task = self.finish(refusal["id"])
        check(self.refunds(refusal["id"]) == 0 and "未执行退款" in refused_task["result"], "拒绝后仍退款或答复错误")
        self.record("拒绝审批不退款", Task=refusal["id"], 退款次数=0, 业务答复=refused_task["result"])

        cancel_session = self.session(aid)
        cancel_task = self.submit(cancel_session["id"], "用户随后撤销")
        waiting, canceled_approval = self.pending(cancel_task)
        self.call("POST", f"/v1/tasks/{cancel_task['id']}/cancel", {"expected_revision": waiting["revision"], "reason": "用户撤销"}, expected=202)
        self.finish(cancel_task["id"], "canceled")
        self.call("POST", f"/v1/approvals/{canceled_approval['id']}/decision", {
            "expected_revision": canceled_approval["revision"], "decision": "approve", "reason": "迟到审批",
        }, expected=409)
        check(self.refunds(cancel_task["id"]) == 0, "撤销任务仍发生退款")
        self.record("等待审批时取消且拒绝迟到批准", Task=cancel_task["id"], 状态="canceled", 退款次数=0, 迟到批准HTTP=409)

        disabled_session = self.session(aid)
        paused = self.submit(disabled_session["id"], "停用期间保留决定")
        _, paused_approval = self.pending(paused)
        agent = self.call("POST", f"/v1/agents/{aid}/disable", {"expected_revision": agent["revision"]})
        saved_decision = self.decide(paused_approval, "approve", "停用期间已核验")
        blocked = self.wait("/v1/approvals/" + paused_approval["id"], lambda item: item["command_state"] == "blocked")
        command = self.call("GET", "/v1/commands/" + blocked["command_id"])
        check(command["error_kind"] == "agent_disabled" and self.refunds(paused["id"]) == 0, "停用没有阻止效果或没有阻塞原因")
        self.call("POST", "/v1/sessions", {"agent_id": aid}, expected=409)
        self.call("POST", "/v1/tasks", {"session_id": disabled_session["id"], "input": "停用时新任务"}, expected=409)
        agent = self.call("POST", f"/v1/agents/{aid}/enable", {"expected_revision": agent["revision"]})
        self.finish(paused["id"])
        after = self.call("GET", "/v1/approvals/" + paused_approval["id"])
        check(after["decided_at"] == saved_decision["decided_at"] and self.refunds(paused["id"]) == 1, "启用后没有沿用原决定或重复退款")
        self.record("停用阻塞并在启用后沿用原决定", Task=paused["id"], 阻塞原因=command["error_kind"], 停用期间退款=0, 启用后退款=1, 决定时间未变=True)

        self.call("POST", f"/v1/agents/{aid}/versions", {
            "version": "v2", "runner_kind": "refund", "implementation_version": "demo-v2", "config": {},
        }, expected=201)
        self.call("POST", f"/v1/agents/{aid}/publish", {"version": "v2", "expected_revision": agent["revision"]})
        fresh = self.session(aid)
        old = self.call("GET", "/v1/sessions/" + source["id"])
        check(old["version"] == "v1" and fresh["version"] == "v2", "发布修改了旧会话版本")
        newest = self.submit(fresh["id"], "新版会话退款")
        _, newest_approval = self.pending(newest)
        self.decide(newest_approval, "approve", "新版批准")
        check(self.finish(newest["id"])["result"].startswith("v2："), "新会话未按v2执行")
        before_history = self.call("GET", f"/v1/sessions/{source['id']}/history?limit=200")["items"]
        branch = self.call("POST", f"/v1/sessions/{source['id']}/fork", {"up_to": 0, "expected_revision": old["revision"]}, expected=202)
        branch = self.wait("/v1/sessions/" + branch["id"], lambda item: item["status"] == "ready")
        check(branch["version"] == "v1" and branch["parent_id"] == source["id"], "Fork没有继承版本和来源")
        check(self.call("GET", f"/v1/sessions/{branch['id']}/history?limit=200")["items"] == before_history, "Fork初始历史与源不一致")
        branch_task = self.submit(branch["id"], "分支独立退款")
        _, branch_approval = self.pending(branch_task)
        self.decide(branch_approval, "approve", "分支批准")
        check(self.finish(branch_task["id"])["result"].startswith("v1："), "分支未按v1执行")
        check(self.call("GET", f"/v1/sessions/{source['id']}/history?limit=200")["items"] == before_history, "分支写入污染源会话")
        check(self.refunds(first["id"]) == 1 and self.refunds(newest["id"]) == 1 and self.refunds(branch_task["id"]) == 1, "版本/Fork操作重复执行了源任务")
        self.record("版本发布与Fork独立执行", 旧会话版本="v1", 新会话版本="v2", 分支版本="v1", 源历史未变=True, 分支任务=branch_task["id"])

        before_events, before_fingerprint = self.event_state(source["id"])
        summary = None
        for _ in range(3):
            run = self.call("GET", "/v1/runs/" + first["id"])
            check(run["usage"]["Root"]["KnownTokens"]["total_tokens"] == 10, "重复查询改变用量")
            summary = self.call("GET", f"/v1/runs/summary?agent_id={aid}&limit=1")
        after_events, after_fingerprint = self.event_state(source["id"])
        check((before_events, before_fingerprint) == (after_events, after_fingerprint), "查询改写了源日志")
        check(summary["totals"]["tasks"] == 6, "跨页汇总遗漏任务")
        check(summary["totals"]["known_tokens"]["total_tokens"] == 55, "跨页或重复查询重复计量")
        check(summary["totals"]["states"] == {"succeeded": 5, "canceled": 1}, "汇总状态不一致")
        task_ids = [first["id"], refusal["id"], cancel_task["id"], paused["id"], newest["id"], branch_task["id"]]
        identifiers = ",".join("'" + self.resource_id(task_id) + "'" for task_id in task_ids)
        counts = self.sql("SELECT count(*)::text||','||COALESCE(sum(amount),0)::text FROM demo_management_refunds "
                          f"WHERE tenant_id='demo' AND task_id IN ({identifiers})")
        refund_count, refund_amount = map(int, counts.split(","))
        check((refund_count, refund_amount) == (4, 200), "数据库最终退款次数或金额异常")
        self.record("只读查询和跨页汇总与数据库一致", 查询前事件数=before_events, 查询后事件数=after_events, 日志内容摘要未变=before_fingerprint == after_fingerprint, 任务数=6, Token=55, 退款次数=refund_count, 退款总额=refund_amount, 状态=summary["totals"]["states"])
        return {"Agent": aid, "cases": self.results, "http_requests": self.requests}

    def run_extended(self):
        # 专用数据库允许重复验收；跨Agent汇总按本轮增量核对，不假定demo租户为空。
        before_totals = self.call("GET", "/v1/runs/summary?limit=1")["totals"]
        key = str(uuid.uuid4())
        body = {"name": "扩展案例客服"}
        agent = self.call("POST", "/v1/agents", body, expected=201, key=key)
        duplicate = self.call("POST", "/v1/agents", body, expected=201, key=key)
        check(duplicate["id"] == agent["id"], "注册重投创建了第二个Agent")
        self.call("POST", "/v1/agents", {"name": "不同注册内容"}, expected=409, key=key)
        aid = self.resource_id(agent["id"])
        self.record("Agent注册幂等与不同内容冲突", Agent=aid, 重投复用=True, 内容冲突HTTP=409)

        version_key = str(uuid.uuid4())
        version = {"version": "v1", "runner_kind": "refund", "implementation_version": "demo-v1",
                   "config": {"large": 9007199254740993, "nested": {"b": 2, "a": 1}}}
        saved = self.call("POST", f"/v1/agents/{aid}/versions", version, expected=201, key=version_key)
        reordered = dict(reversed(list(version.items())))
        reordered["config"] = {"nested": {"a": 1, "b": 2}, "large": 9007199254740993}
        repeated = self.call("POST", f"/v1/agents/{aid}/versions", reordered, expected=201, key=version_key)
        check(saved["id"] == repeated["id"] and saved["config_hash"] == repeated["config_hash"], "JSON键序改变了幂等结果")
        self.call("POST", f"/v1/agents/{aid}/versions", {**version, "config": {"large": 9007199254740992}}, expected=409)
        versions = self.call("GET", f"/v1/agents/{aid}/versions?limit=200")["items"]
        check(len(versions) == 1 and versions[0]["config"]["large"] == 9007199254740993, "版本被覆盖或数字失真")
        self.record("版本不可覆盖与JSON精确数值", 版本数=1, 大整数=versions[0]["config"]["large"], 键序不影响幂等=True)

        self.call("POST", f"/v1/agents/{aid}/publish", {"version": "不存在", "expected_revision": agent["revision"]}, expected=404)
        unchanged = self.call("GET", "/v1/agents/" + aid)
        check(unchanged == agent, "失败发布改变Agent状态")
        self.call("POST", f"/v1/agents/{aid}/versions", {"version": "v2", "runner_kind": "refund",
                  "implementation_version": "demo-v2", "config": {}}, expected=201)
        published = self.call("POST", f"/v1/agents/{aid}/publish", {"version": "v1", "expected_revision": agent["revision"]})
        self.call("POST", f"/v1/agents/{aid}/publish", {"version": "v2", "expected_revision": agent["revision"]}, expected=409)
        self.call("POST", f"/v1/agents/{aid}/disable", {"expected_revision": agent["revision"]}, expected=409)
        check(self.call("GET", "/v1/agents/" + aid) == published, "陈旧修订覆盖当前配置")
        agent = self.call("POST", f"/v1/agents/{aid}/publish", {"version": "v2", "expected_revision": published["revision"]})
        self.record("发布失败与修订冲突保持状态", 不存在版本HTTP=404, 陈旧修订HTTP=409, 当前版本="v2")

        legacy = self.session(aid, "v1")
        fresh = self.session(aid)
        spare = self.session(aid)
        check(legacy["version"] == "v1" and fresh["version"] == "v2", "显式旧版本或默认新版本绑定错误")
        self.record("显式旧版本与默认新版本会话", 显式版本=legacy["version"], 默认版本=fresh["version"])

        prefix = f"/v1/sessions?agent_id={aid}&limit=1"
        page = self.call("GET", prefix)
        first_cursor = page["next_cursor"]
        check(bool(first_cursor), "分页没有返回后续游标")
        seen = []
        for _ in range(10):
            seen.extend(item["id"] for item in page["items"])
            cursor = page.get("next_cursor")
            if not cursor:
                break
            page = self.call("GET", prefix + "&cursor=" + urllib.parse.quote(cursor, safe=""))
        else:
            raise AssertionError("Session分页游标没有结束")
        check(len(seen) == 3 and set(seen) == {legacy["id"], fresh["id"], spare["id"]}, "分页重复或遗漏会话")
        self.call("GET", f"/v1/sessions?agent_id={aid}&limit=2&cursor=" + urllib.parse.quote(first_cursor, safe=""), expected=400)
        self.call("GET", "/v1/sessions?agent_id=" + str(uuid.uuid4()) + "&limit=1&cursor=" + urllib.parse.quote(first_cursor, safe=""), expected=400)
        for query in ["limit=-1", "limit=201", "limit=非数字", "cursor=not-a-cursor", "created_from=not-a-date"]:
            self.call("GET", "/v1/sessions?" + urllib.parse.quote(query, safe="=&-"), expected=400)
        self.record("分页完整且游标绑定查询", 会话数=3, 无重复遗漏=True, 变更查询或非法参数HTTP=400)

        before_agents = self.sql("SELECT count(*) FROM mg_agents WHERE tenant_id='demo'")
        for raw in [b'{"name":"bad","name":"duplicate"}', b'{"name":"bad","unknown":1}',
                    b'{"Name":"bad"}', b'{"name":"\xff"}', b'{"name":"bad"} {}']:
            self.call("POST", "/v1/agents", raw=raw, expected=400)
        self.call("POST", "/v1/agents", {"name": "缺少幂等键"}, key=False, expected=400)
        self.call("POST", "/v1/agents", raw=b'{"name":"' + b'x' * (1 << 20) + b'"}', expected=413)
        missing = str(uuid.uuid4())
        for path in ["/v1/tasks/" + missing, "/v1/commands/" + missing, "/v1/sessions/" + missing + "/history"]:
            self.call("GET", path, expected=404)
        check(self.sql("SELECT count(*) FROM mg_agents WHERE tenant_id='demo'") == before_agents, "非法请求留下Agent")
        self.record("非法请求拒绝且不创建Agent", JSON拒绝数=5, 缺幂等键HTTP=400, 超大请求HTTP=413, 未知资源HTTP=404, Agent数量未变=True)

        first = self.submit(legacy["id"], "前序任务等待审批")
        _, first_approval = self.pending(first)
        queued = self.submit(legacy["id"], "排队后取消")
        current = self.wait("/v1/tasks/" + queued["id"], lambda item: item["state"] == "queued")
        queue_page = self.call("GET", f"/v1/tasks?session_id={legacy['id']}&status=queued")
        wait_page = self.call("GET", f"/v1/tasks?session_id={legacy['id']}&status=waiting_approval")
        check([item["id"] for item in queue_page["items"]] == [queued["id"]], "待处理任务列表不准确")
        check([item["id"] for item in wait_page["items"]] == [first["id"]], "审批等待列表混入其他任务")
        cancel_key = str(uuid.uuid4())
        cancel_body = {"expected_revision": current["revision"], "reason": "取消队尾"}
        canceled = self.call("POST", f"/v1/tasks/{queued['id']}/cancel", cancel_body, key=cancel_key, expected=202)
        self.finish(queued["id"], "canceled")
        still_waiting = self.call("GET", "/v1/tasks/" + first["id"])
        check(still_waiting["state"] == "waiting_approval" and self.refunds(first["id"]) == 0, "取消队尾影响前序任务")
        self.record("同会话队列查询及取消隔离", 前序状态="waiting_approval", 队尾状态="canceled", 前序退款=0)

        repeated_cancel = self.call("POST", f"/v1/tasks/{queued['id']}/cancel", cancel_body, key=cancel_key, expected=202)
        check(repeated_cancel == canceled, "取消重投没有复用响应")
        self.call("POST", f"/v1/tasks/{queued['id']}/resume", {"expected_revision": canceled["revision"],
                  "wait_id": "无等待点", "input": 1}, expected=409)
        queued_run = self.call("GET", "/v1/runs/" + queued["id"])
        check(not queued_run["attempts"] and queued_run["usage"] is None and self.refunds(queued["id"]) == 0,
              "排队取消仍执行或将未知用量当作0")
        self.record("取消幂等与终态禁止续跑", 重投复用=True, 取消后恢复HTTP=409, 尝试数=0, 用量=None, 退款=0)

        self.decide(first_approval, "approve", "前序批准")
        completed = self.finish(first["id"])
        check(completed["result"].startswith("v1："), "显式v1会话没有使用旧版本执行")
        first_history = self.call("GET", f"/v1/sessions/{legacy['id']}/history?limit=200")["items"]
        first_end = [event["seq"] for event in first_history if event["type"] == "turn/end"][-1]
        following = self.submit(legacy["id"], "队尾取消后继续执行")
        _, following_approval = self.pending(following)
        self.decide(following_approval, "approve", "后续批准")
        self.finish(following["id"])
        newest = self.submit(fresh["id"], "默认v2执行")
        _, newest_approval = self.pending(newest)
        self.decide(newest_approval, "approve", "新版批准")
        newest_done = self.finish(newest["id"])
        check(newest_done["result"].startswith("v2："), "默认新版本没有按v2执行")
        terminal = self.call("POST", f"/v1/tasks/{first['id']}/cancel", {"expected_revision": 0}, expected=202)
        check(terminal == completed, "已完成任务被取消覆盖")
        self.call("POST", f"/v1/tasks/{first['id']}/resume", {"expected_revision": completed["revision"],
                  "wait_id": first_approval["wait"]["id"], "input": 1}, expected=409)
        check(self.refunds(first["id"]) == 1 and len(self.call("GET", "/v1/runs/" + first["id"])["attempts"]) == 2,
              "终态操作重复运行或重复退款")
        self.record("终态结果保护与队列继续执行", 完成后取消保持成功=True, 完成后恢复HTTP=409, 旧版实际结果=completed["result"], 新版实际结果=newest_done["result"])

        full_history = self.call("GET", f"/v1/sessions/{legacy['id']}/history?limit=200")["items"]
        history_items = []
        after = 0
        for _ in range(len(full_history) + 2):
            page = self.call("GET", f"/v1/sessions/{legacy['id']}/history?limit=2&after_seq={after}")
            items = page["items"] or []
            if not items:
                break
            check(page["next_seq"] > after, "历史游标没有推进")
            history_items.extend(items)
            after = page["next_seq"]
        else:
            raise AssertionError("历史分页没有结束")
        check(history_items == full_history, "历史分页遗漏、重复或改写事件")
        check(all(item["type"] != "boundary/snapshot" and not item["type"].startswith("management/") for item in full_history),
              "默认历史暴露内部快照")
        for kind in ["boundary/snapshot", "management/session"]:
            self.call("GET", f"/v1/sessions/{legacy['id']}/history?type=" + urllib.parse.quote(kind, safe=""), expected=403)
        self.call("GET", f"/v1/sessions/{legacy['id']}/history?after_seq=-1", expected=400)
        self.record("历史分页完整并保护内部类型", 历史事件数=len(full_history), 页大小=2, 内部类型HTTP=403, 负序号HTTP=400)

        source = self.call("GET", "/v1/sessions/" + legacy["id"])
        fork_path = f"/v1/sessions/{source['id']}/fork"
        request = {"expected_revision": source["revision"], "up_to": first_end}
        copied = self.call("POST", fork_path, request, expected=202)
        copied = self.wait("/v1/sessions/" + copied["id"], lambda item: item["status"] == "ready")
        copied_history = self.call("GET", f"/v1/sessions/{copied['id']}/history?limit=200")["items"]
        check(copied_history == first_history and copied["fork_seq"] == first_end, "精确轮次Fork复制了未来历史")
        self.call("POST", fork_path, {**request, "up_to": full_history[0]["seq"]}, expected=400)
        self.call("POST", fork_path, {**request, "up_to": full_history[-1]["seq"] + 100}, expected=400)
        self.call("POST", fork_path, {**request, "expected_revision": source["revision"] - 1}, expected=409)
        self.call("POST", f"/v1/sessions/{spare['id']}/fork", {"expected_revision": spare["revision"], "up_to": 0}, expected=400)
        self.record("精确轮次Fork与非法边界拒绝", Fork序号=first_end, 分支事件数=len(copied_history), 源事件数=len(full_history), 非结束或未来边界HTTP=400, 陈旧修订HTTP=409)

        agent = self.call("GET", "/v1/agents/" + aid)
        disabled = self.call("POST", f"/v1/agents/{aid}/disable", {"expected_revision": agent["revision"]})
        fork_key = str(uuid.uuid4())
        before_sessions = self.sql(f"SELECT count(*) FROM mg_sessions WHERE tenant_id='demo' AND data->>'agent_id'='{aid}'")
        self.call("POST", fork_path, request, key=fork_key, expected=409)
        check(self.sql(f"SELECT count(*) FROM mg_sessions WHERE tenant_id='demo' AND data->>'agent_id'='{aid}'") == before_sessions,
              "停用期间Fork留下子会话")
        self.call("POST", f"/v1/agents/{aid}/enable", {"expected_revision": disabled["revision"]})
        retried = self.call("POST", fork_path, request, key=fork_key, expected=202)
        self.wait("/v1/sessions/" + retried["id"], lambda item: item["status"] == "ready")
        self.record("停用阻止Fork且启用后原键重试", 停用ForkHTTP=409, 拒绝未新增会话=True, 启用后同键成功=True)

        summary = self.call("GET", f"/v1/runs/summary?agent_id={aid}&limit=1")
        check(summary["totals"]["tasks"] == 4 and summary["totals"]["known_tokens"]["total_tokens"] == 30,
              "扩展案例任务或消耗汇总错误")
        check(summary["totals"]["states"] == {"succeeded": 3, "canceled": 1}, "扩展状态汇总错误")
        for task in [first, following, newest]:
            check(self.refunds(task["id"]) == 1, "扩展成功任务退款不为1")
        check(self.refunds(queued["id"]) == 0, "扩展取消任务仍退款")
        combined = self.call("GET", "/v1/runs/summary?limit=1")
        check(combined["totals"]["tasks"] == before_totals["tasks"] + 4
              and combined["totals"]["known_tokens"]["total_tokens"] == before_totals["known_tokens"]["total_tokens"] + 30,
              "跨Agent汇总重复或遗漏")
        return {"cases": self.results, "extended_agent": aid, "extended_tasks": 4, "extended_refunds": 3,
                "extended_tokens": 30, "combined_tasks": combined["totals"]["tasks"],
                "combined_tokens": combined["totals"]["known_tokens"]["total_tokens"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-binary", required=True, type=Path)
    parser.add_argument("--postgres-container", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--extended", action="store_true", help="在原8组案例后追加12组功能与边界案例")
    args = parser.parse_args()
    check(bool(os.environ.get("GOAGENT_PGSTORE_DSN")), "必须设置专用案例数据库GOAGENT_PGSTORE_DSN")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {"status": "failed"}
    process = None
    cases = None
    exit_code = 1
    stderr_path = args.output.with_suffix(".host-stderr.log")
    with stderr_path.open("w", encoding="utf-8") as stderr:
        try:
            process = subprocess.Popen([str(args.host_binary.resolve()), "-migrate", "-listen", "127.0.0.1:0"], stdout=subprocess.PIPE, stderr=stderr, text=True)
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                check(bool(selector.select(timeout=15)), "案例宿主启动超时")
                ready = process.stdout.readline()
            address = re.search(r"127\.0\.0\.1:\d+", ready)
            check(address is not None, f"案例宿主启动失败：{ready}；诊断见{stderr_path}")
            cases = Cases("http://" + address.group(0), args.postgres_container)
            result.update(cases.run())
            if args.extended:
                result.update(cases.run_extended())
            result["status"] = "passed"
            exit_code = 0
        except Exception as error:
            result["error"] = str(error)
            if cases is not None:
                result.update({"cases": cases.results, "http_requests": cases.requests})
            print("案例失败：" + str(error), file=sys.stderr)
        finally:
            if process is not None:
                try:
                    process.send_signal(signal.SIGINT)
                    host_exit = process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    host_exit = process.wait()
                result["host_exit_code"] = host_exit
                if host_exit != 0:
                    result["status"] = "failed"
                    result["shutdown_error"] = f"宿主退出码为{host_exit}"
                    exit_code = 1
                process.stdout.close()
            args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
