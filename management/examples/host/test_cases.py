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

    def call(self, method, path, body=None, expected=200, key=None, authorized=True):
        headers = {"Content-Type": "application/json"}
        if authorized:
            headers["Authorization"] = "Bearer " + self.token
        if method == "POST":
            headers["Idempotency-Key"] = key or str(uuid.uuid4())
        request = urllib.request.Request(
            self.url + path,
            data=None if body is None else json.dumps(body, ensure_ascii=False).encode(),
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

    def session(self, agent_id):
        value = self.call("POST", "/v1/sessions", {"agent_id": agent_id}, expected=202)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-binary", required=True, type=Path)
    parser.add_argument("--postgres-container", required=True)
    parser.add_argument("--output", required=True, type=Path)
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
