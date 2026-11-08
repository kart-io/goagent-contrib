# 真实模型管理验收

`integration/live_model_test.go` 通过已有DeepSeek、Gemini客户端发出真实流式请求，运行原生Agent、Postgres持久日志、管理Service/Worker及TCP HTTP接口。模型自主生成工具调用和回答，观察包装器不替换请求或响应，用量来自提供方终态。

每个提供方执行以下五组，正常完整执行预计11次请求：

| 案例 | 必须验证的行为 |
| --- | --- |
| 批准与宿主重建 | 首轮模型调用后等待审批、SQL效果0；重建Service/Worker后批准；第二次请求返回SQL工具回执，效果1；重复提交、重复审批不新增请求和效果 |
| 拒绝审批 | 模型提出退款调用；拒绝后明确说明未执行，SQL效果0，不重新申请 |
| 等待审批时取消 | 首轮真实调用已完成；取消返回202，迟到批准409，不再调用模型，不产生效果 |
| 多轮上下文与精确Fork | 父会话先记随机代码A再更新为B；在A轮结束处分叉；子会话回答A、父会话回答B，查询不携带答案 |
| 发布版本与会话绑定 | 发布v2后，旧Session的模型仍执行v1提示，新Session执行v2提示；同时检查Task版本及实际回答 |

每个案例逐Task比对提供方实际prompt/completion/total用量与`GET /v1/runs/{id}`，再核对跨Session汇总。重复读取不触发模型。不会用prompt加completion推导total，部分提供方的total还包含思考用量；缺失基础字段会失败，不冒充已知零用量。

## 复跑命令

默认`go test`不触网，新套件跳过。显式启用后，缺模型凭据或数据库属于失败。凭据只从已加载的环境继承，不写入命令、脚本或证据。可设置DEEPSEEK_MODEL、GEMINI_MODEL；未设置时分别使用deepseek-flash、gemini-2.5-flash。

```bash
# 从已加载模型凭据的核心仓库启动；使用专用Postgres，勿指向业务库。
export GOAGENT_CORE_ROOT="$PWD"
export GOAGENT_CONTRIB_ROOT="$(cd ../goagent-contrib && pwd)"
export GOAGENT_PGSTORE_DSN='postgres://postgres:management-local@127.0.0.1:55444/postgres?sslmode=disable'
export GOAGENT_MANAGEMENT_LIVE=1
export GOAGENT_MANAGEMENT_LIVE_PROVIDERS=deepseek,gemini
export GOAGENT_MANAGEMENT_LIVE_REPORT_DIR="$PWD/.claude/management-live-$(date +%Y%m%d-%H%M%S)"

env -u GOROOT GOTOOLCHAIN=go1.27.3 \
  bash "$GOAGENT_CONTRIB_ROOT/scripts/with-local-modules.sh" \
  go -C "$GOAGENT_CONTRIB_ROOT/management" test -race -count=1 -timeout 20m \
  -run '^TestLiveManagement$' -v ./integration
```

先确认DEEPSEEK_API_KEY、GEMINI_API_KEY非空，不要打印值。若运行环境按工作区注入凭据，从另一个工作目录启动可能得到不同或空的配置。上述命令保留启动工作区的环境，使用绝对路径指定contrib源码。

每次运行使用全新报告目录；已有文件会拒绝覆盖。每个案例创建随机schema，退出时清除；需要事后审计的数据会先写入报告。测试异常被强制终止时可能留下schema，只清理自己的专用测试容器。运行期间不要对该数据库执行重启或故障注入。

## 请求与证据边界

- 每套、每个提供方最多20次客户端请求；单次输出上限1024Token同时设置在Agent和客户端，并在发送前检查，单次60秒超时。没有自动重试或失败时切换提供方。
- 每次重跑都会产生新的模型请求和费用；失败记录单独保留，不能只报告成功轮次的请求和已知用量。
- 包装器记录客户端调用数、提供方报告的模型版本、工具调用、终态和实际用量。完成终态证明真实模型响应；未完成的调用不算成功，无法确认的用量保持未知。
- 重建的是同一进程中的Service、Worker及新构建的Provider/Agent，复用真实持久存储；不等同于操作系统进程崩溃或数据库重启。
- 取消覆盖真实模型提出工具请求后的等待审批阶段；没有声称验证流式请求中途取消、限流、提供方超时故障或生产准确率。
- 退款只写入隔离数据库的合成业务表，没有调用真实支付系统。只保存最终回答、工具和用量证据，不保存模型思考内容或请求认证头。

本轮实际执行结果及失败说明见[核心验收报告](https://github.com/kart-io/goagent/blob/master/docs/evaluation/2026-11-08-management-live-model/REPORT.md)。远端尚未推送时，使用本地核心仓库对应路径阅读。
