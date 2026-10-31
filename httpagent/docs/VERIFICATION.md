# 文档验证

文档以cfb3e85公开API及GoAgent v0.32.2为基线，覆盖HTTP/SSE/WS、双向Webhook、Hooks/Observer和可替换连接接口。源码行为与已公开版本相同；此文档补充不修改生产Go代码或依赖。

## 重复执行

在httpagent模块目录执行：

```sh
python3 scripts/verify_docs.py
go test -race -count=1 ./...
go vet ./...
```

文档校验使用临时独立模块和已经推送的版本，强制GOWORK=off，不添加本机replace。它检查README及docs下Markdown的本地文件链接，并对含package声明的完整Go代码块执行依赖整理、只读构建和vet。没有package声明的接口签名/装配片段由源码人工核对，不冒充自动编译覆盖。

脚本不访问模型，不验证远端页面、锚点、生产代理、队列或框架部署。模块依赖首次使用可能需下载。完整服务程序编译通过不表示用户配置的模型一定支持原生工具流。

## 源码核对清单

| 文档内容 | 核对来源 |
| --- | --- |
| 配置、容量、默认值、路由、停服 | [handler.go](../handler.go) |
| Native/Answer能力、候选终态与EOF | [binding.go](../binding.go) |
| Output/Event、错误投影、生命周期信封 | [types.go](../types.go) |
| owner冻结、通知/cleanup并行及容量释放 | [run.go](../run.go) |
| HTTP错误映射、SSE提交/心跳、中间件能力 | [transport.go](../transport.go) |
| inline/queued、Receipt、Reply、Sender预算 | [webhook.go](../webhook.go) |
| run/cancel/accepted/event/ready、Origin策略 | [websocket.go](../websocket.go) |
| WebSocket接口与宿主连接入口 | [websocket_transport.go](../websocket_transport.go) |
| 默认具体帧库 | [websocket_gorilla.go](../websocket_gorilla.go) |

特别核对：DeliveryBudget按每次生命周期发布创建，started与终态不共享同一倒计时；Observer先于Delivery。queued只接纳，不启动worker；Handler.Run不持久缓存完成结果。SSE提交前错误可为JSON；WS ready晚于必要清理，早于全局运行槽位最终释放。这些行为需随源码变更同步修订。

## 本轮结果（2026-10-31）

- 模块README与4份正式文档的本地文件链接检查通过；2个完整Go包通过独立临时模块构建及vet，未使用本地replace。
- GoAgent主仓21份入口文档通过链接检查，4个完整Go程序编译通过；旧LLM示例漏import及“离线模拟”错误描述已修正。
- 从使用指南提取完整服务程序，使用已运行的本地Ollama llama3.1:8b实际执行JSON、SSE和inline Webhook；SSE含echo_input tool_start及唯一done，原生Hook实际触发，SIGTERM后exit0。

源码核对、命令输出及实测结果见 [文档验收记录](https://github.com/kart-io/goagent/blob/master/docs/evaluation/2026-10-31-httpagent-documentation/REPORT.md)。未下载模型，普通文档校验脚本仍不调用模型。上述结果不表示已验证所有框架或生产队列。
