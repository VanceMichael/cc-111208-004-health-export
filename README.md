# healthbulk

体检服务 FHIR Bulk Data 导出的可审计接收。所有来源与业务数据都是本地合成数据，不连接真实用户系统，也不含真实患者字段。

使用 Python 3.11 标准库，无 pip 依赖。

## 本地运行

```sh
python -m scripts.materialize
python -m compileall -q .
python -m unittest discover -s tests -v
python -m scripts.demo
```

演示会实际执行完整闭环：创建异步作业、持久化作业身份与状态地址、断点轮询、流式接收与核验、可见批次切换、诊断阻塞、取消竞态和真实子进程被终止后的恢复；输出不含敏感字段的接收摘要与真实请求日志，通常在 10 秒内完成。

## 命令

```sh
# 启动并持久化一个导出作业，等它完成接收
python -m healthbulk start --start 'http://127.0.0.1:8765/Group/G1/$export' \
    --origin '{{ORIGIN}}' --token local-demo --state var/demo --wait

# 只从磁盘恢复；绝不重新创建作业
python -m healthbulk resume --state var/demo --origin '{{ORIGIN}}' --token local-demo

python -m healthbulk status --state var/demo
python -m healthbulk cancel --state var/demo          # 发送 DELETE，之后不再安排下载
python -m healthbulk summary --state var/demo         # 只含计数、摘要与文件回执
python -m healthbulk receive --manifest 'http://…/public/manifest' --state var/demo
```

`--origin` 是可信目标白名单，可用 `--origin` 多次给出；`{{ORIGIN}}` 会以 `--start` 或
`--manifest` 的 origin 展开。文件 URL 与重定向目标都必须落在白名单内。

`--token` 只在 `requiresAccessToken=true` 的导出里作为 `Authorization: Bearer` 发送；
为假时不发送。跨 origin 重定向会剥掉认证头。

原有检查器命令保持不变：

```sh
python -m healthbulk manifest fixtures/manifest.json
python -m healthbulk capture URL --method GET --state var
```

`capture` 仅接受 loopback HTTP，保存原始响应为 SHA256 文件，并在 SQLite captures 表登记；不跟随重定向。

## 接收语义

- 作业身份先落盘再发请求；状态地址在首次轮询前落盘。
- 断线只能恢复已有作业。创建响应丢失或 202 无 `Content-Location` 时进入 `uncertain`，
  不偷偷重建，等待人工核对。
- 轮询支持 `Retry-After` 的整数秒与 HTTP 日期，等待时钟可注入。
- 每个 NDJSON 文件流式读入磁盘，逐行核验声明 `type` 与 `resourceType`；坏行、截断、
  越界 URL 都令整个批次不可见。
- 身份是 `(resourceType,id,meta.versionId)` 的精确字符串；同批重复只留一份，
  同 id 不同版本记为冲突并保留，绝不按版本号大小排序。
- `OperationOutcome` 只作诊断，从不当业务资源。
- 跨资源引用在整份清单接收完成后统一解析；Patient 文件后到不会丢 Observation 关联，
  仍然悬空的引用按悬空计数保留。
- 只有整份清单全部接收且无 `error` 项才切换可见批次；任何失败都保留旧可见批次与下载进度。
- 取消立即推进下载代次并落盘，进行中的文件和后续文件都不会产生新的可见批次；
  重定向复用已有文件回执，不会重复计数。

## 协议与来源

- `docs/source-contract.md`：支持的 Group/$export 子集与失败语义。
- `docs/acceptance.md`：验收边界与本实现对应的测试。
- `fixtures/source.json`：可重放的本地 HTTP 场景（正常、慢速、错误文件、坏行、截断、
  跨引用、版本冲突、重定向、无凭据与不确定创建）。`python -m scripts.source_server --port 8765`
  启动；`/__requests` 返回真实请求日志，`/__control` 可切换 `trusted`、`substitutions`。
- `scripts/materialize.py`：将版本化 base64 二进制夹具还原为原始字节，重复执行安全。
- `healthbulk/journal.py`：只记录原始响应，不代替 export 流程自己的事务与恢复设计。

## 可复现容器

```sh
docker build -t healthbulk:local .
docker run --rm --network none healthbulk:local python -m unittest discover -s tests -v
docker run --rm --network none healthbulk:local
```

镜像在构建期完成材料化；运行和测试不安装包、不下载数据。
