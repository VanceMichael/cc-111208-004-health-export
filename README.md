# healthbulk

体检服务 FHIR 导出清单的离线检查与响应留存。

## 本地运行

使用 Python 3.11 标准库，无 pip 依赖。所有来源与业务数据都是本地合成数据，不连接真实用户系统。

```sh
python -m scripts.materialize
python -m healthbulk manifest fixtures/manifest.json
python -m compileall -q .
python -m unittest discover -s tests -v
python -m scripts.demo
```

检查命令输出单份来源的结构摘要。`capture URL --method GET --state var` 仅接受 loopback HTTP，保存原始响应为 SHA256 文件，并在 SQLite captures 表登记；可用 `--header 'Name:Value'` 传头、`--body FILE` 传正文，不跟随重定向。检查器不执行多页、业务去重或正式接收。当前命令不包含待开发的 export 业务流程；新增能力与现有摘要工具的边界见 docs/acceptance.md。

## 可复现容器

```sh
docker build -t healthbulk:local .
docker run --rm --network none healthbulk:local python -m unittest discover -s tests -v
docker run --rm --network none healthbulk:local
```

镜像固定基础摘要。构建在正式运行前完成；运行和测试不安装包、不下载数据。演示命令实际执行检查器及本地来源，通常不超过 5 秒；它不是新增业务接收已实现的声明。开发完成后扩展 scripts/demo.py 展示一个真实业务闭环，完整输出并控制在 90 秒以内。

## 协议与来源

- docs/source-contract.md 给出支持子集、字段及失败语义。
- docs/acceptance.md 给出业务验收边界。
- fixtures/source.json 是本地可重放 HTTP 路由；`python -m scripts.source_server --port 8765` 启动，`/__requests` 可检查实际请求。
- scripts/materialize.py 将版本化 base64 二进制夹具还原为原始字节，重复执行安全；二进制文件不是用户附件。
- journal.py 只记录原始响应，不代替新增流程自己的事务和恢复设计。
