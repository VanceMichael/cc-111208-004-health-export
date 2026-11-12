# 接收流程核验

本版本在原有原文留存与单份检查之上，新增独立的 Group/$export 接收流程，
有自己的端到端回归测试（`tests/test_export.py`）。原有 `tests/test_baseline.py`
只证明原功能，不因新流程改变。

## 对应关系

| 验收要求 | 实现 | 测试 |
| --- | --- | --- |
| 202 轮询持久化恢复且不重复创建 | `Exporter.create` 先写作业行再请求；状态地址先落盘再轮询；`create` 对未终结作业返回 existing | `test_polling_recovery_never_recreates_the_job`、`test_poll_recovery_after_restart_uses_saved_address` |
| 创建响应丢失不重建 | 缺地址或连接失败置 `uncertain`，`run` 不再发创建 | `test_connection_loss_before_address_marks_uncertain` |
| 取消后没有新增下载 | `mark_cancelled` 同事务推进 `download_epoch`；文件循环每步复查 | `test_cancel_prevents_new_downloads_and_deletes_job`、`test_restart_after_cancel_does_not_download` |
| 下载途中取消不能产生可用批次 | 读取检查点与 promote 事务均校验代次；真实第二进程触发取消 | `test_cancel_race_leaves_no_visible_batch` |
| error/坏 NDJSON 阻塞整批，旧批仍可读 | 有 `error` 项即 `diagnostic`；坏行/类型不符/截断置 `blocked`；`batches` 只增不改 | `test_error_file_blocks_batch_and_previous_batch_stays_visible`、`test_bad_line_and_wrong_type_block_whole_batch`、`test_truncated_response_is_not_accepted` |
| 引用后到可解析 | 整份接收后统一解析；悬空引用计数保留 | `test_late_patient_file_resolves_earlier_observation_reference` |
| 身份与重复/冲突 | `(resourceType,id,versionId)` 字符串精确匹配；重复折叠；异版冲突保留不排序 | `test_version_identity_is_opaque_and_conflicts_are_flagged`、`test_full_receipt_and_duplicate_collapse` |
| OperationOutcome 不当业务资源 | 业务文件中的 OperationOutcome 只入诊断表 | `test_diagnostic_operation_outcome_is_never_business_data` |
| requiresAccessToken 真/假 | 真时带 Bearer；假时不带；均在 `/__requests` 核对 | `test_requires_token_true_sends_bearer_and_false_does_not` |
| 跨 origin 重定向 | 目标须在可信白名单；跨 origin 剥认证头；越界目标在 I/O 前拒绝 | `test_cross_origin_redirect_strips_authorization`、`test_cross_origin_redirect_to_untrusted_target_is_refused`、`test_same_origin_redirect_keeps_authorization` |
| 崩溃后已落盘文件不重复计数 | 文件回执（sha256/字节/行数）与状态同事务持久化；重启只补未完成文件 | `test_completed_files_are_not_recounted_after_restart`、`test_killed_downloader_recovers_reservable_progress`（真实子进程被 `terminate`） |

## 状态词汇

- `waiting`：地址已持久化，来源仍报处理中；等待可注入。
- `downloading`：清单已接受，文件正在流式接收与核验。
- `diagnostic`：清单含 error 文件，整批被诊断阻塞。
- `ready`：这次导出是当前唯一可见批次。
- `blocked`：坏行/截断/下载失败；旧可见批次与进度保留。
- `cancelled`：取消已生效，不再安排新下载。
- `uncertain`：创建响应丢失或地址缺失；必须人工核对，绝不静默重建。

## 演示

`python -m scripts.demo` 依次展示：正常闭环（含重启恢复与唯一一次创建）、诊断阻塞、
requiresAccessToken=false 的真实请求、下载途中取消竞态、真实子进程终止后的恢复，
最后打印不含敏感字段的接收摘要（计数、引用统计、文件 sha256 回执与清单摘要）。

```sh
python -m scripts.source_server --port 8765   # 可选，独立运行场景服务
curl -s http://127.0.0.1:8765/__requests
```
