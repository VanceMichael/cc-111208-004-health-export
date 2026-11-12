# 合成机构 FHIR 导出

GET /Group/G1/$export 携带 Prefer: respond-async 启动作业；202 的 Content-Location 给出唯一作业地址 /jobs/J1，必须持久化后轮询。202 的 Retry-After 支持整数秒或 HTTP 日期，时钟应可注入。DELETE /jobs/J1 请求取消。已保存地址后断线恢复只能继续这个作业；若创建响应丢失、未取得地址，则标记不确定并等待人工核对，不偷偷重建。

完成清单含 transactionTime、布尔 requiresAccessToken 以及 output/error 数组，每项为 type/url。文件为 UTF-8 NDJSON，逐行读取，非空行必须是声明 resourceType 的对象，身份为 (resourceType,id,meta.versionId)，versionId 不透明。Observation 对 Patient/P1 的引用允许后到。OperationOutcome 仅作诊断；任何 error 项阻止整批切换，失败时保留旧可用批及下载进度。

requiresAccessToken=true 时仅向可信本地目标发送 Bearer local-demo；为假时不发送，跨 origin 重定向不能携带凭据。/public/manifest 是无凭据版本，/error/manifest 含错误文件，/__requests 可核验实际请求。取消生效后不得安排新下载，已在途请求的结果也不能将取消作业变成可用。所有资源均虚构，不包含真实患者字段。
