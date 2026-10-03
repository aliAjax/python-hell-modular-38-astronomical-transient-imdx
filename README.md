# 天文瞬变事件警报与后续观测

只使用Python标准库和SQLite的模块化服务，默认端口`8338`。支持全天巡天来源、候选事件去重、坐标与亮度测量合并、优先级计算、观测申请、望远镜排程、撤回、重分类、修正、观测队冲突、角色权限、审计，以及望远镜控制系统回传结果批次的对账。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：状态机、优先级、测量合并和排程冲突约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等、整窗口事务、历史数据迁移和审计查询。
- `src/reconciliation.py`：回传批次对账、矛盾结论挂起、失败释放与补观测、人工确认。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8338
```

首次打开旧版本数据库时会自动升级：历史上处于`scheduled`且没有回传记录的窗口被置为`awaiting_result`（待回传），不会被当成已有结果；每个窗口写入一条`migration_awaiting_result`审计。

## 核心对象

`source`为巡天来源，`candidate`为瞬变候选，`telescope`为望远镜，`observation`为后续观测申请/观测窗口，`result_batch`为一次回传批次，`window_result`为某窗口某来源的一条结论。

观测窗口状态：`requested → awaiting_result → completed/released`；结论冲突时进入`result_pending`，人工确认后再落到`completed`或`released`。`awaiting_result`与`result_pending`都继续占用望远镜和观测队时段，`released`释放时段。

## 回传对账

- `POST /api/result-batches`：投递回传批次，身份为`operator/coordinator/admin`。请求体：
  `{"batch_id":"...", "source":"local|archive", "results":[{"window_id":"...","conclusion":"success|failure"}]}`，可带`expected_version`。
- `POST /api/entities/<window_id>/confirm-result`：协调员/主管对矛盾结论裁决，请求体
  `{"confirmed_conclusion":"success|failure","reason":"...","expected_version":N}`。

对账保证：

- 同一`batch_id`只入账一次：原始批次可由入账人重放续传，已写窗口跳过；他人重复提交或篡改内容的重放一律拒绝并写审计。
- 乱序与跨来源重投按到达顺序（`seq`）保留；同一结论的佐证不改变窗口状态。
- 有效窗口更新为`completed`；失败窗口释放望远镜与观测队时段并生成一条`requested`补观测申请。
- 本地与归档结论冲突时，两份结论都保留为`pending_confirmation`，窗口记`result_pending`和到达顺序；确认前既不归档也不重排。若失败后才出现成功结论，已建补观测申请被撤销、时段重新占用。
- 批次中途失败：已提交的窗口保留，批次标记`failed`，按原批次重放时从断点继续；每个窗口在单个事务内提交，不存在半写窗口。
- 并发提交同一批次：数据库写锁串行化，只有一份成功，另一份收到409冲突并写审计。
- 越权角色、过期批次/窗口版本提交均被拒绝并写审计（`result_batch_denied`、`result_batch_stale`、`result_confirmation_*`）。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `POST /api/result-batches`
- `POST /api/entities/<id>/confirm-result`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

轨道和观测计划使用简化的字符串时间窗比较，不包含真实天文历表、可见性预报、望远镜控制系统和观测数据存储。
