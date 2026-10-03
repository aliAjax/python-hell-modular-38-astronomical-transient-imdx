# 天文瞬变事件警报与后续观测

只使用Python标准库和SQLite的模块化服务，默认端口`8338`。支持全天巡天来源、候选事件去重、坐标与亮度测量合并、优先级计算、观测申请、望远镜排程、撤回、重分类、修正、观测队冲突、角色权限和审计。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：状态机、优先级、测量合并和排程冲突约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等和审计查询。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8338
```

## 核心对象

`source`为巡天来源，`candidate`为瞬变候选，`telescope`为望远镜，`observation`为后续观测申请。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 回传对账（望远镜控制系统）

窗口结束后回传结果批次，批次经`batch_key`幂等：重复投递只入账一次，已入账窗口保留，其余按原批次重试，不留半写状态。同一窗口支持本地回传（`local`）与归档复核（`archive`）两套来源：结论一致时有效窗口更新观测（成功标记完成，失败释放望远镜与观测队时段并生成补观测申请）；结论矛盾时保留双方结论、记录到达顺序、标记`pending_confirmation`，确认前不归档不重排。两人同时提交同一批次只让一份成功；越权或过期版本提交拒绝并留审计。

- `POST /api/batches`：提交回传批次，`{batch_key, source, items:[{window_id, conclusion, payload}], expected_version?}`，重复投递返回`200`
- `GET /api/batches`、`GET /api/batches/<batch_key>`：批次与条目状态
- `GET /api/windows`、`GET /api/windows/<window_id>`：窗口对账状态（`pending_return`/`pending`/`confirmed`/`pending_confirmation`）
- `POST /api/windows/<window_id>/confirm`：确认矛盾窗口，`{decision: "local"|"archive"|{conclusion, payload}}`
- `POST /api/upgrade`：历史数据补`pending_return`状态（幂等，仅管理员）

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

轨道和观测计划使用简化的字符串时间窗比较，不包含真实天文历表、可见性预报、望远镜控制系统和观测数据存储。
