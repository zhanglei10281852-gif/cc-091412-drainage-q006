# 管道内窥影像采集索引后端

为管道检测公司管理机器人内窥影像的采集索引：检测任务、设备校准版本、分段文件、
里程区间、人工缺陷标记与发布报告。核心原则：

- **缺片与坐标冲突必须进复核**，系统绝不静默拼接；
- **分段重复上传幂等**，同序号不同校验和只进复核、不覆盖原片；
- **校准只追加新版本**，轮径变化生成新的定位版本，旧版本永久保留；
- **已被报告引用的缺陷冻结**，坐标、校准版本与换算过程随报告快照固化，
  后续重算只提供“新版本投影值”，不改变历史引用；
- **角色可见范围分离**：工程师可见原始影像，其他角色仅见脱敏截图；
- **长任务可暂停/继续/重试**，队列状态与失败原因落盘，服务重启后可恢复。

仅依赖 Python 3.11+ 标准库（`http.server` + `sqlite3`）。

## 运行

```bash
# 可选：指定数据目录（SQLite 与影像文件），默认 ./data
export INSPECTION_DATA_DIR=./data

python scripts/seed_demo.py     # 可选：写入演示数据
python src/index.py             # 默认 0.0.0.0:8000
python -m unittest discover -s tests   # 25 个测试
```

或 `docker compose up --build`（健康检查保持 `/health`）。

## 鉴权与角色

除 `GET /health` 外所有接口需要 `Authorization: Bearer <token>`，内置演示令牌：

| 令牌 | 角色 | 能力 |
|---|---|---|
| `tok-engineer` | 项目工程师 | 全部写操作；可见原始影像与校验和 |
| `tok-dispatcher` | 调度员 | 只读任务/分段脱敏视图 |
| `tok-supervisor` | 监管人员 | 仅已发布报告与脱敏截图 |
| `tok-readonly` | 只读用户 | 仅已发布报告与脱敏截图 |

原始媒体 `/media/<inspection>/<seg>.bin` 仅工程师可取；
脱敏媒体 `/media/redacted/<seg>.png` 所有认证角色可取。
未发布缺陷/草稿报告对外部角色返回 403。

## 里程模型

编码器每圈 `ppr` 脉冲、校准轮径 `d`（毫米），则：

```
scale = π · d / (1000 · ppr)                # 米/脉冲
chainage(r) = scale · (r - 第一片编码器原点)
缺陷里程 = 分段起点里程 + scale · (缺陷读数 - 分段起点读数)
```

合并任务对有序分段做三类检测：序号缺口（缺片）、相邻里程正间隔（里程无覆盖）、
相邻区间重叠（坐标冲突）。命中点之后的分段标记 `provisional=1`（推算值），
并生成阻断性复核项；与管线标称长度偏差超 2% 生成警告项。

## 关键接口

| 方法与路径 | 说明 |
|---|---|
| `POST /api/devices` / `POST /api/devices/{id}/calibrations` | 设备登记 / 追加校准版本（旧版不可改） |
| `POST /api/inspections` | 登记检测任务（管线起止井、标称长度、设备） |
| `PUT /api/inspections/{id}/segments` | 幂等上传分段（业务键 `(任务,序号)` + sha256） |
| `POST /api/segments/{id}/snapshots` | 登记脱敏截图（同 sha 幂等） |
| `POST /api/inspections/{id}/merge-jobs` | 创建合并/定位任务（可指定校准版本） |
| `POST /api/merge-jobs/{id}/{pause,resume,retry}` | 暂停 / 继续 / 重试 |
| `GET /api/merge-jobs/{id}` | 状态、进度、失败原因、定位结果 |
| `GET /api/inspections/{id}/reviews` | 复核队列；`POST /api/reviews/{id}/{resolve,reject}` |
| `POST /api/inspections/{id}/defects` | 缺陷标记（须已有成功定位版本） |
| `GET /api/defects/{id}` | 片段 + 校准版本 + 换算过程 + 是否允许对外发布 |
| `POST /api/inspections/{id}/reports` / `POST /api/reports/{id}/publish` | 报告快照与发布（冻结缺陷） |

### 发布门禁

存在未解决阻断复核项、缺陷位于推算坐标分段、或缺脱敏截图时，
`publish` 返回 409；空报告同样拒绝发布。

## 持久化与恢复

- SQLite（WAL）库文件在 `$INSPECTION_DATA_DIR/index.db`，影像在 `blobs/`；
- 合并任务状态机 `queued|running|paused|succeeded|failed`，进度按分段断点保存；
  进程崩溃后重启时 `running` 一律恢复为 `paused`，`queued` 任务自动续跑，
  `failed` 任务保留失败原因、可手动重试。

## 代码结构

```
src/inspection/
  config.py       运行配置与数据目录
  errors.py       领域错误（404/409/422/401/403）
  security.py     角色、令牌、可见范围
  clock.py        带时区 ISO 8601 时间
  positioning.py  里程换算与冲突检测（纯函数）
  storage.py      SQLite schema/迁移/查询
  service.py      业务用例（上传幂等、合并队列、复核、冻结、发布）
  app.py          HTTP 路由
scripts/seed_demo.py  演示数据（完整管线 + 缺片/重叠管线各一条）
tests/                  unittest（纯逻辑 + HTTP 端到端 + 重启恢复）
```
