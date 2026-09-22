# 管道内窥影像采集索引后端

面向管道检测公司的影像采集索引服务：管理**检测任务、设备校准、分段文件、里程区间、人工标记与发布版本**。同一盘视频分段重复上传幂等；里程缺失或坐标冲突一律进入人工复核，不做静默拼接；校准替换只能产生新的定位版本；已被报告引用的缺陷定位记录冻结，旧数据不能改动它。

纯 Python 标准库实现（`http.server` + SQLite/WAL），无需第三方依赖。需要 Python 3.11+。

## 运行

```bash
python src/index.py                 # 默认 .runtime/inspection.db，监听 8000
INSPECTION_DB=/data/idx.db python src/index.py
python -m unittest discover         # 全部测试（30 个）
python -m inspection.seed .runtime/demo.db   # 写入样例数据（在 src/ 下执行）
docker compose up --build
```

## 领域模型与不变量

| 概念 | 表/实体 | 关键规则 |
|---|---|---|
| 管线起止井 | `pipelines` | 起点井、终点井、全长 |
| 设备参数 | `devices` | 名义轮径等 |
| 校准记录 | `calibrations` | **只增不改**；新校准 `supersedes` 旧校准，保留来源/接收时间 |
| 定位版本 | `loc_versions` | 校准+锚点（里程零点）的固化版本；换校准必须新建版本并引用旧版本 |
| 检测任务 | `tasks` | 绑定一条管线与一台设备 |
| 分段文件 | `segments` | `UNIQUE(task_id, sha256)`：重复上传返回同一记录，文件事实不被改写 |
| 合并作业 | `merge_jobs` | 分阶段 `validate→anchor→convert→conflict→index`，状态与失败原因全部落盘 |
| 里程区间 | `segment_ranges` | 只有合并成功才写入；已被缺陷引用的区间拒绝改写 |
| 复核单 | `reviews` | `missing_gap` / `coordinate_conflict` / `range_overflow` / `prerequisite_pending` |
| 缺陷 | `defects` + `defect_sightings` | 每条定位记录带完整里程换算过程；`candidate→confirmed`，新版本复算后旧记录 `superseded`，**已发布的冻结** |
| 报告/发布版本 | `reports` / `releases` | 报告钉住具体 sighting；发布后条目不可增删；发布版本不可变，只能撤回 |
| 影像附件 | `assets` | `raw_frame`（原始）与 `redacted`（脱敏）分开登记，按角色裁剪可见性 |

### 里程换算

合并与人工标记共用同一纯函数（`inspection.merger.MergeWorker.convert_pulse`），结果与逐步过程都存入 `defect_sightings.calc_json`：

```
distance_m = anchor_m
           + (pulse_reading - anchor_pulses)
             * π * wheel_diameter_m / 1000
             / pulses_per_rev
```

### 不静默拼接

合并的 `conflict` 阶段按 0.10m 容差做几何核对：

- **缺口**（与相邻分段间距离 > 容差）→ 作业 `blocked`，开 `missing_gap` 复核；
- **重叠**（疑似重复/轮径跳点）→ `coordinate_conflict` 复核；
- **越出起止井** → `range_overflow` 复核；
- **前置分段缺失**（`prereq_segment_id` 未合并）→ `prerequisite_pending`/`missing_gap` 复核。

冲突/越界复核必须由有权角色填写**非空处理说明**后才放行；缺失类在缺口补齐时自动结案并让后继作业重新排队。任何阶段异常都记录 `last_error`，作业可 `failed`/`blocked` 后继续。

### 重启恢复

- 队列状态、阶段、尝试次数、`last_error`、全局暂停开关都在 SQLite；
- 启动时 `MergeWorker.recover()` 把中断在 `running` 的作业退回 `queued`，从头阶段重跑；
- `blocked`/`failed` 作业及其原因原样保留。

## 角色（`X-Role` 请求头）

| 角色 | 头部取值 | 写入 | 发布 | 原始影像 | 脱敏截图 | 未发布内容 |
|---|---|---|---|---|---|---|
| 运维人员 | `ops`（或百分号编码的`运维人员`）| ✅ | ❌ | ✅ | ✅ | ✅ |
| 调度员 | `dispatcher`/`调度员` | ✅ | ✅ | ❌ | ✅ | ✅ |
| 监管人员 | `regulator`/`监管人员` | ❌ | ❌ | ❌ | ✅（仅 active 发布版本内）| ❌ |
| 只读用户 | `readonly`/`只读用户` | ❌ | ❌ | ❌ | 同上 | ❌ |

## HTTP 接口（均为 JSON，写接口需相应角色）

```
POST /pipelines /devices
POST /devices/{id}/calibrations            # 新校准（可带 supersedes_id）
POST /tasks
POST /tasks/{id}/loc-versions              # 新定位版本（可带 supersedes_id）
GET  /tasks/{id}                            # 任务总览：作业/区间/复核/队列开关
POST /tasks/{id}/segments                  # 上传分段（可一并给 loc_version_id/prereq_segment_id 自动入队）
GET  /tasks/{id}/segments                   # 分段 + 原始读数 + 合并区间
POST /segments/{id}/merge                  # 单独入队
GET  /jobs                                  # 合并队列（?task_id=）
POST /jobs/{id}/pause | /resume
POST /queue/pause    {"paused": true|false}
GET  /reviews        (?task_id=&status=)
POST /reviews/{id}/resolve   {"resolution": "必须填写处理说明"}
POST /tasks/{id}/defects
POST /defects/{id}/sightings                # 人工标记（服务端按定位版本换算里程）
GET  /defects/{id}                          # 完整索引：片段/校准/换算过程/发布判定
POST /sightings/{id}/assets                 # raw_frame（仅 ops）/ redacted
POST /tasks/{id}/reports
POST /reports/{id}/items    {"sighting_id": "..."}
POST /reports/{id}/publish | /withdraw
POST /tasks/{id}/releases    {"report_id","version_label"}
POST /releases/{id}/retract
```

### 查询一条缺陷返回什么

`GET /defects/{id}` 对每条定位记录返回：

- `segment`：所属文件片段（批次、文件名、sha256、设备时钟、接收时间）与 `segment_range`；
- `loc_version` + `calibration`：定位版本与所用校准（轮径、脉冲当量、替代链）；
- `conversion`：里程换算的公式、输入与逐步过程；
- `assets`：按角色裁剪后的影像附件；
- `reports` / `releases`：引用它的报告与发布版本及其状态；
- `publish`：`publishable`（能否进入新的发布版本）、`in_active_release`（当前是否在有效期发布中）与逐条阻塞原因（未确认/有更新定位版本/缺脱敏截图/存在未决复核）；
- 顶层 `externally_publishable` 与 `externally_visible_now` 概括**当前是否允许对外发布**。

## 样例数据（需求场景）

`python -m inspection.seed` 构造：管线 WS-YL-017（起 YL-起#12 / 止 YL-止#31，320m）、设备 RBT-Q5-03、两次校准（200mm → 磨损后 197.5mm，后者替代前者）、同一盘视频两个分段（0–100m 与 120–220m，中间 20m 缺口）。结果：第一段合并 `done`，第二段 `blocked` 并产生 `missing_gap` 复核——不会被悄悄拼接。
