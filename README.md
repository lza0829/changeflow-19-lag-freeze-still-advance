# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。
- **Lag Demo CDC（滞后演示）**：JSONL，seq 1→6 连续，默认阈值 600s 下 seq 2/5 两条超阈，其余在阈内，用于分别跑通冻结与记票放行。

首次启动时会在数据目录（`DATA_DIR/fixtures`）生成时间贴近当前的夹具副本：包内夹具时间固定会变旧，运行期优先读数据目录副本，包内原文件仅作回落。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致。

## 滞后 SLA 与游标冻结（lag_sla_breach）

服务于游标推进策略的滞后分析，不是类目指标大盘，也不是「条数对账通过才提交水位」的提交闸。

- **口径**：`lag_seconds = 落地时刻(land_ts) − event_ts`；同批拟应用事件共用分析开始时刻作为 land_ts；样本仅含本批拟应用事件（重复序、缺口隔离事件不入样）；分位数取 nearest-rank，给出 min / 中位 / P95 / max 与超阈条数。
- **可配置保存**：阈值 `lag_sla_threshold_seconds`（正整数秒）与策略 `lag_sla_policy` 存 settings 表，在「滞后 SLA」页保存，非法值返回 400。
- **互斥策略**：
  - `freeze_cursor`（默认）：任一事件超阈即冻结——本批不写 Sink、不写 applied_seqs、不抬高已提交游标，账本记 `lag_frozen`，并为每条超阈事件开违约票（action=freeze_cursor）。
  - `ticket_and_advance`：照常写 Sink 并推进游标，但报告标记 breach，每条超阈事件开违约票（action=ticket_and_advance）可查。
- **同源**：每次 run 的样本数 / 超阈数 / 策略动作 / 游标是否推进同时落在 apply_runs.report、lag_run_stats、lag_breach_tickets；应用台、滞后 SLA 页、违约列表、游标页读同一批表，刷新与服务重启后仍在。
- 演示：选 Lag Demo CDC，默认策略跑一次得 `frozen`（游标 0→0）；在滞后 SLA 页切到记票放行再跑，得 `succeeded`（游标 0→6）且 2 张票可查。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见题单）

当前仓库已实现「连续序幂等应用」主链与「滞后 SLA 游标冻结」。缺口等待/跳过、乱序缓冲、事务整批、双 sink 部分失败、毒丸选择性跳过等仍在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。
