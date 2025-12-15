## CEX 企业级整体技术方案（现货 + 杠杆 + 合约）

**技术选型**：撮合/行情（Go），其余业务（Java），事件驱动 + CQRS + 复式记账总账（Ledger）为真相源；钱包域独立、签名强隔离；强平/保险基金/ADL/Portfolio Margin 全量支持。

---

## 1) 目标与原则

### 1.1 目标（企业级）
- **资金安全**：私钥/签名/冷钱包强隔离；提币审批与审计完备；对账可闭环。
- **强一致资产**：所有资产变化进入 **Ledger 复式记账**，余额为投影，可回放重建。
- **低延迟交易**：撮合内存簿 + 顺序事件流 + 可恢复（WAL+快照/回放）。
- **可扩展与可运维**：K8s、灰度、熔断开关、可观测、演练与恢复工具。
- **可合规**：KYC/AML/KYT、审计留痕、权限最小化与关键操作双人复核。

### 1.2 核心原则
- **撮合不改余额、不触链**：撮合只产出 `order/trade/book` 事件。
- **Ledger 为真相源**：余额/仓位/报表/推送均由事件与总账驱动。
- **至少一次投递 + 业务幂等 = 等价“恰好一次”**：Outbox + 唯一约束。
- **同一订单簿顺序一致**：按 `instrumentId` 分片与顺序消费，事件带 `matchSeq`。

### 1.3 全局工程规范（建议在开工前冻结）
- **ID 与关联**
  - **业务单号**：`orderId/tradeId/depositId/withdrawId/liquidationId/adlId/portfolioLiqId` 全局唯一。
  - **关联 ID**：跨服务链路统一使用 `correlationId`（优先取业务单号，例如强平链路用 `liquidationId`）。
  - **幂等键**：所有“扣钱/下单/提币/借贷”等写接口必须支持 `Idempotency-Key`；内部消息也要有 `idempotencyKey/clientOrderId`。
- **金额与精度（强制）**
  - **禁止使用 float/double**（包括前端与后端）。
  - 统一使用 **定点整数（atomic amount）** 或 **BigDecimal + 固定 scale**；建议事件与存储采用 `amountAtomic` + `assetScale`（或资产表给定 scale）。
  - 统一舍入规则（向下/银行家舍入）按业务类型固定，避免跨语言差异。
- **时间与序号**
  - 服务节点开启 NTP；事件统一携带 `occurredAtMs`。
  - 撮合事件必须携带 `matchSeq`（同 `instrumentId` 单调递增）；深度事件携带 `bookSeq`（可复用 `matchSeq` 或独立）。
- **唯一约束（落库）**
  - Ledger：`(businessType, businessId)` 唯一。
  - Deposit：EVM/TRON `(chainId, txHash, logIndex)` 唯一；BTC `(chainId, txHash, vout)` 唯一。
  - Withdrawal：`withdrawId` 唯一；EVM 额外建议 `(chainId, fromAddress, nonce)` 唯一；BTC UTXO `(txHash, vout)` 唯一。

---

## 2) 逻辑架构与服务拆分

### 2.1 Go（低延迟路径）
- **`match-engine`**：按 `instrumentId` 分片（单簿单线程/actor），维护内存订单簿；输入 `order_commands`，输出 `order_events/trade_events`；维护 `matchSeq`；WAL + snapshot。
- **`market-data`**：消费 `trade_events`（可选再消费 `book_delta`），生成 `ticker/kline/depth/trades`，对外公共 WS；深度采用 snapshot+delta（含序号/校验）。
- **`ws-gateway`（建议）**：公共行情 WS 与私有 WS 的连接与路由（可拆两套网关）。

### 2.2 Java（企业业务与强一致资产/风险）
- **`api-gateway` / `bff`**：鉴权、签名验签、限流、聚合接口、灰度。
- **`auth-service`**：登录、2FA、API Key、设备风控。
- **`user-service`**：用户资料、KYC 状态、等级与权限。
- **`order-service`**：下单/撤单入口、幂等（`Idempotency-Key`/`clientOrderId`）、订单读模型；**系统订单调度器**（强平/ADL/MM/组合强平统一限流与优先队列）。
- **`risk-service`**：统一风控入口（现货余额、杠杆负债、合约 IM/MM、频率、价格保护、出金限制）。
- **`ledger-service`（核心）**：复式记账（Postgres 强一致）、幂等、审计、对账口径；发布 `ledger.entries`。
- **`balance-service`**：消费 `ledger.entries` 生成余额投影（可用/冻结/占用），缓存 Redis，发布 `balance.updates`。
- **`position-service`**：消费 `trade_events` 更新仓位、已实现/未实现 PnL、保证金占用、风险指标；发布 `position.updates`。
- **`pricing-service`**：指数价/标记价/资金费率；发布 `pricing.*`。
- **`funding-service`**：资金费率结算（可做应计 + 定点入账）。
- **`liquidation-service`**：强平触发、分批执行（slicing）、价格带（band）降级、案件管理。
- **`adl-service`**：保险基金不足触发 ADL；候选排序、分批执行（走撮合）。
- **`margin-service`**：杠杆借/还、利息计提、强平逻辑（可并入 liquidation-service 但建议独立）。
- **`wallet-service`**：充值/提币/归集/补热状态机；与 signer/节点适配交互；对账与熔断。
- **`chain-watcher`**：多链监听（EVM/TRON/BTC），处理确认数与 reorg。
- **`node-adapter`**：节点适配（广播/查询/估费/nonce/UTXO）。
- **`signer-service`（强隔离域）**：HSM/KMS/MPC/TSS；审批/限额/时间锁；只提供 payload→签名。
- **`ops-console/ops-service`**：运营后台、审批流、开关、对账、重建回放工具。
- **`notify/ws-notify`**：私有事件写入与推送编排（与 `ws-gateway` 配合）。

---

## 3) 产品与账户模型

### 3.1 Instrument（统一抽象）
- **Spot**：`BTC/USDT`
- **Margin**：同 spot 对，但支持借贷（全仓/逐仓）
- **Derivatives**：永续 `BTCUSDT-PERP`、交割 `BTCUSDT-YYYYMMDD`
- 合约参数：`contractMultiplier/contractValue`、`pnlFormulaType(LINEAR/INVERSE)`、`marginCurrency`、`tickSize/stepSize`

### 3.2 子账户模型（建议）
- `SPOT`、`MARGIN`、`DERIV`（可选 `FUNDING`）
- Deriv 支持 `CROSS/ISOLATED`
- Portfolio Margin：在风险计量层启用，不推翻总账模型（仅改变 IM/MM 计算与占用规则）

---

## 4) 数据与中间件
- **PostgreSQL**：Ledger、订单元数据、仓位、钱包状态机、强平/ADL 案件、配置、Outbox（真相源）。
- **Kafka**：事件总线（命令/事件/投影/推送通知）。
- **Redis**：限流/锁/短期缓存/WS 路由会话。
- **ClickHouse**：成交、Kline、报表、风控分析（事实仓）。
- **对象存储**：撮合快照、审计归档、报表导出。
- **可观测**：Prometheus/Grafana、OpenTelemetry、日志检索（OpenSearch/ELK）。

---

## 5) Kafka Topic 设计（摘要）
- `trading.order_commands`（key=`instrumentId`，Java→Go）
- `trading.order_events`（key=`instrumentId`，Go→多方）
- `trading.trade_events`（key=`instrumentId`，Go→仓位/账本/行情/ETL）
- `market.book_delta_events`（可选，Go→行情）
- `pricing.mark_price_updates` / `pricing.index_updates` / `pricing.funding_rate_updates`
- `ledger.entries`、`balance.updates`、`position.updates`
- `wallet.deposit_events`、`wallet.withdraw_events`
- `ws.notifications`（key=`userId`，投影落库后驱动私有推送）

**要求**：所有事件包含 `eventId`、`occurredAt`、`schemaVersion`、`correlationId`；交易事件必须带 `matchSeq`；消费端按幂等键去重。

### 5.1 投递语义与一致性（必须）
- **Outbox Pattern**：所有“写库 + 发 Kafka”的服务（`order/ledger/wallet/position/...`）必须用 outbox 表保证一致性。
- **消费幂等**：所有消费者以业务唯一键去重（如 `tradeId`、`journalEntryId`、`depositId`、`withdrawId`），实现“至少一次投递”下的业务等价“恰好一次”。
- **保留期建议**（用于回放/排障，按成本可调）
  - `trading.trade_events`: 30d（至少 7d）
  - `trading.order_events`: 14d（至少 3d）
  - `market.book_delta_events`: 3d（至少 24h）
  - `ledger.entries`: 30d（资金排障建议更长或永久入仓）

---

## 6) 撮合（Go）设计要点

### 6.1 顺序与分片
- Kafka 分区 key = `instrumentId`，保证同簿顺序。
- 每个 instrument 维护 `matchSeq` 单调递增（写入 `order_events/trade_events`）。

### 6.2 订单簿与撮合
- 价格优先、时间优先（FIFO）。
- 订单类型：Limit/Market/IOC/FOK/Post-Only；STP（自成交防护）；Deriv 支持 reduce-only（硬校验放 Java，撮合弱校验/透传）。

### 6.3 可恢复
- WAL（append-only）+ 快照（snapshot）+ 回放；或从 Kafka offset 回放重建（需足够保留期）。
- 事件输出需透传 `source/correlationId/sliceId/riskSnapshotId/refPrice/bandBps` 用于审计，但不影响撮合排序。

---

## 7) 行情（Go）与 WS
- Depth：snapshot + delta（绝对量更新），带 `bookSeq`/checksum；断档客户端拉 snapshot 重置。
- Ticker/Trades/Kline：消费 `trade_events` 聚合；Kline 写入 ClickHouse（或 Kafka→CH MV）。
- 公共行情 WS 与私有 WS 建议隔离域名与限流策略。

---

## 8) 资产系统：Ledger（核心）与投影

### 8.1 科目体系（建议最小完备集）
- 用户：`U.Spot.Available/Frozen`、`U.Margin.Available/Frozen/LoanLiability/InterestAccrued`、`U.Deriv.Available/MarginUsed`
- 平台钱包：`P.HotWallet`、`P.ColdWallet`、`P.OnChainClearing`、`P.InFlight`
- 费用：`P.FeeIncome`、`P.GasExpense`
- 合约结算与兜底：`P.DerivSettlementPool`、`P.InsuranceFund`、`P.BadDebt`、`P.ADLPool`

### 8.2 分录模板（摘要）
- 现货成交：买卖双方资产互换 + 手续费入 `P.FeeIncome`
- 杠杆：借贷负债科目 + 利息计提 + 强制还款（卖出现货→还款分录）
- 合约：保证金占用、手续费、PnL 进出 `DerivSettlementPool`、Funding、BadDebt/Insurance/ADL
- 钱包：充值入账/冲正、提币冻结/在途/出账/解冻、归集/补热、gas 成本
- 账本规则：`businessType + businessId` 唯一；借贷平衡校验；余额不可穿透。

### 8.3 现货/杠杆“先冻结再撮合”的明确策略（建议默认）
- **下单前冻结**（通过 Ledger）：`U.<sub>.Available -> U.<sub>.Frozen`，以 `orderId`/`clientOrderId` 幂等。
- **成交结算**：撮合产出 `trade_events` 后，按 `tradeId` 幂等记账完成真实扣减/资产交换/手续费。
- **撤单释放**：收到 `ORDER_CANCELED` 后，按 `orderId` 幂等释放冻结。

> 这样风控与可用余额语义清晰，撮合无需依赖余额，且易审计与对账。

---

## 9) 钱包（上链）全量方案（EVM + TRON + BTC）

### 9.1 组件与隔离
- `wallet-service`（状态机编排）、`chain-watcher`（监听确认/reorg）、`node-adapter`（广播/查询/估费/nonce/UTXO）、`signer-service`（强隔离签名）。
- signer 域：mTLS、最小权限、审批与时间锁、审计留痕；冷热分层（热限额、冷多签/MPC）。

### 9.2 充值（Deposit）
- 识别：EVM/TRON 解析 Transfer 日志；BTC 扫 vout；幂等键：EVM/TRON `txHash+logIndex`，BTC `txHash+vout`
- 确认策略（可配置）：`minConfirmations + reorgWindow + creditPolicy`
- reorg：窗口内 blockHash 校验，必要时冲正分录（`DEPOSIT_REVERSAL`）并联动风控（暂停提币/告警）。

### 9.3 提币（Withdrawal）
- 申请→风控/合规→审批→Ledger 冻结→构建 tx（nonce/UTXO 锁）→签名→广播→确认→Ledger 出账；失败则解冻。
- EVM 支持 speed-up/cancel（同 nonce 替换）；所有状态机 CAS 更新，幂等以 `withdrawId` 与 `txId/txHash` 约束。
- 引入 `P.InFlight`：广播后平台侧资金进入在途，确认后完成出账，更易对账与解释 pending。

### 9.4 归集/补热与 ERC20 Gas Top-up
- ERC20 归集需 gas：top-up（受白名单/上限/失败次数控制）→ token sweep。
- 归集与补热均需在账本体现（平台科目迁移），对账可闭环。

### 9.5 对账与熔断
- 热/冷/保险基金地址集合的链上余额 ↔ 对应科目余额（解释项：`InFlight`）。
- 差异不可解释：自动暂停提币（链/资产维度），触发工单与补偿任务（backfill/track-tx）。

### 9.6 默认链参数（可作为初始值，按实际链稳定性调整）
- **EVM L1（ETH）**：`minConfirmations=12`，`reorgWindow=256`，`creditPolicy=CONFIRMED_ONLY`
- **EVM L2（ARB/OP 等）**：`minConfirmations=20`，`reorgWindow=512`，`creditPolicy=CONFIRMED_ONLY`
- **TRON**：`minConfirmations=20`，`reorgWindow=512`，`creditPolicy=CONFIRMED_ONLY`
- **BTC**：`minConfirmations=3~6(按金额分层)`，`reorgWindow=144`，`creditPolicy=CONFIRMED_ONLY`

### 9.7 钱包状态机（摘要，落地时应固化枚举）
- **Deposit**：`DETECTED -> PENDING_CONFIRMATION -> CONFIRMED -> CREDITED`，异常：`REORGED -> REVERSED`
- **Withdrawal**：`REQUESTED -> RISK_REVIEW -> (APPROVAL_PENDING) -> APPROVED -> FROZEN -> TX_BUILDING -> TX_SIGNING -> TX_BROADCASTED -> TX_CONFIRMED -> DEBITED`，异常：`FAILED/CANCELLED -> UNFROZEN`
- **Sweep/Replenish**：`CREATED -> TX_BUILDING -> TX_SIGNING -> TX_BROADCASTED -> TX_CONFIRMED -> ACCOUNTED`

---

## 10) 风控、强平、保险基金、ADL、Portfolio Margin

### 10.1 定价
- Index：多源加权 + 异常剔除 + TWAP
- Mark：基于 index 与 premium（带 clamp/band）
- 强平触发与估值以 Mark 为准（不使用 last）。

### 10.2 风险档位（Risk Limit Tiers）
- `instrument_risk_limit_tier`：`min/maxNotional, IMR, MMR, mmDeduction, maxLeverage`
- 公式：`IM=N*IMR`；`MM=N*MMR - deduction`；触发：`equity <= MM + buffer + hysteresis`

### 10.3 强平（Liquidation）
- 触发：position-service/risk-service 基于 mark 与 equity/mm。
- 执行：slicing（按深度模拟 VWAP/滑点）+ IOC 限价（price band），带全局 token bucket（每秒冲击限额）。
- 失败/不成交降级：逐步放宽 band（上限）、调整 slice、backoff、必要时 reduce-only/暂停开仓。

### 10.4 穿仓与保险基金
- 结算时若余额不足：形成 `P.BadDebt`，并保持 `DerivSettlementPool` 守恒；用 `P.InsuranceFund` 冲销坏账。
- 保险基金不足：进入 ADL。

### 10.5 ADL（方式 A：走撮合）
- 建 `adl_case`，将缺口从 `DerivSettlementPool` 转入 `ADLPool`（可审计）。
- 候选：同合约对手方向盈利仓位；评分/灯级（盈利、杠杆、名义、流动性）。
- 分批 reduce-only 系统单（source=ADL）走撮合；按成交回收至 `ADLPool` 直至归零；超限触发熔断/暂停。

### 10.6 Portfolio Margin 与组合强平
- 简化 SPAN：多情景冲击下的最差损失作为 `portfolioIM`，`portfolioMM` 用比例或独立情景计算；存 `risk_snapshot_portfolio` 便于审计。
- 组合强平执行顺序（默认）：先减 Deriv 风险贡献最大仓位 → 收缩杠杆负债（卖现货还款）→ 再卖 Spot 降净敞口；每一步系统单均受调度器统一限流与优先级控制。

### 10.7 费率/返佣/做市的入账口径（避免审计争议）
- **手续费收入**：统一先记入 `P.FeeIncome:<asset>`。
- **做市返佣/负 maker**：不要用“负手续费”隐式抵扣；建议单独业务类型 `FEE_REBATE`，以分录形式从 `P.FeeIncome -> U.<sub>.Available`（便于审计与报表）。
- **系统订单标识**：强平/ADL/MM/组合强平的成交必须在 `trade_events` 与后续分录中可追踪（`source + correlationId + sliceId`）。

---

## 11) 系统订单调度器（order-service 内）
- 统一纳管：LIQUIDATION / ADL / MM / BACKSTOP / PORTFOLIO_LIQ
- 优先级（示例）：LIQ(100) > PORTFOLIO(90) > ADL(80) > BACKSTOP(60) > MM(40)
- 限流：每 instrument 名义/秒、每 source 操作/秒、全局上限；in-flight 上限（防回报延迟雪崩）
- 反馈闭环：订阅 `order_events` 分类拒单原因与降级策略（band/重试/退避/熔断）。

---

## 12) 私有推送（断线补发 sinceEventId）
- 事件存储：`user_event_store`（短期 TTL 30–60min），按 `(userId,eventId)` 索引。
- 推送时机：**投影落库后写入 event_store，再推送**，确保推送与查询一致。
- WS：支持 `sinceEventId` 补发 + ACK；客户端去重（至少一次投递）。
- 扇出与背压：Redis 维护 `userId -> sessions` 路由；高频事件合并（balance/position debounce）；慢连接断开后重连补发。

### 12.1 断线兜底（必须）
- 若 `sinceEventId` 已过期：服务端返回 `NEED_RESYNC`，客户端执行“全量刷新”（open orders / balances / positions）后再订阅。
- 私有推送的 **真相一致性**：推送消息必须来自“已落库读模型”（或 event_store），避免“推送到了但查询不到”。

---

## 13) 运营后台（Ops）与审计
- 提币审批、补热、链节点切换、对账看板、熔断开关、参数变更审计（双人复核/时间锁）。
- 重建回放工具：orders/orderbook/balances（从 ledger 重算、从 Kafka 回放）。
- 工单与审计日志：覆盖签名审批、资金调拨、风控参数、熔断与恢复、补偿与重建操作。

---

## 14) 部署与可观测（摘要）
- K8s：撮合/行情独占节点（建议），Java 服务水平扩展；Kafka/PG/Redis/CH 高可用。
- 可观测：交易链路延迟、撮合 p99、Kafka lag、钱包广播失败率、对账差异、强平队列长度、保险基金消耗速率、WS 背压与 resync 率。
- 熔断：全局/链/资产/instrument 维度开关；异常自动触发并可审计解除。

---

## 15) 测试与验证（先到这里）
- 先落 **基础契约测试**：proto 编译 + 必填字段/语义断言（tradeId/matchSeq/source/correlationId 等）。
- 回放测试与更完整的 CI/演练后续补充。

---

## 16) 落地交付物（建议仓库结构）
- `contracts/`：`proto/` + `topics.yaml` + schema version policy
- `docs/design/`：架构图、关键时序（下单/充提/强平/ADL/组合强平）、科目体系与分录模板、运维开关与对账口径
- `ops/runbooks/`：熔断、对账修复、节点故障、撮合恢复等运行手册
