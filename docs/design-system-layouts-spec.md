# GAOSHOU PLATFORM - DESIGN SYSTEM & LAYOUT SPECIFICATION
# (高手投研平台 - 象牙暖白松风主题与多版面重构规范)

---

## 一、 全局主题与排版规范 (Global Theme & Typography)

本规范采用 **“象牙暖白松风” (Ivory & Pine)** 视觉风格，致力于构建高密度、高信息量、沉浸感强的量化投研控制台。

### 1. 颜色标记 (Color System Tokens)
*   **页面背景 (Page BG)**: `#fdfbf7` (象牙暖白) - 纸张般舒适的淡黄色基底。
*   **卡片/分栏背景 (Card BG)**: `#f5f2ea` (浅沙灰) - 相比背景略暗，用于勾勒界面层次。
*   **悬停与激活 (Hover & Active BG)**: `#ebe7dc` (暖沙色) / `#eef3f0` (浅苔绿)
*   **主轴边框 (Border Color)**: `#e5dfd3` (精准发丝线，`1px solid`)
*   **首要文本 (Text Main)**: `#22302a` (松烟墨绿) - 替代纯黑，护眼并具备学术 teal 质感。
*   **次级文本 (Text Secondary)**: `#54635c` (暗苔绿)
*   **静默文本 (Text Muted)**: `#7e8d86` (鼠尾草灰)
*   **核心品牌色 (Pine Green)**: `#1b3d32` (松风绿) / `#355e4f` (介质绿)
*   **量化语义状态色 (Quant Statuses)**:
    *   *Ready / Good*: 文字 `#2d6a4f` (软玉绿) | 背景 `#eaf5f0`
    *   *Warning / Gap*: 文字 `#b27a1e` (温赭黄) | 背景 `#fdf6e6`
    *   *Critical / Risk*: 文字 `#a83232` (茜草红) | 背景 `#fbf1f1`
    *   *Neutral*: 文字 `#5c6863` | 背景 `#f2f2ef`

### 2. 字体比例 (Enlarged Typography Scale)
为了保证长时间看盘不疲劳，全局字号相比老版整体放大，规避 11px 以下的微小文本：
*   `--text-xs`: `13px` (微型指标、时间戳、次要 Kicker 标签)
*   `--text-sm`: `15px` (正文、表格单元格数据、常规表单标签)
*   `--text-base`: `17px` (二级区域标题、主数据指标)
*   `--text-lg`: `19px` (卡片/面板标题)
*   `--text-xl`: `22px` (模块页面标题)
*   `--text-2xl`: `26px` (醒目仪表数值)
*   `--text-3xl`: `32px` (核心统计巨型字)
*   `--leading-normal`: `1.5` / `--leading-tight`: `1.25`

### 3. 全局布局骨架变更
*   **右侧上下文栏 (Context Rail) 彻底移除**。
*   主区域宽度拓展为 100% 自适应双栏：`grid-template-columns: var(--sidebar-width) minmax(0, 1fr);`。

---

## 二、 核心页面多版面设计说明 (Page-by-Page Layout Specs)

下文为每个左侧工作栏页面提供的 3 套 Layout 设计，实现 Agent 应允许在页面顶部以 `layoutMode` 状态（如 `'A' | 'B' | 'C'`）进行实时预览与切换。

---

### 1. 今日工作台 (Cockpit Home - `/home`)
*   **功能定位**：决策决策辅助桌。引导研究员启动“今日首要任务”。
*   **三套 Layout 规范**：
    *   **Layout A: Split Pane (分栏终端)**: 取消所有卡片容器。左侧罗列 IDE 项目级垂直流水线节点（01-06）；中间横向呈现 Readiness 评分横幅，下方展示行动建议表格；右侧展示数据就绪点阵与事件记录。
    *   **Layout B: Matrix Audit Sheet (矩阵审计表)**: Excel 财务报表风格。将数据口径状态与行动按钮压缩进同一行。底部以单行滚动日志显示事件流。
    *   **Layout C: Console Dashboard (极客命令行)**: 终端配色（`#1a2420` 基底），左侧为 `$ select.step` 仿真交互输入及操作跳转，右侧为 ASCII 进度条仪表盘。

---

### 2. 数据查看 (Data View - `/data`)
*   **功能定位**：多源股票、指标及舆情口径的一站式多维对比查看器。
*   **Layout A: 三栏联动检索流（IDE 联动风格 - 推荐）**
    ```text
    +========================================================================+
    | [ 切换模式: A | B | C ] 股票代码/简拼检索: [_________]                  |
    +------------------+----------------------------------+------------------+
    | STOCK LIST       | MULTI-DIMENSIONAL DATA PANELS    | CORRELATED EVENTS|
    | * 600519 贵州茅台| [ Tab: 行情数据 | 财务报表 | 舆情 ] | * 09:30 日线同步 |
    | * 000001 平安银行| +------------------------------+ | * 11:00 舆情获取 |
    | * 300750 宁德时代| | Date     | Open   | Close    | | * 昨天 因子落盘  |
    | * 000333 美的集团| +------------------------------+ |                  |
    |                  | | 06-26    | 1500.0 | 1512.2   | |                  |
    |                  | | 06-25    | 1492.1 | 1498.0   | |                  |
    |                  | +------------------------------+ |                  |
    +==================+==================================+==================+
    ```
    *   *DOM 结构*：`.layout-data-a` (Flex/Grid) -> 包含 `.stock-list` (200px), `.data-viewport` (Flex 1), `.sidebar-events` (260px)。
    *   *数据绑定*：左栏列表点击触发 `activeStockCode` 的更新，右栏与中栏动态重载 API 数据。
*   **Layout B: 平铺财务大表网格**
    *   无左栏，整宽表格。顶部为多条件复合筛选器（行业、市值、就绪状态）。主体为密集型 Data Grid，通过 Excel 方式展示所有股票的日线及财务核心指标。
*   **Layout C: 股票多维看板瓷块**
    *   双列网格。左列为大字展示的当前个股基本面与行情图表；右列以瓷块形式横向平铺 `分钟就绪度`、`舆情指数`、`因子覆盖率` 三个小微表格。

---

### 3. 数据同步 (Data Sync - `/data/sync`)
*   **功能定位**：长任务数据拉取与运维审计台。
*   **Layout A: 双栏任务控制流 (Recommended)**
    ```text
    +========================================================================+
    | [ 切换模式: A | B | C ] 一键全量同步: [ RUN ALL ]                       |
    +----------------------------------+-------------------------------------+
    | DATA SOURCE TASK LIST            | LIVE EXECUTION QUEUE & HISTORICALS  |
    | [Sync] 日线行情 ... [ 触发 ] [G] | Running: [=== 72% ===] 财务报表同步 |
    | [Sync] 分钟行情 ... [ 触发 ] [G] | Queue:   1. 舆情爬虫 (Awaiting)     |
    | [Sync] 财务报表 ... [ 触发 ] [W] | History:                            |
    | [Sync] 因子计算 ... [ 触发 ] [R] | * 06-26 09:00: 日线同步成功 (OK)    |
    +==================================+=====================================+
    ```
    *   *DOM 结构*：`.layout-sync-a` -> 左栏 `.source-list` (40% 宽度)，右栏 `.queue-and-history` (60% 宽度)。
    *   *数据绑定*：点击左栏“触发”发送 `syncApi.start(task_type)`，右栏轮询 `syncApi.getStatus()` 获取队列与历史记录。
*   **Layout B: Kanban Pipeline 看板同步**
    *   以 `排队中`、`同步中`、`已完成/就绪` 作为三列看板。任务以卡片形式在看板中移动，并在卡片上直接呈现当前日志 tail 输出。
*   **Layout C: Console Log Stream 纯控制台模式**
    *   顶部展示极窄的任务就绪状态排。下方 90% 空间为统一的 ANSI 滚动日志流阅读器，用户从下拉菜单选择具体任务以实时查看后端 stdout。

---

### 4. 数据浏览器 (Data Explorer - `/explorer`)
*   **功能定位**：底层的 Parquet 分区表或中间因子缓存的 SQL 级元数据探针。
*   **Layout A: SQL IDE 混合布局 (Recommended)**
    ```text
    +========================================================================+
    | SCHEMA TREE    | SQL EDITOR & INTERACTIVE RUNNER                       |
    | |- market_daily| [ SELECT * FROM market_daily LIMIT 100            ]   |
    |    |- date(Str)|                                         [ RUN QUERY ] |
    |    |- code(Str)| +---------------------------------------------------+ |
    |    |- close(F64| | DATE       | CODE      | CLOSE  | HIGH   | VOLUME | |
    | |- stocks      | +------------+-----------+--------+--------+--------| |
    | |- sentiment   | | 2026-06-26 | 600519.SH | 1512.2 | 1520.0 | 45000  | |
    +================+===================================================++
    ```
    *   *DOM 结构*：`.layout-explorer-a` -> 左侧 `.schema-sidebar` (220px)，右侧 `.work-pane` (Flex 列布局) -> 包含顶部文本域 `.sql-input` 和底部 `.result-grid`。
    *   *数据绑定*：SQL 编辑器组件绑定 `sqlQuery` 变量，点击 `RUN QUERY` 按钮调用后端 `parquetExplorerApi.query()`，并刷新表格。
*   **Layout B: 无代码可视化过滤器**
    *   隐藏 SQL 输入，改为下拉列表选择表名，通过 `[+] 添加过滤条件`（如 close > 100）的可视化 Query Builder 组合查询。
*   **Layout C: Parquet 分区物理结构树**
    *   左侧展示物理文件树（具体的 `.parquet` 文件大小、时间），右侧展示文件头 Meta (Schema, row count, compression type) 与前 50 行抽样。

---

### 5. 自选股 (Watchlist - `/watchlist`)
*   **功能定位**：股票池的动态归组与投研标段追踪。
*   **Layout A: 双栏股票池分类网格 (Recommended)**
    ```text
    +========================================================================+
    | UNIVERSE GROUPS    | STOCK TABLE IN GROUP: [ 小市值因子候选池 ]         |
    | * 默认自选池 (12)  | +--------+----------+--------+---------+------------+ |
    | * 核心白马股 (20)  | | 代码   | 名称     | 价格   | 涨跌幅  | 因子就绪度 | |
    | * 小市值候选 (120) | +--------+----------+--------+---------+------------+ |
    | * 周期套利池 (5)   | | 600000 | 浦发银行 | 8.21   | +1.2%   | [ 90% G ]  | |
    | [+ 创建新分组]     | | 000002 | 万科A    | 9.45   | -0.8%   | [ 60% W ]  | |
    +====================+===================================================+
    ```
    *   *DOM 结构*：`.layout-watchlist-a` -> 左栏 `.group-sidebar` (200px) 支持拖拽/编辑，右栏 `.stock-table` (Flex 1) 支持列排序。
    *   *数据绑定*：点击左栏分组获取 `activeGroupId` 并异步调用 `watchlistApi.getStocks(groupId)`。
*   **Layout B: 股票热力瓷块图 (Heatmap/Grid)**
    *   把所有股票平铺为紧凑的方块，以红/绿颜色深度代表今日涨跌幅，方块内部以文字显示代码与主要因子值，右键弹出快捷管理菜单。
*   **Layout C: 股票双列审计面板**
    *   双列等宽。左列显示自选股的行情变动表，右列显示绑定的对应策略触发的买卖信号。

---

### 6. 因子定义 (Factor Definition - `/factor`)
*   **功能定位**：因子算子逻辑管理与预计算覆盖率审计。
*   **Layout A: 因子算子 IDE 分栏 (Recommended)**
    ```text
    +========================================================================+
    | FACTOR TREE  | EXPRESSION CODE WORKSPACE           | PRECOMPUTE PANELS  |
    | |- momentum  | # 动量因子算子                      | Target: [小市值池] |
    |    * Alpha_1 | Close / Delay(Close, 20) - 1        | Status: [ 85% ]    |
    |    * Alpha_2 |                                     |                    |
    | |- value     |                                     | [ START COMPUTE ]  |
    |    * EP_ratio|                                     | Log:               |
    |    * BP_ratio|                                     | * calculating...   |
    +==============+=====================================+====================+
    ```
    *   *DOM 结构*：三栏布局。`.layout-factor-a` -> 包含 `.factor-tree` (220px), `.editor-workspace` (Flex 1), `.precompute-panel` (280px)。
    *   *数据绑定*：编辑器绑定 `factorCode` 状态，右栏覆盖率数据由 `factorApi.getPrecomputeProgress(factorId)` 驱动。
*   **Layout B: 因子元数据网格大表**
    *   整宽表格。以列表形式呈现所有因子，各列展示：`名称` | `状态` | `落盘数据范围` | `依赖项` | `最新计算日期`。
*   **Layout C: 依赖拓扑图布局**
    *   可视化渲染因子的依赖拓扑结构，展示因子是从哪些行情数据（日线、分钟线、财务）通过何种算子链派生出来的。

---

### 7. 因子评估 (Factor Evaluation - `/factor/evaluation`)
*   **功能定位**：评估因子在不同持有期下的表现（IC、多空收益率）。
*   **Layout A: 评估参数与图表双栏 (Recommended)**
    ```text
    +========================================================================+
    | SETTINGS PANEL | REPORT GRAPHICS VIEWPORTS                             |
    | Factor: [A1]   | [ Tab: 累计收益曲线 | IC时序 | 分组柱状图 ]           |
    | Pool:   [全A]  | +---------------------------------------------------+ |
    | Period: [20D]  | | Chart Area                                        | |
    | Rebal:  [1W]   | |                                                   | |
    |                | |                                                   | |
    | [ RUN EVAL ]   | +---------------------------------------------------+ |
    +================+======================================================+
    ```
    *   *DOM 结构*：`.layout-evaluation-a` -> 左栏 `.config-sidebar` (280px) 存放所有表单参数，右侧 `.chart-viewport` (Flex 1) 容纳 ECharts 等图表。
    *   *数据绑定*：点击 `RUN EVAL` 发起 `factorApi.evaluate(params)`，后端返回数据后利用响应式变量重新渲染 ECharts 实例。
*   **Layout B: 2x2 图表大板 (Multi-Chart Grid)**
    *   取消左侧参数栏（移至顶部单行），主体铺满展示四个图表（IC时序、多空超额、分组累计、换手率分布），无滚动条。
*   **Layout C: 历史分析报告流 (Report Log)**
    *   类似一份学术论文格式，从上往下滚动排列，依次为因子简述、单变量检验指标、详细分组收益和风险敞口分析。

---

### 8. 研究实验室 (Research Lab - `/research`)
*   **功能定位**：投研点子孵化日志与证据链归档。
*   **Layout A: 双栏笔记本模式 (Recommended)**
    ```text
    +========================================================================+
    | RESEARCH IDEAS LIST  | NOTEBOOK VIEW & EVIDENCE CHAIN                  |
    | * 小市值因子失效复盘  | # 针对小市值因子的复盘结论                      |
    |   [已归档] 06-25     | 发现由于调仓摩擦增加，实际回撤偏大...           |
    | * 舆情假设性检验      |                                                 |
    |   [进行中] 06-24     | 关联证据链:                                     |
    | * 财务虚假信号审计    | * 因子验证: EP_Ratio [G]   * 回测 #1024 [FAIL]   |
    +======================+=================================================+
    ```
    *   *DOM 结构*：`.layout-research-a` -> 左栏 `.idea-list` (300px)，右栏 `.note-editor` (Flex 1) 支持 Markdown 编辑与证据绑定。
    *   *数据绑定*：右侧编辑器与所选的 `selectedIdea` 双向绑定，证据链部分支持点击跳转到对应回测与因子页。
*   **Layout B: 看板模式 (Kanban Mode)**
    *   按 `Draft` | `In Progress` | `Validated` | `Archived` 四列展示研究卡片，支持卡片的拖拽状态流转。
*   **Layout C: 日志日历流 (Calendar Timeline)**
    *   以时间轴或日历排布的研究日志，重点展示每天的“研究活动点”及实验结论。

---

### 9. 策略回测 (Strategy Backtest - `/backtest`)
*   **功能定位**：策略代码运行、日志调试及绩效评估。
*   **Layout A: IDE 与报告双栏分屏 (Recommended)**
    ```text
    +========================================================================+
    | RUNNER & STRATEGY WRITER          | DIAGNOSTICS & PERFORMANCE REPORT   |
    | [ Select Strategy: SmallCap    ]  | [ Tab: 净值曲线 | 订单流水 | 执行日志 ] |
    | Code Preview:                     | +--------------------------------+ |
    | class SmallCap(Strategy):         | | Cumulative Return: +24.5%      | |
    |    def handle_bar(self, bar):...  | | Max Drawdown:       -12.2%      | |
    |                                   | +--------------------------------+ |
    | [ RUN BACKTEST ]                  | |                                | |
    +===================================+==================================+
    ```
    *   *DOM 结构*：`.layout-backtest-a` -> 左右对开分栏（各占 50%）。左栏为配置与代码，右栏为执行详情与绩效指标。
    *   *数据绑定*：点击 `RUN BACKTEST` 调用 `backtestApi.run()`，右侧切换至“执行日志”页签并拉取 `stdout`，回测完成后重载绩效图表。
*   **Layout B: 三栏式终端面板**
    *   左侧回测历史树（20%），中间策略编辑与参数（50%），右侧详细委托明细及回放（30%）。
*   **Layout C: 纯绩效报告平铺**
    *   无代码和控制台。全宽平铺展示回测绩效指标明细、收益曲线、回撤分布图及行业敞口归因分析。

---

### 10. 模拟 / 实盘 (Live/Paper Trading - `/trade`)
*   **功能定位**：策略运行、持仓诊断与强平风控面板。
*   **Layout A: 交易室驾驶舱 (Recommended)**
    ```text
    +========================================================================+
    | EMERGENCY GUARDRAILS: [ AUTOMATED SUBMIT ACTIVE ] [ FORCE LIQUIDATE ] |
    +------------------------------------+-----------------------------------+
    | EXECUTION SIGNAL STREAM (TAPE)     | PORTFOLIO POSITION DEVIATION      |
    | * 14:55 BUY  600519.SH 100 股 [OK] | Stock   | Target% | Real%  | Dev  |
    | * 14:50 SELL 000001.SZ 500 股 [OK] | 600519  | 10.0%   | 9.8%   | -0.2 |
    | * 14:45 BUY  300750.SZ 200 股 [ERR]| 000001  | 0.0%    | 1.2%   | +1.2 |
    +====================================+===================================+
    ```
    *   *DOM 结构*：顶部为固定风控栏 `.risk-bar`，下方为双栏 `.layout-trade-a` -> 左栏为委托流 `.signal-tape` (50%)，右栏为持仓偏离表 `.position-audit` (50%)。
    *   *数据绑定*：下单信号和偏离表由 `liveTradingApi.status()` 的 WebSocket 或短轮询持续更新，紧急强平绑定 `liveTradingApi.forceLiquidate()`。
*   **Layout B: 风控审计大矩阵**
    *   整宽布局。不分栏，以表格形式展现所有交易策略的仓位偏离度、滑点、延迟状态和下单接口健康度。
*   **Layout C: 多账户资产卡片**
    *   以卡片形式横向平铺展示各个交易账户（如“QMT模拟一”、“实盘主账号”）的净值走势、持仓分布和资金利用率。

---

### 11. 系统运维 (Platform Ops - `/monitor`)
*   **功能定位**：系统健康指标与长轮询进程检测。
*   **Layout A: 服务点阵与双日志台**
    *   左侧展示系统各个子服务的状态色块（Backend, DB, Sync, QMT），右侧展示选中服务的日志 Tail。

---

### 12. 文档中心 (Runbook - `/docs`)
*   **功能定位**：静态知识库。
*   **Layout A: 规范 Markdown 阅读器**
    *   左侧为文档分级目录树，右侧为主文档 Markdown 渲染主体。

---
