<template>
  <div class="docs-container">
    <aside class="docs-sidebar">
      <div class="sidebar-title">文档中心</div>
      <nav class="sidebar-nav">
        <a v-for="sec in sections" :key="sec.id" :href="`#${sec.id}`" class="nav-link">
          {{ sec.title }}
        </a>
      </nav>
    </aside>
    <main class="docs-main">
      <h1>策略回测使用手册</h1>

      <!-- 概述 -->
      <section id="overview">
        <h2>概述</h2>
        <p>Gaoshou 策略回测支持两种模式，覆盖从快速因子验证到完整策略模拟的全场景：</p>
        <table class="info-table">
          <thead>
            <tr><th></th><th>表达式模式</th><th>脚本模式</th></tr>
          </thead>
          <tbody>
            <tr><td><strong>引擎</strong></td><td>向量化（Vectorized）</td><td>事件驱动（Event-Driven）</td></tr>
            <tr><td><strong>执行方式</strong></td><td>按因子值分层 → 分组调仓</td><td>逐 Bar 模拟交易 → 订单→风控→撮合→成交</td></tr>
            <tr><td><strong>输入</strong></td><td>单行因子表达式</td><td>init() + handle_bar() 完整 Python 脚本</td></tr>
            <tr><td><strong>数据频率</strong></td><td>仅日线</td><td>日线 + 分钟线（bar_type=minute）</td></tr>
            <tr><td><strong>适合场景</strong></td><td>因子有效性验证、分层收益分析</td><td>策略研发、仓位管理、风控测试、日内策略</td></tr>
          </tbody>
        </table>
      </section>

      <!-- 事件驱动引擎 -->
      <section id="event-driven">
        <h2>事件驱动引擎</h2>
        <p>脚本模式背后是一个完整的事件驱动回测引擎，模拟真实交易的全生命周期：</p>
        <ol>
          <li><code>ENGINE_START</code> — 引擎初始化，调用 <code>init(context)</code></li>
          <li><code>BEFORE_TRADING</code> — 开盘前，调用 <code>before_trading(context)</code>（可选，适合计算日内信号）</li>
          <li><code>PRE_BAR</code> — Bar 预处理（系统事件）</li>
          <li><code>BAR</code> — 主策略逻辑，调用 <code>handle_bar(context, bar_dict)</code></li>
          <li><code>POST_BAR</code> — Bar 后处理，订单推入风控链</li>
          <li><code>AFTER_TRADING</code> — 收盘后，调用 <code>after_trading(context)</code>（可选）</li>
          <li><code>SETTLEMENT</code> — 日终结算，持仓市值按收盘价重估</li>
          <li><code>ENGINE_END</code> — 回测结束，计算绩效指标</li>
        </ol>
        <p>事件通过 <code>EventBus</code> 广播，策略和系统组件各自订阅感兴趣的事件。每个交易日上述事件按顺序触发一次。</p>

        <h3>bar_type 参数</h3>
        <p>回测配置支持 <code>bar_type</code> 参数控制数据频率：</p>
        <ul>
          <li><code>daily</code>（默认）— 仅加载 <code>klines_daily</code> 日线数据</li>
          <li><code>minute</code> 或 <code>1m</code> — 同时加载 <code>klines_daily</code> 和 <code>klines_minute</code>，策略可通过 <code>context.get_intraday()</code> 访问分钟数据</li>
        </ul>
        <p>注意：事件驱动引擎的 <code>handle_bar</code> 始终按日触发（每个交易日一次），分钟数据通过独立的数据访问方法获取，不影响事件节奏。</p>
      </section>

      <!-- 表达式模式 -->
      <section id="expression-mode">
        <h2>表达式模式</h2>
        <p>输入因子表达式，系统自动：计算每日因子值 → 按因子值排序分组 → 等权买入各组 → 输出分层收益曲线。</p>

        <h3>表达式语法</h3>
        <p>表达式使用 <code>open</code>/<code>high</code>/<code>low</code>/<code>close</code>/<code>volume</code>/<code>amount</code> 字段组合内置指标：</p>
        <div class="code-block">
          <pre><code>close / MA(close, 20) - 1          → 收盘价偏离 20 日均线的幅度
RSI(close, 14)                    → 14 日 RSI 值
MACD(close, 12, 26, 9)[0]         → MACD DIF 线
(close - LLV(low, 20)) / (HHV(high, 20) - LLV(low, 20))  → 随机指标
EMA(close, 5) / EMA(close, 20)    → 快慢均线比</code></pre>
        </div>

        <h3>参数配置</h3>
        <ul>
          <li><strong>分组数</strong>：2-10 层，默认 5（Q1 最低因子值，Q5 最高）</li>
          <li><strong>调仓频率</strong>：每天/每周/每月</li>
          <li>结果包含分组净值曲线、Long-Short 收益、分层年化统计</li>
        </ul>

        <h3>从因子研究选择</h3>
        <p>点击下拉框从已保存的因子中选取，其表达式会自动填入。与因子研究的分析结果可对照验证。</p>
      </section>

      <!-- 脚本模式 -->
      <section id="script-mode">
        <h2>脚本模式</h2>
        <p>编写 RQAlpha 风格的 <code>init(context)</code> + <code>handle_bar(context, bar_dict)</code> 策略脚本。引擎每天触发一次 handle_bar，bar_dict 提供当日所有标的的 Bar 数据。完整模拟下单→风控→撮合→成交→持仓的全生命周期。</p>

        <h3>策略生命周期回调</h3>
        <table class="info-table">
          <thead>
            <tr><th>函数</th><th>触发时机</th><th>用途</th></tr>
          </thead>
          <tbody>
            <tr><td><code>init(context)</code></td><td>回测开始时，仅一次</td><td>设置参数、初始化状态变量</td></tr>
            <tr><td><code>before_trading(context)</code></td><td>每个交易日开盘前</td><td>计算日内信号（从分钟数据）、预处理</td></tr>
            <tr><td><code>handle_bar(context, bar_dict)</code></td><td>每个交易日 BAR 阶段</td><td>主策略逻辑、下单、持仓管理</td></tr>
            <tr><td><code>after_trading(context)</code></td><td>每个交易日收盘后</td><td>收盘后清理、日志记录</td></tr>
          </tbody>
        </table>

        <h3>策略结构</h3>
        <div class="code-block">
          <pre><code>def init(context):
    """策略初始化 — 在回测开始时调用一次"""
    context.fast = 5
    context.slow = 20
    context.position_pct = 0.2

def before_trading(context):
    """开盘前 — 可选，适合从分钟数据计算日内信号"""
    for symbol in context.universe:
        bars = context.get_intraday(symbol)
        # ... 计算信号 ...

def handle_bar(context, bar_dict):
    """每天触发一次 — bar_dict 为 BarDict 对象，可 [symbol] 访问任意标的"""
    for symbol in bar_dict:
        bar = bar_dict[symbol]
        if bar.suspended or bar.isnan:
            continue
        # ... 策略逻辑 ...
        context.order_value(symbol, context.cash * context.position_pct)

def after_trading(context):
    """收盘后 — 可选"""
    log(f"[{context.now.date()}] NAV={context.portfolio.unit_net_value:.3f}")</code></pre>
        </div>

        <h3>执行流程</h3>
        <ol>
          <li><code>ENGINE_START</code> → 调用 <code>init(context)</code></li>
          <li>每个交易日：<code>BEFORE_TRADING</code> → <code>before_trading(context)</code>（可选）</li>
          <li><code>PRE_BAR</code> → 系统预处理</li>
          <li><code>BAR</code> → <code>handle_bar(context, bar_dict)</code> → 策略下单</li>
          <li><code>POST_BAR</code> → 订单推入风控链 → 风控校验 → 撮合成交</li>
          <li><code>AFTER_TRADING</code> → <code>after_trading(context)</code>（可选）</li>
          <li><code>SETTLEMENT</code> → 持仓按收盘价重估，记录 NAV</li>
          <li><code>ENGINE_END</code> → 计算绩效指标</li>
        </ol>
      </section>

      <!-- 风控系统 -->
      <section id="risk-control">
        <h2>风控系统</h2>
        <p>所有订单在成交前经过风控校验链（Validator Chain），任一校验失败则订单被拒绝：</p>

        <table class="info-table">
          <thead>
            <tr><th>校验器</th><th>检查内容</th><th>拒绝条件</th></tr>
          </thead>
          <tbody>
            <tr><td><code>CashValidator</code></td><td>资金校验</td><td>买入金额 > 可用资金</td></tr>
            <tr><td><code>PriceValidator</code></td><td>价格校验</td><td>委托价格偏离昨日收盘 > 10%</td></tr>
            <tr><td><code>PositionLimitValidator</code></td><td>仓位限制</td><td>持仓数量 > max_positions</td></tr>
          </tbody>
        </table>

        <p>订单状态流转：<code>PENDING_NEW</code> → 风控校验 → <code>ACTIVE</code>（通过）/ <code>REJECTED</code>（拒绝）→ 撮合成交 → <code>FILLED</code></p>
        <p>被风控拒绝的订单可通过 <code>context.get_open_orders()</code> 查看拒绝原因。</p>
      </section>

      <!-- Context API -->
      <section id="context-api">
        <h2>Context API 参考</h2>
        <p><code>context</code> 是策略与回测引擎交互的唯一入口，提供以下属性：</p>

        <h3>运行信息</h3>
        <div class="code-block">
          <pre><code>context.now              # datetime — 当前时间
context.current_date     # date — 当前交易日（同 get_current_date()）
context.run_info         # RunInfo 对象 — start_date, end_date, frequency, capital, symbols</code></pre>
        </div>

        <h3>账户 & 组合</h3>
        <div class="code-block">
          <pre><code>context.cash                        # 可用资金
context.total_value                 # 总资产（现金 + 持仓市值）
context.portfolio                   # PortfolioProxy
context.portfolio.total_value       # 总资产
context.portfolio.unit_net_value    # 单位净值
context.portfolio.get_position(sym) # 获取持仓 Position
context.portfolio.get_positions()   # 所有有效持仓列表
context.stock_account               # AccountProxy（cash, available_cash, frozen_cash, market_value）
context.stock_account.positions     # dict[str, Position]</code></pre>
        </div>

        <h3>持仓对象 (Position) — FIFO 追踪</h3>
        <p>持仓采用 <strong>FIFO（先进先出）</strong> 成本核算。每笔买入形成独立的持仓批次（lot），卖出时按买入顺序匹配：</p>
        <div class="code-block">
          <pre><code>pos.symbol           # 标的代码
pos.total_shares      # 持有股数
pos.avg_cost          # 持仓均价（FIFO 加权）
pos.market_value      # 市值
pos.unrealized_pnl    # 浮动盈亏
pos.lots              # list[Lot] — 持仓批次
  lot.shares          #   批次股数
  lot.cost_price      #   批次成本价
  lot.buy_date        #   买入日期</code></pre>
        </div>

        <h3>股票池</h3>
        <div class="code-block">
          <pre><code>context.universe                  # set[str] — 当前股票池
context.update_universe(symbols)   # 更新股票池</code></pre>
        </div>

        <h3>策略日志</h3>
        <div class="code-block">
          <pre><code>log(msg)  # 输出到回测日志，在结果面板中可见</code></pre>
        </div>
      </section>

      <!-- BarDict -->
      <section id="bardict-api">
        <h2>BarDict 参考</h2>
        <p><code>handle_bar(context, bar_dict)</code> 的第二个参数是 BarDict 对象，提供 dict-like 访问：</p>
        <div class="code-block">
          <pre><code>bar_dict[symbol]       # 获取 symbol 的当前 Bar 对象
symbol in bar_dict    # 检查 symbol 是否在 bar_dict 中
len(bar_dict)         # 标的数量
bar_dict.keys()       # 所有标的代码
bar_dict.items()      # (symbol, Bar) 对
bar_dict.dt           # 当前日期</code></pre>
        </div>
      </section>

      <!-- Bar 对象 -->
      <section id="bar-api">
        <h2>Bar 对象参考</h2>
        <p>通过 <code>bar_dict[symbol]</code> 获取单个 Bar 对象：</p>

        <h3>属性</h3>
        <div class="code-block">
          <pre><code>bar.symbol            # str   — 标的代码
bar.order_book_id     # str   — 同 symbol
bar.trade_date        # date  — 交易日期
bar.datetime          # datetime — 日期时间（分钟 Bar 含具体时间）
bar.minute_time       # datetime | None — 分钟时间戳（仅分钟 Bar）
bar.open              # float — 开盘价
bar.high              # float — 最高价
bar.low               # float — 最低价
bar.close             # float — 收盘价
bar.last              # float — 最新价（日线=close）
bar.volume            # int   — 成交量
bar.total_turnover    # float — 成交额
bar.prev_close        # float — 昨日收盘价
bar.limit_up          # float — 涨停价（prev_close × 1.1）
bar.limit_down        # float — 跌停价（prev_close × 0.9）
bar.suspended         # bool  — 是否停牌（无成交量 或 close 为 NaN）
bar.is_trading        # bool  — 是否有成交
bar.isnan             # bool  — 数据是否缺失</code></pre>
        </div>

        <h3>方法</h3>
        <div class="code-block">
          <pre><code>bar.mavg(intervals)    # 最近 intervals 根 Bar 收盘价均值
bar.vwap(intervals)    # 最近 intervals 根 Bar 成交量加权均价

# 示例
ma20 = bar.mavg(20)    # 等价于 MA(close, 20) 的当前值
vwap5 = bar.vwap(5)    # 5 日成交量加权均价</code></pre>
        </div>
      </section>

      <!-- 下单 API -->
      <section id="order-api">
        <h2>下单 & 订单管理</h2>

        <h3>下单函数</h3>
        <div class="code-block">
          <pre><code># 按股数下单（正=买入，负=卖出）
context.order_shares(symbol, shares, price=None) → Order | None

# 按金额下单（正=买入，负=卖出）
context.order_value(symbol, value, price=None) → Order | None

# 目标仓位百分比（调至总资产的 pct%）
context.order_target_pct(symbol, pct, price=None) → Order | None

# RQAlpha 兼容别名
context.order(symbol, shares)    # 等同于 order_shares
context.order_target_percent(symbol, pct)  # 等同于 order_target_pct</code></pre>
        </div>

        <h3>订单管理</h3>
        <div class="code-block">
          <pre><code># 获取未成交订单
context.get_open_orders(symbol=None) → list[Order]

# 撤单
context.cancel_order(order) → Order | None</code></pre>
        </div>

        <h3>订单生命周期</h3>
        <div class="code-block">
          <pre><code>PENDING_NEW  →  风控校验  →  ACTIVE  →  撮合成交  →  FILLED
                  ↓                        ↓
              REJECTED                 CANCELLED（撤单）</code></pre>
        </div>

        <h3>订单状态 (Order)</h3>
        <div class="code-block">
          <pre><code>order.order_id          # str   — 订单 ID
order.symbol            # str   — 标的
order.direction         # str   — "buy" / "sell"
order.quantity          # int   — 委托数量
order.price             # float — 委托价格
order.filled_quantity   # int   — 已成交数量
order.avg_price         # float — 成交均价
order.status            # str   — PENDING_NEW → ACTIVE → FILLED / REJECTED / CANCELLED
order.transaction_cost  # float — 手续费
order.reject_reason     # str   — 拒绝原因（仅 REJECTED 状态）</code></pre>
        </div>
      </section>

      <!-- 数据 API -->
      <section id="data-api">
        <h2>数据获取</h2>
        <div class="code-block">
          <pre><code># 获取历史行情 DataFrame
context.get_history(symbol, n_days=252) → pd.DataFrame

# 获取历史 Bar 返回 numpy 数组（RQAlpha 兼容）
context.history_bars(symbol, bar_count, frequency="1d", fields=None) → np.ndarray

# 当前市场快照
context.current_snapshot(symbol) → dict | None

# 交易日历
context.get_trading_dates(start_date, end_date) → list[date]</code></pre>
        </div>

        <h3>分钟数据（需要 bar_type=minute）</h3>
        <div class="code-block">
          <pre><code># 获取某日所有分钟 Bar（在 before_trading 中调用以计算日内信号）
context.get_intraday(symbol, trade_date=None) → list[Bar]
# trade_date 默认为 current_date。每个 Bar 有 open/high/low/close/volume/minute_time

# 获取 N 日分钟 OHLCV DataFrame（用于计算历史阈值等）
context.get_intraday_history(symbol, n_days=5, end_date=None) → pd.DataFrame</code></pre>
        </div>
        <p>分钟数据仅在回测配置中将 bar_type 设为 <code>minute</code> 时加载（后台同时查询 klines_minute 表）。前端在策略编辑区上方提供日线/分钟的切换选择器。</p>
      </section>

      <!-- 内置指标 -->
      <section id="indicators">
        <h2>内置指标</h2>
        <p>策略脚本中可直接使用以下函数（无需 import）：</p>

        <table class="info-table">
          <thead>
            <tr><th>函数</th><th>签名</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr><td><code>MA</code></td><td><code>MA(series, period)</code></td><td>简单移动平均</td></tr>
            <tr><td><code>EMA</code></td><td><code>EMA(series, period)</code></td><td>指数移动平均</td></tr>
            <tr><td><code>SMA</code></td><td><code>SMA(series, period, weight=1)</code></td><td>扩展移动平均</td></tr>
            <tr><td><code>MACD</code></td><td><code>MACD(series, fast=12, slow=26, signal=9)</code></td><td>返回 (DIF, DEA, BAR)</td></tr>
            <tr><td><code>RSI</code></td><td><code>RSI(series, period=14)</code></td><td>相对强弱指标</td></tr>
            <tr><td><code>ATR</code></td><td><code>ATR(close, high, low, period=14)</code></td><td>平均真实波幅</td></tr>
            <tr><td><code>STD</code></td><td><code>STD(series, period)</code></td><td>滚动标准差</td></tr>
            <tr><td><code>HHV</code></td><td><code>HHV(series, period)</code></td><td>N 周期最高值</td></tr>
            <tr><td><code>LLV</code></td><td><code>LLV(series, period)</code></td><td>N 周期最低值</td></tr>
            <tr><td><code>CROSS</code></td><td><code>CROSS(a, b)</code></td><td>返回 Series：+1=上穿, -1=下穿, 0=无</td></tr>
            <tr><td><code>REF</code></td><td><code>REF(series, n)</code></td><td>前移 N 周期</td></tr>
            <tr><td><code>COUNT</code></td><td><code>COUNT(cond, n)</code></td><td>N 周期内满足条件的次数</td></tr>
            <tr><td><code>EVERY</code></td><td><code>EVERY(cond, n)</code></td><td>N 周期内是否全部满足</td></tr>
            <tr><td><code>ROUND</code></td><td><code>ROUND(x, n=2)</code></td><td>四舍五入</td></tr>
          </tbody>
        </table>

        <h3>可用模块</h3>
        <div class="code-block">
          <pre><code>np    # numpy
pd    # pandas</code></pre>
        </div>
      </section>

      <!-- 回测 API v2 -->
      <section id="api-v2">
        <h2>回测 API v2</h2>
        <p>事件驱动引擎通过 <code>/api/v2/backtest</code> 端点提供服务：</p>

        <h3>POST /api/v2/backtest/run — 提交回测任务</h3>
        <div class="code-block">
          <pre><code>{
  "mode": "event_driven",         // "vectorized" | "event_driven"
  "factor_expression": "def init(ctx): ...",  // 策略代码（event_driven 模式）
  "symbols": ["000001.SZ", ...],  // 股票池
  "start_date": "2025-04-09",     // 起始日期
  "end_date": "2026-04-30",       // 结束日期
  "initial_capital": 1000000,     // 初始资金
  "bar_type": "minute",           // "daily" | "minute"
  "commission_rate": 0.0003,      // 手续费率
  "slippage": 0.001               // 滑点
}</code></pre>
        </div>

        <h3>GET /api/v2/backtest/status/{task_id} — 查询进度</h3>
        <p>返回实时状态：<code>queued → running → done/failed</code>，含当前日期、持仓快照、事件流。</p>

        <h3>GET /api/v2/backtest/result/{task_id} — 获取结果</h3>
        <p>返回完整回测报告：绩效指标、净值曲线、交易记录（含每笔 FIFO 盈亏）、订单记录。</p>
      </section>

      <!-- 完整示例 -->
      <section id="examples">
        <h2>完整示例</h2>

        <h3>示例 1：表达式模式 — 反转因子</h3>
        <div class="code-block">
          <pre><code># 5 日反转：前 5 日涨幅越大，未来可能回调
-(close - REF(close, 5)) / REF(close, 5)</code></pre>
        </div>

        <h3>示例 2：MA 金叉死叉策略</h3>
        <div class="code-block">
          <pre><code>def init(context):
    context.fast = 5
    context.slow = 20

def handle_bar(context, bar_dict):
    for symbol in bar_dict:
        bar = bar_dict[symbol]
        if bar.suspended or bar.isnan:
            continue

        hist = context.get_history(symbol, 252)
        if hist.empty or len(hist) < context.slow:
            continue
        close = hist['close']

        mf = MA(close, context.fast)
        ms = MA(close, context.slow)

        # 金叉买入
        if CROSS(mf, ms).iloc[-1] == 1:
            if not context.portfolio.get_position(symbol):
                context.order_value(symbol, context.portfolio.total_value * 0.2)

        # 死叉卖出
        elif CROSS(mf, ms).iloc[-1] == -1:
            pos = context.portfolio.get_position(symbol)
            if pos and pos.total_shares > 0:
                context.order_shares(symbol, -pos.total_shares)</code></pre>
        </div>

        <h3>示例 3：日内信号 + 日线交易</h3>
        <div class="code-block">
          <pre><code>def init(context):
    context.lookback = 5
    context.vol_pct = 0.90
    context.portfolio_size = 5
    context.signal_history = {}  # {date: {symbol: score}}

def before_trading(context):
    """从分钟数据计算日内信号"""
    today = context.now.date()
    signals = {}
    for symbol in context.universe:
        hist = context.get_intraday_history(symbol, context.lookback, today)
        if hist is None or hist.empty:
            continue
        bars = context.get_intraday(symbol, today)
        if len(bars) < 10:
            continue
        vols = [b.volume for b in bars]
        threshold = np.quantile(vols, context.vol_pct)
        trend_vol = sum(v for v in vols if v > threshold)
        total_vol = sum(vols)
        if total_vol > 0:
            signals[symbol] = trend_vol / total_vol
    if signals:
        context.signal_history[today] = signals

def handle_bar(context, bar_dict):
    """用已计算的信号做买入决策"""
    # 每 5 个交易日调仓
    if context.now.day % 5 != 0:
        return

    dates = sorted(context.signal_history.keys())
    if len(dates) < 3:
        return

    # 融合近 3 天信号
    recent = dates[-3:]
    scores = {}
    for sym in context.universe:
        vals = [context.signal_history[d].get(sym, 0) for d in recent]
        scores[sym] = sum(vals)

    # 买入 top N
    ranked = sorted(scores.items(), key=lambda x: -x[1])[:context.portfolio_size]
    per_stock = context.portfolio.total_value * 0.9 / context.portfolio_size
    for sym, _ in ranked:
        if sym in bar_dict and not bar_dict[sym].suspended:
            context.order_value(sym, per_stock)</code></pre>
        </div>
      </section>

      <!-- 绩效指标说明 -->
      <section id="metrics">
        <h2>绩效指标说明</h2>
        <table class="info-table">
          <thead>
            <tr><th>指标</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr><td>Total Return</td><td>总收益率 = (最终资产 - 初始资金) / 初始资金</td></tr>
            <tr><td>Annual Return</td><td>年化收益率</td></tr>
            <tr><td>Annual Volatility</td><td>年化波动率（收益率标准差 × sqrt(252)）</td></tr>
            <tr><td>Sharpe Ratio</td><td>夏普比率 = (年化收益 - 无风险利率) / 年化波动</td></tr>
            <tr><td>Sortino Ratio</td><td>索提诺比率 — 仅用下行波动率</td></tr>
            <tr><td>Max Drawdown</td><td>最大回撤</td></tr>
            <tr><td>Calmar Ratio</td><td>卡尔马比率 = 年化收益 / 最大回撤</td></tr>
            <tr><td>Alpha / Beta</td><td>相对于基准的超额收益 / 市场敏感度</td></tr>
            <tr><td>Information Ratio</td><td>信息比率 = 超额收益 / 跟踪误差</td></tr>
            <tr><td>Win Rate</td><td>胜率 = 盈利交易 / 总卖出笔数（基于 FIFO 实现盈亏）</td></tr>
            <tr><td>Total Trades</td><td>总成交笔数（买入 + 卖出）</td></tr>
            <tr><td>Avg Return</td><td>单笔平均盈亏（基于 FIFO 实现盈亏）</td></tr>
            <tr><td>Turnover Rate</td><td>换手率</td></tr>
          </tbody>
        </table>
      </section>
    </main>
  </div>
</template>

<script setup lang="ts">
const sections = [
  { id: 'overview', title: '概述' },
  { id: 'event-driven', title: '事件驱动引擎' },
  { id: 'expression-mode', title: '表达式模式' },
  { id: 'script-mode', title: '脚本模式' },
  { id: 'risk-control', title: '风控系统' },
  { id: 'context-api', title: 'Context API' },
  { id: 'bardict-api', title: 'BarDict' },
  { id: 'bar-api', title: 'Bar 对象' },
  { id: 'order-api', title: '下单 & 订单管理' },
  { id: 'data-api', title: '数据获取' },
  { id: 'indicators', title: '内置指标' },
  { id: 'api-v2', title: '回测 API v2' },
  { id: 'examples', title: '完整示例' },
  { id: 'metrics', title: '绩效指标' },
]
</script>

<style scoped>
.docs-container {
  display: grid;
  grid-template-columns: 220px 1fr;
  height: calc(100vh - 32px);
  gap: 0;
  background: var(--bg-base, #0a0a0c);
}

.docs-sidebar {
  background: #0d0d12;
  border-right: 1px solid #1a1a25;
  padding: 20px 16px;
  overflow-y: auto;
  position: sticky;
  top: 0;
  height: 100vh;
}

.sidebar-title {
  font-size: 14px;
  font-weight: 600;
  color: #e0e0e0;
  margin-bottom: 16px;
  padding-bottom: 12px;
  border-bottom: 1px solid #1a1a25;
}

.sidebar-nav {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.nav-link {
  display: block;
  padding: 6px 10px;
  font-size: 13px;
  color: #888;
  text-decoration: none;
  border-radius: 4px;
  transition: all 0.15s;
}

.nav-link:hover {
  color: #d4d4d4;
  background: #1a1a25;
}

.docs-main {
  padding: 32px 48px;
  max-width: 860px;
  overflow-y: auto;
  height: 100vh;
}

h1 {
  font-size: 26px;
  font-weight: 700;
  color: #e8e8e8;
  margin: 0 0 24px 0;
  padding-bottom: 16px;
  border-bottom: 1px solid #1a1a25;
}

h2 {
  font-size: 20px;
  font-weight: 600;
  color: #d4d4d4;
  margin: 40px 0 16px 0;
  padding-top: 8px;
}

h3 {
  font-size: 15px;
  font-weight: 600;
  color: #bbb;
  margin: 20px 0 10px 0;
}

p {
  font-size: 14px;
  color: #999;
  line-height: 1.7;
  margin: 0 0 12px 0;
}

ul, ol {
  font-size: 14px;
  color: #999;
  line-height: 1.8;
  margin: 0 0 16px 0;
  padding-left: 20px;
}

li {
  margin-bottom: 4px;
}

li strong {
  color: #ccc;
}

code {
  background: #1a1a25;
  color: #c586c0;
  padding: 1px 6px;
  border-radius: 3px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 13px;
}

.info-table {
  width: 100%;
  border-collapse: collapse;
  margin: 12px 0 20px 0;
  font-size: 13px;
}

.info-table th {
  text-align: left;
  padding: 8px 12px;
  background: #111118;
  color: #aaa;
  font-weight: 600;
  border-bottom: 1px solid #1a1a25;
}

.info-table td {
  padding: 8px 12px;
  color: #999;
  border-bottom: 1px solid #111118;
}

.info-table td code {
  font-size: 12px;
  white-space: nowrap;
}

.code-block {
  background: #0d0d12;
  border: 1px solid #1a1a25;
  border-radius: 6px;
  padding: 14px 16px;
  margin: 10px 0 16px 0;
  overflow-x: auto;
}

.code-block pre {
  margin: 0;
}

.code-block code {
  background: none;
  padding: 0;
  color: #d4d4d4;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  line-height: 1.7;
}

section:target {
  scroll-margin-top: 24px;
}
</style>
