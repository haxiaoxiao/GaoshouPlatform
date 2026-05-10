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
        <p>Gaoshou 策略回测平台支持<strong>双引擎架构</strong>，可在 AKQuant 和内置引擎之间自由切换。策略语法统一为 AKQuant 风格（推荐），同时兼容 RQAlpha 旧语法。</p>
        <table class="info-table">
          <thead>
            <tr><th></th><th>AKQuant 引擎（推荐）</th><th>内置引擎（Builtin）</th></tr>
          </thead>
          <tbody>
            <tr><td><strong>策略语法</strong></td><td><code>class MyStrategy(aq.Strategy)</code></td><td><code>init(context)</code> + <code>handle_bar(context, bar_dict)</code></td></tr>
            <tr><td><strong>执行方式</strong></td><td>akquant 内核，逐 Bar 回调</td><td>事件驱动，订单→风控→撮合→成交</td></tr>
            <tr><td><strong>支持模式</strong></td><td>Python 代码 + 因子表达式</td><td>Python 代码 + 因子表达式</td></tr>
            <tr><td><strong>可视化</strong></td><td>quantstats HTML 报告 + NAV 曲线</td><td>NAV 曲线 + 交易记录</td></tr>
            <tr><td><strong>LLM 集成</strong></td><td>代码转换 + 研报生成策略</td><td>—</td></tr>
            <tr><td><strong>适合场景</strong></td><td>策略研发、研报复现、完整回测分析</td><td>兼容旧策略、因子分层验证</td></tr>
          </tbody>
        </table>
        <p>前端右上角引擎选择器切换引擎后，左侧编辑区 Tab 和回测行为自动适配。</p>
      </section>

      <!-- 引擎架构 -->
      <section id="engine-architecture">
        <h2>引擎架构</h2>
        <p>平台采用 <code>IBacktestEngine</code> 抽象接口 + <code>EngineRegistry</code> 注册表模式，新增引擎只需实现抽象接口并注册：</p>
        <div class="code-block">
          <pre><code># 引擎接口 (backend/app/backtest/engine/interface.py)
class IBacktestEngine(ABC):
    name: str           # 引擎标识，如 "akquant" / "builtin"
    label: str          # 前端显示名
    supported_modes: list[str]  # 支持的回测模式

    async def run(config, data_provider, progress_callback) → BacktestResult
    def validate_config(config) → list[str]

# 引擎注册
@EngineRegistry.register
class AkquantEngine(IBacktestEngine): ...</code></pre>
        </div>
        <p>所有引擎通过统一的 <code>IDataProvider</code> 获取数据（底层为 ClickHouse），保证数据一致性。</p>
        <h3>可用引擎列表</h3>
        <table class="info-table">
          <thead>
            <tr><th>name</th><th>label</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr><td><code>akquant</code></td><td>AKQuant</td><td>基于 akquant 框架的高性能回测引擎，支持 quantstats 报告</td></tr>
            <tr><td><code>builtin</code></td><td>Builtin</td><td>平台内置事件驱动引擎，RQAlpha 兼容</td></tr>
          </tbody>
        </table>
        <p>API: <code>GET /api/v2/backtest/engines</code> 获取引擎列表。</p>
      </section>

      <!-- AKQuant 策略语法 -->
      <section id="akquant-strategy">
        <h2>AKQuant 策略语法</h2>
        <p>AKQuant 引擎使用标准的 Python 类策略语法。策略必须继承 <code>aq.Strategy</code>，在 <code>on_bar</code> 方法中编写交易逻辑：</p>

        <h3>策略结构</h3>
        <div class="code-block">
          <pre><code>import akquant as aq
import numpy as np

class MyStrategy(aq.Strategy):
    def on_start(self):
        """回测开始时调用一次，用于初始化参数"""
        self.set_history_depth(300)  # 必须调用，否则 get_history() 报错
        self.fast = 5
        self.slow = 20

    def on_bar(self, bar):
        """每根K线触发一次 — bar 包含当前 symbol 的 OHLCV 数据"""
        sym = bar.symbol
        pos = self.get_position(sym)

        # 获取历史数据
        hist_close = self.get_history(252, sym, 'close')

        # 计算信号
        mf = np.mean(hist_close[-self.fast:])
        ms = np.mean(hist_close[-self.slow:])

        # 交易逻辑
        if mf > ms and pos == 0:
            qty = int(self.equity * 0.2 // bar.close // 100) * 100
            if qty >= 100:
                self.buy(sym, qty)
        elif mf < ms and pos > 0:
            self.close_position(sym)</code></pre>
        </div>

        <h3>Bar 对象属性</h3>
        <p><code>on_bar(self, bar)</code> 的 <code>bar</code> 参数提供当前 K 线数据：</p>
        <div class="code-block">
          <pre><code>bar.symbol       # str   — 股票代码
bar.open         # float — 开盘价
bar.high         # float — 最高价
bar.low          # float — 最低价
bar.close        # float — 收盘价
bar.volume       # float — 成交量
bar.timestamp    # int   — 时间戳（纳秒）</code></pre>
        </div>

        <h3>策略方法 (self.*)</h3>
        <div class="code-block">
          <pre><code># ── 必须调用 ──
self.set_history_depth(n)         # 启用历史数据追踪（在 on_start 中调用）

# ── 下单 ──
self.buy(symbol, quantity)         # 买入 quantity 股
self.sell(symbol, quantity)        # 卖出 quantity 股
self.close_position(symbol)        # 清仓指定标的

# ── 查询 ──
self.get_position(symbol) → float  # 当前持仓数量（0=空仓）
self.equity → float                # 当前总资产
self.get_history(n, symbol, field) # 最近 n 根 Bar 的 field 序列 → np.ndarray
# field 可选: 'open', 'high', 'low', 'close', 'volume'

# ── 日志 ──
self.log(msg)                      # 输出日志</code></pre>
        </div>

        <h3>A 股交易规则</h3>
        <ul>
          <li><strong>T+1</strong> — 当日买入次日才能卖出</li>
          <li><strong>最小交易单位</strong> — 100 股（1 手），数量须为 100 的整数倍</li>
          <li><strong>涨跌停</strong> — 普通股 ±10%，ST 股 ±5%</li>
          <li><strong>佣金</strong> — 默认万三（0.0003），印花税千一仅卖出</li>
        </ul>
      </section>

      <!-- AKQuant 表达式模式 -->
      <section id="akquant-expression">
        <h2>表达式模式（AKQuant）</h2>
        <p>在 AKQuant 引擎下，也可直接输入因子表达式。系统自动将表达式转为 <code>FunctionalStrategy</code>：</p>
        <ul>
          <li>表达式值 &gt; 0 → 买入 100 股</li>
          <li>表达式值 &lt; 0 → 清仓</li>
        </ul>
        <p>可用变量：<code>close</code>, <code>open</code>, <code>high</code>, <code>low</code>, <code>volume</code>（当前 bar 值）。</p>
        <div class="code-block">
          <pre><code>close - np.mean(self.get_history(20, bar.symbol, 'close'))   # 收盘价偏离 20 日均线
(close - open) / open * 100                                   # 日内涨幅百分比
volume / np.mean(self.get_history(20, bar.symbol, 'volume'))  # 量比</code></pre>
        </div>
        <p>注意：表达式模式仅支持简单的交易信号（买入/清仓），复杂策略请使用完整策略类。</p>
      </section>

      <!-- Builtin 引擎 -->
      <section id="builtin-engine">
        <h2>Builtin 引擎（RQAlpha 兼容）</h2>
        <p>内置引擎保留 RQAlpha 风格语法，适用于已有策略的迁移和兼容：</p>

        <h3>策略结构</h3>
        <div class="code-block">
          <pre><code>def init(context):
    """策略初始化 — 回测开始时调用一次"""
    context.fast = 5
    context.slow = 20

def handle_bar(context, bar_dict):
    """每天触发一次 — bar_dict 为 BarDict 对象"""
    for symbol in bar_dict:
        bar = bar_dict[symbol]
        if bar.suspended or bar.isnan:
            continue
        # ... 策略逻辑 ...

def before_trading(context):
    """开盘前 — 可选"""
    pass

def after_trading(context):
    """收盘后 — 可选"""
    pass</code></pre>
        </div>

        <h3>事件执行流程</h3>
        <ol>
          <li><code>ENGINE_START</code> → <code>init(context)</code></li>
          <li><code>BEFORE_TRADING</code> → <code>before_trading(context)</code>（可选）</li>
          <li><code>PRE_BAR</code> → 系统预处理</li>
          <li><code>BAR</code> → <code>handle_bar(context, bar_dict)</code> → 策略下单</li>
          <li><code>POST_BAR</code> → 风控校验 → 撮合成交</li>
          <li><code>AFTER_TRADING</code> → <code>after_trading(context)</code>（可选）</li>
          <li><code>SETTLEMENT</code> → 持仓按收盘价重估</li>
          <li><code>ENGINE_END</code> → 计算绩效</li>
        </ol>

        <h3>Context 下单 API</h3>
        <div class="code-block">
          <pre><code>context.order_shares(symbol, shares)          # 按股数（正买负卖）
context.order_value(symbol, value)            # 按金额（正买负卖）
context.order_target_pct(symbol, pct)         # 调至总资产 pct%
context.get_open_orders(symbol=None)          # 未成交订单
context.cancel_order(order)                   # 撤单</code></pre>
        </div>

        <h3>Context 查询 API</h3>
        <div class="code-block">
          <pre><code>context.now / context.current_date            # 当前时间/日期
context.cash / context.total_value            # 可用资金/总资产
context.portfolio.total_value                 # 组合总资产
context.portfolio.unit_net_value              # 单位净值
context.portfolio.get_position(sym)           # 持仓对象
context.get_history(symbol, n_days)           # 历史行情 DataFrame
context.get_intraday(symbol, date)            # 分钟数据（需 bar_type=minute）
context.universe                              # 股票池 set</code></pre>
        </div>

        <h3>表达式模式（Builtin）</h3>
        <p>仅内置引擎支持向量化分层回测。输入因子表达式 → 按因子值排序分组 → 等权买入各组 → 输出分层收益曲线。支持 <code>open</code>/<code>high</code>/<code>low</code>/<code>close</code>/<code>volume</code>/<code>amount</code> 及内置指标函数（MA/RSI/MACD/EMA/STD/HHV/LLV/CROSS/REF/COUNT 等）。</p>
      </section>

      <!-- 风控系统 -->
      <section id="risk-control">
        <h2>风控系统（Builtin 引擎）</h2>
        <p>内置引擎在成交前经过风控校验链（Validator Chain），任一校验失败则订单被拒绝：</p>
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
      </section>

      <!-- LLM 策略生成 -->
      <section id="llm-strategy">
        <h2>LLM 策略生成（AKQuant 引擎专享）</h2>
        <p>AKQuant 引擎下，编辑器左侧提供 "LLM策略生成" Tab，包含两个功能面板：</p>

        <h3>代码转换</h3>
        <p>将其他框架（RQAlpha / Backtrader / VNPY / 自定义 Python）的策略代码自动转换为 AKQuant 格式。</p>
        <ol>
          <li>在文本框中粘贴源代码</li>
          <li>点击 "转换代码"</li>
          <li>等待 LLM 生成（通常 10-60 秒）</li>
          <li>点击 "应用到编辑器" 将代码填入编辑区</li>
        </ol>
        <p>LLM 会自动识别原框架的关键 API（context.portfolio / bar_dict / self.datas / on_tick 等），映射到 AKQuant 等价 API，并保留原策略的完整逻辑和参数。</p>

        <h3>研报生成策略</h3>
        <p>上传 PDF/TXT 研报，LLM 先理解研报中的选股逻辑、调仓频率和风控规则，然后生成 AKQuant 策略代码。支持多轮对话：</p>
        <ol>
          <li>上传研报文件（PDF/TXT），创建对话会话</li>
          <li>LLM 返回策略逻辑总结，并向你确认不明确的地方</li>
          <li>你可以在对话框中补充或调整需求</li>
          <li>确认后 LLM 生成完整可运行的策略代码</li>
        </ol>
        <p>对话会话存储在服务端，TTL 1 小时自动清理。</p>

        <h3>API</h3>
        <table class="info-table">
          <thead>
            <tr><th>方法</th><th>路径</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr><td>POST</td><td><code>/api/strategy/convert-to-akquant</code></td><td>代码转换（单次，body: {"source_code":"..."}）</td></tr>
            <tr><td>POST</td><td><code>/api/strategy/chat-session</code></td><td>创建研报对话（multipart: file）</td></tr>
            <tr><td>POST</td><td><code>/api/strategy/chat-session/{id}/send</code></td><td>发送对话消息（body: {"message":"..."}）</td></tr>
          </tbody>
        </table>
      </section>

      <!-- quantstats 报告 -->
      <section id="quantstats-report">
        <h2>quantstats 可视化报告</h2>
        <p>AKQuant 引擎回测完成后自动生成 <strong>quantstats HTML 报告</strong>，包含专业的绩效分析图表：</p>
        <ul>
          <li>净值曲线 vs 基准对比</li>
          <li>回撤曲线（水下图）</li>
          <li>月度/年度收益热力图</li>
          <li>滚动夏普比率</li>
          <li>收益率分布直方图</li>
          <li>完整的统计指标表格</li>
        </ul>
        <p>回测结果面板中点击 "quantstats 报告" 标签即可查看，也可点击 "新窗口打开" 在独立页面查看。</p>
        <p>API: <code>GET /api/v2/backtest/report/{task_id}</code> 返回 HTML。</p>
      </section>

      <!-- AKShare 数据 API -->
      <section id="akshare-api">
        <h2>AKShare 数据 API</h2>
        <p>平台集成 <a href="https://github.com/akfamily/akshare" target="_blank">AKShare</a> 作为补充数据源，提供中国金融市场数据：</p>

        <table class="info-table">
          <thead>
            <tr><th>方法</th><th>路径</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr><td>POST</td><td><code>/api/akshare/stock/daily</code></td><td>单只股票日线数据</td></tr>
            <tr><td>POST</td><td><code>/api/akshare/stock/daily/batch</code></td><td>批量日线数据</td></tr>
            <tr><td>GET</td><td><code>/api/akshare/stock/spot</code></td><td>实时行情快照</td></tr>
            <tr><td>GET</td><td><code>/api/akshare/stock/list</code></td><td>A 股股票列表</td></tr>
            <tr><td>GET</td><td><code>/api/akshare/stock/info</code></td><td>个股基本信息（?symbol=sh600000）</td></tr>
            <tr><td>GET</td><td><code>/api/akshare/stock/hist</code></td><td>历史行情（复用回测查询接口）</td></tr>
          </tbody>
        </table>
        <p>注意：AKShare 数据源为网络 HTTP 调用，响应较慢。回测主引擎使用 ClickHouse 数据（通过 QMT 同步），AKShare 作为查询和验证的补充。</p>
      </section>

      <!-- 回测 API v2 -->
      <section id="api-v2">
        <h2>回测 API v2</h2>

        <h3>POST /api/v2/backtest/run — 提交回测任务</h3>
        <div class="code-block">
          <pre><code>{
  "engine": "akquant",              // "akquant" | "builtin"
  "mode": "event_driven",           // "vectorized" | "event_driven"
  "strategy_code": "class MyStrategy(aq.Strategy): ...",  // 策略代码
  "symbols": ["000001.SZ", ...],    // 股票池
  "start_date": "2025-04-09",       // 起始日期
  "end_date": "2026-04-30",         // 结束日期
  "initial_capital": 1000000,       // 初始资金
  "benchmark_symbol": "000300.SH",  // 基准指数（可选，akquant 引擎）
  "commission_rate": 0.0003,        // 手续费率
  "slippage": 0.001                 // 滑点
}</code></pre>
        </div>

        <h3>其他端点</h3>
        <table class="info-table">
          <thead>
            <tr><th>方法</th><th>路径</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr><td>GET</td><td><code>/api/v2/backtest/engines</code></td><td>列出所有可用引擎</td></tr>
            <tr><td>GET</td><td><code>/api/v2/backtest/status/{task_id}</code></td><td>查询回测进度（queued→running→done/failed）</td></tr>
            <tr><td>GET</td><td><code>/api/v2/backtest/result/{task_id}</code></td><td>获取回测结果（绩效+N领曲线+交易记录）</td></tr>
            <tr><td>GET</td><td><code>/api/v2/backtest/report/{task_id}</code></td><td>获取 quantstats HTML 报告（akquant 引擎）</td></tr>
            <tr><td>GET</td><td><code>/api/v2/backtest/pools/{name}</code></td><td>预定义股票池（top100/top300/top500/all）</td></tr>
            <tr><td>GET</td><td><code>/api/v2/backtest/stock-names</code></td><td>股票代码→中文名映射</td></tr>
            <tr><td>POST</td><td><code>/api/v2/backtest/factor</code></td><td>因子分层回测</td></tr>
          </tbody>
        </table>
      </section>

      <!-- 完整示例 -->
      <section id="examples">
        <h2>完整示例</h2>

        <h3>示例 1：双均线策略（AKQuant）</h3>
        <div class="code-block">
          <pre><code>import akquant as aq
import numpy as np

class DualMAStrategy(aq.Strategy):
    def on_start(self):
        self.set_history_depth(300)
        self.fast = 5
        self.slow = 20

    def on_bar(self, bar):
        sym = bar.symbol
        hist_close = self.get_history(252, sym, 'close')
        if len(hist_close) < self.slow:
            return

        mf = np.mean(hist_close[-self.fast:])
        ms = np.mean(hist_close[-self.slow:])
        pos = self.get_position(sym)

        if mf > ms and pos == 0:
            value = self.equity * 0.2
            qty = int(value // bar.close // 100) * 100
            if qty >= 100:
                self.buy(sym, qty)
        elif mf < ms and pos > 0:
            self.close_position(sym)</code></pre>
        </div>

        <h3>示例 2：RSI 超买超卖策略（AKQuant）</h3>
        <div class="code-block">
          <pre><code>import akquant as aq
import numpy as np

class RSIStrategy(aq.Strategy):
    def on_start(self):
        self.set_history_depth(100)
        self.rsi_period = 14
        self.oversold = 30
        self.overbought = 70

    def on_bar(self, bar):
        sym = bar.symbol
        closes = self.get_history(self.rsi_period + 10, sym, 'close')
        if len(closes) < self.rsi_period + 1:
            return

        # 计算 RSI
        deltas = np.diff(closes)
        gain = np.mean(deltas[deltas > 0]) if any(deltas > 0) else 0
        loss = abs(np.mean(deltas[deltas < 0])) if any(deltas < 0) else 1e-9
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        pos = self.get_position(sym)
        if rsi < self.oversold and pos == 0:
            qty = int(self.equity * 0.2 // bar.close // 100) * 100
            if qty >= 100:
                self.buy(sym, qty)
        elif rsi > self.overbought and pos > 0:
            self.close_position(sym)</code></pre>
        </div>

        <h3>示例 3：布林带策略（AKQuant）</h3>
        <div class="code-block">
          <pre><code>import akquant as aq
import numpy as np

class BollingerStrategy(aq.Strategy):
    def on_start(self):
        self.set_history_depth(100)
        self.period = 20
        self.std_mult = 2.0

    def on_bar(self, bar):
        sym = bar.symbol
        closes = self.get_history(self.period + 5, sym, 'close')
        if len(closes) < self.period:
            return

        ma = np.mean(closes[-self.period:])
        std = np.std(closes[-self.period:])
        upper = ma + self.std_mult * std
        lower = ma - self.std_mult * std

        pos = self.get_position(sym)
        if bar.close < lower and pos == 0:
            qty = int(self.equity * 0.2 // bar.close // 100) * 100
            if qty >= 100:
                self.buy(sym, qty)
        elif bar.close > upper and pos > 0:
            self.close_position(sym)</code></pre>
        </div>

        <h3>示例 4：MA 金叉死叉（Builtin）</h3>
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
        if CROSS(mf, ms).iloc[-1] == 1:
            if not context.portfolio.get_position(symbol):
                context.order_value(symbol, context.portfolio.total_value * 0.2)
        elif CROSS(mf, ms).iloc[-1] == -1:
            pos = context.portfolio.get_position(symbol)
            if pos and pos.total_shares > 0:
                context.order_shares(symbol, -pos.total_shares)</code></pre>
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
            <tr><td>Win Rate</td><td>胜率 = 盈利交易 / 总卖出笔数</td></tr>
            <tr><td>Total Trades</td><td>总成交笔数（买入 + 卖出）</td></tr>
            <tr><td>Avg Return</td><td>单笔平均盈亏</td></tr>
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
  { id: 'engine-architecture', title: '引擎架构' },
  { id: 'akquant-strategy', title: 'AKQuant 策略语法' },
  { id: 'akquant-expression', title: '表达式模式（AKQuant）' },
  { id: 'builtin-engine', title: 'Builtin 引擎（兼容）' },
  { id: 'risk-control', title: '风控系统' },
  { id: 'llm-strategy', title: 'LLM 策略生成' },
  { id: 'quantstats-report', title: 'quantstats 报告' },
  { id: 'akshare-api', title: 'AKShare 数据 API' },
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
