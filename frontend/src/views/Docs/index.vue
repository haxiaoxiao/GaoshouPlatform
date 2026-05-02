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
            <tr><td><strong>适合场景</strong></td><td>因子有效性验证、分层收益分析</td><td>策略研发、仓位管理、风控测试</td></tr>
          </tbody>
        </table>
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
        <p>编写 RQAlpha 风格的 <code>init(context)</code> + <code>handle_bar(context, bar)</code> 策略脚本。引擎按交易日历逐日触发 handle_bar，完整模拟下单→风控→撮合→成交→持仓的全生命周期。</p>

        <h3>策略结构</h3>
        <div class="code-block">
          <pre><code>def init(context):
    """策略初始化 — 在回测开始时调用一次"""
    context.fast = 5
    context.slow = 20
    context.position_pct = 0.2

def handle_bar(context, bar):
    """逐日触发 — bar 为当前 Bar 对象"""
    if bar.suspended or bar.isnan:
        return

    # ... 策略逻辑 ...
    context.order_value(bar.symbol, context.cash * context.position_pct)</code></pre>
        </div>

        <h3>执行流程</h3>
        <ol>
          <li><code>ENGINE_START</code> → 调用 <code>init(context)</code></li>
          <li>每个交易日：<code>BAR</code> → <code>handle_bar(context, bar)</code> → 订单进入风控链</li>
          <li>风控链：资金校验 → 价格校验 → 仓位限制</li>
          <li>通过后撮合成交，订单状态更新为 FILLED</li>
          <li><code>ENGINE_END</code> → 计算绩效指标</li>
        </ol>
      </section>

      <!-- Context API -->
      <section id="context-api">
        <h2>Context API 参考</h2>
        <p><code>context</code> 是策略与回测引擎交互的唯一入口，提供以下属性：</p>

        <h3>运行信息</h3>
        <div class="code-block">
          <pre><code>context.now              # datetime — 当前时间
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

        <h3>持仓对象 (Position)</h3>
        <div class="code-block">
          <pre><code>pos.symbol           # 标的代码
pos.total_shares      # 持有股数
pos.avg_cost          # 持仓均价（FIFO）
pos.market_value      # 市值
pos.unrealized_pnl    # 浮动盈亏</code></pre>
        </div>

        <h3>股票池</h3>
        <div class="code-block">
          <pre><code>context.universe                  # set[str] — 当前股票池
context.update_universe(symbols)   # 更新股票池</code></pre>
        </div>
      </section>

      <!-- Bar 对象 -->
      <section id="bar-api">
        <h2>Bar 对象参考</h2>
        <p>每个交易日的 <code>handle_bar(context, bar)</code> 接收一个丰富的 Bar 对象：</p>

        <h3>属性</h3>
        <div class="code-block">
          <pre><code>bar.symbol            # str   — 标的代码
bar.order_book_id     # str   — 同 symbol
bar.trade_date        # date  — 交易日期
bar.datetime          # datetime — 日期时间
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
context.order(symbol, shares)    # 等同于 order_shares</code></pre>
        </div>

        <h3>订单管理</h3>
        <div class="code-block">
          <pre><code># 获取未成交订单
context.get_open_orders(symbol=None) → list[Order]

# 撤单
context.cancel_order(order) → Order | None</code></pre>
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
order.transaction_cost  # float — 手续费</code></pre>
        </div>
      </section>

      <!-- 数据 API -->
      <section id="data-api">
        <h2>数据获取</h2>
        <div class="code-block">
          <pre><code># 获取历史行情 DataFrame（兼容原有 API）
context.get_history(symbol, n_days=252) → pd.DataFrame

# 获取历史 Bar 返回 numpy 数组（RQAlpha 兼容）
context.history_bars(symbol, bar_count, frequency="1d", fields=None) → np.ndarray

# 当前市场快照
context.current_snapshot(symbol) → dict | None

# 交易日历
context.get_trading_dates(start_date, end_date) → list[date]</code></pre>
        </div>
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

def handle_bar(context, bar):
    if bar.suspended or bar.isnan:
        return

    hist = context.get_history(bar.symbol, 252)
    if hist.empty or len(hist) < context.slow:
        return
    close = hist['close']

    mf = MA(close, context.fast)
    ms = MA(close, context.slow)

    # 金叉买入
    if CROSS(mf, ms).iloc[-1] == 1:
        if not context.portfolio.get_position(bar.symbol):
            context.order_value(bar.symbol, context.portfolio.total_value * 0.2)

    # 死叉卖出
    elif CROSS(mf, ms).iloc[-1] == -1:
        pos = context.portfolio.get_position(bar.symbol)
        if pos and pos.total_shares > 0:
            context.order_shares(bar.symbol, -pos.total_shares)</code></pre>
        </div>

        <h3>示例 3：动态仓位 + 风控</h3>
        <div class="code-block">
          <pre><code>def init(context):
    context.atr_period = 14
    context.max_positions = 3

def handle_bar(context, bar):
    if bar.suspended:
        return

    hist = context.get_history(bar.symbol, 252)
    if hist.empty:
        return
    close, high, low = hist['close'], hist['high'], hist['low']

    atr = ATR(close, high, low, context.atr_period)
    rsi = RSI(close, 14)

    # RSI 超卖 + ATR 放大时买入
    if rsi.iloc[-1] < 30 and atr.iloc[-1] > atr.iloc[-20:].mean():
        current_positions = len(context.stock_account.positions)
        if current_positions < context.max_positions:
            risk_per_share = atr.iloc[-1] * 2
            position_value = context.portfolio.total_value * 0.05
            shares = int(position_value / risk_per_share / 100) * 100
            if shares > 0:
                context.order_shares(bar.symbol, shares)

    # 清理已有持仓的止盈
    for sym, pos in context.stock_account.positions.items():
        if pos.unrealized_pnl / (pos.avg_cost * pos.total_shares) > 0.15:
            context.order_shares(sym, -pos.total_shares)</code></pre>
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
            <tr><td>Win Rate</td><td>胜率 = 盈利交易 / 总交易</td></tr>
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
  { id: 'expression-mode', title: '表达式模式' },
  { id: 'script-mode', title: '脚本模式' },
  { id: 'context-api', title: 'Context API' },
  { id: 'bar-api', title: 'Bar 对象' },
  { id: 'order-api', title: '下单 & 订单管理' },
  { id: 'data-api', title: '数据获取' },
  { id: 'indicators', title: '内置指标' },
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
