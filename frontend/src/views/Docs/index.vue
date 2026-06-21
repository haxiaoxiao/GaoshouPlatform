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

      <section id="overview">
        <h2>概览</h2>
        <p>平台以 AKQuant 事件驱动回测为主路径，同时保留内置回测能力。新回测接口统一使用 <code>/api/backtest/*</code>。</p>
      </section>

      <section id="engine-architecture">
        <h2>引擎架构</h2>
        <p>前端提交 payload 后，后端通过统一回测引擎路由运行任务，并用任务状态接口查询进度和结果。</p>
      </section>

      <section id="akquant-strategy">
        <h2>AKQuant 策略语法</h2>
        <p>推荐使用类风格策略：<code>class MyStrategy(aq.Strategy)</code>，在 <code>on_start</code> 初始化，在 <code>on_bar</code> 处理行情并下单。</p>
        <div class="code-block"><pre><code>import akquant as aq

class MyStrategy(aq.Strategy):
    def on_bar(self, bar):
        position = self.ctx.get_position(bar.symbol)
        if position == 0:
            self.buy(bar.symbol, 100)</code></pre></div>
      </section>

      <section id="akquant-expression">
        <h2>表达式模式</h2>
        <p>表达式适合快速验证因子信号，例如 <code>Mean($close, 5)</code>、<code>RSI($close, 14)</code> 或 Alpha101 风格公式。</p>
      </section>

      <section id="builtin-engine">
        <h2>Builtin 引擎</h2>
        <p>Builtin 引擎主要用于兼容旧策略和简单表达式回测。</p>
      </section>

      <section id="risk-control">
        <h2>风控规则</h2>
        <ul>
          <li>A 股交易单位按 100 股一手处理。</li>
          <li>手续费、滑点、印花税、过户费由控制面板或 API payload 控制。</li>
          <li>策略参数通过 <code>strategy_params</code> 读取，不在策略代码中硬编码日期、资金或股票池。</li>
        </ul>
      </section>

      <section id="llm-strategy">
        <h2>LLM 策略生成</h2>
        <p>策略回测页提供 LLM 生成入口，可辅助把研报逻辑或旧框架代码转换为 AKQuant 策略。</p>
      </section>

      <section id="quantstats-report">
        <h2>QuantStats 报告</h2>
        <p>AKQuant 回测结果可返回净值、交易记录、指标和 HTML 报告。</p>
      </section>

      <section id="api-v2">
        <h2>Backtest API</h2>
        <table class="info-table">
          <thead><tr><th>接口</th><th>说明</th></tr></thead>
          <tbody>
            <tr><td><code>POST /api/backtest/run</code></td><td>提交回测任务</td></tr>
            <tr><td><code>GET /api/backtest/status/{task_id}</code></td><td>查询任务状态</td></tr>
            <tr><td><code>GET /api/backtest/result/{task_id}</code></td><td>读取任务结果</td></tr>
            <tr><td><code>POST /api/backtest/optimize/walk-forward</code></td><td>Walk-forward 参数优化</td></tr>
          </tbody>
        </table>
      </section>

      <section id="examples">
        <h2>示例</h2>
        <p>CashAware 策略可先在策略回测页验证参数，再进入模拟 / 实盘页通过 Profile 白名单生成信号、审计订单并按权限提交。</p>
      </section>
    </main>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { usePageContext } from '@/app/pageContext'

const route = useRoute()

const sections = [
  { id: 'overview', title: '概览' },
  { id: 'engine-architecture', title: '引擎架构' },
  { id: 'akquant-strategy', title: 'AKQuant 策略语法' },
  { id: 'akquant-expression', title: '表达式模式' },
  { id: 'builtin-engine', title: 'Builtin 引擎' },
  { id: 'risk-control', title: '风控规则' },
  { id: 'llm-strategy', title: 'LLM 策略生成' },
  { id: 'quantstats-report', title: 'QuantStats 报告' },
  { id: 'api-v2', title: 'Backtest API' },
  { id: 'examples', title: '示例' },
]

const pageContextBlocks = computed(() => [
  {
    title: 'Docs',
    rows: [
      { label: '文档类型', value: 'Backtest Manual' },
      { label: '章节数', value: `${sections.length}` },
      { label: '当前锚点', value: route.hash || '#overview' },
    ],
  },
  {
    title: 'Topics',
    rows: [
      { label: '引擎', value: 'AKQuant / Builtin' },
      { label: '接口', value: '/api/backtest/*' },
      { label: '用途', value: '策略编写与回测说明' },
    ],
  },
])

usePageContext(pageContextBlocks)
</script>

<style scoped>
.docs-container {
  display: grid;
  grid-template-columns: 220px 1fr;
  gap: 24px;
  min-height: calc(100vh - 80px);
}

.docs-sidebar {
  position: sticky;
  top: 16px;
  align-self: start;
  padding: 16px;
  border: 1px solid var(--el-border-color);
  border-radius: 8px;
}

.sidebar-title {
  font-weight: 700;
  margin-bottom: 12px;
}

.sidebar-nav {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.nav-link {
  color: var(--el-text-color-regular);
  text-decoration: none;
  font-size: 13px;
}

.docs-main {
  max-width: 960px;
  line-height: 1.7;
}

section {
  margin-bottom: 28px;
}

.code-block,
pre {
  background: #111827;
  color: #e5e7eb;
  border-radius: 8px;
  padding: 12px;
  overflow: auto;
}

.info-table {
  width: 100%;
  border-collapse: collapse;
}

.info-table th,
.info-table td {
  border: 1px solid var(--el-border-color);
  padding: 8px 10px;
  text-align: left;
}
</style>
