<template>
  <div class="docs-container" :class="`docs-layout--${layoutMode.toLowerCase()}`">
    <header class="docs-header" style="display: flex; justify-content: space-between; align-items: center; padding: 12px 24px; background: var(--bg-elevated); border-bottom: 1px solid var(--border-default);">
      <div style="font-weight: 800; color: var(--text-bright);">GaoshouPlatform Docs</div>
      <div class="layout-switcher" aria-label="切换文档页面布局" style="display: inline-flex; gap: 4px; padding: 4px; border: 1px solid var(--border-default); border-radius: var(--radius-full); background: rgba(253, 251, 247, 0.78);">
        <button
          v-for="option in [
            { key: 'A', label: '规范阅读', hint: '规范 Markdown 阅读器' },
            { key: 'B', label: '目录卡片', hint: '网格目录卡片' },
            { key: 'C', label: '极简阅读', hint: '纯净阅读模式' }
          ] as const"
          :key="option.key"
          type="button"
          :class="{ active: layoutMode === option.key }"
          :title="option.hint"
          style="border: 0; border-radius: var(--radius-full); background: transparent; color: var(--text-secondary); cursor: pointer; font-size: var(--text-xs); font-weight: 800; padding: 7px 11px;"
          :style="layoutMode === option.key ? 'background: var(--accent-primary) !important; color: #fdfbf7 !important;' : ''"
          @click="layoutMode = option.key"
        >
          <span>{{ option.key }}</span>
          {{ option.label }}
        </button>
      </div>
    </header>

    <div v-if="layoutMode === 'A'" class="layout-wrapper-a" style="display: flex; height: calc(100vh - 60px);">
      <aside class="docs-sidebar" style="width: 260px; border-right: 1px solid var(--border-default); padding: 24px; overflow-y: auto;">
        <div class="sidebar-title">文档中心</div>
        <nav class="sidebar-nav">
          <a v-for="sec in sections" :key="sec.id" :href="`#${sec.id}`" class="nav-link">
            {{ sec.title }}
          </a>
        </nav>
      </aside>

      <main class="docs-main" style="flex: 1; padding: 32px 48px; overflow-y: auto; max-width: 900px; margin: 0 auto;">
        <h1>平台操作手册</h1>
        <template v-for="sec in contentSections" :key="sec.id">
          <section :id="sec.id" v-html="sec.html" class="doc-section"></section>
        </template>
      </main>
    </div>

    <div v-else-if="layoutMode === 'B'" class="layout-wrapper-b" style="padding: 32px; max-width: 1200px; margin: 0 auto;">
      <h1 style="text-align: center; margin-bottom: 48px; color: var(--text-bright);">平台文档导览</h1>
      <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 24px;">
        <div v-for="sec in sections" :key="sec.id" class="doc-card" style="padding: 24px; border: 1px solid var(--border-default); border-radius: var(--radius-lg); background: var(--bg-elevated); cursor: pointer; transition: all 0.2s;" @click="onCardClick(sec.id)">
          <h3 style="margin: 0 0 12px; color: var(--accent-primary);">{{ sec.title }}</h3>
          <p style="margin: 0; font-size: var(--text-sm); color: var(--text-secondary); line-height: 1.5;">{{ sec.excerpt }}</p>
        </div>
      </div>
    </div>

    <div v-else-if="layoutMode === 'C'" class="layout-wrapper-c" style="padding: 48px 24px; max-width: 720px; margin: 0 auto; overflow-y: auto; height: calc(100vh - 60px);">
      <h1 style="font-size: var(--text-3xl); margin-bottom: 48px; color: var(--text-bright);">平台操作手册</h1>
      <template v-for="sec in contentSections" :key="sec.id">
        <section :id="sec.id" v-html="sec.html" class="doc-section" style="margin-bottom: 48px;"></section>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRoute } from 'vue-router'
import { usePageContext } from '@/app/pageContext'

const layoutMode = ref<'A' | 'B' | 'C'>('A')
const route = useRoute()

const sections = [
  { id: 'overview', title: '概览', excerpt: '平台以 AKQuant 事件驱动回测为主路径，同时保留内置回测能力。' },
  { id: 'engine-architecture', title: '引擎架构', excerpt: '前端提交 payload 后，后端通过统一回测引擎路由运行任务。' },
  { id: 'akquant-strategy', title: 'AKQuant 策略语法', excerpt: '推荐使用类风格策略，在 on_start 初始化，在 on_bar 处理行情并下单。' },
  { id: 'akquant-expression', title: '表达式模式', excerpt: '适合快速验证因子信号，例如 Mean($close, 5) 或 Alpha101 风格公式。' },
  { id: 'builtin-engine', title: 'Builtin 引擎', excerpt: '主要用于兼容旧策略和简单表达式回测。' },
  { id: 'risk-control', title: '风控规则', excerpt: 'A 股交易单位按 100 股一手处理，手续费、滑点由面板控制。' },
  { id: 'llm-strategy', title: 'LLM 策略生成', excerpt: '辅助把研报逻辑或旧框架代码转换为 AKQuant 策略。' },
  { id: 'quantstats-report', title: 'QuantStats 报告', excerpt: '回测结果可返回净值、交易记录、指标和 HTML 报告。' },
  { id: 'api-v2', title: 'Backtest API', excerpt: '回测与优化的 RESTful API 接口说明。' },
  { id: 'live-trading-guide', title: '实盘交易操作手册', excerpt: '包含上线前安全前提、Profile 选择、人工实盘和自动 runner 操作说明。' },
  { id: 'examples', title: '示例', excerpt: 'CashAware 策略等示例。' },
]

const contentSections = [
  {
    id: 'overview',
    html: `
      <h2>概览</h2>
      <p>平台以 AKQuant 事件驱动回测为主路径，同时保留内置回测能力。新回测接口统一使用 <code>/api/backtest/*</code>。</p>
    `
  },
  {
    id: 'engine-architecture',
    html: `
      <h2>引擎架构</h2>
      <p>前端提交 payload 后，后端通过统一回测引擎路由运行任务，并用任务状态接口查询进度和结果。</p>
    `
  },
  {
    id: 'akquant-strategy',
    html: `
      <h2>AKQuant 策略语法</h2>
      <p>推荐使用类风格策略：<code>class MyStrategy(aq.Strategy)</code>，在 <code>on_start</code> 初始化，在 <code>on_bar</code> 处理行情并下单。</p>
      <div class="code-block"><pre><code>import akquant as aq

class MyStrategy(aq.Strategy):
    def on_bar(self, bar):
        position = self.ctx.get_position(bar.symbol)
        if position == 0:
            self.buy(bar.symbol, 100)</code></pre></div>
    `
  },
  {
    id: 'akquant-expression',
    html: `
      <h2>表达式模式</h2>
      <p>表达式适合快速验证因子信号，例如 <code>Mean($close, 5)</code>、<code>RSI($close, 14)</code> 或 Alpha101 风格公式。</p>
    `
  },
  {
    id: 'builtin-engine',
    html: `
      <h2>Builtin 引擎</h2>
      <p>Builtin 引擎主要用于兼容旧策略和简单表达式回测。</p>
    `
  },
  {
    id: 'risk-control',
    html: `
      <h2>风控规则</h2>
      <ul>
        <li>A 股交易单位按 100 股一手处理。</li>
        <li>手续费、滑点、印花税、过户费由控制面板或 API payload 控制。</li>
        <li>策略参数通过 <code>strategy_params</code> 读取，不在策略代码中硬编码日期、资金或股票池。</li>
      </ul>
    `
  },
  {
    id: 'llm-strategy',
    html: `
      <h2>LLM 策略生成</h2>
      <p>策略回测页提供 LLM 生成入口，可辅助把研报逻辑或旧框架代码转换为 AKQuant 策略。</p>
    `
  },
  {
    id: 'quantstats-report',
    html: `
      <h2>QuantStats 报告</h2>
      <p>AKQuant 回测结果可返回净值、交易记录、指标和 HTML 报告。</p>
    `
  },
  {
    id: 'api-v2',
    html: `
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
    `
  },
  {
    id: 'live-trading-guide',
    html: `
      <h2>实盘交易操作手册</h2>
      <p>实盘模块位于 <code>/trade</code>，核心是“策略 Profile 白名单 + 订单篮子 + 审计”。默认接入两个 CashAware Profile：稳健版策略 ID <code>62</code> 和进攻版策略 ID <code>63</code>。</p>

      <div class="guide-card">
        <h3>一、上线前安全前提</h3>
        <ol class="step-list">
          <li>打开 miniQMT/华泰 QMT，确认已登录交易账号，并保持客户端在线。</li>
          <li>在 prod 后端配置文件中确认 <code>QMT_ACCOUNT_ID</code>、<code>QMT_ACCOUNT_TYPE</code>、<code>QMT_TRADER_PATH</code> 已填写正确。</li>
          <li>首次使用时保持 <code>LIVE_TRADING_ENABLE_ORDER_SUBMIT=false</code>、<code>LIVE_TRADING_AUTO_EXECUTE_ENABLED=false</code>，先只生成信号和模拟成交。</li>
          <li>进入 <code>/trade</code> 后点击“刷新”，状态栏应看到“行情：已连接”“交易模块：可用”。如果账号未配置，真实提交会被后端拒绝。</li>
        </ol>
        <p class="callout">真实下单需要后端配置、前端二次确认、QMT 连接、Profile 权限同时满足；任一条件不满足都会写入审计表，不会静默下单。</p>
      </div>

      <div class="guide-card">
        <h3>二、Profile 选择与管理</h3>
        <table class="info-table">
          <thead><tr><th>Profile</th><th>策略 ID</th><th>用途</th><th>建议</th></tr></thead>
          <tbody>
            <tr><td><code>tsmf_cashaware_stable</code></td><td><code>62</code></td><td>稳健 CashAware</td><td>默认优先使用</td></tr>
            <tr><td><code>tsmf_cashaware_aggressive</code></td><td><code>63</code></td><td>进攻 MissingGuard</td><td>确认风险后使用</td></tr>
          </tbody>
        </table>
        <ol class="step-list">
          <li>在“策略配置”下拉框选择 Profile。</li>
          <li>确认 Profile 处于“启用”状态；停用的 Profile 不能生成信号，也不能启动 runner。</li>
          <li>需要接入未来的新策略时，点击“新增 Profile”，填写策略 ID、Profile Key、显示名并保存。只有进入白名单的 Profile 才能进入实盘模块。</li>
          <li>需要改变默认策略时，选中 Profile 后点击“设为默认”。</li>
        </ol>
      </div>

      <div class="guide-card">
        <h3>三、推荐流程：先模拟，再人工实盘</h3>
        <ol class="step-list">
          <li>在顶部模式选择“模拟”。</li>
          <li>选择 Profile，交易日默认使用当天；指数池默认 <code>399101.SZ</code>，如需其它股票池可在 Profile 或参数里配置。</li>
          <li>点击“生成信号”，查看账户摘要、候选数量、目标持仓、订单篮子和跳过订单。</li>
          <li>检查“跳过订单”原因，例如缺行情、现金不足、整手约束、过滤后无候选。</li>
          <li>在订单篮子里人工调整数量或删除不想提交的订单。</li>
          <li>点击“提交篮子”。模拟模式只更新模拟账户和审计，不会发 QMT 真单。</li>
          <li>连续多个交易日确认信号、订单和审计都符合预期后，再考虑进入实盘模式。</li>
        </ol>
      </div>

      <div class="guide-card">
        <h3>四、人工实盘提交</h3>
        <ol class="step-list">
          <li>确认后端已设置 <code>LIVE_TRADING_ENABLE_ORDER_SUBMIT=true</code>，并重启 prod 后端。</li>
          <li>保持 <code>LIVE_TRADING_AUTO_EXECUTE_ENABLED=false</code>，先不要打开自动实盘。</li>
          <li>进入 <code>/trade</code>，选择“实盘”模式，点击“刷新”并确认状态栏显示真实下单“开启”。</li>
          <li>选择 Profile 后点击“生成信号”，逐笔检查订单篮子。</li>
          <li>必要时调整数量或删除订单，再点击“提交篮子”。前端会弹出真实委托确认框，确认后才会调用 QMT。</li>
          <li>提交后查看“订单审计”，确认状态为 <code>submitted</code> 或查看失败原因。</li>
        </ol>
        <p class="callout">人工实盘适合日常接管：系统负责生成 CashAware 两阶段订单，人负责最后确认和提交。</p>
      </div>

      <div class="guide-card">
        <h3>五、自动 runner</h3>
        <ol class="step-list">
          <li>只在模拟和人工实盘都稳定后启用自动 runner。</li>
          <li>自动实盘必须同时设置 <code>LIVE_TRADING_ENABLE_ORDER_SUBMIT=true</code> 与 <code>LIVE_TRADING_AUTO_EXECUTE_ENABLED=true</code>，并重启 prod 后端。</li>
          <li>在 <code>/trade</code> 选择 Profile 和模式，点击“启动自动”。实盘模式会再次弹出确认框。</li>
          <li>runner 在交易窗口内循环做早盘检查、准备盘中因子、生成信号，并按 Profile 权限自动提交订单。</li>
          <li>系统使用 <code>signal_hash</code> 防重复，同一批订单已经提交过时会写入 <code>duplicate</code> 审计并跳过。</li>
          <li>需要停止自动交易时点击“停止”；需要人接管时点击“人工接管”。</li>
        </ol>
      </div>

      <div class="guide-card">
        <h3>六、明天盘中因子准备流程</h3>
        <ol class="step-list">
          <li><strong>9:30 登录系统：</strong>进入 <code>/trade</code> 后，“早盘运行检查”会显示 QMT、交易阶段、数据依赖、因子缓存和“因子日期”。日频排序因子使用上一可用交易日，例如 2026-06-22 盘中会使用 2026-06-18 的 <code>market_cap</code> 等日频缓存。</li>
          <li><strong>9:30 到 10:30 前：</strong>如果当天 <code>10:30</code> 分钟线还没到，系统会把 <code>is_paused</code>、<code>is_limit_up</code>、<code>is_limit_down</code> 标记为等待，不会提前计算，也不会误下单。</li>
          <li><strong>10:30 调仓窗口：</strong>手动点击“生成信号”或 runner 自动执行时，后端先自动补当天 <code>stock_limit_prices</code> 和 <code>10:30</code> 分钟线，然后只预计算当天 timer 过滤因子。</li>
          <li><strong>生成信号：</strong>排序因子仍读取上一可用日频缓存；盘中过滤因子读取当天 <code>10:30</code>。因此当天 <code>daily_basic</code> 即使盘后或供应商提前写入，也不会用于当天盘中排序。</li>
          <li><strong>异常处理：</strong>如果 miniQMT 分钟线、Tushare 涨跌停价格或 QMT 账户不可用，系统会在预检和订单审计里写明原因，信号生成被阻断。</li>
        </ol>
        <p class="callout">当前两个 CashAware Profile 只需要 10:30 的停牌/涨跌停过滤因子。未来 Profile 配入 14:30 放量类因子后，同一套流程会在对应 timer 后触发准备。</p>
      </div>

      <div class="guide-card">
        <h3>七、人工接管与审计排障</h3>
        <table class="info-table">
          <thead><tr><th>审计状态</th><th>含义</th><th>处理方式</th></tr></thead>
          <tbody>
            <tr><td><code>generated</code></td><td>信号生成了订单草案</td><td>人工检查后提交或删除</td></tr>
            <tr><td><code>skipped</code></td><td>订单被策略或执行约束跳过</td><td>查看原因，例如现金不足、缺行情</td></tr>
            <tr><td><code>blocked</code></td><td>配置或权限阻止提交</td><td>检查真实下单开关、QMT 账号、Profile 权限</td></tr>
            <tr><td><code>paper_filled</code></td><td>模拟成交成功</td><td>可继续模拟验证</td></tr>
            <tr><td><code>submitted</code></td><td>真实订单已提交给 QMT</td><td>到 QMT 客户端核对委托和成交</td></tr>
            <tr><td><code>failed</code></td><td>提交失败</td><td>查看失败消息并检查 QMT 连接</td></tr>
            <tr><td><code>duplicate</code></td><td>重复信号被拦截</td><td>确认是否已经提交过同一批订单</td></tr>
            <tr><td><code>takeover</code></td><td>人工接管已触发</td><td>runner 停止，后续可手动生成信号和提交</td></tr>
          </tbody>
        </table>
      </div>

      <div class="guide-card">
        <h3>八、常见问题</h3>
        <ol class="step-list">
          <li><strong>真实下单显示关闭：</strong>后端仍是 <code>LIVE_TRADING_ENABLE_ORDER_SUBMIT=false</code>，需要修改配置并重启后端。</li>
          <li><strong>自动实盘启动失败：</strong>检查 <code>LIVE_TRADING_AUTO_EXECUTE_ENABLED</code>、QMT 账号配置、行情连接和交易模块状态。</li>
          <li><strong>没有订单：</strong>可能是股票池为空、因子过滤后无候选、现金不足或缺少实时行情。先看“跳过订单”和审计。</li>
          <li><strong>早盘看到当天日频数据缺失：</strong>这是正常保护逻辑。实盘盘中不会要求当天 <code>daily_basic</code>，日频排序因子只取上一可用缓存。</li>
          <li><strong>手动接管后自动不再提交：</strong>这是预期行为。确认后可重新点击“启动自动”。</li>
          <li><strong>新增策略不能运行：</strong>策略必须先写入 <code>live_strategy_profiles</code>，并且当前 v1 adapter 类型为 <code>multi_factor_cash_aware</code>。</li>
        </ol>
      </div>
    `
  },
  {
    id: 'examples',
    html: `
      <h2>示例</h2>
      <p>CashAware 策略可先在策略回测页验证参数，再进入模拟 / 实盘页通过 Profile 白名单生成信号、审计订单并按权限提交。</p>
    `
  }
]

const pageContextBlocks = computed(() => [
  {
    title: 'Docs',
    rows: [
      { label: '文档类型', value: 'Platform Manual' },
      { label: '章节数', value: String(sections.length) },
      { label: '当前锚点', value: route.hash || '#overview' },
    ],
  },
  {
    title: 'Topics',
    rows: [
      { label: '引擎', value: 'AKQuant / Builtin' },
      { label: '接口', value: '/api/backtest/*' },
      { label: '用途', value: '回测、模拟与实盘说明' },
    ],
  },
])

usePageContext(pageContextBlocks)

function onCardClick(id: string) {
  layoutMode.value = 'A'
  setTimeout(() => {
    document.getElementById(id)?.scrollIntoView()
  }, 100)
}
</script>

<style scoped>
.docs-container {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--bg-void);
  overflow: hidden;
}

.docs-sidebar {
  background: var(--bg-elevated);
}

.sidebar-title {
  margin-bottom: var(--space-4);
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}

.sidebar-nav {
  display: flex;
  flex-direction: column;
  gap: var(--space-1);
}

.nav-link {
  padding: 6px 12px;
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  font-size: var(--text-sm);
  text-decoration: none;
  transition: all 0.2s;
}

.nav-link:hover {
  background: var(--bg-hover);
  color: var(--text-bright);
}

.docs-main h1 {
  margin-top: 0;
  margin-bottom: var(--space-6);
  color: var(--text-bright);
  font-size: var(--text-3xl);
  font-weight: 800;
  letter-spacing: -0.02em;
}

.doc-card:hover {
  border-color: var(--accent-primary) !important;
  transform: translateY(-2px);
  box-shadow: var(--shadow-card);
}

:deep(.doc-section) {
  margin-bottom: var(--space-6);
}

:deep(.doc-section h2) {
  margin-bottom: var(--space-3);
  padding-bottom: var(--space-2);
  border-bottom: 1px solid var(--border-subtle);
  color: var(--text-primary);
  font-size: var(--text-xl);
  font-weight: 700;
}

:deep(.doc-section h3) {
  margin-top: var(--space-4);
  margin-bottom: var(--space-2);
  color: var(--text-primary);
  font-size: var(--text-lg);
  font-weight: 600;
}

:deep(.doc-section p) {
  margin-bottom: var(--space-3);
  color: var(--text-secondary);
  font-size: var(--text-base);
  line-height: 1.7;
}

:deep(.doc-section ul), :deep(.doc-section ol) {
  margin-bottom: var(--space-4);
  padding-left: var(--space-5);
  color: var(--text-secondary);
  font-size: var(--text-base);
  line-height: 1.7;
}

:deep(.doc-section li) {
  margin-bottom: var(--space-1);
}

:deep(.doc-section code) {
  padding: 2px 6px;
  border-radius: var(--radius-sm);
  background: rgba(148, 163, 184, 0.1);
  color: var(--accent-primary);
  font-family: monospace;
  font-size: 0.9em;
}

:deep(.doc-section pre) {
  margin-bottom: var(--space-4);
  padding: var(--space-4);
  border-radius: var(--radius-md);
  background: #1a2420;
  color: #dbe4f0;
  overflow-x: auto;
}

:deep(.doc-section pre code) {
  padding: 0;
  background: none;
  color: inherit;
  font-size: var(--text-sm);
  line-height: 1.5;
}

:deep(.info-table) {
  width: 100%;
  margin-bottom: var(--space-4);
  border-collapse: collapse;
  font-size: var(--text-sm);
}

:deep(.info-table th), :deep(.info-table td) {
  padding: var(--space-2) var(--space-3);
  border: 1px solid var(--border-default);
  text-align: left;
}

:deep(.info-table th) {
  background: var(--bg-elevated);
  color: var(--text-primary);
  font-weight: 600;
}

:deep(.guide-card) {
  margin-bottom: var(--space-5);
  padding: var(--space-4);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  background: var(--bg-elevated);
}

:deep(.guide-card h3) {
  margin-top: 0;
  color: var(--accent-primary);
}

:deep(.step-list) {
  margin-bottom: 0;
}

:deep(.callout) {
  margin-top: var(--space-3);
  margin-bottom: 0;
  padding: var(--space-3);
  border-left: 4px solid var(--accent-warning);
  background: rgba(245, 158, 11, 0.1);
  color: var(--text-primary);
  font-size: var(--text-sm);
}
</style>
