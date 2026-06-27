export type NavSectionKey = 'research' | 'operations'
export type ContextTone = 'good' | 'warn' | 'bad' | 'neutral'

export interface ContextRow {
  label: string
  value: string
  tone?: ContextTone
}

export interface ContextBlock {
  title: string
  action?: string
  rows: ContextRow[]
}

export interface AppNavItem {
  key: string
  path: string
  activePatterns?: string[]
  section: NavSectionKey
  label: string
  hint: string
  subtitle: string
  kicker: string
  badge?: string
  icon: string
  context: ContextBlock[]
}

export interface NavSection {
  key: NavSectionKey
  label: string
}

const icons = {
  home: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 11l9-8 9 8"/><path d="M5 10v10h14V10"/><path d="M9 20v-6h6v6"/></svg>',
  data: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg>',
  sync: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.5 2v6h-6"/><path d="M2.5 22v-6h6"/><path d="M2 11.5a10 10 0 0 1 18.8-4.3"/><path d="M22 12.5a10 10 0 0 1-18.8 4.2"/></svg>',
  explorer: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/><path d="M11 8v6"/><path d="M8 11h6"/></svg>',
  watchlist: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>',
  factor: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="M18.7 8l-5.1 5.2-2.8-2.7L7 14.3"/></svg>',
  research: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M4 4.5A2.5 2.5 0 0 1 6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5z"/><path d="M8 7h8"/><path d="M8 11h6"/></svg>',
  backtest: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18"/><path d="M9 21V9"/></svg>',
  trade: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/></svg>',
  monitor: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8"/><path d="M12 17v4"/></svg>',
  docs: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M20 22V2H6.5A2.5 2.5 0 0 0 4 4.5v15"/><path d="M8 7h8"/><path d="M8 11h8"/></svg>',
}

export const NAV_SECTIONS: NavSection[] = [
  { key: 'research', label: 'Research Pipeline' },
  { key: 'operations', label: 'Operations' },
]

export const APP_NAV_ITEMS: AppNavItem[] = [
  {
    key: 'home',
    path: '/home',
    section: 'research',
    label: '工作台',
    hint: 'Home',
    subtitle: '一屏查看系统健康、数据新鲜度、任务状态、因子/回测链路和交易护栏。',
    kicker: 'PLATFORM COCKPIT',
    badge: 'Now',
    icon: icons.home,
    context: [
      {
        title: '首页原则',
        rows: [
          { label: '状态', value: '健康 + 新鲜度', tone: 'good' },
          { label: '任务', value: '活动优先' },
          { label: '操作', value: '跳转模块' },
        ],
      },
      {
        title: '护栏',
        rows: [
          { label: 'Prod', value: '不在首页直接写入', tone: 'warn' },
          { label: '交易', value: '默认仅信号', tone: 'good' },
        ],
      },
    ],
  },
  {
    key: 'data',
    path: '/data',
    activePatterns: ['/stock/*'],
    section: 'research',
    label: '数据查看',
    hint: 'View',
    subtitle: '按最新口径查看行情、基础数据、财务、指标、概念和舆情，不把同步操作挤进同一屏。',
    kicker: 'DATA VIEW',
    badge: 'Core',
    icon: icons.data,
    context: [
      {
        title: '查看口径',
        rows: [
          { label: 'Daily', value: '最新 trade_date', tone: 'good' },
          { label: 'Minute', value: '最新 datetime' },
          { label: 'Stocks', value: '最近基础同步' },
          { label: 'Sentiment', value: 'latest published_at' },
        ],
      },
      {
        title: '查看原则',
        rows: [
          { label: '容量', value: '不做首屏指标' },
          { label: '查询', value: '按需加载' },
          { label: '同步', value: '独立页面', tone: 'good' },
        ],
      },
    ],
  },
  {
    key: 'data-sync',
    path: '/data/sync',
    section: 'operations',
    label: '数据同步',
    hint: 'Sync',
    subtitle: '集中管理同步任务目录、执行参数、队列、运行进度和最近记录。',
    kicker: 'DATA SYNC',
    icon: icons.sync,
    context: [
      {
        title: '同步护栏',
        rows: [
          { label: '长任务', value: '队列化运行', tone: 'good' },
          { label: 'QMT', value: '显式依赖', tone: 'warn' },
          { label: 'Relay', value: 'Key 可见' },
        ],
      },
      {
        title: '隔离原则',
        rows: [
          { label: 'dev/prod', value: '端口和存储分离', tone: 'good' },
          { label: '失败策略', value: 'skip/retry/stop' },
        ],
      },
    ],
  },
  {
    key: 'explorer',
    path: '/explorer',
    section: 'research',
    label: '数据浏览器',
    hint: 'Query',
    subtitle: '本地行情、因子缓存和派生数据的快速预览，精确统计按需执行。',
    kicker: 'DATA EXPLORER',
    icon: icons.explorer,
    context: [
      {
        title: '查询策略',
        rows: [
          { label: '首屏', value: '分区元数据', tone: 'good' },
          { label: 'Preview', value: '默认不 count(*)' },
          { label: 'Schema', value: '后台线程读取' },
        ],
      },
      {
        title: '性能提醒',
        rows: [
          { label: '大目录', value: '禁止默认全扫', tone: 'warn' },
          { label: '精确行数', value: '用户显式刷新' },
        ],
      },
    ],
  },
  {
    key: 'watchlist',
    path: '/watchlist',
    section: 'research',
    label: '自选股',
    hint: 'Pool',
    subtitle: '分组管理研究股票池，后续和策略、因子、数据覆盖联动。',
    kicker: 'UNIVERSE',
    icon: icons.watchlist,
    context: [
      {
        title: '股票池上下文',
        rows: [
          { label: '分组', value: 'Watchlist groups' },
          { label: '详情', value: 'Stock detail route' },
          { label: '用途', value: '研究 / 回测候选' },
        ],
      },
    ],
  },
  {
    key: 'factor',
    path: '/factor',
    activePatterns: ['/factor/detail/*'],
    section: 'research',
    label: '因子定义',
    hint: 'Define',
    subtitle: '管理因子目录、覆盖率、参数版本和预计算；表达式只在新建或编辑时打开。',
    kicker: 'FACTOR DEFINITION',
    badge: 'Alpha',
    icon: icons.factor,
    context: [
      {
        title: '定义上下文',
        rows: [
          { label: 'Catalog', value: '因子定义目录' },
          { label: 'Coverage', value: '缓存覆盖率' },
          { label: 'Precompute', value: '落盘入口', tone: 'good' },
        ],
      },
      {
        title: '使用边界',
        rows: [
          { label: 'Expression', value: '创建/编辑时打开' },
          { label: '依赖数据', value: '缺口显式提示', tone: 'warn' },
        ],
      },
    ],
  },
  {
    key: 'factor-evaluation',
    path: '/factor/evaluation',
    activePatterns: ['/factor/analysis-new/*'],
    section: 'research',
    label: '因子评估',
    hint: 'Eval',
    subtitle: '查看 IC、ICIR、多空收益、回撤、换手和已计算组合；只消费已预计算的因子缓存。',
    kicker: 'FACTOR EVALUATION',
    icon: icons.factor,
    context: [
      {
        title: '评估指标',
        rows: [
          { label: 'IC', value: '滚动窗口' },
          { label: 'Long-short', value: '多空收益' },
          { label: 'Drawdown', value: '最大回撤', tone: 'warn' },
        ],
      },
      {
        title: '计算前置',
        rows: [
          { label: 'Cache', value: '先预计算', tone: 'good' },
          { label: 'OOS', value: '必须验证', tone: 'warn' },
        ],
      },
    ],
  },
  {
    key: 'research',
    path: '/research',
    section: 'research',
    label: '研究实验室',
    hint: 'Idea',
    subtitle: '从研究假设到实验记录再到策略转化，保留证据链和失败复盘。',
    kicker: 'RESEARCH LAB',
    icon: icons.research,
    context: [
      {
        title: '研究链路',
        rows: [
          { label: '假设', value: 'Idea cards' },
          { label: '证据', value: '公告 / 行情 / 财务' },
          { label: '转化', value: '策略候选' },
        ],
      },
    ],
  },
  {
    key: 'backtest',
    path: '/backtest',
    activePatterns: ['/backtest/factor/*', '/backtest/optimization/*'],
    section: 'research',
    label: '策略回测',
    hint: 'Run',
    subtitle: '把策略编辑、参数配置、运行日志、报告预览和交易回放并排呈现。',
    kicker: 'BACKTEST COCKPIT',
    badge: 'Run',
    icon: icons.backtest,
    context: [
      {
        title: '运行配置',
        rows: [
          { label: 'Engine', value: 'Built-in / AKQuant' },
          { label: 'Bar Type', value: 'daily / minute_timer' },
          { label: 'Warm Start', value: '可控分段', tone: 'good' },
        ],
      },
      {
        title: '报告护栏',
        rows: [
          { label: 'Benchmark', value: '指数净值对比' },
          { label: 'Warnings', value: '不静默吞掉', tone: 'warn' },
          { label: 'Checkpoint', value: '序列化失败即失败' },
        ],
      },
    ],
  },
  {
    key: 'trade',
    path: '/trade',
    section: 'research',
    label: '模拟 / 实盘',
    hint: 'Live',
    subtitle: '先模拟再实盘，聚焦仓位偏离、订单审计、风控护栏和日终复盘。',
    kicker: 'TRADING GUARDRAILS',
    icon: icons.trade,
    context: [
      {
        title: '交易状态',
        rows: [
          { label: 'Mode', value: 'Paper first', tone: 'good' },
          { label: 'Submit', value: '默认关闭', tone: 'warn' },
          { label: 'QMT', value: '可选外部依赖' },
        ],
      },
    ],
  },
  {
    key: 'monitor',
    path: '/monitor',
    section: 'operations',
    label: '系统运维',
    hint: 'Ops',
    subtitle: '集中观察前端、后端、同步服务、存储后端和任务日志。',
    kicker: 'PLATFORM OPS',
    icon: icons.monitor,
    context: [
      {
        title: '服务健康',
        rows: [
          { label: 'Backend', value: '8800' },
          { label: 'Frontend', value: '3500' },
          { label: 'Sync', value: '8810' },
          { label: 'QMT', value: 'Optional' },
        ],
      },
      {
        title: '运行原则',
        rows: [
          { label: '长任务', value: '启动 + 轮询' },
          { label: 'CPU', value: '避免阻塞扫描', tone: 'good' },
        ],
      },
    ],
  },
  {
    key: 'docs',
    path: '/docs',
    section: 'operations',
    label: '文档中心',
    hint: 'Docs',
    subtitle: '沉淀数据源、因子、回测、AKQuant 和操作手册。',
    kicker: 'RUNBOOK',
    icon: icons.docs,
    context: [
      {
        title: '文档入口',
        rows: [
          { label: '用户手册', value: 'docs/user-manual' },
          { label: '数据源', value: 'cheatsheet' },
          { label: 'AKQuant', value: 'integration notes' },
        ],
      },
    ],
  },
]

export function navItemsForSection(section: NavSectionKey): AppNavItem[] {
  return APP_NAV_ITEMS.filter(item => item.section === section)
}

export function routeMetaForPath(path: string, title?: string, subtitle?: string) {
  const item = APP_NAV_ITEMS.find(navItem => navItem.path === path)
  return {
    title: title || item?.label || '高手平台',
    subtitle: subtitle || item?.subtitle || '',
    kicker: item?.kicker || 'GAOSHOU',
  }
}

export function resolveNavItem(path: string): AppNavItem | undefined {
  const normalizedPath = path || '/'
  return APP_NAV_ITEMS
    .filter(item => isRouteMatch(normalizedPath, item))
    .sort((left, right) => right.path.length - left.path.length)[0]
}

function isRouteMatch(path: string, item: AppNavItem): boolean {
  const patterns = [item.path, ...(item.activePatterns || [])]
  return patterns.some(pattern => {
    if (pattern.endsWith('*')) return path.startsWith(pattern.slice(0, -1))
    return path === pattern || path.startsWith(`${pattern}/`)
  })
}
