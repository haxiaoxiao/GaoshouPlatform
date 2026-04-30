import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      redirect: '/data',
    },
    {
      path: '/data',
      name: 'DataManage',
      component: () => import('@/views/DataManage/index.vue'),
      meta: { title: '数据管理' },
    },
    {
      path: '/explorer',
      name: 'DataExplorer',
      component: () => import('@/views/DataExplorer.vue'),
      meta: { title: '数据浏览器' },
    },
    {
      path: '/watchlist',
      name: 'Watchlist',
      component: () => import('@/views/Watchlist.vue'),
      meta: { title: '自选股' },
    },
    {
      path: '/stock/:symbol',
      name: 'StockDetail',
      component: () => import('@/views/StockDetail.vue'),
      meta: { title: '股票详情' },
    },
    {
      path: '/factor',
      name: 'FactorResearch',
      component: () => import('@/views/FactorResearch/index.vue'),
      meta: { title: '因子研究' },
      children: [
        {
          path: 'analysis/:id',
          name: 'FactorAnalysis',
          component: () => import('@/views/FactorResearch/FactorAnalysis.vue'),
          meta: { title: '因子分析' },
        },
      ],
    },
    {
      path: '/backtest',
      name: 'StrategyBacktest',
      component: () => import('@/views/StrategyBacktest/index.vue'),
      meta: { title: '策略回测' },
    },
    {
      path: '/trade',
      name: 'LiveTrading',
      component: () => import('@/views/LiveTrading/index.vue'),
      meta: { title: '实盘交易' },
    },
    {
      path: '/trend-capital',
      name: 'TrendCapital',
      component: () => import('@/views/TrendCapital/index.vue'),
      meta: { title: '趋势资金策略' },
    },
    {
      path: '/monitor',
      name: 'SystemMonitor',
      component: () => import('@/views/SystemMonitor/index.vue'),
      meta: { title: '系统监控' },
    },
  ],
})

export default router
