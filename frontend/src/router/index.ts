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
      path: '/monitor',
      name: 'SystemMonitor',
      component: () => import('@/views/SystemMonitor/index.vue'),
      meta: { title: '系统监控' },
    },
  ],
})

export default router
