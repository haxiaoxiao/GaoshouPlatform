import { createRouter, createWebHistory } from 'vue-router'
import { routeMetaForPath } from '@/app/navigation'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      redirect: '/trade',
    },
    {
      path: '/home',
      name: 'HomeWorkbench',
      component: () => import('@/views/HomeWorkbench.vue'),
      meta: routeMetaForPath('/home'),
    },
    {
      path: '/data',
      name: 'DataManage',
      component: () => import('@/views/DataManage/index.vue'),
      meta: routeMetaForPath('/data'),
    },
    {
      path: '/data/sync',
      name: 'DataSync',
      component: () => import('@/views/DataManage/SyncPage.vue'),
      meta: routeMetaForPath('/data/sync'),
    },
    {
      path: '/explorer',
      name: 'DataExplorer',
      component: () => import('@/views/DataExplorer.vue'),
      meta: routeMetaForPath('/explorer'),
    },
    {
      path: '/watchlist',
      name: 'Watchlist',
      component: () => import('@/views/Watchlist.vue'),
      meta: routeMetaForPath('/watchlist'),
    },
    {
      path: '/stock/:symbol',
      name: 'StockDetail',
      component: () => import('@/views/StockDetail.vue'),
      meta: routeMetaForPath('/data', '股票详情', '个股行情、基础信息和研究入口。'),
    },
    {
      path: '/factor',
      name: 'FactorResearch',
      component: () => import('@/views/FactorResearch/index.vue'),
      meta: routeMetaForPath('/factor'),
      children: [
        {
          path: 'analysis-new/:id',
          name: 'FactorAnalysisNew',
          component: () => import('@/views/FactorResearch/FactorAnalysisNew.vue'),
          meta: routeMetaForPath('/factor', '因子分析', '查看单次因子评估、分组净值、基准和超额表现。'),
        },
        {
          path: 'detail/:factorName',
          name: 'FactorDetail',
          component: () => import('@/views/FactorResearch/FactorDetail.vue'),
          meta: routeMetaForPath('/factor', '因子详情', '检查因子定义、覆盖率、缓存和研究结果。'),
        },
      ],
    },
    {
      path: '/factor/evaluation',
      name: 'FactorEvaluation',
      component: () => import('@/views/FactorResearch/index.vue'),
      meta: routeMetaForPath('/factor/evaluation'),
    },
    {
      path: '/research',
      name: 'InvestmentResearch',
      component: () => import('@/views/InvestmentResearch/index.vue'),
      meta: routeMetaForPath('/research'),
    },
    {
      path: '/backtest',
      name: 'StrategyBacktest',
      component: () => import('@/views/StrategyBacktest/index.vue'),
      meta: routeMetaForPath('/backtest'),
    },
    {
      path: '/backtest/factor/:id',
      name: 'FactorBacktest',
      component: () => import('@/views/FactorBacktest/index.vue'),
      meta: routeMetaForPath('/backtest', '因子回测', '把因子多空策略和指数基准放在同一条曲线上验证。'),
    },
    {
      path: '/backtest/optimization/:id',
      name: 'OptimizationReport',
      component: () => import('@/views/StrategyBacktest/OptimizationReport.vue'),
      meta: routeMetaForPath('/backtest', '优化结果', '检查参数稳定性、样本外窗口和发布门槛。'),
    },
    {
      path: '/trade',
      name: 'LiveTrading',
      component: () => import('@/views/LiveTrading/index.vue'),
      meta: routeMetaForPath('/trade'),
    },
    {
      path: '/monitor',
      name: 'SystemMonitor',
      component: () => import('@/views/SystemMonitor/index.vue'),
      meta: routeMetaForPath('/monitor'),
    },
    {
      path: '/docs',
      name: 'Docs',
      component: () => import('@/views/Docs/index.vue'),
      meta: routeMetaForPath('/docs'),
    },
  ],
})

export default router
