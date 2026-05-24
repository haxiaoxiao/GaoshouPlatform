import { createApp } from 'vue'
import { createPinia } from 'pinia'
import { VueQueryPlugin, QueryClient } from '@tanstack/vue-query'
import { ElLoading } from 'element-plus'
import 'element-plus/theme-chalk/el-message.css'
import 'element-plus/theme-chalk/el-message-box.css'
import 'element-plus/theme-chalk/el-loading.css'

import App from './App.vue'
import router from './router'

const app = createApp(App)
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
    mutations: {
      retry: 0,
    },
  },
})

app.use(createPinia())
app.use(VueQueryPlugin, { queryClient })
app.use(router)
app.use(ElLoading)

// 全局错误处理，防止错误导致 Vue 响应式系统失效
app.config.errorHandler = (err, _instance, info) => {
  console.error('[Vue Error]', err)
  console.error('[Vue Error Info]', info)
}

// 捕获未处理的 Promise 错误
window.addEventListener('unhandledrejection', (event) => {
  console.error('[Unhandled Promise Rejection]', event.reason)
})

// 捕获全局错误
window.addEventListener('error', (event) => {
  console.error('[Global Error]', event.error)
})

app.mount('#app')
