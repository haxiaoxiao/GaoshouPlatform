import { createApp } from 'vue'
import { createPinia } from 'pinia'
import { ElLoading, ElMessage } from 'element-plus'
import 'element-plus/theme-chalk/el-message.css'
import 'element-plus/theme-chalk/el-message-box.css'
import 'element-plus/theme-chalk/el-loading.css'

import App from './App.vue'
import { setRequestErrorNotifier } from './api/requestNotifications'
import router from './router'

const app = createApp(App)
setRequestErrorNotifier(message => ElMessage.error(message))

app.use(createPinia())
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
