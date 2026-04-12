import axios, { type AxiosInstance, type AxiosResponse } from 'axios'
import { ElMessage } from 'element-plus'

const instance: AxiosInstance = axios.create({
  baseURL: '/api',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// 响应拦截器 - 解包 response.data，并解包后端的 { code, message, data } 包装
instance.interceptors.response.use(
  (response: AxiosResponse) => {
    // 先解包 axios 的 response.data
    const backendData = response.data
    // 如果后端返回的是 { code, message, data } 格式，解包 data 字段
    if (backendData && typeof backendData === 'object' && 'code' in backendData && 'data' in backendData) {
      if (backendData.code === 0) {
        return backendData.data
      } else {
        // 业务错误
        ElMessage.error(backendData.message || '请求失败')
        return Promise.reject(new Error(backendData.message || '请求失败'))
      }
    }
    return backendData
  },
  (error) => {
    const message = error.response?.data?.detail || error.message || '请求失败'
    ElMessage.error(message)
    return Promise.reject(error)
  }
)

// 包装请求方法，返回正确的类型（拦截器已经解包了 data）
const request = {
  get: <T>(url: string, config?: object): Promise<T> =>
    instance.get(url, config) as Promise<T>,

  post: <T>(url: string, data?: unknown, config?: object): Promise<T> =>
    instance.post(url, data, config) as Promise<T>,

  put: <T>(url: string, data?: unknown, config?: object): Promise<T> =>
    instance.put(url, data, config) as Promise<T>,

  delete: <T>(url: string, config?: object): Promise<T> =>
    instance.delete(url, config) as Promise<T>,
}

export default request
