import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'
import AutoImport from 'unplugin-auto-import/vite'
import Components from 'unplugin-vue-components/vite'
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers'

const apiProxyTarget = process.env.VITE_API_PROXY_TARGET || 'http://127.0.0.1:18800'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    AutoImport({
      resolvers: [ElementPlusResolver({ importStyle: 'css' })],
    }),
    Components({
      directives: true,
      resolvers: [ElementPlusResolver({ importStyle: 'css' })],
    }),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  },
  build: {
    rolldownOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) return
          if (id.includes('echarts/charts')) return 'vendor-echarts-charts'
          if (id.includes('echarts/components')) return 'vendor-echarts-components'
          if (id.includes('echarts/renderers')) return 'vendor-echarts-renderers'
          if (id.includes('echarts/core')) return 'vendor-echarts-core'
          if (id.includes('zrender')) return 'vendor-zrender'
          if (id.includes('echarts')) return 'vendor-echarts'
          if (id.includes('@codemirror') || id.includes('codemirror')) return 'vendor-codemirror'
          if (id.includes('element-plus/es/components/table')) return 'vendor-element-table'
          if (id.includes('element-plus/es/components/date-picker')) return 'vendor-element-date-picker'
          if (id.includes('element-plus/es/components/select')) return 'vendor-element-select'
          if (id.includes('element-plus/es/components')) return 'vendor-element-components'
          if (id.includes('element-plus/es/directives')) return 'vendor-element-directives'
          if (id.includes('@element-plus/icons-vue')) return 'vendor-element-icons'
          if (id.includes('element-plus')) return 'vendor-element-core'
          if (id.includes('lightweight-charts')) return 'vendor-lightweight-charts'
          if (id.includes('@tanstack')) return 'vendor-query'
          if (id.includes('@vueuse')) return 'vendor-vueuse'
          if (id.includes('axios')) return 'vendor-request'
          if (id.includes('vue-router')) return 'vendor-router'
          if (id.includes('pinia')) return 'vendor-pinia'
          if (id.includes('vue')) return 'vendor-vue'
        },
      },
    },
  },
  server: {
    host: '127.0.0.1',
    port: 13500,
    proxy: {
      '/api': {
        target: apiProxyTarget,
        changeOrigin: true,
      },
    },
  },
})
