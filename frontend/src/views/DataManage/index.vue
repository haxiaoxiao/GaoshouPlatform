<template>
  <div class="page-container">
    <el-tabs v-model="activeTab" class="data-tabs">
      <el-tab-pane label="股票列表" name="stockList">
        <StockList />
      </el-tab-pane>
      <el-tab-pane label="自选股" name="watchlist">
        <div class="placeholder-content">
          <el-empty description="自选股功能开发中..." />
        </div>
      </el-tab-pane>
      <el-tab-pane label="数据导入" name="import">
        <div class="placeholder-content">
          <el-empty description="数据导入功能开发中..." />
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import StockList from './StockList.vue'

const activeTab = ref('stockList')
</script>

<style scoped>
.page-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.data-tabs {
  height: 100%;
  background: #fff;
  border-radius: 4px;
  padding: 16px;
}

.data-tabs :deep(.el-tabs__content) {
  flex: 1;
  overflow: auto;
}

.data-tabs :deep(.el-tab-pane) {
  height: 100%;
}

.placeholder-content {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 300px;
}
</style>
