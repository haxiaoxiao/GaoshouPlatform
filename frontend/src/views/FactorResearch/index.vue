<template>
  <div class="factor-research">
    <el-tabs v-model="activeTab" type="border-card" class="main-tabs">
      <el-tab-pane label="指标总览" name="overview" lazy>
        <IndicatorOverview />
      </el-tab-pane>
      <el-tab-pane label="选股筛选" name="screen" lazy>
        <StockScreen />
      </el-tab-pane>
      <el-tab-pane label="因子列表" name="factors" lazy>
        <FactorList />
      </el-tab-pane>
      <el-tab-pane label="因子合成" name="compose" lazy>
        <div class="placeholder">
          <el-empty description="因子合成模块开发中..." />
        </div>
      </el-tab-pane>
    </el-tabs>

    <router-view />
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useRoute } from 'vue-router'
import { computed } from 'vue'
import IndicatorOverview from './IndicatorOverview.vue'
import StockScreen from './StockScreen.vue'
import FactorList from './FactorList.vue'

const route = useRoute()
const activeTab = ref('overview')

const isAnalysisPage = computed(() => route.name === 'FactorAnalysis')
</script>

<style scoped>
.factor-research {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.main-tabs {
  flex: 1;
  display: flex;
  flex-direction: column;
}

.main-tabs :deep(.el-tabs__content) {
  flex: 1;
  overflow: auto;
}

.main-tabs :deep(.el-tab-pane) {
  height: 100%;
}

.placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
}
</style>
