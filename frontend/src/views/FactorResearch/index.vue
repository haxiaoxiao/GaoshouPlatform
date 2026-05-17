<template>
  <div class="factor-research">
    <el-tabs v-model="activeTab" class="factor-tabs">
      <el-tab-pane label="Factor Board" name="board">
        <FactorBoard />
      </el-tab-pane>
      <el-tab-pane label="Feature Store" name="features">
        <FeatureStore />
      </el-tab-pane>
    </el-tabs>
    <router-view />
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import FactorBoard from './FactorBoard.vue'
import FeatureStore from './FeatureStore.vue'

const activeTab = ref('board')
</script>

<style scoped>
.factor-research {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.factor-tabs {
  min-height: 0;
  flex: 1;
}

:deep(.el-tabs__content) {
  height: calc(100% - 48px);
  overflow: auto;
}

:deep(.el-tab-pane) {
  height: 100%;
}
</style>
