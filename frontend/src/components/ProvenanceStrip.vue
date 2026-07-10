<template>
  <section class="provenance-strip" aria-label="研究与发布血缘">
    <div class="provenance-item">
      <span>数据日期</span>
      <strong>{{ context.dataDate }}</strong>
    </div>
    <div class="provenance-item">
      <span>数据快照</span>
      <strong>{{ snapshotId || '未绑定' }}</strong>
    </div>
    <div class="provenance-item">
      <span>因子版本</span>
      <strong>{{ factorVersion || '未绑定' }}</strong>
    </div>
    <div class="provenance-item">
      <span>发布版本</span>
      <strong>{{ releaseId || '未绑定' }}</strong>
    </div>
    <div class="provenance-item provenance-item--gaps" :class="{ 'provenance-item--risk': gaps.length }">
      <span>数据缺口</span>
      <strong>{{ gaps.length ? gaps.join(' / ') : '无' }}</strong>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { formatReadinessContext, systemApi, type PlatformReadiness } from '@/api/system'

defineProps<{
  snapshotId?: string | null
  factorVersion?: string | null
  releaseId?: string | null
}>()

const readiness = ref<PlatformReadiness | null>(null)
const context = computed(() => readiness.value
  ? formatReadinessContext(readiness.value)
  : {
      dataDate: '检查中',
      environmentLabel: 'UNKNOWN',
      readinessLabel: '检查中',
      orderSubmitLabel: '下单状态未知',
      orderSubmitRisk: false,
    })
const gaps = computed(() => Object.values(readiness.value?.datasets || {})
  .filter(item => item.status !== 'ready')
  .map(item => `${item.dataset}:${item.status}`))

onMounted(async () => {
  readiness.value = await systemApi.readiness().catch(() => null)
})
</script>

<style scoped>
.provenance-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(110px, 0.7fr)) minmax(180px, 1.2fr);
  gap: 1px;
  overflow: hidden;
  flex: 0 0 auto;
  border: 1px solid var(--border-default);
  border-radius: 6px;
  background: var(--border-default);
}

.provenance-item {
  min-width: 0;
  display: grid;
  gap: 2px;
  padding: 7px 9px;
  background: rgba(253, 251, 247, 0.94);
}

.provenance-item span {
  color: var(--text-muted);
  font-size: 10px;
}

.provenance-item strong {
  overflow: hidden;
  color: var(--text-primary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.provenance-item--risk strong {
  color: var(--accent-warning);
}

@media (max-width: 900px) {
  .provenance-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .provenance-item--gaps {
    grid-column: 1 / -1;
  }
}
</style>
