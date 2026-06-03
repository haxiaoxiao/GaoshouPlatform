<template>
  <div class="page-frame data-sync-page">
    <header class="panel-card sync-hero">
      <div>
        <span class="section-kicker">DATA SYNC / QUEUE FIRST</span>
        <h2>数据同步</h2>
        <p>
          同步任务从数据查看页拆出：这里专注任务目录、执行参数、队列、运行进度和最近记录，避免查看页被操作控件挤满。
        </p>
      </div>
      <div class="sync-hero__side">
        <div class="sync-hero__tips">
          <span>dev / prod 存储隔离</span>
          <span>长任务排队执行</span>
          <span>QMT / Relay 显式依赖</span>
        </div>
        <el-button type="primary" :loading="quickSyncing" :disabled="quickSyncDisabled" @click="runQuickSync">
          一键同步核心数据
        </el-button>
      </div>
    </header>

    <SyncPanel />
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import SyncPanel from './SyncPanel.vue'
import { syncApi, type SyncStatus } from '@/api/sync'

const quickSyncing = ref(false)
const syncStatus = ref<SyncStatus | null>(null)
const quickSyncDisabled = computed(() => (
  syncStatus.value?.can_trigger === false
  || syncStatus.value?.sync_service_available === false
  || syncStatus.value?.details?.sync_service_unavailable === true
))
const syncUnavailableReason = computed(() => (
  syncStatus.value?.reason
  || String(syncStatus.value?.details?.proxy_error || '')
  || 'dev 同步服务未启动或正在执行任务，请先启动 18810 同步服务后再提交。'
))

onMounted(loadSyncStatus)

async function loadSyncStatus() {
  try {
    const status = await syncApi.getStatus()
    syncStatus.value = status.details?.sync_service_unavailable
      ? {
          ...status,
          sync_service_available: false,
          can_trigger: false,
          reason: status.reason || String(status.details.proxy_error || 'dev 同步服务未启动'),
        }
      : status
  } catch (error: any) {
    syncStatus.value = {
      sync_type: null,
      status: 'idle',
      total: 0,
      current: 0,
      success_count: 0,
      failed_count: 0,
      progress_percent: 0,
      start_time: null,
      end_time: null,
      error_message: null,
      details: {},
      sync_service_available: false,
      can_trigger: false,
      reason: error?.message || 'dev 同步服务状态接口不可用',
    }
  }
}

async function runQuickSync() {
  quickSyncing.value = true
  try {
    await loadSyncStatus()
    if (quickSyncDisabled.value) {
      ElMessage.warning(syncUnavailableReason.value)
      return
    }
    await syncApi.trigger({
      sync_type: 'datasync',
      failure_strategy: 'skip',
      full_sync: false,
    })
    await loadSyncStatus()
    ElMessage.success('一键同步任务已提交，可在下方进度和最近记录中查看')
  } catch (error: any) {
    const detail = error?.response?.data?.detail || error?.message || String(error)
    ElMessage.error(`一键同步提交失败：${detail}`)
  } finally {
    quickSyncing.value = false
  }
}
</script>

<style scoped>
.data-sync-page {
  overflow: auto;
}

.sync-hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: var(--space-4);
  padding: var(--space-5);
}

.sync-hero h2 {
  margin: var(--space-1) 0 var(--space-2);
}

.sync-hero p {
  max-width: 760px;
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.sync-hero__side {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: var(--space-3);
}

.sync-hero__tips {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: var(--space-2);
  max-width: 420px;
}

.sync-hero__tips span {
  padding: 7px 10px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-full);
  color: var(--text-secondary);
  background: rgba(10, 14, 20, 0.58);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

:deep(.sync-workbench) {
  min-height: 0;
}

@media (max-width: 900px) {
  .sync-hero {
    grid-template-columns: 1fr;
  }

  .sync-hero__tips {
    justify-content: flex-start;
  }

  .sync-hero__side {
    align-items: flex-start;
  }
}
</style>
