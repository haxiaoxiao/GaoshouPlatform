<template>
  <div class="page-frame data-sync-page">
    <SyncPanel />
  </div>
</template>

<script setup lang="ts">
import SyncPanel from './SyncPanel.vue'
</script>

<style scoped>
.data-sync-page {
  min-height: 0;
  overflow: hidden;
  color: var(--text-primary);
  background: var(--bg-void);
}

@media (max-width: 900px) {
  .data-sync-page {
    height: 100%;
    min-height: 0;
    overflow: auto;
  }
}
</style>
