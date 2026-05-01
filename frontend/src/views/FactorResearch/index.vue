<template>
  <div class="factor-research">
    <FactorBoard />
    <router-view />
  </div>
</template>

<script setup lang="ts">
import FactorBoard from './FactorBoard.vue'
</script>

<style scoped>
.factor-research {
  height: 100%;
  display: flex;
  flex-direction: column;
}
</style>
