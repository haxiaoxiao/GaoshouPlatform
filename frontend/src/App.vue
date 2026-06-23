<template>
  <ElConfigProvider :message="messageConfig">
    <MainLayout />
  </ElConfigProvider>
</template>

<script setup lang="ts">
import { ElConfigProvider } from 'element-plus'
import MainLayout from '@/layouts/MainLayout.vue'

const messageConfig = {
  showClose: true,
}
</script>

<style>
/* Import design system */
@import '@/styles/design-system.css';
</style>
