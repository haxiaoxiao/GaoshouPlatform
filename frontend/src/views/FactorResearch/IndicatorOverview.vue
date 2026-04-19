<template>
  <div class="indicator-overview">
    <div class="category-filter">
      <el-radio-group v-model="activeCategory" @change="loadIndicators">
        <el-radio-button value="">全部</el-radio-button>
        <el-radio-button
          v-for="cat in categories"
          :key="cat.key"
          :value="cat.key"
        >
          {{ cat.label }} ({{ cat.count }})
        </el-radio-button>
      </el-radio-group>
    </div>

    <div v-loading="loading" class="indicator-grid">
      <el-card
        v-for="ind in indicators"
        :key="ind.name"
        class="indicator-card"
        shadow="hover"
        @click="showDetail(ind)"
      >
        <div class="card-title">{{ ind.display_name }}</div>
        <div class="card-meta">
          <el-tag size="small" :type="categoryTagType(ind.category)">
            {{ ind.category_label }}
          </el-tag>
          <el-tag size="small" type="info" style="margin-left: 6px">
            {{ ind.data_type }}
          </el-tag>
          <el-tag
            v-if="!ind.is_precomputed"
            size="small"
            type="warning"
            style="margin-left: 6px"
          >
            实时
          </el-tag>
        </div>
        <div class="card-desc">{{ ind.description }}</div>
        <div class="card-tags" v-if="ind.tags.length">
          <el-tag
            v-for="tag in ind.tags"
            :key="tag"
            size="small"
            effect="plain"
            style="margin: 2px"
          >
            {{ tag }}
          </el-tag>
        </div>
      </el-card>
    </div>

    <el-dialog
      v-model="detailVisible"
      :title="detailData?.display_name"
      width="500px"
    >
      <el-descriptions :column="1" border v-if="detailData">
        <el-descriptions-item label="标识">{{ detailData.name }}</el-descriptions-item>
        <el-descriptions-item label="分类">{{ detailData.category_label }}</el-descriptions-item>
        <el-descriptions-item label="数据类型">{{ detailData.data_type }}</el-descriptions-item>
        <el-descriptions-item label="计算方式">
          {{ detailData.is_precomputed ? '预计算' : '实时计算' }}
        </el-descriptions-item>
        <el-descriptions-item label="描述">{{ detailData.description }}</el-descriptions-item>
        <el-descriptions-item label="依赖" v-if="detailData.dependencies.length">
          {{ detailData.dependencies.join(', ') }}
        </el-descriptions-item>
        <el-descriptions-item label="标签">
          <el-tag v-for="tag in detailData.tags" :key="tag" size="small" style="margin: 2px">{{ tag }}</el-tag>
        </el-descriptions-item>
      </el-descriptions>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { indicatorApi, type CategoryInfo, type IndicatorInfo } from '@/api/indicator'

const loading = ref(false)
const categories = ref<CategoryInfo[]>([])
const indicators = ref<IndicatorInfo[]>([])
const activeCategory = ref('')
const detailVisible = ref(false)
const detailData = ref<IndicatorInfo | null>(null)

const categoryTagType = (category: string): '' | 'success' | 'warning' | 'danger' | 'info' => {
  const map: Record<string, '' | 'success' | 'warning' | 'danger' | 'info'> = {
    valuation: 'danger',
    growth: 'success',
    quality: '',
    momentum: 'warning',
    volatility: 'info',
    liquidity: '',
    technical: 'warning',
    theme: 'success',
  }
  return map[category] || 'info'
}

const loadCategories = async () => {
  try {
    categories.value = await indicatorApi.getCategories()
  } catch (e) {
    console.error('Load categories failed:', e)
  }
}

const loadIndicators = async () => {
  loading.value = true
  try {
    indicators.value = await indicatorApi.listIndicators(activeCategory.value || undefined)
  } catch (e) {
    console.error('Load indicators failed:', e)
    indicators.value = []
  } finally {
    loading.value = false
  }
}

const showDetail = (ind: IndicatorInfo) => {
  detailData.value = ind
  detailVisible.value = true
}

onMounted(async () => {
  await loadCategories()
  await loadIndicators()
})
</script>

<style scoped>
.indicator-overview {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.category-filter {
  flex-shrink: 0;
}

.category-filter :deep(.el-radio-group) {
  flex-wrap: wrap;
}

.indicator-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
  gap: 12px;
  flex: 1;
  align-content: start;
}

.indicator-card {
  cursor: pointer;
  transition: transform 0.15s;
}

.indicator-card:hover {
  transform: translateY(-2px);
}

.card-title {
  font-size: 16px;
  font-weight: 600;
  margin-bottom: 8px;
}

.card-meta {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
}

.card-desc {
  font-size: 12px;
  color: #909399;
  margin-bottom: 8px;
  line-height: 1.5;
}

.card-tags {
  display: flex;
  flex-wrap: wrap;
}
</style>
