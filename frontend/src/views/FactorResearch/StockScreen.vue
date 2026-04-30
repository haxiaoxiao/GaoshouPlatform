<template>
  <div class="stock-screen">
    <el-card shadow="never" class="filter-card">
      <template #header>
        <div class="card-header">
          <span>筛选条件</span>
          <el-button type="primary" link @click="addFilter">
            <el-icon><Plus /></el-icon>
            添加条件
          </el-button>
        </div>
      </template>

      <div class="filter-list">
        <div v-for="(filter, index) in filters" :key="index" class="filter-row">
          <el-select v-model="filter.indicator_name" placeholder="选择指标" filterable style="width: 200px">
            <el-option-group v-for="cat in categories" :key="cat.key" :label="cat.label">
              <el-option v-for="ind in getIndicatorsByCategory(cat.key)" :key="ind.name" :label="ind.display_name + (ind.unit ? ' (' + ind.unit + ')' : '')" :value="ind.name" />
            </el-option-group>
          </el-select>
          <el-select v-model="filter.op" style="width: 80px; margin-left: 8px">
            <el-option label=">" value="gt" />
            <el-option label=">=" value="gte" />
            <el-option label="<" value="lt" />
            <el-option label="<=" value="lte" />
            <el-option label="=" value="eq" />
            <el-option label="区间" value="between" />
          </el-select>
          <template v-if="filter.op === 'between'">
            <el-input-number v-model="filter.valueLow" style="width: 100px; margin-left: 8px" :controls="false" :placeholder="getUnitPlaceholder(filter.indicator_name, 'min')" />
            <span style="margin: 0 4px">~</span>
            <el-input-number v-model="filter.valueHigh" style="width: 100px" :controls="false" :placeholder="getUnitPlaceholder(filter.indicator_name, 'max')" />
          </template>
          <template v-else>
            <el-input-number v-model="filter.value" style="width: 120px; margin-left: 8px" :controls="false" :placeholder="getUnitPlaceholder(filter.indicator_name)" />
          </template>
          <el-button type="danger" link style="margin-left: 8px" @click="removeFilter(index)">
            <el-icon><Delete /></el-icon>
          </el-button>
        </div>
        <div v-if="filters.length === 0" class="empty-filter">
          点击"添加条件"设置筛选规则
        </div>
      </div>

      <div class="filter-actions">
        <el-select v-model="sortBy" placeholder="排序指标" clearable filterable style="width: 200px; margin-right: 8px">
          <el-option-group v-for="cat in categories" :key="cat.key" :label="cat.label">
            <el-option v-for="ind in getIndicatorsByCategory(cat.key)" :key="ind.name" :label="ind.display_name + (ind.unit ? ' (' + ind.unit + ')' : '')" :value="ind.name" />
          </el-option-group>
        </el-select>
        <el-select v-model="sortOrder" style="width: 80px; margin-right: 16px" v-if="sortBy">
          <el-option label="降序" value="desc" />
          <el-option label="升序" value="asc" />
        </el-select>
        <el-button type="primary" @click="handleScreen" :loading="loading">筛选</el-button>
        <el-button @click="resetFilters">重置</el-button>
      </div>
    </el-card>

    <el-card v-if="results.length > 0" shadow="never" class="result-card">
      <template #header>
        <div class="card-header">
          <span>筛选结果 ({{ total }})</span>
          <el-button link @click="handleExport">
            <el-icon><Download /></el-icon>
            导出
          </el-button>
        </div>
      </template>
      <el-table :data="results" stripe border max-height="500" size="small">
        <el-table-column prop="symbol" label="代码" width="120" fixed />
        <el-table-column prop="name" label="名称" width="100" />
        <el-table-column v-for="ind in selectedIndicatorNames" :key="ind" :label="getIndicatorLabel(ind)" width="120" align="right">
          <template #default="{ row }">
            <span v-if="row.indicators[ind] != null">{{ formatValue(row.indicators[ind]) }}</span>
            <span v-else class="text-muted">-</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Plus, Delete, Download } from '@element-plus/icons-vue'
import { indicatorApi, type CategoryInfo, type IndicatorInfo, type ScreenFilter } from '@/api/indicator'

interface FilterRow {
  indicator_name: string
  op: string
  value: number | null
  valueLow: number | null
  valueHigh: number | null
}

const loading = ref(false)
const categories = ref<CategoryInfo[]>([])
const allIndicators = ref<IndicatorInfo[]>([])
const filters = ref<FilterRow[]>([])
const sortBy = ref('')
const sortOrder = ref<'asc' | 'desc'>('desc')
const results = ref<Array<{ symbol: string; name: string | null; indicators: Record<string, number | null> }>>([])
const total = ref(0)

const nameToInfo = computed(() => {
  const map: Record<string, IndicatorInfo> = {}
  allIndicators.value.forEach(i => { map[i.name] = i })
  return map
})

const selectedIndicatorNames = computed(() => {
  const names = new Set(filters.value.map(f => f.indicator_name))
  if (sortBy.value) names.add(sortBy.value)
  return Array.from(names)
})

const getIndicatorsByCategory = (category: string) => allIndicators.value.filter(i => i.category === category)
const getIndicatorLabel = (name: string) => nameToInfo.value[name]?.display_name || name

const getUnitPlaceholder = (name: string, type: 'min' | 'max' | 'value' = 'value') => {
  const unit = nameToInfo.value[name]?.unit
  if (!unit) return type === 'min' ? '最小值' : type === 'max' ? '最大值' : '数值'
  return unit === '%' ? (type === 'min' ? '如: 5' : type === 'max' ? '如: 20' : '如: 10') :
         unit === 'x' ? (type === 'min' ? '如: 5' : type === 'max' ? '如: 15' : '如: 10') :
         type === 'min' ? `最小值(${unit})` : type === 'max' ? `最大值(${unit})` : `数值(${unit})`
}
const formatValue = (val: number | null) => {
  if (val == null) return '-'
  if (Math.abs(val) >= 1000) return val.toFixed(0)
  if (Math.abs(val) >= 1) return val.toFixed(2)
  return val.toFixed(4)
}

const addFilter = () => {
  filters.value.push({ indicator_name: '', op: 'gt', value: null, valueLow: null, valueHigh: null })
}
const removeFilter = (index: number) => { filters.value.splice(index, 1) }
const resetFilters = () => { filters.value = []; sortBy.value = ''; results.value = []; total.value = 0 }

const handleScreen = async () => {
  const validFilters = filters.value.filter(f => f.indicator_name)
  if (validFilters.length === 0) { ElMessage.warning('请至少添加一个筛选条件'); return }
  const screenFilters: ScreenFilter[] = validFilters.map(f => {
    if (f.op === 'between') return { indicator_name: f.indicator_name, op: 'between', value: [f.valueLow || 0, f.valueHigh || 0] }
    return { indicator_name: f.indicator_name, op: f.op, value: f.value || 0 }
  })
  loading.value = true
  try {
    const response = await indicatorApi.screenStocks({ filters: screenFilters, sort_by: sortBy.value || undefined, sort_order: sortOrder.value, limit: 100 })
    results.value = response.items
    total.value = response.total
  } catch (e) { ElMessage.error('筛选失败'); results.value = []; total.value = 0 } finally { loading.value = false }
}

const handleExport = () => {
  if (!results.value.length) return
  const headers = ['代码', '名称', ...selectedIndicatorNames.value.map(n => getIndicatorLabel(n))]
  const rows = results.value.map(row => [row.symbol, row.name || '', ...selectedIndicatorNames.value.map(n => row.indicators[n] != null ? String(row.indicators[n]) : '')])
  const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n')
  const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = `screen_result_${new Date().toISOString().slice(0, 10)}.csv`
  link.click()
  URL.revokeObjectURL(url)
}

onMounted(async () => {
  try {
    const [cats, inds] = await Promise.all([indicatorApi.getCategories(), indicatorApi.listIndicators()])
    categories.value = cats
    allIndicators.value = inds
  } catch (e) { console.error('Load data failed:', e) }
})
</script>

<style scoped>
.stock-screen { display: flex; flex-direction: column; gap: 16px; }
.filter-card, .result-card { flex-shrink: 0; }
.card-header { display: flex; justify-content: space-between; align-items: center; }
.filter-list { display: flex; flex-direction: column; gap: 8px; margin-bottom: 12px; }
.filter-row { display: flex; align-items: center; }
.empty-filter { color: #909399; font-size: 13px; padding: 12px 0; }
.filter-actions { display: flex; align-items: center; padding-top: 8px; border-top: 1px solid #ebeef5; }
.text-muted { color: #909399; }
</style>
