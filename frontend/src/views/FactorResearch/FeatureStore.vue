<template>
  <div class="feature-store">
    <section class="toolbar">
      <div class="store-title">
        <strong>Feature Store / 数据仓库</strong>
        <span>管理特征定义、覆盖率、预览和预计算；因子收益表现请回到因子看板。</span>
      </div>
      <el-form :inline="true" :model="form" label-width="76px" class="control-form">
        <el-form-item label="Group">
          <el-select v-model="form.groupName" filterable class="feature-select">
            <el-option
              v-for="group in groups"
              :key="group.name"
              :label="group.display_name"
              :value="group.name"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="Feature">
          <el-select v-model="form.featureName" filterable class="feature-select">
            <el-option
              v-for="item in definitions"
              :key="item.name"
              :label="item.display_name"
              :value="item.name"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="Index">
          <el-input v-model="form.indexSymbol" class="short-input" />
        </el-form-item>
        <el-form-item label="Dates">
          <el-date-picker
            v-model="form.dateRange"
            type="daterange"
            value-format="YYYY-MM-DD"
            range-separator="to"
            start-placeholder="Start"
            end-placeholder="End"
            class="date-range"
          />
        </el-form-item>
        <el-form-item label="Time">
          <el-input v-model="form.asOfTime" class="time-input" />
        </el-form-item>
        <el-form-item label="Window">
          <el-input-number v-model="form.window" :min="2" :max="500" controls-position="right" />
        </el-form-item>
        <el-form-item label="Threshold">
          <el-input-number
            v-model="form.threshold"
            :min="0"
            :max="2"
            :step="0.05"
            controls-position="right"
          />
        </el-form-item>
        <el-form-item>
          <el-button :icon="Search" @click="loadCoverage" :loading="coverageLoading">
            Coverage
          </el-button>
          <el-button type="primary" :icon="Refresh" @click="runPrecompute" :loading="precomputeLoading">
            Precompute
          </el-button>
          <el-button :icon="Refresh" @click="runGroupPrecompute" :loading="groupPrecomputeLoading">
            Precompute Group
          </el-button>
          <el-button :icon="View" @click="loadPreview" :loading="previewLoading">
            Preview
          </el-button>
        </el-form-item>
      </el-form>
    </section>

    <section class="summary" v-if="coverage">
      <div>
        <span class="label">Rows</span>
        <strong>{{ coverage.total_rows.toLocaleString() }}</strong>
      </div>
      <div>
        <span class="label">Symbols</span>
        <strong>{{ coverage.symbol_count.toLocaleString() }}</strong>
        <small v-if="coverage.requested_symbol_count">/ {{ coverage.requested_symbol_count }}</small>
      </div>
      <div>
        <span class="label">Dates</span>
        <strong>{{ coverage.date_count.toLocaleString() }}</strong>
      </div>
      <div>
        <span class="label">Range</span>
        <strong>{{ coverage.min_date || '-' }} - {{ coverage.max_date || '-' }}</strong>
      </div>
    </section>

    <el-alert
      v-if="lastResult"
      type="success"
      :closable="false"
      class="result-alert"
      :title="`Precomputed ${lastResult.rows_written.toLocaleString()} rows for ${lastResult.symbols.toLocaleString()} symbols`"
    />

    <section class="group-panel" v-if="selectedGroup">
      <div>
        <strong>{{ selectedGroup.display_name }}</strong>
        <span>{{ selectedGroup.description }}</span>
      </div>
      <div class="group-tags">
        <el-tag v-for="name in selectedGroup.feature_names" :key="name" size="small">
          {{ name }}
        </el-tag>
      </div>
    </section>

    <el-table v-if="preview" :data="preview.items" stripe class="preview-table" max-height="240">
      <el-table-column prop="symbol" label="Symbol" width="140" />
      <el-table-column prop="value" label="Value" align="right">
        <template #default="{ row }">{{ formatValue(row.value) }}</template>
      </el-table-column>
    </el-table>

    <el-table :data="definitions" v-loading="definitionsLoading" stripe height="calc(100vh - 360px)">
      <el-table-column prop="display_name" label="Name" min-width="190" />
      <el-table-column prop="name" label="Key" min-width="180" />
      <el-table-column prop="feature_type" label="Type" width="110" />
      <el-table-column prop="category" label="Category" width="120" />
      <el-table-column prop="frequency" label="Frequency" width="120" />
      <el-table-column prop="as_of_time" label="Timer" width="100" />
      <el-table-column label="PIT" width="80" align="center">
        <template #default="{ row }">
          <el-tag :type="row.point_in_time_safe ? 'success' : 'warning'" size="small">
            {{ row.point_in_time_safe ? 'Yes' : 'Check' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="Dependencies" min-width="220">
        <template #default="{ row }">
          <el-tag
            v-for="dep in row.dependencies"
            :key="dep"
            size="small"
            class="dep-tag"
          >
            {{ dep }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="description" label="Description" min-width="320" show-overflow-tooltip />
    </el-table>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh, Search, View } from '@element-plus/icons-vue'
import {
  featureApi,
  type FeatureCoverage,
  type FeatureDefinition,
  type FeatureGroup,
  type FeaturePreview,
  type FeaturePrecomputeResult,
} from '@/api/features'

const definitions = ref<FeatureDefinition[]>([])
const groups = ref<FeatureGroup[]>([])
const coverage = ref<FeatureCoverage | null>(null)
const preview = ref<FeaturePreview | null>(null)
const lastResult = ref<FeaturePrecomputeResult | null>(null)
const definitionsLoading = ref(false)
const coverageLoading = ref(false)
const precomputeLoading = ref(false)
const groupPrecomputeLoading = ref(false)
const previewLoading = ref(false)

const form = reactive({
  groupName: 'small_cap_v4_core',
  featureName: 'high_volume_signal',
  indexSymbol: '399101.SZ',
  dateRange: ['2020-01-01', '2020-12-31'] as [string, string],
  asOfTime: '14:30',
  window: 120,
  threshold: 0.9,
  dailyVolumeToShareMultiplier: 100,
})

const selectedDefinition = computed(() => definitions.value.find(item => item.name === form.featureName))
const selectedGroup = computed(() => groups.value.find(item => item.name === form.groupName))

const syncDefaultsFromDefinition = () => {
  const def = selectedDefinition.value
  if (!def) return
  if (def.as_of_time) form.asOfTime = def.as_of_time
  const schema = def.params_schema as Record<string, { default?: unknown }>
  if (schema.window?.default) form.window = Number(schema.window.default)
  if (schema.threshold?.default) form.threshold = Number(schema.threshold.default)
  if (schema.daily_volume_to_share_multiplier?.default) {
    form.dailyVolumeToShareMultiplier = Number(schema.daily_volume_to_share_multiplier.default)
  }
}

const loadDefinitions = async () => {
  definitionsLoading.value = true
  try {
    definitions.value = await featureApi.definitions()
    groups.value = await featureApi.groups()
    syncDefaultsFromDefinition()
  } finally {
    definitionsLoading.value = false
  }
}

const requestParams = () => ({
  feature_name: form.featureName,
  start_date: form.dateRange[0],
  end_date: form.dateRange[1],
  index_symbol: form.indexSymbol || undefined,
  as_of_time: form.asOfTime,
  window: form.window,
  threshold: form.threshold,
  daily_volume_to_share_multiplier: form.dailyVolumeToShareMultiplier,
})

const loadCoverage = async () => {
  coverageLoading.value = true
  try {
    coverage.value = await featureApi.coverage(requestParams())
  } finally {
    coverageLoading.value = false
  }
}

const runPrecompute = async () => {
  precomputeLoading.value = true
  try {
    lastResult.value = await featureApi.precompute({
      feature_names: [form.featureName],
      start_date: form.dateRange[0],
      end_date: form.dateRange[1],
      index_symbol: form.indexSymbol || undefined,
      params: {
        time: form.asOfTime,
        window: form.window,
        threshold: form.threshold,
        daily_volume_to_share_multiplier: form.dailyVolumeToShareMultiplier,
      },
    })
    ElMessage.success('Feature precompute completed')
    await loadCoverage()
  } finally {
    precomputeLoading.value = false
  }
}

const runGroupPrecompute = async () => {
  groupPrecomputeLoading.value = true
  try {
    lastResult.value = await featureApi.precomputeGroup({
      group_name: form.groupName,
      start_date: form.dateRange[0],
      end_date: form.dateRange[1],
      index_symbol: form.indexSymbol || undefined,
      params: {
        time: form.asOfTime,
        include_high_volume: true,
      },
    })
    ElMessage.success('Feature group precompute completed')
    await loadCoverage()
  } finally {
    groupPrecomputeLoading.value = false
  }
}

const loadPreview = async () => {
  previewLoading.value = true
  try {
    preview.value = await featureApi.preview({
      feature_name: form.featureName,
      trade_date: form.dateRange[1],
      index_symbol: form.indexSymbol || undefined,
      as_of_time: form.asOfTime,
      window: form.window,
      threshold: form.threshold,
      limit: 80,
    })
  } finally {
    previewLoading.value = false
  }
}

const formatValue = (value: number) => {
  if (Math.abs(value) >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  return Number(value).toFixed(6).replace(/\.?0+$/, '')
}

watch(() => form.featureName, syncDefaultsFromDefinition)

onMounted(loadDefinitions)
</script>

<style scoped>
.feature-store {
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-height: 0;
}

.toolbar {
  padding: 12px;
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;
  background: var(--el-bg-color);
}

.store-title {
  display: flex;
  flex-direction: column;
  gap: 3px;
  margin-bottom: 10px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.store-title strong {
  font-size: 14px;
  color: var(--el-text-color-primary);
}

.control-form {
  display: flex;
  flex-wrap: wrap;
  gap: 0 8px;
}

.feature-select {
  width: 240px;
}

.short-input {
  width: 130px;
}

.date-range {
  width: 260px;
}

.time-input {
  width: 86px;
}

.summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(140px, 1fr));
  gap: 10px;
}

.summary > div {
  padding: 10px 12px;
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;
  background: var(--el-bg-color);
}

.summary .label {
  display: block;
  margin-bottom: 4px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.summary strong {
  font-size: 16px;
  font-weight: 600;
}

.summary small {
  margin-left: 4px;
  color: var(--el-text-color-secondary);
}

.result-alert {
  margin: 0;
}

.group-panel {
  display: grid;
  grid-template-columns: minmax(220px, 340px) 1fr;
  gap: 12px;
  padding: 10px 12px;
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;
  background: var(--el-bg-color);
}

.group-panel strong {
  display: block;
  margin-bottom: 4px;
}

.group-panel span {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.group-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.preview-table {
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;
}

.dep-tag {
  margin-right: 4px;
  margin-bottom: 4px;
}
</style>
