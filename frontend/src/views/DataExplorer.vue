<template>
  <div class="data-explorer">
    <aside class="explorer-sidebar">
      <section class="side-section">
        <div class="side-title">
          <span>TABLES</span>
          <strong>数据表</strong>
        </div>
        <el-input v-model="tableSearch" placeholder="搜索表名" clearable size="small" />
        <div v-if="tablesLoading" class="side-state">正在加载数据表...</div>
        <div v-else-if="!filteredTables.length" class="side-state">暂无数据表</div>
        <div v-else class="table-list">
          <button
            v-for="table in filteredTables"
            :key="table.name"
            class="table-item"
            :class="{ 'table-item--selected': selectedTable === table.name }"
            @click="selectTable(table.name)"
          >
            <span>{{ table.name }}</span>
            <small>{{ formatNumber(table.row_count) }} 行</small>
          </button>
        </div>
      </section>

      <section class="side-section side-section--fields">
        <div class="side-title">
          <span>FIELDS</span>
          <strong>字段</strong>
        </div>
        <el-input v-model="fieldSearch" placeholder="搜索字段" clearable size="small" />
        <div class="field-list">
          <button
            v-for="column in filteredSchema"
            :key="column.name"
            class="field-chip"
            :class="{
              'field-chip--hidden': hiddenColumns.has(column.name),
              'field-chip--key': keyColumns.has(column.name),
            }"
            @click="toggleColumnVisibility(column.name)"
          >
            <span>{{ hiddenColumns.has(column.name) ? '□' : '☑' }} {{ column.name }}</span>
            <small>{{ column.type }}</small>
          </button>
        </div>
      </section>
    </aside>

    <main class="explorer-workspace">
      <header class="workspace-header">
        <div>
          <span class="panel-kicker">DATA EXPLORER</span>
          <h2>{{ selectedTable || '选择数据表' }}</h2>
          <p v-if="selectedTableInfo">
            {{ formatNumber(selectedTableInfo.row_count) }} 行
            <template v-if="selectedTableInfo.min_date || selectedTableInfo.max_date">
              · {{ selectedTableInfo.min_date || '-' }} 至 {{ selectedTableInfo.max_date || '-' }}
            </template>
          </p>
          <p v-else>选择左侧表后，通过快捷筛选或条件构造器查询。</p>
        </div>
        <div class="header-actions">
          <el-button @click="showSql = !showSql" :disabled="!selectedTable">
            {{ showSql ? '收起 SQL' : '高级 SQL' }}
          </el-button>
          <el-button @click="copyQuery" :disabled="!selectedTable">复制条件</el-button>
          <el-button @click="exportCsv" :disabled="!result.rows.length">导出 CSV</el-button>
          <el-button type="primary" @click="loadData" :disabled="!selectedTable" :loading="loading">
            查询
          </el-button>
        </div>
      </header>

      <section v-if="selectedTable" class="query-panel">
        <div class="panel-block">
          <div class="block-title">
            <strong>快捷筛选</strong>
            <span>只显示当前表存在的高频字段</span>
          </div>
          <div class="quick-grid">
            <el-form-item v-if="hasColumn('symbol')" label="股票代码">
              <el-input v-model="quickSearch.symbol" clearable placeholder="600519.SH, 000001.SZ" @keyup.enter="applySearch" />
            </el-form-item>
            <el-form-item v-if="dateColumn" label="日期范围">
              <div class="date-pair">
                <el-date-picker v-model="quickSearch.start_date" type="date" value-format="YYYY-MM-DD" placeholder="开始日期" />
                <span>至</span>
                <el-date-picker v-model="quickSearch.end_date" type="date" value-format="YYYY-MM-DD" placeholder="结束日期" />
              </div>
            </el-form-item>
            <el-form-item v-if="hasColumn('factor_name')" label="因子名">
              <el-select
                v-model="quickSearch.factor_name"
                filterable
                remote
                clearable
                placeholder="搜索因子名"
                :remote-method="(value: string) => loadSuggestions('factor_name', value)"
              >
                <el-option v-for="item in suggestions.factor_name || []" :key="item" :label="item" :value="item" />
              </el-select>
            </el-form-item>
            <el-form-item v-if="hasColumn('indicator_name')" label="指标名">
              <el-select
                v-model="quickSearch.indicator_name"
                filterable
                remote
                clearable
                placeholder="搜索指标名"
                :remote-method="(value: string) => loadSuggestions('indicator_name', value)"
              >
                <el-option v-for="item in suggestions.indicator_name || []" :key="item" :label="item" :value="item" />
              </el-select>
            </el-form-item>
          </div>
        </div>

        <div class="panel-block">
          <div class="block-title">
            <strong>条件构造器</strong>
            <span>组合字段、操作符和值，不用手写 WHERE</span>
          </div>
          <div class="filter-list">
            <div
              v-for="(filter, index) in filters"
              :key="filter.id"
              class="filter-row"
              :class="{ 'filter-row--invalid': !filter.column }"
            >
              <el-select v-model="filter.column" filterable placeholder="字段" @change="onFilterColumnChange(filter)">
                <el-option v-for="column in schema" :key="column.name" :label="column.name" :value="column.name" />
              </el-select>
              <el-select v-model="filter.op" placeholder="操作符">
                <el-option v-for="op in operatorOptions" :key="op.value" :label="op.label" :value="op.value" />
              </el-select>
              <template v-if="filter.op === 'between'">
                <el-input v-model="filter.value" placeholder="起始值" />
                <el-input v-model="filter.value_to" placeholder="结束值" />
              </template>
              <template v-else-if="filter.op === 'in'">
                <el-input v-model="filter.value" placeholder="逗号分隔多个值" />
              </template>
              <template v-else-if="filter.op !== 'is null' && filter.op !== 'not null'">
                <el-select
                  v-if="suggestibleColumns.has(filter.column)"
                  v-model="filter.value"
                  filterable
                  remote
                  allow-create
                  clearable
                  placeholder="搜索或输入值"
                  :remote-method="(value: string) => loadSuggestions(filter.column, value)"
                >
                  <el-option v-for="item in suggestions[filter.column] || []" :key="item" :label="item" :value="item" />
                </el-select>
                <el-input v-else v-model="filter.value" placeholder="值" />
              </template>
              <el-button text type="danger" @click="removeFilter(index)">删除</el-button>
            </div>
          </div>
          <div class="filter-actions">
            <el-button @click="addFilter">添加条件</el-button>
            <el-button @click="resetFilters">重置</el-button>
            <el-button type="primary" @click="applySearch">应用查询</el-button>
          </div>
        </div>

        <el-collapse-transition>
          <div v-if="showSql" class="panel-block sql-panel">
            <div class="block-title">
              <strong>高级 SQL / WHERE</strong>
              <span>保留给临时排查；主流程建议使用条件构造器</span>
            </div>
            <el-input
              v-model="whereClause"
              type="textarea"
              :rows="2"
              placeholder="WHERE 条件，例如：symbol='600519.SH' AND trade_date >= '2026-05-01'"
            />
            <el-button @click="loadLegacyPreview" :disabled="!selectedTable">执行 WHERE</el-button>
          </div>
        </el-collapse-transition>
      </section>

      <section class="result-panel">
        <div class="result-header">
          <div>
            <strong>查询结果</strong>
            <span v-if="result.total">
              {{ formatNumber(result.total) }} 行 · 第 {{ result.page }}/{{ result.total_pages || 1 }} 页
            </span>
            <span v-else>暂无结果</span>
          </div>
          <code v-if="result.generated_sql">{{ result.generated_sql }}</code>
        </div>

        <div v-if="loading" class="state-box">加载中...</div>
        <div v-else-if="error" class="state-box state-box--error">{{ error }}</div>
        <div v-else-if="!selectedTable" class="state-box">从左侧选择一个数据表开始查询。</div>
        <div v-else-if="!result.rows.length" class="state-box">没有匹配数据。</div>
        <div v-else class="table-virtual">
          <el-auto-resizer>
            <template #default="{ height, width }">
              <el-table-v2
                :columns="tableColumns"
                :data="tableRows"
                :width="width"
                :height="height"
                :row-height="34"
                :header-height="36"
                :sort-by="tableSortBy"
                :on-column-sort="handleVirtualSort"
                row-key="__rowKey"
                fixed
                scrollbar-always-on
                class="explorer-table-v2"
              >
                <template #cell="{ column, rowData }">
                  <span
                    v-if="column.key === '__rowNumber'"
                    class="cell-text cell-text--muted cell-text--right"
                  >
                    {{ rowData.__rowNumber }}
                  </span>
                  <el-button
                    v-else-if="column.key === '__actions'"
                    text
                    size="small"
                    @click="copyText(JSON.stringify(rowData.__raw, null, 2), '行 JSON 已复制')"
                  >
                    复制行
                  </el-button>
                  <span
                    v-else
                    class="cell-text"
                    :title="formatCell(rowData[column.dataKey])"
                    @click="copyText(formatCell(rowData[column.dataKey]), '单元格已复制')"
                  >
                    {{ formatCell(rowData[column.dataKey]) }}
                  </span>
                </template>
              </el-table-v2>
            </template>
          </el-auto-resizer>
        </div>

        <div v-if="result.total_pages > 1" class="pagination">
          <el-button size="small" :disabled="result.page <= 1" @click="goPage(1)">首页</el-button>
          <el-button size="small" :disabled="result.page <= 1" @click="goPage(result.page - 1)">上一页</el-button>
          <span>{{ result.page }} / {{ result.total_pages }}</span>
          <el-button size="small" :disabled="result.page >= result.total_pages" @click="goPage(result.page + 1)">下一页</el-button>
          <el-button size="small" :disabled="result.page >= result.total_pages" @click="goPage(result.total_pages)">末页</el-button>
          <el-select v-model="pageSize" size="small" class="page-size" @change="applySearch">
            <el-option :value="50" label="50 行" />
            <el-option :value="100" label="100 行" />
            <el-option :value="200" label="200 行" />
            <el-option :value="500" label="500 行" />
          </el-select>
        </div>
      </section>
    </main>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, TableV2FixedDir, TableV2SortOrder, type Column, type SortBy } from 'element-plus'
import {
  getDistinctValues,
  getTableSchema,
  getTables,
  previewTable,
  searchTable,
  type ColumnInfo,
  type ExplorerFilter,
  type ExplorerFilterOp,
  type PreviewResult,
  type TableInfo,
} from '@/api/explorer'

type FilterRow = {
  id: number
  column: string
  op: ExplorerFilterOp
  value: string
  value_to: string
}

type ExplorerRow = Record<string, unknown> & {
  __raw: Record<string, unknown>
  __rowKey: string
  __rowNumber: number
}

const tables = ref<TableInfo[]>([])
const selectedTable = ref('')
const schema = ref<ColumnInfo[]>([])
const tableSearch = ref('')
const fieldSearch = ref('')
const whereClause = ref('')
const orderBy = ref('')
const orderDir = ref<'ASC' | 'DESC'>('ASC')
const page = ref(1)
const pageSize = ref(50)
const hiddenColumns = ref(new Set<string>())
const filters = ref<FilterRow[]>([])
const showSql = ref(false)
const tablesLoading = ref(false)
const loading = ref(false)
const error = ref('')
const suggestions = reactive<Record<string, string[]>>({})
const quickSearch = reactive<Record<string, string>>({
  symbol: '',
  start_date: '',
  end_date: '',
  factor_name: '',
  indicator_name: '',
})

const operatorOptions: { label: string; value: ExplorerFilterOp }[] = [
  { label: '等于', value: '=' },
  { label: '不等于', value: '!=' },
  { label: '包含', value: 'contains' },
  { label: '属于', value: 'in' },
  { label: '区间', value: 'between' },
  { label: '大于', value: '>' },
  { label: '大于等于', value: '>=' },
  { label: '小于', value: '<' },
  { label: '小于等于', value: '<=' },
  { label: '为空', value: 'is null' },
  { label: '非空', value: 'not null' },
]

const result = ref<PreviewResult>({
  columns: [],
  rows: [],
  total: 0,
  page: 1,
  page_size: 50,
  total_pages: 0,
})

const filteredTables = computed(() => {
  const keyword = tableSearch.value.trim().toLowerCase()
  return keyword ? tables.value.filter(table => table.name.toLowerCase().includes(keyword)) : tables.value
})

const selectedTableInfo = computed(() => tables.value.find(table => table.name === selectedTable.value))
const keyColumns = computed(() => new Set(['symbol', 'trade_date', 'datetime', 'factor_name', 'indicator_name', 'as_of_time']))
const suggestibleColumns = computed(() => new Set(['symbol', 'factor_name', 'indicator_name', 'source', 'as_of_time']))
const dateColumn = computed(() => schema.value.find(column => ['trade_date', 'datetime', 'date'].includes(column.name))?.name || '')
const visibleColumns = computed(() => result.value.columns.filter(column => !hiddenColumns.value.has(column)))
const tableRows = computed<ExplorerRow[]>(() => result.value.rows.map((row, rowIndex) => ({
  ...row,
  __raw: row,
  __rowKey: `${result.value.page}-${rowIndex}`,
  __rowNumber: (result.value.page - 1) * result.value.page_size + rowIndex + 1,
})))
const tableColumns = computed<Column<unknown>[]>(() => [
  {
    key: '__rowNumber',
    dataKey: '__rowNumber',
    title: '#',
    width: 72,
    align: 'right',
    fixed: TableV2FixedDir.LEFT,
  },
  ...visibleColumns.value.map(column => ({
    key: column,
    dataKey: column,
    title: column,
    width: getColumnWidth(column),
    minWidth: 120,
    sortable: true,
    class: 'explorer-table-v2__cell',
    headerClass: orderBy.value === column ? 'explorer-table-v2__header explorer-table-v2__header--sorted' : 'explorer-table-v2__header',
  })),
  {
    key: '__actions',
    dataKey: '__actions',
    title: '操作',
    width: 92,
    fixed: TableV2FixedDir.RIGHT,
  },
])
const tableSortBy = computed<SortBy | undefined>(() => {
  if (!orderBy.value) return undefined
  return {
    key: orderBy.value,
    order: orderDir.value === 'ASC' ? TableV2SortOrder.ASC : TableV2SortOrder.DESC,
  }
})
const filteredSchema = computed(() => {
  const keyword = fieldSearch.value.trim().toLowerCase()
  return keyword ? schema.value.filter(column => column.name.toLowerCase().includes(keyword)) : schema.value
})

function hasColumn(name: string) {
  return schema.value.some(column => column.name === name)
}

function formatNumber(value: number) {
  if (value >= 100_000_000) return `${(value / 100_000_000).toFixed(1)}亿`
  if (value >= 10_000) return `${(value / 10_000).toFixed(1)}万`
  return Number(value || 0).toLocaleString()
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined || value === '') return '-'
  if (typeof value === 'number') {
    return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(6).replace(/\.?0+$/, '')
  }
  const text = String(value)
  return text.length > 80 ? `${text.slice(0, 77)}...` : text
}

function getColumnWidth(column: string) {
  const type = schema.value.find(item => item.name === column)?.type?.toLowerCase() || ''
  if (['symbol', 'trade_date', 'datetime'].includes(column)) return column === 'datetime' ? 180 : 132
  if (type.includes('int') || type.includes('float') || type.includes('double') || type.includes('decimal')) return 132
  if (column.includes('name')) return 160
  return 180
}

async function selectTable(tableName: string) {
  selectedTable.value = tableName
  schema.value = []
  filters.value = []
  whereClause.value = ''
  orderBy.value = ''
  orderDir.value = 'ASC'
  page.value = 1
  hiddenColumns.value = new Set()
  Object.keys(quickSearch).forEach(key => { quickSearch[key] = '' })
  result.value = { columns: [], rows: [], total: 0, page: 1, page_size: pageSize.value, total_pages: 0 }
  error.value = ''
  loading.value = true
  try {
    schema.value = await getTableSchema(tableName)
    await loadData()
  } catch (err: any) {
    error.value = err?.message || '加载表结构失败'
  } finally {
    loading.value = false
  }
}

function addFilter() {
  filters.value.push({
    id: Date.now() + Math.random(),
    column: schema.value[0]?.name || '',
    op: '=',
    value: '',
    value_to: '',
  })
}

function removeFilter(index: number) {
  filters.value.splice(index, 1)
}

function onFilterColumnChange(filter: FilterRow) {
  filter.value = ''
  filter.value_to = ''
  if (suggestibleColumns.value.has(filter.column)) {
    loadSuggestions(filter.column, '')
  }
}

function buildFilters(): ExplorerFilter[] {
  return filters.value
    .filter(filter => filter.column)
    .map(filter => ({
      column: filter.column,
      op: filter.op,
      value: filter.value,
      value_to: filter.value_to,
      values: filter.op === 'in'
        ? filter.value.split(',').map(item => item.trim()).filter(Boolean)
        : undefined,
    }))
}

async function loadData() {
  if (!selectedTable.value) return
  loading.value = true
  error.value = ''
  try {
    const response = await searchTable(selectedTable.value, {
      page: page.value,
      page_size: pageSize.value,
      order_by: orderBy.value || undefined,
      order_dir: orderDir.value,
      filters: buildFilters(),
      quick_search: { ...quickSearch },
    })
    result.value = response
  } catch (err: any) {
    error.value = err?.response?.data?.detail || err?.message || '查询失败'
  } finally {
    loading.value = false
  }
}

async function loadLegacyPreview() {
  if (!selectedTable.value) return
  loading.value = true
  error.value = ''
  try {
    result.value = await previewTable(selectedTable.value, {
      page: page.value,
      page_size: pageSize.value,
      order_by: orderBy.value || undefined,
      order_dir: orderDir.value,
      where: whereClause.value || undefined,
    })
  } catch (err: any) {
    error.value = err?.message || '查询失败'
  } finally {
    loading.value = false
  }
}

function applySearch() {
  page.value = 1
  loadData()
}

function resetFilters() {
  filters.value = []
  whereClause.value = ''
  orderBy.value = ''
  orderDir.value = 'ASC'
  page.value = 1
  Object.keys(quickSearch).forEach(key => { quickSearch[key] = '' })
  loadData()
}

function toggleSort(column: string) {
  if (orderBy.value === column) {
    if (orderDir.value === 'ASC') {
      orderDir.value = 'DESC'
    } else {
      orderBy.value = ''
      orderDir.value = 'ASC'
    }
  } else {
    orderBy.value = column
    orderDir.value = 'ASC'
  }
  loadData()
}

function handleVirtualSort({ key }: { key: string | number | symbol }) {
  const column = String(key)
  if (visibleColumns.value.includes(column)) toggleSort(column)
}

function toggleColumnVisibility(column: string) {
  const next = new Set(hiddenColumns.value)
  if (next.has(column)) next.delete(column)
  else next.add(column)
  hiddenColumns.value = next
}

function goPage(nextPage: number) {
  page.value = nextPage
  loadData()
}

async function loadSuggestions(column: string, keyword: string) {
  if (!selectedTable.value || !column) return
  try {
    const values = await getDistinctValues(selectedTable.value, column, 100, keyword)
    suggestions[column] = values.map(value => String(value))
  } catch {
    suggestions[column] = []
  }
}

async function copyText(text: string, message = '已复制') {
  await navigator.clipboard.writeText(text)
  ElMessage.success(message)
}

function copyQuery() {
  const payload = {
    table: selectedTable.value,
    quick_search: quickSearch,
    filters: buildFilters(),
    order_by: orderBy.value,
    order_dir: orderDir.value,
  }
  copyText(JSON.stringify(payload, null, 2), '查询条件已复制')
}

function exportCsv() {
  if (!result.value.rows.length) return
  const rows = result.value.rows
  const columns = visibleColumns.value
  const escape = (value: unknown) => `"${String(value ?? '').replace(/"/g, '""')}"`
  const csv = [
    columns.join(','),
    ...rows.map(row => columns.map(column => escape(row[column])).join(',')),
  ].join('\n')
  const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' })
  const link = document.createElement('a')
  link.href = URL.createObjectURL(blob)
  link.download = `explorer_${selectedTable.value}_${new Date().toISOString().slice(0, 10)}.csv`
  link.click()
  URL.revokeObjectURL(link.href)
  ElMessage.success('CSV 已导出')
}

onMounted(async () => {
  tablesLoading.value = true
  try {
    tables.value = await getTables()
    if (tables.value.length) {
      await selectTable(tables.value[0].name)
    }
  } catch (err: any) {
    error.value = err?.message || '加载数据表失败'
  } finally {
    tablesLoading.value = false
  }
})
</script>

<style scoped>
.data-explorer {
  display: grid;
  grid-template-columns: 280px minmax(0, 1fr);
  gap: 12px;
  height: 100%;
  min-height: 0;
}

.explorer-sidebar,
.explorer-workspace,
.query-panel,
.result-panel,
.panel-block {
  min-height: 0;
}

.explorer-sidebar {
  display: grid;
  grid-template-rows: minmax(180px, 0.85fr) minmax(220px, 1.15fr);
  gap: 8px;
}

.side-section,
.panel-block,
.result-panel,
.workspace-header {
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-elevated);
  box-shadow: var(--shadow-sm);
}

.side-section {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 10px;
  overflow: hidden;
}

.side-title,
.block-title,
.workspace-header,
.result-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
}

.side-title,
.block-title {
  flex-direction: column;
  gap: 2px;
}

.side-title span,
.block-title span,
.panel-kicker {
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 10px;
  letter-spacing: 0;
}

.side-title strong,
.block-title strong,
.result-header strong {
  color: var(--text-bright);
  font-size: 13px;
}

.table-list,
.field-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
  overflow: auto;
  padding-right: 2px;
}

.side-state {
  border: 1px dashed var(--border-subtle);
  border-radius: 6px;
  color: var(--text-secondary);
  font-size: 12px;
  padding: 10px;
}

.table-item,
.field-chip {
  width: 100%;
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.02);
  color: var(--text-primary);
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  gap: 8px;
  padding: 8px;
  text-align: left;
}

.table-item:hover,
.field-chip:hover {
  border-color: rgba(56, 189, 248, 0.42);
  background: rgba(56, 189, 248, 0.06);
}

.table-item--selected {
  border-color: var(--accent-primary);
  background: rgba(56, 189, 248, 0.12);
}

.table-item span,
.field-chip span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.table-item small,
.field-chip small {
  color: var(--text-secondary);
  flex-shrink: 0;
}

.field-chip--hidden {
  opacity: 0.48;
}

.field-chip--key {
  border-color: rgba(251, 191, 36, 0.28);
}

.explorer-workspace {
  display: grid;
  grid-template-rows: auto auto minmax(0, 1fr);
  gap: 8px;
  overflow: hidden;
}

.workspace-header {
  align-items: center;
  padding: 12px 14px;
}

.workspace-header h2 {
  margin: 2px 0;
  color: var(--text-bright);
  font-size: 18px;
}

.workspace-header p {
  margin: 0;
  color: var(--text-secondary);
  font-size: 12px;
}

.header-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}

.query-panel {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(360px, 1fr);
  gap: 8px;
}

.panel-block {
  padding: 12px;
}

.quick-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(180px, 1fr));
  gap: 8px 12px;
  margin-top: 10px;
}

.date-pair {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
  align-items: center;
  gap: 6px;
}

.filter-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin-top: 10px;
}

.filter-row {
  display: grid;
  grid-template-columns: minmax(120px, 1fr) 110px minmax(140px, 1fr) minmax(120px, 1fr) auto;
  gap: 6px;
  align-items: center;
  padding: 6px;
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
  background: rgba(0, 0, 0, 0.12);
}

.filter-row--invalid {
  border-color: rgba(239, 68, 68, 0.35);
}

.filter-actions,
.sql-panel {
  display: flex;
  gap: 8px;
  margin-top: 10px;
}

.sql-panel {
  grid-column: 1 / -1;
  flex-direction: column;
}

.result-panel {
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.result-header {
  padding: 10px 12px;
  border-bottom: 1px solid var(--border-subtle);
}

.result-header div {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.result-header span,
.result-header code {
  color: var(--text-secondary);
  font-size: 12px;
}

.result-header code {
  max-width: 58%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.state-box {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 180px;
  color: var(--text-secondary);
}

.state-box--error {
  color: var(--accent-danger);
}

.table-virtual {
  flex: 1;
  min-height: 240px;
  overflow: hidden;
}

.explorer-table-v2 {
  --el-table-v2-row-bg-color: rgba(11, 16, 24, 0.98);
  --el-table-v2-row-hover-bg-color: rgba(56, 189, 248, 0.08);
  --el-table-v2-header-bg-color: rgba(10, 10, 12, 0.95);
  --el-table-v2-header-text-color: var(--text-secondary);
  --el-table-v2-border-color: var(--border-subtle);
  background: #0b1018;
  color: var(--text-primary);
  font-size: 12px;
}

.explorer-table-v2 :deep(.el-table-v2__main),
.explorer-table-v2 :deep(.el-table-v2__left),
.explorer-table-v2 :deep(.el-table-v2__right),
.explorer-table-v2 :deep(.el-table-v2__header),
.explorer-table-v2 :deep(.el-table-v2__body),
.explorer-table-v2 :deep(.el-table-v2__empty) {
  background: #0b1018;
  color: var(--text-primary);
}

.explorer-table-v2 :deep(.el-table-v2__row),
.explorer-table-v2 :deep(.el-table-v2__row-cell) {
  background: rgba(11, 16, 24, 0.98);
  color: var(--text-primary);
}

.explorer-table-v2 :deep(.el-table-v2__header-cell) {
  background: rgba(10, 10, 12, 0.95);
  color: var(--text-secondary);
  font-weight: 600;
}

.explorer-table-v2 :deep(.explorer-table-v2__header--sorted),
.explorer-table-v2 :deep(.el-table-v2__header-cell:hover) {
  color: var(--accent-primary);
}

.explorer-table-v2 :deep(.el-table-v2__row-cell) {
  border-bottom: 1px solid var(--border-subtle);
}

.explorer-table-v2 :deep(.el-table-v2__row:hover .el-table-v2__row-cell) {
  background: rgba(56, 189, 248, 0.08);
}

.cell-text {
  color: var(--text-primary);
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  width: 100%;
}

.cell-text:hover {
  color: var(--text-bright);
  cursor: copy;
}

.cell-text--muted {
  color: var(--text-secondary);
}

.cell-text--right {
  text-align: right;
}

.pagination {
  align-items: center;
  border-top: 1px solid var(--border-subtle);
  display: flex;
  gap: 8px;
  justify-content: center;
  padding: 10px;
}

.page-size {
  width: 96px;
}

:deep(.el-input__wrapper),
:deep(.el-select__wrapper),
:deep(.el-textarea__inner) {
  background: rgba(8, 8, 10, 0.38);
  box-shadow: 0 0 0 1px var(--border-subtle) inset;
}

@media (max-width: 1180px) {
  .data-explorer,
  .query-panel {
    grid-template-columns: 1fr;
  }

  .explorer-sidebar {
    grid-template-columns: 1fr 1fr;
    grid-template-rows: 240px;
  }
}
</style>
