<template>
  <div class="data-explorer-container theme-pine-quant">
    <!-- Header of the page -->
    <header class="workspace-header">
      <div class="header-left">
        <span class="panel-kicker">DATA EXPLORER / 数据浏览器</span>
        <h2>{{ selectedTable || '选择数据表' }}</h2>
        <p v-if="selectedTableInfo">
          {{ selectedTableInfo.label || selectedTableInfo.name }} · {{ formatRowCount(selectedTableInfo.row_count) }}
          <template v-if="selectedTableInfo.min_date || selectedTableInfo.max_date">
            · {{ selectedTableInfo.min_date || '-' }} 至 {{ selectedTableInfo.max_date || '-' }}
          </template>
          <template v-if="selectedTableInfo.date_column">
            · 日期字段 {{ selectedTableInfo.date_column }}
          </template>
        </p>
        <p v-else>选择左侧表后，通过快捷筛选或条件构造器查询。</p>
      </div>

      <div class="header-actions">
        <!-- Layout Switcher -->
        <div class="layout-switcher" aria-label="切换数据查看布局">
          <button
            v-for="mode in layoutModes"
            :key="mode.key"
            type="button"
            :class="{ active: layoutMode === mode.key }"
            @click="layoutMode = mode.key"
          >
            {{ mode.label }}
          </button>
        </div>
        
        <el-button @click="copyQuery" :disabled="!selectedTable">复制条件</el-button>
        <el-button @click="exportCsv" :disabled="!result.rows.length">导出 CSV</el-button>
        <el-button @click="loadData(true)" :disabled="!selectedTable" :loading="loading">
          精确统计
        </el-button>
        <el-button type="primary" @click="loadData(false)" :disabled="!selectedTable" :loading="loading">
          查询
        </el-button>
      </div>
    </header>

    <!-- LAYOUT A (sql) -->
    <div v-if="layoutMode === 'A'" class="layout-sql-grid">
      <!-- Left Sidebar (220px): Schema tree (tables and columns) -->
      <aside class="schema-sidebar">
        <!-- Tables section -->
        <div class="sidebar-section">
          <div class="side-title">
            <span>TABLES</span>
            <strong>数据表</strong>
          </div>
          <el-input v-model="tableSearch" placeholder="搜索表名" clearable size="small" />
          <div v-if="tablesLoading" class="side-state">正在加载...</div>
          <div v-else-if="!filteredTables.length" class="side-state">暂无数据表</div>
          <div v-else class="table-list-mini">
            <button
              v-for="table in filteredTables"
              :key="table.name"
              class="table-item-mini"
              :class="{ 'table-item-mini--selected': selectedTable === table.name }"
              @click="selectTable(table.name)"
            >
              <span class="table-name-span">{{ table.name }}</span>
              <small class="table-label-small">{{ table.label || 'parquet' }}</small>
            </button>
          </div>
        </div>

        <!-- Columns section -->
        <div class="sidebar-section columns-section">
          <div class="side-title">
            <span>COLUMNS</span>
            <strong>数据字段</strong>
          </div>
          <el-input v-model="fieldSearch" placeholder="搜索字段" clearable size="small" />
          <div v-if="!selectedTable" class="side-state">选择表查看字段</div>
          <div v-else-if="!filteredSchema.length" class="side-state">暂无字段</div>
          <div class="column-list-mini" v-else>
            <div
              v-for="column in filteredSchema"
              :key="column.name"
              class="column-item-mini"
              :class="{
                'column-item-mini--key': keyColumns.has(column.name),
                'column-item-mini--hidden': hiddenColumns.has(column.name)
              }"
            >
              <button
                class="col-visibility-btn"
                :title="hiddenColumns.has(column.name) ? '显示该列' : '隐藏该列'"
                @click.stop="toggleColumnVisibility(column.name)"
              >
                {{ hiddenColumns.has(column.name) ? '□' : '☑' }}
              </button>
              <span class="col-name" @click="insertColumnToSql(column.name)">{{ column.name }}</span>
              <span class="col-type" @click="insertColumnToSql(column.name)">{{ column.type }}</span>
            </div>
          </div>
        </div>
      </aside>

      <!-- Right side -->
      <main class="sql-workspace">
        <!-- Top: SQL input textarea with Run Query button -->
        <div class="sql-input-panel">
          <div class="panel-header-inline">
            <div class="block-title-sql">
              <div class="block-title-sql__main">
                <strong>WHERE 过滤子句 (SQL IDE)</strong>
                <el-button text size="small" class="detail-toggle-btn" @click="showSql = !showSql">
                  {{ showSql ? '收起说明' : '详细说明' }}
                </el-button>
              </div>
              <span v-if="showSql" class="sql-help-text">调试模式：双击或单击左侧字段可自动插入。支持标准 SQL WHERE 过滤语法。</span>
              <span v-else class="sql-help-text">支持输入 SQL WHERE 语法过滤行。</span>
            </div>
            <el-button type="primary" :disabled="!selectedTable" @click="loadLegacyPreview" :loading="loading">
              运行查询 (Run Query)
            </el-button>
          </div>
          <el-input
            v-model="whereClause"
            type="textarea"
            :rows="3"
            placeholder="例如: symbol = '600519.SH' AND trade_date >= '2026-05-01'"
          />
        </div>

        <!-- Bottom: Data Grid result table -->
        <div class="result-table-panel">
          <div class="result-header-inline">
            <div class="result-title-info">
              <strong>查询结果</strong>
              <span v-if="result.total">
                {{ result.total_estimated ? '约 ' : '' }}{{ formatNumber(result.total) }} 行 · 第 {{ result.page }}/{{ result.total_pages || 1 }} 页
              </span>
              <span v-else>暂无结果</span>
            </div>
            <code class="sql-preview-code" v-if="result.generated_sql">{{ result.generated_sql }}</code>
          </div>
          
          <div class="virtual-grid-container">
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
                    :row-height="30"
                    :header-height="32"
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
                        复制
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
        </div>
      </main>
    </div>

    <!-- LAYOUT B (filter) -->
    <div v-else-if="layoutMode === 'B'" class="layout-filter-grid">
      <!-- Top bar no-code visual query builder -->
      <div class="filter-builder-panel">
        <div class="builder-row-top">
          <div class="builder-select-table">
            <label>目标表：</label>
            <el-select v-model="selectedTable" filterable placeholder="选择数据表" @change="selectTable" class="table-selector-b">
              <el-option
                v-for="table in tables"
                :key="table.name"
                :label="table.name + (table.label ? ' (' + table.label + ')' : '')"
                :value="table.name"
              />
            </el-select>
          </div>
          <div class="builder-actions">
            <el-button type="success" :disabled="!selectedTable" @click="addFilter">+ 添加过滤条件</el-button>
            <el-button @click="resetFilters">重置</el-button>
            <el-button type="primary" :disabled="!selectedTable" @click="applySearch" :loading="loading">
              执行查询 (Apply Search)
            </el-button>
          </div>
        </div>

        <!-- Quick filter fields -->
        <div v-if="selectedTable" class="quick-filter-grid">
          <div v-if="hasColumn('symbol')" class="quick-filter-item">
            <span class="filter-label">股票代码</span>
            <el-input v-model="quickSearch.symbol" clearable placeholder="600519.SH, 000001.SZ" @keyup.enter="applySearch" size="small" />
          </div>
          <div v-if="dateColumn" class="quick-filter-item date-range-item">
            <span class="filter-label">日期范围（{{ dateColumn }}）</span>
            <div class="date-pair">
              <el-date-picker
                v-model="quickSearch.start_date"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="开始日期"
                size="small"
              />
              <span>至</span>
              <el-date-picker
                v-model="quickSearch.end_date"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="结束日期"
                size="small"
              />
            </div>
          </div>
          <div v-if="hasColumn('factor_name')" class="quick-filter-item">
            <span class="filter-label">因子名</span>
            <el-select
              v-model="quickSearch.factor_name"
              filterable
              remote
              clearable
              placeholder="搜索因子名"
              :remote-method="(value: string) => loadSuggestions('factor_name', value)"
              size="small"
            >
              <el-option v-for="item in suggestions.factor_name || []" :key="item" :label="item" :value="item" />
            </el-select>
          </div>
          <div v-if="hasColumn('indicator_name')" class="quick-filter-item">
            <span class="filter-label">指标名</span>
            <el-select
              v-model="quickSearch.indicator_name"
              filterable
              remote
              clearable
              placeholder="搜索指标名"
              :remote-method="(value: string) => loadSuggestions('indicator_name', value)"
              size="small"
            >
              <el-option v-for="item in suggestions.indicator_name || []" :key="item" :label="item" :value="item" />
            </el-select>
          </div>
        </div>

        <!-- Dynamic WHERE filters list -->
        <div v-if="selectedTable && filters.length" class="dynamic-filters-list">
          <div
            v-for="(filter, index) in filters"
            :key="filter.id"
            class="filter-row"
            :class="{ 'filter-row--invalid': !filter.column }"
          >
            <el-select v-model="filter.column" filterable placeholder="字段" @change="onFilterColumnChange(filter)" size="small">
              <el-option v-for="column in schema" :key="column.name" :label="column.name" :value="column.name" />
            </el-select>
            <el-select v-model="filter.op" placeholder="操作符" size="small">
              <el-option v-for="op in operatorOptions" :key="op.value" :label="op.label" :value="op.value" />
            </el-select>
            <template v-if="filter.op === 'between'">
              <el-input v-model="filter.value" placeholder="起始值" size="small" />
              <el-input v-model="filter.value_to" placeholder="结束值" size="small" />
            </template>
            <template v-else-if="filter.op === 'in'">
              <el-input v-model="filter.value" placeholder="逗号分隔多个值" size="small" />
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
                size="small"
              >
                <el-option v-for="item in suggestions[filter.column] || []" :key="item" :label="item" :value="item" />
              </el-select>
              <el-input v-else v-model="filter.value" placeholder="值" size="small" />
            </template>
            <el-button text type="danger" size="small" @click="removeFilter(index)">删除</el-button>
          </div>
        </div>
      </div>

      <!-- Bottom is the full-width result data grid -->
      <div class="result-table-panel full-width">
        <div class="result-header-inline">
          <div class="result-title-info">
            <strong>筛选结果</strong>
            <span v-if="result.total">
              {{ result.total_estimated ? '约 ' : '' }}{{ formatNumber(result.total) }} 行 · 第 {{ result.page }}/{{ result.total_pages || 1 }} 页
            </span>
            <span v-else>暂无结果</span>
          </div>
          <code class="sql-preview-code" v-if="result.generated_sql">{{ result.generated_sql }}</code>
        </div>
        
        <div class="virtual-grid-container">
          <div v-if="loading" class="state-box">加载中...</div>
          <div v-else-if="error" class="state-box state-box--error">{{ error }}</div>
          <div v-else-if="!selectedTable" class="state-box">从上方选择一个数据表开始查询。</div>
          <div v-else-if="!result.rows.length" class="state-box">没有匹配数据。</div>
          <div v-else class="table-virtual">
            <el-auto-resizer>
              <template #default="{ height, width }">
                <el-table-v2
                  :columns="tableColumns"
                  :data="tableRows"
                  :width="width"
                  :height="height"
                  :row-height="30"
                  :header-height="32"
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
                      复制
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
      </div>
    </div>

    <!-- LAYOUT C (physical) -->
    <div v-else class="layout-physical-grid">
      <!-- Left sidebar: physical Parquet partition files/folders -->
      <aside class="parquet-sidebar">
        <div class="side-title">
          <span>PARQUET PARTITIONS</span>
          <strong>物理分区文件</strong>
        </div>
        <div class="partition-list">
          <div v-if="!selectedTable" class="side-state">选择数据表以加载物理分区</div>
          <template v-else>
            <button
              v-for="file in mockPartitionFiles"
              :key="file.path"
              class="partition-item"
              :class="{ 'partition-item--selected': selectedPartitionFile?.path === file.path }"
              @click="selectPartitionFile(file)"
            >
              <span class="file-path">📁 {{ file.path }}</span>
              <span class="file-info">{{ file.size }} · {{ file.date }}</span>
            </button>
          </template>
        </div>
      </aside>

      <!-- Right side: File metadata and 50-row data sample -->
      <main class="physical-workspace">
        <div class="metadata-card">
          <div class="block-title">
            <span>METADATA</span>
            <strong>分区元数据</strong>
          </div>
          <div class="metadata-grid" v-if="selectedPartitionFile">
            <div class="metadata-cell">
              <label>物理路径 (File Path)</label>
              <span class="mono-text" :title="selectedPartitionFile.fullPath">{{ selectedPartitionFile.fullPath }}</span>
            </div>
            <div class="metadata-cell">
              <label>数据格式 (Format)</label>
              <span>Parquet (Hive Partitioned)</span>
            </div>
            <div class="metadata-cell">
              <label>预计行数 (Row Count)</label>
              <span>{{ selectedPartitionFile.rowCount }} 行</span>
            </div>
            <div class="metadata-cell">
              <label>压缩格式 (Compression)</label>
              <span>Snappy / Zstd</span>
            </div>
          </div>
          <!-- Schema list in metadata -->
          <div class="metadata-schema" v-if="selectedTable">
            <label>列模式 (Schema)</label>
            <div class="schema-chips">
              <span v-for="col in schema" :key="col.name" class="schema-chip">
                <strong>{{ col.name }}</strong>: <small>{{ col.type }}</small>
              </span>
            </div>
          </div>
        </div>

        <div class="sample-data-card">
          <div class="block-title">
            <span>SAMPLE ROWS</span>
            <strong>50 行数据示例 (Data Sample)</strong>
          </div>
          
          <div class="sample-grid-container">
            <div v-if="loading" class="state-box">加载中...</div>
            <div v-else-if="error" class="state-box state-box--error">{{ error }}</div>
            <div v-else-if="!selectedTable" class="state-box">选择数据表加载示例数据。</div>
            <div v-else-if="!result.rows.length" class="state-box">没有数据样本。</div>
            <div v-else class="table-virtual">
              <el-auto-resizer>
                <template #default="{ height, width }">
                  <el-table-v2
                    :columns="tableColumns"
                    :data="tableRows"
                    :width="width"
                    :height="height"
                    :row-height="30"
                    :header-height="32"
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
                        复制
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
          </div>
        </div>
      </main>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage, TableV2FixedDir, TableV2SortOrder, type Column, type SortBy } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
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

// Layout Mode Switcher Ref
type LayoutMode = 'A' | 'B' | 'C'
const layoutMode = ref<LayoutMode>('A')
const layoutModes: { key: LayoutMode; label: string; hint: string }[] = [
  { key: 'A', label: 'SQL 终端', hint: 'SQL IDE 混合布局' },
  { key: 'B', label: '快捷筛选', hint: '无代码可视化过滤器' },
  { key: 'C', label: '物理分析', hint: 'Parquet 分区物理结构树' },
]

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
const keyColumns = computed(() => new Set([
  'symbol',
  'trade_date',
  'trade_date_1',
  'datetime',
  'available_date',
  'snapshot_date',
  'ann_date',
  'f_ann_date',
  'end_date',
  'report_date',
  'factor_name',
  'indicator_name',
  'as_of_time',
]))
const suggestibleColumns = computed(() => new Set(['symbol', 'factor_name', 'indicator_name', 'source', 'as_of_time']))
const dateColumn = computed(() => {
  const preferred = selectedTableInfo.value?.date_column
  if (preferred && hasColumn(preferred)) return preferred
  return schema.value.find(column => [
    'trade_date_1',
    'trade_date',
    'datetime',
    'available_date',
    'date',
    'snapshot_date',
    'ann_date',
    'f_ann_date',
    'end_date',
    'report_date',
    'publish_time',
    'time',
  ].includes(column.name))?.name || ''
})
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

function formatRowCount(value: number | null | undefined) {
  if (value === null || value === undefined) return '未统计'
  return `${formatNumber(value)} 行`
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
  if (['symbol', 'trade_date', 'trade_date_1', 'datetime', 'available_date'].includes(column)) {
    return column === 'datetime' ? 180 : 132
  }
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

async function loadData(includeTotal = false) {
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
      include_total: includeTotal,
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
      include_total: true,
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

// SQL IDE Append Column Helper
function insertColumnToSql(columnName: string) {
  if (!whereClause.value) {
    whereClause.value = columnName
  } else {
    const trimmed = whereClause.value.trim()
    if (trimmed.endsWith('AND') || trimmed.endsWith('OR') || trimmed.endsWith('WHERE') || trimmed.endsWith('=')) {
      whereClause.value = `${whereClause.value} ${columnName}`
    } else {
      whereClause.value = `${whereClause.value} AND ${columnName}`
    }
  }
}

// Physical partition mocking logic
interface ParquetFile {
  path: string
  fullPath: string
  size: string
  date: string
  rowCount: string
}

const selectedPartitionFile = ref<ParquetFile | null>(null)

const mockPartitionFiles = computed<ParquetFile[]>(() => {
  if (!selectedTable.value) return []
  const name = selectedTable.value
  const list: ParquetFile[] = []
  const baseDir = `E:/Projects/data/BaiduSyncdisk/parquet/${name}`
  
  const dates = [
    { year: '2024', months: ['10', '11', '12'] },
    { year: '2025', months: ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'] },
    { year: '2026', months: ['01', '02', '03', '04', '05'] },
  ]
  
  const rowCountNum = selectedTableInfo.value?.row_count || 120000
  const fileCount = 12
  const rowCountPerFile = Math.floor(rowCountNum / fileCount)
  
  let count = 0
  for (const group of dates) {
    for (const m of group.months) {
      count++
      if (count > fileCount) break
      const sizeMb = (5.5 + Math.random() * 8.5).toFixed(1)
      const formattedRowc = formatNumber(rowCountPerFile)
      list.push({
        path: `year=${group.year}/month=${m}/part-0.parquet`,
        fullPath: `${baseDir}/year=${group.year}/month=${m}/part-0.parquet`,
        size: `${sizeMb} MB`,
        date: `${group.year}-${m}-01`,
        rowCount: formattedRowc,
      })
    }
    if (count > fileCount) break
  }
  return list
})

async function selectPartitionFile(file: ParquetFile) {
  selectedPartitionFile.value = file
  if (selectedTable.value) {
    pageSize.value = 50
    page.value = 1
    await loadData()
  }
}

watch(mockPartitionFiles, (newVal) => {
  if (newVal.length > 0) {
    selectedPartitionFile.value = newVal[0]
  } else {
    selectedPartitionFile.value = null
  }
}, { immediate: true })

watch(layoutMode, async (newVal) => {
  if (newVal === 'C' && selectedTable.value) {
    pageSize.value = 50
    page.value = 1
    await loadData()
  }
})

const pageContextBlocks = computed(() => [
  {
    title: 'Layout & View',
    rows: [
      { label: '布局模式', value: layoutMode.value === 'A' ? 'SQL 终端' : layoutMode.value === 'B' ? '快捷筛选' : '物理分析' },
      { label: '当前数据表', value: selectedTable.value || '未选择' },
      { label: '表数量', value: `${filteredTables.value.length} / ${tables.value.length}` },
      { label: '字段数', value: selectedTable.value ? `${filteredSchema.value.length}` : '-' },
    ],
  },
  {
    title: 'Query State',
    rows: [
      { label: '加载状态', value: tablesLoading.value || loading.value ? '加载中' : '已就绪', tone: tablesLoading.value || loading.value ? 'warn' : 'good' },
      { label: '筛选条件', value: `${filters.value.length} 条` },
      { label: '结果行数', value: result.value.total ? `${result.value.total.toLocaleString()} 行` : '暂无' },
      { label: '分页', value: `${page.value} / ${result.value.total_pages || 1} · ${pageSize.value} 行/页` },
    ],
  },
])

usePageContext(pageContextBlocks)

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
.data-explorer-container {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
  background-color: #fdfbf7;
  color: #22302a;
}

/* Pine Green theme aesthetics overrides */
.theme-pine-quant {
  --bg-void: #fdfbf7;
  --bg-primary: #fdfbf7;
  --bg-elevated: #f5f2ea;
  --bg-surface: #f5f2ea;
  --bg-hover: #ebe7dc;
  --bg-active: #eef3f0;
  --text-bright: #22302a;
  --text-primary: #22302a;
  --text-secondary: #54635c;
  --text-muted: #7e8d86;
  --text-label: #22302a;
  --accent-primary: #1b3d32;
  --accent-secondary: #355e4f;
  --border-subtle: #e5dfd3;
  --border-default: #e5dfd3;
  --border-accent: #1b3d32;
}

.workspace-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 14px;
  border-bottom: 1px solid #e5dfd3;
  background-color: #f5f2ea;
  flex-shrink: 0;
}

.header-left {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.panel-kicker {
  font-size: 10px;
  letter-spacing: 0.05em;
  color: #54635c;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
}

.workspace-header h2 {
  margin: 0;
  font-size: 16px;
  font-weight: 700;
  color: #22302a;
}

.workspace-header p {
  margin: 0;
  font-size: 11px;
  color: #54635c;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

/* Layout Switcher */
.layout-switcher {
  display: inline-flex;
  gap: 2px;
  padding: 2px;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background: #ebe7dc;
}

.layout-switcher button {
  border: 0;
  border-radius: 3px;
  padding: 5px 10px;
  color: #54635c;
  background: transparent;
  font-size: 12px;
  font-weight: 700;
  cursor: pointer;
  transition: all 0.2s ease;
}

.layout-switcher button:hover {
  background: #f5f2ea;
  color: #22302a;
}

.layout-switcher button.active {
  color: #fdfbf7;
  background: #1b3d32;
}

/* General layout styles */
.layout-sql-grid {
  display: grid;
  grid-template-columns: 220px minmax(0, 1fr);
  gap: 0;
  flex: 1;
  min-height: 0;
  overflow: hidden;
  border-bottom: 1px solid #e5dfd3;
}

.layout-filter-grid {
  display: flex;
  flex-direction: column;
  gap: 0;
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

.layout-physical-grid {
  display: grid;
  grid-template-columns: 260px minmax(0, 1fr);
  gap: 0;
  flex: 1;
  min-height: 0;
  overflow: hidden;
  border-bottom: 1px solid #e5dfd3;
}

/* Sidebar Styles */
.schema-sidebar,
.parquet-sidebar {
  width: 100%;
  border-right: 1px solid #e5dfd3;
  background-color: #f5f2ea;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}

.sidebar-section {
  display: flex;
  flex-direction: column;
  padding: 10px;
  border-bottom: 1px solid #e5dfd3;
  min-height: 0;
}

.sidebar-section.columns-section {
  flex: 1;
}

.side-title {
  display: flex;
  flex-direction: column;
  margin-bottom: 6px;
}

.side-title span {
  font-size: 9px;
  color: #7e8d86;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
}

.side-title strong {
  font-size: 12px;
  color: #22302a;
}

.side-state {
  font-size: 11px;
  color: #7e8d86;
  padding: 8px;
  text-align: center;
  border: 1px dashed #e5dfd3;
  border-radius: 4px;
  margin-top: 4px;
}

/* Table and Column Mini Lists */
.table-list-mini,
.column-list-mini {
  display: flex;
  flex-direction: column;
  gap: 2px;
  overflow-y: auto;
  margin-top: 6px;
  flex: 1;
}

.table-item-mini {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  width: 100%;
  padding: 6px 8px;
  border: 1px solid transparent;
  border-radius: 3px;
  background: transparent;
  text-align: left;
  cursor: pointer;
}

.table-item-mini:hover {
  background-color: #ebe7dc;
}

.table-item-mini--selected {
  background-color: #eef3f0;
  border-color: #1b3d32;
}

.table-name-span {
  font-size: 12px;
  font-weight: 700;
  color: #22302a;
  word-break: break-all;
}

.table-label-small {
  font-size: 10px;
  color: #54635c;
}

.column-item-mini {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
  padding: 5px 6px;
  border: none;
  background: transparent;
  text-align: left;
  cursor: pointer;
  border-radius: 2px;
}

.column-item-mini:hover {
  background-color: #ebe7dc;
  color: #1b3d32;
}

.column-item-mini--key .col-name {
  color: #b27a1e;
  font-weight: bold;
}

.col-name {
  font-size: 11px;
  color: #22302a;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
}

.col-type {
  font-size: 10px;
  color: #7e8d86;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
}

/* SQL IDE workspace */
.sql-workspace {
  display: flex;
  flex-direction: column;
  min-height: 0;
  flex: 1;
  background-color: #fdfbf7;
}

.sql-input-panel {
  padding: 12px;
  border-bottom: 1px solid #e5dfd3;
  background-color: #fdfbf7;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.panel-header-inline {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.block-title {
  display: flex;
  flex-direction: column;
}

.block-title strong {
  font-size: 13px;
  color: #22302a;
}

.block-title span {
  font-size: 11px;
  color: #54635c;
}

/* Result panel grid */
.result-table-panel {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0;
  padding: 0;
}

.result-header-inline {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 12px;
  background-color: #f5f2ea;
  border-bottom: 1px solid #e5dfd3;
}

.result-title-info {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.result-title-info strong {
  font-size: 12px;
  color: #22302a;
}

.result-title-info span {
  font-size: 11px;
  color: #54635c;
}

.sql-preview-code {
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
  font-size: 10px;
  background-color: #ebe7dc;
  padding: 2px 6px;
  border-radius: 3px;
  max-width: 60%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: #54635c;
}

.virtual-grid-container {
  flex: 1;
  min-height: 200px;
  position: relative;
  overflow: hidden;
}

.table-virtual {
  height: 100%;
  width: 100%;
}

.state-box {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  font-size: 12px;
  color: #7e8d86;
  background: #fdfbf7;
}

.state-box--error {
  color: #a83232;
  font-weight: bold;
}

/* Pagination */
.pagination {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 6px 12px;
  background-color: #f5f2ea;
  border-top: 1px solid #e5dfd3;
}

.pagination span {
  font-size: 12px;
  color: #22302a;
}

.page-size {
  width: 90px;
}

/* Layout B: Filter Page Builder */
.filter-builder-panel {
  padding: 12px;
  border-bottom: 1px solid #e5dfd3;
  background-color: #f5f2ea;
  display: flex;
  flex-direction: column;
  gap: 10px;
  flex-shrink: 0;
}

.builder-row-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.builder-select-table {
  display: flex;
  align-items: center;
  gap: 8px;
}

.builder-select-table label {
  font-size: 12px;
  font-weight: 700;
  color: #22302a;
}

.table-selector-b {
  width: 280px;
}

.builder-actions {
  display: flex;
  gap: 8px;
}

.quick-filter-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 8px 12px;
  padding: 8px;
  background: #fdfbf7;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
}

.quick-filter-item {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.quick-filter-item.date-range-item {
  grid-column: span 2;
}

.filter-label {
  font-size: 11px;
  font-weight: bold;
  color: #54635c;
}

.date-pair {
  display: grid;
  grid-template-columns: 1fr auto 1fr;
  align-items: center;
  gap: 6px;
}

.date-pair span {
  font-size: 11px;
  color: #7e8d86;
}

.dynamic-filters-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-height: 150px;
  overflow-y: auto;
  padding: 4px;
}

.filter-row {
  display: grid;
  grid-template-columns: 1fr 100px 1fr 1fr auto;
  gap: 6px;
  align-items: center;
  padding: 4px 8px;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #fdfbf7;
}

.filter-row--invalid {
  border-color: #a83232;
}

/* Layout C: Physical workspace */
.partition-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  overflow-y: auto;
  padding: 10px;
  flex: 1;
}

.partition-item {
  display: flex;
  flex-direction: column;
  width: 100%;
  padding: 6px 8px;
  border: 1px solid transparent;
  border-radius: 3px;
  background: transparent;
  cursor: pointer;
  text-align: left;
}

.partition-item:hover {
  background-color: #ebe7dc;
}

.partition-item--selected {
  background-color: #eef3f0;
  border-color: #1b3d32;
}

.file-path {
  font-size: 11px;
  font-weight: 700;
  color: #22302a;
  word-break: break-all;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
}

.file-info {
  font-size: 10px;
  color: #7e8d86;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
}

.physical-workspace {
  display: flex;
  flex-direction: column;
  min-height: 0;
  flex: 1;
  background-color: #fdfbf7;
  overflow-y: auto;
}

.metadata-card {
  padding: 12px;
  border-bottom: 1px solid #e5dfd3;
  background-color: #fdfbf7;
  display: flex;
  flex-direction: column;
  gap: 8px;
  flex-shrink: 0;
}

.metadata-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
}

.metadata-cell {
  display: flex;
  flex-direction: column;
  padding: 6px 8px;
  border: 1px solid #e5dfd3;
  background-color: #f5f2ea;
  border-radius: 3px;
}

.metadata-cell label {
  font-size: 9px;
  color: #7e8d86;
}

.metadata-cell span {
  font-size: 11px;
  font-weight: bold;
  color: #22302a;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mono-text {
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
}

.metadata-schema {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.metadata-schema label {
  font-size: 11px;
  font-weight: bold;
  color: #54635c;
}

.schema-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  max-height: 80px;
  overflow-y: auto;
  border: 1px solid #e5dfd3;
  padding: 6px;
  background-color: #f5f2ea;
  border-radius: 3px;
}

.schema-chip {
  font-size: 10px;
  padding: 2px 6px;
  background: #fdfbf7;
  border: 1px solid #e5dfd3;
  border-radius: 3px;
  font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
}

.schema-chip strong {
  color: #1b3d32;
}

.schema-chip small {
  color: #7e8d86;
}

.sample-data-card {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0;
  padding: 12px;
}

.sample-grid-container {
  flex: 1;
  min-height: 200px;
  position: relative;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  overflow: hidden;
  margin-top: 8px;
}

/* Deep overrides for Element Plus inside theme-pine-quant */
.theme-pine-quant :deep(.el-input__wrapper),
.theme-pine-quant :deep(.el-select__wrapper),
.theme-pine-quant :deep(.el-textarea__inner) {
  background-color: #fdfbf7 !important;
  color: #22302a !important;
  box-shadow: 0 0 0 1px #e5dfd3 inset !important;
  border-radius: 4px;
}

.theme-pine-quant :deep(.el-input__inner),
.theme-pine-quant :deep(.el-select__text),
.theme-pine-quant :deep(.el-textarea__inner) {
  color: #22302a !important;
}

.theme-pine-quant :deep(.el-input__wrapper:hover),
.theme-pine-quant :deep(.el-select__wrapper:hover),
.theme-pine-quant :deep(.el-input__wrapper.is-focus),
.theme-pine-quant :deep(.el-select__wrapper.is-focus) {
  box-shadow: 0 0 0 1px #1b3d32 inset !important;
}

.theme-pine-quant :deep(.el-button) {
  background-color: #f5f2ea;
  border-color: #e5dfd3;
  color: #22302a;
}

.theme-pine-quant :deep(.el-button:hover) {
  background-color: #ebe7dc;
  border-color: #1b3d32;
  color: #1b3d32;
}

.theme-pine-quant :deep(.el-button--primary) {
  background-color: #1b3d32;
  border-color: #1b3d32;
  color: #fdfbf7;
}

.theme-pine-quant :deep(.el-button--primary:hover) {
  background-color: #355e4f;
  border-color: #355e4f;
  color: #fdfbf7;
}

.theme-pine-quant :deep(.el-button--success) {
  background-color: #2d6a4f;
  border-color: #2d6a4f;
  color: #fdfbf7;
}

.theme-pine-quant :deep(.el-button--danger) {
  background-color: #a83232;
  border-color: #a83232;
  color: #fdfbf7;
}

/* High density monospaced table overrides */
.theme-pine-quant :deep(.explorer-table-v2) {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  --el-table-v2-row-bg-color: #fdfbf7;
  --el-table-v2-row-hover-bg-color: #ebe7dc;
  --el-table-v2-header-bg-color: #f5f2ea;
  --el-table-v2-header-text-color: #54635c;
  --el-table-v2-border-color: #e5dfd3;
}

.theme-pine-quant :deep(.el-table-v2__main),
.theme-pine-quant :deep(.el-table-v2__left),
.theme-pine-quant :deep(.el-table-v2__right),
.theme-pine-quant :deep(.el-table-v2__header),
.theme-pine-quant :deep(.el-table-v2__body),
.theme-pine-quant :deep(.el-table-v2__empty) {
  background: #fdfbf7;
  color: #22302a;
}

.theme-pine-quant :deep(.el-table-v2__row),
.theme-pine-quant :deep(.el-table-v2__row-cell) {
  background: #fdfbf7;
  color: #22302a;
}

.theme-pine-quant :deep(.el-table-v2__header-cell) {
  background: #f5f2ea;
  color: #54635c;
  font-weight: 600;
  border-bottom: 1px solid #e5dfd3;
}

.theme-pine-quant :deep(.el-table-v2__row-cell) {
  border-bottom: 1px solid #e5dfd3;
}

.cell-text {
  color: #22302a;
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  width: 100%;
}

.cell-text:hover {
  color: #1b3d32;
  cursor: copy;
}

.cell-text--muted {
  color: #7e8d86;
}

.cell-text--right {
  text-align: right;
}

@media (max-width: 1180px) {
  .layout-sql-grid,
  .layout-physical-grid {
    grid-template-columns: 1fr;
  }
  
  .schema-sidebar,
  .parquet-sidebar {
    height: 300px;
    border-right: none;
    border-bottom: 1px solid #e5dfd3;
  }
}
</style>
