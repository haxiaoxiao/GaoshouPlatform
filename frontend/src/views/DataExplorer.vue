<template>
  <div class="data-explorer">
    <div class="explorer-toolbar">
      <div class="toolbar-left">
        <select v-model="selectedTable" @change="onTableChange" class="table-select">
          <option value="">选择数据表...</option>
          <option v-for="t in tables" :key="t.name" :value="t.name">
            {{ t.name }} &nbsp;({{ formatNumber(t.row_count) }} 行)
          </option>
        </select>
        <div v-if="schema.length" class="schema-hint">
          {{ schema.length }} 列
        </div>
      </div>
      <div class="toolbar-right">
        <input
          v-model="whereClause"
          type="text"
          class="where-input"
          placeholder="WHERE 条件，如: symbol='600051.SH'"
          @keyup.enter="loadData"
        />
        <button class="btn btn--primary" @click="loadData" :disabled="!selectedTable">查询</button>
        <button class="btn btn--ghost" @click="resetFilters" :disabled="!selectedTable">重置</button>
      </div>
    </div>

    <div v-if="loading" class="loading-state">
      <div class="spinner"></div>
      <span>加载中...</span>
    </div>

    <div v-else-if="error" class="error-state">
      <span class="error-icon">!</span>
      <span>{{ error }}</span>
    </div>

    <div v-else-if="!selectedTable" class="empty-state">
      <div class="empty-icon">&#128202;</div>
      <p>选择一个数据表开始浏览</p>
    </div>

    <template v-else>
      <div v-if="schema.length" class="schema-bar">
        <details>
          <summary>表结构 ({{ schema.length }} 列)</summary>
          <div class="schema-grid">
            <div v-for="col in schema" :key="col.name" class="schema-col" @click="toggleColumnVisibility(col.name)">
              <span class="col-name" :class="{ 'col-hidden': hiddenColumns.has(col.name) }">{{ col.name }}</span>
              <span class="col-type">{{ col.type }}</span>
            </div>
          </div>
        </details>
      </div>

      <div v-if="result.rows.length" class="table-container">
        <div class="table-stats">
          共 {{ formatNumber(result.total) }} 行 · 第 {{ result.page }}/{{ result.total_pages }} 页
        </div>
        <div class="table-scroll">
          <table class="data-table">
            <thead>
              <tr>
                <th class="row-num">#</th>
                <th
                  v-for="col in visibleColumns"
                  :key="col"
                  class="data-th"
                  :class="{ 'th-sorted': orderBy === col }"
                  @click="toggleSort(col)"
                >
                  {{ col }}
                  <span v-if="orderBy === col" class="sort-arrow">{{ orderDir === 'ASC' ? '↑' : '↓' }}</span>
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, idx) in result.rows" :key="idx">
                <td class="row-num">{{ (result.page - 1) * result.page_size + idx + 1 }}</td>
                <td v-for="col in visibleColumns" :key="col" class="data-td" :title="formatCell(row[col])">
                  {{ formatCell(row[col]) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <div v-else-if="selectedTable" class="empty-state">
        <p>该表无数据</p>
      </div>

      <div v-if="result.total_pages > 1" class="pagination">
        <button class="btn btn--small" :disabled="result.page <= 1" @click="goPage(1)">⟪</button>
        <button class="btn btn--small" :disabled="result.page <= 1" @click="goPage(result.page - 1)">⟨</button>
        <span class="page-info">{{ result.page }} / {{ result.total_pages }}</span>
        <button class="btn btn--small" :disabled="result.page >= result.total_pages" @click="goPage(result.page + 1)">⟩</button>
        <button class="btn btn--small" :disabled="result.page >= result.total_pages" @click="goPage(result.total_pages)">⟫</button>
        <select v-model.number="pageSize" @change="loadData" class="page-size-select">
          <option :value="50">50</option>
          <option :value="100">100</option>
          <option :value="200">200</option>
        </select>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import {
  getTables, getTableSchema, previewTable,
  type TableInfo, type ColumnInfo, type PreviewResult
} from '@/api/explorer'

const tables = ref<TableInfo[]>([])
const selectedTable = ref('')
const schema = ref<ColumnInfo[]>([])
const whereClause = ref('')
const orderBy = ref('')
const orderDir = ref<'ASC' | 'DESC'>('ASC')
const page = ref(1)
const pageSize = ref(50)
const hiddenColumns = ref(new Set<string>())

const loading = ref(false)
const error = ref('')

const result = ref<PreviewResult>({
  columns: [], rows: [], total: 0, page: 1, page_size: 50, total_pages: 0
})

const visibleColumns = computed(() => {
  return result.value.columns.filter(c => !hiddenColumns.value.has(c))
})

function formatNumber(n: number) {
  if (n >= 100_000_000) return (n / 100_000_000).toFixed(1) + '亿'
  if (n >= 10_000) return (n / 10_000).toFixed(1) + '万'
  return n.toLocaleString()
}

function formatCell(val: any): string {
  if (val === null || val === undefined) return '-'
  if (typeof val === 'number') {
    if (Number.isInteger(val)) return val.toLocaleString()
    return val.toFixed(4)
  }
  const s = String(val)
  return s.length > 60 ? s.substring(0, 57) + '...' : s
}

function toggleSort(col: string) {
  if (orderBy.value === col) {
    if (orderDir.value === 'ASC') orderDir.value = 'DESC'
    else { orderBy.value = ''; orderDir.value = 'ASC' }
  } else {
    orderBy.value = col
    orderDir.value = 'ASC'
  }
  loadData()
}

function toggleColumnVisibility(col: string) {
  const s = new Set(hiddenColumns.value)
  if (s.has(col)) s.delete(col)
  else s.add(col)
  hiddenColumns.value = s
}

async function onTableChange() {
  schema.value = []
  whereClause.value = ''
  orderBy.value = ''
  orderDir.value = 'ASC'
  page.value = 1
  hiddenColumns.value = new Set()
  result.value = { columns: [], rows: [], total: 0, page: 1, page_size: 50, total_pages: 0 }

  if (!selectedTable.value) return

  loading.value = true
  error.value = ''
  try {
    const res = await getTableSchema(selectedTable.value)
    if (res && res.length > 0) schema.value = res
    await loadData()
  } catch (e: any) {
    error.value = e.message || '加载失败'
  } finally {
    loading.value = false
  }
}

async function loadData() {
  if (!selectedTable.value) return
  loading.value = true
  error.value = ''
  try {
    const res = await previewTable(selectedTable.value, {
      page: page.value,
      page_size: pageSize.value,
      order_by: orderBy.value || undefined,
      order_dir: orderDir.value,
      where: whereClause.value || undefined,
    })
    if (res) {
      result.value = res
    } else {
      error.value = '查询失败'
    }
  } catch (e: any) {
    error.value = e.message || '查询出错'
  } finally {
    loading.value = false
  }
}

function resetFilters() {
  whereClause.value = ''
  orderBy.value = ''
  orderDir.value = 'ASC'
  page.value = 1
  loadData()
}

function goPage(p: number) {
  page.value = p
  loadData()
}

onMounted(async () => {
  try {
    const res = await getTables()
    if (res && res.length > 0) tables.value = res
  } catch {}
})
</script>

<style scoped>
.data-explorer {
  display: flex;
  flex-direction: column;
  gap: var(--space-4);
  height: 100%;
}

.explorer-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: var(--space-3);
  flex-wrap: wrap;
}

.toolbar-left {
  display: flex;
  align-items: center;
  gap: var(--space-3);
}

.toolbar-right {
  display: flex;
  align-items: center;
  gap: var(--space-2);
}

.table-select {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  padding: var(--space-2) var(--space-3);
  font-size: var(--text-sm);
  min-width: 260px;
  cursor: pointer;
}

.schema-hint {
  font-size: var(--text-xs);
  color: var(--text-muted);
  padding: var(--space-1) var(--space-2);
  background: var(--bg-surface);
  border-radius: var(--radius-sm);
}

.where-input {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  padding: var(--space-2) var(--space-3);
  font-size: var(--text-sm);
  width: 320px;
  font-family: var(--font-ui);
}

.where-input:focus {
  outline: none;
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.1);
}

.btn {
  padding: var(--space-2) var(--space-4);
  border-radius: var(--radius-md);
  font-size: var(--text-sm);
  font-weight: 500;
  cursor: pointer;
  border: 1px solid transparent;
  transition: all 0.15s;
}

.btn--primary {
  background: var(--accent-primary);
  color: var(--bg-void);
  border-color: var(--accent-primary);
}

.btn--primary:hover { opacity: 0.9; }
.btn--primary:disabled { opacity: 0.4; cursor: not-allowed; }

.btn--ghost {
  background: transparent;
  color: var(--text-secondary);
  border-color: var(--border-default);
}

.btn--ghost:hover { background: var(--bg-hover); color: var(--text-primary); }
.btn--ghost:disabled { opacity: 0.4; cursor: not-allowed; }

.btn--small {
  padding: var(--space-1) var(--space-2);
  font-size: var(--text-xs);
  background: var(--bg-surface);
  color: var(--text-secondary);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  cursor: pointer;
}

.btn--small:hover { background: var(--bg-hover); }
.btn--small:disabled { opacity: 0.3; cursor: not-allowed; }

.loading-state, .error-state, .empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--space-3);
  padding: var(--space-16);
  color: var(--text-muted);
}

.spinner {
  width: 24px; height: 24px;
  border: 2px solid var(--border-default);
  border-top-color: var(--accent-primary);
  border-radius: 50%;
  animation: spin 0.6s linear infinite;
}

@keyframes spin { to { transform: rotate(360deg); } }

.error-icon {
  width: 32px; height: 32px;
  background: var(--accent-danger);
  color: white;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-weight: 700;
}

.empty-icon { font-size: 48px; opacity: 0.3; }

.schema-bar {
  margin-bottom: 0;
}

.schema-bar details {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: var(--space-2) var(--space-3);
}

.schema-bar summary {
  cursor: pointer;
  font-size: var(--text-xs);
  color: var(--text-muted);
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: var(--tracking-wider);
}

.schema-grid {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-2);
  margin-top: var(--space-2);
}

.schema-col {
  display: flex;
  align-items: center;
  gap: var(--space-1);
  padding: 2px var(--space-2);
  background: var(--bg-elevated);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: background 0.15s;
}

.schema-col:hover { background: var(--bg-hover); }

.col-name {
  font-size: var(--text-xs);
  font-weight: 500;
  color: var(--text-primary);
  font-family: var(--font-display);
}

.col-name.col-hidden {
  text-decoration: line-through;
  color: var(--text-ghost);
}

.col-type {
  font-size: 10px;
  color: var(--text-muted);
  font-family: var(--font-display);
}

.table-stats {
  font-size: var(--text-xs);
  color: var(--text-muted);
  padding: var(--space-1) var(--space-3);
}

.table-scroll {
  overflow: auto;
  max-height: calc(100vh - 320px);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--text-xs);
  font-family: var(--font-ui);
}

.data-th {
  position: sticky;
  top: 0;
  background: var(--bg-elevated);
  color: var(--text-secondary);
  padding: var(--space-2) var(--space-3);
  text-align: left;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: var(--tracking-wider);
  border-bottom: 1px solid var(--border-default);
  cursor: pointer;
  white-space: nowrap;
  user-select: none;
}

.data-th:hover { color: var(--accent-primary); }

.th-sorted { color: var(--accent-primary); }

.sort-arrow { margin-left: 2px; }

.row-num {
  color: var(--text-ghost);
  text-align: right;
  font-family: var(--font-display);
  min-width: 40px;
}

.data-td {
  padding: var(--space-1) var(--space-3);
  border-bottom: 1px solid var(--border-subtle);
  color: var(--text-primary);
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.data-table tbody tr:hover { background: rgba(56, 189, 248, 0.04); }

.pagination {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--space-2);
  padding: var(--space-3) 0;
}

.page-info {
  font-size: var(--text-xs);
  color: var(--text-muted);
  min-width: 80px;
  text-align: center;
}

.page-size-select {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  padding: 2px var(--space-2);
  font-size: var(--text-xs);
  margin-left: var(--space-2);
}
</style>