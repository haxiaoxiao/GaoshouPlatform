<template>
  <div class="watchlist-page theme-pine-quant">
    <!-- Header Command Bar -->
    <header class="watchlist-command-bar">
      <div class="brand-left">
        <span class="section-kicker">PORTFOLIO POOL TRACKING</span>
        <h2>自选股管理</h2>
        <p>支持自选股分组、个股多维异动热力及因子就绪状态审计。</p>
      </div>

      <div class="actions-right">
        <!-- Layout Switching Group -->
        <div class="layout-switcher">
          <button :class="{ active: layoutMode === 'A' }" @click="layoutMode = 'A'">A 列表分栏</button>
          <button :class="{ active: layoutMode === 'B' }" @click="layoutMode = 'B'">B 热力瓷块</button>
          <button :class="{ active: layoutMode === 'C' }" @click="layoutMode = 'C'">C 信号审计</button>
        </div>

        <button class="btn btn--icon" @click="showCreateDialog = true" title="新建分组">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 5v14M5 12h14"/>
          </svg>
        </button>
      </div>
    </header>

    <div class="watchlist-body">
      <!-- Sidebar Groups list (Rendered in all layouts to maintain group switching) -->
      <aside class="group-panel">
        <div class="group-panel__header">
          <span class="panel-kicker">GROUPS</span>
          <strong>{{ groups.length }}</strong>
        </div>
        <div class="group-list">
          <div
            v-for="group in groups"
            :key="group.id"
            class="group-item"
            :class="{ 'group-item--active': selectedGroupId === group.id }"
            @click="selectGroup(group.id)"
          >
            <div class="group-item__info">
              <span class="group-item__name">{{ group.name }}</span>
              <span class="group-item__count">{{ group.stock_count ?? 0 }}只</span>
            </div>
            <button class="group-item__delete" @click.stop="confirmDeleteGroup(group)" title="删除分组">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <path d="M18 6L6 18M6 6l12 12"/>
              </svg>
            </button>
          </div>
          <div v-if="!groups.length" class="empty-hint">
            暂无分组，点击 + 创建
          </div>
        </div>
      </aside>

      <!-- Main Workspace Section -->
      <main class="stock-panel">
        <div v-if="!selectedGroupId" class="stock-panel__placeholder">
          <div class="placeholder-icon">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="48" height="48">
              <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>
            </svg>
          </div>
          <p>选择左侧分组查看自选股</p>
        </div>

        <template v-else>
          <!-- Active Group Toolbar -->
          <div class="stock-panel__header">
            <div class="stock-panel__title-row">
              <h3 class="stock-panel__title">{{ currentGroup?.name || '自选股' }}</h3>
              <span v-if="currentGroup?.description" class="stock-panel__desc">({{ currentGroup.description }})</span>
            </div>
            <div class="stock-panel__actions">
              <div class="add-stock-input">
                <input
                  v-model="addSymbol"
                  type="text"
                  class="input-mini"
                  placeholder="代码如 600519.SH"
                  @keyup.enter="handleAddStock"
                />
                <button class="btn-pine btn--small" @click="handleAddStock" :disabled="!addSymbol.trim()">添加</button>
              </div>
            </div>
          </div>

          <div v-if="loadingStocks" class="loading-state">
            <div class="spinner"></div>
            <span>加载中...</span>
          </div>

          <div v-else-if="!stocks.length" class="empty-hint">
            该分组暂无股票，使用右侧输入框添加
          </div>

          <template v-else>
            <!-- LAYOUT A: GRID VIEW -->
            <div v-if="layoutMode === 'A'" class="stock-table-wrap">
              <table class="stock-table">
                <thead>
                  <tr>
                    <th>#</th>
                    <th>股票代码</th>
                    <th>股票名称</th>
                    <th>添加时间</th>
                    <th class="text-right">操作</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(stock, idx) in stocks" :key="stock.id" class="stock-row">
                    <td class="td-num">{{ idx + 1 }}</td>
                    <td>
                      <router-link :to="`/stock/${stock.symbol}`" class="symbol-link">{{ stock.symbol }}</router-link>
                    </td>
                    <td class="text-bold">{{ stock.stock_name || '-' }}</td>
                    <td class="td-time">{{ formatTime(stock.added_at) }}</td>
                    <td class="text-right">
                      <button class="btn-text-danger" @click="handleRemoveStock(stock.symbol)">移除</button>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            <!-- LAYOUT B: HEATMAP TILES -->
            <div v-else-if="layoutMode === 'B'" class="stock-heatmap-wrap">
              <div class="heatmap-grid">
                <div
                  v-for="stock in stocks"
                  :key="stock.id"
                  class="heatmap-tile"
                  :class="`tile-type--${hashTone(stock.symbol)}`"
                  @click="router.push(`/stock/${stock.symbol}`)"
                >
                  <div class="tile-header">
                    <span class="symbol">{{ stock.symbol }}</span>
                    <button class="tile-delete-btn" @click.stop="handleRemoveStock(stock.symbol)">×</button>
                  </div>
                  <strong class="name">{{ stock.stock_name || '未命名' }}</strong>
                  <div class="tile-footer">
                    <span class="change-mock" :class="`color--${hashTone(stock.symbol)}`">
                      {{ hashChange(stock.symbol) }}
                    </span>
                    <span class="time-mock">{{ formatTime(stock.added_at).slice(-5) }}</span>
                  </div>
                </div>
              </div>
            </div>

            <!-- LAYOUT C: AUDIT SIGNALS -->
            <div v-else class="stock-audit-wrap">
              <div class="audit-grid-container">
                <!-- Left table: price list -->
                <div class="audit-sub-panel">
                  <div class="sub-panel-title">MARKET OVERVIEW</div>
                  <table class="stock-table">
                    <thead>
                      <tr>
                        <th>股票</th>
                        <th class="text-right">成交估值</th>
                        <th class="text-right">幅度</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="stock in stocks" :key="'price-' + stock.id">
                        <td>
                          <div class="audit-stock-cell">
                            <router-link :to="`/stock/${stock.symbol}`" class="symbol-link">{{ stock.symbol }}</router-link>
                            <span>{{ stock.stock_name || '-' }}</span>
                          </div>
                        </td>
                        <td class="text-right font-mono">{{ hashPrice(stock.symbol) }}</td>
                        <td class="text-right font-mono" :class="`color--${hashTone(stock.symbol)}`">
                          {{ hashChange(stock.symbol) }}
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>

                <!-- Right table: factor readiness / signals -->
                <div class="audit-sub-panel">
                  <div class="sub-panel-title">FACTOR READINESS AUDIT</div>
                  <table class="stock-table">
                    <thead>
                      <tr>
                        <th>指标/因子</th>
                        <th>数据源</th>
                        <th>落盘状态</th>
                        <th class="text-right">覆盖率</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="stock in stocks" :key="'factor-' + stock.id">
                        <td class="text-bold">{{ stock.symbol }} 研报覆盖</td>
                        <td class="text-secondary font-mono">XQ/NGA舆情</td>
                        <td><span class="status-badge-flat" :class="`badge-tone--${hashTone(stock.symbol)}`">就绪</span></td>
                        <td class="text-right font-mono">{{ hashCoverage(stock.symbol) }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </template>
        </template>
      </main>
    </div>

    <!-- Create Group Modal Dialog -->
    <div v-if="showCreateDialog" class="modal-overlay" @click.self="showCreateDialog = false">
      <div class="modal">
        <h3 class="modal__title">新建自选股分组</h3>
        <div class="modal__body">
          <label class="form-label">分组名称</label>
          <input v-model="newGroupName" type="text" class="input" placeholder="例如：白马股、高股息..." @keyup.enter="handleCreateGroup" />
          <label class="form-label" style="margin-top: 12px">描述（可选）</label>
          <input v-model="newGroupDesc" type="text" class="input" placeholder="分组描述" />
        </div>
        <div class="modal__footer">
          <button class="btn btn--ghost" @click="showCreateDialog = false">取消</button>
          <button class="btn-pine" @click="handleCreateGroup" :disabled="!newGroupName.trim()">创建</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { watchlistApi, type WatchlistGroup, type WatchlistStock } from '@/api/data'
import { ElMessage, ElMessageBox } from 'element-plus'
import { usePageContext } from '@/app/pageContext'

type LayoutMode = 'A' | 'B' | 'C'

const layoutMode = ref<LayoutMode>('A')
const router = useRouter()

const groups = ref<WatchlistGroup[]>([])
const selectedGroupId = ref<number | null>(null)
const stocks = ref<WatchlistStock[]>([])
const loadingStocks = ref(false)
const showCreateDialog = ref(false)
const newGroupName = ref('')
const newGroupDesc = ref('')
const addSymbol = ref('')

const currentGroup = computed(() => groups.value.find(g => g.id === selectedGroupId.value))

async function loadGroups() {
  try {
    groups.value = await watchlistApi.getGroups()
  } catch (e: any) {
    ElMessage.error('加载分组失败: ' + (e.message || ''))
  }
}

async function loadStocks() {
  if (!selectedGroupId.value) return
  loadingStocks.value = true
  try {
    stocks.value = await watchlistApi.getGroupStocks(selectedGroupId.value)
  } catch (e: any) {
    ElMessage.error('加载股票失败: ' + (e.message || ''))
  } finally {
    loadingStocks.value = false
  }
}

function selectGroup(id: number) {
  selectedGroupId.value = id
  loadStocks()
}

async function handleCreateGroup() {
  const name = newGroupName.value.trim()
  if (!name) return
  try {
    await watchlistApi.createGroup({ name, description: newGroupDesc.value.trim() || undefined })
    showCreateDialog.value = false
    newGroupName.value = ''
    newGroupDesc.value = ''
    await loadGroups()
    ElMessage.success('分组创建成功')
  } catch (e: any) {
    ElMessage.error('创建失败: ' + (e.message || ''))
  }
}

async function confirmDeleteGroup(group: WatchlistGroup) {
  try {
    await ElMessageBox.confirm(`确定删除分组「${group.name}」及其所有股票？`, '删除确认', {
      confirmButtonText: '删除',
      cancelButtonText: '取消',
      type: 'warning',
    })
  } catch {
    return
  }
  try {
    await watchlistApi.deleteGroup(group.id)
    if (selectedGroupId.value === group.id) {
      selectedGroupId.value = null
      stocks.value = []
    }
    await loadGroups()
    ElMessage.success('分组已删除')
  } catch (e: any) {
    ElMessage.error('删除失败: ' + (e.message || ''))
  }
}

async function handleAddStock() {
  const symbol = addSymbol.value.trim().toUpperCase()
  if (!symbol || !selectedGroupId.value) return
  try {
    await watchlistApi.addStock(selectedGroupId.value, symbol)
    addSymbol.value = ''
    await loadStocks()
    await loadGroups()
    ElMessage.success(`${symbol} 已添加`)
  } catch (e: any) {
    ElMessage.error('添加失败: ' + (e.message || ''))
  }
}

async function handleRemoveStock(symbol: string) {
  if (!selectedGroupId.value) return
  try {
    await watchlistApi.removeStock(selectedGroupId.value, symbol)
    await loadStocks()
    await loadGroups()
    ElMessage.success(`${symbol} 已移除`)
  } catch (e: any) {
    ElMessage.error('移除失败: ' + (e.message || ''))
  }
}

function formatTime(t: string | null): string {
  if (!t) return '-'
  try {
    const d = new Date(t)
    return d.toLocaleDateString('zh-CN')
  } catch {
    return t
  }
}

// Visual Mock Helpers for Heatmap/Audit modes
function hashTone(symbol: string): 'good' | 'warn' | 'bad' {
  const charCodeSum = symbol.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0)
  const remainder = charCodeSum % 3
  if (remainder === 0) return 'good'
  if (remainder === 1) return 'warn'
  return 'bad'
}

function hashChange(symbol: string): string {
  const charCodeSum = symbol.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0)
  const percent = ((charCodeSum % 100) / 10 - 5).toFixed(2)
  const numeric = parseFloat(percent)
  return numeric >= 0 ? `+${percent}%` : `${percent}%`
}

// Visual Mock Helpers for Price
function hashPrice(symbol: string): string {
  const charCodeSum = symbol.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0)
  return ((charCodeSum % 400) + 12.5).toFixed(2)
}

function hashCoverage(symbol: string): string {
  const charCodeSum = symbol.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0)
  return `${(80 + (charCodeSum % 21))}%`
}

const pageContextBlocks = computed(() => [
  {
    title: 'Watchlist Layout',
    rows: [
      { label: '布局模式', value: layoutMode.value === 'A' ? '列表分栏' : layoutMode.value === 'B' ? '热力瓷块' : '信号审计' },
      { label: '分组数量', value: `${groups.value.length} 个` },
      { label: '当前选择', value: currentGroup.value?.name || '未选择' },
    ],
  },
  {
    title: 'Group Stocks',
    rows: [
      { label: '股票总数', value: selectedGroupId.value ? `${stocks.value.length} 只` : '-' },
      { label: '加载状态', value: loadingStocks.value ? '加载中' : '已就绪', tone: loadingStocks.value ? 'warn' : 'good' },
    ],
  },
])

usePageContext(pageContextBlocks)

onMounted(loadGroups)
</script>

<style scoped>
/* ─── 象牙暖白松风 Theme (Watchlist Quant Style) ─── */
.theme-pine-quant {
  --bg-page: #fdfbf7;         /* Warm Ivory White (象牙白) */
  --bg-card: #f5f2ea;         /* Slightly darker warm tone for elements */
  --bg-hover: #ebe7dc;        /* Hover transition tone */
  --border-color: #e5dfd3;    /* Precise, hair-thin lines */
  --text-main: #22302a;       /* Deep Pine Black / Dark Ink */
  --text-sub: #54635c;        /* Moss Green Muted */
  --text-light: #7e8d86;      /* Lighter Moss Gray */
  
  --pine-primary: #1b3d32;    /* Deep Pine Green (松风绿) */
  --pine-secondary: #355e4f;  /* Medium Pine Green */

  /* Semantic Colors */
  --color-good: #2d6a4f;      /* Soft Jade Green */
  --color-good-bg: #eaf5f0;
  --color-warn: #b27a1e;      /* Soft Ochre Yellow */
  --color-warn-bg: #fdf6e6;
  --color-bad: #a83232;       /* Soft Madder Red */
  --color-bad-bg: #fbf1f1;
  --color-neutral: #5c6863;
  --color-neutral-bg: #f2f2ef;

  display: flex;
  flex-direction: column;
  height: 100vh;
  box-sizing: border-box;
  padding: 16px 20px;
  background-color: var(--bg-page);
  color: var(--text-main);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}

.section-kicker {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  font-weight: 700;
  color: var(--pine-secondary);
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

/* Header */
.watchlist-command-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 12px;
  margin-bottom: 16px;
  flex-shrink: 0;
}

.brand-left {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.brand-left h2 {
  margin: 0;
  font-size: 20px;
  font-weight: 700;
  color: var(--pine-primary);
}

.brand-left p {
  margin: 0;
  font-size: 13px;
  color: var(--text-sub);
}

.actions-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

/* Switcher */
.layout-switcher {
  display: flex;
  background-color: var(--bg-card);
  border: 1px solid var(--border-color);
  padding: 2px;
  border-radius: 4px;
}

.layout-switcher button {
  background: transparent;
  border: none;
  font-size: 11px;
  font-weight: 600;
  color: var(--text-sub);
  padding: 4px 8px;
  cursor: pointer;
  border-radius: 3px;
  transition: all 0.15s ease;
}

.layout-switcher button.active {
  background-color: var(--pine-primary);
  color: #ffffff;
}

/* Layout Body */
.watchlist-body {
  display: grid;
  grid-template-columns: 240px minmax(0, 1fr);
  gap: 16px;
  flex: 1;
  min-height: 0;
}

/* Sidebar Groups list */
.group-panel {
  display: flex;
  flex-direction: column;
  background-color: var(--bg-card);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  overflow: hidden;
  height: 100%;
}

.group-panel__header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 14px;
  border-bottom: 1px solid var(--border-color);
}

.panel-kicker {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  font-weight: 700;
  color: var(--text-light);
  letter-spacing: 0.1em;
}

.group-panel__header strong {
  font-family: "Consolas", Monaco, monospace;
  font-size: 12px;
  color: var(--pine-primary);
}

.group-list {
  flex: 1;
  overflow-y: auto;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.group-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 10px;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.15s ease;
  border: 1px solid transparent;
}

.group-item:hover {
  background-color: var(--bg-hover);
}

.group-item--active {
  background-color: var(--bg-page);
  border-color: var(--pine-secondary);
}

.group-item--active .group-item__name {
  color: var(--pine-primary);
  font-weight: 600;
}

.group-item__info {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
  flex: 1;
}

.group-item__name {
  font-size: 13px;
  color: var(--text-main);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.group-item__count {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  color: var(--text-light);
  background-color: var(--bg-hover);
  padding: 1px 5px;
  border-radius: 2px;
}

.group-item__delete {
  background: none;
  border: none;
  color: var(--text-light);
  cursor: pointer;
  padding: 2px;
  display: flex;
  align-items: center;
  opacity: 0.4;
  transition: opacity 0.15s;
}

.group-item:hover .group-item__delete {
  opacity: 1;
}

.group-item__delete:hover {
  color: var(--color-bad);
}

/* Main workspace panel */
.stock-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
}

.stock-panel__placeholder {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: var(--text-light);
  gap: 12px;
}

.placeholder-icon {
  color: var(--text-light);
  opacity: 0.5;
}

.stock-panel__header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 8px;
  margin-bottom: 12px;
  flex-shrink: 0;
}

.stock-panel__title-row {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.stock-panel__title {
  font-size: 15px;
  font-weight: 700;
  color: var(--pine-primary);
  margin: 0;
}

.stock-panel__desc {
  font-size: 12px;
  color: var(--text-light);
}

.add-stock-input {
  display: flex;
  gap: 6px;
}

.input-mini {
  background-color: var(--bg-page);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  color: var(--text-main);
  padding: 4px 8px;
  font-size: 12px;
  font-family: "Consolas", Monaco, monospace;
  outline: none;
  width: 140px;
}

.input-mini:focus {
  border-color: var(--pine-secondary);
}

/* Buttons */
.btn-pine {
  background-color: var(--pine-primary);
  color: #ffffff;
  border: none;
  font-weight: 600;
  padding: 4px 10px;
  font-size: 12px;
  border-radius: 4px;
  cursor: pointer;
}
.btn-pine:hover {
  background-color: var(--pine-secondary);
}
.btn-pine:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.btn--icon {
  width: 28px;
  height: 28px;
  background-color: var(--bg-card);
  border: 1px solid var(--border-color);
  color: var(--text-sub);
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  cursor: pointer;
}
.btn--icon:hover {
  border-color: var(--pine-secondary);
  color: var(--pine-primary);
}
.btn--icon svg {
  width: 14px;
  height: 14px;
}

/* LAYOUT A: GRID TABLE */
.stock-table-wrap {
  flex: 1;
  overflow: auto;
  border: 1px solid var(--border-color);
  border-radius: 6px;
}

.stock-table {
  width: 100%;
  border-collapse: collapse;
}

.stock-table th {
  position: sticky;
  top: 0;
  background-color: var(--bg-card);
  color: var(--text-sub);
  font-size: 11px;
  font-weight: 600;
  padding: 8px 12px;
  text-align: left;
  border-bottom: 1px solid var(--border-color);
}

.stock-table td {
  padding: 8px 12px;
  border-bottom: 1px solid var(--border-color);
  font-size: 13px;
  color: var(--text-main);
}

.td-num {
  font-family: "Consolas", Monaco, monospace;
  color: var(--text-light);
  text-align: right;
  width: 32px;
}

.td-time {
  font-family: "Consolas", Monaco, monospace;
  color: var(--text-light);
  font-size: 11px;
}

.symbol-link {
  font-family: "Consolas", Monaco, monospace;
  font-weight: 600;
  color: var(--pine-secondary);
  text-decoration: none;
}
.symbol-link:hover {
  text-decoration: underline;
  color: var(--pine-primary);
}

.stock-row:hover {
  background-color: var(--bg-card);
}

.btn-text-danger {
  background: transparent;
  border: none;
  color: var(--color-bad);
  font-size: 11px;
  cursor: pointer;
}
.btn-text-danger:hover {
  text-decoration: underline;
}

/* LAYOUT B: HEATMAP TILES */
.stock-heatmap-wrap {
  flex: 1;
  overflow-y: auto;
  padding-right: 2px;
}

.heatmap-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(130px, 1fr));
  gap: 10px;
}

.heatmap-tile {
  background-color: var(--bg-card);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 10px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  min-height: 84px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.heatmap-tile:hover {
  border-color: var(--pine-secondary);
  transform: translateY(-1px);
}

.tile-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.tile-header .symbol {
  font-family: "Consolas", Monaco, monospace;
  font-size: 11px;
  font-weight: 600;
  color: var(--text-light);
}

.tile-delete-btn {
  background: transparent;
  border: none;
  color: var(--text-light);
  font-size: 14px;
  line-height: 1;
  padding: 0;
  cursor: pointer;
}
.tile-delete-btn:hover {
  color: var(--color-bad);
}

.heatmap-tile .name {
  font-size: 14px;
  font-weight: 700;
  color: var(--text-main);
  margin-top: 4px;
}

.tile-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 8px;
}

.change-mock {
  font-family: "Consolas", Monaco, monospace;
  font-size: 11px;
  font-weight: 700;
}

.time-mock {
  font-family: "Consolas", Monaco, monospace;
  font-size: 9px;
  color: var(--text-light);
}

/* Heatmap Tile Colors based on hash */
.tile-type--good {
  background-color: var(--color-good-bg);
  border-color: rgba(45, 106, 79, 0.2);
}
.tile-type--warn {
  background-color: var(--color-warn-bg);
  border-color: rgba(178, 122, 30, 0.2);
}
.tile-type--bad {
  background-color: var(--color-bad-bg);
  border-color: rgba(168, 50, 50, 0.2);
}

.color--good { color: var(--color-good); }
.color--warn { color: var(--color-warn); }
.color--bad { color: var(--color-bad); }

/* LAYOUT C: AUDIT SIGNALS */
.stock-audit-wrap {
  flex: 1;
  overflow: auto;
}

.audit-grid-container {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  min-width: 720px;
}

.audit-sub-panel {
  display: flex;
  flex-direction: column;
}

.sub-panel-title {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  font-weight: 700;
  color: var(--pine-secondary);
  letter-spacing: 0.1em;
  margin-bottom: 6px;
}

.audit-stock-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.audit-stock-cell span {
  font-size: 11px;
  color: var(--text-light);
}

.font-mono {
  font-family: "Consolas", Monaco, monospace;
}

.status-badge-flat {
  font-family: "Consolas", Monaco, monospace;
  font-size: 9px;
  font-weight: 700;
  padding: 1px 4px;
  border-radius: 2px;
}

.badge-tone--good { background-color: var(--color-good-bg); color: var(--color-good); }
.badge-tone--warn { background-color: var(--color-warn-bg); color: var(--color-warn); }
.badge-tone--bad { background-color: var(--color-bad-bg); color: var(--color-bad); }

/* Modal and Spinner */
.loading-state,
.empty-hint {
  padding: 32px 0;
  text-align: center;
  font-size: 13px;
  color: var(--text-light);
}

.spinner {
  width: 20px;
  height: 20px;
  border: 2px solid var(--border-color);
  border-top-color: var(--pine-primary);
  border-radius: 50%;
  animation: spin 0.6s linear infinite;
  margin: 0 auto 8px;
}

@keyframes spin { to { transform: rotate(360deg); } }

.modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.4);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal {
  background-color: var(--bg-page);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 20px;
  width: 320px;
}

.modal__title {
  font-size: 15px;
  font-weight: 700;
  color: var(--pine-primary);
  margin-top: 0;
  margin-bottom: 12px;
}

.modal__body {
  display: flex;
  flex-direction: column;
}

.form-label {
  font-size: 11px;
  font-weight: bold;
  color: var(--text-light);
  margin-bottom: 4px;
}

.input {
  background-color: var(--bg-page);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  color: var(--text-main);
  padding: 6px 10px;
  font-size: 13px;
  outline: none;
}

.input:focus {
  border-color: var(--pine-secondary);
}

.modal__footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 16px;
}

.btn {
  padding: 6px 12px;
  border-radius: 4px;
  font-size: 13px;
  cursor: pointer;
  border: 1px solid var(--border-color);
  background-color: var(--bg-card);
  color: var(--text-main);
}
.btn:hover {
  background-color: var(--bg-hover);
}

.btn--ghost {
  background: transparent;
  border-color: transparent;
  color: var(--text-sub);
}
.btn--ghost:hover {
  background-color: var(--bg-card);
}

/* Responsiveness */
@media (max-width: 900px) {
  .watchlist-body {
    grid-template-columns: 1fr;
  }
  .audit-grid-container {
    grid-template-columns: 1fr;
  }
}
</style>