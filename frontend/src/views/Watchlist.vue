<template>
  <div class="watchlist-page">
    <div class="watchlist-layout">
      <aside class="group-panel">
        <div class="group-panel__header">
          <h2 class="group-panel__title">自选分组</h2>
          <button class="btn btn--icon" @click="showCreateDialog = true" title="新建分组">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M12 5v14M5 12h14"/>
            </svg>
          </button>
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
          <div class="stock-panel__header">
            <div class="stock-panel__title-row">
              <h2 class="stock-panel__title">{{ currentGroup?.name || '自选股' }}</h2>
              <span v-if="currentGroup?.description" class="stock-panel__desc">{{ currentGroup.description }}</span>
            </div>
            <div class="stock-panel__actions">
              <div class="add-stock-input">
                <input
                  v-model="addSymbol"
                  type="text"
                  class="input"
                  placeholder="输入股票代码如 600051.SH"
                  @keyup.enter="handleAddStock"
                />
                <button class="btn btn--primary btn--small" @click="handleAddStock" :disabled="!addSymbol.trim()">添加</button>
              </div>
            </div>
          </div>

          <div v-if="loadingStocks" class="loading-state">
            <div class="spinner"></div>
            <span>加载中...</span>
          </div>

          <div v-else-if="stocks.length" class="stock-table-wrap">
            <table class="stock-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>股票代码</th>
                  <th>股票名称</th>
                  <th>添加时间</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(stock, idx) in stocks" :key="stock.id" class="stock-row">
                  <td class="td-num">{{ idx + 1 }}</td>
                  <td>
                    <router-link :to="`/stock/${stock.symbol}`" class="symbol-link">{{ stock.symbol }}</router-link>
                  </td>
                  <td>{{ stock.stock_name || '-' }}</td>
                  <td class="td-time">{{ formatTime(stock.added_at) }}</td>
                  <td>
                    <button class="btn btn--danger btn--small" @click="handleRemoveStock(stock.symbol)">移除</button>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div v-else class="empty-hint">
            该分组暂无股票，使用上方输入框添加
          </div>
        </template>
      </main>
    </div>

    <div v-if="showCreateDialog" class="modal-overlay" @click.self="showCreateDialog = false">
      <div class="modal">
        <h3 class="modal__title">新建自选股分组</h3>
        <div class="modal__body">
          <label class="form-label">分组名称</label>
          <input v-model="newGroupName" type="text" class="input" placeholder="例如：白马股、高股息..." @keyup.enter="handleCreateGroup" />
          <label class="form-label" style="margin-top: var(--space-3)">描述（可选）</label>
          <input v-model="newGroupDesc" type="text" class="input" placeholder="分组描述" />
        </div>
        <div class="modal__footer">
          <button class="btn btn--ghost" @click="showCreateDialog = false">取消</button>
          <button class="btn btn--primary" @click="handleCreateGroup" :disabled="!newGroupName.trim()">创建</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { watchlistApi, type WatchlistGroup, type WatchlistStock } from '@/api/data'
import { ElMessage, ElMessageBox } from 'element-plus'

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

onMounted(loadGroups)
</script>

<style scoped>
.watchlist-page {
  height: 100%;
}

.watchlist-layout {
  display: flex;
  gap: var(--space-4);
  height: 100%;
}

.group-panel {
  width: 280px;
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  overflow: hidden;
}

.group-panel__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--space-4) var(--space-4) var(--space-3);
  border-bottom: 1px solid var(--border-subtle);
}

.group-panel__title {
  font-size: var(--text-base);
  font-weight: 600;
  color: var(--text-bright);
  margin: 0;
}

.group-list {
  flex: 1;
  overflow-y: auto;
  padding: var(--space-2);
}

.group-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--space-3) var(--space-3);
  border-radius: var(--radius-md);
  cursor: pointer;
  transition: all var(--duration-fast) var(--ease-out);
  margin-bottom: 2px;
}

.group-item:hover {
  background: var(--bg-hover);
}

.group-item--active {
  background: linear-gradient(135deg, rgba(56, 189, 248, 0.12) 0%, rgba(167, 139, 250, 0.06) 100%);
  border: 1px solid rgba(56, 189, 248, 0.2);
}

.group-item--active .group-item__name {
  color: var(--accent-primary);
}

.group-item__info {
  display: flex;
  align-items: center;
  gap: var(--space-2);
  min-width: 0;
  flex: 1;
}

.group-item__name {
  font-size: var(--text-sm);
  font-weight: 500;
  color: var(--text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.group-item__count {
  font-size: var(--text-xs);
  color: var(--text-muted);
  background: var(--bg-elevated);
  padding: 1px 6px;
  border-radius: var(--radius-full);
  white-space: nowrap;
}

.group-item__delete {
  opacity: 0;
  background: none;
  border: none;
  color: var(--text-muted);
  cursor: pointer;
  padding: 2px;
  display: flex;
  align-items: center;
  transition: opacity var(--duration-fast);
}

.group-item:hover .group-item__delete {
  opacity: 1;
}

.group-item__delete:hover {
  color: var(--accent-danger);
}

.stock-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.stock-panel__placeholder {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--space-3);
  color: var(--text-muted);
}

.placeholder-icon {
  color: var(--text-ghost);
}

.stock-panel__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-4);
  padding-bottom: var(--space-4);
  border-bottom: 1px solid var(--border-subtle);
  margin-bottom: var(--space-4);
}

.stock-panel__title-row {
  display: flex;
  align-items: baseline;
  gap: var(--space-3);
  min-width: 0;
}

.stock-panel__title {
  font-size: var(--text-lg);
  font-weight: 600;
  color: var(--text-bright);
  margin: 0;
  white-space: nowrap;
}

.stock-panel__desc {
  font-size: var(--text-sm);
  color: var(--text-muted);
}

.stock-panel__actions {
  flex-shrink: 0;
}

.add-stock-input {
  display: flex;
  gap: var(--space-2);
}

.input {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  padding: var(--space-2) var(--space-3);
  font-size: var(--text-sm);
  font-family: var(--font-display);
  outline: none;
  transition: border-color var(--duration-fast), box-shadow var(--duration-fast);
}

.input:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.1);
}

.input::placeholder {
  color: var(--text-muted);
}

.btn {
  padding: var(--space-2) var(--space-4);
  border-radius: var(--radius-md);
  font-size: var(--text-sm);
  font-weight: 500;
  cursor: pointer;
  border: 1px solid transparent;
  transition: all var(--duration-fast) var(--ease-out);
  white-space: nowrap;
}

.btn--primary {
  background: var(--accent-primary);
  color: var(--bg-void);
  border-color: var(--accent-primary);
}

.btn--primary:hover { opacity: 0.85; }
.btn--primary:disabled { opacity: 0.4; cursor: not-allowed; }

.btn--ghost {
  background: transparent;
  color: var(--text-secondary);
  border-color: var(--border-default);
}

.btn--ghost:hover { background: var(--bg-hover); color: var(--text-primary); }

.btn--danger {
  background: transparent;
  color: var(--accent-danger);
  border-color: rgba(239, 68, 68, 0.3);
}

.btn--danger:hover { background: rgba(239, 68, 68, 0.1); }

.btn--small {
  padding: var(--space-1) var(--space-2);
  font-size: var(--text-xs);
}

.btn--icon {
  width: 32px;
  height: 32px;
  padding: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-elevated);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-md);
  color: var(--text-secondary);
  cursor: pointer;
  transition: all var(--duration-fast);
}

.btn--icon:hover {
  background: var(--accent-primary);
  color: var(--bg-void);
  border-color: var(--accent-primary);
}

.btn--icon svg {
  width: 16px;
  height: 16px;
}

.stock-table-wrap {
  flex: 1;
  overflow: auto;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
}

.stock-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--text-sm);
}

.stock-table th {
  position: sticky;
  top: 0;
  background: var(--bg-elevated);
  color: var(--text-muted);
  font-size: var(--text-xs);
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: var(--tracking-wider);
  padding: var(--space-3) var(--space-4);
  text-align: left;
  border-bottom: 1px solid var(--border-default);
  white-space: nowrap;
}

.stock-table td {
  padding: var(--space-2) var(--space-4);
  border-bottom: 1px solid var(--border-subtle);
  color: var(--text-primary);
}

.td-num {
  color: var(--text-ghost);
  font-family: var(--font-display);
  text-align: right;
  width: 40px;
}

.td-time {
  color: var(--text-muted);
  font-family: var(--font-display);
  font-size: var(--text-xs);
}

.symbol-link {
  color: var(--accent-primary);
  text-decoration: none;
  font-family: var(--font-display);
  font-weight: 500;
}

.symbol-link:hover {
  text-decoration: underline;
}

.stock-row:hover {
  background: rgba(56, 189, 248, 0.04);
}

.loading-state,
.empty-hint {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: var(--space-3);
  padding: var(--space-12) var(--space-4);
  color: var(--text-muted);
  font-size: var(--text-sm);
}

.spinner {
  width: 24px;
  height: 24px;
  border: 2px solid var(--border-default);
  border-top-color: var(--accent-primary);
  border-radius: 50%;
  animation: spin 0.6s linear infinite;
}

@keyframes spin { to { transform: rotate(360deg); } }

.modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.6);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  padding: var(--space-6);
  width: 400px;
  max-width: 90vw;
}

.modal__title {
  font-size: var(--text-lg);
  font-weight: 600;
  color: var(--text-bright);
  margin: 0 0 var(--space-4);
}

.modal__body {
  display: flex;
  flex-direction: column;
}

.form-label {
  font-size: var(--text-xs);
  font-weight: 600;
  color: var(--text-secondary);
  text-transform: uppercase;
  letter-spacing: var(--tracking-wider);
  margin-bottom: var(--space-1);
  display: block;
}

.modal__footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--space-3);
  margin-top: var(--space-5);
}
</style>