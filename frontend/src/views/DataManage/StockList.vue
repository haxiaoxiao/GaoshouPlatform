<template>
  <div class="stock-list-container">
    <!-- 搜索和筛选区域 -->
    <el-card class="filter-card" shadow="never">
      <el-form :inline="true" class="filter-form">
        <el-form-item label="搜索">
          <el-input
            v-model="searchText"
            placeholder="股票代码或名称"
            clearable
            @keyup.enter="handleSearch"
            style="width: 200px"
          >
            <template #prefix>
              <el-icon><Search /></el-icon>
            </template>
          </el-input>
        </el-form-item>
        <el-form-item label="行业">
          <el-select
            v-model="selectedIndustry"
            placeholder="全部行业"
            clearable
            style="width: 180px"
            @change="handleFilterChange"
          >
            <el-option
              v-for="item in industries"
              :key="item.name"
              :label="`${item.name} (${item.count})`"
              :value="item.name"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="自选分组">
          <el-select
            v-model="selectedGroup"
            placeholder="全部分组"
            clearable
            style="width: 150px"
            @change="handleFilterChange"
          >
            <el-option
              v-for="group in watchlistGroups"
              :key="group.id"
              :label="group.name"
              :value="group.id"
            />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="handleSearch">
            <el-icon><Search /></el-icon>
            搜索
          </el-button>
          <el-button @click="handleReset">
            <el-icon><Refresh /></el-icon>
            重置
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- 股票列表 -->
    <el-card class="table-card" shadow="never">
      <template #header>
        <div class="card-header">
          <span>股票列表 (共 {{ total }} 只)</span>
        </div>
      </template>

      <el-table
        v-loading="loading"
        :data="stockList"
        stripe
        style="width: 100%"
        @row-click="handleRowClick"
      >
        <el-table-column prop="symbol" label="代码" width="100" />
        <el-table-column prop="name" label="名称" width="120" />
        <el-table-column prop="industry" label="行业" width="120">
          <template #default="{ row }">
            <el-tag v-if="row.industry" size="small" type="info">
              {{ row.industry }}
            </el-tag>
            <span v-else class="text-muted">-</span>
          </template>
        </el-table-column>
        <el-table-column prop="market" label="市场" width="80">
          <template #default="{ row }">
            <el-tag size="small" :type="row.market === 'SH' ? 'danger' : 'success'">
              {{ row.market }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="list_date" label="上市日期" width="110">
          <template #default="{ row }">
            {{ row.list_date || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="is_active" label="状态" width="80">
          <template #default="{ row }">
            <el-tag :type="row.is_active ? 'success' : 'danger'" size="small">
              {{ row.is_active ? '正常' : '停牌' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="200" fixed="right">
          <template #default="{ row }">
            <el-dropdown trigger="click" @command="(cmd: number) => handleAddToWatchlist(row.id, cmd)">
              <el-button type="primary" link>
                <el-icon><Plus /></el-icon>
                加入自选
              </el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item
                    v-for="group in watchlistGroups"
                    :key="group.id"
                    :command="group.id"
                  >
                    {{ group.name }}
                  </el-dropdown-item>
                  <el-dropdown-item v-if="watchlistGroups.length === 0" disabled>
                    暂无分组
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
            <el-button type="primary" link @click.stop="handleViewDetail(row)">
              K线
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <!-- 分页 -->
      <div class="pagination-container">
        <el-pagination
          v-model:current-page="currentPage"
          v-model:page-size="pageSize"
          :page-sizes="[10, 20, 50, 100]"
          :total="total"
          layout="total, sizes, prev, pager, next, jumper"
          @size-change="handleSizeChange"
          @current-change="handlePageChange"
        />
      </div>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { Search, Refresh, Plus } from '@element-plus/icons-vue'
import {
  stockApi,
  industryApi,
  watchlistApi,
  type Stock,
  type Industry,
  type WatchlistGroup,
} from '@/api/data'

// Router
const router = useRouter()

// 数据状态
const loading = ref(false)
const stockList = ref<Stock[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)

// 筛选状态
const searchText = ref('')
const selectedIndustry = ref<string>('')
const selectedGroup = ref<number | undefined>(undefined)
const industries = ref<Industry[]>([])
const watchlistGroups = ref<WatchlistGroup[]>([])

// 加载股票列表
const loadStocks = async () => {
  loading.value = true
  try {
    const params: Record<string, unknown> = {
      page: currentPage.value,
      page_size: pageSize.value,
    }
    if (searchText.value) {
      params.search = searchText.value
    }
    if (selectedIndustry.value) {
      params.industry = selectedIndustry.value
    }
    if (selectedGroup.value) {
      params.group_id = selectedGroup.value
    }

    const response = await stockApi.getList(params as Parameters<typeof stockApi.getList>[0])
    stockList.value = response.items
    total.value = response.total
  } catch {
    ElMessage.error('加载股票列表失败')
  } finally {
    loading.value = false
  }
}

// 加载行业列表
const loadIndustries = async () => {
  try {
    industries.value = await industryApi.getList()
  } catch {
    console.error('加载行业列表失败')
  }
}

// 加载自选股分组
const loadWatchlistGroups = async () => {
  try {
    watchlistGroups.value = await watchlistApi.getGroups()
  } catch {
    console.error('加载自选股分组失败')
  }
}

// 搜索
const handleSearch = () => {
  currentPage.value = 1
  loadStocks()
}

// 重置筛选
const handleReset = () => {
  searchText.value = ''
  selectedIndustry.value = ''
  selectedGroup.value = undefined
  currentPage.value = 1
  loadStocks()
}

// 筛选条件变化
const handleFilterChange = () => {
  currentPage.value = 1
  loadStocks()
}

// 分页变化
const handlePageChange = (page: number) => {
  currentPage.value = page
  loadStocks()
}

const handleSizeChange = (size: number) => {
  pageSize.value = size
  currentPage.value = 1
  loadStocks()
}

// 添加到自选
const handleAddToWatchlist = async (stockId: number, groupId: number) => {
  try {
    await watchlistApi.addStockToGroup(groupId, { stock_id: stockId })
    ElMessage.success('添加成功')
  } catch {
    ElMessage.error('添加失败')
  }
}

// 查看详情 - 跳转到详情页
const handleViewDetail = (stock: Stock) => {
  router.push(`/stock/${stock.symbol}`)
}

// 行点击 - 跳转到详情页
const handleRowClick = (row: Stock) => {
  handleViewDetail(row)
}

// 初始化
onMounted(() => {
  loadStocks()
  loadIndustries()
  loadWatchlistGroups()
})
</script>

<style scoped>
.stock-list-container {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.filter-card {
  margin-bottom: 0;
}

.filter-form {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.filter-form :deep(.el-form-item) {
  margin-bottom: 0;
  margin-right: 16px;
}

.table-card {
  flex: 1;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.pagination-container {
  display: flex;
  justify-content: flex-end;
  margin-top: 16px;
}

.text-muted {
  color: #909399;
}

/* 表格行可点击样式 */
:deep(.el-table__row) {
  cursor: pointer;
}

:deep(.el-table__row:hover) {
  background-color: #f5f7fa;
}
</style>
