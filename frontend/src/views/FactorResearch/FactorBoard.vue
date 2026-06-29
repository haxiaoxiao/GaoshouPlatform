<template>
  <div class="factor-board">
    <div class="board-sticky">
      <div class="filter-bar">
        <div class="filter-title">
          <div>
            <span class="panel-kicker">BOARD FILTERS</span>
            <strong>研究样本与组合设置</strong>
          </div>
          <span class="filter-count">{{ activeFilterSummary }}</span>
        </div>
        <div class="filter-grid">
          <div class="filter-cluster filter-cluster--wide">
            <div class="filter-field filter-field--keyword">
              <span class="filter-label">因子</span>
              <el-input
                v-model="filters.factor_keyword"
                clearable
                size="small"
                placeholder="搜索名称 / 来源 / 分组"
                class="keyword-input"
              />
            </div>

            <div class="filter-field">
              <span class="filter-label">分类</span>
              <el-select
                v-model="filters.categories"
                multiple
                collapse-tags
                collapse-tags-tooltip
                clearable
                size="small"
                placeholder="全部分类"
                class="filter-select"
              >
                <el-option v-for="cat in categoryOptions" :key="cat.value" :label="cat.label" :value="cat.value" />
              </el-select>
            </div>

            <div class="filter-field filter-field--wide">
              <span class="filter-label">因子组</span>
              <el-select
                v-model="filters.factor_groups"
                multiple
                collapse-tags
                collapse-tags-tooltip
                clearable
                size="small"
                placeholder="全部因子组"
                class="filter-select filter-select--wide"
                @visible-change="refreshFactorGroupsWhenOpen"
              >
                <el-option
                  v-for="group in factorGroups"
                  :key="group.name"
                  :label="group.display_name"
                  :value="group.name"
                />
              </el-select>
            </div>
          </div>

        <div v-if="hasCombinationContext" class="param-hash-panel combo-panel filter-cluster--wide">
          <div class="param-hash-panel__head">
            <strong>已计算组合</strong>
            <span>从成功因子研究记录中筛选；点击参数逐步收窄，点击组合行会回填看板设置。</span>
            <div class="combo-head-actions">
              <button v-if="combinationGroups.length" type="button" class="combo-apply-best" @click="applyCombinationGroup(combinationGroups[0])">
                应用 {{ comboCoverageText(combinationGroups[0]) }}
              </button>
              <button v-if="Object.keys(combinationSelection).length" type="button" class="combo-reset" @click="resetCombinationSelection">清空筛选</button>
            </div>
          </div>
          <div v-loading="combinationLoading" class="combo-panel-body">
            <div v-if="combinationResult" class="combo-facets">
              <div v-for="facet in combinationFacetFields" :key="facet.field" class="combo-facet-row">
                <div class="combo-facet-label">{{ facet.label }}</div>
                <div class="combo-facet-options">
                  <button
                    v-for="option in (combinationFacets[facet.field] || []).slice(0, 10)"
                    :key="`${option.field}_${String(option.value)}`"
                    type="button"
                    class="param-hash-chip combo-chip"
                    :class="{ 'is-active': isCombinationFacetActive(option) }"
                    @click="selectCombinationFacet(option)"
                  >
                    <span>{{ option.field === 'factor_value_params_hash' ? shortHash(String(option.value)) : option.label }}</span>
                    <small>{{ comboCoverageText(option) }} / {{ option.count }}条</small>
                  </button>
                </div>
              </div>
            </div>
            <div v-if="combinationGroups.length" class="combo-group-list">
              <button
                v-for="group in combinationGroups.slice(0, 8)"
                :key="group.combo_id"
                type="button"
                class="combo-group-card"
                :class="{ 'is-active': selectedCombinationId === group.combo_id }"
                @click="applyCombinationGroup(group)"
              >
                <strong>{{ combinationGroupTitle(group) }}</strong>
                <span>{{ comboSettingString(group.settings, 'stock_pool_value') }} / {{ comboSettingString(group.settings, 'portfolio_type') }} / {{ comboSettingString(group.settings, 'pool_membership_mode') }}</span>
              </button>
            </div>
            <div v-if="combinationResult && !combinationGroups.length" class="param-hash-empty">没有匹配的已计算组合</div>
          </div>
        </div>

          <div class="filter-cluster">
            <div class="filter-field">
              <span class="filter-label">股票池</span>
              <el-select v-model="filters.stock_pool" size="small" class="filter-select filter-select--pool" filterable>
                <el-option-group label="指数股票池">
                  <el-option
                    v-for="item in poolEnabledIndexes"
                    :key="item.symbol"
                    :label="`${item.display_name} ${item.symbol}`"
                    :value="item.stock_pool_alias || item.symbol"
                  />
                </el-option-group>
                <el-option-group v-if="watchlistGroups.length" label="自选股分组">
                  <el-option
                    v-for="g in watchlistGroups"
                    :key="'wl_'+g.id"
                    :label="g.name"
                    :value="'watchlist_'+g.id"
                  />
                </el-option-group>
              </el-select>
            </div>

            <div class="filter-field filter-field--period">
              <span class="filter-label">回测周期</span>
              <el-radio-group v-model="filters.period" size="small" @change="clearExplicitDateRange">
                <el-radio-button value="3m">近3个月</el-radio-button>
                <el-radio-button value="1y">近1年</el-radio-button>
                <el-radio-button value="3y">近3年</el-radio-button>
                <el-radio-button value="10y">近10年</el-radio-button>
              </el-radio-group>
              <span v-if="filters.start_date && filters.end_date" class="combo-date-range">
                {{ filters.start_date }} ~ {{ filters.end_date }}
              </span>
            </div>
          </div>

          <div class="filter-cluster">
            <div class="filter-field">
              <span class="filter-label">成分口径</span>
              <el-select v-model="filters.pool_membership_mode" size="small" class="filter-select filter-select--membership">
                <el-option label="Static latest" value="static_latest" />
                <el-option label="Point-in-time" value="point_in_time" />
                <el-option label="Union" value="union" />
              </el-select>
            </div>

            <div class="filter-field filter-field--compact">
              <span class="filter-label">分组</span>
              <el-input-number v-model="filters.group_count" :min="2" :max="20" size="small" controls-position="right" class="group-count-input" />
            </div>

            <div class="filter-field filter-field--compact">
              <span class="filter-label">方向</span>
              <el-select v-model="filters.direction" size="small" class="filter-select filter-select--direction">
                <el-option label="降序" value="desc" />
                <el-option label="升序" value="asc" />
              </el-select>
            </div>
          </div>

          <div class="filter-cluster">
            <div class="filter-field filter-field--portfolio">
              <span class="filter-label help-label">
                组合构建
                <el-tooltip placement="top" effect="dark">
                  <template #content>
                    <div class="tooltip-content">
                      纯多头组合：只买入目标分位股票。<br />
                      多空组合 I：最高分位做多、最低分位做空。<br />
                      多空组合 II：最高分位相对基准增强，最低分位作为对冲腿。
                    </div>
                  </template>
                  <el-icon><QuestionFilled /></el-icon>
                </el-tooltip>
              </span>
              <el-radio-group v-model="filters.portfolio_type" size="small">
                <el-radio-button value="long_only">纯多头组合</el-radio-button>
                <el-radio-button value="long_short_i">多空组合 I</el-radio-button>
                <el-radio-button value="long_short_ii">多空组合 II</el-radio-button>
              </el-radio-group>
            </div>

            <div class="filter-field filter-field--checks">
              <span class="filter-label">成本 / 过滤</span>
              <div class="fee-toggle-group">
                <el-checkbox v-model="filters.use_commission_stamp" size="small">佣金+印花税</el-checkbox>
                <el-checkbox v-model="filters.use_slippage" size="small">滑点</el-checkbox>
                <el-switch v-model="filters.filter_limit_up" size="small" active-text="涨停" />
                <el-switch v-model="filters.filter_limit_down" size="small" active-text="跌停" />
              </div>
            </div>
            <el-radio-group v-if="false" v-model="filters.fee_config" size="small" class="legacy-fee-config">
              <el-radio-button value="none">无</el-radio-button>
              <el-radio-button value="commission_stamp">3‰佣金+1‰印花税</el-radio-button>
              <el-radio-button value="commission_stamp_slippage">+1‰滑点</el-radio-button>
            </el-radio-group>
            <div v-if="false" class="cost-controls cost-controls--compact">
              <span>佣金</span>
              <el-input-number v-model="filters.fee_rate" :min="0" :max="0.05" :step="0.0005" :precision="4" size="small" controls-position="right" />
              <span>印花税</span>
              <el-input-number v-model="filters.stamp_tax_rate" :min="0" :max="0.05" :step="0.0005" :precision="4" size="small" controls-position="right" />
              <span>过户费</span>
              <el-input-number v-model="filters.transfer_fee_rate" :min="0" :max="0.01" :step="0.00001" :precision="5" size="small" controls-position="right" />
              <span>滑点</span>
              <el-input-number v-model="filters.slippage" :min="0" :max="0.05" :step="0.0005" :precision="4" size="small" controls-position="right" />
            </div>
          </div>

          <div class="filter-cluster">
            <div class="filter-field">
              <span class="filter-label">因子预处理</span>
              <el-select v-model="filters.outlier_handling" size="small" class="filter-select filter-select--preprocess">
                <el-option label="不去极值" value="none" />
                <el-option label="Winsor 2.5%" value="winsorize" />
              </el-select>
            </div>
            <div class="filter-field filter-field--checks">
              <span class="filter-label">标准化</span>
              <div class="fee-toggle-group">
                <el-checkbox v-model="filters.industry_neutralization" size="small">行业中性化</el-checkbox>
                <el-checkbox v-model="filters.standardize" size="small">标准化</el-checkbox>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="toolbar">
        <div class="board-title">
          <strong>因子看板</strong>
          <span>展示保存因子的研究表现，因子值缓存负责数据覆盖和预计算。</span>
        </div>
        <div class="toolbar-actions">
          <el-button @click="router.push('/factor')">预计算缓存</el-button>
          <el-button @click="openBatchDialog">批量计算</el-button>
          <el-button type="primary" :icon="Plus" @click="showCreateDialog = true">新建因子</el-button>
        </div>
      </div>
    </div>

    <section class="board-table-shell">
      <el-table class="board-table" :data="rows" stripe v-loading="loading" @sort-change="handleSortChange" height="100%">
        <el-table-column prop="factor_name" label="因子" min-width="220" sortable="custom" show-overflow-tooltip>
          <template #default="{ row }">
            <div class="factor-name-cell">
              <strong>{{ row.display_name || row.factor_name }}</strong>
              <span>{{ row.factor_name }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="factor_group_display_name" label="因子组" min-width="150" show-overflow-tooltip>
          <template #default="{ row }">{{ row.factor_group_display_name || '未分组' }}</template>
        </el-table-column>
        <el-table-column prop="category" label="分类" width="130" show-overflow-tooltip />
        <el-table-column prop="source" label="来源" width="130" show-overflow-tooltip />
        <el-table-column label="缓存覆盖" min-width="190">
          <template #default="{ row }">
            <div class="coverage-cell">
              <el-tag :type="coverageTagType(row.coverage_status)" effect="plain" size="small">
                {{ coverageStatusText(row.coverage_status) }}
              </el-tag>
              <span>{{ coverageRangeText(row) }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="latest_ic_mean" label="最新IC均值" width="120" sortable="custom" align="right">
          <template #default="{ row }">
            <span :class="(row.latest_ic_mean ?? row.ic_mean) >= 0 ? 'positive' : 'negative'">{{ formatNumber(row.latest_ic_mean ?? row.ic_mean) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="latest_icir" label="最新ICIR" width="110" sortable="custom" align="right">
          <template #default="{ row }">{{ formatNumber(row.latest_icir ?? row.ir) }}</template>
        </el-table-column>
        <el-table-column prop="latest_active_symbol_count" label="有效股票" width="100" align="right">
          <template #default="{ row }">{{ row.latest_active_symbol_count ?? '-' }}</template>
        </el-table-column>
        <el-table-column prop="latest_long_short_return" label="多空收益" width="120" sortable="custom" align="right">
          <template #default="{ row }">
            <span :class="(row.latest_long_short_return ?? 0) >= 0 ? 'positive' : 'negative'">
              {{ formatPercent(row.latest_long_short_return) }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="latest_max_drawdown" label="最大回撤" width="120" sortable="custom" align="right">
          <template #default="{ row }">{{ formatPercent(row.latest_max_drawdown) }}</template>
        </el-table-column>
        <el-table-column prop="latest_turnover" label="换手率" width="110" sortable="custom" align="right">
          <template #default="{ row }">{{ formatPercent(row.latest_turnover) }}</template>
        </el-table-column>
        <el-table-column prop="latest_run_at" label="最近计算" width="130" show-overflow-tooltip>
          <template #default="{ row }">{{ formatDate(row.latest_run_at) }}</template>
        </el-table-column>
        <el-table-column label="操作" width="100" fixed="right">
          <template #default="{ row }">
            <el-button class="detail-action" size="small" type="primary" plain @click="goToDetail(row)">详情</el-button>
          </template>
        </el-table-column>
      </el-table>
    </section>

    <el-pagination
      class="board-pagination"
      v-model:current-page="filters.page"
      v-model:page-size="filters.page_size"
      :total="total"
      :page-sizes="[10, 20, 50]"
      layout="total, sizes, prev, pager, next"
      @change="fetchBoard"
    />

    <el-dialog v-model="showBatchDialog" title="批量计算因子研究" width="720px">
      <el-form label-width="96px" class="batch-form">
        <el-form-item label="因子组">
          <el-select
            v-model="batchForm.factor_group"
            placeholder="选择因子组"
            filterable
            style="width:100%"
            @visible-change="refreshFactorGroupsWhenOpen"
            @change="loadCombinations"
          >
            <el-option
              v-for="group in factorGroups"
              :key="group.name"
              :label="`${group.display_name} (${group.factor_names.length})`"
              :value="group.name"
            />
          </el-select>
        </el-form-item>
        <div v-if="hasCombinationContext" class="param-hash-panel param-hash-panel--dialog combo-panel">
          <div class="param-hash-panel__head">
            <strong>本组已计算组合</strong>
            <span>点击组合行会同步批量计算参数和 hash 映射。</span>
            <div class="combo-head-actions">
              <button v-if="combinationGroups.length" type="button" class="combo-apply-best" @click="applyCombinationGroup(combinationGroups[0])">
                应用 {{ comboCoverageText(combinationGroups[0]) }}
              </button>
            </div>
          </div>
          <div v-loading="combinationLoading" class="combo-panel-body">
            <div class="combo-group-list">
              <button
                v-for="group in combinationGroups.slice(0, 8)"
                :key="group.combo_id"
                type="button"
                class="combo-group-card"
                :class="{ 'is-active': selectedCombinationId === group.combo_id }"
                @click="applyCombinationGroup(group)"
              >
                <strong>{{ combinationGroupTitle(group) }}</strong>
                <span>{{ comboCoverageText(group) }}</span>
              </button>
            </div>
          </div>
        </div>
        <div v-if="false && batchParamHashRows.length" class="param-hash-panel param-hash-panel--dialog">
          <div class="param-hash-panel__head">
            <strong>本组缓存参数 Hash</strong>
            <span>批量计算将按这里选中的 hash 读取缓存。</span>
          </div>
          <div class="param-hash-list" v-loading="paramHashLoading">
            <div v-for="row in batchParamHashRows" :key="row.factor_name" class="param-hash-row">
              <div class="param-hash-factor">
                <strong>{{ row.display_name }}</strong>
                <span>{{ row.factor_name }}</span>
              </div>
              <div v-if="row.options.length" class="param-hash-options">
                <button
                  v-for="option in row.options"
                  :key="option.params_hash"
                  type="button"
                  class="param-hash-chip"
                  :class="{ 'is-active': selectedFactorParamHashes[row.factor_name] === option.params_hash, 'is-default': option.is_default }"
                  :title="paramHashTitle(option)"
                  @click="selectFactorParamHash(row.factor_name, option.params_hash)"
                >
                  <span>{{ shortHash(option.params_hash) }}</span>
                  <small>{{ paramHashCoverageText(option) }}</small>
                </button>
              </div>
              <div v-else class="param-hash-empty">未发现缓存版本</div>
            </div>
          </div>
        </div>
        <el-form-item label="股票池">
          <el-select v-model="batchForm.stock_pool_value" filterable style="width:100%">
            <el-option-group label="指数股票池">
              <el-option
                v-for="item in poolEnabledIndexes"
                :key="item.symbol"
                :label="`${item.display_name} ${item.symbol}`"
                :value="item.stock_pool_alias || item.symbol"
              />
            </el-option-group>
            <el-option-group v-if="watchlistGroups.length" label="自选股分组">
              <el-option
                v-for="g in watchlistGroups"
                :key="'batch_wl_'+g.id"
                :label="g.name"
                :value="'watchlist_'+g.id"
              />
            </el-option-group>
          </el-select>
        </el-form-item>
        <el-form-item label="成分口径">
          <el-select v-model="batchForm.pool_membership_mode" style="width:100%">
            <el-option label="Static latest" value="static_latest" />
            <el-option label="Point-in-time" value="point_in_time" />
            <el-option label="Union" value="union" />
          </el-select>
        </el-form-item>
        <el-form-item label="基准">
          <el-select v-model="batchForm.benchmark_symbol" filterable style="width:100%">
            <el-option
              v-for="item in benchmarkOptions"
              :key="item.symbol"
              :label="`${item.display_name} ${item.symbol}`"
              :value="item.symbol"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="日期范围">
          <div class="date-range">
            <el-date-picker v-model="batchForm.start_date" value-format="YYYY-MM-DD" type="date" />
            <span>至</span>
            <el-date-picker v-model="batchForm.end_date" value-format="YYYY-MM-DD" type="date" />
          </div>
        </el-form-item>
        <el-form-item label="组合构建">
          <el-radio-group v-model="batchForm.portfolio_type">
            <el-radio-button value="long_only">纯多头</el-radio-button>
            <el-radio-button value="long_short_i">多空 I</el-radio-button>
            <el-radio-button value="long_short_ii">多空 II</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="分组数">
          <el-input-number v-model="batchForm.group_count" :min="2" :max="20" />
        </el-form-item>
        <el-form-item label="因子预处理">
          <div class="preprocess-controls">
            <el-select v-model="batchForm.outlier_handling" style="width:126px">
              <el-option label="不去极值" value="none" />
              <el-option label="Winsor 2.5%" value="winsorize" />
            </el-select>
            <el-checkbox v-model="batchForm.industry_neutralization">行业中性化</el-checkbox>
            <el-checkbox v-model="batchForm.standardize">标准化</el-checkbox>
          </div>
        </el-form-item>
        <el-form-item label="交易成本">
          <div class="fee-toggle-group">
            <el-checkbox v-model="batchForm.use_commission_stamp">佣金+印花税</el-checkbox>
            <el-checkbox v-model="batchForm.use_slippage">滑点</el-checkbox>
          </div>
          <div v-if="false" class="cost-controls">
            <span>佣金</span>
            <el-input-number v-model="batchForm.fee_rate" :min="0" :max="0.05" :step="0.0005" :precision="4" controls-position="right" />
            <span>印花税</span>
            <el-input-number v-model="batchForm.stamp_tax_rate" :min="0" :max="0.05" :step="0.0005" :precision="4" controls-position="right" />
            <span>过户费</span>
            <el-input-number v-model="batchForm.transfer_fee_rate" :min="0" :max="0.01" :step="0.00001" :precision="5" controls-position="right" />
            <span>滑点</span>
            <el-input-number v-model="batchForm.slippage" :min="0" :max="0.05" :step="0.0005" :precision="4" controls-position="right" />
          </div>
        </el-form-item>
      </el-form>
      <div v-if="batchResult.length" class="batch-result">
        <div v-for="item in batchResult" :key="item.factor_name" class="batch-result-row">
          <span>{{ item.factor_name }}</span>
          <el-tag :type="item.status === 'success' ? 'success' : 'danger'" size="small">{{ item.status }}</el-tag>
          <span v-if="item.status === 'success'" class="batch-result-metric">IC {{ formatNumber(item.ic_mean) }}</span>
          <span v-if="item.status === 'success'" class="batch-result-metric">ICIR {{ formatNumber(item.icir) }}</span>
          <span>{{ item.run_id || item.error_message }}</span>
        </div>
      </div>
      <template #footer>
        <el-button @click="showBatchDialog = false">关闭</el-button>
        <el-button type="primary" :loading="batchLoading" @click="submitBatchRun">开始计算</el-button>
      </template>
    </el-dialog>

    <FactorCreateDialog v-model:visible="showCreateDialog" :watchlist-groups="watchlistGroups" @created="handleFactorCreated" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, reactive, watch, onMounted } from 'vue'
import { Plus, QuestionFilled } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'
import { evaluationApi } from '@/api/factorResearch'
import { indexCatalogApi, watchlistApi } from '@/api/data'
import { factorValueApi, type FactorValueGroup, type FactorValueParamHash } from '@/api/factorValues'
import {
  factorResearchRunApi,
  type FactorResearchCombinationFacetOption,
  type FactorResearchCombinationGroup,
  type FactorResearchCombinationResult,
  type FactorResearchCombinationSelection,
} from '@/api/factorResearchRuns'
import type { BoardRow, BoardQuery } from '@/types/factor'
import type { IndexCatalogItem, WatchlistGroup } from '@/api/data'
import FactorCreateDialog from './FactorCreateDialog.vue'

const router = useRouter()

const categoryBuckets = [
  { value: 'custom', label: '自定义因子库', categories: ['custom'] },
  { value: 'small_cap_core', label: '小市值核心', categories: ['valuation', 'status', 'technical', 'liquidity'] },
  { value: 'ta_lib', label: 'TA-Lib', categories: ['ta_trend', 'ta_momentum', 'ta_volatility', 'ta_volume', 'ta_price', 'ta_regression', 'ta_pattern'] },
  { value: 'alpha101', label: 'Alpha101', categories: ['alpha101'] },
  {
    value: 'research',
    label: '海外研究因子',
    categories: ['research_quality', 'research_investment', 'research_risk', 'research_momentum', 'research_reversal', 'research_liquidity'],
  },
]

type BoardFilterState = BoardQuery & {
  use_commission_stamp: boolean
  use_slippage: boolean
  benchmark_symbol?: string
}

const filters = reactive<BoardFilterState>({
  categories: [],
  factor_groups: [],
  factor_keyword: '',
  stock_pool: 'zz500',
  period: '3y',
  start_date: null,
  end_date: null,
  portfolio_type: 'long_only',
  fee_config: 'none',
  use_commission_stamp: true,
  use_slippage: true,
  filter_limit_up: true,
  filter_limit_down: true,
  group_count: 5,
  direction: 'desc',
  pool_membership_mode: 'static_latest',
  outlier_handling: 'none',
  industry_neutralization: false,
  standardize: false,
  sort_by: 'ic_mean',
  sort_order: 'desc',
  page: 1,
  page_size: 20,
})

const rows = ref<BoardRow[]>([])
const total = ref(0)
const loading = ref(false)
let boardRequestSeq = 0
const showCreateDialog = ref(false)
const showBatchDialog = ref(false)
const batchLoading = ref(false)
const watchlistGroups = ref<WatchlistGroup[]>([])
const indexCatalog = ref<IndexCatalogItem[]>([])
const factorGroups = ref<FactorValueGroup[]>([])
const factorCatalogLoading = ref(false)
const factorParamHashes = ref<FactorValueParamHash[]>([])
const paramHashLoading = ref(false)
const selectedFactorParamHashes = reactive<Record<string, string>>({})
const combinationLoading = ref(false)
const combinationResult = ref<FactorResearchCombinationResult | null>(null)
const combinationSelection = reactive<FactorResearchCombinationSelection>({})
const selectedCombinationId = ref('')
const batchResult = ref<Array<{
  factor_name: string
  status: string
  run_id?: string | null
  error_message?: string | null
  summary?: Record<string, number | string> | null
  ic_mean?: number | null
  icir?: number | null
}>>([])
const poolEnabledIndexes = computed(() => indexCatalog.value.filter(item => item.pool_enabled))
const benchmarkOptions = computed(() =>
  indexCatalog.value.filter(item => item.benchmark_enabled && (item.common_benchmark || item.pool_enabled))
)
const categoryOptions = computed(() => categoryBuckets)
const rowsByFactorName = computed(() => new Map(rows.value.map(row => [row.factor_name, row])))
const factorGroupByName = computed(() => new Map(factorGroups.value.map(group => [group.name, group])))
const queryCategories = computed(() => {
  const selected = new Set(filters.categories || [])
  if (!selected.size) return undefined
  return categoryBuckets
    .filter(item => selected.has(item.value))
    .flatMap(item => item.categories)
})
const activeFilterSummary = computed(() => {
  const categoryCount = filters.categories?.length || 0
  const groupCount = filters.factor_groups?.length || 0
  const keywordLabel = filters.factor_keyword ? '已搜索' : '全部'
  return `${keywordLabel} 因子 / ${categoryCount || '全部'} 分类 / ${groupCount || '全部'} 组`
})
const filterParamHashFactorNames = computed(() => {
  const names = factorNamesForGroups(filters.factor_groups || [])
  if (names.length) return names
  const keyword = String(filters.factor_keyword || '').trim()
  if (!keyword) return []
  const exactRows = rows.value.filter(row => isExactFactorMatch(row, keyword))
  if (exactRows.length) return exactRows.map(row => row.factor_name)
  if (rows.value.length <= 20) return rows.value.map(row => row.factor_name)
  return []
})
const todayText = new Date().toISOString().slice(0, 10)
const batchForm = reactive({
  factor_group: '',
  stock_pool_value: 'zz500',
  benchmark_symbol: '000300.SH',
  start_date: '2023-01-01',
  end_date: todayText,
  portfolio_type: 'long_only' as 'long_only' | 'long_short_i' | 'long_short_ii',
  rebalance_period: 'monthly' as 'daily' | 'weekly' | 'monthly',
  use_commission_stamp: true,
  use_slippage: true,
  fee_rate: 0,
  stamp_tax_rate: 0,
  transfer_fee_rate: 0,
  slippage: 0,
  filter_limit_up: true,
  filter_limit_down: true,
  group_count: 5,
  direction: 'desc' as 'asc' | 'desc',
  pool_membership_mode: 'static_latest' as 'static_latest' | 'point_in_time' | 'union',
  outlier_handling: 'none' as 'none' | 'winsorize',
  industry_neutralization: false,
  standardize: false,
})
const batchParamHashFactorNames = computed(() => factorNamesForGroups(batchForm.factor_group ? [batchForm.factor_group] : []))
const activeParamHashFactorNames = computed(() => {
  const names = new Set<string>()
  filterParamHashFactorNames.value.forEach(name => names.add(name))
  if (showBatchDialog.value) {
    batchParamHashFactorNames.value.forEach(name => names.add(name))
  }
  return [...names]
})
const batchParamHashRows = computed(() => paramHashRowsForNames(batchParamHashFactorNames.value))
const boardCombinationFactorNames = computed(() => filterParamHashFactorNames.value)
const activeCombinationFactorNames = computed(() => (
  showBatchDialog.value ? batchParamHashFactorNames.value : boardCombinationFactorNames.value
))
const combinationGroups = computed(() => combinationResult.value?.combo_groups || [])
const combinationFacets = computed(() => combinationResult.value?.facets || {})
const hasCombinationContext = computed(() => activeCombinationFactorNames.value.length > 0)
const combinationFacetFields = [
  { field: 'benchmark_symbol', label: '基准' },
  { field: 'factor_value_params_hash', label: '参数 Hash' },
  { field: 'stock_pool_value', label: '股票池' },
  { field: 'date_range', label: '日期区间' },
  { field: 'portfolio_type', label: '组合构建' },
  { field: 'fee_profile', label: '交易成本' },
  { field: 'pool_membership_mode', label: '成分口径' },
  { field: 'group_count', label: '分组数' },
  { field: 'direction', label: '方向' },
  { field: 'outlier_handling', label: '去极值' },
  { field: 'industry_neutralization', label: '行业中性化' },
  { field: 'standardize', label: '标准化' },
] as const

function resolveDateRangeFromPeriod(period: string) {
  const end = new Date()
  const start = new Date(end)
  if (period === '3m') start.setMonth(start.getMonth() - 3)
  else if (period === '1y') start.setFullYear(start.getFullYear() - 1)
  else if (period === '3y') start.setFullYear(start.getFullYear() - 3)
  else if (period === '10y') start.setFullYear(start.getFullYear() - 10)
  return {
    start_date: start.toISOString().slice(0, 10),
    end_date: end.toISOString().slice(0, 10),
  }
}

async function loadWatchlistGroups() {
  try {
    watchlistGroups.value = await watchlistApi.getGroups()
  } catch {
    watchlistGroups.value = []
  }
}

async function loadIndexCatalog() {
  try {
    indexCatalog.value = await indexCatalogApi.list()
    if (!poolEnabledIndexes.value.some(item => (item.stock_pool_alias || item.symbol) === filters.stock_pool)) {
      filters.stock_pool = poolEnabledIndexes.value[0]?.stock_pool_alias || poolEnabledIndexes.value[0]?.symbol || 'zz500'
    }
  } catch {
    indexCatalog.value = []
  }
}

async function loadFactorCatalog() {
  if (factorCatalogLoading.value) return
  factorCatalogLoading.value = true
  try {
    const groups = await factorValueApi.groups()
    factorGroups.value = groups
    if (!batchForm.factor_group && groups.length) {
      batchForm.factor_group = groups[0].name
    } else if (batchForm.factor_group && !groups.some(group => group.name === batchForm.factor_group)) {
      batchForm.factor_group = groups[0]?.name || ''
    }
  } catch {
    factorGroups.value = []
  } finally {
    factorCatalogLoading.value = false
  }
}

async function refreshFactorGroupsWhenOpen(open: boolean) {
  if (!open) return
  await loadFactorCatalog()
}

function factorNamesForGroups(groupNames: string[]) {
  const names = new Set<string>()
  groupNames.forEach(groupName => {
    const group = factorGroupByName.value.get(groupName)
    group?.factor_names.forEach(name => names.add(name))
  })
  return [...names]
}

function isExactFactorMatch(row: BoardRow, keyword: string) {
  const normalized = keyword.trim().toLowerCase()
  return [row.factor_name, row.display_name || ''].some(value => String(value).toLowerCase() === normalized)
}

function paramHashRowsForNames(names: string[]) {
  const optionsByFactor = new Map<string, FactorValueParamHash[]>()
  factorParamHashes.value.forEach(option => {
    if (!optionsByFactor.has(option.factor_name)) {
      optionsByFactor.set(option.factor_name, [])
    }
    optionsByFactor.get(option.factor_name)?.push(option)
  })
  return names.map(name => ({
    factor_name: name,
    display_name: rowsByFactorName.value.get(name)?.display_name || name,
    options: optionsByFactor.get(name) || [],
  }))
}

function preferredParamHash(options: FactorValueParamHash[]) {
  return options.find(option => option.is_default)?.params_hash || options[0]?.params_hash || ''
}

function applyParamHashDefaults(names: string[]) {
  let changed = false
  const wanted = new Set(names)
  const optionMap = new Map<string, FactorValueParamHash[]>()
  factorParamHashes.value.forEach(option => {
    if (!optionMap.has(option.factor_name)) optionMap.set(option.factor_name, [])
    optionMap.get(option.factor_name)?.push(option)
  })
  names.forEach(name => {
    const options = optionMap.get(name) || []
    if (!options.length) {
      if (selectedFactorParamHashes[name]) {
        delete selectedFactorParamHashes[name]
        changed = true
      }
      return
    }
    const current = selectedFactorParamHashes[name]
    if (!current || !options.some(option => option.params_hash === current)) {
      selectedFactorParamHashes[name] = preferredParamHash(options)
      changed = true
    }
  })
  Object.keys(selectedFactorParamHashes).forEach(name => {
    if (!wanted.has(name) && !showBatchDialog.value) {
      delete selectedFactorParamHashes[name]
      changed = true
    }
  })
  return changed
}

async function loadParamHashOptions() {
  const names = activeParamHashFactorNames.value
  if (!names.length) {
    factorParamHashes.value = []
    return false
  }
  paramHashLoading.value = true
  try {
    const { start_date, end_date } = resolveDateRangeFromPeriod(filters.period || '3y')
    factorParamHashes.value = await factorValueApi.paramHashes({
      factor_names: names,
      start_date,
      end_date,
      limit_per_factor: 8,
    })
    return applyParamHashDefaults(names)
  } catch {
    factorParamHashes.value = []
    return false
  } finally {
    paramHashLoading.value = false
  }
}
void loadParamHashOptions

function selectFactorParamHash(factorName: string, paramsHash: string) {
  selectedFactorParamHashes[factorName] = paramsHash
  fetchBoard({ refreshCatalog: false, refreshParamHashes: false })
}

function selectedParamHashesForNames(names: string[]) {
  const selected: Record<string, string> = {}
  names.forEach(name => {
    const value = selectedFactorParamHashes[name]
    if (value) selected[name] = value
  })
  return selected
}

function shortHash(value: string) {
  return value ? value.slice(0, 8) : 'empty'
}

function paramHashCoverageText(option: FactorValueParamHash) {
  const rows = Number(option.total_rows || 0).toLocaleString()
  const range = option.min_date && option.max_date ? `${option.min_date}~${option.max_date}` : '无日期'
  return `${range} / ${rows}行`
}

function paramHashTitle(option: FactorValueParamHash) {
  const parts = [
    `hash: ${option.params_hash}`,
    option.is_default ? '默认参数' : '',
    option.as_of_time ? `as_of: ${option.as_of_time}` : '',
    paramHashCoverageText(option),
  ].filter(Boolean)
  return parts.join('\n')
}

function resolveDateRangeFromFilters() {
  if (filters.start_date && filters.end_date) {
    return { start_date: filters.start_date, end_date: filters.end_date }
  }
  return resolveDateRangeFromPeriod(filters.period || '3y')
}

function clearExplicitDateRange() {
  filters.start_date = null
  filters.end_date = null
  delete combinationSelection.date_range
  delete combinationSelection.start_date
  delete combinationSelection.end_date
}

function comboSelectionPayload() {
  return Object.fromEntries(
    Object.entries(combinationSelection).filter(([, value]) => value !== undefined && value !== null && value !== ''),
  ) as FactorResearchCombinationSelection
}

async function loadCombinations() {
  const names = activeCombinationFactorNames.value
  if (!names.length) {
    combinationResult.value = null
    selectedCombinationId.value = ''
    return
  }
  combinationLoading.value = true
  try {
    combinationResult.value = await factorResearchRunApi.combinations({
      factor_names: names,
      selection: comboSelectionPayload(),
      limit: 200,
    })
  } catch {
    combinationResult.value = null
  } finally {
    combinationLoading.value = false
  }
}

async function selectCombinationFacet(option: FactorResearchCombinationFacetOption) {
  const field = option.field as keyof FactorResearchCombinationSelection
  const current = combinationSelection[field]
  if (String(current ?? '') === String(option.value)) {
    delete combinationSelection[field]
  } else {
    combinationSelection[field] = option.value as never
  }
  await loadCombinations()
  if (combinationGroups.value.length === 1) {
    applyCombinationGroup(combinationGroups.value[0])
  }
}

function isCombinationFacetActive(option: FactorResearchCombinationFacetOption) {
  return String(combinationSelection[option.field as keyof FactorResearchCombinationSelection] ?? '') === String(option.value)
}

function resetCombinationSelection() {
  Object.keys(combinationSelection).forEach(key => {
    delete combinationSelection[key as keyof FactorResearchCombinationSelection]
  })
  selectedCombinationId.value = ''
  loadCombinations()
}

function comboCoverageText(group: FactorResearchCombinationGroup | FactorResearchCombinationFacetOption) {
  return `覆盖 ${group.covered_factor_count}/${group.total_factor_count}`
}

function comboSettingString(settings: Record<string, unknown>, key: string) {
  const value = settings[key]
  return value === undefined || value === null ? '' : String(value)
}

function comboBooleanSetting(settings: Record<string, unknown>, key: string, fallback: boolean) {
  const value = settings[key]
  if (value === undefined || value === null || value === '') return fallback
  if (typeof value === 'boolean') return value
  return String(value).toLowerCase() === 'true'
}

function comboNumberSetting(settings: Record<string, unknown>, key: string, fallback: number) {
  const value = Number(settings[key])
  return Number.isFinite(value) ? value : fallback
}

function combinationGroupTitle(group: FactorResearchCombinationGroup) {
  const settings = group.settings
  return [
    comboCoverageText(group),
    comboSettingString(settings, 'stock_pool_value'),
    `${comboSettingString(settings, 'start_date')}~${comboSettingString(settings, 'end_date')}`,
    comboSettingString(settings, 'portfolio_type'),
    comboSettingString(settings, 'pool_membership_mode'),
    shortHash(comboSettingString(settings, 'factor_value_params_hash')),
  ].filter(Boolean).join(' / ')
}

function applyCombinationGroup(group: FactorResearchCombinationGroup) {
  const settings = group.settings
  selectedCombinationId.value = group.combo_id
  filters.stock_pool = comboSettingString(settings, 'stock_pool_value') || filters.stock_pool
  filters.start_date = comboSettingString(settings, 'start_date') || null
  filters.end_date = comboSettingString(settings, 'end_date') || null
  filters.portfolio_type = (comboSettingString(settings, 'portfolio_type') || 'long_only') as BoardFilterState['portfolio_type']
  filters.pool_membership_mode = (comboSettingString(settings, 'pool_membership_mode') || 'static_latest') as BoardFilterState['pool_membership_mode']
  filters.filter_limit_up = comboBooleanSetting(settings, 'filter_limit_up', true)
  filters.filter_limit_down = comboBooleanSetting(settings, 'filter_limit_down', true)
  filters.group_count = comboNumberSetting(settings, 'group_count', 5)
  filters.direction = (comboSettingString(settings, 'direction') || 'desc') as BoardFilterState['direction']
  filters.outlier_handling = (comboSettingString(settings, 'outlier_handling') || 'none') as BoardFilterState['outlier_handling']
  filters.industry_neutralization = comboBooleanSetting(settings, 'industry_neutralization', false)
  filters.standardize = comboBooleanSetting(settings, 'standardize', false)
  filters.fee_rate = Number(settings.fee_rate || 0)
  filters.stamp_tax_rate = Number(settings.stamp_tax_rate || 0)
  filters.transfer_fee_rate = Number(settings.transfer_fee_rate || 0)
  filters.slippage = Number(settings.slippage || 0)
  filters.use_commission_stamp = Boolean((filters.fee_rate || 0) || (filters.stamp_tax_rate || 0) || (filters.transfer_fee_rate || 0))
  filters.use_slippage = Boolean(filters.slippage || 0)
  Object.keys(selectedFactorParamHashes).forEach(name => delete selectedFactorParamHashes[name])
  Object.entries(group.factor_value_params_hashes || {}).forEach(([name, hash]) => {
    if (hash) selectedFactorParamHashes[name] = hash
  })
  if (showBatchDialog.value) {
    batchForm.stock_pool_value = filters.stock_pool || batchForm.stock_pool_value
    batchForm.start_date = filters.start_date || batchForm.start_date
    batchForm.end_date = filters.end_date || batchForm.end_date
    batchForm.portfolio_type = filters.portfolio_type || batchForm.portfolio_type
    batchForm.pool_membership_mode = filters.pool_membership_mode || batchForm.pool_membership_mode
    batchForm.filter_limit_up = filters.filter_limit_up
    batchForm.filter_limit_down = filters.filter_limit_down
    batchForm.group_count = filters.group_count || batchForm.group_count
    batchForm.direction = filters.direction || batchForm.direction
    batchForm.outlier_handling = filters.outlier_handling || 'none'
    batchForm.industry_neutralization = filters.industry_neutralization
    batchForm.standardize = filters.standardize
    batchForm.use_commission_stamp = filters.use_commission_stamp
    batchForm.use_slippage = filters.use_slippage
  }
  fetchBoard({ refreshCatalog: false, refreshParamHashes: false })
}

async function fetchBoard(options: { refreshCatalog?: boolean; refreshParamHashes?: boolean } = { refreshCatalog: true, refreshParamHashes: true }) {
  const requestSeq = ++boardRequestSeq
  loading.value = true
  try {
    if (options.refreshCatalog) {
      await loadFactorCatalog()
    }
    if (options.refreshParamHashes !== false && filters.factor_groups?.length) {
      await loadCombinations()
    }
    const costs = costParamsFromToggles(filters.use_commission_stamp, filters.use_slippage)
    const { start_date, end_date } = resolveDateRangeFromFilters()
    const query: BoardQuery = {
      ...filters,
      ...costs,
      start_date,
      end_date,
      categories: queryCategories.value,
      factor_groups: filters.factor_groups?.length ? filters.factor_groups : undefined,
      factor_value_params_hashes: selectedParamHashesForNames(filterParamHashFactorNames.value),
    }
    const res = await evaluationApi.board(query)
    if (requestSeq !== boardRequestSeq) {
      return
    }
    rows.value = res.rows
    total.value = res.total
    if (options.refreshParamHashes !== false && !filters.factor_groups?.length && filters.factor_keyword) {
      await loadCombinations()
    }
  } catch {
    if (requestSeq !== boardRequestSeq) {
      return
    }
    rows.value = []
    total.value = 0
  } finally {
    if (requestSeq === boardRequestSeq) {
      loading.value = false
    }
  }
}

async function handleFactorCreated() {
  await fetchBoard({ refreshCatalog: true })
}

function handleSortChange({ prop, order }: { prop: string; order: string | null }) {
  if (prop) {
    filters.sort_by = prop
    filters.sort_order = order === 'ascending' ? 'asc' : 'desc'
  }
  fetchBoard()
}

function goToDetail(row: BoardRow) {
  const { start_date, end_date } = resolveDateRangeFromFilters()
  const costs = costParamsFromToggles(filters.use_commission_stamp, filters.use_slippage)
  router.push({
    path: `/factor/detail/${row.factor_name}`,
    query: {
      stock_pool: filters.stock_pool,
      period: filters.period,
      start_date,
      end_date,
      portfolio_type: filters.portfolio_type,
      fee_config: filters.fee_config,
      fee_rate: String(costs.fee_rate),
      stamp_tax_rate: String(costs.stamp_tax_rate),
      transfer_fee_rate: String(costs.transfer_fee_rate),
      slippage: String(costs.slippage),
      factor_value_params_hash: selectedFactorParamHashes[row.factor_name] || '',
      filter_limit_up: String(filters.filter_limit_up),
      filter_limit_down: String(filters.filter_limit_down),
      group_count: String(filters.group_count || 5),
      direction: filters.direction || 'desc',
      pool_membership_mode: filters.pool_membership_mode,
      outlier_handling: filters.outlier_handling || 'none',
      industry_neutralization: String(Boolean(filters.industry_neutralization)),
      standardize: String(Boolean(filters.standardize)),
    },
  })
}

function formatNumber(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return Number(value).toFixed(4)
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return `${(Number(value) * 100).toFixed(2)}%`
}

function formatDate(value?: string | null) {
  if (!value) return '未计算'
  return value.slice(0, 10)
}

function coverageStatusText(status: BoardRow['coverage_status']) {
  if (status === 'covered') return '已覆盖'
  if (status === 'partial') return '部分覆盖'
  if (status === 'empty') return '无缓存'
  return '未知'
}

function coverageTagType(status: BoardRow['coverage_status']) {
  if (status === 'covered') return 'success'
  if (status === 'partial') return 'warning'
  if (status === 'empty') return 'info'
  return 'danger'
}

function coverageRangeText(row: BoardRow) {
  if (row.coverage_status === 'unknown') return '按需加载'
  const rowsText = `${(row.coverage_total_rows || 0).toLocaleString()} 行`
  if (!row.coverage_min_date || !row.coverage_max_date) return rowsText
  return `${row.coverage_min_date} - ${row.coverage_max_date} / ${rowsText}`
}

function costParamsFromToggles(useCommissionStamp: boolean, useSlippage: boolean) {
  return {
    fee_rate: useCommissionStamp ? 0.003 : 0,
    stamp_tax_rate: useCommissionStamp ? 0.001 : 0,
    transfer_fee_rate: 0,
    slippage: useSlippage ? 0.001 : 0,
  }
}

async function openBatchDialog() {
  await loadFactorCatalog()
  const { start_date, end_date } = resolveDateRangeFromFilters()
  if (filters.factor_groups?.length) {
    batchForm.factor_group = filters.factor_groups[0]
  }
  batchForm.stock_pool_value = filters.stock_pool || 'zz500'
  batchForm.start_date = start_date
  batchForm.end_date = end_date
  batchForm.portfolio_type = filters.portfolio_type || 'long_only'
  batchForm.use_commission_stamp = filters.use_commission_stamp
  batchForm.use_slippage = filters.use_slippage
  batchForm.filter_limit_up = Boolean(filters.filter_limit_up)
  batchForm.filter_limit_down = Boolean(filters.filter_limit_down)
  batchForm.group_count = Number(filters.group_count || 5)
  batchForm.direction = (filters.direction || 'desc') as 'asc' | 'desc'
  batchForm.pool_membership_mode = filters.pool_membership_mode || 'static_latest'
  batchForm.benchmark_symbol = filters.benchmark_symbol || '000300.SH'
  batchForm.outlier_handling = (filters.outlier_handling || 'none') as 'none' | 'winsorize'
  batchForm.industry_neutralization = Boolean(filters.industry_neutralization)
  batchForm.standardize = Boolean(filters.standardize)
  showBatchDialog.value = true
  await loadCombinations()
}

function syncFiltersFromBatchForm() {
  filters.stock_pool = batchForm.stock_pool_value
  filters.start_date = batchForm.start_date
  filters.end_date = batchForm.end_date
  filters.portfolio_type = batchForm.portfolio_type
  filters.pool_membership_mode = batchForm.pool_membership_mode
  filters.benchmark_symbol = batchForm.benchmark_symbol
  filters.use_commission_stamp = batchForm.use_commission_stamp
  filters.use_slippage = batchForm.use_slippage
  filters.filter_limit_up = batchForm.filter_limit_up
  filters.filter_limit_down = batchForm.filter_limit_down
  filters.group_count = batchForm.group_count
  filters.direction = batchForm.direction
  filters.outlier_handling = batchForm.outlier_handling
  filters.industry_neutralization = batchForm.industry_neutralization
  filters.standardize = batchForm.standardize
}

async function submitBatchRun() {
  const group = factorGroups.value.find(item => item.name === batchForm.factor_group)
  if (!group) {
    ElMessage.warning('请选择因子组')
    return
  }
  batchLoading.value = true
  batchResult.value = []
  try {
    const costs = costParamsFromToggles(batchForm.use_commission_stamp, batchForm.use_slippage)
    const selectedHashes = selectedParamHashesForNames(group.factor_names)
    const result = await factorResearchRunApi.batch({
      factor_names: group.factor_names,
      stock_pool_value: batchForm.stock_pool_value,
      benchmark_symbol: batchForm.benchmark_symbol,
      start_date: batchForm.start_date,
      end_date: batchForm.end_date,
      portfolio_type: batchForm.portfolio_type,
      rebalance_period: batchForm.rebalance_period,
      fee_rate: costs.fee_rate,
      stamp_tax_rate: costs.stamp_tax_rate,
      transfer_fee_rate: costs.transfer_fee_rate,
      slippage: costs.slippage,
      filter_limit_up: batchForm.filter_limit_up,
      filter_limit_down: batchForm.filter_limit_down,
      group_count: batchForm.group_count,
      direction: batchForm.direction,
      pool_membership_mode: batchForm.pool_membership_mode,
      factor_value_params_hashes: selectedHashes,
      outlier_handling: batchForm.outlier_handling,
      industry_neutralization: batchForm.industry_neutralization,
      standardize: batchForm.standardize,
      force: false,
    })
    batchResult.value = result.items
    ElMessage.success('批量计算已完成')
    syncFiltersFromBatchForm()
    fetchBoard()
  } finally {
    batchLoading.value = false
  }
}

watch(
  () => [
    filters.stock_pool,
    filters.period,
    filters.start_date,
    filters.end_date,
    filters.portfolio_type,
    filters.use_commission_stamp,
    filters.use_slippage,
    filters.filter_limit_up,
    filters.filter_limit_down,
    filters.group_count,
    filters.direction,
    filters.pool_membership_mode,
    filters.outlier_handling,
    filters.industry_neutralization,
    filters.standardize,
    filters.factor_keyword || '',
    JSON.stringify(filters.categories || []),
    JSON.stringify(filters.factor_groups || []),
  ],
  () => { filters.page = 1; fetchBoard() }
)

watch(
  () => [showBatchDialog.value, batchForm.factor_group],
  () => {
    if (showBatchDialog.value) {
      loadCombinations()
    }
  }
)

watch(
  () => [filters.factor_keyword || '', JSON.stringify(filters.factor_groups || [])],
  () => {
    Object.keys(combinationSelection).forEach(key => {
      delete combinationSelection[key as keyof FactorResearchCombinationSelection]
    })
    selectedCombinationId.value = ''
  }
)

onMounted(() => {
  loadWatchlistGroups()
  loadIndexCatalog()
  fetchBoard()
})
</script>

<style scoped>
.factor-board {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 0;
  --factor-board-surface: rgba(253, 251, 247, 0.94);
  --factor-board-surface-soft: rgba(245, 242, 234, 0.78);
  --factor-board-border: rgba(34, 48, 42, 0.12);
  --factor-board-border-strong: rgba(27, 61, 50, 0.26);
  --factor-board-row-striped: rgba(245, 242, 234, 0.55);
  --factor-board-row-hover: rgba(238, 243, 240, 0.92);
}
.board-sticky {
  position: sticky;
  top: 0;
  z-index: 12;
  display: grid;
  gap: 8px;
  padding-bottom: 2px;
  background: linear-gradient(180deg, rgba(253, 251, 247, 0.98) 0%, rgba(253, 251, 247, 0.92) 82%, rgba(253, 251, 247, 0) 100%);
  backdrop-filter: blur(12px);
}
.filter-bar {
  background:
    linear-gradient(135deg, rgba(178, 122, 30, 0.08), transparent 30%),
    linear-gradient(180deg, rgba(253, 251, 247, 0.94), rgba(245, 242, 234, 0.72)),
    var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: 6px;
  padding: 8px 10px 10px;
  box-shadow: var(--shadow-sm);
}
.filter-title {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 7px;
}
.filter-title > div {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.panel-kicker {
  font-family: var(--font-data);
  font-size: 10px;
  color: var(--accent-warning);
}
.filter-title strong {
  color: var(--text-bright);
  font-size: 14px;
}
.filter-count {
  padding: 3px 7px;
  border: 1px solid rgba(178, 122, 30, 0.24);
  border-radius: 6px;
  background: rgba(178, 122, 30, 0.08);
  color: var(--accent-warning);
  font-family: var(--font-data);
  font-size: 11px;
  white-space: nowrap;
}
.filter-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
}
.filter-cluster {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  align-items: center;
  gap: 6px 8px;
  min-width: 0;
  padding: 6px 7px;
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
  background: rgba(253, 251, 247, 0.52);
}
.filter-cluster--wide {
  grid-column: 1 / -1;
  grid-template-columns: minmax(220px, 0.8fr) minmax(180px, 0.6fr) minmax(220px, 1fr);
}
.filter-field {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 6px;
  min-width: 0;
}
.filter-field--keyword {
  grid-template-columns: 32px minmax(0, 1fr);
}
.filter-field--compact {
  grid-template-columns: auto minmax(80px, 1fr);
}
.filter-field--period {
  grid-template-columns: auto minmax(0, max-content) auto;
}
.filter-field--portfolio {
  grid-template-columns: auto minmax(0, max-content);
}
.filter-field--checks {
  grid-template-columns: auto minmax(0, 1fr);
}
.filter-label {
  color: var(--text-secondary);
  font-size: var(--gs-font-label);
  font-weight: 700;
  line-height: 1;
  white-space: nowrap;
}
.filter-label--spaced {
  margin-left: 0;
}
.help-label,
.toolbar-actions,
.date-range,
.batch-result-row {
  display: flex;
  align-items: center;
  gap: 5px;
}
.fee-toggle-group {
  display: inline-flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px 9px;
  min-height: var(--gs-control-height-xs);
}
.group-count-input {
  width: 82px;
}
.preprocess-controls {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 12px;
}
.fee-toggle-group :deep(.el-checkbox),
.preprocess-controls :deep(.el-checkbox) {
  margin-right: 0;
}
.fee-toggle-group :deep(.el-switch__label) {
  font-size: var(--gs-font-label);
}
.batch-result-metric {
  font-variant-numeric: tabular-nums;
  color: var(--text-primary);
}
.combo-date-range {
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 11px;
  white-space: nowrap;
}
.param-hash-panel {
  display: grid;
  gap: 7px;
  max-height: 190px;
  overflow: auto;
  padding: 8px 9px;
  border: 1px solid rgba(27, 61, 50, 0.2);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(27, 61, 50, 0.07), transparent 30%),
    rgba(253, 251, 247, 0.74);
}
.param-hash-panel--dialog {
  max-height: 180px;
  margin: 0 0 14px 96px;
}
.combo-panel {
  position: relative;
  z-index: 16;
  max-height: min(300px, 42vh);
  overflow: auto;
}
.filter-bar > .combo-panel {
  margin-bottom: 7px;
}
.param-hash-panel--dialog.combo-panel {
  max-height: 260px;
  overflow: auto;
}
.param-hash-panel__head {
  display: flex;
  align-items: baseline;
  gap: 8px;
  color: var(--text-secondary);
  font-size: 11px;
}
.param-hash-panel__head > span {
  flex: 1 1 auto;
  min-width: 0;
}
.param-hash-panel__head strong {
  color: var(--text-bright);
  font-size: 13px;
}
.param-hash-list {
  display: grid;
  gap: 8px;
}
.param-hash-row {
  display: grid;
  grid-template-columns: minmax(150px, 220px) 1fr;
  gap: 10px;
  align-items: center;
}
.param-hash-factor {
  display: grid;
  gap: 2px;
  min-width: 0;
}
.param-hash-factor strong,
.param-hash-factor span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.param-hash-factor strong {
  color: var(--text-primary);
  font-size: 12px;
}
.param-hash-factor span {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: 11px;
}
.param-hash-options {
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
}
.param-hash-chip {
  display: inline-grid;
  gap: 2px;
  min-width: 116px;
  padding: 4px 7px;
  border: 1px solid rgba(34, 48, 42, 0.14);
  border-radius: 6px;
  color: var(--text-secondary);
  background: rgba(245, 242, 234, 0.72);
  text-align: left;
  cursor: pointer;
}
.param-hash-chip span {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 12px;
}
.param-hash-chip small {
  color: var(--text-muted);
  font-size: 10px;
}
.param-hash-chip.is-default {
  border-color: rgba(251, 191, 36, 0.32);
}
.param-hash-chip.is-active {
  border-color: rgba(27, 61, 50, 0.42);
  background: var(--bg-active);
  box-shadow: 0 0 0 1px rgba(27, 61, 50, 0.12) inset;
}
.param-hash-empty {
  color: var(--text-muted);
  font-size: 12px;
}
.combo-head-actions {
  display: flex;
  flex: 0 0 auto;
  gap: 8px;
  align-items: center;
  margin-left: auto;
}
.combo-apply-best,
.combo-reset {
  height: 26px;
  padding: 0 9px;
  border-radius: 6px;
  font-size: 12px;
  white-space: nowrap;
  cursor: pointer;
}
.combo-apply-best {
  border: 1px solid rgba(27, 61, 50, 0.32);
  color: var(--text-bright);
  background: var(--bg-active);
}
.combo-reset {
  margin-left: auto;
  border: 1px solid transparent;
  color: var(--text-secondary);
  background: transparent;
}
.combo-panel-body {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(260px, 0.6fr);
  gap: 10px;
  align-items: start;
}
.combo-facets {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 8px 10px;
  align-items: start;
}
.combo-facet-row {
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr);
  gap: 6px;
  align-items: start;
}
.combo-facet-label {
  color: var(--text-muted);
  font-size: 12px;
  line-height: 24px;
}
.combo-facet-options {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}
.combo-chip {
  min-width: 96px;
  padding: 4px 7px;
}
.combo-group-list {
  display: grid;
  grid-template-columns: 1fr;
  gap: 8px;
  max-height: 170px;
  overflow: auto;
}
.combo-group-card {
  display: grid;
  gap: 4px;
  padding: 6px 8px;
  border: 1px solid rgba(34, 48, 42, 0.14);
  border-radius: 6px;
  color: var(--text-secondary);
  background: rgba(253, 251, 247, 0.68);
  text-align: left;
  cursor: pointer;
}
.combo-group-card strong {
  color: var(--text-bright);
  font-size: 12px;
}
.combo-group-card span {
  color: var(--text-muted);
  font-size: 11px;
}
.combo-group-card.is-active {
  border-color: rgba(27, 61, 50, 0.42);
  background: var(--bg-active);
}
.param-hash-empty {
  grid-column: 1 / -1;
}
.tooltip-content {
  line-height: 1.7;
}
.filter-select {
  width: 100%;
}
.filter-select--wide {
  width: 100%;
}
.filter-select--pool {
  min-width: 190px;
}
.filter-select--membership {
  min-width: 150px;
}
.filter-select--direction {
  min-width: 88px;
}
.filter-select--preprocess {
  min-width: 118px;
}
.keyword-input {
  width: 100%;
}
.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 8px 10px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(56, 189, 248, 0.055), transparent 24%),
    var(--bg-elevated);
}
.board-title { display: flex; flex-direction: column; gap: 3px; font-size: 12px; color: var(--text-secondary); }
.board-title strong { font-size: 14px; color: var(--text-bright); }
.board-table-shell {
  min-height: 440px;
  height: min(62vh, 760px);
}
.board-pagination {
  margin-top: 4px;
  justify-content: flex-end;
}
.batch-form {
  padding-right: 12px;
}
.batch-result {
  display: grid;
  gap: 7px;
  margin-top: 12px;
  padding: 10px;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: rgba(8, 8, 10, 0.24);
}
.batch-result-row {
  justify-content: space-between;
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 12px;
}
.factor-name-cell,
.coverage-cell {
  display: grid;
  gap: 2px;
  min-width: 0;
}
.factor-name-cell strong {
  overflow: hidden;
  color: var(--text-bright);
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.factor-name-cell span,
.coverage-cell span {
  overflow: hidden;
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.coverage-cell {
  align-items: start;
}
.positive { color: var(--market-up); }
.negative { color: var(--market-down); }
.detail-action {
  color: #fdfbf7;
  border-color: rgba(27, 61, 50, 0.55);
  background: var(--accent-primary);
}
.detail-action:hover {
  color: #ffffff;
  border-color: rgba(27, 61, 50, 0.86);
  background: var(--accent-secondary);
}

:deep(.el-table) {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-row-hover-bg-color: var(--factor-board-row-hover);
  --el-table-border-color: var(--factor-board-border);
  --el-table-header-bg-color: var(--bg-elevated);
  --el-table-text-color: var(--text-primary);
  --el-table-header-text-color: var(--text-secondary);
  border: 1px solid var(--border-default);
  border-radius: 8px;
  overflow: hidden;
  background: var(--bg-elevated);
  box-shadow: var(--shadow-sm);
}

:deep(.el-table__body tr),
:deep(.el-table__body td.el-table__cell) {
  background: transparent !important;
}

:deep(.el-table--striped .el-table__body tr.el-table__row--striped > td.el-table__cell) {
  background: var(--factor-board-row-striped) !important;
}

:deep(.el-table .el-table-fixed-column--left),
:deep(.el-table .el-table-fixed-column--right) {
  background: var(--factor-board-surface) !important;
}

:deep(.el-table th.el-table__cell) {
  background: var(--bg-elevated);
  color: var(--text-secondary);
  font-size: var(--gs-font-table);
  font-weight: 600;
  padding: 5px 0;
}

:deep(.el-table td.el-table__cell) {
  border-bottom-color: var(--border-subtle);
  padding: 5px 0;
}

:deep(.el-table__inner-wrapper::before) {
  background-color: var(--factor-board-border);
}

:deep(.el-pagination) {
  margin-top: 2px !important;
}

:deep(.el-checkbox__label),
:deep(.el-radio-button__inner) {
  font-size: var(--gs-font-label);
}

:deep(.el-radio-button__inner),
:deep(.el-button) {
  border-radius: 6px;
}

:deep(.el-radio-button__inner) {
  height: var(--gs-control-height-xs);
  padding: 5px 8px;
}

:deep(.el-button) {
  min-height: var(--gs-control-height-xs);
  padding: 5px 9px;
}

:deep(.el-input__wrapper),
:deep(.el-select__wrapper),
:deep(.el-input-number .el-input__wrapper) {
  min-height: var(--gs-control-height-xs);
}

:deep(.el-tag) {
  height: 22px;
  padding: 0 6px;
}

:deep(.el-button:not(.el-button--primary)) {
  color: var(--text-primary);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0)), var(--factor-board-surface-soft);
  border-color: var(--factor-board-border);
}

:deep(.el-button:not(.el-button--primary):hover) {
  color: var(--text-bright);
  background: linear-gradient(180deg, rgba(56, 189, 248, 0.08), rgba(56, 189, 248, 0)), rgba(31, 41, 55, 0.92);
  border-color: var(--factor-board-border-strong);
}

:deep(.el-select__wrapper),
:deep(.el-input__wrapper),
:deep(.el-date-editor .el-input__wrapper) {
  background: rgba(8, 8, 10, 0.42);
  box-shadow: 0 0 0 1px var(--factor-board-border) inset;
}

:deep(.el-tag) {
  color: #cbd5e1;
  background: linear-gradient(180deg, rgba(148, 163, 184, 0.14), rgba(148, 163, 184, 0.06));
  border-color: rgba(148, 163, 184, 0.2);
}

:deep(.coverage-cell .el-tag),
:deep(.batch-result .el-tag) {
  color: #dbe4f0 !important;
  background-color: rgba(51, 65, 85, 0.38) !important;
  background-image: linear-gradient(180deg, rgba(148, 163, 184, 0.14), rgba(148, 163, 184, 0.06)) !important;
  border-color: rgba(148, 163, 184, 0.24) !important;
}

:deep(.el-tag--success) {
  color: #86efac;
  background: linear-gradient(180deg, rgba(34, 197, 94, 0.16), rgba(34, 197, 94, 0.06));
  border-color: rgba(34, 197, 94, 0.22);
}

:deep(.el-tag--warning) {
  color: #fcd34d;
  background: linear-gradient(180deg, rgba(245, 158, 11, 0.16), rgba(245, 158, 11, 0.06));
  border-color: rgba(245, 158, 11, 0.22);
}

:deep(.el-tag--danger) {
  color: #fca5a5;
  background: linear-gradient(180deg, rgba(239, 68, 68, 0.16), rgba(239, 68, 68, 0.06));
  border-color: rgba(239, 68, 68, 0.22);
}

:deep(.el-tag--info) {
  color: #93c5fd;
  background: linear-gradient(180deg, rgba(56, 189, 248, 0.14), rgba(56, 189, 248, 0.06));
  border-color: rgba(96, 165, 250, 0.22);
}

@media (max-width: 1100px) {
  .board-sticky {
    gap: 10px;
  }

  .filter-grid,
  .filter-cluster,
  .filter-cluster--wide {
    grid-template-columns: 1fr;
  }

  .filter-field,
  .filter-field--keyword,
  .filter-field--period,
  .filter-field--portfolio,
  .filter-field--checks {
    grid-template-columns: minmax(72px, auto) minmax(0, 1fr);
  }

  .filter-label--spaced {
    margin-left: 0;
  }

  .toolbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .param-hash-row {
    grid-template-columns: 1fr;
  }

  .param-hash-panel--dialog {
    margin-left: 0;
  }

  .combo-panel-body {
    grid-template-columns: 1fr;
  }

  .combo-group-list {
    max-height: none;
  }

  .board-table-shell {
    height: 460px;
  }
}

@media (max-width: 760px) {
  .filter-bar,
  .toolbar {
    padding: 10px;
  }

  .filter-cluster {
    gap: 8px;
    padding: 8px;
  }

  .filter-select,
  .filter-select--wide,
  .keyword-input {
    width: 100%;
  }

  .board-title strong {
    font-size: 14px;
  }

  .board-table-shell {
    height: 380px;
  }

  .board-pagination {
    justify-content: stretch;
  }
}
</style>
