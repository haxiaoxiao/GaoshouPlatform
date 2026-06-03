<template>
  <div class="page-frame page-container backtest-page">
    <div class="backtest-hero panel-card">
      <div class="backtest-hero__copy">
        <span class="section-kicker">BACKTEST COCKPIT</span>
        <h2>策略回测</h2>
        <p>保留策略列表、回测记录、代码编辑器、参数配置、数据检查、参数优化和报告入口；运行页优先放大代码区。</p>
        <div class="backtest-hero__meta">
          <span>策略：{{ activeStrategy?.name || '未选择' }}</span>
          <span>引擎：{{ btEngine }}</span>
          <span>K线：{{ btBarType }}</span>
          <span>状态：{{ btRunning ? '运行中' : btTaskId ? '有结果' : '待运行' }}</span>
        </div>
      </div>
      <div class="page-header-actions">
        <el-button link type="primary" @click="openDocs">
          <el-icon><Document /></el-icon>
          使用手册
        </el-button>
        <el-dropdown @command="handleCreate" style="margin-left:8px">
          <el-button type="primary">
            <el-icon><Plus /></el-icon>
            新建策略
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="script">新建脚本策略</el-dropdown-item>
              <el-dropdown-item command="expression">新建表达式策略</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-button type="success" @click="showUploadDialog = true">
          上传研报
        </el-button>
      </div>
    </div>
    <el-tabs v-model="activeTab" class="strategy-tabs">
      <el-tab-pane label="策略列表" name="strategyList">
        <div class="tab-content">
          <el-table v-loading="loading" :data="strategyList" stripe style="width: 100%">
            <el-table-column prop="id" label="ID" width="80" />
            <el-table-column prop="name" label="策略名称" width="200" />
            <el-table-column prop="description" label="描述" min-width="250">
              <template #default="{ row }">
                {{ row.description || '-' }}
              </template>
            </el-table-column>
            <el-table-column prop="created_at" label="创建时间" width="180">
              <template #default="{ row }">
                {{ formatDateTime(row.created_at) }}
              </template>
            </el-table-column>
            <el-table-column label="操作" width="200" fixed="right">
              <template #default="{ row }">
                <el-button type="primary" link @click="handleBacktest(row)">回测</el-button>
                <el-button type="danger" link @click="handleDelete(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>

          <div class="pagination-container">
            <el-pagination
              v-model:current-page="currentPage"
              v-model:page-size="pageSize"
              :page-sizes="[10, 20, 50]"
              :total="total"
              layout="total, sizes, prev, pager, next, jumper"
              @size-change="handleSizeChange"
              @current-change="handlePageChange"
            />
          </div>
        </div>

        <!-- 上传研报对话框 -->
        <el-dialog v-model="showUploadDialog" title="上传研报生成策略" width="520px" :close-on-click-modal="false">
          <div class="upload-area" v-if="!uploading && !uploadResult"
               @drop.prevent="handleDrop" @dragover.prevent
               @click="() => fileInput?.click()">
            <input type="file" ref="fileInput" accept=".pdf,.txt,.md" @change="handleFileSelect" style="display:none" />
            <p style="font-size:40px;margin:0">📄</p>
            <p style="color:#aaa;margin:8px 0">点击选择或拖拽 PDF/TXT 文件</p>
            <p style="color:#666;font-size:12px">{{ uploadFile?.name || '未选择文件' }}</p>
          </div>
          <div v-if="uploadFile && !uploading && !uploadResult" style="padding:12px 0;text-align:center">
            <el-button type="primary" @click="handleUploadReport" :loading="uploading">开始生成策略</el-button>
          </div>
          <div v-if="uploading" style="text-align:center;padding:24px">
            <el-icon class="is-loading" style="font-size:32px;color:#409eff"><Loading /></el-icon>
            <p style="color:#aaa;margin-top:12px">AI 正在分析研报并生成策略...</p>
          </div>
          <div v-if="uploadResult" style="padding:12px 0">
            <el-alert :title="uploadResult.summary" type="success" :closable="false" style="margin-bottom:12px" />
            <el-descriptions :column="2" border size="small">
              <el-descriptions-item label="策略类型">{{ uploadResult.strategy_type === 'builtin' ? '内置策略' : uploadResult.strategy_type === 'expression' ? '表达式' : '脚本' }}</el-descriptions-item>
              <el-descriptions-item label="调仓频率">{{ uploadResult.frequency || '-' }}</el-descriptions-item>
              <el-descriptions-item label="选股条件" :span="2">{{ (uploadResult.conditions || []).join('; ') || '-' }}</el-descriptions-item>
            </el-descriptions>
            <div style="text-align:center;margin-top:16px">
              <el-button @click="showUploadDialog = false">取消</el-button>
              <el-button type="primary" @click="applyUploadResult">填充到编辑器</el-button>
            </div>
          </div>
        </el-dialog>
      </el-tab-pane>

      <el-tab-pane label="回测记录" name="backtestList">
        <BacktestList />
      </el-tab-pane>

      <el-tab-pane label="回测运行" name="backtestRunner">
        <div v-if="!activeStrategy" class="empty-runner">
          <p>请先在“策略列表”中选择一个策略，然后点击“回测”按钮进入回测运行界面</p>
        </div>
        <div v-else class="split-layout">
          <div class="editor-panel">
            <div class="editor-toolbar">
              <el-input v-model="activeStrategy.name" size="small" class="strategy-name-input" placeholder="策略名称" />
              <el-tabs v-model="editorTab" class="editor-tabs">
                <el-tab-pane
                  v-for="tab in editorTabs"
                  :key="tab.key"
                  :label="tab.label"
                  :name="tab.key"
                />
              </el-tabs>
              <div class="toolbar-actions">
                <el-button size="small" @click="handleSaveStrategy" :loading="saving">保存</el-button>
                <el-button size="small" type="primary" @click="runBacktestTask" :loading="btRunning">编译运行</el-button>
              </div>
            </div>

            <!-- akquant Python 代码编辑器 -->
            <div class="code-editor" v-if="editorTab === 'akquant-code'">
              <div class="expression-hint" style="margin-bottom:4px">
                <span>akquant 策略：class MyStrategy(aq.Strategy) -> def on_bar(self, bar)</span>
              </div>
              <CodeEditor v-model="btCode" language="python" :min-height="'100%'" :placeholder="codePlaceholder" />
            </div>

            <!-- RQAlpha Python 代码编辑器 -->
            <div class="code-editor" v-if="editorTab === 'rqalpha-code'">
              <div class="expression-hint" style="margin-bottom:4px">
                <span>RQAlpha 语法: def init(context) + def handle_bar(context, bar_dict)</span>
              </div>
              <CodeEditor v-model="btCode" language="python" :min-height="'100%'" :placeholder="codePlaceholder" />
            </div>

            <!-- 表达式输入 -->
            <div class="expression-panel" v-if="editorTab === 'expression'">
              <div class="expression-input-row">
                <CodeEditor
                  v-model="btExpression"
                  language="expression"
                  class="expression-code-input"
                  :min-height="132"
                  placeholder="输入因子表达式，例如 close/MA(close, 20) - 1"
                />
                <el-select v-model="selectedFactorId" size="default" placeholder="从因子研究选择"
                  clearable class="factor-select" @change="handleFactorSelect">
                  <el-option v-for="f in savedFactors" :key="f.id" :label="f.name" :value="f.id" />
                </el-select>
              </div>
              <div class="expression-hint">
                <span>向量化回测：表达式按日计算因子值 -> 分层买入 -> 计算分组收益</span>
                <span class="hint-examples">示例: close/MA(close,20)-1 | RSI(close,14) | MACD(close)[0]</span>
              </div>
              <div class="expression-params">
                <span class="param-label">分组数</span>
                <el-input-number v-model="btNGroups" :min="2" :max="10" size="small" style="width:80px" />
              </div>
            </div>

            <!-- LLM 策略生成 -->
            <div class="llm-panel" v-if="editorTab === 'llm'">
              <LLMStrategyPanel :engine="btEngine" @code-generated="onLLMCodeGenerated" />
            </div>

          </div>
          <div class="right-panel">
            <div class="bt-config-bar">
              <el-date-picker v-model="btStartDate" value-format="YYYY-MM-DD" size="small" style="width:130px" placeholder="开始日期" />
              <span>至</span>
              <el-date-picker v-model="btEndDate" value-format="YYYY-MM-DD" size="small" style="width:130px" placeholder="结束日期" />
              <span>资金</span>
              <el-input-number v-model="btCapital" :min="10000" :step="100000" size="small" style="width:130px" />
              <template v-if="btMode === 'expression'">
                <span>调仓</span>
                <el-select v-model="btFrequency" size="small" style="width:80px">
                  <el-option label="每天" value="daily" />
                  <el-option label="每周" value="weekly" />
                  <el-option label="每月" value="monthly" />
                </el-select>
              </template>
              <span>K线</span>
              <el-select v-model="btBarType" size="small" style="width:92px">
                <el-option label="日线" value="daily" />
                <el-option label="分钟" value="minute" />
                <el-option label="定时" value="minute_timer" />
              </el-select>
              <span>引擎</span>
              <el-select v-model="btEngine" size="small" style="width:100px" @change="onEngineChange">
                <el-option v-for="e in engineOptions" :key="e.value" :label="e.label" :value="e.value" />
              </el-select>
              <span>基准</span>
              <el-select v-model="selectedBenchmarkSymbol" size="small" style="width:132px" clearable>
                <el-option
                  v-for="item in benchmarkOptions"
                  :key="item.symbol"
                  :label="`${item.display_name} ${item.symbol}`"
                  :value="item.symbol"
                />
              </el-select>
              <template v-if="btEngine === 'akquant'">
                <span>热启动</span>
                <el-select v-model="warmStartMode" size="small" style="width:86px">
                  <el-option label="Auto" value="auto" />
                  <el-option label="Always" value="always" />
                  <el-option label="Off" value="off" />
                </el-select>
                <el-input-number
                  v-if="warmStartMode !== 'off'"
                  v-model="warmStartChunkDays"
                  :min="1"
                  :max="365"
                  size="small"
                  controls-position="right"
                  style="width:92px"
                />
                <el-checkbox v-if="warmStartMode !== 'off'" v-model="warmStartKeepCheckpoints" size="small">保留</el-checkbox>
              </template>
              <el-button size="small" @click="checkBacktestCoverage" :loading="coverageChecking">检查数据</el-button>
              <el-button type="primary" size="small" @click="runBacktestTask" :loading="btRunning">运行回测</el-button>
              <el-button
                v-if="btTaskId && (btRunning || optimizationRunning)"
                type="danger"
                size="small"
                plain
                @click="stopCurrentTask"
              >
                停止
              </el-button>
              <el-switch
                v-if="btEngine === 'akquant'"
                v-model="showOptimizationPanel"
                size="small"
                active-text="参数优化"
              />
            </div>
            <div class="bt-run-progress" v-if="btRunning || btTaskId">
              <div class="bt-run-progress__head">
                <strong>{{ backtestProgressTitle }}</strong>
                <span>{{ Math.round(btProgress * 100) }}%</span>
              </div>
              <el-progress :percentage="Math.round(btProgress * 100)" :stroke-width="8" />
              <div class="bt-run-progress__meta">
                <span v-if="btTaskId">任务 {{ btTaskId }}</span>
                <span v-if="btLiveData?.current_date">日期 {{ btLiveData.current_date }}</span>
                <span>{{ backtestProgressMessage }}</span>
              </div>
            </div>
            <div class="akquant-opt-panel" v-if="btEngine === 'akquant' && showOptimizationPanel">
              <div class="opt-row">
                <span>参数网格</span>
                <el-input
                  v-model="optParamGridText"
                  type="textarea"
                  :rows="2"
                  class="opt-grid-input"
                  placeholder='{"param_name":[1,2,3]}'
                />
              </div>
              <div class="opt-row">
                <span>排序指标</span>
                <el-select v-model="optMetric" size="small" class="opt-metric-select">
                  <el-option
                    v-for="metric in optimizationMetricOptions"
                    :key="metric.value"
                    :label="metric.label"
                    :value="metric.value"
                  />
                </el-select>
                <span>训练/测试</span>
                <el-input-number v-model="optTrainPeriod" :min="20" size="small" class="opt-number" />
                <el-input-number v-model="optTestPeriod" :min="5" size="small" class="opt-number" />
                <span>并行</span>
                <el-input-number v-model="optMaxWorkers" :min="1" :max="maxWorkerLimit" size="small" class="opt-number" />
                <span class="opt-period-hint">{{ optPeriodHint }}</span>
                <el-button size="small" @click="runOptimizationTask('grid')" :loading="btRunning">Grid Search</el-button>
                <el-button size="small" @click="runOptimizationTask('walk_forward')" :loading="btRunning">Walk-forward</el-button>
              </div>
              <div class="opt-progress" v-if="optimizationRunning || optimizationProgress > 0">
                <div class="opt-progress-label">
                  <span>{{ optimizationLabel }}</span>
                  <span>{{ Math.round(optimizationProgress * 100) }}%</span>
                </div>
                <el-progress
                  :percentage="Math.round(optimizationProgress * 100)"
                  :status="optimizationRunning ? undefined : 'success'"
                  :stroke-width="6"
                />
                <el-button
                  v-if="btTaskId && optimizationRunning"
                  class="opt-stop-btn"
                  type="danger"
                  size="small"
                  plain
                  @click="stopCurrentTask"
                >
                  停止优化
                </el-button>
              </div>
              <el-table
                v-if="optimizationRows.length"
                :data="optimizationRows"
                size="small"
                stripe
                max-height="180"
                class="opt-result-table"
              >
                <el-table-column
                  v-for="col in optimizationColumns"
                  :key="col"
                  :label="col"
                  min-width="110"
                  show-overflow-tooltip
                >
                  <template #default="{ row }">{{ formatOptimizationCell(row, col) }}</template>
                </el-table-column>
              </el-table>
              <el-button
                v-if="optimizationBacktestId"
                size="small"
                type="primary"
                link
                @click="openOptimizationReport"
              >
                查看完整优化报告
              </el-button>
            </div>
            <div class="bt-pool-bar">
              <span class="pool-label">股票池</span>
              <!-- 自选组标签 -->
              <template v-if="poolSource">
                <el-tag size="small" type="success" closable @close="clearAllStocks">
                  {{ poolSource.label }} ({{ poolSource.count }} 只)
                </el-tag>
              </template>
              <!-- 手动输入：逐个标签 -->
              <template v-else>
                <el-tag
                  v-for="sym in btSymbols"
                  :key="sym"
                  closable
                  size="small"
                  type="info"
                  @close="removeSymbol(sym)"
                >{{ sym }}</el-tag>
                <el-input
                  v-model="newSymbolInput"
                  size="small"
                  placeholder="输入代码，回车添加"
                  style="width:180px"
                  @keyup.enter="addSymbol"
                  @blur="addSymbol"
                />
              </template>
              <el-button size="small" text @click="showWatchlistPicker = true" v-if="!poolSource">从自选池导入</el-button>
                <el-select
                v-model="selectedPool"
                size="small"
                placeholder="指数池"
                style="width:130px"
                @change="loadSelectedPool"
                clearable
              >
                <el-option label="中小综指" value="index:399101.SZ" />
                <el-option
                  v-for="item in indexPoolOptions.filter(option => option.symbol !== '399101.SZ')"
                  :key="item.symbol"
                  :label="item.display_name"
                  :value="`index:${item.symbol}`"
                />
                <el-option label="沪深Top100" value="top100" />
                <el-option label="沪深Top300" value="top300" />
                <el-option label="沪深Top500" value="top500" />
                <el-option label="全量A股" value="all" />
              </el-select>
              <el-button size="small" text type="danger" @click="clearAllStocks" v-if="poolSource || btSymbols.length > 0">
                清空 ({{ poolSource ? poolSource.count : btSymbols.length }})
              </el-button>
              <span class="pool-count">{{ poolSource ? poolSource.count : btSymbols.length }} 只</span>
            </div>

            <Teleport to="body">
              <el-dialog v-model="showWatchlistPicker" title="从自选池导入" width="420px">
                <el-select v-model="selectedWatchlistGroup" placeholder="选择分组" style="width:100%">
                  <el-option v-for="g in watchlistGroups" :key="g.id" :label="g.name + ' (' + g.stock_count + '只)'" :value="g.id" />
                </el-select>
                <template #footer>
                  <el-button size="small" @click="showWatchlistPicker = false">取消</el-button>
                  <el-button size="small" type="primary" @click="useWatchlistGroup">使用此分组</el-button>
                </template>
              </el-dialog>
            </Teleport>

            <RunningPanel
              :running="btRunning"
              :completed="!btRunning && btFullResult != null"
              :liveData="btLiveData"
              :logs="[...btLogs, ...btErrors.map(e => '[错误] ' + e)]"
              :progress="btProgress"
              @viewReport="showReport = true"
            />
          </div>

          <ReportOverlay v-model:visible="showReport" :result="btFullResult" :task-id="btTaskId" />
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Document, ArrowDown, Loading } from '@element-plus/icons-vue'
import { strategyApi, type Strategy, type LiveData, type BacktestResultData } from '@/api/backtest'
import { factorApi, type Factor } from '@/api/factor'
import { indexCatalogApi, watchlistApi, type IndexCatalogItem, type WatchlistGroup } from '@/api/data'
import BacktestList from './BacktestList.vue'
import RunningPanel from './RunningPanel.vue'
import ReportOverlay from './ReportOverlay.vue'
import LLMStrategyPanel from './LLMStrategyPanel.vue'
import CodeEditor from '@/components/CodeEditor.vue'
import { formatDateTime } from '@/utils/format'
import { formatPercentValue, metricDisplayName, summarizeWalkForwardRows, valueToString } from '@/utils/optimizationReport'

const route = useRoute()
const router = useRouter()

const SAMPLE_EXPRESSION = 'close / MA(close, 20) - 1'

const TREND_CAPITAL_CODE = `def init(context):
    # 趋势资金识别参数
    context.lookback = 5       # 回看天数
    context.vol_pct = 0.90     # 成交量分位阈值
    context.min_trend_minutes = 3  # 最少趋势分钟数

    # 信号融合参数
    context.fusion_window = 3     # 融合窗口（交易日）
    context.fusion_min_days = 2   # 每个信号至少满足天数

    # 组合参数
    context.portfolio_size = 5    # 持仓数
    context.hold_days = 20        # 每只股票持股交易日数
    context.rebalance_every = 5   # 调仓频率（交易日）

    # 运行状态
    context.signal_history = {}   # {date: {symbol: {...信号数据...}}}
    context.baskets = []          # [{entry_idx, stocks: [{symbol, entry_price}]}]
    context.day_count = 0
    context.last_rebalance = -999
    context.trailing_highs = {}   # 移动止损基准价

    log(f"趋势资金策略: 查{context.lookback}日 阈值{context.vol_pct} 持{context.portfolio_size}只 {context.hold_days}日")

# 信号计算辅助函数
def compute_daily_signal(context, symbol, today):
    # 获取过去 lookback 天的分钟数据，计算成交量阈值
    hist = context.get_intraday_history(symbol, context.lookback, today)
    if hist is None or hist.empty:
        return None

    past_vols = []
    for d in sorted(set(hist.index.date)):
        if d >= today:
            continue
        day_data = hist[hist.index.date == d]
        past_vols.extend(day_data["volume"].values.tolist())

    if len(past_vols) < context.lookback * 10:
        return None
    threshold = np.quantile(past_vols, context.vol_pct)

    # 获取当日分钟数据
    bars = context.get_intraday(symbol, today)
    if len(bars) < 10:
        return None

    vols = np.array([b.volume for b in bars], dtype=float)
    closes = np.array([b.close for b in bars], dtype=float)
    amounts = np.array([b.total_turnover for b in bars], dtype=float)

    is_trend = vols > threshold
    trend_count = int(is_trend.sum())
    if trend_count < context.min_trend_minutes:
        return {
            "sig_b": 0, "sig_b_ok": False,
            "sig_c": 0, "sig_c_ok": False,
            "trend_count": trend_count,
        }

    trend_vol = vols[is_trend]
    trend_close = closes[is_trend]
    trend_amount = amounts[is_trend]

    # Signal B: 趋势资金 VWAP vs 全部 VWAP
    trend_vwap = np.sum(trend_amount) / np.sum(trend_vol) if trend_vol.sum() > 0 else 0
    all_vwap = np.sum(amounts) / np.sum(vols) if vols.sum() > 0 else 0
    sig_b = trend_vwap / all_vwap - 1 if all_vwap > 0 else 0

    # Signal C: 净支撑量
    avg_close = np.mean(closes)
    support_vol = np.sum(trend_vol[trend_close < avg_close])
    resist_vol = np.sum(trend_vol[trend_close > avg_close])
    sig_c = support_vol - resist_vol

    return {
        "sig_b": float(sig_b), "sig_b_ok": sig_b < 0,
        "sig_c": float(sig_c), "sig_c_ok": sig_c > 0,
        "trend_count": trend_count,
    }


`

// State
const activeTab = ref('strategyList')
const loading = ref(false)
const strategyList = ref<Strategy[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const saving = ref(false)


// Strategy list
const loadStrategies = async () => {
  loading.value = true
  try {
    const response = await strategyApi.list(currentPage.value, pageSize.value)
    strategyList.value = response.items
    total.value = response.total
  } catch {
    ElMessage.error('加载策略列表失败')
  } finally {
    loading.value = false
  }
}

const handleCreate = async (type: string) => {
  let name, code, desc
  if (type === 'expression') {
    name = 'New Expression Strategy'
    code = SAMPLE_EXPRESSION
    desc = 'Factor expression strategy'
  } else {
    name = 'New Strategy'
    code = AKQUANT_TEMPLATE
    desc = 'AKQuant Strategy example'
  }
  try {
    const result = await strategyApi.create({ name, code, description: desc })
    ElMessage.success('Strategy created')
    await loadStrategies()
    activeStrategy.value = {
      id: result.id,
      name: result.name,
      code: result.code,
      description: result.description,
      parameters: result.parameters,
      created_at: result.created_at,
      updated_at: result.updated_at,
    }
    if (type === 'expression') {
      editorTab.value = 'expression'
      btMode.value = 'expression'
      btExpression.value = result.code || SAMPLE_EXPRESSION
    } else {
      editorTab.value = btEngine.value === 'akquant' ? 'akquant-code' : 'rqalpha-code'
      btMode.value = 'script'
    }
    btCode.value = result.code || ''
    btExpression.value = SAMPLE_EXPRESSION
    btBarType.value = 'daily'
    btMetrics.value = []
    btLogs.value = []
    btErrors.value = []
    activeTab.value = 'backtestRunner'
  } catch {
    ElMessage.error('创建失败')
  }
}

const handleDelete = async (row: Strategy) => {
  try {
    await ElMessageBox.confirm(`确定要删除策略“${row.name}”吗？`, '确认删除', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning',
    })
    await strategyApi.delete(row.id)
    ElMessage.success('删除成功')
    if (activeStrategy.value?.id === row.id) {
      activeStrategy.value = null
      activeTab.value = 'strategyList'
    }
    loadStrategies()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

const handlePageChange = (page: number) => {
  currentPage.value = page
  loadStrategies()
}

const handleSizeChange = (size: number) => {
  pageSize.value = size
  currentPage.value = 1
  loadStrategies()
}

// Backtest runner state
const activeStrategy = ref<Strategy | null>(null)
const btMode = ref<'script' | 'expression'>('script')
const btEngine = ref('akquant')
const engineOptions = ref<{ value: string; label: string; modes: string[] }[]>([])

// Editor tabs driven by engine
const editorTab = ref('akquant-code')
const editorTabs = computed(() => {
  if (btEngine.value === 'akquant') {
    return [
      { key: 'akquant-code', label: 'Python Code (AKQuant)' },
      { key: 'llm', label: 'LLM Strategy' },
    ]
  }
  return [
    { key: 'rqalpha-code', label: 'Python Code (RQAlpha)' },
    { key: 'expression', label: 'Expression' },
  ]
})

const AKQUANT_TEMPLATE = `import akquant as aq
import numpy as np

class MyStrategy(aq.Strategy):
    def on_start(self):
        self.set_history_depth(300)

    def on_bar(self, bar):
        # Called on each bar
        pos = self.get_position(bar.symbol)
        if bar.close > bar.open and pos == 0:
            self.buy(bar.symbol, 100)
        elif bar.close < bar.open and pos > 0:
            self.close_position(bar.symbol)
`

// 研报上传
const showUploadDialog = ref(false)
const uploadFile = ref<File | null>(null)
const fileInput = ref<HTMLInputElement | null>(null)
const uploading = ref(false)
const uploadResult = ref<{ strategy_type: string; name: string; code: string; summary: string; conditions: string[]; frequency: string } | null>(null)

const btCode = ref('')
const btExpression = ref(SAMPLE_EXPRESSION)

const codePlaceholder = computed(() =>
  btEngine.value === 'akquant'
    ? 'class MyStrategy(aq.Strategy):\n    def on_bar(self, bar):\n        # akquant Strategy\n        pass'
    : 'def init(context):\n    context.ma_fast = 5\n\ndef handle_bar(context, bar):\n    # write your strategy here\n    pass'
)
const btNGroups = ref(5)
const selectedFactorId = ref<number | null>(null)
const savedFactors = ref<Factor[]>([])
const getDefaultEndDate = () => {
  const d = new Date()
  return d.toISOString().slice(0, 10)
}
const getDefaultStartDate = () => {
  const d = new Date()
  d.setFullYear(d.getFullYear() - 1)
  return d.toISOString().slice(0, 10)
}
const SMALL_CAP_MINUTE_START_DATE = '2021-05-15'

const BTSET_KEY = 'backtest_settings'

interface BtSettings {
  symbols?: string[]
  poolSource?: { type: string; label: string; count: number; symbols: string[]; indexSymbol?: string } | null
  startDate?: string
  endDate?: string
  capital?: number
  frequency?: string
  barType?: string
  engine?: string
  benchmarkSymbol?: string
  warmStartMode?: 'auto' | 'always' | 'off'
  warmStartChunkDays?: number
  warmStartKeepCheckpoints?: boolean
  showOptimizationPanel?: boolean
  optParamGridText?: string
  optMetric?: string
  optTrainPeriod?: number
  optTestPeriod?: number
  optMaxWorkers?: number
}

const loadBtSettings = (): BtSettings => {
  try {
    const raw = localStorage.getItem(BTSET_KEY)
    return raw ? JSON.parse(raw) : {}
  } catch { return {} }
}

const collectBtSettings = (): BtSettings => ({
    symbols: btSymbols.value.length > 0 ? btSymbols.value : undefined,
    poolSource: poolSource.value ?? undefined,
    startDate: btStartDate.value,
    endDate: btEndDate.value,
    capital: btCapital.value,
    frequency: btFrequency.value,
    barType: btBarType.value,
    engine: btEngine.value,
    benchmarkSymbol: selectedBenchmarkSymbol.value || undefined,
    warmStartMode: warmStartMode.value,
    warmStartChunkDays: warmStartChunkDays.value,
    warmStartKeepCheckpoints: warmStartKeepCheckpoints.value,
    showOptimizationPanel: showOptimizationPanel.value,
    optParamGridText: optParamGridText.value,
    optMetric: optMetric.value,
    optTrainPeriod: optTrainPeriod.value,
    optTestPeriod: optTestPeriod.value,
    optMaxWorkers: optMaxWorkers.value,
})

const saveBtSettings = () => {
  const s = collectBtSettings()
  try { localStorage.setItem(BTSET_KEY, JSON.stringify(s)) } catch { /* quota exceeded */ }
}

const saved = loadBtSettings()
if (saved.engine) btEngine.value = saved.engine
const btStartDate = ref(saved.startDate || getDefaultStartDate())
const btEndDate = ref(saved.endDate || getDefaultEndDate())
const btCapital = ref(saved.capital ?? 1_000_000)
const btFrequency = ref(saved.frequency || 'monthly')
const btBarType = ref(saved.barType || 'daily')
const selectedBenchmarkSymbol = ref(saved.benchmarkSymbol || '000300.SH')
const warmStartMode = ref<'auto' | 'always' | 'off'>(saved.warmStartMode || 'auto')
const warmStartChunkDays = ref(saved.warmStartChunkDays || 30)
const warmStartKeepCheckpoints = ref(Boolean(saved.warmStartKeepCheckpoints))
const showOptimizationPanel = ref(Boolean(saved.showOptimizationPanel))
const btSymbols = ref<string[]>(saved.symbols || [])
const isAllStocks = ref(false)
const poolSource = ref<{ type: string; label: string; count: number; symbols: string[]; indexSymbol?: string } | null>(saved.poolSource || null)
const allStocksCount = ref(0)
const newSymbolInput = ref('')
const showWatchlistPicker = ref(false)
const selectedPool = ref<string | null>(null)
const watchlistGroups = ref<WatchlistGroup[]>([])
const selectedWatchlistGroup = ref<number | null>(null)
const indexCatalog = ref<IndexCatalogItem[]>([])
const indexPoolOptions = computed(() => indexCatalog.value.filter(item => item.pool_enabled))
const benchmarkOptions = computed(() =>
  indexCatalog.value.filter(item => item.benchmark_enabled && (item.common_benchmark || item.pool_enabled))
)

// 最终提交用的 symbol 列表
const effectiveSymbols = computed(() => {
  if (poolSource.value?.type === 'index') return []
  if (poolSource.value) return poolSource.value.symbols
  return btSymbols.value
})
const selectedIndexSymbol = computed(() => poolSource.value?.type === 'index' ? poolSource.value.indexSymbol : undefined)
const isSmallCapStrategy = computed(() => {
  const name = String(activeStrategy.value?.name || '')
  const code = String(btCode.value || activeStrategy.value?.code || '')
  return activeStrategy.value?.id === 43 || name.includes('小市值') || code.includes('SmallCapV4Strategy')
})
const refreshSmallCapTimerStartDate = async () => {
  if (!isSmallCapStrategy.value || saved.startDate) return
  try {
    const { default: request } = await import('@/api/request')
    const indexSymbol = selectedIndexSymbol.value || '399101.SZ'
    const res = await request.get<any>(
      `/backtest/timer-coverage?index_symbol=${indexSymbol}&start_date=${SMALL_CAP_MINUTE_START_DATE}&end_date=${btEndDate.value}&times=10:00,10:30,14:30,14:50`,
      {}
    )
    const start = res?.earliest_date
    if (start && btStartDate.value < start) {
      btStartDate.value = start
      ElMessage.info(`已按本地稀疏分钟数据将起始日期设置为 ${start}`)
    }
  } catch {
    if (btStartDate.value < '2025-04-23') {
      btStartDate.value = '2025-04-23'
    }
  }
}
watch(isSmallCapStrategy, enabled => {
  if (enabled && !saved.startDate && btStartDate.value < SMALL_CAP_MINUTE_START_DATE) {
    btStartDate.value = SMALL_CAP_MINUTE_START_DATE
  }
  if (enabled && !saved.barType) {
    btBarType.value = 'minute_timer'
  }
  if (enabled) {
    refreshSmallCapTimerStartDate()
  }
}, { immediate: true })

const addSymbol = () => {
  const s = newSymbolInput.value.trim().toUpperCase()
  if (s && /^\d{6}\.(SZ|SH|BJ)$/.test(s) && !btSymbols.value.includes(s)) {
    poolSource.value = null
    btSymbols.value = [...btSymbols.value, s]
  }
  newSymbolInput.value = ''
}

const removeSymbol = (sym: string) => {
  poolSource.value = null
  btSymbols.value = btSymbols.value.filter(s => s !== sym)
}

const loadWatchlistGroups = async () => {
  try {
    watchlistGroups.value = await watchlistApi.getGroups()
  } catch { /* non-critical */ }
}

const loadIndexCatalog = async () => {
  try {
    indexCatalog.value = await indexCatalogApi.list()
  } catch {
    indexCatalog.value = []
  }
}

const useWatchlistGroup = async () => {
  const gid = selectedWatchlistGroup.value
  if (!gid) return
  try {
    const stocks = await watchlistApi.getGroupStocks(gid)
    const symbols = (stocks || []).map((s: any) => s.symbol)
    if (symbols.length === 0) {
      ElMessage.warning('该分组无股票')
      return
    }
    const grp = watchlistGroups.value.find(g => g.id === gid)
    poolSource.value = {
      type: 'watchlist',
      label: grp?.name || '自选池',
      count: symbols.length,
      symbols,
    }
    btSymbols.value = []
    isAllStocks.value = false
    showWatchlistPicker.value = false
    ElMessage.success(`已加载 ${grp?.name || '自选池'} (${symbols.length} 只)`)
  } catch (e: any) {
    ElMessage.error(`加载失败: ${e?.message || e}`)
  }
}

const loadPoolSymbols = async (poolName: string | null) => {
  if (!poolName) return
  try {
    const { default: request } = await import('@/api/request')
    const res = await request.get<any>(`/backtest/pools/${poolName}`)
    if (res?.symbols?.length) {
      const labelMap: Record<string, string> = {
        top100: '沪深Top100', top300: '沪深Top300', top500: '沪深Top500', all: '全量A股',
      }
      poolSource.value = {
        type: 'pool',
        label: labelMap[poolName] || poolName,
        count: res.symbols.length,
        symbols: res.symbols,
      }
      btSymbols.value = []
      isAllStocks.value = false
      ElMessage.success(`已加载 ${labelMap[poolName] || poolName} (${res.symbols.length} 只)`)
    } else {
      ElMessage.warning(`${poolName} 股票池为空`)
    }
  } catch (e: any) {
    ElMessage.error(`加载股票池失败: ${e?.message || e}`)
  }
  selectedPool.value = null
}

const loadIndexPool = async (indexSymbol: string | null) => {
  if (!indexSymbol) return
  try {
    const { default: request } = await import('@/api/request')
    const res = await request.get<any>(
      `/backtest/index-pools/${indexSymbol}?start_date=${btStartDate.value}&end_date=${btEndDate.value}`,
      {}
    )
    if (res?.pool_enabled === false) {
      const reason = res?.reason === 'market_only_index' ? '该指数当前只提供行情基准，不提供严格历史股票池' : '缺少严格历史成分快照'
      ElMessage.warning(reason)
      selectedPool.value = null
      return
    }
    const catalogItem = indexCatalog.value.find(item => item.symbol === (res?.index_symbol || indexSymbol))
    poolSource.value = {
      type: 'index',
      label: catalogItem?.display_name || res?.display_name || res?.index_symbol || indexSymbol,
      count: res?.symbol_count || 0,
      symbols: res?.symbols || [],
      indexSymbol: res?.index_symbol || indexSymbol,
    }
    if (catalogItem?.benchmark_enabled && (!selectedBenchmarkSymbol.value || selectedBenchmarkSymbol.value === '000300.SH')) {
      selectedBenchmarkSymbol.value = catalogItem.symbol
    }
    btSymbols.value = []
    isAllStocks.value = false
    ElMessage.success(`已选择指数池 ${poolSource.value.label}，历史成分 ${poolSource.value.count} 只`)
  } catch (e: any) {
    ElMessage.error(`加载指数池失败: ${e?.message || e}`)
  }
  selectedPool.value = null
}

const loadSelectedPool = async (poolName: string | null) => {
  if (!poolName) return
  if (poolName.startsWith('index:')) {
    await loadIndexPool(poolName.slice('index:'.length))
    return
  }
  await loadPoolSymbols(poolName)
}

const clearAllStocks = () => {
  btSymbols.value = []
  isAllStocks.value = false
  allStocksCount.value = 0
  poolSource.value = null
}

const btRunning = ref(false)
const btProgress = ref(0)
const btLiveData = ref<LiveData | null>(null)
const btFullResult = ref<BacktestResultData | null>(null)
const btTaskId = ref<string | null>(null)
const showReport = ref(false)
const btMetrics = ref<{ label: string; value: string; color?: string }[]>([])
const btLogs = ref<string[]>([])
const btErrors = ref<string[]>([])
const coverageChecking = ref(false)
const backtestProgressTitle = computed(() => {
  if (btFullResult.value && !btRunning.value) return '回测完成'
  if (btTaskId.value) return '回测运行中'
  return '准备提交回测'
})
const backtestProgressMessage = computed(() => {
  const metadata = (btLiveData.value as any)?.metadata || {}
  return metadata.progress_message || (btTaskId.value ? '正在轮询回测任务状态' : '等待任务号返回')
})
let lastLiveLogKey = ''
const optParamGridText = ref('{"param_name":[1,2,3]}')
const optMetric = ref('sharpe_ratio')
const optimizationMetricOptions = [
  { value: 'calmar_ratio', label: '卡尔玛比率 Calmar' },
  { value: 'sharpe_ratio', label: '夏普比率 Sharpe' },
  { value: 'total_return', label: '总收益率' },
  { value: 'annual_return', label: '年化收益率' },
  { value: 'max_drawdown', label: '最大回撤' },
  { value: 'sortino_ratio', label: 'Sortino 比率' },
  { value: 'win_rate', label: '胜率' },
  { value: 'total_trades', label: '交易次数' },
]
const optTrainPeriod = ref(252)
const optTestPeriod = ref(63)
const maxWorkerLimit = Math.max(1, Number(window.navigator?.hardwareConcurrency || 4))
const defaultOptMaxWorkers = Math.max(1, maxWorkerLimit - 2)
const optMaxWorkers = ref(defaultOptMaxWorkers)
if (saved.optParamGridText) optParamGridText.value = saved.optParamGridText
if (saved.optMetric) optMetric.value = saved.optMetric
if (saved.optTrainPeriod) optTrainPeriod.value = saved.optTrainPeriod
if (saved.optTestPeriod) optTestPeriod.value = saved.optTestPeriod
if (saved.optMaxWorkers) optMaxWorkers.value = saved.optMaxWorkers
const optimizationRunning = ref(false)
const optimizationProgress = ref(0)
const optimizationLabel = ref('')
const optPeriodHint = computed(() => {
  if (btBarType.value === 'daily') {
    return '单位: bar（日线=交易日）'
  }
  const trainDays = Math.max(1, Math.round(Number(optTrainPeriod.value || 0) / 240))
  const testDays = Math.max(1, Math.round(Number(optTestPeriod.value || 0) / 240))
  return `单位: bar，约 ${trainDays}/${testDays} 个交易日`
})

const applyBtSettings = (settings?: BtSettings | null) => {
  if (!settings) return
  if (settings.engine) btEngine.value = settings.engine
  if (settings.startDate) btStartDate.value = settings.startDate
  if (settings.endDate) btEndDate.value = settings.endDate
  if (typeof settings.capital === 'number') btCapital.value = settings.capital
  if (settings.frequency) btFrequency.value = settings.frequency
  if (settings.barType) btBarType.value = settings.barType
  if (settings.benchmarkSymbol !== undefined) selectedBenchmarkSymbol.value = settings.benchmarkSymbol || '000300.SH'
  if (settings.warmStartMode) warmStartMode.value = settings.warmStartMode
  if (typeof settings.warmStartChunkDays === 'number') warmStartChunkDays.value = settings.warmStartChunkDays
  if (typeof settings.warmStartKeepCheckpoints === 'boolean') warmStartKeepCheckpoints.value = settings.warmStartKeepCheckpoints
  if (typeof settings.showOptimizationPanel === 'boolean') showOptimizationPanel.value = settings.showOptimizationPanel
  if (typeof settings.optParamGridText === 'string') optParamGridText.value = settings.optParamGridText
  if (typeof settings.optMetric === 'string') optMetric.value = settings.optMetric
  if (typeof settings.optTrainPeriod === 'number') optTrainPeriod.value = settings.optTrainPeriod
  if (typeof settings.optTestPeriod === 'number') optTestPeriod.value = settings.optTestPeriod
  if (typeof settings.optMaxWorkers === 'number') optMaxWorkers.value = settings.optMaxWorkers
  if (settings.poolSource) {
    poolSource.value = settings.poolSource
    btSymbols.value = []
    isAllStocks.value = false
  } else if (Array.isArray(settings.symbols)) {
    poolSource.value = null
    btSymbols.value = settings.symbols
    isAllStocks.value = false
  }
}

const currentStrategyParameters = () => ({
  ...((activeStrategy.value?.parameters || {}) as Record<string, unknown>),
  backtest_settings: collectBtSettings(),
})

const persistActiveStrategyRunSettings = async () => {
  if (!activeStrategy.value?.id) {
    saveBtSettings()
    return
  }
  const parameters = currentStrategyParameters()
  const updated = await strategyApi.update(activeStrategy.value.id, { parameters })
  activeStrategy.value = {
    ...activeStrategy.value,
    parameters: updated.parameters,
    updated_at: updated.updated_at,
  }
  const idx = strategyList.value.findIndex(s => s.id === activeStrategy.value?.id)
  if (idx >= 0) {
    strategyList.value[idx] = { ...strategyList.value[idx], parameters: updated.parameters, updated_at: updated.updated_at }
  }
  saveBtSettings()
}
const optimizationRows = ref<Record<string, unknown>[]>([])
const optimizationBacktestId = ref<number | null>(null)
const optimizationColumns = computed(() => {
  const keys = new Set<string>()
  optimizationRows.value.slice(0, 20).forEach(row => Object.keys(row).forEach(key => keys.add(key)))
  return Array.from(keys).slice(0, 12)
})

const formatOptimizationCell = (row: Record<string, unknown>, col: string) => {
  const params = row.params as Record<string, unknown> | undefined
  const value = params?.[col] ?? row[col]
  if (['return_pct', 'max_drawdown'].includes(col)) {
    return formatPercentValue(typeof value === 'number' ? value : null)
  }
  return valueToString(value)
}

const compactOptimizationRows = (data: any, label: string): Record<string, unknown>[] => {
  const rows = Array.isArray(data?.rows) ? data.rows : []
  if (label === 'Walk-forward') {
    const metric = data?.metric || optMetric.value || 'calmar_ratio'
    return summarizeWalkForwardRows(rows, metric).map(row => ({
      window: row.window,
      train_start: row.train_start,
      train_end: row.train_end,
      rows: row.row_count,
      return_pct: row.return_pct,
      max_drawdown: row.max_drawdown,
      objective_value: row.objective_value,
      objective: metricDisplayName(metric),
      params: row.params,
      ...row.params,
    }))
  }
  return rows.slice(0, 50)
}

const openOptimizationReport = () => {
  if (optimizationBacktestId.value) {
    router.push(`/backtest/optimization/${optimizationBacktestId.value}`)
  }
}

const appendBtLog = (message: string) => {
  if (!message) return
  const line = `[${new Date().toLocaleTimeString('zh-CN', { hour12: false })}] ${message}`
  if (btLogs.value[btLogs.value.length - 1] === line) return
  btLogs.value = [...btLogs.value.slice(-199), line]
}

const syncLiveLogs = (live: LiveData | null, progress = 0) => {
  if (!live) return
  const metadata = (live as any).metadata || {}
  const snapshot = (live as any).metrics_snapshot || {}
  const chunkIndex = metadata.chunk_index ?? snapshot.chunk_index
  const chunkTotal = metadata.chunk_total ?? snapshot.chunk_total
  const currentDate = live.current_date || '-'
  const latestEvent = Array.isArray(live.events) && live.events.length
    ? live.events[live.events.length - 1]
    : null
  const eventMessage = latestEvent?.message || latestEvent?.type || ''
  const logKey = [
    metadata.phase || '',
    currentDate,
    chunkIndex || '',
    chunkTotal || '',
    Math.round(progress * 100),
    eventMessage,
  ].join('|')
  if (logKey === lastLiveLogKey) return
  lastLiveLogKey = logKey

  if (chunkIndex && chunkTotal) {
    appendBtLog(`Segment ${chunkIndex}/${chunkTotal} | Date ${currentDate} | Progress ${Math.round(progress * 100)}% | ${eventMessage || 'Running'}`)
    return
  }
  appendBtLog(`Date ${currentDate} | Progress ${Math.round(progress * 100)}% | ${eventMessage || 'Running'}`)
}

const parseOptimizationGrid = () => {
  try {
    const parsed = JSON.parse(optParamGridText.value || '{}')
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('参数网格必须是 JSON object')
    }
    for (const [key, value] of Object.entries(parsed)) {
      if (!Array.isArray(value) || value.length === 0) {
        throw new Error(`${key} must be a non-empty array`)
      }
    }
    return parsed as Record<string, unknown[]>
  } catch (e: any) {
    ElMessage.error(`参数网格格式错误: ${e?.message || e}`)
    return null
  }
}

const buildBacktestPayload = () => {
  const isExpression = editorTab.value === 'expression'
  const code = isExpression ? btExpression.value : btCode.value
  const engine = btEngine.value
  return {
    engine,
    mode: isExpression ? 'vectorized' : 'event_driven',
    factor_expression: isExpression ? code : undefined,
    strategy_code: !isExpression && engine === 'akquant' ? code : undefined,
    strategy_id: activeStrategy.value?.id || undefined,
    buy_condition: !isExpression && engine === 'builtin' ? code : undefined,
    symbols: effectiveSymbols.value,
    universe_mode: selectedIndexSymbol.value ? 'index' : 'symbols',
    index_symbol: selectedIndexSymbol.value,
    start_date: btStartDate.value,
    end_date: btEndDate.value,
    initial_capital: btCapital.value,
    rebalance_freq: btFrequency.value,
    n_groups: isExpression ? btNGroups.value : 5,
    bar_type: btBarType.value,
    benchmark_symbol: selectedBenchmarkSymbol.value || undefined,
    warm_start: engine === 'akquant'
      ? {
          mode: warmStartMode.value,
          chunk_days: warmStartChunkDays.value,
          keep_checkpoints: warmStartKeepCheckpoints.value,
        }
      : undefined,
  }
}

const formatCoveragePct = (value?: number | null) => `${Math.round(Number(value || 0) * 100)}%`

const coverageSummaryLines = (coverage: any) => {
  const lines = [
    `行情 ${coverage?.market?.dataset || btBarType.value}: ${coverage?.market?.covered_symbol_count || 0}/${coverage?.market?.requested_symbol_count || 0} 只，覆盖率 ${formatCoveragePct(coverage?.market?.coverage_ratio)}`,
  ]
  if (coverage?.factor?.items?.length) {
    const required = coverage.factor.items
      .filter((item: any) => item.required)
      .map((item: any) => `${item.factor_name} ${formatCoveragePct(item.coverage_ratio)} (${item.min_date || '-'} ~ ${item.max_date || '-'})`)
    if (required.length) {
      lines.push(`必需因子: ${required.join('；')}`)
    }
  }
  if (coverage?.market?.missing_symbols_sample?.length) {
    lines.push(`缺行情样本: ${coverage.market.missing_symbols_sample.slice(0, 8).join(', ')}`)
  }
  if (coverage?.benchmark) {
    const b = coverage.benchmark
    lines.push(`基准 ${b.symbol}: ${b.covered_days || 0} 日${b.ok ? '' : '，行情缺失将隐藏对比线'}`)
  }
  if (coverage?.benchmark_warnings?.length) {
    lines.push(`基准提示: ${coverage.benchmark_warnings.join('；')}`)
  }
  return lines
}

const requestBacktestCoverage = async () => {
  const { backtestEngines } = await import('@/api/backtest')
  return backtestEngines.dataCoverage(buildBacktestPayload() as any)
}

const checkBacktestCoverage = async () => {
  coverageChecking.value = true
  try {
    const coverage = await requestBacktestCoverage()
    coverageSummaryLines(coverage).forEach(line => appendBtLog(line))
    if (coverage.ok) {
      ElMessage.success('数据覆盖检查通过')
    } else {
      ElMessage.warning(`数据覆盖可能不足: ${(coverage.warnings || []).join('；')}`)
    }
    return coverage
  } catch (e: any) {
    ElMessage.error(e?.message || '数据覆盖检查失败')
    return null
  } finally {
    coverageChecking.value = false
  }
}

const confirmCoverageBeforeRun = async () => {
  const coverage = await checkBacktestCoverage()
  if (!coverage || coverage.ok) return true
  const lines = coverageSummaryLines(coverage)
  try {
    await ElMessageBox.confirm(
      `${lines.join('\n')}\n\n覆盖不足时可能出现 0 交易。是否继续运行？`,
      '数据覆盖确认',
      {
        type: 'warning',
        confirmButtonText: '继续运行',
        cancelButtonText: '取消',
      }
    )
    return true
  } catch {
    appendBtLog('已取消回测：数据覆盖不足')
    return false
  }
}

const pollBacktestTask = async (request: any, taskId: string, label = 'Backtest') => {
  btTaskId.value = taskId
  appendBtLog(`${label} task submitted (${taskId}), polling status`)
  const isOptimization = label === 'Grid Search' || label === 'Walk-forward'
  if (isOptimization) {
    optimizationRunning.value = true
    optimizationLabel.value = `${label} 已提交`
    optimizationProgress.value = 0
  }
  let terminal = false
  while (!terminal) {
    const statusData = await request.get(`/backtest/status/${taskId}`)
    btProgress.value = statusData?.progress ?? 0
    if (isOptimization) {
      optimizationProgress.value = btProgress.value
      optimizationLabel.value = statusData?.live?.metadata?.progress_message || `${label} 运行中`
    }
    if (statusData?.live) {
      btLiveData.value = statusData.live
      syncLiveLogs(statusData.live, btProgress.value)
    }

    if (statusData?.status === 'done') {
      const data = await request.get(`/backtest/result/${taskId}`)
      if (data) {
        btFullResult.value = data as BacktestResultData
        if (Array.isArray(data.rows)) {
          optimizationRows.value = compactOptimizationRows(data, label)
          optimizationBacktestId.value = Number(data.backtest_id || 0) || null
          appendBtLog(`${label} completed, ${data.count ?? data.rows.length} results`)
        } else {
          appendBtLog(`${label}完成`)
        }
        if (isOptimization) {
          optimizationProgress.value = 1
          optimizationLabel.value = `${label} 完成`
          optimizationRunning.value = false
        }
        saveBtSettings()
      }
      terminal = true
    } else if (statusData?.status === 'failed') {
      const errData = await request.get(`/backtest/result/${taskId}`)
      btErrors.value = [errData?.error || `${label}失败`]
      appendBtLog(`${label}失败: ${btErrors.value[0]}`)
      if (isOptimization) {
        optimizationProgress.value = 1
        optimizationLabel.value = `${label} 失败`
        optimizationRunning.value = false
      }
      terminal = true
    } else if (statusData?.status === 'cancelled') {
      const errData = await request.get(`/backtest/result/${taskId}`).catch(() => null)
      btErrors.value = [errData?.error || `${label}已停止`]
      appendBtLog(`${label}已停止`)
      if (isOptimization) {
        optimizationProgress.value = 1
        optimizationLabel.value = `${label} 已停止`
        optimizationRunning.value = false
      }
      terminal = true
    } else {
      await new Promise(r => setTimeout(r, 2000))
    }
  }
}

const stopCurrentTask = async () => {
  if (!btTaskId.value) return
  try {
    const { default: request } = await import('@/api/request')
    await request.post(`/backtest/cancel/${btTaskId.value}`, {}, {})
    appendBtLog(`已发送停止请求: ${btTaskId.value}`)
    btRunning.value = false
    btProgress.value = 1
    if (optimizationRunning.value) {
      optimizationRunning.value = false
      optimizationProgress.value = 1
      optimizationLabel.value = '任务已停止'
    }
    ElMessage.success('已停止当前任务')
  } catch (e: any) {
    ElMessage.error(e?.message || '停止失败')
  }
}

const loadBacktestTaskResult = async (taskId: string) => {
  try {
    const { default: request } = await import('@/api/request')
    const statusData = await request.get<any>(`/backtest/status/${taskId}`)
    btTaskId.value = taskId
    btProgress.value = statusData?.progress ?? 1
    if (statusData?.live) {
      btLiveData.value = statusData.live
    }
    if (statusData?.status === 'done' || statusData?.status === 'failed' || statusData?.status === 'cancelled') {
      const data = await request.get<any>(`/backtest/result/${taskId}`)
      if (statusData?.status === 'failed' || statusData?.status === 'cancelled') {
        btErrors.value = [data?.error || (statusData?.status === 'cancelled' ? '任务已停止' : '任务失败')]
      } else if (data) {
        btFullResult.value = data as BacktestResultData
      }
      activeTab.value = 'backtestRunner'
    }
  } catch (e: any) {
    ElMessage.warning(e?.message || '任务结果加载失败')
  }
}


const openDocs = () => {
  window.open('/docs', '_blank')
}

// Factor selection
const loadSavedFactors = async () => {
  try {
    const data = await factorApi.getList()
    savedFactors.value = data || []
  } catch {
    // non-critical
  }
}

const handleFactorSelect = (factorId: number | null) => {
  if (!factorId) return
  const factor = savedFactors.value.find(f => f.id === factorId)
  if (factor && factor.code) {
    btExpression.value = factor.code
  }
}

const handleSaveStrategy = async () => {
  if (!activeStrategy.value) return
  saving.value = true
  try {
    const code = btMode.value === 'expression' ? btExpression.value : btCode.value
    await strategyApi.update(activeStrategy.value.id, {
      name: activeStrategy.value.name,
      code,
      parameters: currentStrategyParameters(),
      description: activeStrategy.value.description || undefined,
    })
    activeStrategy.value.parameters = currentStrategyParameters()
    saveBtSettings()
    ElMessage.success('保存成功')
  } catch {
    ElMessage.error('保存失败')
  } finally {
    saving.value = false
  }
}

const handleBacktest = async (row: Strategy) => {
  try {
    activeStrategy.value = { ...row }
    const code = row.code || ''
    if (code.includes('def handle_bar') || code.includes('def init') || code.includes('def before_trading')) {
      editorTab.value = 'rqalpha-code'
      btMode.value = 'script'
      btCode.value = code
    } else if (code.includes('aq.Strategy') && code.includes('def on_bar')) {
      editorTab.value = 'akquant-code'
      btMode.value = 'script'
      btCode.value = code
    } else {
      editorTab.value = 'expression'
      btMode.value = 'expression'
      btExpression.value = code || SAMPLE_EXPRESSION
      btCode.value = code
    }
    // Auto-detect bar_type from code content
    const savedRunSettings = (row.parameters as any)?.backtest_settings as BtSettings | undefined
    if (savedRunSettings) {
      applyBtSettings(savedRunSettings)
    } else {
      btBarType.value = (code.includes('get_intraday') || code.includes('compute_daily_signal'))
        ? 'minute' : 'daily'
    }
    btLiveData.value = null
    btFullResult.value = null
    optimizationBacktestId.value = null
    optimizationRows.value = []
    btMetrics.value = []
    btLogs.value = []
    btErrors.value = []
    await nextTick()
    activeTab.value = 'backtestRunner'
  } catch (e) {
    console.error('handleBacktest error:', e)
    ElMessage.error('加载策略失败')
  }
}


const runBacktestTask = async () => {
  if (!activeStrategy.value) {
    const code = btCode.value || btExpression.value || ''
    if (!code.trim()) {
      ElMessage.warning('请先编写策略代码')
      return
    }
  }

  if (btSymbols.value.length === 0 && !isAllStocks.value && !poolSource.value) {
    try {
      const { default: request } = await import('@/api/request')
      const data = await request.get<{ symbols: string[] }>('/backtest/pools/top100')
      if (data?.symbols?.length) {
        btSymbols.value = data.symbols.slice(0, 10).filter(Boolean)
        ElMessage.success(`已自动加载 ${btSymbols.value.length} 只默认股票`)
      }
    } catch {
      ElMessage.warning('请先添加回测股票或选择指数池')
      return
    }
  }

  btRunning.value = true
  btLiveData.value = null
  btFullResult.value = null
  optimizationBacktestId.value = null
  optimizationRows.value = []
  btMetrics.value = []
  btLogs.value = []
  btErrors.value = []
  btProgress.value = 0
  btTaskId.value = null
  lastLiveLogKey = ''
  appendBtLog('开始回测')

  try {
    await persistActiveStrategyRunSettings()
    const coverageOk = await confirmCoverageBeforeRun()
    if (!coverageOk) return
    const { default: request } = await import('@/api/request')
    const res = await request.post<any>(
      '/backtest/run',
      buildBacktestPayload(),
      {}
    )

    const taskId = (res as any)?.task_id
    if (!taskId) {
      if (res) {
        btFullResult.value = res as BacktestResultData
        appendBtLog('回测完成')
        saveBtSettings()
      }
      return
    }

    await pollBacktestTask(request, taskId, 'Backtest')
  } catch (e: any) {
    btErrors.value = [e?.message || 'Backtest failed']
    appendBtLog(`Backtest failed: ${btErrors.value[0]}`)
  } finally {
    btRunning.value = false
  }
}

const runOptimizationTask = async (type: 'grid' | 'walk_forward') => {
  if (btEngine.value !== 'akquant') {
    ElMessage.warning('参数优化目前只支持 AKQuant 引擎')
    return
  }
  const grid = parseOptimizationGrid()
  if (!grid) return
  const code = editorTab.value === 'expression' ? btExpression.value : btCode.value
  if (!code.trim()) {
    ElMessage.warning('请先填写 AKQuant 策略代码')
    return
  }

  btRunning.value = true
  btLiveData.value = null
  btFullResult.value = null
  optimizationBacktestId.value = null
  optimizationRows.value = []
  btMetrics.value = []
  btLogs.value = []
  btErrors.value = []
  btProgress.value = 0
  btTaskId.value = null
  optimizationRunning.value = true
  optimizationProgress.value = 0
  optimizationLabel.value = type === 'grid' ? 'Grid Search 准备中' : 'Walk-forward 准备中'
  lastLiveLogKey = ''
  appendBtLog(type === 'grid' ? '开始 Grid Search' : '开始 Walk-forward')

  try {
    await persistActiveStrategyRunSettings()
    const { default: request } = await import('@/api/request')
    const payload = {
      ...buildBacktestPayload(),
      engine: 'akquant',
      mode: 'event_driven',
      param_grid: grid,
      sort_by: optMetric.value || 'sharpe_ratio',
      metric: optMetric.value || 'sharpe_ratio',
      ascending: false,
      train_period: optTrainPeriod.value,
      test_period: optTestPeriod.value,
      max_workers: optMaxWorkers.value,
    }
    const url = type === 'grid'
      ? '/backtest/optimize/grid'
      : '/backtest/optimize/walk-forward'
    const res = await request.post<any>(url, payload, {})
    const taskId = (res as any)?.task_id
    if (!taskId) {
      ElMessage.warning('优化任务未返回 task_id')
      return
    }
    await pollBacktestTask(request, taskId, type === 'grid' ? 'Grid Search' : 'Walk-forward')
  } catch (e: any) {
    btErrors.value = [e?.message || 'Optimization failed']
    appendBtLog(`Optimization failed: ${btErrors.value[0]}`)
    optimizationRunning.value = false
    optimizationLabel.value = '优化失败'
    optimizationProgress.value = 1
  } finally {
    btRunning.value = false
  }
}

const seedTrendCapitalStrategy = async () => {
  // 如果已存在则跳过
  if (strategyList.value.some(s => s.name === '趋势资金策略')) return
  try {
    const existing = await strategyApi.list(1, 100)
    if (existing.items.some((s: Strategy) => s.name === '趋势资金策略')) return
    await strategyApi.create({
      name: '趋势资金策略',
      code: TREND_CAPITAL_CODE,
      description: '研报十一 · 趋势资金日内事件驱动',
    })
    await loadStrategies()
  } catch {
    // non-critical: strategy can be created manually
  }
}

// Engine
const seedMultiFactorStrategy = async () => {
  if (strategyList.value.some(s => s.name === '通用多因子模型')) return
  try {
    const { backtestEngines } = await import('@/api/backtest')
    await backtestEngines.createMultiFactorStrategy()
    await loadStrategies()
  } catch {
    // non-critical: built-in template can still be created manually
  }
}

const loadEngines = async () => {
  try {
    const { backtestEngines } = await import('@/api/backtest')
    const data = await backtestEngines.list()
    engineOptions.value = (data as any)?.map((e: any) => ({
      value: e.name,
      label: e.label,
      modes: e.modes || [],
    })) || []
    if (engineOptions.value.length === 0) {
      engineOptions.value = [
        { value: 'builtin', label: '内置引擎', modes: ['vectorized', 'event_driven'] },
        { value: 'akquant', label: 'AKQuant', modes: ['event_driven'] },
      ]
    }
  } catch {
    engineOptions.value = [
      { value: 'builtin', label: '内置引擎', modes: ['vectorized', 'event_driven'] },
      { value: 'akquant', label: 'AKQuant', modes: ['event_driven'] },
    ]
  }
}

const onEngineChange = (engine: string) => {
  if (engine === 'akquant') {
    editorTab.value = 'akquant-code'
    btMode.value = 'script'
    if (!btCode.value || btCode.value.includes('def init(')) {
      btCode.value = AKQUANT_TEMPLATE
    }
  } else {
    editorTab.value = 'rqalpha-code'
    btMode.value = 'script'
  }
}

const onLLMCodeGenerated = (code: string) => {
  btCode.value = code
  editorTab.value = 'akquant-code'
}

// Report viewer for akquant
watch(activeTab, (tab) => {
  if (tab !== 'backtestRunner') {
    showReport.value = false
  }
})

// 研报上传
const handleFileSelect = (e: Event) => {
  const target = e.target as HTMLInputElement
  if (target.files?.length) uploadFile.value = target.files[0]
}
const handleDrop = (e: DragEvent) => {
  const file = e.dataTransfer?.files?.[0]
  if (file) uploadFile.value = file
}
const handleUploadReport = async () => {
  if (!uploadFile.value) return
  uploading.value = true
  uploadResult.value = null
  try {
    const form = new FormData()
    form.append('file', uploadFile.value)
    const { default: request } = await import('@/api/request')
    const res = await request.post<any>('/strategy/from-report', form, {
      headers: { 'Content-Type': 'multipart/form-data' },
    })
    uploadResult.value = res
  } catch (e: any) {
    ElMessage.error('生成失败: ' + (e?.message || '未知错误'))
  } finally {
    uploading.value = false
  }
}
const applyUploadResult = () => {
  if (!uploadResult.value) return
  const r = uploadResult.value
  // Create strategy in DB
  strategyApi.create({
    name: r.name || '研报策略',
    code: r.code || '',
    description: r.summary || '',
  }).then((result) => {
    // Fill editor
    activeStrategy.value = {
      id: result.id, name: result.name, code: result.code,
      description: result.description,
      parameters: result.parameters,
      created_at: result.created_at, updated_at: result.updated_at,
    }
    if (r.strategy_type === 'expression') {
      btMode.value = 'expression'
      btExpression.value = r.code
    } else {
      btMode.value = 'script'
      btCode.value = r.code
    }
    btBarType.value = 'daily'
    showUploadDialog.value = false
    uploadFile.value = null
    uploadResult.value = null
    activeTab.value = 'backtestRunner'
    ElMessage.success('策略已生成并填充到编辑器')
    loadStrategies()
  }).catch(() => {
    // Still fill editor even if DB save fails
    activeStrategy.value = { id: 0, name: r.name || '研报策略', code: r.code || '', description: r.summary || '', parameters: null, created_at: null, updated_at: null }
    if (r.strategy_type === 'expression') {
      btMode.value = 'expression'; btExpression.value = r.code
    } else {
      btMode.value = 'script'; btCode.value = r.code
    }
    showUploadDialog.value = false
    uploadFile.value = null; uploadResult.value = null
    activeTab.value = 'backtestRunner'
    ElMessage.success('Strategy filled into editor')
  })
}

onMounted(async () => {
  await loadStrategies()
  await seedTrendCapitalStrategy()
  await seedMultiFactorStrategy()
  await loadEngines()
  await loadIndexCatalog()
  loadSavedFactors()
  loadWatchlistGroups()
  const queryTaskId = typeof route.query.task_id === 'string' ? route.query.task_id : ''
  if (queryTaskId) {
    await loadBacktestTaskResult(queryTaskId)
  }
})
</script>

<style scoped>
.page-container {
  height: 100%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.backtest-page {
  gap: var(--space-4);
}

.backtest-hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: var(--space-4);
  padding: var(--space-5);
  flex-shrink: 0;
}

.backtest-hero__copy {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: var(--space-2);
}

.backtest-hero h2 {
  margin: 0;
  color: var(--text-bright);
  font-size: 22px;
}

.backtest-hero p {
  max-width: 860px;
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.backtest-hero__meta {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-2);
}

.backtest-hero__meta span {
  padding: 5px 8px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-full);
  color: var(--text-secondary);
  background: rgba(10, 14, 20, 0.58);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.page-header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.strategy-tabs {
  min-height: 0;
  flex: 1;
  display: flex;
  flex-direction: column;
  background: rgba(15, 15, 19, 0.76);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  padding: 10px 12px 12px;
  overflow: hidden;
  box-shadow: var(--shadow-card);
}

.strategy-tabs :deep(.el-tabs__content) {
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

.strategy-tabs :deep(.el-tab-pane) {
  height: 100%;
}

.strategy-tabs :deep(.el-table) {
  --el-table-bg-color: rgba(10, 14, 20, 0.78);
  --el-table-tr-bg-color: rgba(11, 16, 24, 0.72);
  --el-table-header-bg-color: rgba(11, 20, 31, 0.95);
  --el-table-row-hover-bg-color: rgba(56, 189, 248, 0.08);
  --el-table-border-color: rgba(108, 117, 137, 0.22);
  --el-table-text-color: var(--text-primary);
  --el-table-header-text-color: var(--text-secondary);
  background: rgba(10, 14, 20, 0.78) !important;
  color: var(--text-primary);
}

.strategy-tabs :deep(.el-table__inner-wrapper),
.strategy-tabs :deep(.el-table__body-wrapper),
.strategy-tabs :deep(.el-table__fixed),
.strategy-tabs :deep(.el-table__fixed-right),
.strategy-tabs :deep(.el-table__fixed-body-wrapper),
.strategy-tabs :deep(.el-table__fixed-right-patch) {
  background: transparent !important;
}

.strategy-tabs :deep(.el-table th.el-table__cell) {
  background: rgba(11, 20, 31, 0.95) !important;
  color: var(--text-secondary);
  border-bottom-color: rgba(108, 117, 137, 0.24);
}

.strategy-tabs :deep(.el-table tr),
.strategy-tabs :deep(.el-table__body tr),
.strategy-tabs :deep(.el-table__body td.el-table__cell) {
  background: rgba(11, 16, 24, 0.72) !important;
  color: var(--text-primary);
}

.strategy-tabs :deep(.el-table--striped .el-table__body tr.el-table__row--striped td.el-table__cell) {
  background: rgba(15, 23, 34, 0.82) !important;
}

.strategy-tabs :deep(.el-table__body tr:hover > td.el-table__cell) {
  background: rgba(56, 189, 248, 0.08) !important;
}

.strategy-tabs :deep(.el-table .el-table__cell) {
  border-bottom-color: rgba(108, 117, 137, 0.22) !important;
}

.strategy-tabs :deep(.el-table__inner-wrapper::before),
.strategy-tabs :deep(.el-table__border-left-patch),
.strategy-tabs :deep(.el-table__border-bottom-patch) {
  background-color: rgba(108, 117, 137, 0.22) !important;
}

.strategy-tabs :deep(.el-table .el-table-fixed-column--left),
.strategy-tabs :deep(.el-table .el-table-fixed-column--right) {
  background: rgba(11, 16, 24, 0.94) !important;
}

.tab-content {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.pagination-container {
  display: flex;
  justify-content: flex-end;
}

.split-layout {
  display: grid;
  grid-template-columns: minmax(0, 1.34fr) minmax(380px, 0.66fr);
  gap: 12px;
  height: calc(100vh - 250px);
  min-height: 680px;
}
.editor-panel {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  overflow: hidden;
  min-width: 0;
  min-height: 0;
}
.editor-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 12px;
  background: #1e1e1e;
  color: #d4d4d4;
  font-size: 12px;
  gap: 8px;
}
.strategy-name-input {
  width: 200px;
}
.strategy-name-input :deep(.el-input__inner) {
  background: #2d2d2d;
  border-color: #404040;
  color: #d4d4d4;
}
.toolbar-actions {
  display: flex;
  gap: 6px;
  flex-shrink: 0;
}
.code-editor {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 560px;
  padding: 8px;
  background: #1e1e1e;
}

.code-editor :deep(.code-editor-shell) {
  flex: 1;
  min-height: 0;
}
.right-panel {
  display: flex;
  min-width: 0;
  min-height: 0;
  flex-direction: column;
  gap: 12px;
  overflow: auto;
}

.upload-area {
  border: 2px dashed #444;
  border-radius: 8px;
  padding: 40px 20px;
  text-align: center;
  cursor: pointer;
  transition: border-color .2s;
}
.upload-area:hover { border-color: #409eff; }
.bt-config-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: #1a1a22;
  border: 1px solid #2a2a35;
  border-radius: 8px;
  font-size: 12px;
  color: #c0c0cc;
  flex-wrap: wrap;
}
.bt-config-bar span {
  color: #999;
}
.bt-config-bar :deep(.el-input__inner) {
  background: #141418;
  border-color: #2a2a35;
  color: #d4d4d4;
}
.bt-config-bar :deep(.el-input__wrapper) {
  background: #141418;
  box-shadow: none;
}
.bt-pool-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 12px;
  background: #16161d;
  border: 1px solid #2a2a35;
  border-radius: 8px;
  flex-wrap: wrap;
}
.bt-pool-bar .pool-label { color: #777; font-size: 12px; margin-right: 4px; }
.bt-pool-bar .pool-count { color: #666; font-size: 11px; margin-left: 8px; }
.bt-pool-bar :deep(.el-tag) {
  background: #1a1a25;
  border-color: #2a2a35;
  color: #aaa;
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
}
.bt-pool-bar :deep(.el-input__inner) {
  background: #141418;
  border-color: #2a2a35;
  color: #c0c0cc;
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
}
.bt-pool-bar :deep(.el-select .el-input__inner) {
  background: #141418;
  border-color: #2a2a35;
  color: #c0c0cc;
}
.bt-pool-bar :deep(.el-select .el-input__wrapper) {
  background: #141418;
  box-shadow: none;
}
.akquant-opt-panel {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 8px 12px;
  background: #15151d;
  border: 1px solid #2a2a35;
  border-radius: 8px;
  color: #9aa0aa;
  font-size: 12px;
}
.opt-row {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}
.opt-grid-input {
  flex: 1;
  min-width: 280px;
}
.opt-short-input {
  width: 130px;
}
.opt-metric-select {
  width: 170px;
}
.opt-number {
  width: 96px;
}
.opt-period-hint {
  color: #767b88;
  font-size: 11px;
}
.opt-progress {
  padding: 4px 0;
}
.opt-progress-label {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 4px;
  color: #c0c0cc;
  font-size: 12px;
}

.bt-run-progress {
  margin: 8px 0;
  padding: 10px 12px;
  border: 1px solid rgba(96, 165, 250, 0.24);
  border-radius: 8px;
  background: rgba(15, 23, 42, 0.38);
}

.bt-run-progress__head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 6px;
  color: var(--text-bright);
  font-size: 12px;
}

.bt-run-progress__meta {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 12px;
  margin-top: 6px;
  color: var(--text-secondary);
  font-size: 11px;
  font-family: var(--font-data, ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace);
}

.opt-result-table {
  border: 1px solid #2a2a35;
  border-radius: 6px;
}
.opt-result-table :deep(.el-table__body-wrapper) {
  max-height: 260px;
}
.bt-metrics-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
.bt-metric-card {
  border: 1px solid #2a2a35;
  border-radius: 6px;
  padding: 10px;
  text-align: center;
  background: #1a1a22;
}
.bt-metric-label { font-size: 10px; color: #8888a0; }
.bt-metric-value { font-size: 16px; font-weight: 700; margin-top: 4px; color: #e2e2ea; }
.positive { color: #d93026; }
.negative { color: #137333; }
.bt-log-panel {
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  background: var(--bg-surface);
  overflow: hidden;
  flex: 1;
  min-height: 120px;
}
.bt-log-content { padding: 8px 12px; font-size: 11px; font-family: monospace; max-height: 200px; overflow: auto; background: #141418; border-radius: 4px; }
.bt-log-line { color: #8888a0; padding: 1px 0; }
.bt-error { color: #e5484d; }
.bt-log-empty { color: #8888a0; font-style: italic; }

/* Mode switch */
.mode-switch { flex-shrink: 0; }
.mode-switch :deep(.el-radio-button__inner) {
  background: #2d2d2d;
  border-color: #404040;
  color: #999;
  padding: 4px 12px;
  font-size: 12px;
}
.mode-switch :deep(.el-radio-button__original-radio:checked + .el-radio-button__inner) {
  background: var(--el-color-primary);
  border-color: var(--el-color-primary);
  color: #fff;
}

/* Expression panel */
.expression-panel {
  flex: 1;
  background: #1e1e1e;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-height: 560px;
}
.expression-input-row {
  display: flex;
  align-items: flex-start;
  gap: 10px;
}

.expression-code-input {
  flex: 1;
  min-width: 0;
}
.factor-select {
  width: 200px;
}
.factor-select :deep(.el-input__inner) {
  background: #2d2d2d;
  border-color: #404040;
  color: #d4d4d4;
}
.expression-hint {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 11px;
  color: #999;
  line-height: 1.5;
}
.hint-examples {
  color: #777;
  font-family: 'JetBrains Mono', monospace;
}
.expression-params {
  display: flex;
  align-items: center;
  gap: 8px;
}
.param-label {
  font-size: 12px;
  color: #aaa;
}

.empty-runner {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 400px;
  color: #888;
  text-align: center;
}
.empty-runner p {
  font-size: 14px;
  line-height: 1.6;
}

@media (max-width: 1320px) {
  .split-layout {
    grid-template-columns: 1fr;
    height: auto;
    min-height: 0;
  }

  .right-panel {
    max-height: none;
  }
}

@media (max-width: 900px) {
  .backtest-hero {
    grid-template-columns: 1fr;
  }

  .page-header-actions {
    justify-content: flex-start;
    flex-wrap: wrap;
  }

  .editor-toolbar {
    align-items: stretch;
    flex-direction: column;
  }

  .strategy-name-input {
    width: 100%;
  }
}
</style>
