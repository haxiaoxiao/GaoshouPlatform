<template>
  <div
    class="page-frame page-container backtest-page"
    :class="`backtest-page--layout-${layoutMode.toLowerCase()}`"
  >
    <div class="strategy-tabs-frame">
      <div class="page-header-actions backtest-action-strip">
        <el-button link type="primary" @click="openDocs">
          <el-icon><Document /></el-icon>
          使用手册
        </el-button>
        <el-dropdown @command="handleCreate">
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
      <el-tabs v-model="activeTab" class="strategy-tabs">
        <el-tab-pane label="策略列表" name="strategyList">
        <div class="strategy-console">
          <section class="strategy-table-panel">
            <div class="console-toolbar">
              <div>
                <strong>策略库</strong>
                <span>{{ total }} 条策略 · {{ btEngine }} · {{ btBarType }}</span>
              </div>
              <el-input v-model="strategySearch" size="small" clearable placeholder="搜索策略" class="strategy-search" />
            </div>
            <el-table v-loading="loading" :data="filteredStrategyList" stripe style="width: 100%">
              <el-table-column prop="id" label="ID" width="72" />
              <el-table-column prop="name" label="策略名称" min-width="210" show-overflow-tooltip />
              <el-table-column prop="description" label="描述" min-width="260" show-overflow-tooltip>
                <template #default="{ row }">
                  {{ row.description || '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="created_at" label="创建时间" width="160">
                <template #default="{ row }">
                  {{ formatDateTime(row.created_at) }}
                </template>
              </el-table-column>
              <el-table-column label="操作" width="168" fixed="right">
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
          </section>

          <aside class="ops-side-panel">
            <div class="ops-side-panel__head">
              <span>RUN SNAPSHOT</span>
              <strong>{{ activeRunStatus }}</strong>
            </div>
            <div class="ops-metric-grid">
              <div v-for="item in operationsMetrics" :key="item.label" class="ops-metric">
                <span>{{ item.label }}</span>
                <strong>{{ item.value }}</strong>
              </div>
            </div>
            <div class="ops-log-tail">
              <span>LOG TAIL</span>
              <small v-for="(log, index) in runnerRailLogs" :key="`${log}-${index}`">{{ log }}</small>
              <small v-if="!runnerRailLogs.length">暂无执行日志</small>
            </div>
            <div class="ops-actions">
              <el-button type="primary" size="small" :disabled="!activeStrategy" @click="activeTab = 'backtestRunner'">
                打开运行台
              </el-button>
              <el-button size="small" @click="loadStrategies" :loading="loading">刷新策略</el-button>
            </div>
          </aside>
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
        <div v-else class="split-layout" :class="`split-layout--${layoutMode.toLowerCase()}`">
          <aside v-if="layoutMode === 'B'" class="runner-rail" aria-label="回测历史与状态轨道">
            <div class="runner-rail__head">
              <span>RUN TREE</span>
              <strong>{{ btTaskId || 'local-draft' }}</strong>
            </div>
            <div class="runner-rail__node is-active">
              <span>01</span>
              <div>
                <strong>{{ activeStrategy?.name || '未命名策略' }}</strong>
                <small>{{ btEngine }} · {{ btBarType }}</small>
              </div>
            </div>
            <div class="runner-rail__node" :class="{ 'is-active': btRunning }">
              <span>02</span>
              <div>
                <strong>{{ btRunning ? '运行中' : '等待运行' }}</strong>
                <small>{{ backtestProgressMessage }}</small>
              </div>
            </div>
            <div class="runner-rail__log">
              <span>LOG TAIL</span>
              <small v-for="(log, index) in runnerRailLogs" :key="`${log}-${index}`">{{ log }}</small>
              <small v-if="!runnerRailLogs.length">暂无执行日志</small>
            </div>
          </aside>
          <div v-if="layoutMode !== 'C'" class="editor-panel">
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
            <div v-if="btEngine === 'akquant'" class="strategy-param-panel">
              <div class="strategy-param-panel__head">
                <span>策略参数 JSON</span>
                <el-button size="small" link @click="resetStrategyParamsText">重置</el-button>
                <el-button size="small" type="primary" link @click="persistActiveStrategyRunSettings">保存参数</el-button>
              </div>
              <el-input
                v-model="strategyParamsText"
                type="textarea"
                :rows="6"
                resize="vertical"
                placeholder='{"top_n":20,"us_overnight_entry_filter":"combined_downside"}'
              />
            </div>
            <section v-if="btFullResult" class="performance-desk">
              <div class="performance-desk__head">
                <div>
                  <span>PERFORMANCE REVIEW</span>
                  <strong>{{ activeStrategy?.name || '回测结果' }}</strong>
                </div>
                <el-button size="small" type="primary" plain @click="showReport = true">完整报告</el-button>
              </div>
              <div class="performance-metrics">
                <div v-for="item in resultSummaryMetrics" :key="item.label" class="performance-metric">
                  <span>{{ item.label }}</span>
                  <strong :style="{ color: item.color }">{{ item.value }}</strong>
                </div>
              </div>
              <div class="performance-meta">
                <span>{{ btFullResult.start_date || btStartDate }} ~ {{ btFullResult.end_date || btEndDate }}</span>
                <span>{{ btFullResult.trades?.length || 0 }} trades</span>
                <span v-if="btFullResult.benchmark_symbol">Benchmark {{ btFullResult.benchmark_symbol }}</span>
              </div>
            </section>
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
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Document, ArrowDown, Loading } from '@element-plus/icons-vue'
import { usePageContext } from '@/app/pageContext'
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

const activeTab = ref('strategyList')
const loading = ref(false)
const strategyList = ref<Strategy[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const saving = ref(false)
const strategySearch = ref('')
type BacktestLayoutMode = 'A' | 'B' | 'C'
const layoutMode = computed<BacktestLayoutMode>(() => {
  if (activeTab.value !== 'backtestRunner') return 'A'
  if (btFullResult.value && !btRunning.value) return 'C'
  if (btRunning.value || btTaskId.value) return 'B'
  return 'A'
})
const backtestWorkspaceLabel = computed(() => {
  if (layoutMode.value === 'C') return '绩效复盘'
  if (layoutMode.value === 'B') return '运行诊断'
  return activeTab.value === 'strategyList' ? '策略控制台' : '策略编辑'
})
const filteredStrategyList = computed(() => {
  const q = strategySearch.value.trim().toLowerCase()
  if (!q) return strategyList.value
  return strategyList.value.filter(item =>
    `${item.id} ${item.name || ''} ${item.description || ''}`.toLowerCase().includes(q)
  )
})


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
    setStrategyParamsText(result.parameters as Record<string, unknown> | null)
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
const strategyParamsText = ref('{}')

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
  if (poolSource.value?.type === 'all_a') return []
  if (poolSource.value) return poolSource.value.symbols
  return btSymbols.value
})
const selectedIndexSymbol = computed(() => poolSource.value?.type === 'index' ? poolSource.value.indexSymbol : undefined)
const selectedUniverseMode = computed(() => {
  if (poolSource.value?.type === 'index') return 'index'
  if (poolSource.value?.type === 'all_a') return 'all_a'
  return 'symbols'
})
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
        type: poolName === 'all' ? 'all_a' : 'pool',
        label: labelMap[poolName] || poolName,
        count: res.symbols.length,
        symbols: poolName === 'all' ? [] : res.symbols,
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
const activeRunStatus = computed(() => {
  if (btRunning.value) return `RUNNING ${Math.round(btProgress.value * 100)}%`
  if (btFullResult.value) return 'COMPLETED'
  if (btErrors.value.length) return 'FAILED'
  return 'IDLE'
})
const formatCompactMoney = (value?: number | null) => {
  if (value == null || Number.isNaN(Number(value))) return '-'
  const n = Number(value)
  if (Math.abs(n) >= 10000) return `${(n / 10000).toFixed(1)}万`
  return n.toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}
const formatCompactPercent = (value?: number | null) =>
  value == null || Number.isNaN(Number(value)) ? '-' : `${(Number(value) * 100).toFixed(2)}%`
const formatCompactNumber = (value?: number | null, digits = 2) =>
  value == null || Number.isNaN(Number(value)) ? '-' : Number(value).toFixed(digits)
const valueTone = (value?: number | null) => {
  if (value == null) return 'var(--bt-text, #22302a)'
  if (value > 0) return 'var(--market-up, #d93026)'
  if (value < 0) return 'var(--market-down, #137333)'
  return 'var(--bt-text, #22302a)'
}
const operationsMetrics = computed(() => {
  const snapshot = btLiveData.value?.metrics_snapshot || {}
  return [
    { label: '策略数', value: String(total.value || strategyList.value.length) },
    { label: '股票池', value: poolSource.value?.count ? `${poolSource.value.count}只` : `${effectiveSymbols.value.length}只` },
    { label: '当前日期', value: btLiveData.value?.current_date || '-' },
    { label: '成交', value: `${snapshot.n_trades ?? btLiveData.value?.trades?.length ?? 0}` },
    { label: '净值点', value: `${btLiveData.value?.equity_curve?.length || btFullResult.value?.nav_series?.length || 0}` },
    { label: '任务号', value: btTaskId.value || '-' },
  ]
})
const resultSummaryMetrics = computed(() => {
  const r = btFullResult.value
  if (!r) return []
  return [
    { label: '累计收益', value: formatCompactPercent(r.total_return), color: valueTone(r.total_return) },
    { label: '年化收益', value: formatCompactPercent(r.annual_return), color: valueTone(r.annual_return) },
    { label: 'Sharpe', value: formatCompactNumber(r.sharpe_ratio ?? r.sharpe, 2), color: 'var(--bt-text, #22302a)' },
    { label: '最大回撤', value: formatCompactPercent(r.max_drawdown), color: 'var(--status-attention, #ef4444)' },
    { label: '胜率', value: formatCompactPercent(r.win_rate), color: valueTone((r.win_rate || 0) - 0.5) },
    { label: '期末资金', value: formatCompactMoney(r.final_capital), color: 'var(--bt-text, #22302a)' },
  ]
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

const pageContextBlocks = computed(() => [
  {
    title: 'Backtest Run',
    rows: [
      { label: '当前标签', value: activeTab.value },
      { label: '工作台状态', value: backtestWorkspaceLabel.value },
      { label: '策略数量', value: `${strategyList.value.length}` },
      { label: '当前策略', value: activeStrategy.value?.name || '未选择' },
      { label: '引擎 / K线', value: `${btEngine.value} / ${btBarType.value}` },
      {
        label: '运行状态',
        value: btRunning.value ? '运行中' : btTaskId.value ? '有结果' : '待提交',
        tone: btRunning.value ? 'warn' : btTaskId.value ? 'good' : 'neutral',
      },
    ],
  },
  {
    title: 'Universe',
    rows: [
      { label: '股票池', value: poolSource.value?.label || (effectiveSymbols.value.length ? '自定义列表' : '未设置') },
      { label: '样本规模', value: poolSource.value?.count ? `${poolSource.value.count} 只` : `${effectiveSymbols.value.length} 只` },
      { label: '指数池', value: selectedIndexSymbol.value || '-' },
      { label: '基准', value: selectedBenchmarkSymbol.value || '-' },
      {
        label: '参数优化',
        value: optimizationRunning.value ? `${Math.round(optimizationProgress.value * 100)}%` : '未运行',
        tone: optimizationRunning.value ? 'warn' : 'neutral',
      },
    ],
  },
])

usePageContext(pageContextBlocks)

const runnerRailLogs = computed(() => [...btLogs.value, ...btErrors.value.map(error => `[错误] ${error}`)].slice(-6))

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
  ...parseStrategyParamsText(),
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

const mergeResultIntoLive = (result: BacktestResultData | null, live: LiveData | null = btLiveData.value): LiveData | null => {
  if (!result) return live
  return {
    current_date: result.end_date || live?.current_date || null,
    events: [
      ...(live?.events || []),
      {
        type: 'backtest_done',
        timestamp: new Date().toISOString(),
        message: '回测完成，交易簿与净值曲线已生成',
      },
    ].slice(-200),
    positions: live?.positions || {},
    metrics_snapshot: {
      ...(live?.metrics_snapshot || {}),
      total_return: result.total_return,
      max_drawdown: result.max_drawdown,
      sharpe: result.sharpe_ratio ?? result.sharpe,
      cash: result.final_capital,
      total_value: result.final_capital,
      n_trades: result.total_trades ?? result.trades?.length,
    },
    trades: result.trades || live?.trades || [],
    orders: live?.orders || [],
    equity_curve: result.nav_series || live?.equity_curve || [],
    metadata: {
      ...(live?.metadata || {}),
      phase: 'completed',
      progress_message: '回测完成',
      bar_type: btBarType.value,
    },
  }
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
  const strategyParams = parseStrategyParamsText()
  const runtimeStrategyParams = { ...strategyParams }
  delete runtimeStrategyParams.backtest_settings
  const riskConfig = strategyParams.risk_config && typeof strategyParams.risk_config === 'object'
    ? strategyParams.risk_config as Record<string, unknown>
    : undefined
  delete runtimeStrategyParams.risk_config
  return {
    engine,
    mode: isExpression ? 'vectorized' : 'event_driven',
    factor_expression: isExpression ? code : undefined,
    strategy_code: !isExpression && engine === 'akquant' ? code : undefined,
    strategy_id: activeStrategy.value?.id || undefined,
    buy_condition: !isExpression && engine === 'builtin' ? code : undefined,
    symbols: effectiveSymbols.value,
    universe_mode: selectedUniverseMode.value,
    index_symbol: selectedIndexSymbol.value,
    start_date: btStartDate.value,
    end_date: btEndDate.value,
    initial_capital: btCapital.value,
    rebalance_freq: btFrequency.value,
    n_groups: isExpression ? btNGroups.value : 5,
    bar_type: btBarType.value,
    benchmark_symbol: selectedBenchmarkSymbol.value || undefined,
    timer_times: Array.isArray(strategyParams.timer_times) ? strategyParams.timer_times : undefined,
    strategy_params: !isExpression && engine === 'akquant' ? runtimeStrategyParams : undefined,
    risk_config: riskConfig,
    max_positions: typeof riskConfig?.max_positions === 'number'
      ? riskConfig.max_positions
      : typeof strategyParams.max_positions === 'number'
        ? strategyParams.max_positions
        : undefined,
    lot_size: typeof strategyParams.lot_size === 'number' ? strategyParams.lot_size : undefined,
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
          btLiveData.value = mergeResultIntoLive(data as BacktestResultData, btLiveData.value)
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
        btLiveData.value = mergeResultIntoLive(btFullResult.value, btLiveData.value)
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
    setStrategyParamsText((row.parameters || {}) as Record<string, unknown>)
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
        btLiveData.value = mergeResultIntoLive(btFullResult.value, btLiveData.value)
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

const stripRunSettings = (parameters?: Record<string, unknown> | null) => {
  const source = { ...(parameters || {}) }
  delete source.backtest_settings
  return source
}

const setStrategyParamsText = (parameters?: Record<string, unknown> | null) => {
  strategyParamsText.value = JSON.stringify(stripRunSettings(parameters), null, 2)
}

const resetStrategyParamsText = () => {
  setStrategyParamsText((activeStrategy.value?.parameters || {}) as Record<string, unknown>)
}

const parseStrategyParamsText = () => {
  try {
    const parsed = JSON.parse(strategyParamsText.value || '{}')
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? parsed as Record<string, unknown>
      : {}
  } catch (e: any) {
    ElMessage.error(`策略参数 JSON 格式错误: ${e?.message || e}`)
    throw e
  }
}

const seedTechSmallCapStrategy = async () => {
  const expectedNames = ['科技小市值 TSMF - 入场过滤放松风控', '科技小市值 TSMF - 美股入场过滤']
  if (expectedNames.every(name => strategyList.value.some(s => s.name === name))) return
  try {
    const { backtestEngines } = await import('@/api/backtest')
    await backtestEngines.createTechSmallCapStrategy('entry_filter_relaxed_risk')
    await backtestEngines.createTechSmallCapStrategy('us_entry_filter_combined')
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
  await seedTechSmallCapStrategy()
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
  --bt-ivory: #fdfbf7;
  --bt-card: #f5f2ea;
  --bt-card-strong: #ebe7dc;
  --bt-border: #e5dfd3;
  --bt-border-strong: rgba(27, 61, 50, 0.24);
  --bt-text: #22302a;
  --bt-secondary: #54635c;
  --bt-muted: #7e8d86;
  --bt-pine: #1b3d32;
  --bt-pine-2: #355e4f;
  --bt-good: #2d6a4f;
  --bt-warn: #b27a1e;
  --bt-risk: #a83232;
  gap: var(--space-4);
  padding: 14px;
  color: var(--bt-text);
  background:
    linear-gradient(rgba(34, 48, 42, 0.026) 1px, transparent 1px),
    linear-gradient(90deg, rgba(34, 48, 42, 0.022) 1px, transparent 1px),
    radial-gradient(circle at 18% 8%, rgba(238, 243, 240, 0.9), transparent 32%),
    linear-gradient(180deg, rgba(253, 251, 247, 0.98), rgba(245, 242, 234, 0.76));
  background-size: 56px 56px, 56px 56px, auto, auto;
}

.page-header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.backtest-action-strip {
  position: absolute;
  top: 8px;
  right: 12px;
  z-index: 2;
  min-height: 32px;
  flex-shrink: 0;
  padding: 0;
}

.strategy-tabs-frame {
  position: relative;
  min-height: 0;
  flex: 1;
  display: flex;
  flex-direction: column;
  background: rgba(253, 251, 247, 0.78);
  border: 1px solid var(--bt-border);
  border-radius: 18px;
  padding: 10px 12px 12px;
  overflow: hidden;
  box-shadow: 0 18px 42px rgba(27, 61, 50, 0.08);
}

.strategy-tabs {
  min-height: 0;
  flex: 1;
  display: flex;
  flex-direction: column;
}

.strategy-tabs :deep(.el-tabs__header) {
  margin: 0;
  padding: 8px 390px 8px 10px;
  background: rgba(245, 242, 234, 0.72);
}

.strategy-tabs :deep(.el-tabs__nav-scroll) {
  display: flex;
}

.strategy-tabs :deep(.el-tabs__nav) {
  display: inline-flex;
  gap: 4px;
  padding: 3px;
  border: 1px solid rgba(27, 61, 50, 0.16);
  border-radius: 8px;
  background: rgba(253, 251, 247, 0.78);
}

.strategy-tabs :deep(.el-tabs__nav-wrap::after) {
  display: none;
}

.strategy-tabs :deep(.el-tabs__item) {
  height: 30px;
  padding: 0 15px;
  border-radius: 6px;
  color: var(--bt-secondary);
  font-size: 13px;
  font-weight: 800;
  line-height: 30px;
  transition: background 0.16s ease, color 0.16s ease, box-shadow 0.16s ease;
}

.strategy-tabs :deep(.el-tabs__item:hover) {
  color: var(--bt-text);
  background: rgba(238, 243, 240, 0.72);
}

.strategy-tabs :deep(.el-tabs__item.is-active) {
  color: var(--bt-ivory);
  background: var(--bt-pine);
  box-shadow: 0 4px 12px rgba(27, 61, 50, 0.18);
}

.strategy-tabs :deep(.el-tabs__active-bar) {
  display: none;
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
  --el-table-bg-color: rgba(253, 251, 247, 0.86);
  --el-table-tr-bg-color: rgba(253, 251, 247, 0.86);
  --el-table-header-bg-color: rgba(245, 242, 234, 0.95);
  --el-table-row-hover-bg-color: rgba(238, 243, 240, 0.72);
  --el-table-border-color: var(--bt-border);
  --el-table-text-color: var(--bt-text);
  --el-table-header-text-color: var(--bt-secondary);
  background: rgba(253, 251, 247, 0.86) !important;
  color: var(--bt-text);
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
  background: rgba(245, 242, 234, 0.95) !important;
  color: var(--bt-secondary);
  border-bottom-color: var(--bt-border);
}

.strategy-tabs :deep(.el-table tr),
.strategy-tabs :deep(.el-table__body tr),
.strategy-tabs :deep(.el-table__body td.el-table__cell) {
  background: rgba(253, 251, 247, 0.86) !important;
  color: var(--bt-text);
}

.strategy-tabs :deep(.el-table--striped .el-table__body tr.el-table__row--striped td.el-table__cell) {
  background: rgba(245, 242, 234, 0.62) !important;
}

.strategy-tabs :deep(.el-table__body tr:hover > td.el-table__cell) {
  background: rgba(238, 243, 240, 0.74) !important;
}

.strategy-tabs :deep(.el-table .el-table__cell) {
  border-bottom-color: var(--bt-border) !important;
}

.strategy-tabs :deep(.el-table__inner-wrapper::before),
.strategy-tabs :deep(.el-table__border-left-patch),
.strategy-tabs :deep(.el-table__border-bottom-patch) {
  background-color: var(--bt-border) !important;
}

.strategy-tabs :deep(.el-table .el-table-fixed-column--left),
.strategy-tabs :deep(.el-table .el-table-fixed-column--right) {
  background: rgba(253, 251, 247, 0.96) !important;
}

.tab-content {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.strategy-console {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(280px, 320px);
  gap: 12px;
  height: 100%;
  min-height: 0;
  overflow: hidden;
}

.strategy-table-panel,
.ops-side-panel {
  min-width: 0;
  min-height: 0;
  border: 1px solid var(--bt-border);
  border-radius: 8px;
  background: rgba(253, 251, 247, 0.74);
}

.strategy-table-panel {
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.strategy-table-panel :deep(.el-table) {
  flex: 1;
  min-height: 0;
}

.strategy-table-panel :deep(.el-table__body-wrapper) {
  overflow-y: auto;
}

.console-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 10px;
  border-bottom: 1px solid var(--bt-border);
  background: rgba(245, 242, 234, 0.82);
}

.console-toolbar > div {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 10px;
}

.console-toolbar strong {
  color: var(--bt-text);
  font-size: var(--text-sm);
}

.console-toolbar span {
  color: var(--bt-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  white-space: nowrap;
}

.strategy-search {
  width: 180px;
  flex-shrink: 0;
}

.ops-side-panel {
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 10px;
  overflow: hidden;
}

.ops-side-panel__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--bt-border);
}

.ops-side-panel__head span,
.ops-log-tail > span {
  color: var(--bt-muted);
  font-family: var(--font-data);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.06em;
}

.ops-side-panel__head strong {
  color: var(--bt-pine);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.ops-metric-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
}

.ops-metric {
  display: grid;
  gap: 2px;
  min-width: 0;
  padding: 7px 8px;
  border: 1px solid rgba(27, 61, 50, 0.11);
  border-radius: 6px;
  background: rgba(245, 242, 234, 0.72);
}

.ops-metric span {
  color: var(--bt-muted);
  font-size: 10px;
}

.ops-metric strong {
  overflow: hidden;
  color: var(--bt-text);
  font-family: var(--font-data);
  font-size: var(--text-sm);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.ops-log-tail {
  display: flex;
  min-height: 0;
  flex: 1;
  flex-direction: column;
  gap: 6px;
  overflow: auto;
  padding: 8px;
  border: 1px solid rgba(27, 61, 50, 0.1);
  border-radius: 6px;
  background: rgba(253, 251, 247, 0.66);
}

.ops-log-tail small {
  color: var(--bt-secondary);
  font-family: var(--font-data);
  font-size: 10px;
  line-height: 1.45;
}

.ops-actions {
  display: flex;
  justify-content: flex-end;
  gap: 6px;
}

.pagination-container {
  display: flex;
  justify-content: flex-end;
  padding: 8px 0 0;
}

.split-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(420px, 1fr);
  gap: 12px;
  height: calc(100vh - 326px);
  min-height: 620px;
}

.split-layout--b {
  grid-template-columns: minmax(210px, 0.25fr) minmax(0, 0.98fr) minmax(360px, 0.77fr);
}

.split-layout--c {
  grid-template-columns: minmax(320px, 0.54fr) minmax(520px, 1.46fr);
}
.editor-panel {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--bt-border-strong);
  border-radius: 14px;
  overflow: hidden;
  min-width: 0;
  min-height: 0;
  background: #17211d;
  box-shadow: inset 0 1px 0 rgba(253, 251, 247, 0.08);
}
.editor-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 9px 12px;
  background: #18231f;
  color: #dfe8e1;
  font-size: 12px;
  gap: 8px;
  border-bottom: 1px solid rgba(253, 251, 247, 0.08);
}
.strategy-name-input {
  width: 200px;
}
.strategy-name-input :deep(.el-input__inner) {
  color: #dfe8e1;
}

.strategy-name-input :deep(.el-input__wrapper) {
  background: #20322b;
  box-shadow: 0 0 0 1px rgba(253, 251, 247, 0.12) inset;
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
  min-height: 480px;
  padding: 8px;
  background:
    linear-gradient(180deg, rgba(27, 61, 50, 0.16), transparent 36%),
    #151d1a;
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
  border: 1px solid var(--bt-border);
  border-radius: 14px;
  padding: 10px;
  background: rgba(253, 251, 247, 0.68);
}

.split-layout--c .right-panel {
  order: -1;
  background:
    linear-gradient(180deg, rgba(238, 243, 240, 0.86), rgba(253, 251, 247, 0.62)),
    rgba(253, 251, 247, 0.86);
}

.split-layout--c .editor-panel {
  min-height: 0;
}

.upload-area {
  border: 2px dashed var(--bt-border-strong);
  border-radius: 14px;
  padding: 40px 20px;
  text-align: center;
  cursor: pointer;
  transition: border-color .2s;
  background: rgba(253, 251, 247, 0.72);
}
.upload-area:hover { border-color: var(--bt-pine); }
.bt-config-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 12px;
  background: rgba(245, 242, 234, 0.82);
  border: 1px solid var(--bt-border);
  border-radius: 12px;
  font-size: var(--text-xs);
  color: var(--bt-secondary);
  flex-wrap: wrap;
}
.bt-config-bar span {
  color: var(--bt-secondary);
}
.bt-config-bar :deep(.el-input__inner) {
  color: var(--bt-text);
}
.bt-config-bar :deep(.el-input__wrapper) {
  background: rgba(253, 251, 247, 0.86);
  box-shadow: 0 0 0 1px var(--bt-border) inset;
}
.strategy-param-panel {
  padding: 10px 12px;
  background: rgba(245, 242, 234, 0.72);
  border: 1px solid var(--bt-border);
  border-radius: 12px;
}
.strategy-param-panel__head {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
  color: var(--bt-secondary);
  font-size: var(--text-xs);
}
.strategy-param-panel :deep(textarea) {
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 12px;
}
.bt-pool-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 12px;
  background: rgba(245, 242, 234, 0.72);
  border: 1px solid var(--bt-border);
  border-radius: 12px;
  flex-wrap: wrap;
}
.bt-pool-bar .pool-label { color: var(--bt-muted); font-size: var(--text-xs); margin-right: 4px; }
.bt-pool-bar .pool-count { color: var(--bt-muted); font-size: var(--text-xs); margin-left: 8px; }
.bt-pool-bar :deep(.el-tag) {
  background: #eef3f0;
  border-color: rgba(27, 61, 50, 0.18);
  color: var(--bt-pine);
  font-family: 'JetBrains Mono', monospace;
  font-size: var(--text-xs);
}
.bt-pool-bar :deep(.el-input__inner) {
  color: var(--bt-text);
  font-family: 'JetBrains Mono', monospace;
  font-size: var(--text-xs);
}
.bt-pool-bar :deep(.el-input__wrapper) {
  background: rgba(253, 251, 247, 0.86);
  box-shadow: 0 0 0 1px var(--bt-border) inset;
}
.bt-pool-bar :deep(.el-select .el-input__inner) {
  color: var(--bt-text);
}
.bt-pool-bar :deep(.el-select .el-input__wrapper) {
  background: rgba(253, 251, 247, 0.86);
  box-shadow: 0 0 0 1px var(--bt-border) inset;
}
.akquant-opt-panel {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 8px 12px;
  background: rgba(245, 242, 234, 0.72);
  border: 1px solid var(--bt-border);
  border-radius: 12px;
  color: var(--bt-secondary);
  font-size: var(--text-xs);
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
  color: var(--bt-muted);
  font-size: var(--text-xs);
}
.opt-progress {
  padding: 4px 0;
}
.opt-progress-label {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 4px;
  color: var(--bt-secondary);
  font-size: var(--text-xs);
}

.bt-run-progress {
  margin: 8px 0;
  padding: 10px 12px;
  border: 1px solid rgba(27, 61, 50, 0.18);
  border-radius: 12px;
  background: rgba(238, 243, 240, 0.76);
}

.bt-run-progress__head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 6px;
  color: var(--bt-text);
  font-size: var(--text-xs);
}

.bt-run-progress__meta {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 12px;
  margin-top: 6px;
  color: var(--bt-secondary);
  font-size: var(--text-xs);
  font-family: var(--font-data, ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace);
}

.opt-result-table {
  border: 1px solid var(--bt-border);
  border-radius: 10px;
}
.opt-result-table :deep(.el-table__body-wrapper) {
  max-height: 260px;
}
.bt-metrics-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
.bt-metric-card {
  border: 1px solid var(--bt-border);
  border-radius: 10px;
  padding: 10px;
  text-align: center;
  background: rgba(253, 251, 247, 0.74);
}
.bt-metric-label { font-size: var(--text-xs); color: var(--bt-muted); }
.bt-metric-value { font-size: var(--text-base); font-weight: 700; margin-top: 4px; color: var(--bt-text); }
.positive { color: var(--market-up); }
.negative { color: var(--market-down); }
.bt-log-panel {
  border: 1px solid var(--bt-border);
  border-radius: 12px;
  background: rgba(253, 251, 247, 0.72);
  overflow: hidden;
  flex: 1;
  min-height: 120px;
}
.bt-log-content { padding: 8px 12px; font-size: var(--text-xs); font-family: monospace; max-height: 200px; overflow: auto; background: #17211d; border-radius: 8px; }
.bt-log-line { color: #b8c8bf; padding: 1px 0; }
.bt-error { color: var(--bt-risk); }
.bt-log-empty { color: var(--bt-muted); font-style: italic; }

/* Mode switch */
.mode-switch { flex-shrink: 0; }
.mode-switch :deep(.el-radio-button__inner) {
  background: rgba(253, 251, 247, 0.82);
  border-color: var(--bt-border);
  color: var(--bt-secondary);
  padding: 4px 12px;
  font-size: var(--text-xs);
}
.mode-switch :deep(.el-radio-button__original-radio:checked + .el-radio-button__inner) {
  background: var(--bt-pine);
  border-color: var(--bt-pine);
  color: var(--bt-ivory);
}

/* Expression panel */
.expression-panel {
  flex: 1;
  background:
    linear-gradient(180deg, rgba(27, 61, 50, 0.16), transparent 36%),
    #151d1a;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-height: 480px;
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
  color: #d4d4d4;
}
.factor-select :deep(.el-input__wrapper) {
  background: #20322b;
  box-shadow: 0 0 0 1px rgba(253, 251, 247, 0.12) inset;
}
.expression-hint {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: var(--text-xs);
  color: #b8c8bf;
  line-height: 1.5;
}
.hint-examples {
  color: #91a39a;
  font-family: 'JetBrains Mono', monospace;
}
.expression-params {
  display: flex;
  align-items: center;
  gap: 8px;
}
.param-label {
  font-size: var(--text-xs);
  color: #b8c8bf;
}

.empty-runner {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 400px;
  color: var(--bt-muted);
  text-align: center;
  border: 1px dashed var(--bt-border);
  border-radius: 16px;
  background: rgba(253, 251, 247, 0.72);
}
.empty-runner p {
  font-size: 14px;
  line-height: 1.6;
}

.runner-rail {
  display: flex;
  min-width: 0;
  min-height: 0;
  flex-direction: column;
  gap: 10px;
  padding: 12px;
  border: 1px solid rgba(27, 61, 50, 0.22);
  border-radius: 14px;
  background:
    linear-gradient(180deg, rgba(27, 61, 50, 0.95), rgba(26, 36, 32, 0.96)),
    #1a2420;
  color: #e8efe9;
  overflow: hidden;
}

.runner-rail__head,
.runner-rail__node,
.runner-rail__log {
  border: 1px solid rgba(253, 251, 247, 0.12);
  border-radius: 12px;
  background: rgba(253, 251, 247, 0.055);
}

.runner-rail__head {
  display: grid;
  gap: 4px;
  padding: 10px 11px;
}

.runner-rail__head span,
.runner-rail__log span {
  color: #9fc2ae;
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 800;
  letter-spacing: 0.08em;
}

.runner-rail__head strong {
  font-family: var(--font-data);
  font-size: var(--text-sm);
  word-break: break-all;
}

.runner-rail__node {
  display: grid;
  grid-template-columns: 34px minmax(0, 1fr);
  gap: 9px;
  padding: 10px;
}

.runner-rail__node > span {
  display: grid;
  width: 30px;
  height: 30px;
  place-items: center;
  border-radius: 999px;
  background: rgba(253, 251, 247, 0.08);
  color: #9fc2ae;
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.runner-rail__node.is-active {
  border-color: rgba(159, 194, 174, 0.42);
  background: rgba(238, 243, 240, 0.1);
}

.runner-rail__node strong,
.runner-rail__node small {
  display: block;
  min-width: 0;
}

.runner-rail__node strong {
  overflow: hidden;
  color: #fdfbf7;
  font-size: var(--text-sm);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.runner-rail__node small {
  margin-top: 3px;
  color: #b8c8bf;
  font-size: var(--text-xs);
  line-height: 1.4;
}

.runner-rail__log {
  display: flex;
  min-height: 0;
  flex: 1;
  flex-direction: column;
  gap: 7px;
  padding: 10px 11px;
  overflow: auto;
}

.runner-rail__log small {
  color: #c9d8cf;
  font-family: var(--font-data);
  font-size: var(--text-xs);
  line-height: 1.45;
  word-break: break-word;
}

.backtest-page :deep(.el-button--primary) {
  --el-button-bg-color: var(--bt-pine);
  --el-button-border-color: var(--bt-pine);
  --el-button-hover-bg-color: var(--bt-pine-2);
  --el-button-hover-border-color: var(--bt-pine-2);
}

.backtest-page :deep(.el-input__wrapper),
.backtest-page :deep(.el-select__wrapper),
.backtest-page :deep(.el-textarea__inner) {
  background: rgba(253, 251, 247, 0.88);
  box-shadow: 0 0 0 1px var(--bt-border) inset;
}

.right-panel :deep(.running-panel) {
  max-height: none;
  padding: 10px;
  border: 1px solid var(--bt-border);
  border-radius: 12px;
  background: rgba(253, 251, 247, 0.74);
}

.right-panel :deep(.mini-card) {
  background: rgba(245, 242, 234, 0.9);
  border: 1px solid var(--bt-border);
}

.right-panel :deep(.mini-label),
.right-panel :deep(.empty-hint) {
  color: var(--bt-muted);
}

.right-panel :deep(.panel-tabs) {
  border-bottom-color: var(--bt-border);
}

.right-panel :deep(.panel-tab) {
  color: var(--bt-secondary);
}

.right-panel :deep(.panel-tab.active) {
  color: var(--bt-pine);
  border-bottom-color: var(--bt-pine);
}

.right-panel :deep(.metric-label) {
  color: var(--bt-secondary);
}

.right-panel :deep(.log-stream),
.right-panel :deep(.event-stream) {
  color: var(--bt-secondary);
}

.right-panel :deep(.panel-footer) {
  background: rgba(253, 251, 247, 0.86);
}

@media (max-width: 1320px) {
  .split-layout,
  .split-layout--b,
  .split-layout--c {
    grid-template-columns: 1fr;
    height: auto;
    min-height: 0;
  }

  .split-layout--c .right-panel {
    order: 0;
  }

  .right-panel {
    max-height: none;
  }

  .runner-rail {
    max-height: 280px;
  }
}

@media (max-width: 900px) {
  .page-header-actions {
    justify-content: flex-start;
    flex-wrap: wrap;
  }

  .backtest-action-strip {
    position: static;
    order: 0;
    padding: 8px 10px 0;
  }

  .strategy-tabs {
    order: 1;
  }

  .strategy-tabs-frame {
    gap: 8px;
  }

  .strategy-tabs :deep(.el-tabs__header) {
    padding: 0 10px 8px;
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
