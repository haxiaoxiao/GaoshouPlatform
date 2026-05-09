<template>
  <div class="page-container">
    <div class="page-header">
      <h2>策略回测</h2>
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
        <el-button type="success" @click="showUploadDialog = true" style="margin-left:4px">
          上传研报
        </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="script">新建脚本策略</el-dropdown-item>
              <el-dropdown-item command="expression">新建表达式策略</el-dropdown-item>
              <el-dropdown-item command="builtin">新建内置策略(深度价值)</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
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
        <BacktestList ref="backtestListRef" />
      </el-tab-pane>

      <el-tab-pane label="回测运行" name="backtestRunner">
        <div v-if="!activeStrategy" class="empty-runner">
          <p>请先在"策略列表"中选择一个策略，然后点击"回测"按钮进入回测运行界面</p>
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
                <el-button size="small" type="primary" @click="handleRunBacktest" :loading="btRunning">编译运行</el-button>
              </div>
            </div>

            <!-- akquant Python 代码编辑器 -->
            <div class="code-editor" v-if="editorTab === 'akquant-code'">
              <div class="expression-hint" style="margin-bottom:4px">
                <span>akquant Strategy: class MyStrategy(aq.Strategy) → def on_bar(self, bar)</span>
              </div>
              <textarea v-model="btCode" class="editor-textarea" spellcheck="false" :placeholder="codePlaceholder" />
            </div>

            <!-- RQAlpha Python 代码编辑器 -->
            <div class="code-editor" v-if="editorTab === 'rqalpha-code'">
              <div class="expression-hint" style="margin-bottom:4px">
                <span>RQAlpha 语法: def init(context) + def handle_bar(context, bar_dict)</span>
              </div>
              <textarea v-model="btCode" class="editor-textarea" spellcheck="false" :placeholder="codePlaceholder" />
            </div>

            <!-- 表达式输入 -->
            <div class="expression-panel" v-if="editorTab === 'expression'">
              <div class="expression-input-row">
                <el-input v-model="btExpression" size="default" class="expression-input"
                  placeholder="输入因子表达式，如: close/MA(close, 20) - 1" clearable />
                <el-select v-model="selectedFactorId" size="default" placeholder="从因子研究选择"
                  clearable class="factor-select" @change="handleFactorSelect">
                  <el-option v-for="f in savedFactors" :key="f.id" :label="f.name" :value="f.id" />
                </el-select>
              </div>
              <div class="expression-hint">
                <span>向量化回测：表达式按日计算因子值 → 分层买入 → 计算分组收益</span>
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

            <!-- 内置策略面板 -->
            <div class="builtin-panel" v-if="editorTab === 'builtin' && builtinType === 'deep_value'">
              <div class="builtin-params-bar">
                <span class="param-label">选股池</span>
                <el-select v-model="dvPool" size="small" style="width:120px">
                  <el-option label="全量A股" value="all" />
                  <el-option label="Top100" value="top100" />
                  <el-option label="Top300" value="top300" />
                  <el-option label="Top500" value="top500" />
                </el-select>
                <span class="param-label">PE</span>
                <el-input-number v-model="dvPeMin" :min="0" :max="100" size="small" style="width:60px" />—
                <el-input-number v-model="dvPeMax" :min="0" :max="1000" size="small" style="width:60px" />
                <span class="param-label">股息&gt;</span>
                <el-input-number v-model="dvDivMin" :min="0" :max="20" :step="0.5" size="small" style="width:70px" />%
                <span class="param-label">价/MA&lt;</span>
                <el-input-number v-model="dvPriceMA" :min="0.1" :max="1.0" :step="0.05" size="small" style="width:70px" />
                <span class="param-label">持仓</span>
                <el-input-number v-model="dvMaxPos" :min="1" :max="20" size="small" style="width:60px" />
                <span class="param-label">仓位</span>
                <el-input-number v-model="dvSinglePct" :min="5" :max="50" :step="5" size="small" style="width:60px" />%
              </div>
              <div class="code-editor">
                <textarea v-model="builtinCode" class="editor-textarea" spellcheck="false" readonly />
              </div>
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
              <el-select v-model="btBarType" size="small" style="width:80px">
                <el-option label="日线" value="daily" />
                <el-option label="分钟" value="minute" />
              </el-select>
              <span>引擎</span>
              <el-select v-model="btEngine" size="small" style="width:100px" @change="onEngineChange">
                <el-option v-for="e in engineOptions" :key="e.value" :label="e.label" :value="e.value" />
              </el-select>
              <el-button type="primary" size="small" @click="handleRunBacktest" :loading="btRunning">运行回测</el-button>
            </div>
            <div class="bt-pool-bar">
              <span class="pool-label">股票池</span>
              <template v-if="isAllStocks">
                <el-tag size="small" type="success" closable @close="clearAllStocks">全量A股 ({{ allStocksCount }} 只)</el-tag>
              </template>
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
              <el-button size="small" text @click="showWatchlistPicker = true" v-if="!isAllStocks">从自选池导入</el-button>
              <el-select
                v-model="selectedPool"
                size="small"
                placeholder="指数池"
                style="width:130px"
                @change="loadPoolSymbols"
                clearable
              >
                <el-option label="沪深Top100" value="top100" />
                <el-option label="沪深Top300" value="top300" />
                <el-option label="沪深Top500" value="top500" />
                <el-option label="全量A股" value="all" />
              </el-select>
              <el-button size="small" text type="danger" @click="clearAllStocks" v-if="isAllStocks || btSymbols.length > 0">
                清空 ({{ isAllStocks ? allStocksCount : btSymbols.length }})
              </el-button>
              <span class="pool-count">{{ isAllStocks ? allStocksCount : btSymbols.length }} 只</span>
            </div>

            <Teleport to="body">
              <el-dialog v-model="showWatchlistPicker" title="从自选池导入" width="500px">
                <el-select v-model="selectedWatchlistGroup" placeholder="选择分组" style="width:100%" @change="loadWatchlistStocks">
                  <el-option v-for="g in watchlistGroups" :key="g.id" :label="g.name + ' (' + g.stock_count + '只)'" :value="g.id" />
                </el-select>
                <div v-if="watchlistStocks.length > 0" style="margin-top:12px">
                  <el-button size="small" text @click="selectedWatchlistStocks = watchlistStocks.map(s => s.symbol)">全选</el-button>
                  <el-button size="small" text @click="selectedWatchlistStocks = []">取消全选</el-button>
                  <span style="color:#888;font-size:12px;margin-left:8px">已选 {{ selectedWatchlistStocks.length }}/{{ watchlistStocks.length }}</span>
                </div>
                <div style="margin-top:8px;max-height:260px;overflow-y:auto">
                  <el-checkbox-group v-model="selectedWatchlistStocks">
                    <div v-for="s in watchlistStocks" :key="s.symbol" style="padding:2px 0">
                      <el-checkbox :value="s.symbol" :label="s.symbol">
                        {{ s.symbol }} <span style="color:#888;font-size:12px">{{ s.stock_name }}</span>
                      </el-checkbox>
                    </div>
                  </el-checkbox-group>
                </div>
                <template #footer>
                  <el-button size="small" @click="showWatchlistPicker = false">取消</el-button>
                  <el-button size="small" @click="replaceWithWatchlistStocks" type="warning">替换为所选</el-button>
                  <el-button size="small" type="primary" @click="applyWatchlistStocks">添加到股票池</el-button>
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
import { ref, reactive, computed, onMounted, nextTick, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Document, ArrowDown, Loading } from '@element-plus/icons-vue'
import { strategyApi, type Strategy, type LiveData, type TaskStatus, type BacktestResultData } from '@/api/backtest'
import { factorApi, type Factor } from '@/api/factor'
import { watchlistApi, type WatchlistGroup, type WatchlistStock } from '@/api/data'
import BacktestList from './BacktestList.vue'
import RunningPanel from './RunningPanel.vue'
import ReportOverlay from './ReportOverlay.vue'
import LLMStrategyPanel from './LLMStrategyPanel.vue'
import { formatDateTime } from '@/utils/format'

const SAMPLE_CODE = `def init(context):
    # 策略参数
    context.fast = 5
    context.slow = 20

    # 运行信息
    log(f"回测区间: {context.run_info.start_date} ~ {context.run_info.end_date}")
    log(f"初始资金: {context.run_info.capital:,.0f}")
    log(f"标的: {context.run_info.symbols}")

def handle_bar(context, bar_dict):
    # bar_dict 包含当日所有标的的 Bar，每天触发一次
    for symbol in bar_dict:
        bar = bar_dict[symbol]

        # 跳过停牌
        if bar.suspended or bar.isnan:
            continue

        # 获取历史收盘价序列
        hist = context.get_history(symbol, 252)
        if hist.empty or len(hist) < context.slow:
            continue
        close = hist['close']

        # 计算快慢均线
        mf = MA(close, context.fast)
        ms = MA(close, context.slow)

        # 金叉买入
        if CROSS(mf, ms).iloc[-1] == 1:
            if not context.portfolio.get_position(symbol):
                context.order_value(symbol, context.portfolio.total_value * 0.2)

        # 死叉卖出
        elif CROSS(mf, ms).iloc[-1] == -1:
            pos = context.portfolio.get_position(symbol)
            if pos and pos.total_shares > 0:
                context.order_shares(symbol, -pos.total_shares)

    # 定期输出状态
    now = context.now
    if now.day % 30 == 0:
        nav = context.portfolio.unit_net_value
        log(f"[{now.date()}] nav={nav:.3f} cash={context.stock_account.available_cash:,.0f}")`

const SAMPLE_EXPRESSION = 'close / MA(close, 20) - 1'

const DEEP_VALUE_CODE = `def init(context):
    # 深度价值策略参数 (年度调仓)
    context.price_to_ma_max = 0.9      # 股价 < 250周线 * 0.9
    context.dividend_yield_min = 3.5   # 股息率 > 3.5%
    context.pe_min = 0                 # PE > 0 (排除亏损)
    context.pe_max = 40                # PE < 40
    context.max_positions = 10         # 最多持仓数
    context.single_position_pct = 0.10 # 单票10%仓位
    context.rebalance_month = 5        # 每年5月调仓 (Q1财报季结束后)

    # 运行状态
    context.positions = {}             # {symbol: {"entry_price": x, "entry_year": y}}
    context.last_rebalance_year = None # 上次调仓年份

    log("深度价值策略(年度): 股价<250周线{:.0%} 股息率>{}% 0<PE<{} 每年5月调仓".format(
        context.price_to_ma_max, context.dividend_yield_min, context.pe_max
    ))

def handle_bar(context, bar_dict):
    today = context.now.date()

    # ── 年度调仓判断：仅5月的第一个交易日触发 ──
    if today.month < context.rebalance_month:
        return
    if context.last_rebalance_year == today.year:
        return
    context.last_rebalance_year = today.year
    log("=" * 40)
    log(f"[{today}] 年度调仓开始")

    # ── 到期平仓：上一年度的所有持仓全部清仓 ──
    expired = list(context.positions.keys())
    for sym in expired:
        p = context.portfolio.get_position(sym)
        if p and p.total_shares > 0:
            order_shares(sym, -p.total_shares)
            log(f"到期平仓 {sym}")
        del context.positions[sym]

    # ── 筛选候选 (全量A股, 非ST非退市) ──
    candidates = []
    all_symbols = context.get_all_symbols()
    for sym in all_symbols:
        try:
            close = context.get_daily_close(sym, today)
            if not close or close <= 0:
                continue

            ma250w = context.get_weekly_ma(sym, 250, today)
            if not ma250w or ma250w <= 0:
                continue

            # 条件1: 价格 < 250周线 * 0.9
            ratio = close / ma250w
            if ratio >= context.price_to_ma_max:
                continue

            # 条件2: PE > 0 and PE < 40
            pe = context.get_indicator(sym, "pe_ttm", today)
            if pe is None or pe <= context.pe_min or pe >= context.pe_max:
                continue

            # 条件3: 股息率 > 3.5%
            div_yield = context.get_indicator(sym, "dividend_yield", today)
            if div_yield is None or div_yield <= context.dividend_yield_min:
                continue

            # 评分: 折价维度(0~50) + 股息维度(0~50)
            discount_score = min((1 - ratio) / 0.5, 1.0) * 50
            yield_score = min(div_yield / 15.0, 1.0) * 50
            score = discount_score + yield_score
            candidates.append((sym, close, score, ma250w, pe, div_yield))
        except Exception:
            continue

    if not candidates:
        log("无符合条件的标的")
        return

    # 按评分排序
    candidates.sort(key=lambda x: -x[2])
    candidates = candidates[:context.max_positions]

    # 计算每只股票买入金额 (基于总资产的固定比例)
    total_value = context.stock_account.total_value
    target_cash_per_stock = total_value * context.single_position_pct
    available_cash = context.stock_account.cash

    bought = 0
    for sym, price, score, ma_val, pe_val, div_val in candidates:
        if sym in context.positions:
            continue
        if bought >= context.max_positions:
            break
        if target_cash_per_stock > available_cash:
            break
        shares = int(target_cash_per_stock / price / 100) * 100
        if shares < 100:
            continue
        order_shares(sym, shares)
        context.positions[sym] = {"entry_price": price, "entry_year": today.year}
        available_cash -= shares * price
        bought += 1
        log(f"买入 {sym} @ {price:.2f} 折价{(1-price/ma_val)*100:.1f}% PE={pe_val:.1f} 股息率={div_val:.1f}% score={score:.1f}")

    log(f"========== [{today}] 年度调仓完成: 平仓{len(expired)} 买入{bought} ==========")
`

const TREND_CAPITAL_CODE = `def init(context):
    # 趋势资金识别参数
    context.lookback = 5       # k: 回看天数
    context.vol_pct = 0.90     # m: 成交量分位数阈值
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

# ── 信号计算辅助函数 ──
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

def before_trading(context):
    today = context.now.date()
    signals = {}
    for symbol in list(context.universe):
        try:
            sig = compute_daily_signal(context, symbol, today)
            if sig is not None:
                signals[symbol] = sig
        except Exception:
            continue
    if signals:
        context.signal_history[today] = signals
    from datetime import timedelta
    cutoff = today - timedelta(days=10)
    for d in list(context.signal_history.keys()):
        if d < cutoff:
            del context.signal_history[d]

def handle_bar(context, bar_dict):
    context.day_count += 1
    now = context.now.date()

    # 到期平仓
    remaining = []
    for basket in context.baskets:
        held = context.day_count - basket["entry_idx"]
        if held >= context.hold_days:
            for item in basket["stocks"]:
                pos = context.portfolio.get_position(item["symbol"])
                if pos and pos.total_shares > 0:
                    order_shares(item["symbol"], -pos.total_shares)
        else:
            remaining.append(basket)
    context.baskets = remaining

    # 调仓判断
    if context.day_count - context.last_rebalance < context.rebalance_every:
        return
    if context.baskets:
        return
    context.last_rebalance = context.day_count

    dates = sorted(context.signal_history.keys())
    if len(dates) < context.fusion_window:
        return

    # 信号融合评分
    window_dates = dates[-context.fusion_window:]
    candidates = {}
    all_syms = set()
    for d in window_dates:
        all_syms.update(context.signal_history.get(d, {}).keys())

    for sym in all_syms:
        b_days = 0
        c_days = 0
        for d in window_dates:
            sig = context.signal_history.get(d, {}).get(sym, {})
            if sig.get("sig_b_ok"):
                b_days += 1
            if sig.get("sig_c_ok"):
                c_days += 1
        if b_days < context.fusion_min_days or c_days < context.fusion_min_days:
            continue
        latest = context.signal_history.get(window_dates[-1], {}).get(sym, {})
        b_score = -latest.get("sig_b", 0) * 100
        c_score = latest.get("sig_c", 0) / 1e6
        candidates[sym] = b_score + c_score

    if not candidates:
        return

    # 选前N买入
    ranked = sorted(candidates.items(), key=lambda x: -x[1])[:context.portfolio_size]
    total_value = context.portfolio.total_value
    per_stock = total_value * 0.9 / context.portfolio_size

    basket_stocks = []
    for sym, _ in ranked:
        bar = bar_dict[sym] if sym in bar_dict else None
        if bar is None or bar.suspended:
            continue
        context.order_value(sym, per_stock, bar.close)
        basket_stocks.append({"symbol": sym, "entry_price": bar.close})
        context.trailing_highs[sym] = bar.close

    if basket_stocks:
        context.baskets.append({
            "entry_idx": context.day_count,
            "stocks": basket_stocks,
        })
        log(f"[{now}] 建仓 {len(basket_stocks)} 只")
`

// ── State ──
const activeTab = ref('strategyList')
const loading = ref(false)
const strategyList = ref<Strategy[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const saving = ref(false)

const backtestListRef = ref<InstanceType<typeof BacktestList> | null>(null)

// ── Strategy list ──
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
  if (type === 'builtin') {
    name = '深度价值策略'
    code = BUILTIN_CODE
    desc = '低估值+高股息+深度折价：每年5月调仓，独立引擎批量查询'
  } else if (type === 'expression') {
    name = '新建表达式'
    code = SAMPLE_EXPRESSION
    desc = '因子表达式策略'
  } else {
    name = '新建策略'
    code = AKQUANT_TEMPLATE
    desc = 'akquant Strategy 示例'
  }
  try {
    const result = await strategyApi.create({ name, code, description: desc })
    ElMessage.success('策略已创建')
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
    if (type === 'builtin') {
      builtinType.value = 'deep_value'
      builtinCode.value = BUILTIN_CODE
      editorTab.value = 'builtin'
      btMode.value = 'builtin'
    } else if (type === 'expression') {
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
    await ElMessageBox.confirm(`确定要删除策略"${row.name}"吗？`, '确认删除', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning',
    })
    await strategyApi.delete(row.id)
    ElMessage.success('删除成功')
    if (activeStrategy.value?.id === row.id) {
      activeStrategy.value = null
      builtinType.value = null
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

// ── Backtest runner state ──
const activeStrategy = ref<Strategy | null>(null)
const btMode = ref<'script' | 'expression' | 'builtin'>('script')  // kept for backend compat
const btEngine = ref('akquant')
const engineOptions = ref<{ value: string; label: string; modes: string[] }[]>([])

// ── Editor tabs driven by engine ──
const editorTab = ref('akquant-code')
const editorTabs = computed(() => {
  if (builtinType.value) {
    return [{ key: 'builtin', label: '内置策略' }]
  }
  if (btEngine.value === 'akquant') {
    return [
      { key: 'akquant-code', label: 'Python代码（AKQuant）' },
      { key: 'llm', label: 'LLM策略生成' },
    ]
  }
  return [
    { key: 'rqalpha-code', label: 'Python代码（RQAlpha）' },
    { key: 'expression', label: '表达式' },
  ]
})

const AKQUANT_TEMPLATE = `import akquant as aq
import numpy as np

class MyStrategy(aq.Strategy):
    def on_start(self):
        # 策略初始化 — 可注册指标
        pass

    def on_bar(self, bar):
        """每个 bar 调用一次"""
        pos = self.get_position(bar.symbol)
        if bar.close > bar.open and pos == 0:
            self.buy(bar.symbol, 100)
        elif bar.close < bar.open and pos > 0:
            self.close_position(bar.symbol)

    def on_stop(self):
        # 策略结束
        pass`

const BUILTIN_CODE = `"""
深度价值策略 — 年度调仓，独立引擎，批量 ClickHouse 查询
每年5月筛选，等权买入，持有至次年5月。收益含股价变动+现金分红。
"""
from app.db.clickhouse import get_ch_client
import numpy as np
from datetime import date
from dataclasses import dataclass

@dataclass
class Basket:
    entry_date: date
    stocks: list[dict]

class DeepValueStrategy:
    PRICE_TO_MA_MAX = 0.9
    DIVIDEND_YIELD_MIN = 3.5
    PE_MIN = 0
    PE_MAX = 40
    MAX_POSITIONS = 10
    SINGLE_PCT = 0.10

    def __init__(self, pool_symbols=None):
        self.ch = get_ch_client()
        self.pool_symbols = pool_symbols

    def _get_pool(self):
        if self.pool_symbols: return self.pool_symbols
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        from app.core.config import settings
        url = settings.database_url.replace('+aiosqlite', '')
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(select(Stock.symbol)
                .where(Stock.is_st==0, Stock.is_delist==0)).all()
        engine.dispose()
        return [r[0] for r in rows]

    def _get_may_dates(self, start, end):
        rows = self.ch.execute(
            'SELECT DISTINCT trade_date FROM klines_daily '
            'WHERE trade_date>=%(s)s AND trade_date<=%(e)s ORDER BY trade_date',
            {'s':start, 'e':end})
        return [r[0] for r in rows if r[0].month==5
                and (not may or may[-1].year!=r[0].year)]

    def screen(self, as_of_date, pool):
        # 批量查询：close + 250周MA + PE + 股息率
        close_map = {r[0]:float(r[1]) for r in self.ch.execute(
            'SELECT symbol, close FROM klines_daily WHERE symbol IN %(s)s '
            'AND trade_date=(SELECT max(trade_date) FROM klines_daily '
            'WHERE trade_date<=%(d)s AND symbol=klines_daily.symbol)',
            {'s':tuple(pool), 'd':as_of_date})}
        ma_map = {r[0]:float(r[1]) for r in self.ch.execute(
            'SELECT symbol, avg(close) FROM (SELECT symbol, close, '
            'row_number() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn '
            'FROM klines_weekly WHERE symbol IN %(s)s AND trade_date<=%(d)s) '
            'WHERE rn<=250 GROUP BY symbol',
            {'s':tuple(pool), 'd':as_of_date}) if r[1]}
        pe_map = {r[0]:float(r[1]) for r in self.ch.execute(
            'SELECT symbol, value FROM (SELECT symbol, value, '
            'row_number() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn '
            'FROM stock_indicators WHERE symbol IN %(s)s AND indicator_name=%(n)s '
            'AND trade_date<=%(d)s) WHERE rn=1',
            {'s':tuple(pool), 'd':as_of_date, 'n':'pe_ttm'}) if r[1] and float(r[1])>0}
        dy_map = {r[0]:float(r[1]) for r in self.ch.execute(
            'SELECT symbol, value FROM (SELECT symbol, value, '
            'row_number() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn '
            'FROM stock_indicators WHERE symbol IN %(s)s AND indicator_name=%(n)s '
            'AND trade_date<=%(d)s) WHERE rn=1',
            {'s':tuple(pool), 'd':as_of_date, 'n':'dividend_yield'}) if r[1] and float(r[1])>0}
        # 评分筛选
        result = []
        for sym, close in close_map.items():
            if close<=0: continue
            ma=ma_map.get(sym); pe=pe_map.get(sym); dy=dy_map.get(sym)
            if not ma or ma<=0: continue
            ratio=close/ma
            if ratio>=self.PRICE_TO_MA_MAX: continue
            if pe is None or pe<=self.PE_MIN or pe>=self.PE_MAX: continue
            if dy is None or dy<=self.DIVIDEND_YIELD_MIN: continue
            score = min((1-ratio)/0.5,1)*50 + min(dy/15,1)*50
            result.append({'symbol':sym,'close':close,'ratio':round(ratio,4),
                'pe':pe,'dividend_yield':dy,'score':round(score,4)})
        result.sort(key=lambda x:-x['score'])
        return result[:self.MAX_POSITIONS]

    def run(self, start_date, end_date, initial_capital=1_000_000):
        pool = self._get_pool()
        may_dates = self._get_may_dates(start_date, end_date)
        baskets, all_trades = [], []
        for d in may_dates:
            # 平仓
            for b in list(baskets):
                for item in b.stocks:
                    p = self._get_price(item['symbol'], d)
                    if p:
                        div = self._get_dividends(item['symbol'], b.entry_date, d)
                        all_trades.append({'symbol':item['symbol'],'direction':'sell',
                            'trade_date':d.isoformat(),'price':round(p,2),
                            'dividend':round(div,4),'pnl_pct':round(((p+div)/item['entry_price']-1)*100,2)})
                baskets.remove(b)
            # 买入
            candidates = self.screen(d, pool)
            if not candidates: continue
            invest = initial_capital * self.SINGLE_PCT
            stocks = []
            for c in candidates:
                shares = int(invest/c['close']/100)*100
                if shares<100: continue
                stocks.append({'symbol':c['symbol'],'entry_price':c['close'],'shares':shares})
                all_trades.append({'symbol':c['symbol'],'direction':'buy',
                    'trade_date':d.isoformat(),'price':round(c['close'],2),'shares':shares})
            if stocks: baskets.append(Basket(d, stocks))
        # 强制平仓
        for b in baskets:
            for item in b.stocks:
                p=self._get_price(item['symbol'],end_date) or item['entry_price']
                div=self._get_dividends(item['symbol'],b.entry_date,end_date)
                all_trades.append({'symbol':item['symbol'],'direction':'sell',
                    'trade_date':end_date.isoformat(),'price':round(p,2),
                    'dividend':round(div,4),'pnl_pct':round(((p+div)/item['entry_price']-1)*100,2)})
        # 统计
        sells = [t for t in all_trades if 'pnl_pct' in t]
        pnls = [t['pnl_pct'] for t in sells]
        win = [p for p in pnls if p>0]
        groups = {}
        for t in sells: groups.setdefault(t.get('trade_date','?'),[]).append(t['pnl_pct'])
        basket_ret = [float(np.mean(v)) for v in groups.values()]
        total = float(np.prod([1+r/100 for r in basket_ret])-1)*100 if basket_ret else 0
        return {
            'total_return': round(total/100,4),
            'total_trades': len(sells), 'win_trades': len(win),
            'loss_trades': len(pnls)-len(win),
            'win_rate': round(len(win)/len(pnls),4) if pnls else 0,
            'trades': sorted(all_trades, key=lambda x:x.get('trade_date','')),
            'basket_returns': [round(r,2) for r in basket_ret],
        }

    def _get_price(self, symbol, as_of):
        r = self.ch.execute('SELECT close FROM klines_daily '
            'WHERE symbol=%(s)s AND trade_date<=%(d)s ORDER BY trade_date DESC LIMIT 1',
            {'s':symbol,'d':as_of})
        return float(r[0][0]) if r and r[0] and r[0][0] else None

    def _get_dividends(self, symbol, start, end):
        r = self.ch.execute('SELECT sum(value) FROM stock_indicators '
            'WHERE symbol=%(s)s AND indicator_name=%(n)s AND trade_date>%(a)s AND trade_date<=%(b)s',
            {'s':symbol,'n':'dividend_cash','a':start,'b':end})
        return float(r[0][0] or 0) if r and r[0] else 0.0
`

// 内置策略类型
const builtinType = ref<string | null>(null)
const builtinCode = ref('')

// 研报上传
const showUploadDialog = ref(false)
const uploadFile = ref<File | null>(null)
const fileInput = ref<HTMLInputElement | null>(null)
const uploading = ref(false)
const uploadResult = ref<{ strategy_type: string; name: string; code: string; summary: string; conditions: string[]; frequency: string } | null>(null)

// 深度价值策略参数
const dvPool = ref('all')
const dvPeMin = ref(0)
const dvPeMax = ref(40)
const dvDivMin = ref(3.5)
const dvPriceMA = ref(0.9)
const dvMaxPos = ref(10)
const dvSinglePct = ref(10)
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

const btStartDate = ref(getDefaultStartDate())
const btEndDate = ref(getDefaultEndDate())
const btCapital = ref(1_000_000)
const btFrequency = ref('monthly')
const btBarType = ref('daily')
const btSymbols = ref<string[]>([])
const isAllStocks = ref(false)
const allStocksCount = ref(0)
const newSymbolInput = ref('')
const showWatchlistPicker = ref(false)
const selectedPool = ref<string | null>(null)
const watchlistGroups = ref<WatchlistGroup[]>([])
const selectedWatchlistGroup = ref<number | null>(null)
const watchlistStocks = ref<WatchlistStock[]>([])
const selectedWatchlistStocks = ref<string[]>([])

const addSymbol = () => {
  const s = newSymbolInput.value.trim().toUpperCase()
  if (s && /^\d{6}\.(SZ|SH|BJ)$/.test(s) && !btSymbols.value.includes(s)) {
    btSymbols.value.push(s)
  }
  newSymbolInput.value = ''
}

const removeSymbol = (sym: string) => {
  btSymbols.value = btSymbols.value.filter(s => s !== sym)
}

const loadWatchlistGroups = async () => {
  try {
    watchlistGroups.value = await watchlistApi.getGroups()
  } catch { /* non-critical */ }
}

const loadWatchlistStocks = async (groupId: number) => {
  try {
    const stocks = await watchlistApi.getGroupStocks(groupId)
    watchlistStocks.value = stocks || []
    selectedWatchlistStocks.value = []
  } catch { /* non-critical */ }
}

const applyWatchlistStocks = () => {
  for (const sym of selectedWatchlistStocks.value) {
    if (!btSymbols.value.includes(sym)) {
      btSymbols.value.push(sym)
    }
  }
  showWatchlistPicker.value = false
}

const replaceWithWatchlistStocks = () => {
  if (selectedWatchlistStocks.value.length === 0) return
  isAllStocks.value = false
  allStocksCount.value = 0
  btSymbols.value = [...selectedWatchlistStocks.value]
  showWatchlistPicker.value = false
  ElMessage.success(`已替换为 ${btSymbols.value.length} 只自选股`)
}

const loadPoolSymbols = async (poolName: string | null) => {
  if (!poolName) return
  try {
    const { default: request } = await import('@/api/request')
    const res = await request.get<any>(`/v2/backtest/pools/${poolName}`, { timeout: 120000 })
    if (res?.symbols?.length) {
      if (poolName === 'all') {
        isAllStocks.value = true
        allStocksCount.value = res.symbols.length
        btSymbols.value = res.symbols  // store for API call, but don't render tags
        ElMessage.success(`已切换全量A股模式 (${res.symbols.length} 只)`)
      } else {
        isAllStocks.value = false
        allStocksCount.value = 0
        btSymbols.value = res.symbols
        ElMessage.success(`已加载 ${poolName} 股票池 (${res.symbols.length} 只)`)
      }
    } else {
      ElMessage.warning(`${poolName} 股票池为空`)
    }
  } catch (e: any) {
    ElMessage.error(`加载股票池失败: ${e?.message || e}`)
  }
  selectedPool.value = null
}

const clearAllStocks = () => {
  btSymbols.value = []
  isAllStocks.value = false
  allStocksCount.value = 0
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

const freqLabelMap: Record<string, string> = {
  daily: '每天',
  weekly: '每周',
  monthly: '每月',
}

const openDocs = () => {
  window.open('/docs', '_blank')
}

// ── Factor selection ──
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
      description: activeStrategy.value.description || undefined,
    })
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
    // Auto-detect mode: builtin > script > expression
    if (row.name === '深度价值策略' || row.id === 12) {
      builtinType.value = 'deep_value'
      builtinCode.value = BUILTIN_CODE
      editorTab.value = 'builtin'
      btMode.value = 'builtin'
      btCode.value = code
    } else {
      builtinType.value = null
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
    }
    // Auto-detect bar_type from code content
    btBarType.value = (code.includes('get_intraday') || code.includes('compute_daily_signal'))
      ? 'minute' : 'daily'
    btLiveData.value = null
    btFullResult.value = null
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

const handleRunBacktest = async () => {
  if (!activeStrategy.value) return
  if (btSymbols.value.length === 0 && !isAllStocks.value) {
    ElMessage.warning('请先添加回测股票')
    return
  }
  btRunning.value = true
  btLiveData.value = null
  btFullResult.value = null
  btMetrics.value = []
  btLogs.value = ['正在运行回测...']
  btErrors.value = []

  const isExpression = editorTab.value === 'expression'
  const isBuiltin = editorTab.value === 'builtin' && builtinType.value
  const code = isExpression ? btExpression.value : btCode.value
  const mode = isExpression ? 'vectorized' : 'event_driven'
  const engine = btEngine.value

  try {
    const { default: request } = await import('@/api/request')

    if (isBuiltin) {
      // 内置策略独立引擎 — 直接返回结果
      const res = await request.post<any>('/strategy/deep-value/backtest', {
        start_date: btStartDate.value,
        end_date: btEndDate.value,
        initial_capital: btCapital.value,
        pool: dvPool.value,
        pe_min: dvPeMin.value,
        pe_max: dvPeMax.value,
        dividend_yield_min: dvDivMin.value,
        price_to_ma_max: dvPriceMA.value,
        max_positions: dvMaxPos.value,
        single_pct: dvSinglePct.value / 100,
      }, { timeout: 120000 })
      btFullResult.value = {
        total_return: res.total_return ?? 0,
        annual_return: res.annual_return ?? 0,
        total_trades: res.total_trades,
        win_trades: res.win_trades,
        loss_trades: res.loss_trades,
        win_rate: res.win_rate ?? 0,
        avg_return: res.avg_return ?? 0,
        max_drawdown: res.max_drawdown ?? 0,
        trades: res.trades?.map((t: any) => ({
          trade_date: t.trade_date,
          symbol: t.symbol,
          direction: t.direction,
          price: t.price,
          quantity: t.shares,
          pnl: t.pnl_pct,
          dividend: t.dividend,
        })),
        final_capital: res.final_capital,
        basket_returns: res.basket_returns,
      }
      btProgress.value = 1
      btLogs.value = [`回测完成 (${res.basket_count || 0} 期)`]
      btRunning.value = false
      showReport.value = true
      return
    }

    const res = await request.post<any>('/v2/backtest/run', {
      engine,
      mode,
      factor_expression: isExpression ? code : undefined,
      strategy_code: (!isExpression && engine === 'akquant') ? code : undefined,
      buy_condition: (!isExpression && engine === 'builtin') ? code : undefined,
      symbols: btSymbols.value,
      start_date: btStartDate.value,
      end_date: btEndDate.value,
      initial_capital: btCapital.value,
      rebalance_freq: btFrequency.value,
      n_groups: isExpression ? btNGroups.value : 5,
      bar_type: btBarType.value,
    }, { timeout: 600000 })

    // 拦截器已解包 {code, data} → data 字段直接返回
    const taskId = (res as any)?.task_id
    if (taskId) {
      btTaskId.value = taskId
      btLogs.value = [`任务已提交 (${taskId})，等待完成...`]
      let attempts = 0
      while (attempts < 300) {
        await new Promise(r => setTimeout(r, 2000))
        const statusData = await request.get<any>(`/v2/backtest/status/${taskId}`)
        btProgress.value = statusData?.progress ?? 0
        if (statusData?.live) {
          btLiveData.value = statusData.live
        }
        if (statusData?.status === 'done') {
          btLiveData.value = statusData.live
          const data = await request.get<any>(`/v2/backtest/result/${taskId}`)
          if (data) {
            btFullResult.value = data as BacktestResultData
            btLogs.value = ['回测完成']
          }
          break
        } else if (statusData?.status === 'failed') {
          const errData = await request.get<any>(`/v2/backtest/result/${taskId}`)
          btErrors.value = [errData?.error || '回测失败']
          btLogs.value = ['回测执行失败']
          break
        }
        attempts++
      }
      if (attempts >= 300) {
        btErrors.value = ['回测超时']
        btLogs.value = ['回测超时（10分钟）']
      }
    } else {
      // 同步返回结果（vectorized 模式直接返回）
      if (res) {
        btFullResult.value = res as BacktestResultData
        btLogs.value = ['回测完成']
      }
    }
  } catch (e: any) {
    btErrors.value = [e?.message || '回测失败']
    btLogs.value = ['回测执行失败']
  } finally {
    btRunning.value = false
  }
}

const seedDeepValueStrategy = async () => {
  try {
    const existing = await strategyApi.list(1, 100)
    const found = existing.items.find((s: Strategy) => s.name === '深度价值策略')
    const description = '低估值+高股息+深度折价：股价<250周线10%+，股息率>3.5%，0<PE<40，每年5月调仓持有1年'
    if (found) {
      // Update if code is not the annual rebalance version
      if (!found.code?.includes('last_rebalance_year')) {
        await strategyApi.update(found.id, { code: DEEP_VALUE_CODE, description })
        await loadStrategies()
      }
    } else {
      await strategyApi.create({ name: '深度价值策略', code: DEEP_VALUE_CODE, description })
      await loadStrategies()
    }
  } catch {
    // non-critical
  }
}

const seedTrendCapitalStrategy = async () => {
  // Skip if already seeded (check by name)
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
    // non-critical — strategy can be created manually
  }
}

// ── Engine ──
const loadEngines = async () => {
  try {
    const { backtestV2Engines } = await import('@/api/backtest')
    const data = await backtestV2Engines.list()
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

// ── Report viewer for akquant ──
watch(activeTab, (tab) => {
  if (tab !== 'backtestRunner') {
    showReport.value = false
  }
})

// ── 研报上传 ──
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
      timeout: 120000,
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
    if (r.strategy_type === 'builtin') {
      builtinType.value = 'deep_value'
      builtinCode.value = r.code
      btMode.value = 'builtin'
    } else if (r.strategy_type === 'expression') {
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
  }).catch((e) => {
    // Still fill editor even if DB save fails
    activeStrategy.value = { id: 0, name: r.name || '研报策略', code: r.code || '', description: r.summary || '', parameters: null, created_at: null, updated_at: null }
    if (r.strategy_type === 'builtin') {
      builtinType.value = 'deep_value'; builtinCode.value = r.code; btMode.value = 'builtin'
    } else if (r.strategy_type === 'expression') {
      btMode.value = 'expression'; btExpression.value = r.code
    } else {
      btMode.value = 'script'; btCode.value = r.code
    }
    showUploadDialog.value = false
    uploadFile.value = null; uploadResult.value = null
    activeTab.value = 'backtestRunner'
    ElMessage.success('策略已填充到编辑器')
  })
}

onMounted(async () => {
  await loadStrategies()
  await seedTrendCapitalStrategy()
  await seedDeepValueStrategy()
  await loadEngines()
  loadSavedFactors()
  loadWatchlistGroups()
})
</script>

<style scoped>
.page-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.page-header h2 {
  margin: 0;
  font-size: 20px;
  font-weight: 600;
  color: #e2e2ea;
}

.page-header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.strategy-tabs {
  height: 100%;
  background: #131318;
  border-radius: 4px;
  padding: 16px;
}

.strategy-tabs :deep(.el-tabs__content) {
  flex: 1;
  overflow: auto;
}

.strategy-tabs :deep(.el-tab-pane) {
  height: 100%;
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

.split-layout { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; height: calc(100vh - 200px); }
.editor-panel {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  overflow: hidden;
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
.code-editor { flex: 1; background: #1e1e1e; }
.editor-textarea {
  width: 100%;
  height: 100%;
  border: none;
  padding: 12px;
  font-family: 'JetBrains Mono', 'Courier New', monospace;
  font-size: 13px;
  line-height: 1.6;
  color: #d4d4d4;
  background: #1e1e1e;
  resize: none;
  outline: none;
  tab-size: 4;
}
.editor-textarea::placeholder { color: #6e6e6e; }
.right-panel { display: flex; flex-direction: column; gap: 12px; overflow: auto; }

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

/* ── Mode switch ── */
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

/* ── Expression panel ── */
.expression-panel {
  flex: 1;
  background: #1e1e1e;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.expression-input-row {
  display: flex;
  gap: 10px;
}
.expression-input {
  flex: 1;
}
.expression-input :deep(.el-input__inner) {
  background: #2d2d2d;
  border-color: #404040;
  color: #d4d4d4;
  font-family: 'JetBrains Mono', monospace;
  font-size: 14px;
  height: 40px;
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

.builtin-panel {
  flex: 1;
  background: #1e1e1e;
  display: flex;
  flex-direction: column;
}
.builtin-params-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 12px;
  background: #252530;
  border-bottom: 1px solid #333;
  flex-wrap: wrap;
}
.builtin-params-bar .param-label {
  font-size: 11px;
  color: #888;
  margin-left: 4px;
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
</style>
