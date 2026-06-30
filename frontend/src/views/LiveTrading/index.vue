<template>
  <div class="page-frame live-page">
    <header class="panel-card page-head">
      <div>
        <span class="section-kicker">LIVE TRADING DESK</span>
        <h2>模拟 / 实盘</h2>
        <p>可配置策略执行台：默认接入 CashAware 稳健版与进攻版，自动交易与真实下单均受独立护栏控制。</p>
      </div>
      <div class="actions">
        <div class="layout-switcher" aria-label="切换交易页面布局" style="margin-right: 12px; display: inline-flex; gap: 4px; padding: 4px; border: 1px solid var(--border-default); border-radius: var(--radius-full); background: rgba(253, 251, 247, 0.78);">
          <button
            v-for="option in [
              { key: 'A', label: '驾驶舱', hint: '交易室驾驶舱' },
              { key: 'B', label: '风控矩阵', hint: '风控审计大矩阵' },
              { key: 'C', label: '多账户资产', hint: '多账户资产卡片' }
            ] as const"
            :key="option.key"
            type="button"
            :class="{ active: layoutMode === option.key }"
            :title="option.hint"
            style="border: 0; border-radius: var(--radius-full); background: transparent; color: var(--text-secondary); cursor: pointer; font-size: var(--text-xs); font-weight: 800; padding: 7px 11px;"
            :style="layoutMode === option.key ? 'background: var(--accent-primary) !important; color: #fdfbf7 !important;' : ''"
            @click="layoutMode = option.key"
          >
            <span>{{ option.key }}</span>
            {{ option.label }}
          </button>
        </div>
        <el-segmented v-model="mode" :options="modeOptions" />
        <el-button type="primary" class="action-button action-button--preflight" :loading="preflightLoading" @click="loadPreflight">早盘检查</el-button>
        <el-button :loading="loading" @click="loadAll">刷新</el-button>
      </div>
    </header>

    <section class="status-band">
      <div>
        <label>行情</label>
        <span :class="['state-text', status?.quote_connected ? 'state-text--good' : 'state-text--warn']">
          {{ status?.quote_connected ? '已连接' : '未确认' }}
        </span>
      </div>
      <div>
        <label>交易模块</label>
        <span :class="['state-text', status?.xttrader_available ? 'state-text--good' : 'state-text--bad']">
          {{ status?.xttrader_available ? '可用' : '不可用' }}
        </span>
      </div>
      <div>
        <label>真实下单</label>
        <div class="guardrail-toggle">
          <span :class="['state-text', status?.order_submit_enabled ? 'state-text--bad' : 'state-text--neutral']">
            {{ status?.order_submit_enabled ? '开启' : '关闭' }}
          </span>
          <el-switch
            v-model="guardrailDraft.enable_order_submit"
            size="small"
            :loading="savingGuardrails"
            inline-prompt
            active-text="开"
            inactive-text="关"
            @change="onOrderSubmitGuardrailChange"
          />
        </div>
      </div>
      <div>
        <label>自动实盘</label>
        <div class="guardrail-toggle">
          <span :class="['state-text', status?.auto_execute_enabled ? 'state-text--bad' : 'state-text--neutral']">
            {{ status?.auto_execute_enabled ? '允许' : '禁止' }}
          </span>
          <el-switch
            v-model="guardrailDraft.auto_execute_enabled"
            size="small"
            :disabled="!guardrailDraft.enable_order_submit"
            :loading="savingGuardrails"
            inline-prompt
            active-text="开"
            inactive-text="关"
            @change="onAutoExecuteGuardrailChange"
          />
        </div>
      </div>
      <div>
        <label>Runner</label>
        <span :class="['state-text', runnerStateClass]">{{ runnerText }}</span>
      </div>
    </section>

    <!-- EMERGENCY GUARDRAILS FOR LAYOUT A & B / WIND CONTROL -->
    <div v-if="layoutMode === 'A'" class="layout-wrapper-a" style="display: grid; gap: var(--space-4);">
      <section v-if="runtimeVisible" :class="['runtime-strip', `runtime-strip--${runtimeTone}`]">
        <div class="runtime-strip__main">
          <span class="runtime-strip__eyebrow">{{ runtimeState.scope }}</span>
          <strong>{{ runtimeState.title }}</strong>
          <small>{{ runtimeState.detail }}</small>
        </div>
        <div class="runtime-strip__progress">
          <el-progress
            :percentage="runtimeProgress"
            :status="runtimeProgressStatus"
            :stroke-width="8"
            :show-text="false"
          />
          <span>{{ runtimeProgressText }}</span>
        </div>
        <div class="runtime-strip__steps">
          <span
            v-for="step in runtimeState.steps"
            :key="step.id"
            :class="['runtime-step', `runtime-step--${step.status}`]"
            :title="step.detail || step.label"
          >
            {{ step.label }}
          </span>
        </div>
      </section>

      <!-- Layout A Main Desk Grid -->
      <section class="desk-grid">
        <div class="strategy-config-stack">
          <article v-if="brokerAccountSnapshot" class="panel-card broker-account-panel">
            <div class="broker-account__head">
              <div>
                <span class="section-kicker">TOTAL BROKER ACCOUNT</span>
                <h4>QMT 总账户</h4>
              </div>
              <el-tag type="info">只读参考</el-tag>
            </div>
            <div class="account-summary broker-summary">
              <div v-for="item in brokerSummaryItems" :key="item.label">
                <label>{{ item.label }}</label>
                <strong :class="item.tone ? `metric-${item.tone}` : ''">{{ item.value }}</strong>
              </div>
            </div>
          </article>

          <article class="panel-card control-panel">
            <div class="panel-card__head">
              <div>
                <span class="section-kicker">STRATEGY PROFILE</span>
                <h3>策略配置</h3>
              </div>
              <el-button size="small" @click="profileDialogOpen = true">新增 Profile</el-button>
            </div>
            <div class="control-body">
              <el-select v-model="selectedProfileKey" filterable placeholder="选择策略 Profile" @change="onProfileChange">
                <el-option
                  v-for="profile in profiles"
                  :key="profile.profile_key"
                  :label="profile.display_name"
                  :value="profile.profile_key"
                >
                  <span>{{ profile.display_name }}</span>
                  <small> · ID {{ profile.strategy_id }}</small>
                </el-option>
              </el-select>
              <div class="param-row">
                <label>交易日</label>
                <el-date-picker v-model="tradeDate" value-format="YYYY-MM-DD" />
              </div>
              <div class="param-row">
                <label>指数池</label>
                <el-input v-model="indexSymbol" />
              </div>
              <div class="inline-signal">
                <div class="inline-signal__head">
                  <span>候选 / 订单</span>
                  <el-button text size="small" @click="scrollToOrders">订单篮子</el-button>
                </div>
                <div class="inline-signal__stats">
                  <div><label>股票池</label><strong>{{ signalData?.universe_size || 0 }}</strong></div>
                  <div><label>候选</label><strong>{{ signalData?.candidate_count || 0 }}</strong></div>
                  <div><label>订单</label><strong>{{ orderRows.length }}</strong></div>
                </div>
                <div class="inline-signal__preview">
                  <span>
                    <strong>候选</strong>
                    {{ candidatePreview.map(candidateSymbol).join(' / ') || '暂无' }}
                  </span>
                  <span>
                    <strong>订单</strong>
                    {{ orderPreview.map(order => `${order.side} ${order.symbol}`).join(' / ') || '暂无' }}
                  </span>
                </div>
              </div>
              <div class="profile-meta" v-if="selectedProfile">
                <strong>{{ selectedProfile.display_name }}</strong>
                <span>ID {{ selectedProfile.strategy_id }} · {{ selectedProfile.adapter_type }}</span>
                <p>{{ selectedProfile.description || selectedProfile.strategy_name || '-' }}</p>
                <div class="profile-actions">
                  <el-switch
                    :model-value="selectedProfile.enabled"
                    active-text="启用"
                    inactive-text="停用"
                    @change="toggleProfileEnabled"
                  />
                  <el-button text size="small" @click="makeDefaultProfile">设为默认</el-button>
                </div>
              </div>
            </div>
          </article>
        </div>

        <aside class="operations-stack">
          <section class="panel-card preflight-panel">
            <div class="panel-card__head">
              <div>
                <span class="section-kicker">MORNING PREFLIGHT</span>
                <h3>早盘运行检查</h3>
              </div>
              <div class="table-actions">
                <span :class="['state-text', preflightStateClass]">{{ preflightStatusText }}</span>
                <el-button size="small" :loading="preflightLoading" @click="loadPreflight">重新检查</el-button>
              </div>
            </div>
            <div class="preflight-grid">
              <div v-for="item in preflightChecks" :key="item.label" class="preflight-check">
                <label>{{ item.label }}</label>
                <strong :class="`preflight-check__value--${item.tone || 'neutral'}`">{{ item.value }}</strong>
                <small>{{ item.note }}</small>
              </div>
            </div>
            <div v-if="preflightIssues.length" class="preflight-issues">
              <span v-for="issue in preflightIssues" :key="issue">{{ issue }}</span>
            </div>
            <div v-if="preflightActions.length" class="preflight-actions">
              <span v-for="action in preflightActions" :key="action">{{ action }}</span>
            </div>
          </section>

          <article class="panel-card runner-panel">
            <div class="panel-card__head">
              <div>
                <span class="section-kicker">RUNNER</span>
                <h3>自动 / 接管</h3>
              </div>
            </div>
            <div class="runner-actions">
              <el-button type="primary" class="action-button action-button--auto" :loading="runnerLoading" @click="startRunner">启动自动</el-button>
              <el-button :loading="runnerLoading" @click="stopRunner">停止</el-button>
              <el-button type="warning" class="action-button action-button--takeover" :loading="runnerLoading" @click="takeoverRunner">人工接管</el-button>
            </div>
            <div class="runner-state">
              <div><label>状态</label><strong>{{ status?.runner.status || '-' }}</strong></div>
              <div><label>Profile</label><strong>{{ status?.runner.profile_key || '-' }}</strong></div>
              <div><label>最近信号</label><strong>{{ shortHash(status?.runner.last_signal_hash) }}</strong></div>
              <div><label>等待/错误</label><strong>{{ status?.runner.last_wait_reason || status?.runner.last_error || '-' }}</strong></div>
            </div>
          </article>
        </aside>
      </section>

      <!-- Strategy Portfolio View -->
      <section class="panel-card account-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">STRATEGY PORTFOLIO</span>
            <h3>{{ strategyPortfolioTitle }}</h3>
            <p class="portfolio-subtitle">{{ strategyPortfolioSubtitle }}</p>
          </div>
          <div class="table-actions">
            <span :class="['state-text', strategyAccountReady ? 'state-text--good' : 'state-text--warn']">{{ strategyAccountStatusText }}</span>
            <span :class="['live-stream-pill', liveStreamStateClass]">{{ liveStreamStatusText }}</span>
            <el-button size="small" :loading="accountLoading" @click="loadAccount">刷新账户</el-button>
            <el-button size="small" @click="openStrategyReviewDialog">策略表现</el-button>
            <el-button size="small" @click="openPositionsDialog">持仓明细</el-button>
            <span class="drag-hint">拖拽表头排序</span>
            <el-button size="small" @click="capitalDialogOpen = true">
              {{ strategyAccountReady ? '调整本金' : '圈定本金' }}
            </el-button>
          </div>
        </div>
        <el-alert
          v-if="!strategyAccountReady"
          type="warning"
          show-icon
          title="请先圈定本次策略组合本金；策略只会使用这块独立 portfolio，不会清空你原有的 QMT 持仓。"
        />
        <div class="account-summary">
          <div v-for="item in accountSummaryItems" :key="item.label">
            <label>{{ item.label }}</label>
            <strong :class="item.tone ? `metric-${item.tone}` : ''">{{ item.value }}</strong>
          </div>
        </div>
        <el-alert
          v-if="accountSnapshot?.error"
          type="warning"
          show-icon
          :title="accountSnapshot.error"
        />
        <el-table :data="accountPositions" size="small" stripe border height="340" empty-text="暂无持仓">
          <el-table-column
            v-for="column in visiblePositionColumns"
            :key="column.key"
            :prop="column.prop"
            :label="column.label"
            :min-width="column.minWidth || column.width"
            :align="column.align"
            resizable
            show-overflow-tooltip
          >
            <template #header>
              <span
                :class="positionColumnHeaderClass(column.key)"
                :data-position-column-key="column.key"
                @pointerdown="onPositionColumnPointerDown($event, column.key)"
              >
                <span class="position-column-header__grip" aria-hidden="true"></span>
                <span>{{ column.label }}</span>
              </span>
            </template>
            <template #default="{ row }">
              <div
                v-if="column.key === 'quantity_available'"
                class="stack-cell stack-cell--right"
              >
                <strong>{{ formatQuantity(row.quantity) }}</strong>
                <small>{{ formatQuantity(row.available) }}</small>
              </div>
              <div
                v-else-if="column.key === 'price_cost'"
                class="stack-cell stack-cell--right"
              >
                <strong>{{ formatPrice(positionLastPrice(row)) }}</strong>
                <small>{{ formatPrice(row.avg_cost) }}</small>
              </div>
              <span v-else :class="positionCellClass(row, column.key)">{{ positionCellValue(row, column.key) }}</span>
            </template>
          </el-table-column>
        </el-table>
      </section>

      <!-- Order Basket & Audits in layout A -->
      <section ref="orderPanelRef" class="panel-card order-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">ORDER BASKET</span>
            <h3>订单篮子</h3>
          </div>
          <div class="table-actions">
            <span v-if="signalData?.signal_hash" class="signal-hash-pill" :title="signalData.signal_hash">信号 {{ shortHash(signalData.signal_hash) }}</span>
            <el-button size="small" type="primary" class="action-button action-button--signal" :loading="signalsLoading" @click="loadSignals">生成信号</el-button>
            <el-button size="small" type="success" class="action-button action-button--submit" :disabled="!orderRows.length" @click="submitOrders">提交篮子</el-button>
          </div>
        </div>
        <el-table :data="orderRows" size="small" stripe border max-height="340">
          <el-table-column prop="symbol" label="代码" width="110" resizable />
          <el-table-column label="名称" width="120" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.stock_name || '-' }}</template>
          </el-table-column>
          <el-table-column prop="side" label="方向" width="80" resizable>
            <template #default="{ row }">
              <el-tag :type="row.side === 'BUY' ? 'danger' : 'success'" effect="plain">{{ row.side }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="数量" width="160" resizable>
            <template #default="{ row }">
              <el-input-number v-model="row.quantity" :min="0" :step="100" size="small" />
            </template>
          </el-table-column>
          <el-table-column prop="reference_price" label="参考价" width="110" resizable />
          <el-table-column prop="remark" label="原因" min-width="220" resizable show-overflow-tooltip />
          <el-table-column label="操作" width="88" resizable>
            <template #default="{ $index }">
              <el-button text type="danger" size="small" @click="removeOrder($index)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
      </section>

      <section class="panel-card audit-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">ORDER AUDIT</span>
            <h3>订单审计 / 跳过</h3>
          </div>
          <div class="table-actions">
            <span>{{ audits.length }} 条事件</span>
            <span v-if="signalData?.skipped_orders?.length">{{ signalData.skipped_orders.length }} 笔跳过已入审计</span>
            <el-button text size="small" @click="loadAudits">刷新</el-button>
          </div>
        </div>
        <el-table :data="audits" size="small" border height="300" empty-text="暂无审计事件">
          <el-table-column label="时间" width="168" resizable>
            <template #default="{ row }">{{ formatDateTime(row.created_at) }}</template>
          </el-table-column>
          <el-table-column label="事件" width="118" resizable>
            <template #default="{ row }">{{ auditEventLabel(row.event_type, row.status) }}</template>
          </el-table-column>
          <el-table-column label="状态" width="116" resizable>
            <template #default="{ row }">
              <el-tag :type="statusTagType(row.status)" effect="plain">{{ statusLabel(row.status) }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="mode" label="模式" width="72" resizable />
          <el-table-column prop="symbol" label="代码" width="110" resizable />
          <el-table-column label="名称" width="130" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.stock_name || '-' }}</template>
          </el-table-column>
          <el-table-column label="方向" width="80" resizable>
            <template #default="{ row }">{{ sideLabel(row.side) }}</template>
          </el-table-column>
          <el-table-column label="委托量" width="100" resizable align="right">
            <template #default="{ row }">{{ formatQuantity(row.quantity) }}</template>
          </el-table-column>
          <el-table-column label="成交量" width="100" resizable align="right">
            <template #default="{ row }">{{ formatQuantity(row.filled_quantity) }}</template>
          </el-table-column>
          <el-table-column label="价格" width="105" resizable align="right">
            <template #default="{ row }">{{ formatPrice(row.filled_price || row.reference_price) }}</template>
          </el-table-column>
          <el-table-column label="金额" width="125" resizable align="right">
            <template #default="{ row }">{{ formatMoney(row.order_value) }}</template>
          </el-table-column>
          <el-table-column label="委托号" width="130" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.order_id || '-' }}</template>
          </el-table-column>
          <el-table-column label="说明" min-width="260" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.message || row.skip_reason || '-' }}</template>
          </el-table-column>
        </el-table>
      </section>
    </div>

    <!-- Layout B: Risk Audit Matrix -->
    <div v-else-if="layoutMode === 'B'" class="layout-wrapper-b" style="display: grid; gap: var(--space-4);">
      <article class="panel-card matrix-audit-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">RISK AUDIT MATRIX</span>
            <h3>风控及持仓偏差大矩阵</h3>
            <p>呈现所有选定策略对应的实时持仓、偏离百分比、量比及委托滑点明细。</p>
          </div>
          <div class="table-actions">
            <el-button size="small" :loading="accountLoading" @click="loadAccount">刷新数据</el-button>
          </div>
        </div>
        <el-table :data="accountPositions" size="small" border stripe height="500">
          <el-table-column prop="symbol" label="股票代码" width="110" resizable />
          <el-table-column prop="stock_name" label="名称" width="120" resizable show-overflow-tooltip />
          <el-table-column label="持仓股数/可用" width="140" align="right">
            <template #default="{ row }">{{ formatQuantity(row.quantity) }} / {{ formatQuantity(row.available) }}</template>
          </el-table-column>
          <el-table-column label="成本价/最新价" width="140" align="right">
            <template #default="{ row }">{{ formatPrice(row.avg_cost) }} / {{ formatPrice(positionLastPrice(row)) }}</template>
          </el-table-column>
          <el-table-column prop="market_value" label="当前市值" width="130" align="right">
            <template #default="{ row }">{{ formatMoney(row.market_value) }}</template>
          </el-table-column>
          <el-table-column prop="position_pct" label="仓位占比" width="100" align="right">
            <template #default="{ row }">{{ formatPercent(row.position_pct) }}</template>
          </el-table-column>
          <el-table-column prop="unrealized_pnl_pct" label="浮动盈亏" width="110" align="right">
            <template #default="{ row }">
              <span :class="pnlTone(row.unrealized_pnl_pct)">{{ formatPercent(row.unrealized_pnl_pct) }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="volume_ratio" label="量比" width="90" align="right">
            <template #default="{ row }">{{ row.volume_ratio == null ? '-' : Number(row.volume_ratio).toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="turnover_rate" label="换手率" width="100" align="right">
            <template #default="{ row }">{{ formatPercent(row.turnover_rate) }}</template>
          </el-table-column>
          <el-table-column prop="today_change_pct" label="今日涨跌" width="110" align="right">
            <template #default="{ row }">
              <span :class="pnlTone(row.today_change_pct)">{{ formatPercent(row.today_change_pct) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="运行说明" min-width="200" show-overflow-tooltip>
            <template #default="{ row }">
              <span>{{ row.today_change_pct && row.today_change_pct > 0.095 ? '今日强行推涨停' : '正常持股中' }}</span>
            </template>
          </el-table-column>
        </el-table>
      </article>

      <!-- Compact Log/Audit Stream underneath -->
      <section class="panel-card compact-audit-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">LIVE AUDIT LOGS</span>
            <h3>事件流水</h3>
          </div>
          <el-button text size="small" @click="loadAudits">刷新</el-button>
        </div>
        <el-table :data="audits.slice(0, 10)" size="small" border height="160">
          <el-table-column label="时间" width="168">
            <template #default="{ row }">{{ formatDateTime(row.created_at) }}</template>
          </el-table-column>
          <el-table-column prop="symbol" label="代码" width="100" />
          <el-table-column label="事件" width="120">
            <template #default="{ row }">{{ auditEventLabel(row.event_type, row.status) }}</template>
          </el-table-column>
          <el-table-column label="委托/成交量" width="140" align="right">
            <template #default="{ row }">{{ formatQuantity(row.quantity) }} / {{ formatQuantity(row.filled_quantity) }}</template>
          </el-table-column>
          <el-table-column label="价格/金额" width="180" align="right">
            <template #default="{ row }">{{ formatPrice(row.filled_price || row.reference_price) }} / {{ formatMoney(row.order_value) }}</template>
          </el-table-column>
          <el-table-column prop="message" label="审计详情" min-width="200" show-overflow-tooltip />
        </el-table>
      </section>
    </div>

    <!-- Layout C: Multi-Account Asset Cards -->
    <div v-else-if="layoutMode === 'C'" class="layout-wrapper-c" style="display: grid; gap: var(--space-4);">
      <div class="account-cards-grid" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: var(--space-4);">
        <!-- Active Account Details -->
        <article class="panel-card account-card" style="padding: var(--space-4);">
          <div class="broker-account__head" style="border-bottom: 1px solid var(--border-subtle); padding-bottom: var(--space-2); margin-bottom: var(--space-3);">
            <div>
              <span class="section-kicker">STRATEGY PORTFOLIO</span>
              <h4>{{ currentStrategyName }}</h4>
            </div>
            <el-tag type="success">Active</el-tag>
          </div>
          <div class="card-metrics" style="display: grid; grid-template-columns: repeat(2, 1fr); gap: var(--space-3);">
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">总资产</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatMoney(accountSnapshot?.total_asset) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">可用现金</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatMoney(accountSnapshot?.cash) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">持仓市值</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatMoney(accountSnapshot?.market_value) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">持仓比例</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatPercent(accountSnapshot && accountSnapshot.total_asset ? accountSnapshot.market_value / accountSnapshot.total_asset : 0) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">浮盈亏</label><strong :class="pnlTone(accountSnapshot?.unrealized_pnl)" style="font-size: var(--text-lg);">{{ formatMoney(accountSnapshot?.unrealized_pnl) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">累计盈亏</label><strong :class="pnlTone(accountSnapshot?.total_pnl)" style="font-size: var(--text-lg);">{{ formatMoney(accountSnapshot?.total_pnl) }}</strong></div>
          </div>
        </article>

        <!-- QMT Broker Account Details -->
        <article v-if="brokerAccountSnapshot" class="panel-card account-card" style="padding: var(--space-4);">
          <div class="broker-account__head" style="border-bottom: 1px solid var(--border-subtle); padding-bottom: var(--space-2); margin-bottom: var(--space-3);">
            <div>
              <span class="section-kicker">BROKER ACCOUNT</span>
              <h4>QMT 物理账户</h4>
            </div>
            <el-tag type="info">Read-only</el-tag>
          </div>
          <div class="card-metrics" style="display: grid; grid-template-columns: repeat(2, 1fr); gap: var(--space-3);">
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">总资产</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatMoney(brokerAccountSnapshot?.total_asset) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">可用现金</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatMoney(brokerAccountSnapshot?.cash) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">持仓市值</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatMoney(brokerAccountSnapshot?.market_value) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">持仓比例</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatPercent(brokerAccountSnapshot && brokerAccountSnapshot.total_asset ? brokerAccountSnapshot.market_value / brokerAccountSnapshot.total_asset : 0) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">物理持仓数</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ brokerAccountSnapshot?.position_count || 0 }} 只</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">最近更新</label><strong style="font-size: var(--text-lg); color: var(--text-primary); font-family: var(--font-data);">{{ formatClock(liveStreamUpdatedAt) }}</strong></div>
          </div>
        </article>

        <!-- Weekly Performance Summary Card -->
        <article class="panel-card account-card" style="padding: var(--space-4);">
          <div class="broker-account__head" style="border-bottom: 1px solid var(--border-subtle); padding-bottom: var(--space-2); margin-bottom: var(--space-3);">
            <div>
              <span class="section-kicker">WEEKLY AUDIT</span>
              <h4>本周投研成绩单</h4>
            </div>
            <el-tag type="warning">Review</el-tag>
          </div>
          <div class="card-metrics" style="display: grid; grid-template-columns: repeat(2, 1fr); gap: var(--space-3);">
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">本周PnL</label><strong :class="pnlTone(weeklyAnalysis?.summary?.weekly_pnl)" style="font-size: var(--text-lg);">{{ formatMoney(weeklyAnalysis?.summary?.weekly_pnl) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">本周最大回撤</label><strong :class="pnlTone(weeklyAnalysis?.summary?.weekly_max_drawdown_pct)" style="font-size: var(--text-lg);">{{ formatPercent(weeklyAnalysis?.summary?.weekly_max_drawdown_pct) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">至今PnL</label><strong :class="pnlTone(weeklyAnalysis?.summary?.all_time_pnl)" style="font-size: var(--text-lg);">{{ formatMoney(weeklyAnalysis?.summary?.all_time_pnl) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">至今回撤</label><strong :class="pnlTone(weeklyAnalysis?.summary?.all_time_max_drawdown_pct)" style="font-size: var(--text-lg);">{{ formatPercent(weeklyAnalysis?.summary?.all_time_max_drawdown_pct) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">本周净买入</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ formatMoney(weeklyAnalysis?.summary?.net_notional) }}</strong></div>
            <div><label style="display: block; font-size: var(--text-xs); color: var(--text-muted);">委托笔数/成交</label><strong style="font-size: var(--text-lg); color: var(--text-primary);">{{ weeklyAnalysis?.summary?.completed_records || 0 }} / {{ weeklyAnalysis?.summary?.cancelled_records || 0 }}</strong></div>
          </div>
        </article>
      </div>

      <!-- Current strategy stock positions cards fallback -->
      <article class="panel-card positions-card-wrapper" style="padding: var(--space-4);">
        <div class="panel-card__head" style="margin-bottom: var(--space-3);">
          <div>
            <span class="section-kicker">CURRENT HOLDINGS</span>
            <h3>当前策略持仓池</h3>
          </div>
        </div>
        <div v-if="!accountPositions.length" class="empty-state" style="padding: var(--space-5); text-align: center; color: var(--text-muted);">
          当前无持仓股票
        </div>
        <div v-else class="positions-cards-list" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: var(--space-3);">
          <div
            v-for="row in accountPositions"
            :key="row.symbol"
            class="pos-tile"
            style="border: 1px solid var(--border-default); border-radius: var(--radius-sm); padding: var(--space-3); background: rgba(245, 242, 234, 0.4);"
          >
            <div style="display: flex; justify-content: space-between; font-weight: 800; border-bottom: 1px dashed var(--border-default); padding-bottom: 4px; margin-bottom: var(--space-2);">
              <span style="color: var(--text-bright);">{{ row.stock_name || '-' }}</span>
              <span style="font-family: var(--font-data); color: var(--text-secondary);">{{ row.symbol }}</span>
            </div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 4px; font-size: var(--text-xs); color: var(--text-muted);">
              <span>持股/可用:</span>
              <span style="text-align: right; color: var(--text-primary);">{{ formatQuantity(row.quantity) }} / {{ formatQuantity(row.available) }}</span>
              <span>最新/成本:</span>
              <span style="text-align: right; color: var(--text-primary);">{{ formatPrice(positionLastPrice(row)) }} / {{ formatPrice(row.avg_cost) }}</span>
              <span>持仓市值:</span>
              <span style="text-align: right; color: var(--text-primary);">{{ formatMoney(row.market_value) }}</span>
              <span>盈亏比例:</span>
              <span :class="pnlTone(row.unrealized_pnl_pct)" style="text-align: right;">{{ formatPercent(row.unrealized_pnl_pct) }}</span>
            </div>
          </div>
        </div>
      </article>
    </div>

    <section ref="pendingPanelRef" class="panel-card pending-order-panel">
      <div class="panel-card__head">
        <div>
          <span class="section-kicker">PENDING FILLS</span>
          <h3>待成交追踪</h3>
        </div>
        <div class="table-actions">
          <span>{{ pendingOrders.length }} 笔实盘委托待确认</span>
          <span :class="['pending-sync-pill', `pending-sync-pill--${orderSyncTone}`]">{{ orderSyncStatusText }}</span>
          <span class="stale-control">超过
            <el-input-number v-model="staleOrderMinutes" :min="0" :max="240" :step="1" size="small" controls-position="right" />
            分钟
          </span>
          <el-button text size="small" :loading="pendingLoading" @click="syncPendingOrderStatus">同步成交</el-button>
          <el-button text size="small" type="warning" :disabled="!pendingOrders.length" :loading="pendingActionLoading" @click="cancelPendingOrders">批量撤单</el-button>
          <el-button text size="small" type="info" :disabled="!pendingOrders.length" :loading="pendingActionLoading" @click="closeLocalPendingOrders()">本地关闭</el-button>
          <el-button text size="small" type="danger" :disabled="!pendingOrders.length" :loading="pendingActionLoading" @click="cancelAndResubmitPendingOrders">撤单再提交</el-button>
        </div>
      </div>
      <el-table
        :data="pendingOrders"
        size="small"
        border
        height="280"
        empty-text="暂无待成交真实委托"
        @selection-change="onPendingSelectionChange"
      >
        <el-table-column type="selection" width="44" />
        <el-table-column label="提交时间" width="168" resizable>
          <template #default="{ row }">{{ formatDateTime(row.created_at) }}</template>
        </el-table-column>
        <el-table-column label="状态" width="116" resizable>
          <template #default="{ row }">
            <el-tag :type="statusTagType(row.status)" effect="plain">{{ statusLabel(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="side" label="方向" width="80" resizable>
          <template #default="{ row }">{{ sideLabel(row.side) }}</template>
        </el-table-column>
        <el-table-column prop="symbol" label="代码" width="110" resizable />
        <el-table-column label="名称" width="130" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.stock_name || '-' }}</template>
        </el-table-column>
        <el-table-column label="数量" width="100" resizable align="right">
          <template #default="{ row }">{{ formatQuantity(row.quantity) }}</template>
        </el-table-column>
        <el-table-column label="已成" width="100" resizable align="right">
          <template #default="{ row }">{{ formatQuantity(payloadNumber(row, 'filled_quantity')) }}</template>
        </el-table-column>
        <el-table-column label="剩余" width="100" resizable align="right">
          <template #default="{ row }">{{ formatQuantity(pendingRemainingQuantity(row)) }}</template>
        </el-table-column>
        <el-table-column label="委托价" width="110" resizable align="right">
          <template #default="{ row }">{{ formatPrice(row.reference_price) }}</template>
        </el-table-column>
        <el-table-column label="委托金额" width="130" resizable align="right">
          <template #default="{ row }">{{ formatMoney(row.order_value) }}</template>
        </el-table-column>
        <el-table-column label="QMT委托号" width="130" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.order_id || '-' }}</template>
        </el-table-column>
        <el-table-column label="QMT状态" width="140" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.result_payload?.status_msg || row.result_payload?.order_status || '-' }}</template>
        </el-table-column>
        <el-table-column label="说明" min-width="220" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.message || row.result_payload?.message || '-' }}</template>
        </el-table-column>
        <el-table-column label="操作" width="104" fixed="right">
          <template #default="{ row }">
            <el-button text size="small" type="info" :loading="pendingActionLoading" @click="closeLocalPendingOrders([row])">本地关闭</el-button>
          </template>
        </el-table-column>
      </el-table>
    </section>

    <section class="trade-journal-grid">
      <article class="panel-card trade-record-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">TRADE JOURNAL</span>
            <h3>交易记录</h3>
          </div>
          <el-button text size="small" :loading="journalLoading" @click="loadTradeJournal">刷新</el-button>
        </div>
        <el-table :data="tradeRecords" size="small" border height="320">
          <el-table-column label="时间" width="168" resizable>
            <template #default="{ row }">{{ formatDateTime(row.created_at) }}</template>
          </el-table-column>
          <el-table-column prop="mode" label="模式" width="72" resizable />
          <el-table-column label="状态" width="108" resizable>
            <template #default="{ row }">
              <el-tag :type="statusTagType(row.status)" effect="plain">{{ statusLabel(row.status) }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="side" label="方向" width="72" resizable />
          <el-table-column prop="symbol" label="代码" width="110" resizable />
          <el-table-column label="名称" width="120" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.stock_name || '-' }}</template>
          </el-table-column>
          <el-table-column prop="quantity" label="数量" width="100" resizable align="right">
            <template #default="{ row }">{{ formatQuantity(row.quantity) }}</template>
          </el-table-column>
          <el-table-column prop="reference_price" label="参考价" width="110" resizable align="right">
            <template #default="{ row }">{{ formatPrice(row.reference_price) }}</template>
          </el-table-column>
          <el-table-column prop="order_value" label="金额" width="120" resizable align="right">
            <template #default="{ row }">{{ formatMoney(row.order_value) }}</template>
          </el-table-column>
          <el-table-column label="说明" min-width="220" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.message || row.result_payload?.message || '-' }}</template>
          </el-table-column>
        </el-table>
      </article>

      <article class="panel-card weekly-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">WEEKLY REVIEW</span>
            <h3>每周分析</h3>
          </div>
          <span class="meta-text">
            {{ weeklyWindowLabel }}
          </span>
        </div>
        <div class="weekly-summary">
          <div v-for="item in weeklySummaryItems" :key="item.label">
            <label>{{ item.label }}</label>
            <strong :class="item.tone ? `metric-${item.tone}` : ''">{{ item.value }}</strong>
          </div>
        </div>
        <el-alert
          v-if="weeklyAnalysis?.notes?.length"
          type="info"
          :closable="false"
          show-icon
          :title="weeklyAnalysis.notes[0]"
          class="weekly-note"
        />
        <div v-if="(weeklyAnalysis?.notes || []).length > 1" class="weekly-note-list">
          <span v-for="note in weeklyAnalysis?.notes.slice(1) || []" :key="note">{{ note }}</span>
        </div>
        <div class="weekly-table-title">本周交易额 Top 10</div>
        <el-table :data="weeklyAnalysis?.top_symbols || []" size="small" border height="180">
          <el-table-column prop="symbol" label="代码" min-width="84" resizable />
          <el-table-column label="名称" min-width="96" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.stock_name || '-' }}</template>
          </el-table-column>
          <el-table-column prop="records" label="次数" min-width="58" resizable align="right" />
          <el-table-column prop="notional" label="金额" min-width="76" resizable align="right">
            <template #default="{ row }">{{ formatMoney(row.notional) }}</template>
          </el-table-column>
          <el-table-column prop="net_notional" label="净额" min-width="76" resizable align="right">
            <template #default="{ row }">{{ formatMoney(row.net_notional) }}</template>
          </el-table-column>
        </el-table>
      </article>
    </section>

    <el-dialog
      v-model="submitResultDialogOpen"
      title="提交结果"
      width="960px"
      class="submit-result-dialog"
    >
      <div class="submit-summary">
        <div v-for="item in submitResultSummary" :key="item.label">
          <label>{{ item.label }}</label>
          <strong :class="item.tone ? `metric-${item.tone}` : ''">{{ item.value }}</strong>
        </div>
      </div>
      <el-alert
        v-if="submitResultNotice"
        :type="submitResultNotice.type"
        show-icon
        :closable="false"
        :title="submitResultNotice.title"
        class="submit-result-note"
      />
      <el-table :data="submitResultRows" size="small" border max-height="420" empty-text="暂无提交结果">
        <el-table-column label="状态" width="118" resizable>
          <template #default="{ row }">
            <el-tag :type="statusTagType(row.status)" effect="plain">{{ statusLabel(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="方向" width="76" resizable>
          <template #default="{ row }">{{ sideLabel(row.side) }}</template>
        </el-table-column>
        <el-table-column prop="symbol" label="代码" width="110" resizable />
        <el-table-column label="名称" width="130" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.stock_name || '-' }}</template>
        </el-table-column>
        <el-table-column label="数量" width="100" resizable align="right">
          <template #default="{ row }">{{ formatQuantity(row.quantity) }}</template>
        </el-table-column>
        <el-table-column label="价格" width="110" resizable align="right">
          <template #default="{ row }">{{ formatPrice(row.reference_price) }}</template>
        </el-table-column>
        <el-table-column label="金额" width="125" resizable align="right">
          <template #default="{ row }">{{ formatMoney(row.order_value) }}</template>
        </el-table-column>
        <el-table-column label="委托号" width="130" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.order_id || '-' }}</template>
        </el-table-column>
        <el-table-column label="说明" min-width="220" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.message || '-' }}</template>
        </el-table-column>
      </el-table>
      <template #footer>
        <el-button @click="submitResultDialogOpen = false">关闭</el-button>
        <el-button
          v-if="submitResultPendingCount"
          type="primary"
          @click="scrollToPendingOrders"
        >
          查看待成交
        </el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="profileDialogOpen" title="新增 Live Profile" width="520px">
      <el-form label-width="110px">
        <el-form-item label="策略 ID">
          <el-input-number v-model="newProfile.strategy_id" :min="1" />
        </el-form-item>
        <el-form-item label="Profile Key">
          <el-input v-model="newProfile.profile_key" />
        </el-form-item>
        <el-form-item label="显示名">
          <el-input v-model="newProfile.display_name" />
        </el-form-item>
        <el-form-item label="默认">
          <el-switch v-model="newProfile.is_default" />
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="newProfile.enabled" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="profileDialogOpen = false">取消</el-button>
        <el-button type="primary" :loading="profileSaving" @click="createProfile">保存</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="capitalDialogOpen" title="圈定 / 调整量化交易本金" width="560px">
      <el-alert
        type="info"
        :closable="false"
        show-icon
        title="再次调整时，输入的是目标本金：已有持仓按成本价×数量计入本金，剩余部分作为可用现金参与新目标调仓。"
      />
      <el-form class="capital-form" label-width="120px">
        <el-form-item label="当前 Profile">
          <span>{{ selectedProfile?.display_name || selectedProfileKey || '-' }}</span>
        </el-form-item>
        <el-form-item label="模式">
          <el-tag>{{ mode === 'paper' ? '模拟' : '实盘' }}</el-tag>
        </el-form-item>
        <el-form-item label="目标本金">
          <el-input-number
            v-model="capitalForm.capital"
            :min="10000"
            :step="10000"
            :precision="2"
            controls-position="right"
            class="capital-input"
          />
        </el-form-item>
        <el-form-item label="重新初始化">
          <el-switch v-model="capitalForm.reset_existing" />
          <span class="form-hint">关闭时按成本本金口径调整；开启时清空策略资金池重来。</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="capitalDialogOpen = false">取消</el-button>
        <el-button type="primary" :loading="capitalSaving" @click="initializeCapitalPool">
          {{ strategyAccountReady && !capitalForm.reset_existing ? '确认调整' : '确认初始化' }}
        </el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="positionsDialogOpen"
      :title="`${strategyPortfolioTitle} 持仓明细`"
      width="min(1180px, 94vw)"
      class="positions-dialog"
    >
      <div class="positions-dialog__toolbar">
        <div class="positions-dialog__summary">
          <span>现金 {{ formatMoney(accountSnapshot?.cash) }}</span>
          <span>总资产 {{ formatMoney(accountSnapshot?.total_asset) }}</span>
          <span>持仓 {{ accountSnapshot?.position_count || 0 }} 只</span>
        </div>
        <span class="drag-hint">拖拽表头排序</span>
      </div>
      <el-table :data="accountPositions" size="small" stripe border height="560" empty-text="暂无持仓">
        <el-table-column
          v-for="column in visiblePositionColumns"
          :key="column.key"
          :prop="column.prop"
          :label="column.label"
          :min-width="column.minWidth || column.width"
          :align="column.align"
          resizable
          show-overflow-tooltip
        >
          <template #header>
            <span
              :class="positionColumnHeaderClass(column.key)"
              :data-position-column-key="column.key"
              @pointerdown="onPositionColumnPointerDown($event, column.key)"
            >
              <span class="position-column-header__grip" aria-hidden="true"></span>
              <span>{{ column.label }}</span>
            </span>
          </template>
          <template #default="{ row }">
            <div
              v-if="column.key === 'quantity_available'"
              class="stack-cell stack-cell--right"
            >
              <strong>{{ formatQuantity(row.quantity) }}</strong>
              <small>{{ formatQuantity(row.available) }}</small>
            </div>
            <div
              v-else-if="column.key === 'price_cost'"
              class="stack-cell stack-cell--right"
            >
              <strong>{{ formatPrice(positionLastPrice(row)) }}</strong>
              <small>{{ formatPrice(row.avg_cost) }}</small>
            </div>
            <span v-else :class="positionCellClass(row, column.key)">{{ positionCellValue(row, column.key) }}</span>
          </template>
        </el-table-column>
      </el-table>
      <template #footer>
        <el-button @click="positionsDialogOpen = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="strategyReviewDialogOpen"
      :title="`${currentStrategyName} 低频复盘`"
      width="min(1180px, 94vw)"
      class="strategy-review-dialog"
      @opened="renderStrategyReviewCharts"
      @closed="disposeStrategyReviewCharts"
    >
      <div class="strategy-review">
        <div class="strategy-review__intro">
          <div>
            <span class="section-kicker">PORTFOLIO REVIEW</span>
            <h3>{{ strategyPortfolioTitle }}</h3>
            <p>{{ selectedProfile?.description || selectedProfile?.strategy_name || '当前策略组合的建池以来表现、资金使用和成交质量。' }}</p>
          </div>
          <div class="strategy-review__facts">
            <span v-for="row in strategyReviewRows" :key="row.label">
              <label>{{ row.label }}</label>
              <strong>{{ row.value }}</strong>
            </span>
          </div>
        </div>

        <div class="strategy-review__metrics">
          <div v-for="item in strategyReviewMetrics" :key="item.label">
            <label>{{ item.label }}</label>
            <strong :class="item.tone ? `metric-${item.tone}` : ''">{{ item.value }}</strong>
          </div>
        </div>

        <div class="strategy-review__charts">
          <section>
            <div class="strategy-chart-head">
              <span>权益与回撤</span>
              <small>{{ equityCurve.length }} 个快照</small>
            </div>
            <div v-if="equityCurve.length" ref="equityChartRef" class="strategy-chart"></div>
            <div v-else class="strategy-chart strategy-chart--empty">暂无权益快照；刷新账户或成交同步后会自动补点。</div>
          </section>
          <section>
            <div class="strategy-chart-head">
              <span>每日净买入</span>
              <small>{{ weeklyWindowLabel }}</small>
            </div>
            <div v-if="flowChartRows.length" ref="flowChartRef" class="strategy-chart"></div>
            <div v-else class="strategy-chart strategy-chart--empty">本周暂无交易流。</div>
          </section>
          <section>
            <div class="strategy-chart-head">
              <span>成交状态</span>
              <small>订单质量</small>
            </div>
            <div v-if="statusChartRows.length" ref="statusChartRef" class="strategy-chart"></div>
            <div v-else class="strategy-chart strategy-chart--empty">暂无订单状态。</div>
          </section>
        </div>

        <div class="strategy-review__tables">
          <section>
            <div class="weekly-table-title">交易额 Top 10</div>
            <el-table :data="weeklyAnalysis?.top_symbols || []" size="small" border height="220">
              <el-table-column prop="symbol" label="代码" min-width="84" resizable />
              <el-table-column label="名称" min-width="96" resizable show-overflow-tooltip>
                <template #default="{ row }">{{ row.stock_name || '-' }}</template>
              </el-table-column>
              <el-table-column prop="records" label="次数" min-width="58" resizable align="right" />
              <el-table-column prop="notional" label="金额" min-width="76" resizable align="right">
                <template #default="{ row }">{{ formatMoney(row.notional) }}</template>
              </el-table-column>
              <el-table-column prop="net_notional" label="净额" min-width="76" resizable align="right">
                <template #default="{ row }">{{ formatMoney(row.net_notional) }}</template>
              </el-table-column>
            </el-table>
          </section>
          <section>
            <div class="weekly-table-title">当前持仓 Top 10</div>
            <el-table :data="accountPositions.slice(0, 10)" size="small" border height="220">
              <el-table-column prop="symbol" label="代码" width="110" resizable />
              <el-table-column label="股票名称" width="130" resizable show-overflow-tooltip>
                <template #default="{ row }">{{ row.stock_name || '-' }}</template>
              </el-table-column>
              <el-table-column label="市值" width="120" resizable align="right">
                <template #default="{ row }">{{ formatMoney(row.market_value) }}</template>
              </el-table-column>
              <el-table-column label="仓位" width="100" resizable align="right">
                <template #default="{ row }">{{ formatPercent(row.position_pct) }}</template>
              </el-table-column>
              <el-table-column label="总盈亏比例" width="120" resizable align="right">
                <template #default="{ row }">
                  <span :class="pnlTone(row.unrealized_pnl_pct)">{{ formatPercent(row.unrealized_pnl_pct) }}</span>
                </template>
              </el-table-column>
            </el-table>
          </section>
        </div>
      </div>
      <template #footer>
        <el-button @click="strategyReviewDialogOpen = false">关闭</el-button>
        <el-button type="primary" @click="openPositionsDialog">查看持仓明细</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
import * as echarts from '@/lib/echarts'
import { systemApi, type LiveTradingGuardrails } from '@/api/system'
import { syncApi, type SyncStatus } from '@/api/sync'
import {
  liveTradingApi,
  type LiveAccountSnapshot,
  type LiveAccountPosition,
  type LiveOrder,
  type LiveOrderAudit,
  type LiveOrderSyncResponse,
  type LivePreflightResponse,
  type LiveSignalsResponse,
  type LiveStrategyProfile,
  type LiveSubmitOrdersResponse,
  type LiveTradeRecord,
  type LiveTradingMode,
  type LiveTradingStatus,
  type LiveWeeklyAnalysis,
} from '@/api/liveTrading'

const modeOptions = [
  { label: '模拟', value: 'paper' },
  { label: '实盘', value: 'live' },
]

const layoutMode = ref<'A' | 'B' | 'C'>('A')
const mode = ref<LiveTradingMode>('live')
const tradeDate = ref(new Date().toISOString().slice(0, 10))
const indexSymbol = ref('399101.SZ')
const selectedProfileKey = ref('')
const status = ref<LiveTradingStatus | null>(null)
const liveGuardrails = ref<LiveTradingGuardrails | null>(null)
const guardrailDraft = reactive({
  enable_order_submit: false,
  auto_execute_enabled: false,
})
const profiles = ref<LiveStrategyProfile[]>([])
const preflightData = ref<LivePreflightResponse | null>(null)
const signalData = ref<LiveSignalsResponse | null>(null)
const accountSnapshot = ref<LiveAccountSnapshot | null>(null)
const orderRows = ref<LiveOrder[]>([])
const audits = ref<LiveOrderAudit[]>([])
const tradeRecords = ref<LiveTradeRecord[]>([])
const pendingOrders = ref<LiveTradeRecord[]>([])
const selectedPendingOrders = ref<LiveTradeRecord[]>([])
const weeklyAnalysis = ref<LiveWeeklyAnalysis | null>(null)
const orderPanelRef = ref<HTMLElement | null>(null)
const pendingPanelRef = ref<HTMLElement | null>(null)
const loading = ref(false)
const accountLoading = ref(false)
const preflightLoading = ref(false)
const signalsLoading = ref(false)
const runnerLoading = ref(false)
const pendingActionLoading = ref(false)
const journalLoading = ref(false)
const pendingLoading = ref(false)
const savingGuardrails = ref(false)
const profileDialogOpen = ref(false)
const profileSaving = ref(false)
const capitalDialogOpen = ref(false)
const capitalSaving = ref(false)
const positionsDialogOpen = ref(false)
const strategyReviewDialogOpen = ref(false)
const submitResultDialogOpen = ref(false)
const submitResult = ref<LiveSubmitOrdersResponse | null>(null)
const staleOrderMinutes = ref(10)
const draggedPositionColumn = ref<PositionColumnKey | null>(null)
const dragOverPositionColumn = ref<PositionColumnKey | null>(null)
const equityChartRef = ref<HTMLElement | null>(null)
const flowChartRef = ref<HTMLElement | null>(null)
const statusChartRef = ref<HTMLElement | null>(null)
let equityChart: echarts.ECharts | null = null
let flowChart: echarts.ECharts | null = null
let statusChart: echarts.ECharts | null = null
let accountEventSource: EventSource | null = null
let accountPollTimer: number | null = null
let orderSyncTimer: number | null = null
let orderSyncBurstUntil = 0
let orderSyncInFlight = false
const liveStreamState = ref<'off' | 'connecting' | 'live' | 'polling' | 'error'>('off')
const liveStreamUpdatedAt = ref<string | null>(null)
const liveStreamError = ref('')
const orderSyncState = ref<'idle' | 'syncing' | 'fresh' | 'watching' | 'error'>('idle')
const orderSyncUpdatedAt = ref<string | null>(null)
const orderSyncError = ref('')
const newProfile = reactive({
  strategy_id: 62,
  profile_key: '',
  display_name: '',
  enabled: true,
  is_default: false,
})
const capitalForm = reactive({
  capital: 100000,
  reset_existing: false,
})

type RuntimeStepStatus = 'pending' | 'running' | 'done' | 'warn' | 'error'
type RuntimeTone = 'neutral' | 'running' | 'success' | 'warn' | 'error'

interface RuntimeStep {
  id: string
  label: string
  status: RuntimeStepStatus
  detail?: string
}

const runtimeState = reactive({
  visible: false,
  active: false,
  scope: '运行状态',
  title: '待命',
  detail: '等待操作',
  tone: 'neutral' as RuntimeTone,
  progress: 0,
  updatedAt: '',
  steps: [] as RuntimeStep[],
})
let runtimeSyncTimer: number | null = null

type PositionColumnKey =
  | 'symbol'
  | 'stock_name'
  | 'quantity_available'
  | 'price_cost'
  | 'volume_ratio'
  | 'today_change_pct'
  | 'turnover_rate'
  | 'market_value'
  | 'position_pct'
  | 'unrealized_pnl_pct'
  | 'amount'

interface PositionColumnDef {
  key: PositionColumnKey
  prop?: string
  label: string
  width?: number
  minWidth?: number
  align?: 'left' | 'center' | 'right'
}

const positionColumnDefs: PositionColumnDef[] = [
  { key: 'symbol', prop: 'symbol', label: '代码', width: 92 },
  { key: 'stock_name', prop: 'stock_name', label: '股票名称', width: 120 },
  { key: 'quantity_available', label: '持仓/可用', width: 126, align: 'right' },
  { key: 'price_cost', label: '最新价/成本价', width: 132, align: 'right' },
  { key: 'volume_ratio', prop: 'volume_ratio', label: '量比', width: 82, align: 'right' },
  { key: 'today_change_pct', prop: 'today_change_pct', label: '涨跌幅', width: 92, align: 'right' },
  { key: 'turnover_rate', prop: 'turnover_rate', label: '换手率', width: 92, align: 'right' },
  { key: 'market_value', prop: 'market_value', label: '市值', width: 108, align: 'right' },
  { key: 'position_pct', prop: 'position_pct', label: '仓位', width: 78, align: 'right' },
  { key: 'unrealized_pnl_pct', prop: 'unrealized_pnl_pct', label: '总盈亏比例', width: 104, align: 'right' },
  { key: 'amount', prop: 'amount', label: '成交额', width: 116, align: 'right' },
]
const positionColumnOrder = ref<PositionColumnKey[]>(positionColumnDefs.map(column => column.key))

const selectedProfile = computed(() => profiles.value.find(item => item.profile_key === selectedProfileKey.value) || null)
const currentStrategyName = computed(() => selectedProfile.value?.display_name || selectedProfile.value?.strategy_name || selectedProfileKey.value || '策略组合')
const strategyPortfolioTitle = computed(() => `${currentStrategyName.value} Portfolio`)
const strategyPortfolioSubtitle = computed(() => {
  const profile = selectedProfile.value
  if (!profile) return '当前策略独立资金池和策略持仓'
  return `ID ${profile.strategy_id} · ${profile.adapter_type}`
})
const runnerText = computed(() => {
  const runner = status.value?.runner
  if (!runner) return '-'
  return runner.takeover ? '人工接管' : runner.status
})
const candidatePreview = computed(() => (signalData.value?.top_candidates || []).slice(0, 3))
const orderPreview = computed(() => orderRows.value.slice(0, 3))
// Remove unused computed notes
const accountPositions = computed(() => accountSnapshot.value?.positions || [])
const liveStreamStatusText = computed(() => {
  if (mode.value !== 'live') return '实时流关闭'
  if (liveStreamState.value === 'live') return `实时流 ${formatClock(liveStreamUpdatedAt.value)}`
  if (liveStreamState.value === 'connecting') return '实时流连接中'
  if (liveStreamState.value === 'polling') return `轮询 ${formatClock(liveStreamUpdatedAt.value)}`
  if (liveStreamState.value === 'error') return liveStreamError.value || '实时流异常'
  return '实时流关闭'
})
const liveStreamStateClass = computed(() => {
  if (liveStreamState.value === 'live') return 'live-stream-pill--live'
  if (liveStreamState.value === 'polling' || liveStreamState.value === 'connecting') return 'live-stream-pill--polling'
  if (liveStreamState.value === 'error') return 'live-stream-pill--error'
  return 'live-stream-pill--off'
})
const orderSyncTone = computed(() => {
  if (orderSyncState.value === 'syncing' || orderSyncState.value === 'watching') return 'syncing'
  if (orderSyncState.value === 'error') return 'error'
  if (orderSyncState.value === 'fresh') return 'fresh'
  return 'idle'
})
const orderSyncStatusText = computed(() => {
  if (mode.value !== 'live') return '模拟模式'
  if (orderSyncState.value === 'syncing') return '正在同步成交'
  if (orderSyncState.value === 'watching') return `追踪中 ${formatClock(orderSyncUpdatedAt.value)}`
  if (orderSyncState.value === 'fresh') return `已同步 ${formatClock(orderSyncUpdatedAt.value)}`
  if (orderSyncState.value === 'error') return orderSyncError.value || '同步失败'
  return pendingOrders.value.length ? '等待同步' : '无待成交'
})
const runtimeVisible = computed(() => runtimeState.visible)
const runtimeTone = computed(() => runtimeState.tone)
const runtimeProgress = computed(() => Math.max(0, Math.min(100, Math.round(runtimeState.progress || 0))))
const runtimeProgressStatus = computed(() => {
  if (runtimeState.tone === 'success') return 'success'
  if (runtimeState.tone === 'error') return 'exception'
  if (runtimeState.tone === 'warn') return 'warning'
  return undefined
})
const runtimeProgressText = computed(() => {
  const updated = runtimeState.updatedAt ? ` · ${formatClock(runtimeState.updatedAt)}` : ''
  return `${runtimeProgress.value}%${updated}`
})
const guardrailsDirty = computed(() => Boolean(
  liveGuardrails.value
  && (
    guardrailDraft.enable_order_submit !== liveGuardrails.value.enable_order_submit
    || guardrailDraft.auto_execute_enabled !== liveGuardrails.value.auto_execute_enabled
  ),
))
const orderedPositionColumnDefs = computed(() => {
  const byKey = new Map(positionColumnDefs.map(column => [column.key, column]))
  return positionColumnOrder.value.map(key => byKey.get(key)).filter(Boolean) as PositionColumnDef[]
})
function stretchTrailingPositionColumn(columns: PositionColumnDef[]): PositionColumnDef[] {
  if (!columns.length) return columns
  const trailingIndex = columns.length - 1
  return columns.map((column, index) => (
    index === trailingIndex
      ? {
          ...column,
          width: undefined,
          minWidth: Math.max(column.minWidth || 0, column.width || 0, 132),
        }
      : column
  ))
}
const visiblePositionColumns = computed(() => stretchTrailingPositionColumn(orderedPositionColumnDefs.value))
const brokerAccountSnapshot = computed(() => accountSnapshot.value?.broker_account || null)
const strategyAccountReady = computed(() => Boolean(accountSnapshot.value?.meta?.initialized))
const strategyAccountStatusText = computed(() => {
  if (!accountSnapshot.value) return '未加载'
  if (strategyAccountReady.value) return mode.value === 'paper' ? '模拟资金池' : '实盘资金池'
  return '未圈定本金'
})
const accountSourceText = computed(() => {
  const source = accountSnapshot.value?.source || (mode.value === 'paper' ? 'paper' : 'qmt')
  if (accountSnapshot.value?.meta?.stale_display_only) {
    return mode.value === 'paper' ? `模拟 · ${source}` : '实盘 · 最近同步持仓'
  }
  return mode.value === 'paper' ? `模拟 · ${source}` : `实盘 · ${source}`
})
const accountSummaryItems = computed(() => {
  const account = accountSnapshot.value
  const pnl = Number(account?.unrealized_pnl || 0)
  const totalPnl = Number(account?.total_pnl || 0)
  const meta = account?.meta || {}
  const targetCapital = Number(meta.target_capital || meta.initial_capital || 0)
  const positionCostBasis = Number(meta.position_cost_basis || 0)
  return [
    { label: '目标本金', value: formatMoney(targetCapital || null) },
    { label: '持仓成本', value: formatMoney(positionCostBasis) },
    { label: '可用现金', value: formatMoney(account?.cash) },
    { label: '总资产', value: formatMoney(account?.total_asset) },
    { label: '持仓市值', value: formatMoney(account?.market_value) },
    { label: '浮盈亏', value: formatMoney(pnl), tone: pnl > 0 ? 'good' : pnl < 0 ? 'bad' : 'neutral' },
    { label: '总盈亏', value: formatMoney(totalPnl), tone: totalPnl > 0 ? 'good' : totalPnl < 0 ? 'bad' : 'neutral' },
    { label: '总盈亏率', value: formatPercent(account?.total_pnl_pct), tone: totalPnl > 0 ? 'good' : totalPnl < 0 ? 'bad' : 'neutral' },
    { label: '持仓数', value: `${account?.position_count || 0}` },
  ]
})
const brokerSummaryItems = computed(() => {
  const account = brokerAccountSnapshot.value
  const pnl = Number(account?.unrealized_pnl || 0)
  return [
    { label: 'QMT现金', value: formatMoney(account?.cash) },
    { label: 'QMT总资产', value: formatMoney(account?.total_asset) },
    { label: 'QMT持仓市值', value: formatMoney(account?.market_value) },
    { label: 'QMT浮盈亏', value: formatMoney(pnl), tone: pnl > 0 ? 'good' : pnl < 0 ? 'bad' : 'neutral' },
    { label: 'QMT持仓数', value: `${account?.position_count || 0}` },
  ]
})
const weeklyWindowLabel = computed(() => {
  const analysis = weeklyAnalysis.value
  if (!analysis) return '本周'
  return `${analysis.week_start} ~ ${analysis.week_end}`
})
const weeklySummaryItems = computed(() => {
  const summary = weeklyAnalysis.value?.summary
  const weeklyPnl = Number(summary?.weekly_pnl || 0)
  const allTimePnl = Number(summary?.all_time_pnl || 0)
  return [
    { label: '本周PnL', value: formatMoney(summary?.weekly_pnl), tone: weeklyPnl > 0 ? 'good' : weeklyPnl < 0 ? 'bad' : 'neutral' },
    { label: '本周回撤', value: formatPercent(summary?.weekly_max_drawdown_pct), tone: Number(summary?.weekly_max_drawdown_pct || 0) < 0 ? 'bad' : 'neutral' },
    { label: '至今PnL', value: formatMoney(summary?.all_time_pnl), tone: allTimePnl > 0 ? 'good' : allTimePnl < 0 ? 'bad' : 'neutral' },
    { label: '至今回撤', value: formatPercent(summary?.all_time_max_drawdown_pct), tone: Number(summary?.all_time_max_drawdown_pct || 0) < 0 ? 'bad' : 'neutral' },
    { label: '净买入', value: formatMoney(summary?.net_notional) },
    { label: '已成/待成/撤单/失败', value: `${summary?.completed_records || 0}/${summary?.live_submitted_records || 0}/${summary?.cancelled_records || 0}/${summary?.failed_records || 0}` },
  ]
})
const strategyReviewMetrics = computed(() => {
  const summary = weeklyAnalysis.value?.summary
  const account = accountSnapshot.value
  const targetCapital = Number(account?.meta?.target_capital || account?.meta?.initial_capital || 0)
  const cash = Number(account?.cash || 0)
  const totalAsset = Number(account?.total_asset || 0)
  const marketValue = Number(account?.market_value || 0)
  const allTimePnl = Number(summary?.all_time_pnl ?? account?.total_pnl ?? 0)
  const exposure = totalAsset > 0 ? marketValue / totalAsset : null
  const cashRatio = totalAsset > 0 ? cash / totalAsset : null
  const deployment = targetCapital > 0 ? totalAsset / targetCapital : null
  const turnover = targetCapital > 0 ? Math.abs(Number(summary?.buy_notional || 0) + Number(summary?.sell_notional || 0)) / targetCapital : null
  return [
    { label: '建池以来PnL', value: formatMoney(summary?.all_time_pnl ?? account?.total_pnl), tone: allTimePnl > 0 ? 'good' : allTimePnl < 0 ? 'bad' : 'neutral' },
    { label: '建池收益率', value: formatPercent(summary?.all_time_return_pct ?? account?.total_pnl_pct), tone: allTimePnl > 0 ? 'good' : allTimePnl < 0 ? 'bad' : 'neutral' },
    { label: '最大回撤', value: formatPercent(summary?.all_time_max_drawdown_pct), tone: Number(summary?.all_time_max_drawdown_pct || 0) < 0 ? 'bad' : 'neutral' },
    { label: '权益快照', value: `${summary?.all_time_equity_snapshot_points || 0} 点` },
    { label: '仓位暴露', value: formatPercent(exposure) },
    { label: '现金比例', value: formatPercent(cashRatio), tone: cashRatio != null && cashRatio < 0.05 ? 'warn' : 'neutral' },
    { label: '目标资产达成', value: formatPercent(deployment) },
    { label: '本周换手代理', value: formatPercent(turnover) },
    { label: '成交/撤单', value: `${summary?.completed_records || 0}/${summary?.cancelled_records || 0}` },
    { label: '当前持仓', value: `${account?.position_count || 0} 只` },
  ]
})
const strategyReviewRows = computed(() => [
  { label: '策略', value: currentStrategyName.value },
  { label: 'Profile Key', value: selectedProfileKey.value || '-' },
  { label: '模式', value: mode.value === 'paper' ? '模拟' : '实盘' },
  { label: '账户来源', value: accountSourceText.value },
  { label: '目标本金', value: accountSummaryItems.value.find(item => item.label === '目标本金')?.value || '-' },
  { label: '本周窗口', value: weeklyWindowLabel.value },
])
const equityCurve = computed(() => weeklyAnalysis.value?.equity_curve || [])
const flowChartRows = computed(() => weeklyAnalysis.value?.by_day || [])
const statusChartRows = computed(() => weeklyAnalysis.value?.by_status || [])
const submitResultRows = computed(() => {
  const results = submitResult.value?.results || []
  return results.map(result => {
    const order = result.order || {}
    const status = result.status || (result.submitted ? (result.paper ? 'paper_filled' : 'live_pending') : 'failed')
    const quantity = isPendingStatus(status)
      ? toNumber(order.quantity)
      : (toNumber(result.filled_quantity) || toNumber(order.quantity))
    const referencePrice = toNumber(result.filled_price) || toNumber(order.reference_price) || toNumber(order.price)
    const orderValue = isPendingStatus(status)
      ? quantity * referencePrice
      : (toNumber(result.filled_value) || quantity * referencePrice)
    return {
      status,
      side: String(order.side || order.action || ''),
      symbol: String(order.symbol || ''),
      stock_name: String(order.stock_name || '') || null,
      quantity,
      reference_price: referencePrice,
      order_value: orderValue,
      order_id: result.order_id,
      message: result.message || statusLabel(status),
    }
  })
})
const submitResultPendingCount = computed(() => submitResultRows.value.filter(row => isPendingStatus(row.status)).length)
const submitResultFailedCount = computed(() => submitResultRows.value.filter(row => row.status === 'failed').length)
const submitResultFilledCount = computed(() => submitResultRows.value.filter(row => isFilledStatus(row.status)).length)
const submitResultSummary = computed(() => {
  const rows = submitResultRows.value
  const notional = rows.reduce((sum, row) => sum + Number(row.order_value || 0), 0)
  return [
    { label: '总笔数', value: `${rows.length || submitResult.value?.orders?.length || 0}` },
    { label: '已成交', value: `${submitResultFilledCount.value}`, tone: submitResultFilledCount.value ? 'good' : 'neutral' },
    { label: '待成交', value: `${submitResultPendingCount.value}`, tone: submitResultPendingCount.value ? 'bad' : 'neutral' },
    { label: '失败', value: `${submitResultFailedCount.value}`, tone: submitResultFailedCount.value ? 'bad' : 'neutral' },
    { label: '委托金额', value: formatMoney(notional) },
  ]
})
const submitResultNotice = computed(() => {
  if (!submitResult.value) return null
  if (submitResultPendingCount.value) {
    return {
      type: 'warning' as const,
      title: '实盘委托已送达 QMT，但平台尚未确认成交；请在 QMT 当日委托/成交里同步核对。',
    }
  }
  if (submitResultFailedCount.value || submitResult.value.message) {
    return {
      type: 'error' as const,
      title: submitResult.value.message || '部分订单提交失败，请查看明细说明。',
    }
  }
  return {
    type: 'success' as const,
    title: '订单已按当前模式处理完成。',
  }
})
const effectivePreflight = computed(() => signalData.value?.preflight || preflightData.value)
const preflightTagType = computed(() => {
  const data = effectivePreflight.value
  if (!data) return 'info'
  if (data.runner_blocking_reasons?.length || data.blocking_reasons?.length) return 'danger'
  if (data.warnings?.length) return 'warning'
  return 'success'
})
const preflightStatusText = computed(() => {
  const data = effectivePreflight.value
  if (!data) return '未检查'
  if (data.can_auto_submit) return '可自动实盘'
  if (data.can_start_runner && data.can_generate) return '可运行'
  if (data.can_start_runner) return '可启动等待'
  return '需处理'
})
const preflightStateClass = computed(() => stateClassFromType(preflightTagType.value))
const runnerStateClass = computed(() => {
  const runner = status.value?.runner
  if (!runner) return 'state-text--neutral'
  if (runner.takeover) return 'state-text--warn'
  if (runner.last_error) return 'state-text--bad'
  if (String(runner.status || '').toLowerCase() === 'running') return 'state-text--good'
  if (String(runner.status || '').toLowerCase() === 'stopped') return 'state-text--neutral'
  return 'state-text--warn'
})
const stateClassFromType = (type: string) => {
  if (type === 'success') return 'state-text--good'
  if (type === 'danger' || type === 'error') return 'state-text--bad'
  if (type === 'warning') return 'state-text--warn'
  return 'state-text--neutral'
}
const preflightIssues = computed(() => {
  const data = effectivePreflight.value
  if (!data) return []
  return uniqueStrings([...(data.blocking_reasons || []), ...(data.runner_blocking_reasons || []), ...(data.warnings || [])]).slice(0, 8)
})
const preflightActions = computed(() => (effectivePreflight.value?.next_actions || []).slice(0, 5))
const preflightChecks = computed(() => {
  const data = effectivePreflight.value
  const gaps = arrayField(data?.dependency_prepare, 'coverage_gaps')
  const emptyFactors = (data?.factor_coverage || []).filter(item => Number(item.value_count || 0) === 0)
  const qmt = data?.qmt_status || {}
  const phase = data?.market_phase || {}
  const factorDates = data?.factor_dates || {}
  const dailyFactorDate = String(factorDates.daily_factor_date || '-')
  const timerDates = uniqueStrings(
    arrayField(factorDates, 'requirements')
      .filter(item => String(item.date_policy || '') === 'same_day_intraday')
      .map(item => `${String(item.as_of_time || '盘中')}@${String(item.effective_date || '-')}`),
  )
  const phaseCanRun = Boolean(phase.can_run_now)
  const qmtReady = Boolean(qmt.account_configured) && Boolean(qmt.xttrader_available) && Boolean(qmt.quote_connected)
  return [
    {
      label: '交易阶段',
      value: phaseLabel(String(phase.phase || '-')),
      note: String(phase.note || '-'),
      tone: phaseCanRun ? 'good' : 'warn',
    },
    {
      label: '信号',
      value: data?.can_generate ? '可生成' : '阻断',
      note: `${data?.universe?.symbol_count || 0} 只股票池 / ${data?.pipeline_probe?.candidate_count ?? '-'} 候选`,
      tone: data?.can_generate ? 'good' : 'bad',
    },
    {
      label: 'Runner',
      value: data?.can_start_runner ? '可启动' : '阻断',
      note: status.value?.runner.status || '-',
      tone: data?.can_start_runner ? 'good' : 'bad',
    },
    {
      label: '数据依赖',
      value: gaps.length ? `${gaps.length} 项缺口` : '已覆盖',
      note: dependencyLabels(gaps) || '指数、行情、基础数据检查通过',
      tone: gaps.length ? 'warn' : 'good',
    },
    {
      label: '因子缓存',
      value: emptyFactors.length ? `${emptyFactors.length} 项为空` : '已命中',
      note: emptyFactors.slice(0, 3).map(item => String(item.name)).join(' / ') || `${data?.strategy?.factor_names?.length || 0} 个排序因子`,
      tone: emptyFactors.length ? 'warn' : 'good',
    },
    {
      label: '因子日期',
      value: `日频 ${dailyFactorDate}`,
      note: timerDates.slice(0, 2).join(' / ') || '盘中因子按当天 timer',
      tone: dailyFactorDate !== '-' ? 'good' : 'warn',
    },
    {
      label: 'QMT',
      value: qmtReady ? '已就绪' : '未就绪',
      note: `账户 ${qmt.account_configured ? '已配' : '未配'} / 行情 ${qmt.quote_connected ? '连接' : '未连'}`,
      tone: qmtReady ? 'good' : 'warn',
    },
    {
      label: '自动实盘',
      value: data?.can_auto_submit ? '可自动真单' : '受护栏保护',
      note: `真实下单 ${status.value?.order_submit_enabled ? '开' : '关'} / 自动 ${status.value?.auto_execute_enabled ? '开' : '关'}`,
      tone: data?.can_auto_submit ? 'bad' : 'neutral',
    },
  ]
})

function resetLiveGuardrailDraft() {
  if (!liveGuardrails.value) {
    guardrailDraft.enable_order_submit = Boolean(status.value?.order_submit_enabled)
    guardrailDraft.auto_execute_enabled = Boolean(status.value?.auto_execute_enabled)
    return
  }
  guardrailDraft.enable_order_submit = liveGuardrails.value.enable_order_submit
  guardrailDraft.auto_execute_enabled = liveGuardrails.value.auto_execute_enabled
}

function startRuntimeTask(
  scope: string,
  title: string,
  detail: string,
  steps: Array<{ id: string; label: string }>,
) {
  runtimeState.visible = true
  runtimeState.active = true
  runtimeState.scope = scope
  runtimeState.title = title
  runtimeState.detail = detail
  runtimeState.tone = 'running'
  runtimeState.progress = 4
  runtimeState.updatedAt = new Date().toISOString()
  runtimeState.steps = steps.map((step, index) => ({
    ...step,
    status: index === 0 ? 'running' : 'pending',
  }))
}

function setRuntimeMessage(title: string, detail: string, progress?: number) {
  runtimeState.title = title
  runtimeState.detail = detail
  runtimeState.updatedAt = new Date().toISOString()
  if (progress != null) {
    runtimeState.progress = Math.max(runtimeState.progress, progress)
  }
}

function setRuntimeStep(id: string, status: RuntimeStepStatus, detail?: string, progress?: number) {
  const index = runtimeState.steps.findIndex(step => step.id === id)
  if (index >= 0) {
    runtimeState.steps = runtimeState.steps.map((step, stepIndex) => {
      if (step.id === id) return { ...step, status, detail: detail || step.detail }
      if (status === 'running' && stepIndex < index && step.status === 'pending') {
        return { ...step, status: 'done' }
      }
      return step
    })
  }
  runtimeState.updatedAt = new Date().toISOString()
  if (progress != null) {
    runtimeState.progress = Math.max(runtimeState.progress, progress)
  }
}

function finishRuntimeTask(tone: RuntimeTone, title: string, detail: string) {
  runtimeState.active = false
  runtimeState.tone = tone
  runtimeState.title = title
  runtimeState.detail = detail
  runtimeState.updatedAt = new Date().toISOString()
  runtimeState.progress = tone === 'success' ? 100 : Math.max(runtimeState.progress, 8)
  runtimeState.steps = runtimeState.steps.map(step => {
    if (tone === 'success') return { ...step, status: 'done' }
    if (step.status === 'running') return { ...step, status: tone === 'error' ? 'error' : 'warn' }
    return step
  })
}

function startRuntimeSyncPolling() {
  stopRuntimeSyncPolling()
  const poll = async () => {
    try {
      applyRuntimeSyncStatus(await syncApi.getStatus())
    } catch {
      // The primary live-trading request should remain the source of truth.
    }
  }
  void poll()
  runtimeSyncTimer = window.setInterval(poll, 1500)
}

function stopRuntimeSyncPolling() {
  if (runtimeSyncTimer == null) return
  window.clearInterval(runtimeSyncTimer)
  runtimeSyncTimer = null
}

function applyRuntimeSyncStatus(sync: SyncStatus) {
  const state = String(sync.status || '')
  if (state !== 'running' && state !== 'queued') return
  const label = syncTypeLabel(sync.sync_type)
  const percent = Number(sync.progress_percent || 0)
  const total = Number(sync.total || 0)
  const current = Number(sync.current || 0)
  const countText = total > 0 ? `${current}/${total}` : stateLabel(state)
  const detail = `${label} ${countText}，成功 ${sync.success_count || 0}，失败 ${sync.failed_count || 0}`
  setRuntimeStep('sync', 'running', detail, 35 + Math.min(45, percent * 0.45))
  setRuntimeMessage(`正在同步${label}`, detail, 35 + Math.min(45, percent * 0.45))
}

function syncTypeLabel(value?: string | null) {
  const labels: Record<string, string> = {
    live_intraday_factor_dependency: '盘中依赖',
    factor_dependency: '因子依赖',
    kline_minute: '分钟线',
    kline_daily: '日线',
    realtime_mv: '实时市值',
    stock_info: '股票基础信息',
    stock_full: '股票全量信息',
    financial_data: '财务数据',
  }
  return labels[String(value || '')] || String(value || '同步任务')
}

function stateLabel(value: string) {
  const labels: Record<string, string> = {
    queued: '排队中',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
    cancelled: '已取消',
    idle: '空闲',
  }
  return labels[value] || value
}

async function loadGuardrails() {
  liveGuardrails.value = await systemApi.getLiveTradingGuardrails()
  resetLiveGuardrailDraft()
}

async function confirmLiveGuardrailSave(): Promise<{ acknowledge_risk: boolean; confirm_text?: string | null }> {
  if (!liveGuardrails.value) return { acknowledge_risk: false, confirm_text: null }
  const enablingLiveTrading = (
    (guardrailDraft.enable_order_submit && !liveGuardrails.value.enable_order_submit)
    || (guardrailDraft.auto_execute_enabled && !liveGuardrails.value.auto_execute_enabled)
  )
  if (!enablingLiveTrading) {
    await ElMessageBox.confirm(
      '确认关闭实盘交易能力？保存后当前后端进程会立即停止真实下单/自动实盘执行能力。',
      '关闭实盘交易能力',
      { confirmButtonText: '确认关闭', cancelButtonText: '取消', type: 'info' },
    )
    return { acknowledge_risk: false, confirm_text: null }
  }
  const requiredText = liveGuardrails.value.confirm_text || 'ENABLE LIVE TRADING'
  const result = await ElMessageBox.prompt(
    `即将允许系统进入真实下单能力范围。请输入 ${requiredText} 确认。`,
    '危险操作：开启实盘交易护栏',
    {
      confirmButtonText: '确认保存',
      cancelButtonText: '取消',
      type: 'warning',
      inputPlaceholder: requiredText,
      inputValidator: value => String(value || '').trim() === requiredText || `请输入 ${requiredText}`,
      distinguishCancelAndClose: true,
    },
  )
  return { acknowledge_risk: true, confirm_text: String(result.value || '').trim() }
}

async function saveLiveGuardrails() {
  if (!liveGuardrails.value || !guardrailsDirty.value || savingGuardrails.value) return
  if (guardrailDraft.auto_execute_enabled && !guardrailDraft.enable_order_submit) {
    guardrailDraft.auto_execute_enabled = false
    ElMessage.warning('自动实盘执行需要先打开真实下单总开关')
    return
  }

  const confirmation = await confirmLiveGuardrailSave().catch(() => null)
  if (!confirmation) {
    resetLiveGuardrailDraft()
    return
  }

  savingGuardrails.value = true
  try {
    liveGuardrails.value = await systemApi.setLiveTradingGuardrails({
      enable_order_submit: guardrailDraft.enable_order_submit,
      auto_execute_enabled: guardrailDraft.auto_execute_enabled,
      acknowledge_risk: confirmation.acknowledge_risk,
      confirm_text: confirmation.confirm_text,
    })
    resetLiveGuardrailDraft()
    status.value = await liveTradingApi.status()
    await loadPreflight()
    ElMessage.success('实盘交易护栏已更新')
  } catch (error) {
    ElMessage.error('实盘交易护栏保存失败，已恢复当前状态')
    await loadGuardrails().catch(() => resetLiveGuardrailDraft())
    status.value = await liveTradingApi.status()
  } finally {
    savingGuardrails.value = false
  }
}

async function onOrderSubmitGuardrailChange(value: string | number | boolean) {
  if (!Boolean(value)) {
    guardrailDraft.auto_execute_enabled = false
  }
  await saveLiveGuardrails()
}

async function onAutoExecuteGuardrailChange(value: string | number | boolean) {
  if (Boolean(value) && !guardrailDraft.enable_order_submit) {
    guardrailDraft.auto_execute_enabled = false
    ElMessage.warning('自动实盘执行需要先打开真实下单总开关')
    return
  }
  await saveLiveGuardrails()
}

async function loadAll() {
  loading.value = true
  try {
    const [nextStatus, nextProfiles, nextGuardrails] = await Promise.all([
      liveTradingApi.status(),
      liveTradingApi.profiles(true),
      systemApi.getLiveTradingGuardrails(),
    ])
    status.value = nextStatus
    profiles.value = nextProfiles
    liveGuardrails.value = nextGuardrails
    resetLiveGuardrailDraft()
    const resolvedInitialProfile = !selectedProfileKey.value
    if (!selectedProfileKey.value) {
      selectedProfileKey.value = await resolveInitialProfile(nextProfiles, nextStatus)
    }
    await Promise.all([loadAccount(), loadPreflight(), loadAudits(), loadTradeJournal(), loadPendingOrders()])
    if (!resolvedInitialProfile && shouldSwitchFromEmptyProfile()) {
      const fallbackProfile = await resolveInitialProfile(nextProfiles, nextStatus)
      if (fallbackProfile && fallbackProfile !== selectedProfileKey.value) {
        selectedProfileKey.value = fallbackProfile
        await Promise.all([loadAccount(), loadPreflight(), loadAudits(), loadTradeJournal(), loadPendingOrders()])
      }
    }
    updateOrderPolling()
  } finally {
    loading.value = false
  }
}

function shouldSwitchFromEmptyProfile() {
  if (mode.value !== 'live') return false
  const snapshot = accountSnapshot.value
  if (!snapshot) return false
  return !snapshot.meta?.initialized && Number(snapshot.position_count || 0) === 0
}

async function resolveInitialProfile(nextProfiles: LiveStrategyProfile[], nextStatus: LiveTradingStatus) {
  const seen = new Set<string>()
  const preferred = [
    nextProfiles.find(item => item.is_default)?.profile_key,
    nextStatus.default_profile,
    ...nextProfiles.map(item => item.profile_key),
  ].filter((key): key is string => {
    if (!key || seen.has(key)) return false
    seen.add(key)
    return true
  })
  if (mode.value === 'live') {
    for (const profileKey of preferred) {
      try {
        const snapshot = await liveTradingApi.account('live', profileKey, false)
        if (snapshot?.meta?.initialized && Number(snapshot.position_count || 0) > 0) {
          return profileKey
        }
      } catch {
        // Profile selection should still fall back to configured defaults if QMT is temporarily busy.
      }
    }
  }
  return preferred[0] || ''
}

watch(mode, async () => {
  if (!selectedProfileKey.value) return
  signalData.value = null
  orderRows.value = []
  stopOrderStatusPolling()
  orderSyncState.value = 'idle'
  startAccountRealtime()
  await Promise.all([loadAccount(), loadPreflight(), loadAudits(), loadTradeJournal(), loadPendingOrders()])
})

watch(selectedProfileKey, () => {
  signalData.value = null
  orderRows.value = []
  stopOrderStatusPolling()
  orderSyncState.value = 'idle'
  startAccountRealtime()
})

watch([weeklyAnalysis, accountSnapshot, strategyReviewDialogOpen], async () => {
  if (!strategyReviewDialogOpen.value) return
  await nextTick()
  renderStrategyReviewCharts()
})

async function loadAccount() {
  accountLoading.value = true
  try {
    applyAccountSnapshot(await liveTradingApi.account(mode.value, selectedProfileKey.value || undefined))
  } finally {
    accountLoading.value = false
  }
}

function applyAccountSnapshot(snapshot: LiveAccountSnapshot) {
  accountSnapshot.value = {
    ...snapshot,
    broker_account: snapshot.broker_account || accountSnapshot.value?.broker_account,
  }
  liveStreamUpdatedAt.value = snapshot.timestamp || new Date().toISOString()
  const targetCapital = Number(
    snapshot.meta?.target_capital
      || snapshot.meta?.initial_capital
      || 0,
  )
  if (targetCapital > 0) {
    capitalForm.capital = targetCapital
  }
}

function stopAccountRealtime() {
  if (accountEventSource) {
    accountEventSource.close()
    accountEventSource = null
  }
  if (accountPollTimer != null) {
    window.clearInterval(accountPollTimer)
    accountPollTimer = null
  }
  liveStreamState.value = 'off'
}

function startAccountPolling() {
  if (accountPollTimer != null) return
  liveStreamState.value = 'polling'
  accountPollTimer = window.setInterval(() => {
    if (document.hidden || mode.value !== 'live' || !selectedProfileKey.value) return
    liveTradingApi.account(mode.value, selectedProfileKey.value, false)
      .then(snapshot => {
        applyAccountSnapshot(snapshot)
        liveStreamState.value = 'polling'
      })
      .catch(error => {
        liveStreamState.value = 'error'
        liveStreamError.value = error?.message || '账户轮询失败'
      })
  }, 8000)
}

function startAccountRealtime() {
  stopAccountRealtime()
  if (mode.value !== 'live' || !selectedProfileKey.value) return
  liveStreamState.value = 'connecting'
  liveStreamError.value = ''
  try {
    const source = new EventSource(liveTradingApi.accountStreamUrl(mode.value, selectedProfileKey.value, 5))
    accountEventSource = source
    source.addEventListener('account', event => {
      try {
        applyAccountSnapshot(JSON.parse((event as MessageEvent).data) as LiveAccountSnapshot)
        liveStreamState.value = 'live'
        liveStreamError.value = ''
      } catch (error) {
        liveStreamState.value = 'error'
        liveStreamError.value = error instanceof Error ? error.message : '实时数据解析失败'
      }
    })
    source.addEventListener('stream-error', event => {
      try {
        const detail = JSON.parse((event as MessageEvent).data || '{}')
        liveStreamError.value = detail.message || '实时流异常'
      } catch {
        liveStreamError.value = '实时流异常'
      }
      liveStreamState.value = 'error'
    })
    source.onerror = () => {
      if (accountEventSource !== source) return
      source.close()
      accountEventSource = null
      liveStreamError.value = '实时流断开，已切换轮询'
      startAccountPolling()
    }
  } catch (error) {
    liveStreamState.value = 'error'
    liveStreamError.value = error instanceof Error ? error.message : '实时流启动失败'
    startAccountPolling()
  }
}

async function loadPreflight() {
  if (!selectedProfileKey.value) return null
  const ownsRuntime = !runtimeState.active
  if (ownsRuntime) {
    startRuntimeTask(
      '运行检查',
      '正在检查交易环境',
      '读取账户、交易窗口、因子覆盖和数据依赖。',
      [
        { id: 'qmt', label: 'QMT状态' },
        { id: 'account', label: '账户快照' },
        { id: 'factor', label: '因子覆盖' },
        { id: 'dependency', label: '依赖缺口' },
      ],
    )
  }
  preflightLoading.value = true
  try {
    if (ownsRuntime) {
      setRuntimeStep('qmt', 'running', '确认行情、交易模块和实盘护栏。', 12)
    }
    preflightData.value = await liveTradingApi.preflight({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
      evaluate_pipeline: true,
    })
    if (ownsRuntime) {
      const gaps = arrayField(preflightData.value.dependency_prepare, 'coverage_gaps')
      const blocks = uniqueStrings([
        ...(preflightData.value.blocking_reasons || []),
        ...(preflightData.value.runner_blocking_reasons || []),
      ])
      setRuntimeStep('qmt', 'done', 'QMT和护栏状态已读取。', 28)
      setRuntimeStep('account', 'done', '账户快照已读取。', 44)
      setRuntimeStep(
        'factor',
        preflightData.value.factor_coverage?.some(item => Number(item.value_count || 0) === 0) ? 'warn' : 'done',
        '因子覆盖已检查。',
        72,
      )
      setRuntimeStep(
        'dependency',
        gaps.length ? 'warn' : 'done',
        gaps.length ? dependencyLabels(gaps) : '依赖已覆盖。',
        92,
      )
      finishRuntimeTask(
        blocks.length ? 'warn' : 'success',
        blocks.length ? '检查完成，有阻断项' : '检查完成，可以生成',
        blocks[0] || String(preflightData.value.market_phase?.note || '预检通过。'),
      )
    }
    return preflightData.value
  } catch (error) {
    if (ownsRuntime) {
      finishRuntimeTask('error', '检查失败', error instanceof Error ? error.message : '预检请求失败')
    }
    throw error
  } finally {
    preflightLoading.value = false
  }
}

async function loadSignals() {
  if (!selectedProfileKey.value) return
  startRuntimeTask(
    '生成信号',
    '确认策略资金池',
    '先确认本次策略 portfolio 的本金和持仓快照。',
    [
      { id: 'account', label: '资金池' },
      { id: 'preflight', label: '预检' },
      { id: 'sync', label: '盘中同步' },
      { id: 'factor', label: '过滤因子' },
      { id: 'basket', label: '订单篮子' },
      { id: 'audit', label: '审计刷新' },
    ],
  )
  setRuntimeStep('account', 'running', '读取策略资金池。', 8)
  if (!strategyAccountReady.value) {
    await loadAccount()
  }
  if (!strategyAccountReady.value) {
    finishRuntimeTask('warn', '需要先圈定本金', '策略资金池未初始化，已打开本金设置。')
    ElMessage.warning('请先圈定量化交易本金，再生成信号。')
    capitalDialogOpen.value = true
    return
  }
  signalsLoading.value = true
  startRuntimeSyncPolling()
  try {
    setRuntimeStep('account', 'done', '策略资金池已确认。', 16)
    setRuntimeStep('preflight', 'running', '后端正在执行预检、同步成交状态和依赖检查。', 28)
    setRuntimeMessage('后端正在生成信号', '可能会同步当天分钟线，并按当前分钟预计算停牌/涨跌停过滤因子。', 32)
    signalData.value = await liveTradingApi.signals({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
    })
    stopRuntimeSyncPolling()
    preflightData.value = signalData.value.preflight || preflightData.value
    const blocks = uniqueStrings([
      ...(signalData.value.preflight?.blocking_reasons || []),
      ...(signalData.value.preflight?.runner_blocking_reasons || []),
    ])
    const intradayPrepare = signalData.value.preflight?.intraday_prepare || {}
    const intradayStatus = String(intradayPrepare.status || '')
    const intradayAttempted = Boolean(intradayPrepare.attempted)
    const reason = String(signalData.value.reason || '')
    setRuntimeStep('preflight', blocks.length ? 'warn' : 'done', blocks[0] || '预检已通过。', 45)
    setRuntimeStep(
      'sync',
      intradayAttempted && intradayStatus === 'failed' ? 'error' : 'done',
      intradayAttempted ? `盘中依赖准备 ${intradayStatus || '完成'}` : '无需额外盘中同步。',
      72,
    )
    setRuntimeStep(
      'factor',
      intradayAttempted && intradayStatus === 'failed' ? 'error' : 'done',
      intradayAttempted ? '交易过滤因子已按当前分钟处理。' : '过滤因子缓存已命中。',
      82,
    )
    orderRows.value = (signalData.value.orders || []).map(order => ({ ...order }))
    setRuntimeStep(
      'basket',
      reason ? 'warn' : 'done',
      reason || `${signalData.value.candidate_count || 0} 个候选，${orderRows.value.length} 条订单。`,
      92,
    )
    setRuntimeStep('audit', 'running', '刷新订单审计。', 94)
    await loadAudits()
    setRuntimeStep('audit', 'done', '审计已刷新。', 98)
    finishRuntimeTask(
      reason || blocks.length ? 'warn' : 'success',
      reason ? '生成结束，有提示' : '可执行篮子已生成',
      reason || `${orderRows.value.length} 条订单，信号 ${shortHash(signalData.value.signal_hash)}。`,
    )
  } catch (error) {
    finishRuntimeTask('error', '生成失败', error instanceof Error ? error.message : '生成信号请求失败')
    throw error
  } finally {
    stopRuntimeSyncPolling()
    signalsLoading.value = false
  }
}

async function loadAudits() {
  audits.value = await liveTradingApi.audits({
    profile_key: selectedProfileKey.value || undefined,
    mode: mode.value,
    limit: 80,
  })
}

async function loadTradeJournal() {
  journalLoading.value = true
  try {
    const params = {
      profile_key: selectedProfileKey.value || undefined,
      mode: mode.value,
    }
    const [records, weekly] = await Promise.all([
      liveTradingApi.trades({ ...params, limit: 120 }),
      liveTradingApi.weeklyAnalysis(params),
    ])
    tradeRecords.value = records
    weeklyAnalysis.value = weekly
  } finally {
    journalLoading.value = false
  }
}

async function loadPendingOrders() {
  pendingLoading.value = true
  try {
    pendingOrders.value = await liveTradingApi.pendingOrders({
      profile_key: selectedProfileKey.value || undefined,
      mode: 'live',
      limit: 120,
    })
    selectedPendingOrders.value = []
    updateOrderSyncAfterPendingLoad()
  } finally {
    pendingLoading.value = false
  }
}

async function refreshLiveOrderViews(options: { skipPendingSync?: boolean; includeStatus?: boolean } = {}) {
  const params = {
    profile_key: selectedProfileKey.value || undefined,
    mode: 'live' as const,
  }
  const pendingRequest = liveTradingApi.pendingOrders({
    ...params,
    limit: 120,
    sync: !options.skipPendingSync,
  })
  const tasks: Promise<unknown>[] = [
    pendingRequest.then(rows => {
      pendingOrders.value = rows
      selectedPendingOrders.value = []
      updateOrderSyncAfterPendingLoad()
    }),
    loadAudits(),
    loadTradeJournal(),
    loadAccount(),
  ]
  if (options.includeStatus) {
    tasks.push(liveTradingApi.status().then(next => { status.value = next }))
  }
  await Promise.all(tasks)
}

function markOrderSyncSuccess(result?: LiveOrderSyncResponse | null) {
  orderSyncUpdatedAt.value = new Date().toISOString()
  orderSyncError.value = ''
  if (result && Number(result.pending_count || 0) > 0) {
    orderSyncState.value = 'watching'
  } else {
    orderSyncState.value = 'fresh'
  }
}

function markOrderSyncError(message: string) {
  orderSyncUpdatedAt.value = new Date().toISOString()
  orderSyncError.value = message
  orderSyncState.value = 'error'
}

function updateOrderSyncAfterPendingLoad() {
  if (mode.value !== 'live') {
    orderSyncState.value = 'idle'
    return
  }
  if (orderSyncState.value === 'syncing' || orderSyncState.value === 'error') return
  if (pendingOrders.value.length) {
    orderSyncState.value = 'watching'
  } else if (orderSyncUpdatedAt.value) {
    orderSyncState.value = 'fresh'
  } else {
    orderSyncState.value = 'idle'
  }
}

async function syncLiveOrderStatus(options: { silent?: boolean; refresh?: boolean; includeStatus?: boolean } = {}) {
  if (mode.value !== 'live' || !selectedProfileKey.value || orderSyncInFlight) return null
  orderSyncInFlight = true
  orderSyncState.value = 'syncing'
  try {
    const result = await liveTradingApi.syncOrders({
      profile_key: selectedProfileKey.value || undefined,
      mode: 'live',
      limit: 200,
    })
    if (!result.synced) {
      markOrderSyncError(result.error || 'QMT 成交状态同步失败')
      if (!options.silent) {
        ElMessage.warning(orderSyncError.value)
      }
      return result
    }
    markOrderSyncSuccess(result)
    if (options.refresh !== false) {
      await refreshLiveOrderViews({
        skipPendingSync: true,
        includeStatus: options.includeStatus,
      })
    }
    if (!options.silent) {
      if (result.account_reconcile_errors?.length) {
        ElMessage.warning(`成交状态已同步，策略资金池稍后继续对齐：${result.account_reconcile_errors[0]}`)
      } else {
        ElMessage.success(`已同步 QMT 成交状态，更新 ${result.updated_count || 0} 笔。`)
      }
    }
    return result
  } catch (error) {
    const message = error instanceof Error ? error.message : 'QMT 成交状态同步失败'
    markOrderSyncError(message)
    if (!options.silent) {
      ElMessage.warning(message)
    }
    return null
  } finally {
    orderSyncInFlight = false
    updateOrderPolling()
  }
}

function startOrderStatusBurst(durationMs = 45000) {
  if (mode.value !== 'live') return
  orderSyncBurstUntil = Math.max(orderSyncBurstUntil, Date.now() + durationMs)
  orderSyncState.value = 'watching'
  updateOrderPolling()
}

function stopOrderStatusPolling() {
  if (orderSyncTimer != null) {
    window.clearInterval(orderSyncTimer)
    orderSyncTimer = null
  }
  orderSyncBurstUntil = 0
}

function updateOrderPolling() {
  if (mode.value !== 'live' || document.hidden) {
    if (orderSyncTimer != null) {
      window.clearInterval(orderSyncTimer)
      orderSyncTimer = null
    }
    return
  }
  const shouldPoll = pendingOrders.value.length > 0 || Date.now() < orderSyncBurstUntil
  if (!shouldPoll) {
    if (orderSyncTimer != null) {
      window.clearInterval(orderSyncTimer)
      orderSyncTimer = null
    }
    return
  }
  if (orderSyncTimer != null) return
  orderSyncTimer = window.setInterval(() => {
    if (document.hidden || mode.value !== 'live') {
      updateOrderPolling()
      return
    }
    const intervalMs = Date.now() < orderSyncBurstUntil ? 2500 : 8000
    void syncLiveOrderStatus({ silent: true, refresh: true, includeStatus: false })
    if (orderSyncTimer != null && intervalMs === 8000) {
      window.clearInterval(orderSyncTimer)
      orderSyncTimer = window.setInterval(() => {
        if (document.hidden || mode.value !== 'live') {
          updateOrderPolling()
          return
        }
        void syncLiveOrderStatus({ silent: true, refresh: true, includeStatus: false })
      }, intervalMs)
    }
  }, 2500)
}

async function syncPendingOrderStatus() {
  pendingLoading.value = true
  try {
    await syncLiveOrderStatus({ silent: false, refresh: true, includeStatus: true })
  } finally {
    pendingLoading.value = false
  }
}

function selectedOrStalePendingRows() {
  if (selectedPendingOrders.value.length) return selectedPendingOrders.value
  const thresholdMs = Math.max(0, staleOrderMinutes.value || 0) * 60 * 1000
  if (!thresholdMs) return pendingOrders.value
  const now = Date.now()
  return pendingOrders.value.filter(row => {
    const created = row.created_at ? new Date(row.created_at).getTime() : 0
    return created > 0 && now - created >= thresholdMs
  })
}

async function cancelPendingOrders() {
  const rows = selectedOrStalePendingRows()
  const useSelectedRows = selectedPendingOrders.value.length > 0
  if (!rows.length) {
    ElMessage.info(`没有超过 ${staleOrderMinutes.value} 分钟的待成交委托；也可以勾选具体订单后再撤。`)
    return
  }
  await ElMessageBox.confirm(
    `确认向 QMT 真实撤销 ${rows.length} 笔委托？这不是平台内纸面取消。`,
    '批量真实撤单',
    {
      type: 'warning',
      confirmButtonText: '真实撤单',
      cancelButtonText: '取消',
    },
  )
  pendingActionLoading.value = true
  try {
    const result = await liveTradingApi.cancelOrders({
      profile_key: selectedProfileKey.value || undefined,
      mode: 'live',
      limit: 200,
      min_age_seconds: useSelectedRows ? 0 : Math.max(0, staleOrderMinutes.value || 0) * 60,
      record_ids: rows.map(row => row.record_id),
      order_ids: rows.map(row => String(row.order_id || '')).filter(Boolean),
      confirm: true,
    })
    ElMessage.success(`已向 QMT 发送 ${result.cancel_count || 0} 笔撤单请求。`)
    await refreshLiveOrderViews({ includeStatus: true })
    startOrderStatusBurst(30000)
  } finally {
    pendingActionLoading.value = false
  }
}

async function closeLocalPendingOrders(inputRows?: LiveTradeRecord[]) {
  const rows = inputRows?.length ? inputRows : selectedOrStalePendingRows()
  const useSelectedRows = Boolean(inputRows?.length) || selectedPendingOrders.value.length > 0
  if (!rows.length) {
    ElMessage.info(`没有超过 ${staleOrderMinutes.value} 分钟的待确认委托；也可以勾选具体订单后再本地关闭。`)
    return
  }
  await ElMessageBox.confirm(
    `确认只在平台本地关闭 ${rows.length} 笔待确认委托？这个操作不会向 QMT 发送撤单，只用于你已在客户端撤单或确认不会成交的记录。`,
    '本地关闭待确认委托',
    {
      type: 'warning',
      confirmButtonText: '本地关闭',
      cancelButtonText: '取消',
    },
  )
  pendingActionLoading.value = true
  try {
    const result = await liveTradingApi.closeLocalOrders({
      profile_key: selectedProfileKey.value || undefined,
      mode: 'live',
      limit: 200,
      record_ids: rows.map(row => row.record_id),
      order_ids: rows.map(row => String(row.order_id || '')).filter(Boolean),
      reason: useSelectedRows ? 'user_confirmed_client_cancelled' : `stale_${staleOrderMinutes.value}m_user_confirmed`,
      confirm: true,
    })
    ElMessage.success(`已本地关闭 ${result.closed_count || 0} 笔待确认委托。`)
    selectedPendingOrders.value = []
    await refreshLiveOrderViews({ includeStatus: true })
  } finally {
    pendingActionLoading.value = false
  }
}

async function cancelAndResubmitPendingOrders() {
  const rows = selectedOrStalePendingRows()
  const useSelectedRows = selectedPendingOrders.value.length > 0
  if (!rows.length) {
    ElMessage.info(`没有超过 ${staleOrderMinutes.value} 分钟的待成交委托；也可以勾选具体订单后再重提。`)
    return
  }
  await ElMessageBox.confirm(
    `确认先向 QMT 真实撤销 ${rows.length} 笔委托；撤单确认后按最新行情重新生成差额订单并提交？如 QMT 尚未确认撤单，系统不会重复提交。`,
    '撤单后重新提交',
    {
      type: 'warning',
      confirmButtonText: '撤单再提交',
      cancelButtonText: '取消',
    },
  )
  pendingActionLoading.value = true
  try {
    const result = await liveTradingApi.cancelAndResubmit({
      profile_key: selectedProfileKey.value || undefined,
      mode: 'live',
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
      limit: 200,
      min_age_seconds: useSelectedRows ? 0 : Math.max(0, staleOrderMinutes.value || 0) * 60,
      record_ids: rows.map(row => row.record_id),
      order_ids: rows.map(row => String(row.order_id || '')).filter(Boolean),
      confirm_cancel: true,
      confirm_submit: true,
    })
    if (result.signal_result) {
      signalData.value = result.signal_result
      preflightData.value = result.signal_result.preflight || preflightData.value
      orderRows.value = (result.signal_result.orders || []).map(order => ({ ...order }))
    }
    if (result.submit_result) {
      submitResult.value = result.submit_result
      submitResultDialogOpen.value = true
    }
    if (result.submitted) {
      ElMessage.success('撤单确认后已重新提交差额订单。')
    } else {
      ElMessage.warning(result.message || '撤单后尚未提交新订单。')
    }
    await refreshLiveOrderViews({ includeStatus: true })
    startOrderStatusBurst(result.submitted ? 45000 : 30000)
  } finally {
    pendingActionLoading.value = false
  }
}

async function startRunner() {
  if (!selectedProfileKey.value) return
  if (!strategyAccountReady.value) {
    await loadAccount()
  }
  if (!strategyAccountReady.value) {
    ElMessage.warning('请先圈定量化交易本金，再启动自动交易。')
    capitalDialogOpen.value = true
    return
  }
  const check = await loadPreflight()
  if (check?.runner_blocking_reasons?.length) {
    await ElMessageBox.alert(uniqueStrings(check.runner_blocking_reasons).join('\n'), '启动前检查未通过', {
      confirmButtonText: '知道了',
    })
    return
  }
  if (mode.value === 'live') {
    await ElMessageBox.confirm('确认启动实盘自动交易？', '实盘自动交易确认', {
      type: 'warning',
      confirmButtonText: '确认启动',
      cancelButtonText: '取消',
    })
  }
  startRuntimeTask(
    '自动运行',
    '正在启动 runner',
    '提交启动请求并刷新 runner 状态。',
    [
      { id: 'start', label: '启动请求' },
      { id: 'status', label: '状态刷新' },
    ],
  )
  runnerLoading.value = true
  try {
    setRuntimeStep('start', 'running', '后端正在启动自动交易循环。', 35)
    await liveTradingApi.startRunner({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
      interval_seconds: 60,
    })
    setRuntimeStep('start', 'done', 'runner 已启动。', 72)
    ElMessage.success('自动交易已启动')
    setRuntimeStep('status', 'running', '刷新 runner 状态。', 86)
    status.value = await liveTradingApi.status()
    setRuntimeStep('status', 'done', '状态已刷新。', 96)
    finishRuntimeTask('success', 'runner 已启动', status.value.runner.run_id || '自动运行循环已进入后台。')
  } catch (error) {
    finishRuntimeTask('error', 'runner 启动失败', error instanceof Error ? error.message : '启动请求失败')
    throw error
  } finally {
    runnerLoading.value = false
  }
}

async function stopRunner() {
  startRuntimeTask(
    '自动运行',
    '正在停止 runner',
    '发送停止请求并刷新状态。',
    [
      { id: 'stop', label: '停止请求' },
      { id: 'status', label: '状态刷新' },
    ],
  )
  runnerLoading.value = true
  try {
    setRuntimeStep('stop', 'running', '后端正在停止自动交易循环。', 40)
    await liveTradingApi.stopRunner()
    setRuntimeStep('stop', 'done', 'runner 已停止。', 72)
    ElMessage.success('自动交易已停止')
    setRuntimeStep('status', 'running', '刷新 runner 状态。', 86)
    status.value = await liveTradingApi.status()
    setRuntimeStep('status', 'done', '状态已刷新。', 96)
    finishRuntimeTask('success', 'runner 已停止', '自动运行循环已退出。')
  } catch (error) {
    finishRuntimeTask('error', 'runner 停止失败', error instanceof Error ? error.message : '停止请求失败')
    throw error
  } finally {
    runnerLoading.value = false
  }
}

async function takeoverRunner() {
  await ElMessageBox.confirm('确认人工接管并停止自动提交？', '人工接管', {
    type: 'warning',
    confirmButtonText: '接管',
    cancelButtonText: '取消',
  })
  startRuntimeTask(
    '人工接管',
    '正在切换接管状态',
    '停止自动提交，并刷新 runner 状态。',
    [
      { id: 'takeover', label: '接管请求' },
      { id: 'status', label: '状态刷新' },
    ],
  )
  runnerLoading.value = true
  try {
    setRuntimeStep('takeover', 'running', '后端正在切换人工接管。', 40)
    await liveTradingApi.takeover('human takeover from UI')
    setRuntimeStep('takeover', 'done', '人工接管已生效。', 72)
    ElMessage.warning('已切换为人工接管')
    setRuntimeStep('status', 'running', '刷新 runner 状态。', 86)
    status.value = await liveTradingApi.status()
    setRuntimeStep('status', 'done', '状态已刷新。', 96)
    finishRuntimeTask('warn', '已人工接管', '自动提交已停止，后续由人工处理。')
  } catch (error) {
    finishRuntimeTask('error', '人工接管失败', error instanceof Error ? error.message : '接管请求失败')
    throw error
  } finally {
    runnerLoading.value = false
  }
}

async function submitOrders() {
  const orders = orderRows.value.filter(order => Number(order.quantity || 0) > 0)
  if (!orders.length) {
    ElMessage.info('没有可提交的订单')
    return
  }
  const title = mode.value === 'live' ? '真实委托确认' : '模拟成交确认'
  await ElMessageBox.confirm(`确认提交 ${orders.length} 笔订单？`, title, {
    type: mode.value === 'live' ? 'warning' : 'info',
    confirmButtonText: '确认提交',
    cancelButtonText: '取消',
  })
  startRuntimeTask(
    '提交篮子',
    mode.value === 'live' ? '正在提交真实委托' : '正在提交模拟订单',
    `${orders.length} 条订单正在处理。`,
    [
      { id: 'submit', label: '提交请求' },
      { id: 'refresh', label: '刷新账户' },
      { id: 'audit', label: '刷新记录' },
    ],
  )
  try {
    setRuntimeStep('submit', 'running', `${orders.length} 条订单正在提交。`, 32)
    const result = await liveTradingApi.submitOrders({
      mode: mode.value,
      orders: orders as unknown as Record<string, unknown>[],
      confirm: true,
    })
    submitResult.value = result
    submitResultDialogOpen.value = true
    setRuntimeStep(
      'submit',
      submitResultFailedCount.value ? 'warn' : 'done',
      submitResultFailedCount.value ? `${submitResultFailedCount.value} 笔失败。` : '提交请求已完成。',
      62,
    )
    if (submitResultPendingCount.value) {
      ElMessage.warning(`${submitResultPendingCount.value} 笔真实委托已提交，等待成交确认。`)
    } else if (submitResultFailedCount.value) {
      ElMessage.warning(`${submitResultFailedCount.value} 笔订单提交失败，请查看提交结果。`)
    } else {
      ElMessage.success('订单提交完成')
    }
    setRuntimeStep('refresh', 'running', '刷新账户、待成交和状态。', 76)
    await refreshLiveOrderViews({ includeStatus: true })
    if (mode.value === 'live' && submitResultPendingCount.value) {
      setRuntimeStep('refresh', 'running', '正在向 QMT 拉取首轮成交回报。', 82)
      await syncLiveOrderStatus({ silent: true, refresh: true, includeStatus: true })
      startOrderStatusBurst(45000)
    }
    setRuntimeStep('refresh', 'done', '账户、待成交和状态已刷新。', 88)
    setRuntimeStep('audit', 'done', '审计和复盘已刷新。', 98)
    finishRuntimeTask(
      submitResultFailedCount.value ? 'warn' : 'success',
      submitResultFailedCount.value ? '提交完成，有失败项' : '订单提交完成',
      pendingOrders.value.length ? `${pendingOrders.value.length} 笔仍等待成交确认，系统会继续自动同步。` : '账户、审计和成交状态已刷新。',
    )
  } catch (error) {
    finishRuntimeTask('error', '订单提交失败', error instanceof Error ? error.message : '提交请求失败')
    throw error
  }
}

async function initializeCapitalPool() {
  if (!selectedProfileKey.value) return
  const adjustingCapital = strategyAccountReady.value && !capitalForm.reset_existing
  if (strategyAccountReady.value && capitalForm.reset_existing) {
    await ElMessageBox.confirm('重新初始化会清空该 Profile 当前策略资金池和策略持仓账本。确认继续？', '重新初始化确认', {
      type: 'warning',
      confirmButtonText: '确认重置',
      cancelButtonText: '取消',
    })
  }
  capitalSaving.value = true
  try {
    accountSnapshot.value = await liveTradingApi.initializeAccount({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      capital: Number(capitalForm.capital || 0),
      reset_existing: capitalForm.reset_existing,
    })
    capitalDialogOpen.value = false
    capitalForm.reset_existing = false
    ElMessage.success(adjustingCapital ? '量化资金池目标本金已调整' : '量化资金池已初始化')
    await Promise.all([loadPreflight(), loadAudits(), loadTradeJournal(), loadPendingOrders()])
  } finally {
    capitalSaving.value = false
  }
}

function removeOrder(index: number) {
  orderRows.value.splice(index, 1)
}

async function toggleProfileEnabled(value: string | number | boolean) {
  if (!selectedProfile.value) return
  const updated = await liveTradingApi.updateProfile(selectedProfile.value.profile_key, { enabled: Boolean(value) })
  profiles.value = profiles.value.map(item => item.profile_key === updated.profile_key ? updated : item)
}

async function makeDefaultProfile() {
  if (!selectedProfile.value) return
  const updated = await liveTradingApi.updateProfile(selectedProfile.value.profile_key, { is_default: true })
  profiles.value = profiles.value.map(item => ({ ...item, is_default: item.profile_key === updated.profile_key }))
  ElMessage.success('默认 Profile 已更新')
}

async function createProfile() {
  profileSaving.value = true
  try {
    const created = await liveTradingApi.createProfile({
      strategy_id: newProfile.strategy_id,
      profile_key: newProfile.profile_key,
      display_name: newProfile.display_name,
      enabled: newProfile.enabled,
      is_default: newProfile.is_default,
      adapter_type: 'multi_factor_cash_aware',
      universe_config: { type: 'strategy' },
      execution_policy: {
        allow_auto_trade: true,
        allow_manual_submit: true,
        allow_live_submit: true,
      },
    })
    profiles.value = [created, ...profiles.value.filter(item => item.profile_key !== created.profile_key)]
    selectedProfileKey.value = created.profile_key
    profileDialogOpen.value = false
    ElMessage.success('Profile 已创建')
    await loadSignals()
  } finally {
    profileSaving.value = false
  }
}

function shortHash(value?: string | null) {
  return value ? `${value.slice(0, 8)}…` : '-'
}

function formatMoney(value?: number | null) {
  if (value == null || Number.isNaN(Number(value))) return '-'
  return Number(value).toLocaleString('zh-CN', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })
}

function formatQuantity(value?: number | null) {
  if (value == null || Number.isNaN(Number(value))) return '-'
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}

function formatPrice(value?: number | null) {
  if (value == null || Number.isNaN(Number(value))) return '-'
  return Number(value).toLocaleString('zh-CN', {
    minimumFractionDigits: 3,
    maximumFractionDigits: 3,
  })
}

function formatPercent(value?: number | null) {
  if (value == null || Number.isNaN(Number(value))) return '-'
  return `${(Number(value) * 100).toFixed(2)}%`
}

function formatDateTime(value?: string | null) {
  if (!value) return '-'
  return value.replace('T', ' ').replace(/\.\d+$/, '').slice(0, 19)
}

function formatClock(value?: string | null) {
  if (!value) return '--:--:--'
  return formatDateTime(value).slice(11, 19) || '--:--:--'
}

function pnlTone(value?: number | null) {
  const numberValue = Number(value || 0)
  if (numberValue > 0) return 'metric-good'
  if (numberValue < 0) return 'metric-bad'
  return 'metric-neutral'
}

function candidateSymbol(candidate: Record<string, unknown>) {
  return String(candidate.symbol || '-')
}

async function onProfileChange() {
  signalData.value = null
  orderRows.value = []
  stopOrderStatusPolling()
  orderSyncState.value = 'idle'
  startAccountRealtime()
  await Promise.all([loadAccount(), loadPreflight(), loadAudits(), loadTradeJournal(), loadPendingOrders()])
  updateOrderPolling()
}

// formatHeatFilterNote removed

function phaseLabel(value: string) {
  const labels: Record<string, string> = {
    active: '可执行',
    before_market: '未开市',
    before_rebalance: '等待调仓',
    lunch_break: '午休',
    after_close: '已收盘',
    outside_window: '窗口外',
    non_trading_day: '非交易日',
    historical: '历史检查',
    future: '未来日期',
  }
  return labels[value] || value || '-'
}

function arrayField(source: Record<string, unknown> | null | undefined, key: string) {
  const value = source?.[key]
  return Array.isArray(value) ? value as Array<Record<string, unknown>> : []
}

function uniqueStrings(items: string[]) {
  return Array.from(new Set(items.filter(Boolean)))
}

function dependencyLabels(items: Array<Record<string, unknown>>) {
  return items
    .slice(0, 3)
    .map(item => String(item.label || item.dependency || ''))
    .filter(Boolean)
    .join(' / ')
}

function scrollToOrders() {
  orderPanelRef.value?.scrollIntoView({ behavior: 'smooth', block: 'start' })
}

function scrollToPendingOrders() {
  submitResultDialogOpen.value = false
  requestAnimationFrame(() => {
    pendingPanelRef.value?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  })
}

function toNumber(value: unknown) {
  const numberValue = Number(value ?? 0)
  return Number.isFinite(numberValue) ? numberValue : 0
}

function isPendingStatus(status?: string | null) {
  return ['live_pending', 'submitted', 'accepted', 'partially_filled', 'cancel_requested'].includes(String(status || ''))
}

function isFilledStatus(status?: string | null) {
  return ['paper_filled', 'live_filled', 'filled', 'partially_cancelled'].includes(String(status || ''))
}

function statusLabel(status?: string | null) {
  const labels: Record<string, string> = {
    live_pending: '待成交',
    submitted: '待成交',
    accepted: '已接收',
    partially_filled: '部分成交',
    cancel_requested: '撤单中',
    paper_filled: '模拟成交',
    live_filled: '实盘成交',
    partially_cancelled: '部成已撤',
    cancelled: '已撤单',
    filled: '已成交',
    failed: '失败',
    skipped: '跳过',
    generated: '已生成',
    strategy_account_initialized: '资金池初始化',
    strategy_account_adjusted: '资金池调整',
    blocked: '被护栏拦截',
    duplicate: '重复跳过',
  }
  return labels[String(status || '')] || String(status || '-')
}

function auditEventLabel(eventType?: string | null, status?: string | null) {
  const labels: Record<string, string> = {
    signal_generated: '信号生成',
    order_skipped: '跳过订单',
    capital_pool: '资金池',
    cancel: '撤单',
    execution_update: '执行更新',
    guardrail: '护栏',
    control: '控制',
  }
  return labels[String(eventType || '')] || statusLabel(status)
}

function statusTagType(status?: string | null) {
  if (isPendingStatus(status)) return 'warning'
  if (isFilledStatus(status)) return 'success'
  if (['failed', 'cancelled'].includes(String(status || ''))) return 'danger'
  return 'info'
}

function payloadNumber(row: LiveTradeRecord, key: string) {
  return toNumber(row.result_payload?.[key])
}

function pendingRemainingQuantity(row: LiveTradeRecord) {
  const total = toNumber(row.quantity)
  const filled = payloadNumber(row, 'filled_quantity')
  const remaining = payloadNumber(row, 'remaining_quantity')
  if (remaining > 0) return remaining
  if (filled > 0) return Math.max(0, total - filled)
  return total
}

function onPendingSelectionChange(rows: LiveTradeRecord[]) {
  selectedPendingOrders.value = rows
}

function sideLabel(side?: string | null) {
  const value = String(side || '').toUpperCase()
  if (value === 'BUY') return '买入'
  if (value === 'SELL') return '卖出'
  return value || '-'
}

async function openPositionsDialog() {
  positionsDialogOpen.value = true
  if (!accountSnapshot.value && !accountLoading.value) {
    await loadAccount().catch(error => ElMessage.error(error?.message || '持仓读取失败'))
  }
}

async function openStrategyReviewDialog() {
  strategyReviewDialogOpen.value = true
  if (!weeklyAnalysis.value && !journalLoading.value) {
    await loadTradeJournal().catch(error => ElMessage.error(error?.message || '策略复盘读取失败'))
  }
  if (!accountSnapshot.value && !accountLoading.value) {
    await loadAccount().catch(error => ElMessage.error(error?.message || '账户读取失败'))
  }
  await nextTick()
  renderStrategyReviewCharts()
}

function disposeStrategyReviewCharts() {
  equityChart?.dispose()
  flowChart?.dispose()
  statusChart?.dispose()
  equityChart = null
  flowChart = null
  statusChart = null
}

function renderStrategyReviewCharts() {
  if (!strategyReviewDialogOpen.value) return
  disposeStrategyReviewCharts()
  renderEquityChart()
  renderFlowChart()
  renderStatusChart()
}

function renderEquityChart() {
  if (!equityChartRef.value) return
  equityChart = echarts.init(equityChartRef.value)
  const rows = equityCurve.value
  const x = rows.map(row => formatDateTime(row.created_at || row.trade_date || '').slice(5, 16))
  const assets = rows.map(row => Number(row.total_asset || 0))
  const drawdown = rows.map(row => row.drawdown_pct == null ? null : Number(row.drawdown_pct) * 100)
  equityChart.setOption({
    tooltip: {
      trigger: 'axis',
      formatter: (points: any[]) => {
        const asset = points.find(point => point.seriesName === '总资产')
        const dd = points.find(point => point.seriesName === '回撤')
        return `${points[0]?.axisValue || '-'}<br/>总资产 ${formatMoney(asset?.data)}<br/>回撤 ${dd?.data == null ? '-' : `${Number(dd.data).toFixed(2)}%`}`
      },
    },
    legend: { data: ['总资产', '回撤'], textStyle: { color: '#a9b7c9', fontSize: 11 }, top: 0 },
    grid: { left: 58, right: 54, top: 34, bottom: 36 },
    xAxis: {
      type: 'category',
      data: x,
      axisLabel: { color: '#a9b7c9', fontSize: 10, rotate: 35 },
      axisLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.3)' } },
    },
    yAxis: [
      {
        type: 'value',
        axisLabel: { color: '#a9b7c9', fontSize: 10 },
        splitLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.12)' } },
      },
      {
        type: 'value',
        axisLabel: { color: '#a9b7c9', fontSize: 10, formatter: '{value}%' },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: '总资产',
        type: 'line',
        data: assets,
        showSymbol: false,
        smooth: true,
        lineStyle: { color: '#38bdf8', width: 2 },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(56, 189, 248, 0.24)' },
            { offset: 1, color: 'rgba(56, 189, 248, 0.02)' },
          ]),
        },
      },
      {
        name: '回撤',
        type: 'line',
        yAxisIndex: 1,
        data: drawdown,
        showSymbol: false,
        lineStyle: { color: '#f87171', width: 1.4 },
        areaStyle: { color: 'rgba(248, 113, 113, 0.08)' },
      },
    ],
  })
}

function renderFlowChart() {
  if (!flowChartRef.value) return
  flowChart = echarts.init(flowChartRef.value)
  const rows = flowChartRows.value
  flowChart.setOption({
    tooltip: {
      trigger: 'axis',
      formatter: (points: any[]) => {
        const buy = points.find(point => point.seriesName === '买入')
        const sell = points.find(point => point.seriesName === '卖出')
        return `${points[0]?.axisValue || '-'}<br/>买入 ${formatMoney(buy?.data)}<br/>卖出 ${formatMoney(Math.abs(Number(sell?.data || 0)))}`
      },
    },
    legend: { data: ['买入', '卖出'], textStyle: { color: '#a9b7c9', fontSize: 11 }, top: 0 },
    grid: { left: 58, right: 18, top: 34, bottom: 36 },
    xAxis: {
      type: 'category',
      data: rows.map(row => row.trade_date),
      axisLabel: { color: '#a9b7c9', fontSize: 10, rotate: 35 },
      axisLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.3)' } },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#a9b7c9', fontSize: 10 },
      splitLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.12)' } },
    },
    series: [
      { name: '买入', type: 'bar', data: rows.map(row => Number(row.buy_notional || 0)), itemStyle: { color: '#22c55e' } },
      { name: '卖出', type: 'bar', data: rows.map(row => -Number(row.sell_notional || 0)), itemStyle: { color: '#f87171' } },
    ],
  })
}

function renderStatusChart() {
  if (!statusChartRef.value) return
  statusChart = echarts.init(statusChartRef.value)
  const rows = statusChartRows.value
  statusChart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 44, right: 16, top: 24, bottom: 42 },
    xAxis: {
      type: 'category',
      data: rows.map(row => statusLabel(row.status)),
      axisLabel: { color: '#a9b7c9', fontSize: 10, rotate: 30 },
      axisLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.3)' } },
    },
    yAxis: {
      type: 'value',
      minInterval: 1,
      axisLabel: { color: '#a9b7c9', fontSize: 10 },
      splitLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.12)' } },
    },
    series: [{
      name: '订单数',
      type: 'bar',
      data: rows.map(row => Number(row.records || 0)),
      itemStyle: { color: '#7dd3fc' },
    }],
  })
}

function movePositionColumnTo(source: PositionColumnKey, target: PositionColumnKey) {
  if (source === target) return
  const next = [...positionColumnOrder.value]
  const sourceIndex = next.indexOf(source)
  const targetIndex = next.indexOf(target)
  if (sourceIndex < 0 || targetIndex < 0) return
  const [item] = next.splice(sourceIndex, 1)
  next.splice(targetIndex, 0, item)
  positionColumnOrder.value = next
}

function columnKeyFromPointer(event: PointerEvent) {
  const target = document.elementFromPoint(event.clientX, event.clientY)
  const header = target?.closest<HTMLElement>('[data-position-column-key]')
  return header?.dataset.positionColumnKey as PositionColumnKey | undefined
}

function onPositionColumnPointerDown(event: PointerEvent, key: PositionColumnKey) {
  if (event.button !== 0) return
  const target = event.currentTarget as HTMLElement
  draggedPositionColumn.value = key
  dragOverPositionColumn.value = key
  target.setPointerCapture?.(event.pointerId)

  const onPointerMove = (moveEvent: PointerEvent) => {
    const targetKey = columnKeyFromPointer(moveEvent)
    if (targetKey && targetKey !== draggedPositionColumn.value) {
      dragOverPositionColumn.value = targetKey
    }
  }

  const onPointerUp = (upEvent: PointerEvent) => {
    const targetKey = columnKeyFromPointer(upEvent) || dragOverPositionColumn.value
    if (draggedPositionColumn.value && targetKey) {
      movePositionColumnTo(draggedPositionColumn.value, targetKey)
    }
    target.releasePointerCapture?.(event.pointerId)
    window.removeEventListener('pointermove', onPointerMove)
    window.removeEventListener('pointerup', onPointerUp)
    onPositionColumnDragEnd()
  }

  window.addEventListener('pointermove', onPointerMove)
  window.addEventListener('pointerup', onPointerUp, { once: true })
}

function onPositionColumnDragEnd() {
  draggedPositionColumn.value = null
  dragOverPositionColumn.value = null
}

function positionColumnHeaderClass(key: PositionColumnKey) {
  return [
    'position-column-header',
    draggedPositionColumn.value === key ? 'position-column-header--dragging' : '',
    dragOverPositionColumn.value === key && draggedPositionColumn.value !== key ? 'position-column-header--over' : '',
  ]
}

function positionCellValue(row: LiveAccountPosition, key: PositionColumnKey) {
  if (key === 'symbol') return row.symbol || '-'
  if (key === 'stock_name') return row.stock_name || '-'
  if (key === 'volume_ratio') return row.volume_ratio == null ? '-' : Number(row.volume_ratio).toFixed(2)
  if (key === 'today_change_pct' || key === 'turnover_rate' || key === 'position_pct' || key === 'unrealized_pnl_pct') {
    return formatPercent(row[key])
  }
  if (key === 'market_value' || key === 'amount') return formatMoney(row[key])
  return '-'
}

function positionCellClass(row: LiveAccountPosition, key: PositionColumnKey) {
  if (key === 'today_change_pct') return pnlTone(row.today_change_pct)
  if (key === 'unrealized_pnl_pct') return pnlTone(row.unrealized_pnl_pct)
  return ''
}

function positionLastPrice(row: LiveAccountPosition) {
  return row.last_price ?? row.latest_price ?? null
}

function handlePageContextAction(event: Event) {
  const action = String((event as CustomEvent<{ action?: string }>).detail?.action || '')
  if (action === 'open-strategy-review') {
    openStrategyReviewDialog()
    return
  }
  if (action === 'open-capital-positions') {
    openPositionsDialog()
  }
}

function handleVisibilityChange() {
  if (document.hidden) {
    updateOrderPolling()
    return
  }
  updateOrderPolling()
  if (mode.value === 'live' && pendingOrders.value.length) {
    void syncLiveOrderStatus({ silent: true, refresh: true, includeStatus: true })
  }
}

const pageContextBlocks = computed(() => [
  {
    title: 'Live Trading',
    rows: [
      { label: '模式', value: mode.value === 'paper' ? '模拟' : '实盘', tone: mode.value === 'paper' ? 'good' : 'warn' },
      { label: 'Profile', value: selectedProfileKey.value || '-' },
      { label: 'Runner', value: runnerText.value, tone: status.value?.runner.takeover ? 'warn' : 'neutral' },
      { label: '预检', value: preflightStatusText.value, tone: preflightTagType.value === 'success' ? 'good' : 'warn' },
      { label: '真实下单', value: status.value?.order_submit_enabled ? '开启' : '关闭', tone: status.value?.order_submit_enabled ? 'bad' : 'good' },
    ],
  },
  {
    title: 'Basket',
    rows: [
      { label: '订单', value: `${orderRows.value.length} 笔` },
      { label: '跳过', value: `${signalData.value?.skipped_orders?.length || 0} 笔` },
      { label: '待成交', value: `${pendingOrders.value.length} 笔` },
      { label: '账户', value: signalData.value?.account.source || '-' },
      { label: '信号', value: shortHash(signalData.value?.signal_hash) },
    ],
  },
  {
    title: currentStrategyName.value,
    action: 'open-strategy-review',
    rows: [
      { label: '状态', value: strategyAccountStatusText.value, tone: strategyAccountReady.value ? 'good' : 'warn' },
      { label: '来源', value: accountSourceText.value, tone: accountSnapshot.value?.error ? 'warn' : 'good' },
      { label: '目标本金', value: accountSummaryItems.value.find(item => item.label === '目标本金')?.value || '-' },
      { label: '可用现金', value: formatMoney(accountSnapshot.value?.cash) },
      { label: '总资产', value: formatMoney(accountSnapshot.value?.total_asset) },
      { label: '持仓市值', value: formatMoney(accountSnapshot.value?.market_value) },
      {
        label: '至今PnL',
        value: formatMoney(weeklyAnalysis.value?.summary?.all_time_pnl ?? accountSnapshot.value?.total_pnl),
        tone: Number(weeklyAnalysis.value?.summary?.all_time_pnl ?? accountSnapshot.value?.total_pnl ?? 0) > 0
          ? 'good'
          : Number(weeklyAnalysis.value?.summary?.all_time_pnl ?? accountSnapshot.value?.total_pnl ?? 0) < 0 ? 'bad' : 'neutral',
      },
      { label: '至今回撤', value: formatPercent(weeklyAnalysis.value?.summary?.all_time_max_drawdown_pct), tone: Number(weeklyAnalysis.value?.summary?.all_time_max_drawdown_pct || 0) < 0 ? 'bad' : 'neutral' },
      { label: '持仓', value: `${accountSnapshot.value?.position_count || 0} 只` },
    ],
  },
])

usePageContext(pageContextBlocks)

onMounted(() => {
  window.addEventListener('page-context-action', handlePageContextAction)
  document.addEventListener('visibilitychange', handleVisibilityChange)
  loadAll()
    .then(() => {
      startAccountRealtime()
      updateOrderPolling()
    })
    .catch(error => ElMessage.error(error?.message || '实盘交易模块加载失败'))
})

onUnmounted(() => {
  window.removeEventListener('page-context-action', handlePageContextAction)
  document.removeEventListener('visibilitychange', handleVisibilityChange)
  stopRuntimeSyncPolling()
  stopOrderStatusPolling()
  stopAccountRealtime()
  disposeStrategyReviewCharts()
})
</script>

<style scoped>
.live-page {
  --trade-card-bg: rgba(253, 251, 247, 0.88);
  --trade-card-bg-soft: rgba(255, 255, 255, 0.72);
  --trade-card-border: rgba(27, 61, 50, 0.16);
  --trade-card-border-strong: rgba(27, 61, 50, 0.26);
  --trade-good: #15803d;
  --trade-warn: #b45309;
  --trade-bad: #b91c1c;
  --trade-info: #0369a1;
  overflow: auto;
}

.live-page > * {
  flex-shrink: 0;
}

.page-head,
.desk-grid,
.lower-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: var(--space-4);
  align-items: start;
}

.page-head {
  align-items: center;
  padding: var(--space-5);
}

.page-head h2,
.panel-card__head h3 {
  margin: var(--space-1) 0 var(--space-2);
  color: var(--text-bright);
}

.page-head p,
.profile-meta p,
.portfolio-subtitle {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
  line-height: 1.6;
}

.actions,
.table-actions,
.runner-actions,
.profile-actions {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-2);
  align-items: center;
  justify-content: flex-end;
}

.stale-control {
  display: inline-flex;
  align-items: center;
  gap: var(--space-1);
  color: var(--text-secondary);
  font-size: var(--text-xs);
  white-space: nowrap;
}

.stale-control :deep(.el-input-number) {
  width: 86px;
}

.action-button {
  min-width: 86px;
  border-width: 1px !important;
  color: #06111d !important;
  font-weight: 800 !important;
  letter-spacing: 0;
  box-shadow: 0 10px 24px rgba(15, 23, 42, 0.28), inset 0 1px 0 rgba(255, 255, 255, 0.22) !important;
}

.action-button--preflight,
.action-button--signal,
.action-button--auto {
  background: linear-gradient(135deg, #7dd3fc, #22d3ee) !important;
  border-color: #bae6fd !important;
}

.action-button--submit {
  background: linear-gradient(135deg, #86efac, #22c55e) !important;
  border-color: #bbf7d0 !important;
  color: #031609 !important;
}

.action-button--takeover {
  background: linear-gradient(135deg, #fde68a, #f59e0b) !important;
  border-color: #fef3c7 !important;
  color: #1b1002 !important;
}

.action-button:hover {
  filter: brightness(1.08);
  transform: translateY(-1px);
}

.action-button.is-disabled,
.action-button.is-disabled:hover {
  background: #111821 !important;
  border-color: rgba(172, 190, 214, 0.14) !important;
  color: var(--text-muted) !important;
  filter: none;
  font-weight: 700 !important;
  box-shadow: none !important;
  transform: none;
}

.status-band {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: var(--space-3);
  padding: var(--space-3);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: var(--bg-surface);
  box-shadow: var(--shadow-card);
}

.summary-band {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: var(--space-3);
  padding: var(--space-3);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: var(--bg-surface);
  box-shadow: var(--shadow-card);
}

.status-band div,
.summary-band div,
.runner-state div {
  min-width: 0;
}

.status-band label,
.summary-band label,
.runner-state label,
.param-row label {
  display: block;
  margin-bottom: 3px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.status-band strong,
.summary-band strong,
.runner-state strong {
  color: var(--text-primary);
  font-family: var(--font-data);
}

.state-text {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-width: 0;
  color: var(--text-secondary);
  font-family: var(--font-ui);
  font-size: var(--text-sm);
  font-weight: 800;
  letter-spacing: 0;
  line-height: 1.2;
}

.state-text::before {
  content: '';
  flex: 0 0 auto;
  width: 7px;
  height: 7px;
  border-radius: var(--radius-full);
  background: currentColor;
  box-shadow: 0 0 10px currentColor;
}

.state-text--good {
  color: var(--trade-good);
}

.state-text--warn {
  color: var(--trade-warn);
}

.state-text--bad {
  color: var(--trade-bad);
}

.state-text--neutral {
  color: var(--text-secondary);
}

.live-stream-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 24px;
  padding: 0 9px;
  border: 1px solid rgba(148, 163, 184, 0.28);
  border-radius: var(--radius-full);
  background: rgba(15, 23, 42, 0.62);
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 800;
  letter-spacing: 0;
  white-space: nowrap;
}

.live-stream-pill::before {
  content: '';
  width: 6px;
  height: 6px;
  border-radius: var(--radius-full);
  background: currentColor;
  box-shadow: 0 0 9px currentColor;
}

.live-stream-pill--live {
  border-color: rgba(134, 239, 172, 0.42);
  background: rgba(22, 101, 52, 0.18);
  color: #86efac;
}

.live-stream-pill--polling {
  border-color: rgba(125, 211, 252, 0.42);
  background: rgba(14, 116, 144, 0.18);
  color: #7dd3fc;
}

.live-stream-pill--error {
  border-color: rgba(252, 165, 165, 0.42);
  background: rgba(127, 29, 29, 0.18);
  color: #fca5a5;
}

.live-stream-pill--off {
  color: var(--text-muted);
}

.runtime-strip {
  display: grid;
  grid-template-columns: minmax(240px, 0.95fr) minmax(180px, 0.45fr) minmax(320px, 1.2fr);
  gap: var(--space-3);
  align-items: center;
  padding: var(--space-3) var(--space-4);
  border: 1px solid rgba(125, 211, 252, 0.24);
  border-radius: var(--radius-md);
  background:
    linear-gradient(135deg, rgba(224, 242, 254, 0.82), rgba(245, 242, 234, 0.7)),
    var(--trade-card-bg);
  box-shadow: var(--shadow-card);
}

.runtime-strip--success {
  border-color: rgba(134, 239, 172, 0.32);
  background:
    linear-gradient(135deg, rgba(22, 101, 52, 0.2), rgba(15, 23, 42, 0.72)),
    var(--bg-surface);
}

.runtime-strip--warn {
  border-color: rgba(253, 230, 138, 0.34);
  background:
    linear-gradient(135deg, rgba(146, 64, 14, 0.2), rgba(15, 23, 42, 0.72)),
    var(--bg-surface);
}

.runtime-strip--error {
  border-color: rgba(252, 165, 165, 0.34);
  background:
    linear-gradient(135deg, rgba(127, 29, 29, 0.24), rgba(15, 23, 42, 0.72)),
    var(--bg-surface);
}

.runtime-strip__main {
  min-width: 0;
  display: grid;
  gap: 2px;
}

.runtime-strip__eyebrow {
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 800;
  letter-spacing: 0;
}

.runtime-strip__main strong {
  overflow: hidden;
  color: var(--text-bright);
  font-size: var(--text-base);
  line-height: 1.3;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.runtime-strip__main small {
  overflow: hidden;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.runtime-strip__progress {
  min-width: 0;
  display: grid;
  gap: 5px;
}

.runtime-strip__progress span {
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.runtime-strip__steps {
  min-width: 0;
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
  justify-content: flex-end;
}

.runtime-step {
  display: inline-flex;
  align-items: center;
  max-width: 132px;
  min-height: 24px;
  padding: 0 9px;
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-full);
  background: rgba(255, 255, 255, 0.68);
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 800;
  line-height: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.runtime-step--running {
  border-color: rgba(3, 105, 161, 0.26);
  background: rgba(240, 249, 255, 0.92);
  color: var(--trade-info);
  box-shadow: 0 0 0 1px rgba(3, 105, 161, 0.08);
}

.runtime-step--done {
  border-color: rgba(21, 128, 61, 0.24);
  background: rgba(240, 253, 244, 0.92);
  color: var(--trade-good);
}

.runtime-step--warn {
  border-color: rgba(180, 83, 9, 0.26);
  background: rgba(255, 251, 235, 0.94);
  color: var(--trade-warn);
}

.runtime-step--error {
  border-color: rgba(185, 28, 28, 0.24);
  background: rgba(254, 242, 242, 0.92);
  color: var(--trade-bad);
}

.guardrail-toggle {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  gap: var(--space-2);
}

.guardrail-toggle .state-text {
  flex: 0 0 auto;
  min-width: 54px;
}

.guardrail-toggle :deep(.el-switch) {
  flex: 0 0 auto;
  order: -1;
}

.strategy-config-stack {
  min-width: 0;
  display: grid;
  gap: var(--space-4);
}

.desk-grid {
  grid-template-columns: minmax(520px, 1.08fr) minmax(380px, 0.92fr);
  align-items: start;
}

.operations-stack {
  min-width: 0;
  display: grid;
  gap: var(--space-4);
  align-content: start;
}

.preflight-panel {
  min-width: 0;
  display: grid;
  gap: var(--space-3);
  padding-bottom: var(--space-3);
}

.preflight-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: var(--space-2);
  padding: 0 var(--space-4);
}

.preflight-check {
  min-width: 0;
  padding: 7px var(--space-2);
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-sm);
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.74), rgba(245, 242, 234, 0.62)),
    var(--trade-card-bg);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.72);
}

.preflight-check label,
.preflight-check small {
  display: block;
  overflow: hidden;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preflight-check strong {
  display: block;
  margin: 2px 0;
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: var(--text-sm);
}

.preflight-check__value--good {
  color: var(--trade-good) !important;
}

.preflight-check__value--warn {
  color: var(--trade-warn) !important;
}

.preflight-check__value--bad {
  color: var(--trade-bad) !important;
}

.preflight-issues,
.preflight-actions {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-2);
  max-height: 76px;
  overflow-x: hidden;
  overflow-y: auto;
  padding: 0 var(--space-4);
}

.preflight-issues span,
.preflight-actions span {
  max-width: 100%;
  padding: 5px 8px;
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.35;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preflight-issues span {
  border: 1px solid rgba(185, 28, 28, 0.24);
  background: rgba(254, 242, 242, 0.9);
  color: var(--trade-bad);
}

.preflight-actions span {
  border: 1px solid rgba(3, 105, 161, 0.22);
  background: rgba(240, 249, 255, 0.92);
  color: var(--trade-info);
}

.runner-panel {
  min-width: 0;
  align-self: start;
}

.lower-grid {
  grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr);
}

.trade-journal-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.15fr) minmax(420px, 0.85fr);
  gap: var(--space-4);
  align-items: start;
}

.trade-record-panel,
.weekly-panel {
  min-width: 0;
}

.pending-order-panel {
  display: grid;
  gap: var(--space-3);
  padding-bottom: var(--space-4);
}

.pending-order-panel :deep(.el-table) {
  margin: 0 var(--space-4);
}

.pending-sync-pill {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 0 8px;
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-sm);
  background: rgba(255, 255, 255, 0.72);
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
  white-space: nowrap;
}

.pending-sync-pill--syncing,
.pending-sync-pill--watching {
  border-color: rgba(3, 105, 161, 0.24);
  background: rgba(240, 249, 255, 0.94);
  color: var(--trade-info);
}

.pending-sync-pill--fresh {
  border-color: rgba(21, 128, 61, 0.24);
  background: rgba(240, 253, 244, 0.94);
  color: var(--trade-good);
}

.pending-sync-pill--error {
  border-color: rgba(185, 28, 28, 0.26);
  background: rgba(254, 242, 242, 0.94);
  color: var(--trade-bad);
}

.signal-hash-pill {
  display: inline-flex;
  align-items: center;
  min-height: 26px;
  padding: 0 9px;
  border: 1px solid rgba(3, 105, 161, 0.2);
  border-radius: var(--radius-sm);
  background: rgba(240, 249, 255, 0.9);
  color: var(--trade-info);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 800;
  white-space: nowrap;
}

.submit-summary {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: var(--space-3);
  margin-bottom: var(--space-3);
}

.submit-summary div {
  min-width: 0;
  padding: var(--space-3);
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-sm);
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.68), rgba(245, 242, 234, 0.72)),
    var(--trade-card-bg);
}

.submit-summary label {
  display: block;
  margin-bottom: 4px;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
}

.submit-summary strong {
  color: var(--text-bright);
  font-family: var(--font-data);
}

.submit-result-note {
  margin-bottom: var(--space-3);
}

.submit-result-dialog :deep(.el-dialog__body) {
  max-height: min(72vh, 760px);
  overflow: auto;
}

.weekly-summary {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: var(--space-2);
  padding: var(--space-4);
}

.weekly-summary div {
  min-width: 0;
  padding: var(--space-2);
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-sm);
  background: rgba(255, 255, 255, 0.72);
}

.weekly-summary label {
  display: block;
  margin-bottom: 3px;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
}

.weekly-summary strong {
  color: var(--text-bright);
  font-family: var(--font-data);
}

.weekly-note {
  margin: 0 var(--space-4) var(--space-3);
  border: 1px solid rgba(56, 189, 248, 0.22);
  background: rgba(8, 47, 73, 0.26) !important;
  color: #bae6fd;
}

.weekly-note :deep(.el-alert__title),
.weekly-note :deep(.el-alert__content) {
  color: #bae6fd;
}

.weekly-note :deep(.el-alert__icon) {
  color: var(--accent-primary);
}

.weekly-note-list {
  display: grid;
  gap: var(--space-2);
  padding: 0 var(--space-4) var(--space-3);
}

.weekly-note-list span {
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.45;
}

.weekly-table-title {
  margin: 0 var(--space-4) var(--space-2);
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 800;
}

.trade-record-panel :deep(.el-table),
.weekly-panel :deep(.el-table) {
  margin: 0 var(--space-4) var(--space-4);
  width: calc(100% - var(--space-4) * 2);
}

.control-body {
  display: grid;
  gap: var(--space-3);
  padding: var(--space-4);
}

.profile-meta {
  display: grid;
  gap: var(--space-2);
  padding: var(--space-3);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(255, 255, 255, 0.03);
}

.profile-meta span,
.table-actions span {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.table-actions .state-text {
  color: var(--text-secondary);
  font-family: var(--font-ui);
  font-size: var(--text-sm);
}

.table-actions .state-text--good {
  color: #86efac;
}

.table-actions .state-text--warn {
  color: #fde68a;
}

.table-actions .state-text--bad {
  color: #fca5a5;
}

.table-actions .state-text--neutral {
  color: var(--text-secondary);
}

.param-row {
  display: grid;
  grid-template-columns: 88px minmax(0, 1fr);
  gap: var(--space-2);
  align-items: center;
}

.param-row label {
  margin: 0;
}

.inline-signal {
  display: grid;
  gap: var(--space-2);
  padding: var(--space-3);
  border: 1px solid rgba(3, 105, 161, 0.18);
  border-radius: var(--radius-md);
  background:
    linear-gradient(135deg, rgba(240, 249, 255, 0.9), rgba(253, 251, 247, 0.78)),
    var(--trade-card-bg-soft);
}

.inline-signal__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.inline-signal__head span {
  color: var(--text-bright);
  font-size: var(--text-xs);
  font-weight: 800;
}

.inline-signal__stats {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: var(--space-2);
}

.inline-signal__stats div {
  min-width: 0;
  padding: var(--space-2);
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-sm);
  background: rgba(255, 255, 255, 0.76);
}

.inline-signal__stats label {
  display: block;
  margin-bottom: 2px;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
}

.inline-signal__stats strong {
  color: #0c4a6e;
  font-family: var(--font-data);
  font-size: var(--text-lg);
}

.inline-signal__preview {
  display: grid;
  gap: 4px;
  color: var(--text-secondary);
  font-size: var(--text-xs);
}

.inline-signal__preview span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.inline-signal__preview strong {
  margin-right: var(--space-2);
  color: var(--text-bright);
}

.runner-actions,
.runner-state {
  padding: var(--space-3) var(--space-4);
}

.runner-actions {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  justify-content: stretch;
}

.runner-actions :deep(.el-button) {
  width: 100%;
  margin-left: 0;
}

.runner-state {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: var(--space-3);
}

.account-panel {
  display: grid;
  gap: var(--space-3);
  padding-bottom: var(--space-4);
}

.account-panel :deep(.el-alert),
.account-panel :deep(.el-table) {
  margin: 0 var(--space-4);
}

.account-panel :deep(.el-table) {
  width: calc(100% - var(--space-4) * 2);
}

.account-summary {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(132px, 1fr));
  gap: var(--space-3);
  padding: 0 var(--space-4);
}

.account-summary div {
  min-width: 0;
  padding: var(--space-3);
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-sm);
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.7), rgba(245, 242, 234, 0.72)),
    var(--trade-card-bg);
}

.account-summary label {
  display: block;
  margin-bottom: 4px;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
}

.account-summary strong,
.metric-neutral {
  color: var(--text-bright);
  font-family: var(--font-data);
}

.broker-account,
.broker-account-panel {
  display: grid;
  gap: var(--space-3);
}

.broker-account {
  margin: var(--space-2) var(--space-4) 0;
  padding-top: var(--space-3);
  border-top: 1px solid rgba(148, 163, 184, 0.16);
}

.broker-account-panel {
  padding: var(--space-4);
}

.broker-account__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-3);
}

.broker-account__head h4 {
  margin: 2px 0 0;
  color: var(--text-bright);
  font-size: var(--text-base);
}

.broker-summary {
  padding: 0;
}

.capital-form {
  margin-top: var(--space-4);
}

.capital-input {
  width: 240px;
}

.positions-dialog :deep(.el-dialog__body) {
  display: grid;
  gap: var(--space-3);
  max-height: min(76vh, 780px);
  overflow: hidden;
}

.positions-dialog__toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-3);
}

.positions-dialog__summary {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-2);
  color: var(--text-secondary);
  font-size: var(--text-xs);
}

.positions-dialog__summary span {
  padding: 5px 8px;
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-sm);
  background: rgba(255, 255, 255, 0.76);
}

.positions-dialog :deep(.el-table) {
  width: 100%;
}

.position-column-header {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  max-width: 100%;
  cursor: grab;
  user-select: none;
  vertical-align: middle;
}

.position-column-header:active {
  cursor: grabbing;
}

.position-column-header--dragging {
  color: #7dd3fc;
  opacity: 0.66;
}

.position-column-header--over {
  color: #a7f3d0;
}

.position-column-header--over::after {
  content: '';
  width: 2px;
  align-self: stretch;
  border-radius: var(--radius-full);
  background: #22d3ee;
  box-shadow: 0 0 10px rgba(34, 211, 238, 0.72);
}

.position-column-header__grip {
  width: 8px;
  height: 14px;
  flex: 0 0 auto;
  opacity: 0.72;
  background:
    radial-gradient(circle, currentColor 1px, transparent 1.4px) 0 0 / 4px 4px,
    radial-gradient(circle, currentColor 1px, transparent 1.4px) 4px 2px / 4px 4px;
}

.drag-hint {
  color: var(--text-muted);
  font-size: var(--text-xs);
  white-space: nowrap;
}

.metric-good {
  color: var(--trade-good) !important;
}

.metric-bad {
  color: var(--trade-bad) !important;
}

.metric-warn {
  color: var(--trade-warn) !important;
}

.stack-cell {
  display: grid;
  gap: 2px;
  min-width: 0;
  line-height: 1.25;
}

.stack-cell--right {
  justify-items: end;
}

.stack-cell strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: var(--text-sm);
  font-weight: 700;
}

.stack-cell small {
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

:global(.strategy-review-dialog.el-dialog),
:global(.strategy-review-dialog .el-dialog) {
  border: 1px solid rgba(148, 163, 184, 0.24);
  background: #0b111c;
  box-shadow: 0 24px 80px rgba(0, 0, 0, 0.56);
}

:global(.strategy-review-dialog .el-dialog__header) {
  border-bottom: 1px solid rgba(148, 163, 184, 0.16);
}

:global(.strategy-review-dialog .el-dialog__title) {
  color: #f8fafc;
  font-weight: 800;
}

:global(.strategy-review-dialog .el-dialog__headerbtn .el-dialog__close) {
  color: #cbd5e1;
}

:global(.strategy-review-dialog .el-dialog__body) {
  max-height: min(76vh, 820px);
  overflow: auto;
  color: var(--text-primary);
  background:
    linear-gradient(180deg, rgba(15, 23, 42, 0.92), rgba(2, 6, 23, 0.96)),
    var(--bg-surface);
}

:global(.strategy-review-dialog .el-dialog__footer) {
  border-top: 1px solid rgba(148, 163, 184, 0.16);
  background: #0b111c;
}

.strategy-review {
  display: grid;
  gap: var(--space-4);
}

.strategy-review__intro {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(300px, 0.72fr);
  gap: var(--space-4);
  align-items: start;
}

.strategy-review__intro h3 {
  margin: var(--space-1) 0 var(--space-2);
  color: var(--text-bright);
}

.strategy-review__intro p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
  line-height: 1.65;
}

.strategy-review__facts,
.strategy-review__metrics {
  display: grid;
  gap: var(--space-2);
}

.strategy-review__facts {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.strategy-review__metrics {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

.strategy-review__facts span,
.strategy-review__metrics div {
  min-width: 0;
  padding: var(--space-2);
  border: 1px solid rgba(148, 163, 184, 0.16);
  border-radius: var(--radius-sm);
  background: rgba(15, 23, 42, 0.52);
}

.strategy-review__facts label,
.strategy-review__metrics label {
  display: block;
  margin-bottom: 3px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.strategy-review__facts strong,
.strategy-review__metrics strong {
  display: block;
  overflow: hidden;
  color: var(--text-primary);
  font-family: var(--font-data);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.strategy-review__charts {
  display: grid;
  grid-template-columns: minmax(0, 1.3fr) minmax(0, 1fr);
  gap: var(--space-3);
}

.strategy-review__charts section,
.strategy-review__tables section {
  min-width: 0;
  border: 1px solid rgba(148, 163, 184, 0.16);
  border-radius: var(--radius-md);
  background: rgba(15, 23, 42, 0.36);
}

.strategy-review__charts section:first-child {
  grid-row: span 2;
}

.strategy-chart-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-2);
  padding: var(--space-3) var(--space-3) 0;
}

.strategy-chart-head span {
  color: var(--text-bright);
  font-size: var(--text-sm);
  font-weight: 800;
}

.strategy-chart-head small {
  overflow: hidden;
  color: var(--text-muted);
  font-size: var(--text-xs);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.strategy-chart {
  min-height: 230px;
}

.strategy-review__charts section:first-child .strategy-chart {
  min-height: 490px;
}

.strategy-chart--empty {
  display: grid;
  place-items: center;
  padding: var(--space-4);
  color: var(--text-secondary);
  font-size: var(--text-sm);
  text-align: center;
}

.strategy-review__tables {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: var(--space-3);
}

.strategy-review__tables .weekly-table-title {
  margin-top: var(--space-3);
}

.strategy-review__tables :deep(.el-table) {
  margin: 0 var(--space-3) var(--space-3);
}

.signal-overview {
  display: grid;
  grid-template-columns: minmax(230px, 0.62fr) minmax(0, 1fr) minmax(0, 1fr);
  gap: var(--space-3);
  padding: var(--space-4);
}

.signal-result-panel {
  display: grid;
  gap: var(--space-2);
}

.signal-overview__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--space-4) var(--space-4) 0;
}

.signal-overview__head span,
.candidate-preview > label,
.order-preview > label {
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
}

.signal-stats {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: var(--space-2);
}

.signal-stats div {
  min-width: 0;
  padding: var(--space-2);
  border: 1px solid rgba(3, 105, 161, 0.18);
  border-radius: var(--radius-sm);
  background: rgba(240, 249, 255, 0.86);
}

.signal-stats label,
.candidate-preview label,
.order-preview label {
  display: block;
  margin-bottom: 3px;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
}

.signal-stats strong {
  color: #0c4a6e;
  font-family: var(--font-data);
  font-size: var(--text-lg);
}

.candidate-list,
.order-preview__rows {
  display: grid;
  gap: var(--space-2);
}

.candidate-pill,
.order-preview__rows span {
  display: grid;
  gap: 2px;
  min-width: 0;
  padding: var(--space-2);
  border: 1px solid var(--trade-card-border);
  border-radius: var(--radius-sm);
  background: rgba(255, 255, 255, 0.72);
  color: var(--text-secondary);
  font-size: var(--text-xs);
}

.candidate-pill strong,
.order-preview__rows strong {
  color: var(--text-bright);
  font-family: var(--font-data);
}

.candidate-pill small {
  overflow: hidden;
  color: var(--text-muted);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.muted-line {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.order-panel :deep(.el-input-number) {
  width: 128px;
}

:deep(.el-table) {
  --el-table-bg-color: rgba(253, 251, 247, 0.98);
  --el-table-tr-bg-color: rgba(253, 251, 247, 0.98);
  --el-table-header-bg-color: rgba(245, 242, 234, 0.98);
  --el-table-header-text-color: var(--text-secondary);
  --el-table-text-color: var(--text-bright);
  --el-table-row-hover-bg-color: rgba(224, 242, 254, 0.72);
  --el-table-border-color: rgba(27, 61, 50, 0.12);
  --el-table-current-row-bg-color: rgba(224, 242, 254, 0.64);
  border: 1px solid rgba(27, 61, 50, 0.1);
  border-radius: var(--radius-sm);
  background: var(--el-table-bg-color);
  color: var(--text-bright);
  overflow: hidden;
}

:deep(.el-table__header-wrapper),
:deep(.el-table__body-wrapper),
:deep(.el-table__footer-wrapper) {
  background: var(--el-table-bg-color);
}

:deep(.el-table td.el-table__cell),
:deep(.el-table th.el-table__cell) {
  background: var(--el-table-tr-bg-color);
  color: var(--text-bright);
}

:deep(.el-table th.el-table__cell) {
  background: var(--el-table-header-bg-color) !important;
  color: var(--text-secondary);
  font-weight: 800;
}

:deep(.el-table .cell) {
  color: inherit;
}

:deep(.el-table .el-table__row--striped td.el-table__cell) {
  background: rgba(238, 243, 240, 0.82) !important;
}

:deep(.el-table__body tr:hover > td.el-table__cell) {
  background: var(--el-table-row-hover-bg-color) !important;
}

@media (max-width: 1100px) {
  .page-head,
  .desk-grid,
  .lower-grid,
  .trade-journal-grid,
  .status-band,
  .runtime-strip,
  .summary-band,
  .submit-summary,
  .preflight-grid,
  .account-summary,
  .weekly-summary,
  .signal-overview,
  .strategy-review__intro,
  .strategy-review__metrics,
  .strategy-review__charts,
  .strategy-review__tables {
    grid-template-columns: 1fr;
  }

  .actions,
  .table-actions,
  .runtime-strip__steps {
    justify-content: flex-start;
  }
}
</style>
