<template>
  <div class="page-frame live-page">
    <header class="panel-card page-head">
      <div>
        <span class="section-kicker">LIVE TRADING DESK</span>
        <h2>模拟 / 实盘</h2>
        <p>可配置策略执行台：默认接入 CashAware 稳健版与进攻版，自动交易与真实下单均受独立护栏控制。</p>
      </div>
      <div class="actions">
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
        <span :class="['state-text', status?.order_submit_enabled ? 'state-text--bad' : 'state-text--neutral']">
          {{ status?.order_submit_enabled ? '开启' : '关闭' }}
        </span>
      </div>
      <div>
        <label>自动实盘</label>
        <span :class="['state-text', status?.auto_execute_enabled ? 'state-text--bad' : 'state-text--neutral']">
          {{ status?.auto_execute_enabled ? '允许' : '禁止' }}
        </span>
      </div>
      <div>
        <label>Runner</label>
        <span :class="['state-text', runnerStateClass]">{{ runnerText }}</span>
      </div>
    </section>

    <section class="desk-grid">
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

    <section class="panel-card account-panel">
      <div class="panel-card__head">
        <div>
          <span class="section-kicker">STRATEGY CAPITAL POOL</span>
          <h3>量化资金池</h3>
        </div>
        <div class="table-actions">
          <span :class="['state-text', strategyAccountReady ? 'state-text--good' : 'state-text--warn']">{{ strategyAccountStatusText }}</span>
          <el-button size="small" :loading="accountLoading" @click="loadAccount">刷新账户</el-button>
          <el-button size="small" @click="capitalDialogOpen = true">
            {{ strategyAccountReady ? '调整本金' : '圈定本金' }}
          </el-button>
        </div>
      </div>
      <el-alert
        v-if="!strategyAccountReady"
        type="warning"
        show-icon
        title="请先圈定本次量化交易本金；策略只会使用这块资金池，不会清空你原有的 QMT 持仓。"
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
      <el-table :data="accountPositions" size="small" stripe border height="260" empty-text="暂无持仓">
        <el-table-column prop="symbol" label="代码" width="110" resizable />
        <el-table-column label="名称" width="120" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.stock_name || '-' }}</template>
        </el-table-column>
        <el-table-column label="持仓" width="110" resizable align="right">
          <template #default="{ row }">{{ formatQuantity(row.quantity) }}</template>
        </el-table-column>
        <el-table-column label="可用" width="110" resizable align="right">
          <template #default="{ row }">{{ formatQuantity(row.available) }}</template>
        </el-table-column>
        <el-table-column label="成本价" width="110" resizable align="right">
          <template #default="{ row }">{{ formatPrice(row.avg_cost) }}</template>
        </el-table-column>
        <el-table-column label="市值" width="130" resizable align="right">
          <template #default="{ row }">{{ formatMoney(row.market_value) }}</template>
        </el-table-column>
        <el-table-column label="浮盈亏" width="120" resizable align="right">
          <template #default="{ row }">
            <span :class="pnlTone(row.unrealized_pnl)">{{ formatMoney(row.unrealized_pnl) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="盈亏率" width="100" resizable align="right">
          <template #default="{ row }">
            <span :class="pnlTone(row.unrealized_pnl)">{{ formatPercent(row.unrealized_pnl_pct) }}</span>
          </template>
        </el-table-column>
      </el-table>

      <div v-if="brokerAccountSnapshot" class="broker-account">
        <div class="broker-account__head">
          <div>
            <span class="section-kicker">BROKER CONTEXT</span>
            <h4>QMT 实际账户参考</h4>
          </div>
          <el-tag type="info">只读参考</el-tag>
        </div>
        <div class="account-summary broker-summary">
          <div v-for="item in brokerSummaryItems" :key="item.label">
            <label>{{ item.label }}</label>
            <strong :class="item.tone ? `metric-${item.tone}` : ''">{{ item.value }}</strong>
          </div>
        </div>
      </div>
    </section>

    <el-alert
      v-if="status?.order_submit_enabled"
      type="error"
      show-icon
      title="真实下单开关已开启，提交前仍需确认。"
    />
    <el-alert
      v-if="signalWarningNote"
      type="warning"
      show-icon
      :title="signalWarningNote"
    />
    <el-alert
      v-if="heatFilterInfoNote"
      type="info"
      :closable="false"
      show-icon
      :title="heatFilterInfoNote"
    />

    <section ref="orderPanelRef" class="panel-card order-panel">
      <div class="panel-card__head">
        <div>
          <span class="section-kicker">ORDER BASKET</span>
          <h3>订单篮子</h3>
        </div>
        <div class="table-actions">
          <span>{{ shortHash(signalData?.signal_hash) }}</span>
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

    <section class="lower-grid">
      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">SKIPS</span>
            <h3>跳过订单</h3>
          </div>
        </div>
        <el-table :data="signalData?.skipped_orders || []" size="small" border height="260">
          <el-table-column prop="symbol" label="代码" width="110" resizable />
          <el-table-column label="名称" width="120" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.stock_name || '-' }}</template>
          </el-table-column>
          <el-table-column prop="side" label="方向" width="80" resizable />
          <el-table-column prop="quantity" label="数量" width="100" resizable />
          <el-table-column prop="reason" label="原因" resizable show-overflow-tooltip />
        </el-table>
      </article>

      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">AUDIT</span>
            <h3>订单审计</h3>
          </div>
          <el-button text size="small" @click="loadAudits">刷新</el-button>
        </div>
        <el-table :data="audits" size="small" border height="260">
          <el-table-column label="时间" width="168" resizable>
            <template #default="{ row }">{{ formatDateTime(row.created_at) }}</template>
          </el-table-column>
          <el-table-column prop="profile_key" label="Profile" width="150" resizable show-overflow-tooltip />
          <el-table-column prop="mode" label="模式" width="72" resizable />
          <el-table-column label="状态" width="100" resizable>
            <template #default="{ row }">
              <el-tag :type="statusTagType(row.status)" effect="plain">{{ statusLabel(row.status) }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="skip_reason" label="说明" resizable show-overflow-tooltip />
        </el-table>
      </article>
    </section>

    <section ref="pendingPanelRef" class="panel-card pending-order-panel">
      <div class="panel-card__head">
        <div>
          <span class="section-kicker">PENDING FILLS</span>
          <h3>待成交追踪</h3>
        </div>
        <div class="table-actions">
          <span>{{ pendingOrders.length }} 笔实盘委托待确认</span>
          <el-button text size="small" :loading="pendingLoading" @click="loadPendingOrders">刷新</el-button>
        </div>
      </div>
      <el-table :data="pendingOrders" size="small" border height="280" empty-text="暂无待成交真实委托">
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
        <el-table-column label="委托价" width="110" resizable align="right">
          <template #default="{ row }">{{ formatPrice(row.reference_price) }}</template>
        </el-table-column>
        <el-table-column label="委托金额" width="130" resizable align="right">
          <template #default="{ row }">{{ formatMoney(row.order_value) }}</template>
        </el-table-column>
        <el-table-column label="QMT委托号" width="130" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.order_id || '-' }}</template>
        </el-table-column>
        <el-table-column label="说明" min-width="220" resizable show-overflow-tooltip>
          <template #default="{ row }">{{ row.message || row.result_payload?.message || '-' }}</template>
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
        <el-table :data="weeklyAnalysis?.top_symbols || []" size="small" border height="220">
          <el-table-column prop="symbol" label="代码" width="110" resizable />
          <el-table-column label="名称" width="120" resizable show-overflow-tooltip>
            <template #default="{ row }">{{ row.stock_name || '-' }}</template>
          </el-table-column>
          <el-table-column prop="records" label="次数" width="80" resizable align="right" />
          <el-table-column prop="notional" label="金额" width="120" resizable align="right">
            <template #default="{ row }">{{ formatMoney(row.notional) }}</template>
          </el-table-column>
          <el-table-column prop="net_notional" label="净额" width="120" resizable align="right">
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

    <el-dialog v-model="capitalDialogOpen" title="圈定量化交易本金" width="520px">
      <el-alert
        type="info"
        :closable="false"
        show-icon
        title="初始化后，策略只用这块资金和它自己买出的持仓调仓；QMT 里原有的手工持仓不会进入卖出列表。"
      />
      <el-form class="capital-form" label-width="120px">
        <el-form-item label="当前 Profile">
          <span>{{ selectedProfile?.display_name || selectedProfileKey || '-' }}</span>
        </el-form-item>
        <el-form-item label="模式">
          <el-tag>{{ mode === 'paper' ? '模拟' : '实盘' }}</el-tag>
        </el-form-item>
        <el-form-item label="本金">
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
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="capitalDialogOpen = false">取消</el-button>
        <el-button type="primary" :loading="capitalSaving" @click="initializeCapitalPool">确认初始化</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
import {
  liveTradingApi,
  type LiveAccountSnapshot,
  type LiveOrder,
  type LiveOrderAudit,
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

const mode = ref<LiveTradingMode>('paper')
const tradeDate = ref(new Date().toISOString().slice(0, 10))
const indexSymbol = ref('399101.SZ')
const selectedProfileKey = ref('')
const status = ref<LiveTradingStatus | null>(null)
const profiles = ref<LiveStrategyProfile[]>([])
const preflightData = ref<LivePreflightResponse | null>(null)
const signalData = ref<LiveSignalsResponse | null>(null)
const accountSnapshot = ref<LiveAccountSnapshot | null>(null)
const orderRows = ref<LiveOrder[]>([])
const audits = ref<LiveOrderAudit[]>([])
const tradeRecords = ref<LiveTradeRecord[]>([])
const pendingOrders = ref<LiveTradeRecord[]>([])
const weeklyAnalysis = ref<LiveWeeklyAnalysis | null>(null)
const orderPanelRef = ref<HTMLElement | null>(null)
const pendingPanelRef = ref<HTMLElement | null>(null)
const loading = ref(false)
const accountLoading = ref(false)
const preflightLoading = ref(false)
const signalsLoading = ref(false)
const runnerLoading = ref(false)
const journalLoading = ref(false)
const pendingLoading = ref(false)
const profileDialogOpen = ref(false)
const profileSaving = ref(false)
const capitalDialogOpen = ref(false)
const capitalSaving = ref(false)
const submitResultDialogOpen = ref(false)
const submitResult = ref<LiveSubmitOrdersResponse | null>(null)
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

const selectedProfile = computed(() => profiles.value.find(item => item.profile_key === selectedProfileKey.value) || null)
const runnerText = computed(() => {
  const runner = status.value?.runner
  if (!runner) return '-'
  return runner.takeover ? '人工接管' : runner.status
})
const heatFilterInfoNote = computed(() => {
  const note = signalData.value?.heat_filter_note || ''
  return note.startsWith('drop_top_') ? formatHeatFilterNote(note) : ''
})
const heatFilterWarningNote = computed(() => {
  const note = signalData.value?.heat_filter_note || ''
  return note && !note.startsWith('drop_top_') ? formatHeatFilterNote(note) : ''
})
const signalWarningNote = computed(() => (
  signalData.value?.quote_error
  || signalData.value?.account.error
  || heatFilterWarningNote.value
  || ''
))
const candidatePreview = computed(() => (signalData.value?.top_candidates || []).slice(0, 3))
const orderPreview = computed(() => orderRows.value.slice(0, 3))
const accountPositions = computed(() => accountSnapshot.value?.positions || [])
const brokerAccountSnapshot = computed(() => accountSnapshot.value?.broker_account || null)
const strategyAccountReady = computed(() => Boolean(accountSnapshot.value?.meta?.initialized))
const strategyAccountStatusText = computed(() => {
  if (!accountSnapshot.value) return '未加载'
  if (strategyAccountReady.value) return mode.value === 'paper' ? '模拟资金池' : '实盘资金池'
  return '未圈定本金'
})
const accountSourceText = computed(() => {
  const source = accountSnapshot.value?.source || (mode.value === 'paper' ? 'paper' : 'qmt')
  return mode.value === 'paper' ? `模拟 · ${source}` : `实盘 · ${source}`
})
const accountSummaryItems = computed(() => {
  const account = accountSnapshot.value
  const pnl = Number(account?.unrealized_pnl || 0)
  const meta = account?.meta || {}
  return [
    { label: '初始本金', value: formatMoney(Number(meta.initial_capital || 0) || null) },
    { label: '可用现金', value: formatMoney(account?.cash) },
    { label: '总资产', value: formatMoney(account?.total_asset) },
    { label: '持仓市值', value: formatMoney(account?.market_value) },
    { label: '浮盈亏', value: formatMoney(pnl), tone: pnl > 0 ? 'good' : pnl < 0 ? 'bad' : 'neutral' },
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
  const pnl = Number(summary?.paper_realized_pnl || 0)
  return [
    { label: '记录数', value: `${summary?.total_records || 0}` },
    { label: '已成/待成/失败', value: `${summary?.completed_records || 0}/${summary?.live_submitted_records || 0}/${summary?.failed_records || 0}` },
    { label: '买入金额', value: formatMoney(summary?.buy_notional) },
    { label: '卖出金额', value: formatMoney(summary?.sell_notional) },
    { label: '净买入', value: formatMoney(summary?.net_notional) },
    { label: '模拟已实现', value: formatMoney(pnl), tone: pnl > 0 ? 'good' : pnl < 0 ? 'bad' : 'neutral' },
  ]
})
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

async function loadAll() {
  loading.value = true
  try {
    const [nextStatus, nextProfiles] = await Promise.all([
      liveTradingApi.status(),
      liveTradingApi.profiles(true),
    ])
    status.value = nextStatus
    profiles.value = nextProfiles
    if (!selectedProfileKey.value) {
      selectedProfileKey.value = nextProfiles.find(item => item.is_default)?.profile_key || nextStatus.default_profile || nextProfiles[0]?.profile_key || ''
    }
    await Promise.all([loadAccount(), loadPreflight(), loadAudits(), loadTradeJournal(), loadPendingOrders()])
  } finally {
    loading.value = false
  }
}

watch(mode, async () => {
  if (!selectedProfileKey.value) return
  signalData.value = null
  orderRows.value = []
  await Promise.all([loadAccount(), loadPreflight(), loadAudits(), loadTradeJournal(), loadPendingOrders()])
})

async function loadAccount() {
  accountLoading.value = true
  try {
    accountSnapshot.value = await liveTradingApi.account(mode.value, selectedProfileKey.value || undefined)
    const initialCapital = Number(accountSnapshot.value?.meta?.initial_capital || 0)
    if (initialCapital > 0) {
      capitalForm.capital = initialCapital
    }
  } finally {
    accountLoading.value = false
  }
}

async function loadPreflight() {
  if (!selectedProfileKey.value) return null
  preflightLoading.value = true
  try {
    preflightData.value = await liveTradingApi.preflight({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
      evaluate_pipeline: true,
    })
    return preflightData.value
  } finally {
    preflightLoading.value = false
  }
}

async function loadSignals() {
  if (!selectedProfileKey.value) return
  if (!strategyAccountReady.value) {
    await loadAccount()
  }
  if (!strategyAccountReady.value) {
    ElMessage.warning('请先圈定量化交易本金，再生成信号。')
    capitalDialogOpen.value = true
    return
  }
  signalsLoading.value = true
  try {
    signalData.value = await liveTradingApi.signals({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
    })
    preflightData.value = signalData.value.preflight || preflightData.value
    orderRows.value = (signalData.value.orders || []).map(order => ({ ...order }))
  } finally {
    signalsLoading.value = false
  }
}

async function loadAudits() {
  audits.value = await liveTradingApi.audits({
    profile_key: selectedProfileKey.value || undefined,
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
  } finally {
    pendingLoading.value = false
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
  runnerLoading.value = true
  try {
    await liveTradingApi.startRunner({
      profile_key: selectedProfileKey.value,
      mode: mode.value,
      params: {
        trade_date: tradeDate.value,
        index_symbol: indexSymbol.value,
      },
      interval_seconds: 60,
    })
    ElMessage.success('自动交易已启动')
    status.value = await liveTradingApi.status()
  } finally {
    runnerLoading.value = false
  }
}

async function stopRunner() {
  runnerLoading.value = true
  try {
    await liveTradingApi.stopRunner()
    ElMessage.success('自动交易已停止')
    status.value = await liveTradingApi.status()
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
  runnerLoading.value = true
  try {
    await liveTradingApi.takeover('human takeover from UI')
    ElMessage.warning('已切换为人工接管')
    status.value = await liveTradingApi.status()
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
  const result = await liveTradingApi.submitOrders({
    mode: mode.value,
    orders: orders as unknown as Record<string, unknown>[],
    confirm: true,
  })
  submitResult.value = result
  submitResultDialogOpen.value = true
  if (submitResultPendingCount.value) {
    ElMessage.warning(`${submitResultPendingCount.value} 笔真实委托已提交，等待成交确认。`)
  } else if (submitResultFailedCount.value) {
    ElMessage.warning(`${submitResultFailedCount.value} 笔订单提交失败，请查看提交结果。`)
  } else {
    ElMessage.success('订单提交完成')
  }
  await Promise.all([
    loadAccount(),
    loadAudits(),
    loadTradeJournal(),
    loadPendingOrders(),
    liveTradingApi.status().then(next => { status.value = next }),
  ])
}

async function initializeCapitalPool() {
  if (!selectedProfileKey.value) return
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
    ElMessage.success('量化资金池已初始化')
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
  await Promise.all([loadAccount(), loadPreflight(), loadAudits(), loadTradeJournal(), loadPendingOrders()])
}

function formatHeatFilterNote(note: string) {
  const dropped = note.match(/^drop_top_(\d+)%_by_(.+):dropped=(\d+)$/)
  if (dropped) return `涨停热度过滤已执行：剔除热度最高 ${dropped[1]}%，共 ${dropped[3]} 个候选。`
  const insufficient = note.match(/^limit_up_heat_data_insufficient:(\d+)\/(\d+)$/)
  if (insufficient) return `涨停热度过滤未执行：有效热度样本 ${insufficient[1]}/${insufficient[2]}，低于最低要求。`
  const fallback = note.match(/^limit_up_heat_fallback_after_filter:(\d+)\/(\d+)$/)
  if (fallback) return `涨停热度过滤已回退：过滤后候选 ${fallback[1]}/${fallback[2]}，低于最低持仓要求。`
  if (note === 'limit_up_heat_data_missing') return '涨停热度过滤未执行：缺少日线或涨停价数据。'
  return note
}

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
  return ['live_pending', 'submitted', 'accepted', 'partially_filled'].includes(String(status || ''))
}

function isFilledStatus(status?: string | null) {
  return ['paper_filled', 'live_filled', 'filled'].includes(String(status || ''))
}

function statusLabel(status?: string | null) {
  const labels: Record<string, string> = {
    live_pending: '待成交',
    submitted: '待成交',
    accepted: '已接收',
    partially_filled: '部分成交',
    paper_filled: '模拟成交',
    live_filled: '实盘成交',
    filled: '已成交',
    failed: '失败',
    skipped: '跳过',
    generated: '已生成',
  }
  return labels[String(status || '')] || String(status || '-')
}

function statusTagType(status?: string | null) {
  if (isPendingStatus(status)) return 'warning'
  if (isFilledStatus(status)) return 'success'
  if (String(status || '') === 'failed') return 'danger'
  return 'info'
}

function sideLabel(side?: string | null) {
  const value = String(side || '').toUpperCase()
  if (value === 'BUY') return '买入'
  if (value === 'SELL') return '卖出'
  return value || '-'
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
    title: 'Capital Pool',
    rows: [
      { label: '状态', value: strategyAccountStatusText.value, tone: strategyAccountReady.value ? 'good' : 'warn' },
      { label: '来源', value: accountSourceText.value, tone: accountSnapshot.value?.error ? 'warn' : 'good' },
      { label: '现金', value: formatMoney(accountSnapshot.value?.cash) },
      { label: '总资产', value: formatMoney(accountSnapshot.value?.total_asset) },
      { label: '持仓市值', value: formatMoney(accountSnapshot.value?.market_value) },
      {
        label: '浮盈亏',
        value: formatMoney(accountSnapshot.value?.unrealized_pnl),
        tone: Number(accountSnapshot.value?.unrealized_pnl || 0) > 0 ? 'good' : Number(accountSnapshot.value?.unrealized_pnl || 0) < 0 ? 'bad' : 'neutral',
      },
      { label: '持仓', value: `${accountSnapshot.value?.position_count || 0} 只` },
    ],
  },
])

usePageContext(pageContextBlocks)

onMounted(() => {
  loadAll().catch(error => ElMessage.error(error?.message || '实盘交易模块加载失败'))
})
</script>

<style scoped>
.live-page {
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
.profile-meta p {
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
  color: #86efac;
}

.state-text--warn {
  color: #fde68a;
}

.state-text--bad {
  color: #fca5a5;
}

.state-text--neutral {
  color: var(--text-secondary);
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
  border: 1px solid rgba(148, 163, 184, 0.16);
  border-radius: var(--radius-sm);
  background: rgba(15, 23, 42, 0.42);
}

.preflight-check label,
.preflight-check small {
  display: block;
  overflow: hidden;
  color: var(--text-muted);
  font-size: var(--text-xs);
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.preflight-check strong {
  display: block;
  margin: 2px 0;
  color: var(--text-primary);
  font-family: var(--font-data);
  font-size: var(--text-sm);
}

.preflight-check__value--good {
  color: #86efac !important;
}

.preflight-check__value--warn {
  color: #fde68a !important;
}

.preflight-check__value--bad {
  color: #fca5a5 !important;
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
  border: 1px solid rgba(248, 113, 113, 0.28);
  background: rgba(127, 29, 29, 0.22);
  color: #fecaca;
}

.preflight-actions span {
  border: 1px solid rgba(56, 189, 248, 0.22);
  background: rgba(8, 47, 73, 0.24);
  color: #bae6fd;
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

.submit-summary {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: var(--space-3);
  margin-bottom: var(--space-3);
}

.submit-summary div {
  min-width: 0;
  padding: var(--space-3);
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: var(--radius-sm);
  background: rgba(15, 23, 42, 0.52);
}

.submit-summary label {
  display: block;
  margin-bottom: 4px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.submit-summary strong {
  color: var(--text-primary);
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
  border: 1px solid rgba(148, 163, 184, 0.16);
  border-radius: var(--radius-sm);
  background: rgba(15, 23, 42, 0.42);
}

.weekly-summary label {
  display: block;
  margin-bottom: 3px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.weekly-summary strong {
  color: var(--text-primary);
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

.trade-record-panel :deep(.el-table),
.weekly-panel :deep(.el-table) {
  margin: 0 var(--space-4) var(--space-4);
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
  border: 1px solid rgba(56, 189, 248, 0.22);
  border-radius: var(--radius-md);
  background: rgba(8, 47, 73, 0.22);
}

.inline-signal__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.inline-signal__head span {
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
}

.inline-signal__stats {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: var(--space-2);
}

.inline-signal__stats div {
  min-width: 0;
  padding: var(--space-2);
  border: 1px solid rgba(148, 163, 184, 0.16);
  border-radius: var(--radius-sm);
  background: rgba(15, 23, 42, 0.42);
}

.inline-signal__stats label {
  display: block;
  margin-bottom: 2px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.inline-signal__stats strong {
  color: #e0f2fe;
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

.account-summary {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: var(--space-3);
  padding: 0 var(--space-4);
}

.account-summary div {
  min-width: 0;
  padding: var(--space-3);
  border: 1px solid rgba(148, 163, 184, 0.16);
  border-radius: var(--radius-sm);
  background: rgba(15, 23, 42, 0.42);
}

.account-summary label {
  display: block;
  margin-bottom: 4px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.account-summary strong,
.metric-neutral {
  color: var(--text-primary);
  font-family: var(--font-data);
}

.broker-account {
  display: grid;
  gap: var(--space-3);
  margin: var(--space-2) var(--space-4) 0;
  padding-top: var(--space-3);
  border-top: 1px solid rgba(148, 163, 184, 0.16);
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

.metric-good {
  color: #86efac !important;
}

.metric-bad {
  color: #fca5a5 !important;
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
  border: 1px solid rgba(56, 189, 248, 0.2);
  border-radius: var(--radius-sm);
  background: rgba(8, 47, 73, 0.32);
}

.signal-stats label,
.candidate-preview label,
.order-preview label {
  display: block;
  margin-bottom: 3px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.signal-stats strong {
  color: #e0f2fe;
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
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: var(--radius-sm);
  background: rgba(15, 23, 42, 0.48);
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
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: rgba(15, 23, 42, 0.9);
  --el-table-header-text-color: #cbd5e1;
  --el-table-text-color: #dbe4f0;
  --el-table-row-hover-bg-color: rgba(56, 189, 248, 0.08);
  --el-table-border-color: rgba(148, 163, 184, 0.16);
  --el-table-current-row-bg-color: rgba(56, 189, 248, 0.08);
}

:deep(.el-table td.el-table__cell),
:deep(.el-table th.el-table__cell) {
  background: rgba(15, 23, 42, 0.34);
}

:deep(.el-table .el-table__row--striped td.el-table__cell) {
  background: rgba(30, 41, 59, 0.72) !important;
}

:deep(.el-table__body tr:hover > td.el-table__cell) {
  background: rgba(56, 189, 248, 0.1) !important;
}

@media (max-width: 1100px) {
  .page-head,
  .desk-grid,
  .lower-grid,
  .trade-journal-grid,
  .status-band,
  .summary-band,
  .submit-summary,
  .preflight-grid,
  .account-summary,
  .weekly-summary,
  .signal-overview {
    grid-template-columns: 1fr;
  }

  .actions,
  .table-actions {
    justify-content: flex-start;
  }
}
</style>
