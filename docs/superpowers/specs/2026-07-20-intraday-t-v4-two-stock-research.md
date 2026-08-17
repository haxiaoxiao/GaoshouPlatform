# 日内做 T v4 两股探索性研究

日期：2026-07-20

## 1. 结论

v4 继续只研究 `603629.SH`（利通电子）和 `688008.SH`（澜起科技）。没有扩大
股票池，没有修改页面、模拟盘或实盘参数，也没有接入真实下单。

三个新增判断能过滤部分坏交易，但没有形成两股共同有效、样本充分且成本稳健的证据。
量价在线预测在 10bp/边下仅余 `+43.40` 元，只有 29 对，并且 603629.SH 仍亏损。
最终决策固定为 `research_only`。

## 2. 源头与边界

- [Llorente et al. 2002](https://web.mit.edu/wangj/Public/Publication/Llorente-Michaely-Saar-Wang02.pdf)
  表明量价关系取决于交易动机，放量并不天然意味着反转。因此这里只在线估计方向，
  不硬编码“放量必反转”。
- [Lee and Mykland 2008](https://galton.uchicago.edu/~mykland/paperlinks/LeeMykland-2535.pdf)
  使用跳跃前的双幂变差估计局部尺度。这里进一步用固定指数区分个股跳跃与市场共跳。
- [Amihud 2002](https://www.cis.upenn.edu/~mkearns/finread/amihud.pdf) 将绝对收益/成交额
  作为粗略价格冲击。这里仅将其用作高冲击否决，不把它解释为盘口或订单流。
- A 股研究同时观察到日内动量、反转和交易成本约束，不能把海外日频结果直接当成分钟
  盈利证据。参考 [Chu, Gu and Zhou 2019](https://www.sciencedirect.com/science/article/pii/S1544612318307414)
  与 [Kang, Lin and Xiong 2022](https://www.sciencedirect.com/science/article/pii/S0165188922000185)。

本地没有覆盖该窗口的历史逐笔成交、盘口和委托队列，因此没有伪造 OFI、microprice、
queue imbalance 或 VPIN。所有新增特征只使用真实股票/指数一分钟 OHLCVA。

## 3. 固定协议

### 数据与执行

- 窗口：2024-07-19 至 2026-03-13，共 398 个完整共同交易日。
- 股票：两股各 `95,520 = 398 * 240` bars；固定指数 `000001.SH`、`000688.SH`
  各 95,520 bars。
- 底仓：603629.SH 为 2,000 股，688008.SH 为 1,000 股。
- 精确涨跌停：796/796 个股票日；缺失、重复、非完整分钟网格均失败关闭。
- 折次：252 日预热后依次测试 42、42、62 日。
- 信号在分钟 `t` 收盘后形成，只能在 `t+1` 开盘成交。
- 成本：固定名义决策成本 2bp；执行分别为 2bp、5bp、10bp，并单独检查 2.5%
  bar 成交量参与率。

### 四个独立变体

1. `directional_move_0_100`：v3 锚点，要求
   `0 <= sign(z) * session_return_bps < 100`。
2. `volume_return_forecast`：成交额先用此前 20 日同分钟中位数/MAD 标准化；每天开盘前
   仅用此前 60 个完整交易日拟合
   `next_residual = c + b1 * residual + b2 * amount_z * residual`，预测方向必须与当前
   `z` 相反。收益和目标均在上午/下午各自重置。
3. `idiosyncratic_jump_veto`：股票与固定指数分别用当前 bar 之前 20 根同交易时段收益的
   双幂尺度计算跳跃分数；最近 10 分钟出现“个股跳、指数不跳”则禁止新入场。
4. `amihud_impact`：先算残差收益/真实成交额，在同一上午或下午取 5 bar 中位数，
   `log1p` 后按此前 20 日同分钟中位数/MAD 标准化，只允许 `impact_z <= 1.5`。

四个变体分别相对同一锚点检查，不做门控组合或参数搜索。门控只阻止新入场，不得阻断
活动配对恢复。

## 4. 冻结回放结果

| 变体 | 名义净收益 | 对数 | 5bp | 10bp | 2.5% 容量 | 容量对数 | 603629.SH | 688008.SH |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 方向位移锚点 | +1,038.90 | 74 | +105.37 | -1,450.49 | +1,195.53 | 71 | -519.12 | +1,558.01 |
| 量价在线预测 | +1,181.44 | 29 | +754.67 | +43.40 | +1,356.44 | 27 | -147.52 | +1,328.96 |
| 个股独立跳跃否决 | +1,137.71 | 71 | +226.24 | -1,292.87 | +1,274.34 | 68 | -420.30 | +1,558.01 |
| 5 分钟 Amihud 冲击 | +1,119.38 | 72 | +218.92 | -1,281.84 | +1,312.85 | 68 | -476.42 | +1,595.80 |

量价在线预测是唯一在 10bp 下未转负的新增门控，但 `+43.40` 元接近成本误差量级，
只在 1/3 折优于方向锚点，603629.SH 仍为负，且每股样本分别只有 11/18 对。容量场景
收益更高来自拒绝了部分交易，不能解释为容量越紧收益越高，也不能替代固定成本压力。

## 5. 完整性与决策

- 完成 `3 折 * 4 变体 * 4 场景 = 48` 个唯一运行。
- 2/5/10bp 对每个折次/变体的信号账本 SHA-256 完全一致；容量场景允许因成交上限改变。
- 48 个运行均为零期末未平配对、零恢复失败、恢复率 100%。
- `runs.csv` SHA-256：`5b258e8e10c4d02ca555da12fa01d4a9a2a151490289cbb5d934e98bc4183d4a`。
- `signal_ledger.csv` SHA-256：`54be1674c25b70ea5bbc2481ccdb5cfdb249d9bbd398de59429a689a816ca737`。
- 实现指纹：`33264baa44ad7668a9f957bd877f8a70a96a15cdabb70bb4d0db1e673a8f1b7a`。

所有可用历史均已参与假设形成，不能再称为未见样本。四个变体都没有解决 603629.SH
持续为负的问题，样本也不足以支持单股部署。因此不修改当前策略默认值，不接入模拟盘
自动运行，更不开放真实提交；只保留量价预测作为后续新数据的 shadow 观察项。

## 6. 产物与复跑

- `.runtime/intraday-t-v4-research/2024-07-19_2026-03-13/research.json`
- `.runtime/intraday-t-v4-research/2024-07-19_2026-03-13/runs.csv`
- `.runtime/intraday-t-v4-research/2024-07-19_2026-03-13/signal_ledger.csv`
- `backend/app/scripts/research_intraday_t_v4.py`

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m app.scripts.research_intraday_t_v4 `
  --start-date 2024-07-19 --end-date 2026-03-13 `
  --limit-price-db E:\Projects\data\BaiduSyncdisk\gaoshou.db `
  --output-dir ..\.runtime\intraday-t-v4-research\2024-07-19_2026-03-13
```
