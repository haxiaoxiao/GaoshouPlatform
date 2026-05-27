# Tushare Relay 探查报告（2026-05-25）

- 来源页面: https://ai-tool.indevs.in/quant/tushare-pro-catalog/
- 鉴权: 使用用户提供的 Indevs Relay 专用 API Key，通过 `X-API-Key` 请求；报告不保存 key 明文。
- 探测域名: `https://ai-tool.indevs.in/tushare/pro`，失败时回退 `https://tushare.indevs.in/tushare/pro`。
- 探测方式: 只读 GET；将大 `limit` 压到 10 以内，`contract_limit` 压到 5，避免拉取大数据。

## 总览

- 覆盖: 99 次探查，98 个唯一 API；其中 `stk_auction_replay` 额外测了 `timeline` 变体。
- 成功有数据: 81
- 成功但样例无数据: 7
- 超时或当前不可用: 11

## 重点接口

| 接口 | 状态 | 返回数 | 字段 | 样例/结论 |
|---|---:|---:|---|---|
| `stk_auction_replay` | OK | 1 | `ts_code, trade_date, trade_time, start_time, end_time, freq, open, high, ...(+12)` | ts_code=000001.SZ; trade_date=20260327; trade_time=2026-03-27 09:25:00; start_time=2026-03-27 09:15:00; end_time=2026-03-27 09:30:00; freq=AUCTION |
| `stk_auction_replay_timeline` | OK-空 | 0 | `` |  |
| `adj_factor` | OK | 1 | `ts_code, trade_date, adj_factor` | ts_code=000001.SZ; trade_date=20240506; adj_factor=0.8672381936553577 |
| `moneyflow` | OK | 1 | `ts_code, trade_date, buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount, buy_md_vol, buy_md_amount, ...(+12)` | ts_code=000001.SZ; trade_date=20240506; buy_sm_vol=; buy_sm_amount=; sell_sm_vol=; sell_sm_amount=951.5104 |
| `ths_index` | OK | 200 | `ts_code, name, count, exchange, list_date, type` | ts_code=883300.TI; name=沪深300样本股; count=300; exchange=A; list_date=20100413; type=N；复测 `limit=1000` 可返回 409 条。 |
| `ths_member` | OK | 32 | `ts_code, con_code, name, weight, in_date, out_date, is_new` | ts_code=885573.TI; con_code=000048.SZ; name=; weight=; in_date=; out_date= |
| `block_moneyflow` | OK | 5 | `trade_date, ts_code, name, lead_stock, close_price, pct_change, industry_index, company_num, ...(+4)` | trade_date=20260525; ts_code=307816; name=国家大基金持股; lead_stock=华虹公司; close_price=214.8; pct_change=5.56；`limit=5` 稳定，较大 limit 有超时/空返回。 |

## 全量接口状态

### 公告与研报

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `anns_d` | OK | 1 | `native_envelope` | `ts_code, symbol, name, title, ann_type, ann_date, ann_time, url` | ts_code=000001.SZ; symbol=000001; name=平安银行; title=关于独立董事任职资格核准的公告; ann_type=; ann_date=20260326 |
| `opt_basic` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: non_json: <html> <head><title>404 Not Found</title></head> <body> <center><h1>404 |
| `opt_daily` | OK | 10 | `list_data` | `ts_code, trade_date, exchange, pre_settle, pre_close, open, high, low, ...(+5)` | ts_code=10001313.SH; trade_date=20181212; exchange=SSE; pre_settle=0.0311; pre_close=0.0312; open=0.0355 |
| `opt_mins` | OK | 10 | `list_data` | `ts_code, trade_time, open, close, high, low, vol, amount, ...(+1)` | ts_code=PP2703-P-7800.DCE; trade_time=2026-04-10 15:00:00; open=1065.5; close=1065.5; high=1065.5; low=1065.5 |
| `report_rc` | OK-空 | 0 | `native_envelope` | `ts_code, name, report_date, report_title, report_type, classify, org_name, author_name, ...(+15)` | 接口可达，本样例无行。 |
| `research_report` | OK | 10 | `native_envelope` | `股票代码, 股票简称, 报告名称, 东财评级, 机构, 日期, 报告PDF链接` | 股票代码=000001; 股票简称=平安银行; 报告名称=2025年报及2026一季报点评：收入利润增速均回正; 东财评级=中性; 机构=国信证券; 日期=2026-04-26 |
| `stock_notice_report` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: non_json: <html> <head><title>404 Not Found</title></head> <body> <center><h1>404 |
| `stock_research_report_em` | OK | 10 | `native_envelope` | `股票代码, 股票简称, 报告名称, 东财评级, 机构, 日期, 报告PDF链接` | 股票代码=000001; 股票简称=平安银行; 报告名称=2025年报及2026一季报点评：收入利润增速均回正; 东财评级=中性; 机构=国信证券; 日期=2026-04-26 |
| `stock_zh_a_disclosure_report_cninfo` | OK | 10 | `native_envelope` | `代码, 简称, 公告标题, 公告时间, 公告链接` | 代码=000001; 简称=平安银行; 公告标题=关于独立董事任职资格核准的公告; 公告时间=2026-03-26; 公告链接=http://www.cninfo.com.cn/new/disclosure/detail?stockCode=000001&announcementId=1225032132&orgId=gssz0000001&announcementTime=2026-03-26 |

### 其他

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `catalog` | OK-空 | 0 | `native_envelope` | `` | 接口可达，本样例无行。 |

### 分析师数据

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `analyst_detail` | OK | 10 | `native_envelope` | `股票代码, 股票名称, 调入日期, 最新评级日期, 当前评级名称, 最新价格, 阶段涨跌幅` | 股票代码=301608; 股票名称=博实结; 调入日期=2026-04-24; 最新评级日期=2026-04-24; 当前评级名称=买入; 最新价格=63.73 |
| `analyst_history` | OK | 10 | `native_envelope` | `股票代码, 股票名称, 调入日期, 调出日期, 调入时评级名称, 调出原因, 累计涨跌幅` | 股票代码=688256; 股票名称=寒武纪; 调入日期=2024-05-21; 调出日期=2024-11-21; 调入时评级名称=买入; 调出原因=到期调出 |
| `analyst_rank` | OK | 10 | `native_envelope` | `分析师名称, 分析师单位, 年度指数, 12个月收益率, 分析师ID, 行业, 更新日期, 年度` | 分析师名称=任志强; 分析师单位=华福证券; 年度指数=6424.01; 12个月收益率=135.17; 分析师ID=11000213851; 行业=电子 |

### 分钟数据

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `a_share_level2` | OK-空 | 0 | `list_data` | `` | 接口可达，本样例无行。 |
| `a_share_mins` | OK | 48 | `list_data` | `ts_code, freq, time, open, high, low, close, vol, ...(+1)` | ts_code=000001.SZ; freq=5MIN; time=2026-05-22 15:00:00; open=10.66; high=10.66; low=10.66 |
| `fut_level2` | OK-空 | 0 | `list_data` | `` | 接口可达，本样例无行。 |
| `minute_latest_batch` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: non_json: <html> <head><title>404 Not Found</title></head> <body> <center><h1>404 |
| `opt_mins_batch` | OK | 50 | `list_data` | `ts_code, trade_time, open, close, high, low, vol, amount, ...(+1)` | ts_code=RM703P2250.ZCE; trade_time=2026-04-10 23:00:00; open=127.0; close=127.0; high=127.0; low=127.0 |
| `rt_fut_level2` | OK-空 | 0 | `list_data` | `` | 接口可达，本样例无行。 |
| `rt_fut_min` | OK | 1 | `native_envelope` | `ts_code, freq, time, open, high, low, close, vol, ...(+5)` | ts_code=RB0; freq=10MIN; time=2026-05-25 21:30:00; open=3188.0; high=3190.0; low=3181.0 |
| `rt_fut_min_daily` | OK | 29 | `native_envelope` | `ts_code, freq, time, open, high, low, close, vol, ...(+5)` | ts_code=RB0; freq=10MIN; time=2026-05-25 09:10:00; open=3224.0; high=3234.0; low=3168.0 |
| `rt_fut_min_health` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: curl_rc_28: curl: (28) Operation timed out after 55014 milliseconds with 0 bytes  |
| `rt_fut_ticks` | OK | 1 | `list_data` | `ts_code, trade_date, time, price, vol, amount, hold, bid_price, ...(+1)` | ts_code=RB0; trade_date=20260526; time=2026-05-26 21:42:56; price=3189.0; vol=399671.0; amount=0.0 |
| `rt_k` | OK | 1 | `native_envelope` | `ts_code, trade_time, trade_date, open, high, low, close, pre_close, ...(+5)` | ts_code=000001.SZ; trade_time=; trade_date=20260525; open=10.68; high=10.76; low=10.64 |
| `rt_min` | OK | 1 | `list_data` | `ts_code, freq, time, open, high, low, close, vol, ...(+5)` | ts_code=000001.SZ; freq=5MIN; time=2026-05-25 15:00:00; open=10.67; high=10.67; low=10.67 |
| `rt_min_daily` | OK | 48 | `list_data` | `code, freq, time, open, high, low, close, vol, ...(+1)` | code=000001.SZ; freq=5MIN; time=2026-05-25 09:35:00; open=10.68; high=10.69; low=10.66 |

### 基金数据

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `fund_announcement_report_em` | OK | 10 | `native_envelope` | `基金代码, 基金名称, 公告标题, 公告日期, 报告ID` | 基金代码=000001; 基金名称=华夏成长混合; 公告标题=华夏成长：2012年半年度报告摘要; 公告日期=2012-08-28; 报告ID=AN201208280005468376 |

### 外汇

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `fx_daily` | OK | 6 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=USDCNH.FXCM; trade_date=20260525; open=6.7892; high=6.7905; low=6.7795; close=6.7847 |
| `fx_mins` | OK | 10 | `native_envelope` | `ts_code, freq, time, open, high, low, close, vol, ...(+1)` | ts_code=USDCNH.FXCM; freq=5MIN; time=2026-05-25 21:35:25; open=6.7855000495910645; high=6.7855000495910645; low=6.7855000495910645 |
| `fx_quote` | OK | 1 | `native_envelope` | `ts_code, trade_time, open, high, low, price, last, pre_close, ...(+5)` | ts_code=USDCNH.FXCM; trade_time=2026-05-25 21:35:27; open=6.7893; high=6.7925; low=6.7803; price=6.7857 |

### 指数数据

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `index_basic` | OK | 9 | `native_envelope` | `ts_code, name, fullname, exchange, market, publisher, index_type, category, ...(+7)` | ts_code=DJI.US; name=Dow Jones Industrial Average; fullname=Dow Jones Industrial Average; exchange=US; market=OTH; publisher=itick |
| `index_daily` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: non_json: <html> <head><title>404 Not Found</title></head> <body> <center><h1>404 |
| `index_monthly` | OK | 74 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=HSI; trade_date=20260228; open=27097.3398; high=27397.6504; low=26295.0293; close=26630.5391 |
| `index_weekly` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: non_json: <html> <head><title>404 Not Found</title></head> <body> <center><h1>404 |
| `index_weight` | OK | 300 | `native_envelope` | `index_code, trade_date, con_code, weight` | index_code=000001.SH; trade_date=2026-04-30; con_code=600000.SH; weight=0.472 |

### 新闻数据

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `cjzc` | OK | 10 | `native_envelope` | `title, summary, pub_time, url, src` | title=东方财富财经早餐 5月25日周一; summary=【东方财富财经早餐 5月25日周一】1、特朗普：美伊协议“尚未完全谈妥”。2、国务院印发《关于推行常住地提供基本公共服务的实施意见》。3、央行今日将开展6000亿元MLF操作，本月加量续做。; pub_time=2026-05-25 06:00:38; url=http://finance.eastmoney.com/a/202605243747519567.html; src=cjzc |
| `express_news` | OK | 10 | `native_envelope` | `title, content, datetime, src` | title=; content=财联社5月25日电，意大利10年期国债收益率跌至3月18日以来的最低点，报3.648%。; datetime=2026-05-25 20:57:52; src=express |
| `gdelt_industry_daily_timeline` | 超时/不可用 | 0 | `empty_or_scalar` | `` | https://ai-tool.indevs.in/tushare/pro: non_json: <html> <head><title>404 Not Found</title></head> <body> <center><h1>404 |
| `major_news` | OK | 307 | `native_envelope` | `title, pub_time, src, url` | title=时隔两个月投资额激增超四倍 康众医疗大手笔加码超声AI领域|速读公告; pub_time=2026-04-14 23:50:40; src=财联社; url=https://www.cls.cn/detail/2344135 |
| `news` | OK-空 | 0 | `native_envelope` | `datetime, content, title, channels` | 接口可达，本样例无行。 |
| `news_cctv` | OK | 1 | `native_envelope` | `date, title, content` | date=20260326; title=【新思想引领新征程】高标准建设海南自由贸易港 助力全国构建新发展格局; content=习近平总书记指出，建设海南自由贸易港的战略目标，就是要把海南自由贸易港打造成为引领我国新时代对外开放的重要门户。随着全岛封关运作稳步推进，海南自由贸易港政策红利持续释放，高水平开放和高质量发展不断呈现崭新气象，汇聚起以开放合作实现发展共赢的强劲动能。潮起海之南，风正好扬帆。海南自由贸易港全岛封关运作以来，口岸通关繁忙有序，经贸往来愈发活跃，市场主体信心倍增，一幅高水平对外开放的生动画卷在南海之滨徐徐铺展。建设海南自由贸易港，是习近平总书记亲自谋划、亲自部署、亲自推动的重大国家战略。党的十八大以来，他多次来到海南考察调研，为高质量发展把脉定向、擘画蓝图。2018年4月，在庆祝海南建省办经济特区30周年大会上，习近平总书记宣布，党中央决定支持海南全岛建设自由贸易试验区，支持海南逐步探索、稳步推进中国特色自由贸易港建设。在习近平总书记的指引推动下，《海南自由贸易港建设总体方案》对外公布，海南自由贸易港法颁布实施，海南自由贸易港建设整体推进蹄疾步稳、有力有序。2025年11月，习近平总书记在听取海南自由贸易港建设工作汇报时强调，建设海南自由贸易港的战略目标，就是要把海南自由贸易港打造成为引领我国新时代对外开放的重要门户。2025年12月18日，海南自由贸易港正式启动全岛封关，这是我国坚定不移扩大高水平对外开放、推动建设开放型世界经济的标志性举措。数据显示，自封关至今年2月底，海南货物贸易进出口654.9亿元，同比增长29.1%。其中，今年1月、2月的外贸规模均创历史同期最高。与此同时，市场活力有效激发。封关以来，新增经营主体8.25万户。其中，新增企业6.79万户，同比增长87.18%；新增外资企业693户，同比增长33.5%。“十五五”规划纲要就“扩大高水平对外开放，开创合作共赢新局面”作出重要部署，提出高标准建设海南自由贸易港，高水平实施全岛封关运作，持续提升贸易投资和要素流动等重点领域开放水平，逐步构建与高水平自由贸易港相适应的政策制度体系。开局之年，海南推动自由贸易港政策红利加速释放。当前，“零关税”商品增至约6600个，“零关税”水平达到74%。今年，海南还将研究推动调整优化进口征税商品目录，完善加工增值免关税政策。前不久，海南岛内居民消费进境商品“零关税”政策和离岛免税新政先后落地，让居民在家门口享受实惠免税购物。开局之年，海南稳步扩大制度型开放。目前，已培育形成22批181项制度集成创新案例。今年，海南还将推动缩减跨境服务贸易负面清单，放宽旅游、医疗、交通、金融等重点领域市场准入。如今，包括医疗器械、集装箱、高端电机等在内的38个商品编码产品，可在这里开展“两头在外”保税维修业务。现在，8个“一线口岸”平均通关时间较封关前压缩26%，10个“二线口岸”采取“分批出岛、集中申报”、智慧监管等高效模式，有力保障了国内国际双循环高效衔接。截至目前，海南自由贸易港累计吸引了180个国家和地区投资，对共建“一带一路”国家进出口占比超61%，对多个新兴市场进出口实现快速增长。在开放中分享机遇，实现共赢。奋进“十五五”，海南自由贸易港在推进高水平对外开放中不断发挥牵引作用，助力全国构建新发展格局。 |
| `news_economic_baidu` | OK | 10 | `native_envelope` | `日期, 时间, 地区, 事件, 公布, 预期, 前值, 重要性` | 日期=2026-03-26; 时间=00:00; 地区=俄罗斯; 事件=俄罗斯2月工业产值年率(%); 公布=-0.9; 预期=1.1 |
| `news_report_time_baidu` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: non_json: <html> <head><title>404 Not Found</title></head> <body> <center><h1>404 |
| `news_trade_notify_dividend_baidu` | OK | 3 | `native_envelope` | `股票代码, 股票简称, 交易所, 除权日, 分红, 送股, 转增, 实物, ...(+1)` | 股票代码=02378; 股票简称=保诚; 交易所=HK; 除权日=2026-03-26; 分红=0.19港元; 送股=- |
| `news_trade_notify_suspend_baidu` | OK | 10 | `native_envelope` | `股票代码, 股票简称, 交易所代码, 停牌时间, 复牌时间, 停牌事项说明, 市值, 公告日期, ...(+4)` | 股票代码=874661; 股票简称=宸芯科技; 交易所代码=NQ; 停牌时间=2026-03-26; 复牌时间=; 停牌事项说明=近期公司股价出现较大波动,停牌核查。 |
| `sge_daily` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: curl_rc_28: curl: (28) Operation timed out after 55014 milliseconds with 0 bytes  |

### 本地量化聚合接口

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `get_all_securities` | OK | 5 | `list_data` | `ts_code, symbol, name, market, snapshot_date` | ts_code=000001.SZ; symbol=000001; name=平安银行; market=SZ; snapshot_date=20260525 |
| `get_index_stocks` | OK | 5 | `list_data` | `index_symbol, con_code, con_name, in_date` | index_symbol=000300.SH; con_code=002625.SZ; con_name=光启技术; in_date=2025-12-15 |
| `get_index_weights` | OK | 5 | `list_data` | `index_symbol, trade_date, index_name, con_code, con_name, weight` | index_symbol=000300.SH; trade_date=2026-04-30; index_name=沪深300; con_code=000001.SZ; con_name=平安银行; weight=0.425 |
| `get_industries` | OK | 5 | `list_data` | `industry_code, industry_name, parent_industry, level, constituent_count, pe, pe_ttm, pb, ...(+1)` | industry_code=801010.SI; industry_name=农林牧渔; parent_industry=; level=L1; constituent_count=104; pe=22.87 |
| `get_industry_stocks` | OK | 5 | `list_data` | `industry_code, con_code, con_name, weight, in_date` | industry_code=801780.SI; con_code=002142.SZ; con_name=宁波银行; weight=4.2275; in_date=2021-12-13 |
| `get_realtime_prices` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: curl_rc_28: curl: (28) Operation timed out after 55008 milliseconds with 0 bytes  |
| `get_stock_chinese_name` | OK | 2 | `list_data` | `ts_code, symbol, name` | ts_code=000001.SZ; symbol=000001; name=平安银行 |
| `get_trade_days` | OK | 5 | `list_data` | `trade_date` | trade_date=2026-03-25T00:00:00 |

### 板块数据

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `block_moneyflow` | OK | 5 | `native_envelope` | `trade_date, ts_code, name, lead_stock, close_price, pct_change, industry_index, company_num, ...(+4)` | trade_date=20260525; ts_code=307816; name=国家大基金持股; lead_stock=华虹公司; close_price=214.8; pct_change=5.56 |
| `dc_index_prev` | OK | 500 | `list_data` | `ts_code, trade_date, name, leading, leading_code, pct_change, leading_pct, total_mv, ...(+3)` | ts_code=BK1659.DC; trade_date=20260326; name=精准诊断; leading=ST天瑞; leading_code=300165.SZ; pct_change=-1.91 |

### 港股

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `hk_adj_factor` | OK | 1000 | `native_envelope` | `ts_code, trade_date, adj_factor` | ts_code=00700.HK; trade_date=20260522; adj_factor=1.0 |
| `hk_basic` | OK | 1 | `native_envelope` | `ts_code, symbol, name, fullname, enname, cn_spell, exchange, market, ...(+8)` | ts_code=00700.HK; symbol=00700; name=Tencent Holdings Limited; fullname=Tencent Holdings Limited; enname=Tencent Holdings Limited; cn_spell= |
| `hk_daily` | OK | 1000 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=00700.HK; trade_date=20260522; open=442.2; high=445.0; low=438.8; close=441.4 |
| `hk_depth` | OK | 1 | `native_envelope` | `ts_code, bid_price1, bid_vol1, bid_order1, bid_price2, bid_vol2, bid_order2, bid_price3, ...(+23)` | ts_code=00700.HK; bid_price1=466.0; bid_vol1=0.0; bid_order1=0.0; bid_price2=466.0; bid_vol2=0.0 |
| `hk_hold` | OK | 1 | `native_envelope` | `code, trade_date, ts_code, name, vol, ratio, exchange` | code=00700; trade_date=20260320; ts_code=00700.HK; name=腾讯控股; vol=1084690611.0; ratio=11.88 |
| `hk_mins` | OK | 8 | `native_envelope` | `ts_code, freq, time, open, high, low, close, vol, ...(+1)` | ts_code=00700.HK; freq=5MIN; time=2026-05-22 16:05:00; open=441.3999938964844; high=441.3999938964844; low=441.3999938964844 |
| `hk_monthly` | OK | 74 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=00700.HK; trade_date=20260228; open=598.0; high=604.5; low=510.5; close=518.0 |
| `hk_quote` | OK | 1 | `native_envelope` | `ts_code, trade_time, open, high, low, price, last, pre_close, ...(+5)` | ts_code=00700.HK; trade_time=2026-05-22 16:05:00; open=442.6000061035156; high=445.0; low=438.79998779296875; price=441.3999938964844 |
| `hk_weekly` | OK | 65 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=00700.HK; trade_date=20260327; open=497.8; high=521.5; low=487.6; close=493.4 |
| `rt_hk_k` | OK | 1 | `native_envelope` | `ts_code, trade_time, trade_date, open, high, low, close, pre_close, ...(+5)` | ts_code=00700.HK; trade_time=2026-05-06 10:45:52; trade_date=20260506; open=470.6; high=473.4; low=464.0 |

### 美股

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `us_adj_factor` | OK | 100 | `native_envelope` | `ts_code, trade_date, adj_factor, split_ratio, name` | ts_code=AAPL; trade_date=20260508; adj_factor=1.0; split_ratio=; name= |
| `us_basic` | OK | 1 | `native_envelope` | `ts_code, name, classify, list_date, delist_date` | ts_code=AAPL; name=Apple Inc.; classify=EQ; list_date=19801212; delist_date= |
| `us_daily` | OK | 1000 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=AAPL; trade_date=20260522; open=306.06; high=311.4; low=305.85; close=308.82 |
| `us_daily_market_cap` | OK | 1000 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+9)` | ts_code=AAPL; trade_date=20260522; open=306.06; high=311.4; low=305.85; close=308.82 |
| `us_depth` | OK | 1 | `native_envelope` | `ts_code, bid_price1, bid_vol1, bid_order1, bid_price2, bid_vol2, bid_order2, bid_price3, ...(+23)` | ts_code=AAPL; bid_price1=308.7582433227539; bid_vol1=; bid_order1=; bid_price2=308.6964793212891; bid_vol2= |
| `us_mins` | OK | 10 | `native_envelope` | `ts_code, freq, time, open, high, low, close, vol, ...(+1)` | ts_code=AAPL; freq=5MIN; time=2026-05-23 04:00:00; open=308.80999755859375; high=309.3999938964844; low=308.6600036621094 |
| `us_monthly` | OK | 74 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=AAPL; trade_date=20260228; open=260.03; high=280.91; low=255.45; close=264.18 |
| `us_quote` | OK | 1 | `native_envelope` | `ts_code, trade_time, open, high, low, price, last, pre_close, ...(+5)` | ts_code=AAPL; trade_time=2026-05-23 04:00:00; open=309.98; high=310.52; low=307.97; price=308.82 |
| `us_tradecal` | 超时/不可用 |  | `non_json` | `` | https://ai-tool.indevs.in/tushare/pro: non_json: <html> <head><title>404 Not Found</title></head> <body> <center><h1>404 |
| `us_weekly` | OK | 65 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=AAPL; trade_date=20260327; open=253.97; high=257.0; low=248.07; close=248.8 |

### 股票

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `adj_factor` | OK | 1 | `list_data` | `ts_code, trade_date, adj_factor` | ts_code=000001.SZ; trade_date=20240506; adj_factor=0.8672381936553577 |
| `daily_basic` | OK | 1 | `native_envelope` | `ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, ...(+10)` | ts_code=000002.SZ; trade_date=20260424; close=3.75; turnover_rate=; turnover_rate_f=; volume_ratio= |
| `moneyflow` | OK | 1 | `native_envelope` | `ts_code, trade_date, buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount, buy_md_vol, buy_md_amount, ...(+12)` | ts_code=000001.SZ; trade_date=20240506; buy_sm_vol=; buy_sm_amount=; sell_sm_vol=; sell_sm_amount=951.5104 |
| `stk_auction_replay` | OK | 1 | `list_data` | `ts_code, trade_date, trade_time, start_time, end_time, freq, open, high, ...(+12)` | ts_code=000001.SZ; trade_date=20260327; trade_time=2026-03-27 09:25:00; start_time=2026-03-27 09:15:00; end_time=2026-03-27 09:30:00; freq=AUCTION |
| `stk_auction_replay_timeline` | OK-空 | 0 | `list_data` | `` | 接口可达，本样例无行。 |
| `stk_factor` | OK | 1 | `native_envelope` | `ts_code, trade_date, close, vol, ma5, ma10, ma20, ma60, ...(+12)` | ts_code=000002.SZ; trade_date=20240506; close=7.46; vol=5244937.88; ma5=7.17; ma10=6.923 |
| `stock_basic` | OK | 1 | `native_envelope` | `ts_code, symbol, name, area, industry, list_date` | ts_code=000001.SZ; symbol=000001; name=平安银行; area=; industry=; list_date= |

### 贵金属

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `metal_daily` | OK | 1000 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=XAUUSD.FXCM; trade_date=20260525; open=4532.0; high=4582.60009765625; low=4531.2998046875; close=4523.2001953125 |
| `metal_depth` | OK | 1 | `native_envelope` | `ts_code, bid_price1, bid_vol1, bid_order1, bid_price2, bid_vol2, bid_order2, bid_price3, ...(+23)` | ts_code=XAUUSD.FXCM; bid_price1=; bid_vol1=; bid_order1=; bid_price2=; bid_vol2= |
| `metal_mins` | OK | 10 | `native_envelope` | `ts_code, freq, time, open, high, low, close, vol, ...(+1)` | ts_code=XAUUSD.FXCM; freq=5MIN; time=2026-05-25 11:55:00; open=; high=; low= |
| `metal_monthly` | OK | 74 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=XAUUSD.FXCM; trade_date=20260228; open=4807.7001953125; high=5280.0; low=4400.0; close=5230.5 |
| `metal_obasic` | OK | 1 | `native_envelope` | `ts_code, name, exchange, market, classify, base_currency, quote_currency` | ts_code=XAUUSD.FXCM; name=XAU; exchange=FXCM; market=GB; classify=precious_metal; base_currency=XAU |
| `metal_quote` | OK | 1 | `native_envelope` | `ts_code, trade_time, open, high, low, price, last, pre_close, ...(+5)` | ts_code=XAUUSD.FXCM; trade_time=2026-05-25 11:55:00; open=; high=; low=; price= |
| `metal_weekly` | OK | 65 | `native_envelope` | `ts_code, trade_date, open, high, low, close, pre_close, change, ...(+3)` | ts_code=XAUUSD.FXCM; trade_date=20260327; open=4353.0; high=4551.89990234375; low=4100.7998046875; close=4492.0 |

### 高频兜底接口

| API | 状态 | count | schema | 字段摘要 | 备注 |
|---|---:|---:|---|---|---|
| `balancesheet` | OK | 1 | `native_envelope` | `ts_code, ann_date, f_ann_date, end_date, report_type, comp_type, end_type, total_share, ...(+150)` | ts_code=000001.SZ; ann_date=20260425; f_ann_date=20260425; end_date=20260331; report_type=; comp_type= |
| `cashflow` | OK | 1 | `native_envelope` | `ts_code, ann_date, f_ann_date, end_date, comp_type, report_type, end_type, net_profit, ...(+89)` | ts_code=000001.SZ; ann_date=20260425; f_ann_date=20260425; end_date=20260331; comp_type=; report_type= |
| `concept_board` | OK | 5 | `native_envelope` | `code, name, trade_date, rank, latest, change, pct_change, total_mv, ...(+5)` | code=BK1101; name=先进封装; trade_date=20260525; rank=1; latest=3704.15; change=207.4 |
| `dividend` | OK | 35 | `native_envelope` | `ts_code, end_date, ann_date, div_proc, stk_div, stk_bo_rate, stk_co_rate, cash_div, ...(+8)` | ts_code=000001.SZ; end_date=20251231; ann_date=20260321; div_proc=股东大会通过; stk_div=0.0; stk_bo_rate= |
| `income` | OK | 1 | `native_envelope` | `ts_code, ann_date, f_ann_date, end_date, report_type, comp_type, end_type, basic_eps, ...(+86)` | ts_code=000001.SZ; ann_date=20260425; f_ann_date=20260425; end_date=20260331; report_type=; comp_type= |
| `moneyflow_hsgt` | OK | 1 | `native_envelope` | `trade_date, ggt_ss, ggt_sz, hgt, sgt, north_money, south_money` | trade_date=20260320; ggt_ss=29249.39; ggt_sz=23752.58; hgt=152840.82; sgt=186247.24; north_money=339088.06 |
| `ths_index` | OK | 200 | `native_envelope` | `ts_code, name, count, exchange, list_date, type` | ts_code=883300.TI; name=沪深300样本股; count=300; exchange=A; list_date=20100413; type=N |
| `ths_member` | OK | 32 | `native_envelope` | `ts_code, con_code, name, weight, in_date, out_date, is_new` | ts_code=885573.TI; con_code=000048.SZ; name=; weight=; in_date=; out_date= |
| `top_list` | OK | 64 | `native_envelope` | `trade_date, ts_code, name, close, pct_change, turnover_rate, amount, l_sell, ...(+7)` | trade_date=20260320; ts_code=920641.BJ; name=格利尔; close=20.68; pct_change=14.0022; turnover_rate=23.328 |

## 接入建议

- 第一批优先接入: `adj_factor`, `moneyflow`, `ths_index`, `ths_member`, `block_moneyflow`, `stk_auction_replay`。这些正好覆盖复权、个股资金流、同花顺板块、板块成分、板块资金流和集合竞价。
- `moneyflow`, `ths_index`, `ths_member`, `block_moneyflow` 返回 native envelope: `data.fields` + `data.items`；接入时不要假设 `data` 一定是 list。
- `stk_auction_replay` 的 `summary` 样例有数据，`timeline` 样例为空；建议落库时支持两种 mode，并把 `source`, `source_api`, `data_level`, `is_complete` 保留下来。
- `block_moneyflow` 小 limit 可用，大 limit 当前不稳定；同步任务应按小批量拉取并重试。
- 对 `timeout_or_network_error` 的接口，先不要作为核心依赖；多数是 relay 路由/上游稳定性问题，不是 key 问题。

## 频率限制与限流风险

- Tushare 官方接口不是无限频率。官方说明把常规接口按用户积分分成不同权限和频次，例如 120 分以下每分钟 50 次，120 分及以上每分钟 500 次，积分越高每日总量越大；分钟线、港美股、公告新闻、报告、期权、期货等属于独立权限，通常还有单独的频次和日总量。
- Relay 当前响应头没有暴露 `X-RateLimit-*` 这类剩余配额字段；`adj_factor` 样例响应头出现 `x-api-relay-cache: HIT`、`x-tushare-cache-bucket: history`，说明部分历史数据会命中 Relay 缓存，不一定每次穿透官方 Tushare。
- 小规模连续请求验证：对 `adj_factor` 做 20 次、间隔 0.2 秒的只读请求，没有观察到 429；但两个 Relay 域名都出现少量 404 路由抖动。这个测试只能说明低频历史缓存请求暂未触发 Relay 限流，不能证明可高并发抓取。
- 推荐默认策略：全局限速 1-2 req/s，单接口串行或低并发；遇到 `429/500/502/503/504/relay_pending/timeout` 使用指数退避，初始 2 秒，最多重试 3 次；每日批量任务按接口和日期 checkpoint，避免失败后从头重刷。
- 对可能穿透上游或大结果接口要更保守：`block_moneyflow`, `stk_auction_replay`, `moneyflow`, `ths_index`, `ths_member` 建议先按小批量同步；分钟、Level2、公告新闻、研报、期权分钟等独立权限接口建议单独队列，低并发执行。
- 接入层要记录响应头中的 `x-api-relay-cache`, `x-tushare-cache-bucket`, `x-request-id` 和状态码，用来区分缓存命中、上游不稳定、Relay 路由问题和真实限流。
