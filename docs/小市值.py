# 来源：聚宽文章 https://www.joinquant.com/post/71632
# 标题：小市值选股V4版  收益：​1056.55%
# 作者：ffzzsh

# 导入函数库
from jqdata import *
from jqfactor import *
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import talib as ta

# 初始化函数
def initialize(context):
    # 开启防未来函数，避免使用未来数据。
    set_option('avoid_future_data', True)
    # 设定基准指数。
    set_benchmark('000001.XSHG')
    # 用真实价格交易。
    set_option('use_real_price', True)
    # 将滑点设置为 0。
    set_slippage(FixedSlippage(3/10000))
    # 设置交易成本。
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, 
                            open_commission=2.5/10000, 
                            close_commission=2.5/10000, 
                            close_today_commission=0, min_commission=5), type='stock')
    # 过滤日志输出。
    log.set_level('order', 'error')
    log.set_level('system', 'error')
    log.set_level('strategy', 'debug')
    
    # 初始化全局变量。
    g.no_trading_today_signal = False  # 是否为可交易日
    g.pass_april = True  # 是否四月空仓
    g.run_stoploss = True  # 是否进行止损
    g.hold_list = [] # 当前持仓
    g.yesterday_HL_list = [] # 昨日涨停股
    g.target_list = [] # 目标股票池
    g.not_buy_again = [] # 不再买入列表
    g.stock_num = 5 # 持仓数量
    g.up_price = 100  # 股价上限
    g.reason_to_sell = '' # 卖出原因
    g.stoploss_strategy = 3  # 止损策略
    g.stoploss_limit = 0.88  # 止损线
    g.stoploss_market = 0.94  # 市场止损参数
    
    # 指标相关参数（优化点 1：动态参数）
    g.enable_indicator = True  # 是否启用主力资金指标
    g.HV_control = True  # 是否启用放量检测
    g.HV_duration = 120  # 放量检测周期
    g.HV_ratio = 0.9     # 放量阈值比例
    g.dynamic_params = True  # 启用动态参数调整
    g.volatility_lookback = 30  # 波动率观察期
    
    # 新增行业风控参数（优化点 2：行业分散）
    g.max_industry_weight = 0.3  # 单一行业最大权重
    g.enable_industry_filter = True  # 启用行业过滤
    
    # 设置交易运行时间（优化点 3：时序调整）
    run_daily(prepare_stock_list, '9:15')  # 延后到9:15避免集合竞价
    run_weekly(weekly_adjustment, 2, '10:30')
    run_daily(sell_stocks, time='10:00')  # 止损函数
    run_daily(trade_afternoon, time='14:30')  # 下午交易检查
    run_daily(close_account, '14:50')
    run_weekly(print_position_info, 5, time='15:10')

# 计算市场波动率（新增函数）
def get_market_volatility(context, days=30):
    """计算市场波动率，用于动态调整 V4GV 周期参数。"""
    index = '399101.XSHE'
    end_date = context.previous_date
    start_date = end_date - timedelta(days=days+10)
    
    df = get_price(index, start_date=start_date, end_date=end_date,
                  frequency='daily', fields=['close'], skip_paused=False)
    
    if df is None or len(df) < days:
        return 0.2  # 默认波动率
    
    returns = np.log(df['close'] / df['close'].shift(1)).dropna()
    volatility = np.std(returns[-days:])
    
    return volatility

# 计算动态周期参数（新增函数）
def get_dynamic_periods(context):
    """根据波动率动态调整 V4GV 周期。"""
    if not g.dynamic_params:
        return 55, 34  # 默认值
    
    volatility = get_market_volatility(context, g.volatility_lookback)
    
    # 波动率高时使用较短周期，低时使用较长周期。
    if volatility > 0.025:  # 高波动市场
        return 40, 25
    elif volatility > 0.015:  # 中等波动
        return 48, 30
    else:  # 低波动市场
        return 55, 34

# 计算主力资金。指标（支持动态周期）
def calculate_indicator(security, context, n=None, m=None):
    """
    计算主力资金指标
    参数:
        security: 股票代码
        context: 策略上下文
        n: 长期周期(动态调整)
        m: 中期周期(动态调整)
    返回:
        (v4gv, v4gv21): 当前指标值元组
    """
    if n is None or m is None:
        n, m = get_dynamic_periods(context)
    
    # 获取历史数据。
    end_date = context.previous_date
    start_date = end_date - timedelta(days=max(n, m) * 2)
    df = get_price(security, start_date=start_date, end_date=end_date, 
                  frequency='daily', fields=['close', 'high', 'low'], 
                  skip_paused=False, fq='pre', panel=False)
    
    # 数据完整性检查（修复 inputs are all NaN 错误）。
    if df is None or len(df) < max(n, m) + 5:
        log.warning(f"数据不足[{security}]: {len(df) if df is not None else 0} < {max(n, m) + 5}")
        return (None, None)
    
    # 检查数据是否全为 NaN。
    if df['close'].isnull().all() or df['high'].isnull().all() or df['low'].isnull().all():
        log.warning(f"数据全为NaN[{security}]")
        return (None, None)
    
    # 填充缺失值。
    df = df.fillna(method='ffill').fillna(method='bfill')
    
    close = df['close'].values
    high = df['high'].values
    low = df['low'].values
    
    # 计算 VAR1。
    llv34 = ta.MIN(low, 34)
    hhv34 = ta.MAX(high, 34)
    # 避免分母为零。
    denominator = np.where(hhv34 - llv34 == 0, 1e-6, hhv34 - llv34)
    rsv = 100 * (close - llv34) / denominator
    var1 = ta.SMA(rsv, 5) - 20
    
    # 计算 A1。
    llv55 = ta.MIN(low, 55)
    hhv55 = ta.MAX(high, 55)
    denominator = np.where(hhv55 - llv55 == 0, 1e-6, hhv55 - llv55)
    rsv2 = 100 * (close - llv55) / denominator
    
    # 计算 SMA。
    sma1 = ta.EMA(rsv2, 5)
    a1 = 3 * sma1 - 2 * sma1  # 原始公式，实际是sma1
    
    # 修复 EMA 周期问题：周期 1 的 EMA 就是原始值。
    a12 = (a1 + var1) / 2  # 相当于EMA(...,1)就是原值
    
    # 计算主力资金。
    rsv3 = 100 * (close - ta.MIN(low, 34)) / np.where(ta.MAX(high, 34) - ta.MIN(low, 34) == 0, 
                      1e-6, ta.MAX(high, 34) - ta.MIN(low, 34))
    main_fund = ta.EMA(rsv3, 3)
    
    # 计算 D0。
    rsv4 = -100 * (hhv34 - close) / denominator
    d0 = ta.EMA(rsv4, 4) + 100
    
    # 计算 V4GV。
    v4gv = ((main_fund + d0) / 2 + a12) / 2
    
    # 计算 V4GV。21 (2日均线)
    v4gv21 = (v4gv + ta.SMA(v4gv, 2)) / 2
    
    # 返回最新值。
    return (v4gv[-1], v4gv21[-1])

# 获取 MACD 信号（新增函数，优化点 4：信号增强）
def get_macd_signal(security, context):
    """获取 MACD 指标信号，作为卖出确认。"""
    end_date = context.previous_date
    start_date = end_date - timedelta(days=50)
    
    df = get_price(security, start_date=start_date, end_date=end_date,
                  frequency='daily', fields=['close'], skip_paused=False)
    
    if df is None or len(df) < 26:
        return False
    
    close = df['close'].values
    macd, signal, hist = ta.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    
    # MACD 金叉且为正值。
    return macd[-1] > signal[-1] and macd[-1] > 0

# 检查买卖信号（增强版）
def check_trading_signals(context):
    """检查主力资金指标买卖信号。"""
    if not g.enable_indicator:
        return
    
    current_data = get_current_data()
    
    # 检查持仓股的卖出信号。
    for stock in list(context.portfolio.positions.keys()):
        if current_data[stock].paused:
            continue
            
        # 获取指标值。
        try:
            v4gv, v4gv21 = calculate_indicator(stock, context)
            if v4gv is None or v4gv21 is None:
                continue
                
            # 检查死叉信号（卖出）+ MACD 确认。
            if v4gv < v4gv21 and v4gv21 > 0:
                # 获取 MACD 确认。
                macd_confirm = not get_macd_signal(stock, context)  # MACD死叉确认
                if macd_confirm:
                    log.info(f"检测到死叉信号(V4GV:{v4gv:.2f} < V4GV21:{v4gv21:.2f})，卖出[{stock}]")
                    position = context.portfolio.positions[stock]
                    if close_position(position):
                        g.hold_list = [s for s in g.hold_list if s != stock]
        except Exception as e:
            log.error(f"计算指标出错[{stock}]: {str(e)}")
            continue

# 获取行业信息（新增函数）
def get_stock_industry(stock_list):
    """获取股票行业信息。"""
    industry_dict = {}
    for stock in stock_list:
        try:
            industry = get_industry(stock)
            # 取一级行业。
            for industry_code in industry:
                if industry[industry_code]['industry_type'] == 'sw_l1':
                    industry_dict[stock] = industry_code
                    break
        except:
            industry_dict[stock] = 'unknown'
    return industry_dict

# 行业过滤。（新增函数，优化点 5：行业风控）
def filter_by_industry(stock_list, current_holdings):
    """根据行业分布过滤股票。"""
    if not g.enable_industry_filter or not stock_list:
        return stock_list
    
    # 获取当前持仓行业分布。
    hold_industries = get_stock_industry(current_holdings)
    industry_count = {}
    for stock, industry in hold_industries.items():
        industry_count[industry] = industry_count.get(industry, 0) + 1
    
    # 获取候选股票行业。
    candidate_industries = get_stock_industry(stock_list)
    filtered_list = []
    
    for stock in stock_list:
        industry = candidate_industries.get(stock, 'unknown')
        current_weight = industry_count.get(industry, 0) / len(current_holdings) if current_holdings else 0
        
        if current_weight < g.max_industry_weight:
            filtered_list.append(stock)
    
    return filtered_list

# 准备股票池。
def prepare_stock_list(context):
    # 获取已持有列表。
    g.hold_list = [pos.security for pos in context.portfolio.positions.values()]
    
    # 获取昨日涨停列表。
    if g.hold_list:
        df = get_price(g.hold_list, end_date=context.previous_date, 
                      frequency='daily', fields=['close', 'high_limit'], 
                      count=1, panel=False, fill_paused=False)
        if df is not None and not df.empty:
            df = df[df['close'] == df['high_limit']]
            g.yesterday_HL_list = list(df.code)
        else:
            g.yesterday_HL_list = []
    else:
        g.yesterday_HL_list = []
    
    # 判断今天是否为账户资金再平衡的日期。
    g.no_trading_today_signal = today_is_between(context)

# 选股模块（增强版）
def get_stock_list(context):
    MKT_index = '399101.XSHE'
    initial_list = get_index_stocks(MKT_index)
    initial_list = filter_new_stock(context, initial_list)
    initial_list = filter_kcbj_stock(initial_list)  # 修改了过滤规则
    initial_list = filter_st_stock(initial_list)
    initial_list = filter_paused_stock(initial_list)
    initial_list = filter_limitup_stock(context, initial_list)
    initial_list = filter_limitdown_stock(context, initial_list)
    
    # 按市值排序。
    q = query(valuation.code, valuation.market_cap).filter(
        valuation.code.in_(initial_list)).order_by(valuation.market_cap.asc())
    df = get_fundamentals(q)
    
    if df is None or df.empty:
        return []
    
    stock_list = list(df.code)[:100]  # 取前100小市值
    
    # 行业过滤。
    stock_list = filter_by_industry(stock_list, g.hold_list)
    
    # 如果启用指标，进一步筛选。
    if g.enable_indicator:
        filtered_list = []
        for stock in stock_list:
            try:
                v4gv, v4gv21 = calculate_indicator(stock, context)
                if v4gv is None or v4gv21 is None:
                    continue
                    
                # 检查金叉信号 (买入) + MACD确认
                if v4gv > v4gv21 and v4gv > 0:
                    macd_confirm = get_macd_signal(stock, context)
                    if macd_confirm:
                        filtered_list.append(stock)
            except Exception as e:
                log.error(f"选股计算指标出错[{stock}]: {str(e)}")
                continue
        
        # 如果信号股不足，补充小市值股
        if len(filtered_list) < g.stock_num:
            filtered_list += stock_list[:g.stock_num - len(filtered_list)]
        
        final_list = filtered_list[:2*g.stock_num]
    else:
        final_list = stock_list[:2*g.stock_num]
    
    log.info(f'今日选股({len(final_list)}只):{final_list[:min(10, len(final_list))]}...')
    return final_list

# 整体调整持仓
def weekly_adjustment(context):
    if g.no_trading_today_signal:
        log.info("今日为特殊月份，不交易")
        return
        
    # 获取应买入列表 
    g.not_buy_again = []
    g.target_list = get_stock_list(context)
    
    if not g.target_list:
        log.warning("今日无符合条件股票")
        return
        
    target_list = g.target_list[:g.stock_num]
    
    # 行业再平衡
    target_list = filter_by_industry(target_list, g.hold_list)
    
    # 调仓卖出
    for stock in list(g.hold_list):
        if (stock not in target_list) and (stock not in g.yesterday_HL_list):
            log.info(f"调仓卖出[{stock}]")
            position = context.portfolio.positions[stock]
            if close_position(position):
                g.hold_list.remove(stock)
        else:
            log.info(f"继续持有[{stock}]")
    
    # 调仓买入
    buy_security(context, target_list)
    
    # 记录已买入股票
    g.not_buy_again = list(context.portfolio.positions.keys())

# 调整昨日涨停股票
def check_limit_up(context):
    if not g.yesterday_HL_list:
        return
        
    now_time = context.current_dt
    # 对昨日涨停股票观察到尾盘如不涨停则提前卖出
    for stock in list(g.yesterday_HL_list):
        if stock not in context.portfolio.positions:
            g.yesterday_HL_list.remove(stock)
            continue
            
        current_data = get_current_data()[stock]
        if current_data.last_price < current_data.high_limit:
            log.info(f"[{stock}]涨停打开，卖出")
            position = context.portfolio.positions[stock]
            if close_position(position):
                g.hold_list.remove(stock)
                g.yesterday_HL_list.remove(stock)
                g.reason_to_sell = 'limitup'
        else:
            log.info(f"[{stock}]涨停，继续持有")

# 余额再投资
def check_remain_amount(context):
    if g.reason_to_sell == 'limitup': 
        # 如果是涨停售出则次日再次交易
        g.hold_list = list(context.portfolio.positions.keys())
        if len(g.hold_list) < g.stock_num and context.portfolio.cash > 100:
            target_list = g.target_list
            # 剔除本周一曾买入的股票
            target_list = [s for s in target_list if s not in g.not_buy_again]
            target_list = target_list[:min(g.stock_num - len(g.hold_list), len(target_list))]
            log.info(f'有余额可用{context.portfolio.cash:.2f}元，买入{target_list}')
            buy_security(context, target_list)
        g.reason_to_sell = ''
    elif g.reason_to_sell == 'stoploss':
        log.info('止损后余额，下周再交易')
        g.reason_to_sell = ''

# 下午交易检查
def trade_afternoon(context):
    if g.no_trading_today_signal:
        return
        
    check_limit_up(context)
    
    if g.HV_control:
        check_high_volume(context)
    
    # 添加指标信号检查
    check_trading_signals(context)
    
    check_remain_amount(context)

# 动态止损线计算（新增函数，优化点6：智能止损）
def get_dynamic_stoploss(context, stock):
    """根据波动率动态计算止损线"""
    if not g.dynamic_params:
        return g.stoploss_limit
    
    # 计算ATR波动率
    end_date = context.previous_date
    start_date = end_date - timedelta(days=20)
    
    df = get_price(stock, start_date=start_date, end_date=end_date,
                  frequency='daily', fields=['high', 'low', 'close'], skip_paused=False)
    
    if df is None or len(df) < 14:
        return g.stoploss_limit
    
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    
    atr = ta.ATR(high, low, close, timeperiod=14)[-1]
    price = close[-1]
    
    if price == 0:
        return g.stoploss_limit
    
    # ATR百分比止损
    atr_percentage = atr / price
    dynamic_stoploss = 1 - min(0.12, max(0.08, atr_percentage * 2))
    
    return dynamic_stoploss

# 止盈止损（增强版）
def sell_stocks(context):
    if not g.run_stoploss:
        return
        
    if g.stoploss_strategy == 1:
        for stock, pos in list(context.portfolio.positions.items()):
            cost = pos.avg_cost
            price = pos.price
            # 股票盈利大于等于100%则卖出
            if price >= cost * 2:
                if order_target_value(stock, 0):
                    log.info(f"收益100%止盈,卖出{stock}")
                    g.hold_list.remove(stock)
            # 动态止损
            elif price < cost * get_dynamic_stoploss(context, stock):
                if order_target_value(stock, 0):
                    log.info(f"动态止损,卖出{stock}")
                    g.hold_list.remove(stock)
                    g.reason_to_sell = 'stoploss'
                
    elif g.stoploss_strategy == 2:
        index = '399101.XSHE'
        df = get_price(index, end_date=context.previous_date, 
                      frequency='daily', fields=['close', 'open'], 
                      count=1, panel=False)
        if df is not None and not df.empty:
            down_ratio = df['close'].iloc[0] / df['open'].iloc[0]
            if down_ratio <= g.stoploss_market:
                g.reason_to_sell = 'stoploss'
                log.info(f"大盘惨跌,跌幅{down_ratio:.2%}")
                for stock in list(context.portfolio.positions.keys()):
                    if order_target_value(stock, 0):
                        log.info(f"清仓卖出{stock}")
                        g.hold_list.remove(stock)
                    
    elif g.stoploss_strategy == 3:
        index = '399101.XSHE'
        df = get_price(index, end_date=context.previous_date, 
                      frequency='daily', fields=['close', 'open'], 
                      count=1, panel=False)
        if df is not None and not df.empty:
            down_ratio = df['close'].iloc[0] / df['open'].iloc[0]
            if down_ratio <= g.stoploss_market:
                g.reason_to_sell = 'stoploss'
                log.info(f"大盘惨跌,跌幅{down_ratio:.2%}")
                for stock in list(context.portfolio.positions.keys()):
                    if order_target_value(stock, 0):
                        log.info(f"清仓卖出{stock}")
                        g.hold_list.remove(stock)
            else:
                for stock, pos in list(context.portfolio.positions.items()):
                    dynamic_stop = get_dynamic_stoploss(context, stock)
                    if pos.price < pos.avg_cost * dynamic_stop:
                        if order_target_value(stock, 0):
                            log.info(f"动态止损,卖出{stock}")
                            g.hold_list.remove(stock)
                            g.reason_to_sell = 'stoploss'

# 调整放量股票
def check_high_volume(context):
    current_data = get_current_data()
    for stock in list(context.portfolio.positions.keys()):
        if current_data[stock].paused:
            continue
        if current_data[stock].last_price == current_data[stock].high_limit:
            continue
        if context.portfolio.positions[stock].closeable_amount == 0:
            continue
            
        try:
            df_volume = get_bars(stock, count=g.HV_duration, 
                                unit='1d', fields=['volume'], 
                                include_now=True)
            if df_volume is None or len(df_volume) < g.HV_duration:
                continue
                
            max_vol = max(df_volume['volume'])
            if df_volume['volume'][-1] > g.HV_ratio * max_vol:
                log.info(f"[{stock}]放量({df_volume['volume'][-1]:.0f} > {g.HV_ratio*max_vol:.0f})，卖出")
                position = context.portfolio.positions[stock]
                if close_position(position):
                    g.hold_list.remove(stock)
        except Exception as e:
            log.error(f"检查放量出错[{stock}]: {str(e)}")
            continue

# 过滤函数
def filter_paused_stock(stock_list):
    if not stock_list:
        return []
        
    current_data = get_current_data()
    return [s for s in stock_list if not current_data[s].paused]

def filter_st_stock(stock_list):
    if not stock_list:
        return []
        
    current_data = get_current_data()
    return [s for s in stock_list
            if not current_data[s].is_st
            and 'ST' not in current_data[s].name
            and '*' not in current_data[s].name
            and '退' not in current_data[s].name]

def filter_kcbj_stock(stock_list):
    """优化点7：支持北交所和科创板"""
    if not stock_list:
        return []
        
    # 修改过滤规则，允许北交所（8开头）和科创板（68开头）
    return [s for s in stock_list if not (s.startswith('68') and s[0] not in ['4', '8'])]

def filter_limitup_stock(context, stock_list):
    if not stock_list:
        return []
        
    current_data = get_current_data()
    return [s for s in stock_list 
            if s in context.portfolio.positions 
            or get_price(s, end_date=context.current_dt, frequency='1m', 
                         fields='close', count=1).iloc[0,0] < current_data[s].high_limit]

def filter_limitdown_stock(context, stock_list):
    if not stock_list:
        return []
        
    current_data = get_current_data()
    return [s for s in stock_list 
            if s in context.portfolio.positions 
            or get_price(s, end_date=context.current_dt, frequency='1m', 
                         fields='close', count=1).iloc[0,0] > current_data[s].low_limit]

def filter_new_stock(context, stock_list):
    if not stock_list:
        return []
        
    yesterday = context.previous_date
    filtered = []
    for s in stock_list:
        info = get_security_info(s)
        if info is None:
            continue
        if yesterday - info.start_date > timedelta(days=375):
            filtered.append(s)
    return filtered

def filter_highprice_stock(context, stock_list):
    if not stock_list:
        return []
        
    prices = get_price(stock_list, end_date=context.previous_date, 
                     frequency='daily', fields='close', count=1, 
                     panel=False)
    if prices is None or prices.empty:
        return stock_list
        
    prices = prices.set_index('code')['close']
    return [s for s in stock_list 
            if s in context.portfolio.positions or prices.get(s, 0) <= g.up_price]

# 交易函数
def order_target_value_(security, value):
    try:
        if value == 0:
            log.debug(f"卖出 {security}")
            return order_target(security, 0)
        else:
            log.debug(f"买入 {security} 金额 {value:.2f}")
            return order_target_value(security, value)
    except Exception as e:
        log.error(f"交易出错: {str(e)}")
        return None

def open_position(security, value):
    order = order_target_value_(security, value)
    if order is not None and order.filled > 0:
        return True
    return False

def close_position(position):
    if position is None:
        return False
    order = order_target_value_(position.security, 0)
    if order is not None and order.filled > 0:
        return True
    return False

def buy_security(context, target_list):
    if not target_list:
        return
        
    position_count = len(context.portfolio.positions)
    target_num = min(g.stock_num, len(target_list))
    
    if target_num > position_count:
        value = context.portfolio.cash / (target_num - position_count)
        for stock in target_list:
            if stock not in context.portfolio.positions:
                if open_position(stock, value):
                    log.info(f"买入[{stock}] {value:.2f}元")
                    g.not_buy_again.append(stock)
                    g.hold_list.append(stock)
                    if len(context.portfolio.positions) == target_num:
                        break

# 特殊月份处理
def today_is_between(context):
    if not g.pass_april:
        return False
        
    today = context.current_dt.strftime('%m-%d')
    return ('04-01' <= today <= '04-30') or ('01-01' <= today <= '01-30')

def close_account(context):
    if g.no_trading_today_signal and context.portfolio.positions:
        log.info("特殊月份清仓")
        for stock in list(context.portfolio.positions.keys()):
            if close_position(context.portfolio.positions[stock]):
                g.hold_list.remove(stock)

def print_position_info(context):
    log.info("="*50)
    log.info(f"日期: {context.current_dt.date()}")
    log.info(f"总资产: {context.portfolio.total_value:.2f}")
    log.info(f"持仓数量: {len(context.portfolio.positions)}")
    
    # 输出行业分布（新增）
    if g.enable_industry_filter:
        industry_dict = get_stock_industry(list(context.portfolio.positions.keys()))
        industry_dist = {}
        for stock, industry in industry_dict.items():
            industry_dist[industry] = industry_dist.get(industry, 0) + 1
        log.info(f"行业分布: {industry_dist}")
    
    for stock, pos in context.portfolio.positions.items():
        cost = pos.avg_cost
        price = pos.price
        ret = (price / cost - 1) * 100 if cost > 0 else 0
        log.info(f"{stock} | 成本:{cost:.2f} | 现价:{price:.2f} | 收益:{ret:.2f}%")
    
    if not context.portfolio.positions:
        log.info("当前无持仓")
    
    log.info("="*50)