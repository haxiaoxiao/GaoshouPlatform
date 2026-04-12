import request from './request'

// K线数据类型
export type KlineType = 'daily' | 'weekly' | 'monthly' | 'minute1' | 'minute5' | 'minute15' | 'minute30' | 'minute60'

// K线数据接口
export interface KlineData {
  trade_time: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  amount: number
}

// K线查询参数
export interface KlineParams {
  symbol: string
  kline_type?: KlineType
  start_date?: string
  end_date?: string
}

export const klineApi = {
  // 获取K线数据
  getKlines: (params: KlineParams) =>
    request.get<KlineData[]>('/data/klines', { params }),
}
