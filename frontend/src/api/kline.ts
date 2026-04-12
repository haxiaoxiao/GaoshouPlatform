import request from './request'

// K线数据类型
export type KlineType = 'daily' | 'minute'

// K线数据接口
export interface KlineData {
  datetime: string
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
  period?: KlineType
  start_date?: string
  end_date?: string
}

export const klineApi = {
  // 获取K线数据
  getKlines: (params: KlineParams) =>
    request.get<{ items: KlineData[], total: number }>('/data/klines', { params }),
}
