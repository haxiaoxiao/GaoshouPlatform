import request from './request'

// K线数据类型
export type KlineType = 'daily' | 'minute'

// K线数据接口
export interface KlineData {
  symbol: string
  trade_date: string  // API returns trade_date
  open: number
  high: number
  low: number
  close: number
  volume: number
  amount: number
}

// K线数据接口（前端显示用）
export interface KlineDataDisplay {
  datetime: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  amount: number
}

// 转换函数
export function toDisplayFormat(data: KlineData[]): KlineDataDisplay[] {
  return data.map(item => ({
    datetime: item.trade_date,
    open: item.open,
    high: item.high,
    low: item.low,
    close: item.close,
    volume: item.volume,
    amount: item.amount,
  }))
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
