import request from './request'

export interface TableInfo {
  name: string
  row_count: number
}

export interface ColumnInfo {
  name: string
  type: string
  default: string | null
  comment: string | null
}

export interface PreviewResult {
  columns: string[]
  rows: Record<string, any>[]
  total: number
  page: number
  page_size: number
  total_pages: number
}

export function getTables() {
  return request.get<{ code: number; data: TableInfo[] }>('/explorer/tables')
}

export function getTableSchema(tableName: string) {
  return request.get<{ code: number; data: ColumnInfo[] }>(`/explorer/tables/${tableName}/schema`)
}

export function previewTable(
  tableName: string,
  params: {
    page?: number
    page_size?: number
    order_by?: string
    order_dir?: 'ASC' | 'DESC'
    where?: string
  } = {}
) {
  return request.get<{ code: number; data: PreviewResult }>(`/explorer/tables/${tableName}/preview`, { params })
}

export function getDistinctValues(tableName: string, column: string, limit?: number) {
  return request.get<{ code: number; data: any[] }>(`/explorer/tables/${tableName}/distinct`, {
    params: { column, limit }
  })
}

export function executeQuery(sql: string, limit?: number) {
  return request.post<{ code: number; data?: any; message?: string }>('/explorer/query', null, {
    params: { sql, limit }
  })
}