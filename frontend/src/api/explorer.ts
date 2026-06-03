import request from './request'

export interface TableInfo {
  name: string
  row_count: number | null
  min_date?: string | null
  max_date?: string | null
  estimated?: boolean
  partition_count?: number
  date_column?: string | null
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
  total_estimated?: boolean
  page: number
  page_size: number
  total_pages: number
  generated_sql?: string
}

export type ExplorerFilterOp =
  | '='
  | '!='
  | 'contains'
  | 'in'
  | 'between'
  | '>'
  | '>='
  | '<'
  | '<='
  | 'is null'
  | 'not null'

export interface ExplorerFilter {
  column: string
  op: ExplorerFilterOp
  value?: unknown
  value_to?: unknown
  values?: unknown[]
}

export interface ExplorerSearchRequest {
  page?: number
  page_size?: number
  order_by?: string
  order_dir?: 'ASC' | 'DESC'
  columns?: string[]
  filters?: ExplorerFilter[]
  quick_search?: Record<string, unknown>
  include_total?: boolean
}

export function getTables() {
  return request.get<TableInfo[]>('/explorer/tables')
}

export function getTableSchema(tableName: string) {
  return request.get<ColumnInfo[]>(`/explorer/tables/${tableName}/schema`)
}

export function previewTable(
  tableName: string,
  params: {
    page?: number
    page_size?: number
    order_by?: string
    order_dir?: 'ASC' | 'DESC'
    where?: string
    include_total?: boolean
  } = {}
) {
  return request.get<PreviewResult>(`/explorer/tables/${tableName}/preview`, { params })
}

export function searchTable(tableName: string, data: ExplorerSearchRequest) {
  return request.post<PreviewResult>(`/explorer/tables/${tableName}/search`, data)
}

export function getDistinctValues(tableName: string, column: string, limit?: number, q?: string) {
  return request.get<any[]>(`/explorer/tables/${tableName}/distinct`, {
    params: { column, limit, q }
  })
}

export function executeQuery(sql: string, limit?: number) {
  return request.post<any>('/explorer/query', null, {
    params: { sql, limit }
  })
}
