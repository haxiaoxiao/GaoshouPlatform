/**
 * 格式化日期时间
 */
export function formatDateTime(dateStr: string | null | undefined): string {
  if (!dateStr) return '-'
  return new Date(dateStr).toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

/**
 * 格式化资金
 */
export function formatCapital(capital: string | null | undefined): string {
  if (!capital) return '-'
  const num = parseFloat(capital)
  if (isNaN(num)) return '-'
  return num.toLocaleString('zh-CN', { style: 'currency', currency: 'CNY' })
}

/**
 * 获取状态标签类型
 */
export function getStatusType(status: string): 'info' | 'warning' | 'success' | 'danger' {
  const types: Record<string, 'info' | 'warning' | 'success' | 'danger'> = {
    pending: 'info',
    running: 'warning',
    completed: 'success',
    failed: 'danger',
  }
  return types[status] || 'info'
}

/**
 * 获取状态标签文本
 */
export function getStatusLabel(status: string): string {
  const labels: Record<string, string> = {
    pending: '待运行',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
  }
  return labels[status] || status
}
