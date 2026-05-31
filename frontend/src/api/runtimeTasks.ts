import request from './request'

export type RuntimeTaskStatus = 'queued' | 'running' | 'done' | 'completed' | 'failed' | 'cancelled'

export interface RuntimeTask {
  task_id: string
  kind: string
  title: string
  status: RuntimeTaskStatus | string
  progress: number
  result_ref: string | null
  error: string | null
  created_at: number
  updated_at: number
  finished_at: number | null
  meta: Record<string, unknown>
}

export const runtimeTaskApi = {
  list: (includeFinished = true) =>
    request.get<RuntimeTask[]>('/system/tasks', {
      params: { include_finished: includeFinished },
    }),
  get: (taskId: string) => request.get<RuntimeTask>(`/system/tasks/${taskId}`),
}
