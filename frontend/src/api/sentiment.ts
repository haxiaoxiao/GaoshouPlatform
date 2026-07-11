import request from './request'

export type SentimentSource = 'xueqiu_spyder' | 'eastmoney_guba' | 'taoguba' | 'tieba_stock' | 'laohu8_stock' | 'jisilu' | 'wechat_sogou' | 'flocktrader'

export interface SentimentOverviewSource {
  source: SentimentSource
  label: string
  project_dir: string
  project_ready: boolean
  cookie_configured: boolean
  cache_dir: string | null
  cache_file_count: number
  ready: boolean
  pipeline_state?: 'healthy' | 'stale' | 'auth_required' | 'failing' | 'blocked'
  data_state?: 'fresh' | 'quiet' | 'stale' | 'missing'
  last_attempt_at?: string | null
  last_success_at?: string | null
  lag_hours?: number | null
  verification_required?: boolean
  last_error?: string | null
  post_count: number
  symbol_count: number
  latest_published_at: string | null
}

export interface SentimentOverview {
  sources: SentimentOverviewSource[]
  total_posts: number
  symbol_count: number
  latest_published_at: string | null
}

export interface SentimentSummarySource {
  source: SentimentSource
  post_count: number
  comment_count: number
  bullish_ratio: number
  bearish_ratio: number
  avg_sentiment: number | null
  weighted_sentiment?: number | null
  confidence?: number
  sample_count?: number
  top_keywords: string
}

export interface SentimentPost {
  id: number
  source: SentimentSource
  source_post_id: string
  symbol: string
  title: string | null
  content: string | null
  author: string | null
  published_at: string | null
  url: string | null
  reply_count: number
  like_count: number
  comment_count: number
  sentiment_score: number | null
  sentiment_label: string | null
  keywords: string[]
  source_meta: Record<string, unknown>
  analysis?: {
    model_version?: string
    score?: number
    label?: string
    confidence?: number
    evidence?: Array<Record<string, unknown>>
  } | null
  match?: {
    method?: string
    confidence?: number
    evidence?: string | null
  } | null
}

export interface SentimentThread {
  id: number
  source: SentimentSource
  source_thread_id: string
  title: string | null
  content: string | null
  author: string | null
  published_at: string | null
  last_reply_at: string | null
  url: string | null
  reply_count: number
  comment_count: number
  sentiment_score: number | null
  sentiment_label: string | null
  symbols: string[]
  keywords: string[]
  comments: Array<{
    content: string
    publish_time: string | null
  }>
  full_text: string
}

export interface SentimentSummary {
  symbol: string
  start_date: string | null
  end_date: string | null
  sources: SentimentSummarySource[]
  weighted_score?: number | null
  confidence?: number
  sample_count?: number
  trend?: 'up' | 'down' | 'flat' | null
  model_version?: string
  event_clusters?: Array<{
    title: string
    post_count: number
    sources: SentimentSource[]
    latest_at: string | null
    sentiment_score: number | null
  }>
  hottest_posts: SentimentPost[]
}

export interface SentimentIngestSourceResult {
  ok?: boolean
  source: SentimentSource
  symbol: string | null
  mode?: string
  collected?: number
  matched?: number
  analyzed?: number
  upserted?: number
  threads_upserted?: number
  loaded_dates?: string[]
  crawled_dates?: string[]
  date_files?: string[]
  page_url?: string
  auth?: Record<string, unknown>
  error?: string
}

export interface SentimentIngestBatchResult {
  symbol: string | null
  requested_sources: SentimentSource[]
  succeeded_sources: SentimentSource[]
  failed_sources: SentimentSource[]
  all_succeeded: boolean
  total_upserted: number
  total_collected: number
  total_matched: number
  results: SentimentIngestSourceResult[]
}

export interface SentimentQueryParams {
  start_date?: string
  end_date?: string
  sources?: SentimentSource[]
}

export interface SentimentPostsQueryParams extends SentimentQueryParams {
  limit?: number
}

export interface SentimentThreadsQueryParams extends SentimentPostsQueryParams {
  symbol?: string
}

export interface SentimentIngestParams {
  symbol?: string
  source?: SentimentSource
  sources?: SentimentSource[]
  max_pages?: number
  min_reply?: number
  start_date?: string
  end_date?: string
  force_refresh?: boolean
}

export interface SentimentIngestRun {
  task_id: string
  run_id: string
  sync_type: 'sentiment'
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled'
  total: number
  current: number
  success_count: number
  failed_count: number
  progress_percent: number
  start_time: string | null
  end_time: string | null
  error_message: string | null
  details: Record<string, unknown>
}

const toSourceQuery = (sources?: SentimentSource[]) =>
  sources && sources.length ? sources.join(',') : undefined

export const sentimentApi = {
  overview: (sources?: SentimentSource[]) =>
    request.get<SentimentOverview>('/sentiment/overview', {
      params: { sources: toSourceQuery(sources) },
    }),

  summary: (symbol: string, params?: SentimentQueryParams) =>
    request.get<SentimentSummary>(`/sentiment/summary/${symbol}`, {
      params: {
        start_date: params?.start_date,
        end_date: params?.end_date,
        sources: toSourceQuery(params?.sources),
      },
    }),

  posts: (symbol: string, params?: SentimentPostsQueryParams) =>
    request.get<SentimentPost[]>(`/sentiment/posts/${symbol}`, {
      params: {
        start_date: params?.start_date,
        end_date: params?.end_date,
        sources: toSourceQuery(params?.sources),
        limit: params?.limit,
      },
    }),

  threads: (params?: SentimentThreadsQueryParams) =>
    request.get<SentimentThread[]>('/sentiment/threads', {
      params: {
        start_date: params?.start_date,
        end_date: params?.end_date,
        sources: toSourceQuery(params?.sources),
        symbol: params?.symbol,
        limit: params?.limit,
      },
    }),

  ingest: (payload: SentimentIngestParams) =>
    request.post<SentimentIngestRun>('/sentiment/ingest/run', payload),

  ingestRun: (runId: string) =>
    request.get<SentimentIngestRun>(`/sentiment/ingest/runs/${runId}`),
}
