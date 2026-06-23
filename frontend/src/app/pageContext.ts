import { computed, onScopeDispose, ref, toValue, watchEffect, type MaybeRefOrGetter } from 'vue'
import { useRoute } from 'vue-router'
import type { ContextBlock, ContextTone } from '@/app/navigation'

interface LooseContextRow {
  label: string
  value: unknown
  tone?: string | null
}

interface LooseContextBlock {
  title: string
  action?: string
  rows: LooseContextRow[]
}

type ContextBlocksLike = ContextBlock[] | LooseContextBlock[]

interface PageContextEntry {
  id: number
  path: string
  fullPath: string
  matchedLength: number
  updatedAt: number
  blocks: ContextBlock[]
}

interface ResolvedPageContext {
  blocks: ContextBlock[]
  isDynamic: boolean
}

const entries = ref<PageContextEntry[]>([])
let nextEntryId = 1

function normalizeTone(value?: string | null): ContextTone | undefined {
  if (value === 'good' || value === 'warn' || value === 'bad' || value === 'neutral') {
    return value
  }
  return undefined
}

function normalizeBlocks(blocks: ContextBlocksLike | null | undefined): ContextBlock[] {
  return (blocks || [])
    .map(block => ({
      ...block,
      action: typeof block.action === 'string' ? block.action : undefined,
      rows: block.rows
        .filter(row => String(row.value || '').trim())
        .map(row => ({
          label: row.label,
          value: row.value == null ? '-' : String(row.value),
          tone: normalizeTone(row.tone),
        })),
    }))
    .filter(block => block.rows.length > 0)
}

function upsertEntry(entry: PageContextEntry) {
  const next = [...entries.value]
  const index = next.findIndex(item => item.id === entry.id)
  if (index >= 0) {
    next[index] = entry
  } else {
    next.push(entry)
  }
  entries.value = next
}

function removeEntry(id: number) {
  const next = entries.value.filter(entry => entry.id !== id)
  if (next.length !== entries.value.length) {
    entries.value = next
  }
}

export function usePageContext(blocksSource: MaybeRefOrGetter<ContextBlocksLike | null | undefined>) {
  const route = useRoute()
  const entryId = nextEntryId++

  watchEffect(() => {
    const blocks = normalizeBlocks(toValue(blocksSource))
    if (!blocks.length) {
      removeEntry(entryId)
      return
    }

    upsertEntry({
      id: entryId,
      path: route.path,
      fullPath: route.fullPath,
      matchedLength: route.matched.length,
      updatedAt: Date.now(),
      blocks,
    })
  })

  onScopeDispose(() => {
    removeEntry(entryId)
  })
}

export function useResolvedPageContext(
  fallbackSource: MaybeRefOrGetter<ContextBlocksLike | null | undefined>,
) {
  const route = useRoute()

  return computed<ResolvedPageContext>(() => {
    const fallbackBlocks = normalizeBlocks(toValue(fallbackSource))
    const matches = entries.value.filter(entry => (
      entry.fullPath === route.fullPath || entry.path === route.path
    ))

    if (!matches.length) {
      return {
        blocks: fallbackBlocks,
        isDynamic: false,
      }
    }

    const [bestMatch] = [...matches].sort((left, right) => (
      right.matchedLength - left.matchedLength || right.updatedAt - left.updatedAt
    ))

    return {
      blocks: bestMatch?.blocks.length ? bestMatch.blocks : fallbackBlocks,
      isDynamic: Boolean(bestMatch?.blocks.length),
    }
  })
}
