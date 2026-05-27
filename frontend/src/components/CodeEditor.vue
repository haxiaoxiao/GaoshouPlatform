<template>
  <div class="code-editor-shell" :class="{ 'is-fullscreen': isFullscreen }">
    <div class="code-editor-toolbar">
      <div class="editor-meta">
        <span class="language-pill">{{ languageLabel }}</span>
        <span v-if="lineCount" class="line-count">{{ lineCount }} 行</span>
      </div>
      <div class="editor-actions">
        <el-tooltip content="复制代码" placement="top">
          <el-button text size="small" @click="copyCode">复制</el-button>
        </el-tooltip>
        <el-tooltip :content="wrapLines ? '关闭自动换行' : '开启自动换行'" placement="top">
          <el-button text size="small" @click="toggleWrap">{{ wrapLines ? '换行开' : '换行关' }}</el-button>
        </el-tooltip>
        <el-tooltip content="缩小字体" placement="top">
          <el-button text size="small" @click="changeFontSize(-1)">A-</el-button>
        </el-tooltip>
        <el-tooltip content="放大字体" placement="top">
          <el-button text size="small" @click="changeFontSize(1)">A+</el-button>
        </el-tooltip>
        <el-tooltip :content="isFullscreen ? '退出全屏' : '全屏编辑'" placement="top">
          <el-button text size="small" @click="isFullscreen = !isFullscreen">
            {{ isFullscreen ? '退出' : '全屏' }}
          </el-button>
        </el-tooltip>
      </div>
    </div>
    <div ref="editorHost" class="code-editor-host" :style="{ minHeight: normalizedMinHeight }" />
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { EditorView, keymap, lineNumbers, highlightActiveLine, highlightActiveLineGutter, placeholder, Decoration, type DecorationSet, ViewPlugin, type ViewUpdate } from '@codemirror/view'
import { EditorState, Compartment, RangeSetBuilder } from '@codemirror/state'
import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands'
import { bracketMatching, foldGutter, indentOnInput, syntaxHighlighting, StreamLanguage } from '@codemirror/language'
import { searchKeymap, highlightSelectionMatches } from '@codemirror/search'
import { autocompletion, closeBrackets, closeBracketsKeymap, completionKeymap } from '@codemirror/autocomplete'
import { python } from '@codemirror/lang-python'
import { json } from '@codemirror/lang-json'
import { oneDark, oneDarkHighlightStyle } from '@codemirror/theme-one-dark'

type CodeLanguage = 'python' | 'json' | 'expression' | 'text'

const props = withDefaults(defineProps<{
  modelValue: string
  language?: CodeLanguage
  readonly?: boolean
  minHeight?: string | number
  placeholder?: string
}>(), {
  language: 'text',
  readonly: false,
  minHeight: 220,
  placeholder: '',
})

const emit = defineEmits<{ (event: 'update:modelValue', value: string): void }>()

const editorHost = ref<HTMLDivElement>()
const isFullscreen = ref(false)
const wrapLines = ref(true)
const fontSize = ref(13)
const lineCount = computed(() => (props.modelValue ? props.modelValue.split('\n').length : 1))
const normalizedMinHeight = computed(() => typeof props.minHeight === 'number' ? `${props.minHeight}px` : props.minHeight)
const languageLabel = computed(() => {
  if (props.language === 'python') return 'Python'
  if (props.language === 'json') return 'JSON'
  if (props.language === 'expression') return 'Expression'
  return 'Text'
})

let editorView: EditorView | null = null
const languageCompartment = new Compartment()
const editableCompartment = new Compartment()
const wrapCompartment = new Compartment()
const fontCompartment = new Compartment()
const semanticCompartment = new Compartment()

const expressionLanguage = StreamLanguage.define({
  token(stream) {
    if (stream.eatSpace()) return null
    if (stream.match(/#[^\n]*/)) return 'comment'
    if (stream.match(/\$[A-Za-z_][\w]*/)) return 'variableName'
    if (stream.match(/[A-Za-z_][\w]*(?=\s*\()/)) return 'function'
    if (stream.match(/[A-Za-z_][\w]*/)) return 'keyword'
    if (stream.match(/\d+(?:\.\d+)?/)) return 'number'
    if (stream.match(/"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/)) return 'string'
    if (stream.match(/[+\-*/%=<>!&|]+/)) return 'operator'
    if (stream.match(/[()[\]{},.]/)) return 'bracket'
    stream.next()
    return null
  },
})

function languageExtension(language: CodeLanguage) {
  if (language === 'python') return python()
  if (language === 'json') return json()
  if (language === 'expression') return expressionLanguage
  return []
}

function semanticExtension(language: CodeLanguage) {
  return language === 'python' ? pythonSemanticHighlight : []
}

function collectPythonSemanticMarks(view: EditorView): DecorationSet {
  const marks: Array<{ from: number; to: number; className: string }> = []

  for (const { from, to } of view.visibleRanges) {
    const text = view.state.doc.sliceString(from, to)

    collectRegexMarks(text, /\bself\b/g, from, marks, 'cm-token-self')
    collectRegexMarks(text, /\.([A-Za-z_]\w*)(?=\s*\()/g, from, marks, 'cm-token-method', 1)
    collectRegexMarks(text, /\b([A-Za-z_]\w*)(?=\s*=)/g, from, marks, 'cm-token-kwarg', 1)
    collectRegexMarks(text, /\b([A-Za-z_]\w*)(?=\s*\()/g, from, marks, 'cm-token-call', 1, (absoluteStart) => {
      return view.state.doc.sliceString(Math.max(0, absoluteStart - 1), absoluteStart) !== '.'
    })
    collectRegexMarks(text, /([(,=]\s*)([A-Za-z_]\w*)/g, from, marks, 'cm-token-argument', 2, (_absoluteStart, absoluteEnd) => {
      const nextChar = nextNonSpace(view.state.doc.sliceString(absoluteEnd, Math.min(view.state.doc.length, absoluteEnd + 8)))
      return nextChar !== '='
    })
  }

  marks.sort((left, right) => left.from - right.from || left.to - right.to)
  const builder = new RangeSetBuilder<Decoration>()
  let lastTo = -1
  for (const mark of marks) {
    if (mark.from < lastTo) continue
    builder.add(mark.from, mark.to, Decoration.mark({ class: mark.className }))
    lastTo = mark.to
  }
  return builder.finish()
}

function collectRegexMarks(
  text: string,
  regex: RegExp,
  rangeStart: number,
  marks: Array<{ from: number; to: number; className: string }>,
  className: string,
  groupIndex = 0,
  shouldInclude?: (absoluteStart: number, absoluteEnd: number) => boolean,
) {
  for (const match of text.matchAll(regex)) {
    const token = match[groupIndex]
    if (!token || match.index === undefined) continue
    const groupOffset = groupIndex === 0 ? 0 : match[0].indexOf(token)
    if (groupOffset < 0) continue
    const absoluteStart = rangeStart + match.index + groupOffset
    const absoluteEnd = absoluteStart + token.length
    if (shouldInclude && !shouldInclude(absoluteStart, absoluteEnd)) continue
    marks.push({ from: absoluteStart, to: absoluteEnd, className })
  }
}

function nextNonSpace(text: string) {
  return text.trimStart().charAt(0)
}

const pythonSemanticHighlight = ViewPlugin.fromClass(class {
  decorations: DecorationSet

  constructor(view: EditorView) {
    this.decorations = collectPythonSemanticMarks(view)
  }

  update(update: ViewUpdate) {
    if (update.docChanged || update.viewportChanged) {
      this.decorations = collectPythonSemanticMarks(update.view)
    }
  }
}, {
  decorations: (plugin) => plugin.decorations,
})

function buildExtensions() {
  return [
    lineNumbers(),
    foldGutter(),
    history(),
    highlightActiveLine(),
    highlightActiveLineGutter(),
    bracketMatching(),
    closeBrackets(),
    indentOnInput(),
    autocompletion(),
    highlightSelectionMatches(),
    oneDark,
    syntaxHighlighting(oneDarkHighlightStyle),
    keymap.of([
      indentWithTab,
      ...defaultKeymap,
      ...historyKeymap,
      ...searchKeymap,
      ...closeBracketsKeymap,
      ...completionKeymap,
    ]),
    placeholder(props.placeholder),
    languageCompartment.of(languageExtension(props.language)),
    semanticCompartment.of(semanticExtension(props.language)),
    editableCompartment.of(EditorView.editable.of(!props.readonly)),
    wrapCompartment.of(wrapLines.value ? EditorView.lineWrapping : []),
    fontCompartment.of(EditorView.theme({
      '&': { fontSize: `${fontSize.value}px` },
    })),
    EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        emit('update:modelValue', update.state.doc.toString())
      }
    }),
    EditorView.theme({
      '&': {
        height: '100%',
        minHeight: normalizedMinHeight.value,
        backgroundColor: '#282c34',
        color: '#abb2bf',
      },
      '.cm-scroller': {
        fontFamily: '"JetBrains Mono", "Fira Code", Consolas, monospace',
        lineHeight: '1.65',
      },
      '.cm-content': {
        padding: '12px 0',
        caretColor: '#38bdf8',
      },
      '.cm-line': {
        padding: '0 14px',
      },
      '.cm-gutters': {
        backgroundColor: '#21252b',
        color: '#636d83',
        borderRight: '1px solid rgba(99, 109, 131, 0.28)',
      },
      '.cm-activeLine': {
        backgroundColor: 'rgba(153, 187, 255, 0.08)',
      },
      '.cm-activeLineGutter': {
        backgroundColor: 'rgba(153, 187, 255, 0.12)',
        color: '#d7dae0',
      },
      '.cm-selectionBackground, &.cm-focused .cm-selectionBackground': {
        backgroundColor: '#3e4451',
      },
      '&.cm-focused': {
        outline: 'none',
      },
      '.cm-tooltip': {
        backgroundColor: '#21252b',
        border: '1px solid #3e4451',
      },
      '.cm-search': {
        backgroundColor: '#21252b',
        border: '1px solid #3e4451',
      },
      '.cm-token-self': {
        color: '#e06c75',
        fontWeight: '700',
      },
      '.cm-token-method': {
        color: '#61afef',
        fontWeight: '700',
      },
      '.cm-token-call': {
        color: '#98c379',
        fontWeight: '650',
      },
      '.cm-token-kwarg': {
        color: '#e5c07b',
        fontStyle: 'italic',
      },
      '.cm-token-argument': {
        color: '#c678dd',
      },
    }),
  ]
}

function dispatchConfig() {
  if (!editorView) return
  editorView.dispatch({
    effects: [
      languageCompartment.reconfigure(languageExtension(props.language)),
      semanticCompartment.reconfigure(semanticExtension(props.language)),
      editableCompartment.reconfigure(EditorView.editable.of(!props.readonly)),
      wrapCompartment.reconfigure(wrapLines.value ? EditorView.lineWrapping : []),
      fontCompartment.reconfigure(EditorView.theme({ '&': { fontSize: `${fontSize.value}px` } })),
    ],
  })
}

function toggleWrap() {
  wrapLines.value = !wrapLines.value
  dispatchConfig()
}

function changeFontSize(delta: number) {
  fontSize.value = Math.min(18, Math.max(11, fontSize.value + delta))
  dispatchConfig()
}

async function copyCode() {
  await navigator.clipboard.writeText(props.modelValue || '')
  ElMessage.success('代码已复制')
}

watch(() => props.modelValue, (value) => {
  if (!editorView) return
  const currentValue = editorView.state.doc.toString()
  if (value !== currentValue) {
    editorView.dispatch({
      changes: { from: 0, to: currentValue.length, insert: value || '' },
    })
  }
})

watch(() => [props.language, props.readonly], dispatchConfig)

onMounted(() => {
  if (!editorHost.value) return
  editorView = new EditorView({
    parent: editorHost.value,
    state: EditorState.create({
      doc: props.modelValue || '',
      extensions: buildExtensions(),
    }),
  })
})

onBeforeUnmount(() => {
  editorView?.destroy()
  editorView = null
})
</script>

<style scoped>
.code-editor-shell {
  display: flex;
  flex-direction: column;
  width: 100%;
  overflow: hidden;
  background: #282c34;
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: 8px;
}

.code-editor-shell.is-fullscreen {
  position: fixed;
  inset: 24px;
  z-index: 3000;
  box-shadow: 0 24px 80px rgba(0, 0, 0, 0.55);
}

.code-editor-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  min-height: 38px;
  padding: 6px 8px 6px 12px;
  background: #21252b;
  border-bottom: 1px solid rgba(99, 109, 131, 0.28);
}

.editor-meta,
.editor-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.language-pill {
  display: inline-flex;
  align-items: center;
  height: 22px;
  padding: 0 8px;
  border-radius: 4px;
  background: rgba(97, 175, 239, 0.14);
  color: #61afef;
  font-family: 'JetBrains Mono', Consolas, monospace;
  font-size: 11px;
  letter-spacing: 0;
}

.line-count {
  color: #94a3b8;
  font-size: 12px;
}

.code-editor-host {
  flex: 1;
  min-width: 0;
}

.is-fullscreen .code-editor-host {
  min-height: calc(100vh - 110px) !important;
}

.editor-actions :deep(.el-button) {
  color: #cbd5e1;
}

.editor-actions :deep(.el-button:hover) {
  color: #38bdf8;
  background: rgba(56, 189, 248, 0.1);
}
</style>
