import { useApiFileByPath, useMutationApiSaveFile } from '@api/index'
import { type Extension } from '@codemirror/state'
import { type KeyBinding } from '@codemirror/view'
import { useLineageFlow } from '@components/graph/context'
import { useStoreEditor } from '@context/editor'
import { useStoreFileTree } from '@context/fileTree'
import { type ModelSQLMeshModel } from '@models/sqlmesh-model'
import { useQueryClient, isCancelledError } from '@tanstack/react-query'
import { type Column, type File, type Model } from '~/api/client'
import {
  debounceAsync,
  debounceSync,
  isFalse,
  isStringEmptyOrNil,
} from '@utils/index'
import { useMemo, useCallback, useEffect, useState } from 'react'
import { events, HoverTooltip, SqlMeshModel } from './extensions'
import { dracula, tomorrow } from 'thememirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage, LanguageSupport } from '@codemirror/language'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { EnumColorScheme, useColorScheme } from '@context/theme'
import { type FileExtensions, EnumFileExtensions } from '@models/file'
import { findModel, findColumn } from './extensions/help'
import {
  completeFromList,
  type CompletionContext,
  type Completion,
} from '@codemirror/autocomplete'
import { keywordCompletionSource, SQLDialect } from '@codemirror/lang-sql'
import { sqlglotWorker } from '~/workers'

const cache = new Map<string, (e: MessageEvent) => void>()
const WHITE_SPACE = ' '

type ExtensionCleanUp = () => void
type ExtensionSqlMeshDialect = (
  models: Map<string, Model>,
  options?: { types: string; keywords: string },
  dialects?: string[],
) => LanguageSupport

export function useDefaultExtensions(type: FileExtensions): Extension[] {
  const { mode } = useColorScheme()

  return useMemo(() => {
    return [
      mode === EnumColorScheme.Dark ? dracula : tomorrow,
      type === EnumFileExtensions.Python && python(),
      type === EnumFileExtensions.YAML && StreamLanguage.define(yaml),
      type === EnumFileExtensions.YML && StreamLanguage.define(yaml),
    ].filter(Boolean) as Extension[]
  }, [type, mode])
}

export function useSQLMeshModelExtensions(
  path?: string,
  handleModelClick?: (model: ModelSQLMeshModel) => void,
  handleModelColumn?: (model: ModelSQLMeshModel, column: Column) => void,
): Extension[] {
  const { models, lineage } = useLineageFlow()
  const files = useStoreFileTree(s => s.files)
  const model = path == null ? undefined : models.get(path)

  const [isActionMode, setIsActionMode] = useState(false)

  const extensions = useMemo(() => {
    const columns =
      lineage == null
        ? new Set<string>()
        : new Set(
            Object.keys(lineage)
              .map(modelName => models.get(modelName)?.columns.map(c => c.name))
              .flat()
              .filter(Boolean) as string[],
          )

    function handleEventModelClick(event: MouseEvent): void {
      if (event.metaKey) {
        const model = findModel(event, models)

        if (model == null) return

        handleModelClick?.(model)
      }
    }

    function handleEventlColumnClick(event: MouseEvent): void {
      if (event.metaKey) {
        if (model == null) return

        const column = findColumn(event, model)

        if (column == null) return

        handleModelColumn?.(model, column)
      }
    }

    return [
      models.size > 0 && isActionMode && HoverTooltip(models),
      events({
        keydown: e => {
          if (e.metaKey) {
            setIsActionMode(true)
          }
        },
        keyup: e => {
          if (isFalse(e.metaKey)) {
            setIsActionMode(false)
          }
        },
      }),
      handleModelClick != null && events({ click: handleEventModelClick }),
      handleModelColumn != null && events({ click: handleEventlColumnClick }),
      model != null && SqlMeshModel(models, model, columns, isActionMode),
    ].filter(Boolean) as Extension[]
  }, [model, models, files, handleModelClick, handleModelColumn, isActionMode])

  return extensions
}

export function useSQLMeshModelKeymaps(path: string): KeyBinding[] {
  const client = useQueryClient()

  const files = useStoreFileTree(s => s.files)
  const refreshTab = useStoreEditor(s => s.refreshTab)

  const { refetch: getFileContent } = useApiFileByPath(path)
  const debouncedGetFileContent = debounceAsync(getFileContent, 1000, true)

  const mutationSaveFile = useMutationApiSaveFile(client, {
    onSuccess: saveChangeSuccess,
  })

  const debouncedSaveChange = useCallback(
    debounceSync(saveChange, 1000, true),
    [path],
  )

  useEffect(() => {
    const file = files.get(path)

    if (file == null) return

    if (isStringEmptyOrNil(file.content)) {
      debouncedGetFileContent({
        throwOnError: true,
      })
        .then(({ data }) => {
          file.update(data)
        })
        .catch(error => {
          if (isCancelledError(error)) {
            console.log('getFileContent', 'Request aborted by React Query')
          } else {
            console.log('getFileContent', error)
          }
        })
        .finally(() => {
          refreshTab()
        })
    }
  }, [path])

  function saveChange(): void {
    const file = files.get(path)

    if (file == null) return

    mutationSaveFile.mutate({
      path: file.path,
      body: { content: file.content },
    })
  }

  function saveChangeSuccess(newfile: File): void {
    const file = files.get(path)

    if (newfile == null || file == null) return

    file.update(newfile)

    refreshTab()
  }

  return [
    {
      mac: 'Cmd-s',
      win: 'Ctrl-s',
      linux: 'Ctrl-s',
      preventDefault: true,
      run() {
        debouncedSaveChange()

        return true
      },
    },
  ]
}

export function useSqlMeshDialect(): [
  ExtensionSqlMeshDialect,
  ExtensionCleanUp,
] {
  const SqlMeshDialect: ExtensionSqlMeshDialect = function SqlMeshDialect(
    models,
    options,
    dialects?,
  ): LanguageSupport {
    const SQLTypes = (options?.types ?? '') + WHITE_SPACE
    const SQLKeywords = (options?.keywords ?? '') + WHITE_SPACE
    const SQLMeshModelDictionary = SQLMeshModelKeywords(dialects)
    const SQLMeshKeywords =
      'columns grain tags audit model name kind owner cron start storage_format time_column partitioned_by pre post batch_size audits dialect' +
      WHITE_SPACE
    const SQLMeshTypes =
      'seed full incremental_by_time_range incremental_by_unique_key view embedded' +
      WHITE_SPACE

    const lang = SQLDialect.define({
      keywords: SQLKeywords + SQLMeshKeywords,
      types: SQLTypes + SQLMeshTypes,
    })

    const tables: Completion[] = Array.from(new Set(Object.values(models))).map(
      label => ({
        label,
        type: 'keyword',
      }),
    )

    let handler = cache.get('message')

    if (handler != null) {
      sqlglotWorker.removeEventListener('message', handler)
    }

    handler = function getTokensFromSQLGlot(e: MessageEvent): void {
      if (e.data.topic === 'parse') {
        // TODO: set parsed tree and use it to improve editor autompletion
      }
    }

    cache.set('message', handler)

    sqlglotWorker.addEventListener('message', handler)

    return new LanguageSupport(lang.language, [
      lang.language.data.of({
        autocomplete: completeFromList([
          {
            label: 'model',
            type: 'keyword',
            apply: 'MODEL (\n\r)',
          },
        ]),
      }),
      lang.language.data.of({
        async autocomplete(ctx: CompletionContext) {
          const match = ctx.matchBefore(/\w*$/)?.text.trim() ?? ''
          const text = ctx.state.doc.toJSON().join('\n')
          const keywordFrom = ctx.matchBefore(/from.+/i)
          const keywordKind = ctx.matchBefore(/kind.+/i)
          const keywordDialect = ctx.matchBefore(/dialect.+/i)
          const matchModels =
            text.match(/MODEL \((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)/g) ??
            []
          const isInsideModel = matchModels
            .filter(str => str.includes(match))
            .map<[number, number]>(str => [
              text.indexOf(str),
              text.indexOf(str) + str.length,
            ])
            .some(
              ([start, end]: [number, number]) =>
                ctx.pos >= start && ctx.pos <= end,
            )

          let suggestions: Completion[] = tables

          if (isFalse(isInsideModel)) {
            if (keywordFrom != null)
              return await completeFromList(suggestions)(ctx)

            return await keywordCompletionSource(lang)(ctx)
          }

          suggestions = SQLMeshModelDictionary.get('keywords') ?? []

          if (keywordKind != null) {
            suggestions = SQLMeshModelDictionary.get('kind') ?? []
          }

          if (keywordDialect != null) {
            suggestions = SQLMeshModelDictionary.get('dialect') ?? []
          }

          return await completeFromList(suggestions)(ctx)
        },
      }),
    ])
  }

  function SqlMeshDialectCleanUp(): void {
    const handler = cache.get('message')

    if (handler == null) return

    sqlglotWorker.removeEventListener('message', handler)
  }

  return [SqlMeshDialect, SqlMeshDialectCleanUp]
}

function SQLMeshModelKeywords(
  dialects: string[] = [],
): Map<string, Completion[]> {
  return new Map([
    [
      'kind',
      [
        {
          label: 'FULL',
          type: 'keyword',
          apply: 'FULL,',
        },
        {
          label: 'INCREMENTAL_BY_TIME_RANGE',
          type: 'keyword',
          apply: 'INCREMENTAL_BY_TIME_RANGE ()',
        },
        {
          label: 'INCREMENTAL_BY_UNIQUE_KEY',
          type: 'keyword',
          apply: 'INCREMENTAL_BY_UNIQUE_KEY ()',
        },
        {
          label: 'VIEW',
          type: 'keyword',
          apply: 'VIEW,',
        },
        {
          label: 'SEED',
          type: 'keyword',
          apply: 'SEED ()',
        },
      ],
    ],
    [
      'dialect',
      dialects.map(w => ({ label: w, type: 'keyword', apply: `${w}, ` })),
    ],
    [
      'keywords',
      [
        {
          label: 'from',
          type: 'keyword',
          apply: 'from ',
        },
        {
          label: 'model',
          type: 'keyword',
          apply: 'MODEL (\n\r)',
        },
        {
          label: 'name',
          type: 'keyword',
          apply: 'name ,',
        },
        {
          label: 'kind',
          type: 'keyword',
          apply: 'kind ',
        },
        {
          label: 'dialect',
          type: 'keyword',
          apply: 'dialect ',
        },
      ],
    ],
  ])
}
