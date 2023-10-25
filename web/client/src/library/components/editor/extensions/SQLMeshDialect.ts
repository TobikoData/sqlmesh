import {
  completeFromList,
  type CompletionContext,
  type Completion,
} from '@codemirror/autocomplete'
import { keywordCompletionSource, SQLDialect } from '@codemirror/lang-sql'
import { LanguageSupport } from '@codemirror/language'
import { type Model } from '~/api/client'
import { isArrayEmpty, isNil, isNotNil } from '~/utils'
import { sqlglotWorker } from '~/workers'

const cache = new Map<string, (e: MessageEvent) => void>()
const WHITE_SPACE = ' '

export type ExtensionSQLMeshDialect = (
  models: Map<string, Model>,
  options?: { types: string; keywords: string },
  dialects?: string[],
) => LanguageSupport

export const SQLMeshDialect: ExtensionSQLMeshDialect = function SQLMeshDialect(
  models,
  options = { types: '', keywords: '' },
  dialects,
): LanguageSupport {
  const SQLKeywords = options.keywords
  const SQLTypes = options.types
  const SQLMeshModelDictionary = getSQLMeshModelKeywords(dialects)
  const SQLMeshKeywords =
    'columns grain grains references metric tags audit model name kind owner cron start storage_format time_column partitioned_by pre post batch_size audits dialect'
  const SQLMeshTypes =
    'expression seed full incremental_by_time_range incremental_by_unique_key view embedded'
  const lang = SQLDialect.define({
    keywords: (SQLKeywords + WHITE_SPACE + SQLMeshKeywords).toLowerCase(),
    types: (SQLTypes + WHITE_SPACE + SQLMeshTypes).toLowerCase(),
  })
  const allModels = Array.from(new Set(models.values()))
  const modelColumns = Array.from(
    new Set(
      allModels.map(model => model.columns.map(column => column.name)).flat(),
    ),
  )
  const modelNames: Completion[] = allModels.map(model => ({
    label: model.name,
    type: 'model',
  }))
  const columnNames: Completion[] = modelColumns.map(name => ({
    label: name,
    type: 'column',
  }))

  SQLMeshDialectCleanUp()

  const handler = function getTokensFromSQLGlot(e: MessageEvent): void {
    if (e.data.topic === 'parse') {
      // TODO: set parsed tree and use it to improve editor autompletion
    }
  }

  cache.set('message', handler)

  sqlglotWorker.addEventListener('message', handler)

  return new LanguageSupport(lang.language, [
    lang.language.data.of({
      autocomplete(ctx: CompletionContext) {
        const dot = ctx.matchBefore(/[\w.]+\.([^ ]+)$/)

        if (isNotNil(dot)) {
          let suggestions = columnNames

          const maybeModelName = dot.text.split('.').slice(0, -1).join('.')
          const maybeModel = models.get(maybeModelName)

          if (isNotNil(maybeModel)) {
            suggestions = maybeModel.columns.map(column => ({
              label: column.name,
              type: 'column',
            }))
          }

          return completeFromList(suggestions)(ctx)
        }

        const word = ctx.matchBefore(/\w*$/)

        if (isNil(word) || (word?.from === word?.to && !ctx.explicit)) return

        const keywordKind = ctx.matchBefore(matchWordWithSpacesAfter('kind'))
        const keywordDialect = ctx.matchBefore(
          matchWordWithSpacesAfter('dialect'),
        )
        const keywordModel = ctx.matchBefore(matchWordWithSpacesAfter('model'))
        const keywordFrom = ctx.matchBefore(matchWordWithSpacesAfter('from'))
        const keywordJoin = ctx.matchBefore(matchWordWithSpacesAfter('join'))
        const keywordSelect = ctx.matchBefore(
          matchWordWithSpacesAfter('select'),
        )

        const text = ctx.state.doc.toJSON().join('\n')
        const matchModels = text.match(/MODEL \(([\s\S]+?)\);/g) ?? []
        const isInMODEL = matchModels
          .filter(str => str.includes(word.text))
          .map<[number, number]>(str => [
            text.indexOf(str),
            text.indexOf(str) + str.length,
          ])
          .some(
            ([start, end]: [number, number]) =>
              ctx.pos >= start && ctx.pos <= end,
          )

        if (isInMODEL) {
          let suggestions = SQLMeshModelDictionary.get('keywords')

          if (isNotNil(keywordKind)) {
            suggestions = SQLMeshModelDictionary.get('kind')
          } else if (isNotNil(keywordDialect)) {
            suggestions = SQLMeshModelDictionary.get('dialect')
          }

          return completeFromList(suggestions ?? [])(ctx)
        } else {
          let suggestions: Completion[] = []

          if (isNotNil(keywordModel) && isArrayEmpty(matchModels)) {
            suggestions = [
              {
                label: 'model',
                type: 'keyword',
                apply: 'MODEL (\n\r);',
              },
            ]
          } else if (isNotNil(keywordSelect)) {
            suggestions = columnNames
          } else if (isNotNil(keywordFrom) || isNotNil(keywordJoin)) {
            suggestions = modelNames
          }

          return isArrayEmpty(suggestions)
            ? keywordCompletionSource(lang)(ctx)
            : completeFromList(suggestions)(ctx)
        }
      },
    }),
  ])
}

function matchWordWithSpacesAfter(word: string): RegExp {
  return new RegExp(`${word}\\s*(?=\\S)([^ ]+)`, 'i')
}

export function SQLMeshDialectCleanUp(): void {
  const handler = cache.get('message')

  if (isNil(handler)) return

  sqlglotWorker.removeEventListener('message', handler)
}

export function getSQLMeshModelKeywords(
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
        {
          label: 'EMBEDDED',
          type: 'keyword',
          apply: 'EMBEDDED, ',
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
