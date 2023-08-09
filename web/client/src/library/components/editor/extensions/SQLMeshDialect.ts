import {
  completeFromList,
  type CompletionContext,
  type Completion,
} from '@codemirror/autocomplete'
import { keywordCompletionSource, SQLDialect } from '@codemirror/lang-sql'
import { LanguageSupport } from '@codemirror/language'
import { type Model } from '~/api/client'
import { isFalse, isNil } from '~/utils'
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

  const tables: Completion[] = Array.from(new Set(Object.values(models))).map(
    label => ({
      label,
      type: 'keyword',
    }),
  )

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
      autocomplete: completeFromList([
        {
          label: 'model',
          type: 'keyword',
          apply: 'MODEL (\n\r);',
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
        const matchModels = text.match(/MODEL \(([\s\S]+?)\);/g) ?? []
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
