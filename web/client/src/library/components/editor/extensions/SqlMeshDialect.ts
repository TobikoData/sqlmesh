import {
  completeFromList,
  type CompletionContext,
  type Completion,
} from '@codemirror/autocomplete'
import { keywordCompletionSource, SQLDialect } from '@codemirror/lang-sql'
import { LanguageSupport } from '@codemirror/language'
import { type Model } from '~/api/client'
import { type ModelFile } from '~/models'
import { isFalse } from '~/utils'
import { sqlglotWorker } from '~/workers'

const cache = new Map<string, (e: MessageEvent) => void>()
const WHITE_SPACE = ' '

type ExtensionCleanUp = () => void
type ExtensionSqlMeshDialect = (
  models: Map<string, Model>,
  file: ModelFile,
  options: { types: string; keywords: string },
  dialects?: string[],
) => LanguageSupport

export function useSqlMeshExtension(): [
  ExtensionSqlMeshDialect,
  ExtensionCleanUp,
] {
  const SqlMeshDialectExtension: ExtensionSqlMeshDialect =
    function SqlMeshDialectExtension(
      models,
      file,
      options,
      dialects?,
    ): LanguageSupport {
      const SQLTypes = options.types
      const SQLKeywords = options.keywords
      const SQLMeshModelDictionary = SQLMeshModelKeywords(dialects)
      const SQLMeshKeywords =
        'model name kind owner cron start storage_format time_column partitioned_by pre post batch_size audits dialect'
      const SQLMeshTypes =
        'seed full incremental_by_time_range incremental_by_unique_key view embedded'

      const lang = SQLDialect.define({
        keywords:
          SQLKeywords +
          (file.isSQLMeshModel ? SQLMeshKeywords : '') +
          WHITE_SPACE,
        types:
          SQLTypes + (file.isSQLMeshModel ? SQLMeshTypes : '') + WHITE_SPACE,
      })

      const tables: Completion[] = Array.from(
        new Set(Object.values(models)),
      ).map(label => ({
        label,
        type: 'keyword',
      }))

      let handler = cache.get('message')

      if (handler != null) {
        sqlglotWorker.removeEventListener('message', handler)
      }

      handler = function getTokensFromSqlGlot(e: MessageEvent): void {
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

            if (isFalse(isInsideModel) || isFalse(file.isSQLMeshModel)) {
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

  const SqlMeshDialectCleanUp: ExtensionCleanUp =
    function SqlMeshDialectCleanUp(): void {
      const handler = cache.get('message')

      if (handler == null) return

      sqlglotWorker.removeEventListener('message', handler)
    }

  return [SqlMeshDialectExtension, SqlMeshDialectCleanUp]
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
