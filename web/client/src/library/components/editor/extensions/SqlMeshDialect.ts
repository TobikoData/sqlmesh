import {
  completeFromList,
  CompletionContext,
  Completion,
} from '@codemirror/autocomplete'
import { keywordCompletionSource, SQLDialect } from '@codemirror/lang-sql'
import { LanguageSupport } from '@codemirror/language'
import { ModelsModels } from '~/api/client'
import { ModelFile } from '~/models'
import { isFalse } from '~/utils'
import { sqlglotWorker } from '../workers'

const cache = new Map<string, (e: MessageEvent) => void>()

export function useSqlMeshExtention(): [
  (
    models: ModelsModels,
    file: ModelFile,
    options: { types: string; keywords: string },
  ) => LanguageSupport,
  () => void,
] {
  function SqlMeshDialectExtension(
    models: ModelsModels,
    file: ModelFile,
    options: { types: string; keywords: string },
  ): LanguageSupport {
    const SQLTypes = options.types
    const SQLKeywords = options.keywords
    const SQLMeshModelDictionary = SQLMeshModelKeywords()
    const SQLMeshKeywords =
      'model name kind owner cron start storage_format time_column partitioned_by pre post batch_size audits dialect '

    const lang = SQLDialect.define({
      keywords: SQLKeywords + (file.isSQLMeshModel ? SQLMeshKeywords : ''),
      types:
        SQLTypes +
        (file.isSQLMeshModel
          ? 'seed full incremental_by_time_range incremental_by_unique_key view embedded'
          : ''),
    })

    const tables: Completion[] = Object.keys(models).map(label => ({
      label,
      type: 'keyword',
    }))

    let handler = cache.get('message')

    if (handler != null) {
      sqlglotWorker.removeEventListener('message', handler)
    }

    let tree: any

    handler = function getTokensFromSqlGlot(e: MessageEvent): void {
      if (e.data.topic === 'parse') {
        tree = e.data.payload
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
        autocomplete(ctx: CompletionContext) {
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

          console.log({ tree })

          let suggestions: Completion[] = tables

          if (isFalse(isInsideModel) || isFalse(file.isSQLMeshModel)) {
            if (keywordFrom != null) return completeFromList(suggestions)(ctx)

            return keywordCompletionSource(lang)(ctx)
          }

          suggestions = SQLMeshModelDictionary.get('keywords') ?? []

          if (keywordKind != null) {
            suggestions = SQLMeshModelDictionary.get('kind') ?? []
          }

          if (keywordDialect != null) {
            suggestions = SQLMeshModelDictionary.get('dialect') ?? []
          }

          return completeFromList(suggestions)(ctx)
        },
      }),
    ])
  }

  function SqlMeshDialectCleanUp(): void {
    const handler = cache.get('message')

    if (handler == null) return

    sqlglotWorker.removeEventListener('message', handler)
  }

  return [SqlMeshDialectExtension, SqlMeshDialectCleanUp]
}

function SQLMeshModelKeywords(): Map<string, Completion[]> {
  const dialect = [
    'Hive',
    'Spark',
    'Databricks',
    'Snowflake',
    'BigQuery',
    'Redshift',
    'Postgres',
    'DuckDB',
  ]
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
      dialect.map(w => ({ label: w, type: 'keyword', apply: `${w}, ` })),
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
          apply: 'name sushi.,',
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
