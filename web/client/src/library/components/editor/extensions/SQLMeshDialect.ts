import { type Model } from '@api/client'
import {
  completeFromList,
  type CompletionContext,
  type Completion,
} from '@codemirror/autocomplete'
import { keywordCompletionSource, SQLDialect } from '@codemirror/lang-sql'
import { LanguageSupport } from '@codemirror/language'
import { isArrayEmpty, isNil, isNotNil, isStringEmptyOrNil } from '~/utils'
import { sqlglotWorker } from '~/workers'

const cache = new Map<string, (e: MessageEvent) => void>()
const WHITE_SPACE = ' '

export type ExtensionSQLMeshDialect = (
  models: Map<string, Model>,
  allModelsNames: Array<{ label: string; type: string }>,
  allColumnsNames: Array<{ label: string; type: string }>,
  options?: { types: string; keywords: string },
  dialects?: string[],
) => LanguageSupport

export const SQLMeshDialect: ExtensionSQLMeshDialect = function SQLMeshDialect(
  models,
  allModelsNames,
  allColumnsNames,
  options = { types: '', keywords: '' },
  dialects,
): LanguageSupport {
  const SQLKeywords =
    options.keywords + ' coalesce sum count avg min max cast round'
  const SQLTypes = options.types + ' string'
  const SQLMeshModelDictionary = getSQLMeshModelKeywords(dialects)
  const SQLMeshKeywords =
    'path threshold jinja_query_begin number_of_rows jinja_end not_null forall criteria length unique_values interval_unit unique_key columns grain grains references metric tags audit model name kind owner cron start storage_format time_column partitioned_by pre post batch_size audits dialect'
  const SQLMeshTypes =
    'expression seed full incremental_by_time_range incremental_by_unique_key view embedded'
  const lang = SQLDialect.define({
    keywords: (SQLKeywords + WHITE_SPACE + SQLMeshKeywords).toLowerCase(),
    types: (SQLTypes + WHITE_SPACE + SQLMeshTypes).toLowerCase(),
  })

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
        const dot = ctx.matchBefore(/[A-Za-z0-9_.]*\.(\w+)?\s*$/i)

        if (isNotNil(dot)) {
          const blocks = dot.text.split('.')
          const text = blocks.pop()
          const maybeModelName = blocks.join('.')
          const maybeModel = models.get(maybeModelName) ?? models.get(dot.text)
          let options = allColumnsNames
          let from = isStringEmptyOrNil(text)
            ? dot.to
            : dot.to - dot.text.length

          if (isNotNil(maybeModel)) {
            options = maybeModel.columns.map(column => ({
              label: column.name,
              type: 'column',
            }))
          } else {
            if (maybeSuggestion(allModelsNames, dot.text)) {
              options = allModelsNames.filter(n => n.label.startsWith(dot.text))
              from = dot.from
            }
          }

          return {
            from,
            to: dot.to,
            options,
          }
        }

        const word = ctx.matchBefore(/\w*$/)

        if (isNil(word) || (word?.from === word?.to && !ctx.explicit)) return

        const keywordKind = matchWordWithSpacesAfter(ctx, 'kind')
        const keywordDialect = matchWordWithSpacesAfter(ctx, 'dialect')
        const keywordModel = matchWordWithSpacesAfter(ctx, 'model')

        const text = ctx.state.doc.toJSON().join('\n')
        const matchModels = text.match(/MODEL \(([\s\S]+?)\);/g) ?? []
        const isInModel = matchModels
          .filter(str => str.includes(word.text))
          .map<[number, number]>(str => [
            text.indexOf(str),
            text.indexOf(str) + str.length,
          ])
          .some(
            ([start, end]: [number, number]) =>
              ctx.pos >= start && ctx.pos <= end,
          )

        if (isInModel) {
          let suggestions = SQLMeshModelDictionary.get('keywords')

          if (isNotNil(keywordKind)) {
            suggestions = SQLMeshModelDictionary.get('kind')
          } else if (isNotNil(keywordDialect)) {
            suggestions = SQLMeshModelDictionary.get('dialect')
          }

          return completeFromList(suggestions ?? [])(ctx)
        }

        let suggestions: Completion[] = []

        if (isNotNil(keywordModel) && isArrayEmpty(matchModels)) {
          suggestions = [
            {
              label: 'model',
              type: 'keyword',
              apply: 'MODEL (\n\r);',
            },
          ]
        }

        if (maybeSuggestion(allModelsNames, word.text)) {
          suggestions.push(...allModelsNames)
        }

        if (maybeSuggestion(allColumnsNames, word.text)) {
          suggestions.push(...allColumnsNames)
        }

        const wordLastChar = getFirstChar(word.text)
        const isUpperCase =
          isStringEmptyOrNil(wordLastChar) &&
          wordLastChar === wordLastChar.toUpperCase()

        if (isUpperCase) {
          suggestions = suggestions.map(suggestion => ({
            ...suggestion,
            label: suggestion.label.toUpperCase(),
          }))
        }

        const source = isArrayEmpty(suggestions)
          ? keywordCompletionSource(lang, isUpperCase)(ctx)
          : {
              from: word.from,
              to: word.to,
              options: suggestions,
              validFor: /^\w*$/,
            }

        return source
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

function matchWordWithSpacesAfter(
  ctx: CompletionContext,
  word: string,
): Maybe<{
  from: number
  to: number
  text: string
}> {
  return ctx.matchBefore(new RegExp(`${word}\\s*(?=\\S)([^ ]+)`, 'i'))
}

function maybeSuggestion(suggestions: Completion[], word: string): boolean {
  return (
    word.length > 1 &&
    suggestions.some(suggestion =>
      suggestion.label.toLowerCase().includes(word.toLowerCase()),
    )
  )
}

function getFirstChar(word: string): string {
  for (const char of word) {
    if (/[a-zA-Z]/.test(char)) return char
  }

  return ''
}
