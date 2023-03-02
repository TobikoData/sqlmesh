import { useMemo } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { SQLDialect, keywordCompletionSource } from '@codemirror/lang-sql'
import { python } from '@codemirror/lang-python'
import {
  LanguageSupport,
  StreamLanguage,
  syntaxTree,
} from '@codemirror/language'
import {
  completeFromList,
  CompletionContext,
  Completion,
} from '@codemirror/autocomplete'
import { yaml } from '@codemirror/legacy-modes/mode/yaml'
import { Extension } from '@codemirror/state'
import { ModelsModels } from '~/api/client'
import { isFalse, isNil } from '~/utils'
import {
  EditorView,
  Decoration,
  DecorationSet,
  ViewUpdate,
  ViewPlugin,
} from '@codemirror/view'
import { useStoreFileTree } from '~/context/fileTree'
import { ModelFile } from '~/models'

export default function CodeEditor({
  file,
  models,
  onChange,
}: {
  file: ModelFile
  models?: ModelsModels
  onChange: (value: string) => void
}): JSX.Element {
  const files = useStoreFileTree(s => s.files)
  const selectFile = useStoreFileTree(s => s.selectFile)

  const extensions = useMemo(() => {
    return [
      models != null && events(models, files, selectFile),
      models != null && sqlMeshModelExtension(models),
      file.extension === '.sql' && models != null && SqlMesh(models, file),
      file.extension === '.py' && python(),
      file.extension === '.yaml' && StreamLanguage.define(yaml),
    ].filter(Boolean) as Extension[]
  }, [file, models])

  return (
    <CodeMirror
      value={file.content}
      height="100%"
      width="100%"
      className="w-full h-full overflow-auto"
      extensions={extensions}
      onChange={onChange}
    />
  )
}

function SqlMesh(models: ModelsModels, file: ModelFile): LanguageSupport {
  const SQLTypes =
    'array binary bit boolean char character clob date decimal double float int integer interval large national nchar nclob numeric object precision real smallint time timestamp varchar varying '
  const SQLKeywords =
    'absolute action add after all allocate alter and any are as asc assertion at authorization before begin between both breadth by call cascade cascaded case cast catalog check close collate collation column commit condition connect connection constraint constraints constructor continue corresponding count create cross cube current current_date current_default_transform_group current_transform_group_for_type current_path current_role current_time current_timestamp current_user cursor cycle data day deallocate declare default deferrable deferred delete depth deref desc describe descriptor deterministic diagnostics disconnect distinct do domain drop dynamic each else elseif end end-exec equals escape except exception exec execute exists exit external fetch first for foreign found from free full function general get global go goto grant group grouping handle having hold hour identity if immediate in indicator initially inner inout input insert intersect into is isolation join key language last lateral leading leave left level like limit local localtime localtimestamp locator loop map match method minute modifies module month names natural nesting new next no none not of old on only open option or order ordinality out outer output overlaps pad parameter partial path prepare preserve primary prior privileges procedure public read reads recursive redo ref references referencing relative release repeat resignal restrict result return returns revoke right role rollback rollup routine row rows savepoint schema scroll search second section select session session_user set sets signal similar size some space specific specifictype sql sqlexception sqlstate sqlwarning start state static system_user table temporary then timezone_hour timezone_minute to trailing transaction translation treat trigger under undo union unique unnest until update usage user using value values view when whenever where while with without work write year zone '

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
          text.match(/MODEL \((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)/g) ?? []
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

        if (isFalse(isInsideModel) || isFalse(file.isSQLMeshModel)) {
          if (keywordFrom != null) return completeFromList(tables)(ctx)

          return keywordCompletionSource(lang)(ctx)
        }

        let options: Completion[] = SQLMeshModelDictionary.get('keywords') ?? []

        if (keywordKind != null) {
          options = SQLMeshModelDictionary.get('kind') ?? []
        }

        if (keywordDialect != null) {
          options = SQLMeshModelDictionary.get('dialect') ?? []
        }

        return completeFromList(options)(ctx)
      },
    }),
  ])
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

function sqlMeshModelExtension(models: ModelsModels): Extension {
  return ViewPlugin.fromClass(
    class SqlMeshModelView {
      decorations: DecorationSet = Decoration.set([])
      update(viewUpdate: ViewUpdate): void {
        if (viewUpdate.docChanged || viewUpdate.viewportChanged) {
          const decorations: any[] = []

          for (const range of viewUpdate.view.visibleRanges) {
            syntaxTree(viewUpdate.view.state).iterate({
              from: range.from,
              to: range.to,
              enter({ from, to }) {
                const model = viewUpdate.view.state.doc.sliceString(from, to)

                if (isNil(models[model])) return true

                const decoration = Decoration.mark({
                  attributes: {
                    class: 'sqlmesh-model',
                    model,
                  },
                }).range(from, to)

                decorations.push(decoration)
              },
            })
          }

          this.decorations = Decoration.set(decorations)
        }
      }
    },
    {
      decorations: value => value.decorations,
    },
  )
}

function events(
  models: ModelsModels,
  files: Map<ID, ModelFile>,
  selectFile: (file: ModelFile) => void,
): Extension {
  return EditorView.domEventHandlers({
    click(event: MouseEvent) {
      if (event.target == null) return

      const id = models[event.target.getAttribute('model')]?.path as ID
      const file = files.get(id)

      if (file != null) {
        selectFile(file)
      }
    },
  })
}
