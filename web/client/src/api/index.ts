import {
  type QueryClient,
  type UseQueryResult,
  type UseMutationResult,
  useQuery,
  useMutation,
  isCancelledError,
} from '@tanstack/react-query'
import {
  type ContextEnvironment,
  type GetEnvironmentsApiEnvironmentsGet200,
  type BodyWriteFileApiFilesPathPost,
  type PlanDates,
  type File,
  type Directory,
  type ApplyResponse,
  type PlanOptions,
  getFileApiFilesPathGet,
  getFilesApiFilesGet,
  getEnvironmentsApiEnvironmentsGet,
  writeFileApiFilesPathPost,
  runPlanApiPlanPost,
  applyApiCommandsApplyPost,
  cancelPlanApiPlanCancelPost,
  type BodyApplyApiCommandsApplyPostCategories,
  getModelsApiModelsGet,
  type ModelLineageApiLineageModelNameGet200,
  modelLineageApiLineageModelNameGet,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
  columnLineageApiLineageModelNameColumnNameGet,
  fetchdfApiCommandsFetchdfPost,
  renderApiCommandsRenderPost,
  type RenderInput,
  type Query,
  evaluateApiCommandsEvaluatePost,
  type EvaluateInput,
  type Model,
  getTableDiffApiTableDiffGet,
  type GetTableDiffApiTableDiffGetParams,
  type TableDiff,
  type FetchdfInput,
  type Meta,
  getApiMetaApiMetaGet,
} from './client'
import {
  useIDE,
  type ErrorIDE,
  EnumErrorKey,
} from '~/library/pages/ide/context'

export function useApiMeta(): UseQueryResult<Meta> {
  const { addError, removeError } = useIDE()

  return useQuery<Meta, ErrorIDE>({
    queryKey: [`/api/meta`],
    queryFn: async ({ signal }) => {
      removeError(EnumErrorKey.Meta)

      return await getApiMetaApiMetaGet({ signal })
    },
    cacheTime: 0,
    enabled: false,
    onError(error) {
      if (isCancelledError(error)) {
        console.log('getApiMetaApiMetaGet', 'Request aborted by React Query')
      } else {
        addError(EnumErrorKey.Meta, error)
      }
    },
  })
}

export function useApiModelLineage(
  modelName: string,
): UseQueryResult<ModelLineageApiLineageModelNameGet200> {
  return useQuery({
    queryKey: [`/api/lineage`, modelName],
    queryFn: async ({ signal }) =>
      await modelLineageApiLineageModelNameGet(modelName, { signal }),
    cacheTime: 0,
    enabled: false,
  })
}

export function useApiColumnLineage(
  model: string,
  column: string,
): UseQueryResult<ColumnLineageApiLineageModelNameColumnNameGet200> {
  return useQuery({
    queryKey: [`/api/lineage`, model, column],
    queryFn: async ({ signal }) =>
      await columnLineageApiLineageModelNameColumnNameGet(model, column, {
        signal,
      }),
    cacheTime: 0,
    enabled: false,
  })
}

export function useApiFileByPath(path: string): UseQueryResult<File> {
  return useQuery({
    queryKey: [`/api/files`, path],
    queryFn: async ({ signal }) =>
      await getFileApiFilesPathGet(path, { signal }),
    cacheTime: 0,
    enabled: false,
  })
}

export function useApiModels(): UseQueryResult<Model[]> {
  const { addError, removeError } = useIDE()

  return useQuery<Model[], ErrorIDE>({
    queryKey: ['/api/models'],
    queryFn: async ({ signal }) => {
      removeError(EnumErrorKey.Models)

      return (await getModelsApiModelsGet({ signal })) as Model[]
    },
    cacheTime: 0,
    enabled: false,
    onError(error) {
      if (isCancelledError(error)) {
        console.log(
          'getEnvironmentsApiEnvironmentsGet',
          'Request aborted by React Query',
        )
      } else {
        addError(EnumErrorKey.Models, error)
      }
    },
  })
}

export function useApiFiles(): UseQueryResult<Directory> {
  return useQuery<Directory, ErrorIDE>({
    queryKey: ['/api/files'],
    queryFn: async ({ signal }) => await getFilesApiFilesGet({ signal }),
    cacheTime: 0,
    enabled: false,
  })
}

export function useApiEnvironments(): UseQueryResult<GetEnvironmentsApiEnvironmentsGet200> {
  const { addError, removeError } = useIDE()

  return useQuery<GetEnvironmentsApiEnvironmentsGet200, ErrorIDE>({
    queryKey: ['/api/environments'],
    queryFn: async ({ signal }) => {
      removeError(EnumErrorKey.Environments)

      return await getEnvironmentsApiEnvironmentsGet({ signal })
    },
    cacheTime: 0,
    enabled: false,
    onError(error) {
      if (isCancelledError(error)) {
        console.log(
          'getEnvironmentsApiEnvironmentsGet',
          'Request aborted by React Query',
        )
      } else {
        addError(EnumErrorKey.Environments, error)
      }
    },
  })
}

export function useApiPlanRun(
  environment: string,
  options?: {
    planDates?: PlanDates
    planOptions?: PlanOptions
  },
): UseQueryResult<ContextEnvironment> {
  const { addError, removeError } = useIDE()

  return useQuery<ContextEnvironment, ErrorIDE>({
    queryKey: ['/api/plan', environment],
    queryFn: async ({ signal }) => {
      removeError(EnumErrorKey.RunPlan)

      return await runPlanApiPlanPost(
        {
          environment,
          plan_dates: options?.planDates,
          plan_options: options?.planOptions,
        },
        { signal },
      )
    },
    enabled: false,
    cacheTime: 0,
    onError(error) {
      if (isCancelledError(error)) {
        console.log('runPlanApiPlanPost', 'Request aborted by React Query')
      } else {
        addError(EnumErrorKey.RunPlan, error)
      }
    },
  })
}

export function useApiPlanApply(
  environment: string,
  options?: {
    planDates?: PlanDates
    planOptions?: PlanOptions
    categories?: BodyApplyApiCommandsApplyPostCategories
  },
): UseQueryResult<ApplyResponse> {
  return useQuery({
    queryKey: ['/api/commands/apply', environment],
    queryFn: async ({ signal }) =>
      await applyApiCommandsApplyPost(
        {
          environment,
          plan_dates: options?.planDates,
          plan_options: options?.planOptions,
          categories: options?.categories,
        },
        { signal },
      ),
    enabled: false,
    cacheTime: 0,
  })
}

export function useApiFetchdf(options: FetchdfInput): UseQueryResult<unknown> {
  return useQuery<unknown, ErrorIDE>({
    queryKey: ['/api/commands/fetchd'],
    queryFn: async ({ signal }) =>
      await fetchdfApiCommandsFetchdfPost(options, { signal }),
    enabled: false,
    cacheTime: 0,
  })
}

export function useApiRender(options: RenderInput): UseQueryResult<Query> {
  return useQuery<Query, ErrorIDE>({
    queryKey: ['/api/commands/render'],
    queryFn: async ({ signal }) =>
      await renderApiCommandsRenderPost(options, { signal }),
    enabled: false,
    cacheTime: 0,
  })
}

export function useApiTableDiff(
  options: GetTableDiffApiTableDiffGetParams,
): UseQueryResult<TableDiff> {
  return useQuery<TableDiff, ErrorIDE>({
    queryKey: ['/api/commands/table_diff'],
    queryFn: async ({ signal }) =>
      await getTableDiffApiTableDiffGet(options, { signal }),
    enabled: false,
    cacheTime: 0,
  })
}

export function useApiEvaluate(
  options: EvaluateInput,
): UseQueryResult<unknown> {
  return useQuery<unknown, ErrorIDE>({
    queryKey: ['/api/commands/evaluate'],
    queryFn: async ({ signal }) =>
      await evaluateApiCommandsEvaluatePost(options, { signal }),
    enabled: false,
    cacheTime: 0,
  })
}

export function useMutationApiSaveFile(
  client: QueryClient,
  callbacks: {
    onSuccess?: (file: File) => void
    onMutate?: () => void
  },
): UseMutationResult<
  File,
  unknown,
  { path: string; body: BodyWriteFileApiFilesPathPost },
  void
> {
  return useMutation({
    mutationFn: async ({ path, body }) =>
      await writeFileApiFilesPathPost(path, body),
    async onMutate({ path }) {
      await client.cancelQueries({
        queryKey: [`/api/files`, path],
      })

      if (callbacks.onMutate != null) {
        callbacks.onMutate()
      }
    },
    async onSuccess({ path, ...args }) {
      if (callbacks.onSuccess != null) {
        callbacks.onSuccess({ path, ...args })
      }
    },
  })
}

export async function apiCancelPlanApply(client: QueryClient): Promise<void> {
  void client.cancelQueries({ queryKey: ['/api/commands/apply'] })

  return await cancelPlanApiPlanCancelPost()
}

export async function apiCancelPlanRun(client: QueryClient): Promise<void> {
  void client.cancelQueries({ queryKey: ['/api/plan'] })

  return await cancelPlanApiPlanCancelPost()
}

export function apiCancelFetchdf(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/commands/fetchdf'] })
}

export function apiCancelRender(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/commands/render'] })
}

export function apiCancelEvaluate(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/commands/evaluate'] })
}

export function apiCancelLineage(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/lineage'] })
}

export function apiCancelGetEnvironments(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/environments'] })
}

export function apiCancelFiles(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/files'] })
}

export function apiCancelModels(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/models'] })
}
