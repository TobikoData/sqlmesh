import {
  type QueryClient,
  type UseQueryResult,
  type UseMutationResult,
  useQuery,
  useMutation,
  isCancelledError,
  useQueryClient,
  type QueryKey,
  type UseQueryOptions,
  type QueryMeta,
} from '@tanstack/react-query'
import {
  type ContextEnvironment,
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
  getTableDiffApiTableDiffGet,
  type GetTableDiffApiTableDiffGetParams,
  type TableDiff,
  type FetchdfInput,
  type Meta,
  getApiMetaApiMetaGet,
  type GetModelsApiModelsGet200,
  type ApiExceptionPayload,
  deleteEnvironmentApiEnvironmentsEnvironmentDelete as apiDeleteEnvironment,
  type Environments,
} from './client'
import {
  useIDE,
  type ErrorIDE,
  EnumErrorKey,
  type ErrorKey,
} from '~/library/pages/ide/context'
import { useState } from 'react'
import { isNotNil } from '@utils/index'

export interface ApiOptions {
  delay?: number
  trigger?: string
  removeTimeoutErrorAfter?: number
}

export interface ApiQueryMeta extends QueryMeta {
  onError: (error: ApiExceptionPayload) => void
  onSuccess: () => void
}

const DELAY = 60 * 1000 // 1min

export type UseQueryWithTimeoutOptions<
  TData = any,
  TError extends ApiExceptionPayload = ApiExceptionPayload,
> = UseQueryResult<TData, TError> & {
  cancel: () => void
  isTimeout: boolean
}

export function useApiMeta(
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<Meta> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/meta'],
      queryFn: getApiMetaApiMetaGet,
      enabled: true,
    },
    {
      ...options,
      errorKey: EnumErrorKey.Meta,
      trigger: 'API -> useApiMeta',
    },
  )
}

export function useApiModels(
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<GetModelsApiModelsGet200> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/models'],
      queryFn: getModelsApiModelsGet,
    },
    {
      ...options,
      errorKey: EnumErrorKey.Models,
      trigger: 'API -> useApiModels',
    },
  )
}

export function useApiFiles(
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<Directory> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/files'],
      queryFn: getFilesApiFilesGet,
    },
    {
      ...options,
      errorKey: EnumErrorKey.FileExplorer,
      trigger: 'API -> useApiFiles',
    },
  )
}

export function useApiFileByPath(
  path: string,
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<File> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/files', path],
      queryFn: async ({ signal }) =>
        await getFileApiFilesPathGet(path, { signal }),
    },
    {
      ...options,
      errorKey: EnumErrorKey.FileExplorer,
      trigger: 'API -> useApiFileByPath',
    },
  )
}

export function useApiModelLineage(
  modelName: string,
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<ModelLineageApiLineageModelNameGet200> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/lineage', modelName],
      queryFn: async ({ signal }) =>
        await modelLineageApiLineageModelNameGet(modelName, { signal }),
    },
    {
      ...options,
      errorKey: EnumErrorKey.ModelLineage,
      trigger: 'API -> useApiModelLineage',
    },
  )
}

export function useApiColumnLineage(
  model: string,
  column: string,
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<ColumnLineageApiLineageModelNameColumnNameGet200> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/lineage', model, column],
      queryFn: async ({ signal }) =>
        await columnLineageApiLineageModelNameColumnNameGet(model, column, {
          signal,
        }),
    },
    {
      ...options,
      errorKey: EnumErrorKey.ColumnLineage,
      trigger: 'API -> useApiColumnLineage',
    },
  )
}

export function useApiEnvironments(
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<Environments> {
  return useQueryWithTimeout<Environments, ErrorIDE>(
    {
      queryKey: ['/api/environments'],
      queryFn: getEnvironmentsApiEnvironmentsGet,
    },
    {
      ...options,
      errorKey: EnumErrorKey.Environments,
      trigger: 'API -> useApiEnvironments',
    },
  )
}

export { apiDeleteEnvironment }

export function useApiCancelPlan(
  options?: ApiOptions,
): UseQueryWithTimeoutOptions {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/commands/apply'],
      queryFn: cancelPlanApiPlanCancelPost,
    },
    {
      ...options,
      errorKey: EnumErrorKey.CancelPlan,
      trigger: 'API -> useApiCancelPlan',
    },
  )
}

export function useApiPlanRun(
  environment: string,
  inputs?: {
    planDates?: PlanDates
    planOptions?: PlanOptions
  },
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<ContextEnvironment> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/plan', environment],
      queryFn: async ({ signal }) =>
        await runPlanApiPlanPost(
          {
            environment,
            plan_dates: inputs?.planDates,
            plan_options: inputs?.planOptions,
          },
          { signal },
        ),
    },
    {
      ...options,
      errorKey: EnumErrorKey.RunPlan,
      trigger: 'API -> useApiPlanRun',
    },
  )
}

export function useApiPlanApply(
  environment: string,
  inputs?: {
    planDates?: PlanDates
    planOptions?: PlanOptions
    categories?: BodyApplyApiCommandsApplyPostCategories
  },
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<ApplyResponse> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/commands/apply', environment],
      queryFn: async ({ signal }) =>
        await applyApiCommandsApplyPost(
          {
            environment,
            plan_dates: inputs?.planDates,
            plan_options: inputs?.planOptions,
            categories: inputs?.categories,
          },
          { signal },
        ),
    },
    {
      ...options,
      errorKey: EnumErrorKey.ApplyPlan,
      trigger: 'API -> useApiPlanApply',
    },
  )
}

export function useApiFetchdf(
  inputs: FetchdfInput,
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<unknown> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/commands/fetchd'],
      queryFn: async ({ signal }) =>
        await fetchdfApiCommandsFetchdfPost(inputs, { signal }),
    },
    {
      ...options,
      errorKey: EnumErrorKey.Fetchdf,
      trigger: 'API -> useApiFetchdf',
    },
  )
}

export function useApiRender(
  inputs: RenderInput,
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<Query> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/commands/render'],
      queryFn: async ({ signal }) =>
        await renderApiCommandsRenderPost(inputs, { signal }),
    },
    {
      ...options,
      errorKey: EnumErrorKey.RenderQuery,
      trigger: 'API -> useApiRender',
    },
  )
}

export function useApiTableDiff(
  inputs: GetTableDiffApiTableDiffGetParams,
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<TableDiff> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/commands/table_diff'],
      queryFn: async ({ signal }) =>
        await getTableDiffApiTableDiffGet(inputs, { signal }),
    },
    {
      ...options,
      errorKey: EnumErrorKey.TableDiff,
      trigger: 'API -> useApiTableDiff',
    },
  )
}

export function useApiEvaluate(
  inputs: EvaluateInput,
  options?: ApiOptions,
): UseQueryWithTimeoutOptions<unknown> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/commands/evaluate'],
      queryFn: async ({ signal }) =>
        await evaluateApiCommandsEvaluatePost(inputs, { signal }),
    },
    {
      ...options,
      errorKey: EnumErrorKey.EvaluateModel,
      trigger: 'API -> useApiEvaluate',
    },
  )
}

export function useMutationApiSaveFile(
  client: QueryClient,
): UseMutationResult<
  File,
  unknown,
  { path: string; body: BodyWriteFileApiFilesPathPost },
  void
> {
  const { addError } = useIDE()

  return useMutation({
    mutationFn: async ({ path, body }) =>
      await writeFileApiFilesPathPost(path, body),
    onError(error: ErrorIDE) {
      addError(EnumErrorKey.SaveFile, error)
    },
    async onMutate({ path }) {
      await client.cancelQueries({
        queryKey: ['/api/files', path],
      })
    },
  })
}

function useQueryWithTimeout<
  TQueryFnData = unknown,
  TError extends ApiExceptionPayload = ApiExceptionPayload,
  TData = TQueryFnData,
  TQueryKey extends QueryKey = QueryKey,
>(
  options: UseQueryOptions<TQueryFnData, TError, TData, TQueryKey> & {
    meta?: ApiQueryMeta
    queryKey: TQueryKey
  },
  {
    delay = DELAY,
    removeTimeoutErrorAfter,
    errorKey = EnumErrorKey.API,
    trigger,
  }: ApiOptions & { errorKey: ErrorKey },
): UseQueryWithTimeoutOptions<TData, TError> {
  const key = options.queryKey.join(' -> ')
  const queryClient = useQueryClient()
  const { addError } = useIDE()

  const [isTimeout, setIsTimeout] = useState(false)

  let timeoutId: Optional<ReturnType<typeof setTimeout>>
  let timeoutCallback: Optional<() => void> = function timeoutCallback(): void {
    console.log(
      `[REQUEST TIMEOUT] ${key} id: ${String(
        timeoutId,
      )} timed out after ${delay}ms`,
    )

    const { removeError } = addError(errorKey, {
      message: 'Request timed out',
      description: `Request ${key} with timeoutId: ${String(
        timeoutId,
      )} timed out after ${delay}ms`,
      timestamp: Date.now(),
      origin: 'useQueryTimeout',
      trigger,
    })

    setIsTimeout(true)

    void cancel()

    if (isNotNil(removeTimeoutErrorAfter)) {
      setTimeout(() => removeError(), removeTimeoutErrorAfter)
    }
  }

  function timeoutClear(): void {
    console.log(
      `[CLEAR TIMEOUT] ${key} id: ${String(timeoutId)} at ${Date.now()}`,
    )

    clearTimeout(timeoutId)

    timeoutId = undefined
    timeoutCallback = undefined
  }

  function timeout(): void {
    timeoutId = setTimeout(() => {
      timeoutCallback?.()
    }, delay)

    console.log(
      `[START TIMEOUT] ${key} id: ${String(timeoutId)} at ${Date.now()}`,
    )
  }

  function cancel(): void {
    timeoutClear()

    void queryClient.cancelQueries({ queryKey: options.queryKey })

    console.log(`[REQUEST CANCELED] ${key} at ${Date.now()}`)
  }

  function onError(err: TError & { name?: string }): void {
    timeoutClear()

    if (isCancelledError(err) || err.name === 'AbortError') {
      console.log(
        `[REQUEST ABORTED] ${key} aborted by React Query at ${Date.now()}`,
      )
    } else {
      console.log(`[REQUEST FAILED] ${key} failed at ${Date.now()}`)

      addError(errorKey, err)
    }
  }

  function onSuccess(): void {
    timeoutClear()

    console.log(`[REQUEST COMPLETED] ${key} completed at ${Date.now()}`)
  }

  async function queryFn(...args: any[]): Promise<TQueryFnData> {
    timeout()

    return (options.queryFn as (...args: any[]) => Promise<TQueryFnData>)!(
      ...args,
    )
  }

  const q = useQuery<TQueryFnData, TError, TData, TQueryKey>({
    cacheTime: 0,
    enabled: false,
    queryKey: options.queryKey,
    queryFn,
    meta: {
      ...options.meta,
      onError,
      onSuccess,
    },
  })

  return {
    ...q,
    refetch: async (...args: any[]) =>
      new Promise(resolve => {
        q.refetch(...args, { throwOnError: true })
          .then(resolve)
          .catch(onError)
      }),
    cancel,
    isTimeout,
  }
}
