import {
  type UseQueryResult,
  useQuery,
  type QueryKey,
  type UseQueryOptions,
  type QueryMeta,
} from '@tanstack/react-query'
import {
  getModelsApiModelsGet,
  type ModelLineageApiLineageModelNameGet200,
  modelLineageApiLineageModelNameGet,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
  columnLineageApiLineageModelNameColumnNameGet,
  type Meta,
  getApiMetaApiMetaGet,
  type GetModelsApiModelsGet200,
  type ApiExceptionPayload,
  type Model,
  getModelApiModelsNameGet,
  type ColumnLineageApiLineageModelNameColumnNameGetParams,
} from './client'

export interface ApiOptions {
  delay?: number
  trigger?: string
  removeTimeoutErrorAfter?: number
}

export interface ApiQueryOptions {
  enabled?: boolean
}

export interface ApiQueryMeta extends QueryMeta {
  onError: (error: ApiExceptionPayload) => void
  onSuccess: () => void
}

export type UseQueryWithTimeoutOptions<
  TData = any,
  TError extends ApiExceptionPayload = ApiExceptionPayload,
> = UseQueryResult<TData, TError> & {
  cancel: () => void
  isTimeout: boolean
}

export function useApiMeta(
): UseQueryWithTimeoutOptions<Meta> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/meta'],
      queryFn: getApiMetaApiMetaGet,
      enabled: true,
    },
   )
}

export function useApiModels(
): UseQueryWithTimeoutOptions<GetModelsApiModelsGet200> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/models'],
      queryFn: getModelsApiModelsGet,
    },
  )
}

export function useApiModel(
  modelName: string,
): UseQueryWithTimeoutOptions<Model> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/models', modelName],
      queryFn: async ({ signal }) =>
        await getModelApiModelsNameGet(modelName, { signal }),
    },
  )
}

export function useApiModelLineage(
  modelName: string,
): UseQueryResult<ModelLineageApiLineageModelNameGet200> {
  return useQuery(
    {
      queryKey: ['/api/lineage', modelName],
      queryFn: async ({ signal }) => {
        console.log('modelName', modelName)
        return await modelLineageApiLineageModelNameGet(modelName, { signal })
      },
    },
  )
}

export function useApiColumnLineage(
  model: string,
  column: string,
  params?: ColumnLineageApiLineageModelNameColumnNameGetParams,
): UseQueryWithTimeoutOptions<ColumnLineageApiLineageModelNameColumnNameGet200> {
  return useQueryWithTimeout(
    {
      queryKey: ['/api/lineage', model, column],
      queryFn: async ({ signal }) =>
        await columnLineageApiLineageModelNameColumnNameGet(
          model,
          column,
          params,
          {
            signal,
          },
        ),
    },
  )
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
): UseQueryWithTimeoutOptions<TData, TError> {
  async function queryFn(...args: any[]): Promise<TQueryFnData> {
    return (options.queryFn as (...args: any[]) => Promise<TQueryFnData>)(
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
     },
  })

  return {
    ...q,
    refetch: async (...args: any[]) =>
      new Promise(resolve => {
        q.refetch(...args, { throwOnError: true })
          .then(resolve)
          .catch(err => err)
      }),
    cancel: () => {
      console.log('cancel')
    },
    isTimeout: false,
  }
}
