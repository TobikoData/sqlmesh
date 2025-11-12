import {
  type UseQueryResult,
  useQuery,
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

export function useApiMeta(): UseQueryResult<Meta> {
  return useQuery({
    queryKey: ['/api/meta'],
    queryFn: getApiMetaApiMetaGet,
    enabled: true,
  })
}

export function useApiModels(): UseQueryResult<GetModelsApiModelsGet200> {
  return useQuery({
    queryKey: ['/api/models'],
    queryFn: getModelsApiModelsGet,
  })
}

export function useApiModel(modelName: string): UseQueryResult<Model> {
  return useQuery({
    queryKey: ['/api/models', modelName],
    queryFn: async ({ signal }) =>
      await getModelApiModelsNameGet(modelName, { signal }),
  })
}

export function useApiModelLineage(
  modelName: string,
): UseQueryResult<ModelLineageApiLineageModelNameGet200> {
  return useQuery({
    queryKey: ['/api/lineage', modelName],
    queryFn: async ({ signal }) => {
      try {
        const response = await modelLineageApiLineageModelNameGet(modelName, {
          signal,
        })
        return response
      } catch (error) {
        console.error('error fetching lineage', error)
        throw error
      }
    },
  })
}

export function useApiColumnLineage(
  model: string,
  column: string,
  params?: ColumnLineageApiLineageModelNameColumnNameGetParams,
): UseQueryResult<ColumnLineageApiLineageModelNameColumnNameGet200> {
  return useQuery({
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
  })
}
