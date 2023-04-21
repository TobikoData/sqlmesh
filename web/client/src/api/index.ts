import {
  type QueryClient,
  type UseQueryResult,
  type UseMutationResult,
  useQuery,
  useMutation,
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
  type Model,
  type ModelLineageApiLineageModelNameGet200,
  modelLineageApiLineageModelNameGet,
  type ColumnLineageApiLineageModelNameColumnNameGet200,
  columnLineageApiLineageModelNameColumnNameGet,
} from './client'

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
  return useQuery({
    queryKey: ['/api/models'],
    queryFn: async ({ signal }) => await getModelsApiModelsGet({ signal }),
    cacheTime: 0,
    enabled: false,
  })
}

export function useApiFiles(): UseQueryResult<Directory> {
  return useQuery({
    queryKey: ['/api/files'],
    queryFn: async ({ signal }) => await getFilesApiFilesGet({ signal }),
    cacheTime: 0,
    enabled: false,
  })
}

export function useApiEnvironments(): UseQueryResult<GetEnvironmentsApiEnvironmentsGet200> {
  return useQuery({
    queryKey: ['/api/environments'],
    queryFn: async ({ signal }) =>
      await getEnvironmentsApiEnvironmentsGet({ signal }),
    cacheTime: 0,
    enabled: false,
  })
}

export function useApiPlanRun(
  environment: string,
  options?: {
    planDates?: PlanDates
    planOptions?: PlanOptions
  },
): UseQueryResult<ContextEnvironment> {
  return useQuery({
    queryKey: ['/api/plan', environment],
    queryFn: async ({ signal }) =>
      await runPlanApiPlanPost(
        {
          environment,
          plan_dates: options?.planDates,
          plan_options: options?.planOptions,
        },
        { signal },
      ),
    enabled: false,
    cacheTime: 0,
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

export function apiCancelLineage(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/lineage'] })
}

export function apiCancelPlanRun(client: QueryClient): void {
  void client.cancelQueries({ queryKey: ['/api/plan'] })
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
