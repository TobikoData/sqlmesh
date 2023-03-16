import {
  type QueryClient,
  type UseQueryResult,
  type UseMutationResult,
  useQuery,
  useMutation,
} from '@tanstack/react-query'
import {
  type ContextEnvironment,
  type DagApiCommandsDagGet200,
  type GetEnvironmentsApiEnvironmentsGet200,
  type BodyWriteFileApiFilesPathPost,
  type PlanDates,
  type File,
  type Directory,
  type Context,
  type ApplyResponse,
  type PlanOptions,
  getFileApiFilesPathGet,
  getFilesApiFilesGet,
  getApiContextApiContextGet,
  dagApiCommandsDagGet,
  getEnvironmentsApiEnvironmentsGet,
  writeFileApiFilesPathPost,
  runPlanApiPlanPost,
  applyApiCommandsApplyPost,
  cancelPlanApiPlanCancelPost,
} from './client'

export function useApiFileByPath(path?: string): UseQueryResult<File> {
  const shouldEnable = path != null && path !== ''

  return useQuery({
    queryKey: [`/api/files`, path],
    queryFn: shouldEnable
      ? async () => await getFileApiFilesPathGet(path)
      : undefined,
    enabled: shouldEnable,
    cacheTime: 0,
  })
}

export function useApiFiles(): UseQueryResult<Directory> {
  return useQuery({
    queryKey: ['/api/files'],
    queryFn: getFilesApiFilesGet,
    cacheTime: 0,
  })
}

export function useApiEnvironments(): UseQueryResult<GetEnvironmentsApiEnvironmentsGet200> {
  return useQuery({
    queryKey: ['/api/environments'],
    queryFn: getEnvironmentsApiEnvironmentsGet,
    cacheTime: 0,
  })
}

export function useApiContext(): UseQueryResult<Context> {
  return useQuery({
    queryKey: ['/api/context'],
    queryFn: getApiContextApiContextGet,
    cacheTime: 0,
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
    queryFn: async () =>
      await runPlanApiPlanPost({
        environment,
        plan_dates: options?.planDates,
        plan_options: options?.planOptions,
      }),
    enabled: false,
    cacheTime: 0,
  })
}

export function useApiPlanApply(
  environment: string,
  options?: {
    planDates?: PlanDates
    planOptions?: PlanOptions
  },
): UseQueryResult<ApplyResponse> {
  return useQuery({
    queryKey: ['/api/commands/apply', environment],
    queryFn: async () =>
      await applyApiCommandsApplyPost({
        environment,
        plan_dates: options?.planDates,
        plan_options: options?.planOptions,
      }),
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

export async function useApiContextCancel(client: QueryClient): Promise<void> {
  await client.cancelQueries({ queryKey: ['/api/context'] })
}

export async function apiCancelPlanRun(
  client: QueryClient,
  environment: string,
): Promise<void> {
  await client.cancelQueries({ queryKey: ['/api/plan', environment] })
}

export async function apiCancelPlanApply(
  client: QueryClient,
  environment: string,
): Promise<void> {
  await client.cancelQueries({ queryKey: ['/api/commands/apply', environment] })
}

export async function apiCancelPlanApplyAndRun(
  client: QueryClient,
  environment: string,
): Promise<void> {
  await Promise.allSettled([
    apiCancelPlanRun(client, environment),
    apiCancelPlanApply(client, environment),
    cancelPlanApiPlanCancelPost(),
  ])
}

export function useApiDag(): UseQueryResult<DagApiCommandsDagGet200> {
  return useQuery({
    queryKey: ['/api/commands/dag'],
    queryFn: dagApiCommandsDagGet,
  })
}
