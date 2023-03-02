import {
  useQuery,
  useMutation,
  QueryClient,
  UseQueryResult,
  UseMutationResult,
} from '@tanstack/react-query'
import {
  getFileApiFilesPathGet,
  getFilesApiFilesGet,
  getPlanApiPlanGet,
  getApiContextApiContextGet,
  ContextEnvironment,
  DagApiCommandsDagGet200,
  dagApiCommandsDagGet,
  GetEnvironmentsApiEnvironmentsGet200,
  getEnvironmentsApiEnvironmentsGet,
  writeFileApiFilesPathPost,
  BodyWriteFileApiFilesPathPost,
  getModelsApiModelsGet,
  Models,
} from './client'
import type { File, Directory, Context } from './client'

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

export function useApiModels(): UseQueryResult<Models> {
  return useQuery({
    queryKey: ['/api/models'],
    queryFn: getModelsApiModelsGet,
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
    enabled: false,
  })
}

export function useApiContext(): UseQueryResult<Context> {
  return useQuery({
    queryKey: ['/api/context'],
    queryFn: getApiContextApiContextGet,
    cacheTime: 0,
  })
}

export function useApiPlan(
  environment: string,
): UseQueryResult<ContextEnvironment> {
  return useQuery({
    queryKey: ['/api/plan', environment],
    queryFn: async () => await getPlanApiPlanGet({ environment }),
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

export async function useApiPlanCancel(
  client: QueryClient,
  environment: string,
): Promise<void> {
  await client.cancelQueries({ queryKey: ['/api/plan', environment] })
}

export function useApiDag(): UseQueryResult<DagApiCommandsDagGet200> {
  return useQuery({
    queryKey: ['/api/commands/dag'],
    queryFn: dagApiCommandsDagGet,
  })
}
