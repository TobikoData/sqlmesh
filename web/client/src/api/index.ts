import {
  useQuery,
  useMutation,
  QueryClient,
  UseQueryResult,
  UseMutationResult,
} from '@tanstack/react-query'
import { saveFileByPath } from './endpoints'
import {
  getFileApiFilesPathGet,
  getFilesApiFilesGet,
  getPlanApiPlanGet,
  getApiContextApiContextGet,
  ContextEnvironment,
} from './client'
import type { File, Directory, Context } from './client'

export function useApiFileByPath(path?: string): UseQueryResult<File> {
  return useQuery({
    queryKey: [`/api/files`, path],
    queryFn: path != null ? async () => await getFileApiFilesPathGet(path) : undefined,
    enabled: path != null,
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

export function useApiContext(): UseQueryResult<Context> {
  return useQuery({
    queryKey: ['/api/context'],
    queryFn: getApiContextApiContextGet,
    cacheTime: 0,
  })
}

export function useApiContextByEnvironment(value?: string): UseQueryResult<ContextEnvironment> {
  const environment = value ?? ''

  return useQuery({
    queryKey: [`/api/plan`, environment],
    queryFn: async () => await getPlanApiPlanGet({ environment }),
    enabled: false,
    cacheTime: 0,
  })
}

export function useMutationApiSaveFile<T extends object>(
  client: QueryClient,
  callbacks: {
    onSuccess?: () => void
    onMutate?: () => void
  }
): UseMutationResult<File, unknown, { path: string; body: T }, void> {
  return useMutation({
    mutationFn: saveFileByPath<T>,
    async onMutate({ path }) {
      await client.cancelQueries({
        queryKey: [`/api/files`, path],
      })

      if (callbacks.onMutate != null) {
        callbacks.onMutate()
      }
    },
    async onSuccess({ path }) {
      await client.invalidateQueries({
        queryKey: [`/api/files`, path],
      })

      if (callbacks.onSuccess != null) {
        callbacks.onSuccess()
      }
    },
  })
}

export async function useApiContextCancel(client: QueryClient): Promise<void> {
  await client.cancelQueries({ queryKey: [`/api/context`] })
}
