import { useQuery, useMutation, QueryClient } from '@tanstack/react-query'
import { saveFileByPath, getContext } from './endpoints'
import { getFileApiFilesPathGet, getFilesApiFilesGet, getApiContextByEnvironmentApiContextEnvironmentGet } from "./client";

export function useApiFileByPath(path?: string) {
  return useQuery({
    queryKey: [`/api/files`, path],
    queryFn: path ? () => getFileApiFilesPathGet(path) : undefined,
    enabled: !!path,
  })
}

export function useApiFiles() {
  return useQuery({
    queryKey: ['/api/files'],
    queryFn: getFilesApiFilesGet,
  })
}

export function useApiContext() {
  return useQuery({
    queryKey: ['/api/context'],
    queryFn: getContext,
  })
}

export function useApiContextByEnvironment(environment?: string) {
  return useQuery({
    queryKey: [`/api/context/`, environment],
    queryFn: () => getApiContextByEnvironmentApiContextEnvironmentGet(environment || 'prod'),
    enabled: environment != null,
  })
}

export function useMutationApiSaveFile(client: QueryClient) {
  return useMutation({
    mutationFn: saveFileByPath,
    onMutate: async ({ path }: any) => {
      await client.cancelQueries({ queryKey: [`/api/files`, path] })
    },
  })
}
