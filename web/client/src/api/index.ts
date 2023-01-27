import { useQuery, useMutation, QueryClient } from '@tanstack/react-query'
import { saveFileByPath, getContext, getContextByEnvironment } from './endpoints'
import { getFileApiFilesPathGet, getFilesApiFilesGet } from "./client";

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
    queryFn: () => getContextByEnvironment(environment),
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
