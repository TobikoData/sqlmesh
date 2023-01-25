import { useQuery, useMutation, QueryClient } from '@tanstack/react-query'
import { getFileByPath, getFiles, getContext, saveFileByPath, getContextByEnvironment } from './endpoints'

export function useApiFileByPath(path?: string) {
  return useQuery({
    queryKey: [`/api/files`, path],
    queryFn: path ? () => getFileByPath(path) : undefined,
    enabled: !!path,
  })
}

export function useApiFiles() {
  return useQuery({
    queryKey: ['/api/files'],
    queryFn: getFiles,
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
