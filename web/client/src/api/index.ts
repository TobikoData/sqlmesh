import { useQuery, useMutation, QueryClient } from '@tanstack/react-query'
import { saveFileByPath } from './endpoints'
import { getFileApiFilesPathGet, getFilesApiFilesGet, getPlanApiPlanGet, getApiContextApiContextGet } from "./client";

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
    queryFn: getApiContextApiContextGet,
  })
}

export async function useApiContextCancel(client: QueryClient) {
  await client.cancelQueries({ queryKey: [`/api/context`] })
}

export function useApiContextByEnvironment(environment?: string) {
  return useQuery({
    queryKey: [`/api/plan`, environment],
    queryFn: () => getPlanApiPlanGet({ environment }),
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
