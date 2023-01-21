import { UseQueryResult, useQuery, useMutation, QueryClient } from "@tanstack/react-query";

export type File = {
  name: string
  path: string
  is_supported: boolean
  content: string
  extension: string
};

export type Directory = {
  name: string
  path: string
  directories: Directory[]
  files: File[]
};

type Payload = {
  name: string,
  directories: Directory[]
  files: File[]
}

export async function getFiles(): Promise<Payload | null> {
  return await (await fetch("/api/files")).json()
}

export async function getFileByPath(path: string): Promise<File | null> {
  return await (await fetch(`/api/files/${path}`)).json()
}

export async function saveFileByPath({ path, body = '' }: any): Promise<void> {
  await fetch(`/api/files/${path}`, { method: 'post', body })
}

export function useApiFileByPath(path?: string): UseQueryResult<File> {
  return useQuery({
    queryKey: [`/api/files`, path],
    queryFn: path ? () => getFileByPath(path) : undefined,
    enabled: !!path,
  });
}

export function useApiFiles(): UseQueryResult<Payload> {
  return useQuery({
    queryKey: ["/api/files"],
    queryFn: getFiles,
  });
}

export function useMutationApiSaveFile(client: QueryClient) {
  return useMutation({
    mutationFn: saveFileByPath,
    onMutate: async ({ path }: any) => {
      await client.cancelQueries({ queryKey: [`/api/files`, path] })
    }
  })
} 