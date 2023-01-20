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

export async function getFiles(): Promise<Payload | undefined> {
  return await (await fetch("/api/files")).json()
}

export async function getFileByPath(path?: string): Promise<File | undefined> {
  return await (await fetch(`/api/files/${path}`)).json()
}

export async function saveFileByPath({ path, body = '' }: any): Promise<void> {
  await fetch(`/api/files/${path}`, { method: 'post', body })
}

export function useApiFileByPath(client: QueryClient, path?: string): UseQueryResult<File> {
  return useQuery({
    queryKey: [`/api/files`, path],
    queryFn: () => {
      client.cancelQueries({ queryKey: [`/api/files`, path] })
      
      return getFileByPath(path)
    },
    onSuccess: () => {
    },
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
    // When mutate is called:
    onMutate: async ({ path, body = '' }: any) => {

      // Cancel any outgoing refetches
      // (so they don't overwrite our optimistic update)
      await client.cancelQueries({ queryKey: [`/api/files`, path] })
  
      // // Snapshot the previous value
      // const previousTodos = client.getQueryData<Todos>(['todos'])
  
      // // Optimistically update to the new value
      // if (previousTodos) {
      //   client.setQueryData<Todos>(['todos'], {
      //     ...previousTodos,
      //     items: [
      //       ...previousTodos.items,
      //       { id: Math.random().toString(), text: newTodo },
      //     ],
      //   })
      // }
  
      // return { previousTodos }
    }
  })
} 