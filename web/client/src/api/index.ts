import { useQuery, useMutation, QueryClient } from "@tanstack/react-query";
import { getFileByPath, getFiles, saveFileByPath } from "./endpoints";


export function useApiFileByPath(path?: string) {
  return useQuery({
    queryKey: [`/api/files`, path],
    queryFn: path ? () => getFileByPath(path) : undefined,
    enabled: !!path,
  });
}

export function useApiFiles() {
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
