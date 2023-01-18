import { UseQueryResult, useQuery } from "@tanstack/react-query";

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
  directories: Directory[]
  files: File[]
}

export async function getProjectStructure(): Promise<Payload> {
  const [ directories = [], files = [] ] = await (await fetch("/api/files")).json();

  return {
    directories,
    files
  };
}

export function useApiFiles(): UseQueryResult<Payload> {
  return useQuery({
    queryKey: ["/api/v1/files"],
    queryFn: getProjectStructure,
  });
}
