import { UseQueryResult, useQuery } from "@tanstack/react-query";

export type File = {
  id: number;
  name: string;
  is_supported: boolean;
  value: string;
  extension: string;
};

export type Folder = {
  id: number;
  name: string;
  files: File[];
  folders: Folder[];
};

type Payload = {
  folders: Folder[];
  files: File[];
};

export async function getProjectStructure(): Promise<Payload> {
  const data = await (await fetch("/api/v1/projects/1/structure")).json();

  if (data.ok) return data.payload;

  return { folders: [], files: [] };
}

export function useProjectStructure(): UseQueryResult<Payload> {
  return useQuery({
    queryKey: ["/api/v1/projects/1/structure"],
    queryFn: getProjectStructure,
  });
}
