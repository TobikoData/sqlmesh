import { ModelDirectory } from "./directory";

export interface Artifact {
  name: string;
  path: string;
  parent: ModelDirectory | undefined;

  rename(newName: string): void;
}