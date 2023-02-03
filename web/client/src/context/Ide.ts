import { createContext } from "react";
import { ModelFile } from "../models/file";

interface IDE {
  openedFiles: Set<ModelFile>
  activeFile: ModelFile | null
  setActiveFile: (file: ModelFile | null) => void
  setOpenedFiles: (files: Set<ModelFile>) => void
}

export default createContext<IDE>({
  openedFiles: new Set(),
  activeFile: null,
  setActiveFile: () => {},
  setOpenedFiles: () => {}
})
