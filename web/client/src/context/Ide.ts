import { createContext } from "react";
import type { File } from '../api/client';

interface IDE {
  openedFiles: Set<File>
  activeFile: File | null
  setActiveFile: (file: File | null) => void
  setOpenedFiles: (files: Set<File>) => void
}

export default createContext<IDE>({
  openedFiles: new Set(),
  activeFile: null,
  setActiveFile: () => {},
  setOpenedFiles: () => {}
})
