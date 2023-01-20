import { createContext } from "react";
import { type File } from '../api';

interface IDE {
  files: Set<File>
  file?: File
  setFile: (file?: File) => void
  setFiles: (files: Set<File>) => void
}

export default createContext<IDE>({
  files: new Set(),
  file: undefined,
  setFile: () => {},
  setFiles: () => {}
})