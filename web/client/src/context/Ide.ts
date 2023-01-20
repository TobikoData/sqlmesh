import { createContext } from "react";
import { type File } from '../api';
interface IdeContext {
  files?: Set<File>
  file?: File
  setFile?: (file: File) => void
}

const IdeContext = createContext<IdeContext>({
  files: new Set()
});

export default IdeContext;
