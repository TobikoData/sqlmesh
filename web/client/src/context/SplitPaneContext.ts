import { MouseEventHandler, createContext } from "react";

interface SplitPaneContext {
  clientHeight: number;
  clientWidth: number;
  setClientHeight?: (clientHeight: number) => void;
  setClientWidth?: (clientWidth: number) => void;
  onMouseDown?: MouseEventHandler;
}

const SplitPaneContext = createContext<SplitPaneContext>({
  clientHeight: 0,
  clientWidth: 0,
});

export default SplitPaneContext;
