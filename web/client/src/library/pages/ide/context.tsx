import { createContext, type ReactNode, useState, useContext } from 'react'

interface IDE {
  isPlanOpen: boolean
  setIsPlanOpen: (isPlanOpen: boolean) => void
}

export const IDEContext = createContext<IDE>({
  isPlanOpen: false,
  setIsPlanOpen: () => {},
})

export default function IDEProvider({
  children,
}: {
  children: ReactNode
}): JSX.Element {
  const [isPlanOpen, setIsPlanOpen] = useState(false)

  return (
    <IDEContext.Provider
      value={{
        isPlanOpen,
        setIsPlanOpen,
      }}
    >
      {children}
    </IDEContext.Provider>
  )
}

export function useIDE(): IDE {
  return useContext(IDEContext)
}
