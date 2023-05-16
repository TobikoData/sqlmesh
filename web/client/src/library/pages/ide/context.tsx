import { createContext, type ReactNode, useState, useContext } from 'react'

export const ERROR_KEY_GENERAL = 'general'

export interface Error {
  origin?: string
  message?: string
  traceback?: string
  timestamp: number
}

interface IDE {
  isPlanOpen: boolean
  setIsPlanOpen: (isPlanOpen: boolean) => void
  errors: Map<string, Error>
  addError: (key: string, error: Error) => void
  removeError: (key: string) => void
}

export const IDEContext = createContext<IDE>({
  isPlanOpen: false,
  errors: new Map(),
  setIsPlanOpen: () => {},
  addError: () => {},
  removeError: () => {},
})

export default function IDEProvider({
  children,
}: {
  children: ReactNode
}): JSX.Element {
  const [isPlanOpen, setIsPlanOpen] = useState(false)
  const [errors, setErrors] = useState<Map<string, Error>>(new Map())

  function addError(key: string, error: Error): void {
    setErrors(errors => {
      if (errors.has(ERROR_KEY_GENERAL)) {
        errors.delete(ERROR_KEY_GENERAL)
      }

      return new Map(
        errors.set(key, {
          origin: key,
          message: (error as any).message,
          traceback:
            (error as any).traceback ??
            (error as any).stack ??
            (error as any).detail,
          timestamp: Date.now(),
        }),
      )
    })
  }

  function removeError(key: string): void {
    setErrors(errors => {
      errors.delete(key)

      return new Map(errors)
    })
  }

  return (
    <IDEContext.Provider
      value={{
        isPlanOpen,
        setIsPlanOpen,
        errors,
        addError,
        removeError,
      }}
    >
      {children}
    </IDEContext.Provider>
  )
}

export function useIDE(): IDE {
  return useContext(IDEContext)
}
