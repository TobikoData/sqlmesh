import { type ApiExceptionPayload } from '@api/client'
import { createContext, type ReactNode, useState, useContext } from 'react'

export const EnumErrorKey = {
  General: 'general',
  RunPlan: 'run-plan',
  Environments: 'environments',
  Models: 'models',
  ApplyPlan: 'apply-plan',
  Fetchdf: 'fetchdf',
  EvaluateModel: 'evaluate-model',
  RenderModel: 'render-model',
  ColumnLineage: 'column-lineage',
  Meta: 'meta',
} as const

export type ErrorKey = (typeof EnumErrorKey)[keyof typeof EnumErrorKey]

export interface ErrorIDE extends ApiExceptionPayload {
  key: ErrorKey
}

interface IDE {
  isPlanOpen: boolean
  setIsPlanOpen: (isPlanOpen: boolean) => void
  errors: Map<ErrorKey, ErrorIDE>
  addError: (key: ErrorKey, error: ErrorIDE) => void
  removeError: (key: ErrorKey) => void
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
  const [errors, setErrors] = useState<Map<ErrorKey, ErrorIDE>>(new Map())

  function addError(key: ErrorKey, error: ErrorIDE): void {
    setErrors(errors => {
      if (errors.has(EnumErrorKey.General)) {
        errors.delete(EnumErrorKey.General)
      }

      return new Map(
        errors.set(key, {
          key,
          status: error.status,
          timestamp: error.timestamp ?? Date.now(),
          message: error.message,
          description: error.description,
          type: error.type,
          origin: error.origin,
          trigger: error.trigger,
          stack: error.stack,
          traceback: error.traceback,
        }),
      )
    })
  }

  function removeError(key: ErrorKey): void {
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
