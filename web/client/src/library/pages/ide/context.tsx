import { type ApiExceptionPayload } from '@api/client'
import { isString, uid } from '@utils/index'
import { createContext, type ReactNode, useState, useContext } from 'react'

export const EnumErrorKey = {
  Internal: 'internal',
  API: 'api',
  General: 'general',
  RunPlan: 'run-plan',
  ApplyPlan: 'apply-plan',
  CancelPlan: 'cancel-plan',
  Environments: 'environments',
  Models: 'models',
  Fetchdf: 'fetchdf',
  EvaluateModel: 'evaluate-model',
  RenderQuery: 'render-query',
  ColumnLineage: 'column-lineage',
  ModelLineage: 'model-lineage',
  Meta: 'meta',
  FileExplorer: 'file-explorer',
  Table: 'table',
  TableDiff: 'table-diff',
  SaveFile: 'save-file',
} as const

export type ErrorKey = (typeof EnumErrorKey)[keyof typeof EnumErrorKey]
export interface ErrorIDE extends ApiExceptionPayload {
  key: ErrorKey
  id: ID
  solution?: string
  tip?: string
}

interface IDE {
  errors: Set<ErrorIDE>
  isPlanOpen: boolean
  setIsPlanOpen: (isPlanOpen: boolean) => void
  addError: (
    key: ErrorKey,
    error: ApiExceptionPayload,
  ) => {
    removeError: () => void
    error: ErrorIDE
  }
  removeError: (error: ErrorIDE | ErrorKey) => void
}

export const IDEContext = createContext<IDE>({
  errors: new Set(),
  isPlanOpen: false,
  setIsPlanOpen: () => {},
  addError: () => ({ removeError: () => {}, error: {} as unknown as ErrorIDE }),
  removeError: () => {},
})

export default function IDEProvider({
  children,
}: {
  children: ReactNode
}): JSX.Element {
  const [isPlanOpen, setIsPlanOpen] = useState(false)
  const [errors, setErrors] = useState<Set<ErrorIDE>>(new Set())

  function addError(
    key: ErrorKey,
    error: ApiExceptionPayload,
  ): {
    removeError: () => void
    error: ErrorIDE
  } {
    const err = {
      id: uid(),
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
    }

    setErrors(errors => new Set(errors.add(err)))

    return {
      removeError: () => removeError(err),
      error: err,
    }
  }

  function removeError(error: ErrorIDE | ErrorKey): void {
    setErrors(
      errors =>
        new Set(
          Array.from(errors).filter(e =>
            isString(error) ? e.key !== error : e !== error,
          ),
        ),
    )
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
