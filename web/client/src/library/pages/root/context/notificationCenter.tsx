import { type ApiExceptionPayload } from '@api/client'
import { uid } from '@utils/index'
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
  Modules: 'modules',
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

interface NotificationCenter {
  errors: Set<ErrorIDE>
  addError: (
    key: ErrorKey,
    error: ApiExceptionPayload,
  ) => {
    removeError: () => void
    error: ErrorIDE
  }
  removeError: (error: ErrorIDE | ErrorKey) => void
  clearErrors: () => void
}

export const NotificationCenterContext = createContext<NotificationCenter>({
  errors: new Set(),
  addError: () => ({ removeError: () => {}, error: {} as unknown as ErrorIDE }),
  removeError: () => {},
  clearErrors: () => {},
})

export default function NotificationCenterProvider({
  children,
}: {
  children: ReactNode
}): JSX.Element {
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
    setErrors(errors => {
      errors.forEach(err => {
        if (err === error || err.key === error) {
          errors.delete(err)
        }
      })

      return new Set(errors)
    })
  }

  function clearErrors(): void {
    setErrors(new Set())
  }

  return (
    <NotificationCenterContext.Provider
      value={{
        errors,
        addError,
        removeError,
        clearErrors,
      }}
    >
      {children}
    </NotificationCenterContext.Provider>
  )
}

export function useNotificationCenter(): NotificationCenter {
  return useContext(NotificationCenterContext)
}
