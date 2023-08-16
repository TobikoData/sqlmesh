import { isNil, isNotNil } from '@utils/index'
import { create } from 'zustand'
import { persist, subscribeWithSelector } from 'zustand/middleware'

export const EnumAction = {
  None: 'none',
  Audits: 'audits',
  Tests: 'tests',
  Plan: 'plan',
  PlanApply: 'planApply',
  Diff: 'diff',
  Query: 'query',
  ModelRender: 'modelRender',
  ModelEvaluate: 'modelEvaluate',
  FileExplorerCreate: 'fileExplorerCreate',
  FileExplorerDelete: 'fileExplorerDelete',
  FileExplorerRename: 'fileExplorerRename',
  FileExplorerMove: 'fileExplorerMove',
  FileExplorerModify: 'fileExplorerModify',
} as const

export { useStoreActionManager }

export type Action = KeyOf<typeof EnumAction>
interface ActionHandel {
  action: Action
  callback?: AsyncCallback | Callback
  cancel?: AsyncCallback | Callback
  onCallbackSuccess?: Callback
  onCallbackError?: Callback
  onCallbackFinally?: Callback
  onCancelSuccess?: Callback
  onCancelError?: Callback
  onCancelFinally?: Callback
}

interface ActionManager {
  currentAction: Action
  currentCallback?: AsyncCallback | Callback
  currentCancel?: Callback
  queue: ActionHandel[]
  resetCurrentAction: () => void
  cancelCurrentAction: () => void
  isRunningAction: (action?: Action) => boolean
  isRunningActionCancel: (action?: Action) => boolean
  isWaitingAction: (action?: Action) => boolean
  isActiveAction: (action?: Action) => boolean
  enqueueAction: (payload: ActionHandel) => void
  dequeueAction: () => void
  shouldLock: (action: Action) => boolean
}

const lockDefault = [
  EnumAction.Audits,
  EnumAction.Plan,
  EnumAction.PlanApply,
  EnumAction.Tests,
  EnumAction.Diff,
  EnumAction.ModelEvaluate,
]

const lock: Record<string, Action[]> = {
  [EnumAction.Plan]: lockDefault,
  [EnumAction.PlanApply]: lockDefault,
  [EnumAction.Audits]: lockDefault,
  [EnumAction.Tests]: lockDefault,
  [EnumAction.Diff]: lockDefault,
  [EnumAction.ModelEvaluate]: lockDefault,
}

const useStoreActionManager = create(
  subscribeWithSelector(
    persist<ActionManager, [], [], { currentAction: Action }>(
      (set, get) => ({
        isDequeueing: false,
        currentAction: EnumAction.None,
        queue: [],
        resetCurrentAction() {
          const s = get()

          set({
            currentAction: EnumAction.None,
            currentCallback: undefined,
            currentCancel: undefined,
            queue: s.queue.slice(),
          })

          void s.dequeueAction()
        },
        cancelCurrentAction() {
          const s = get()

          if (isNotNil(s.currentCancel)) {
            s.currentCancel()
          }

          set({
            currentAction: EnumAction.None,
            currentCallback: undefined,
            currentCancel: undefined,
            queue: s.queue.slice(),
          })

          void s.dequeueAction()
        },
        isRunningAction(action) {
          const s = get()

          return isNil(action)
            ? isNotNil(s.currentCallback)
            : isNotNil(s.currentCallback) && s.currentAction === action
        },
        isRunningActionCancel(action) {
          const s = get()

          return isNil(action)
            ? isNotNil(s.currentCancel)
            : isNotNil(s.currentCancel) && s.currentAction === action
        },
        isWaitingAction(action) {
          const s = get()

          return isNil(action)
            ? isNil(s.currentCallback) && s.currentAction !== EnumAction.None
            : isNil(s.currentCallback) && s.currentAction === action
        },
        isActiveAction(action): boolean {
          const s = get()

          return isNil(action)
            ? s.currentAction !== EnumAction.None
            : s.currentAction === action
        },
        shouldLock(action) {
          const s = get()

          return lock[s.currentAction]?.includes(action) ?? false
        },
        enqueueAction(payload) {
          const s = get()

          if (payload.action === s.currentAction && isNil(payload.callback))
            return

          const queue = s.queue.filter(({ action: a }) => a !== payload.action)

          queue.push(payload)

          set({ queue })

          if (
            payload.action === s.currentAction &&
            isNotNil(payload.callback)
          ) {
            s.resetCurrentAction()
          } else {
            void s.dequeueAction()
          }
        },
        dequeueAction() {
          const s = get()

          if (
            s.isActiveAction() ||
            s.isRunningAction(EnumAction.None) ||
            s.queue.length < 1
          )
            return

          const payload = s.queue.shift()

          if (
            isNil(payload) ||
            isNil(payload.action) ||
            (isNil(payload.callback) && payload.action === EnumAction.None)
          ) {
            s.resetCurrentAction()
          } else {
            const currentAction = payload.action
            const currentCallback = (): void => {
              if (isNil(payload.callback)) return

              try {
                ;(payload.callback as AsyncCallback)()
                  .then(payload.onCallbackSuccess)
                  .catch(payload.onCallbackError)
                  .finally(() => {
                    payload.onCallbackFinally?.()

                    s.resetCurrentAction()
                  })
              } catch {
                ;(payload.callback as Callback)()

                s.resetCurrentAction()
              }
            }

            const currentCancel = (): void => {
              if (isNil(payload.cancel)) return

              try {
                ;(payload.cancel as AsyncCallback)()
                  .then(payload.onCancelSuccess)
                  .catch(payload.onCancelError)
                  .finally(payload.onCancelFinally)
              } catch {
                ;(payload.cancel as Callback)()
              }
            }

            set({
              currentAction,
              currentCallback,
              currentCancel,
            })

            currentCallback()
          }
        },
      }),
      {
        name: 'action-manager',
        partialize: s => ({ currentAction: s.currentAction }),
      },
    ),
  ),
)
