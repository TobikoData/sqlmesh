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

interface ActionManager {
  currentAction: Action
  currentCallback?: Cancelable<AsyncCallback>
  queue: Array<[Action, Optional<() => Promise<any>>]>
  resetCurrentAction: () => void
  isRunningAction: (action?: Action) => boolean
  isWaitingAction: (action?: Action) => boolean
  isActiveAction: (action?: Action) => boolean
  enqueueAction: (action: Action, callback?: () => Promise<any>) => void
  dequeueAction: () => Promise<void>
  shouldLock: (action: Action) => boolean
}

const lockDefaul = [
  EnumAction.Audits,
  EnumAction.Plan,
  EnumAction.PlanApply,
  EnumAction.Tests,
]

const lock: Record<string, Action[]> = {
  [EnumAction.Plan]: lockDefaul,
  [EnumAction.PlanApply]: lockDefaul,
  [EnumAction.Audits]: lockDefaul,
  [EnumAction.Tests]: lockDefaul,
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

          if (isNotNil(s.currentCallback)) {
            s.currentCallback.cancel?.()
          }

          set({
            currentAction: EnumAction.None,
            currentCallback: undefined,
            queue: s.queue.slice(),
          })

          void s.dequeueAction()
        },
        isRunningAction(action?: Action) {
          const s = get()

          return isNil(action)
            ? isNotNil(s.currentCallback)
            : isNotNil(s.currentCallback) && s.currentAction === action
        },
        isWaitingAction(action?: Action) {
          const s = get()

          return isNil(action)
            ? isNil(s.currentCallback) && s.currentAction !== EnumAction.None
            : isNil(s.currentCallback) && s.currentAction === action
        },
        isActiveAction(action?: Action): boolean {
          const s = get()

          return isNil(action)
            ? s.currentAction !== EnumAction.None
            : s.currentAction === action
        },
        shouldLock(action: Action) {
          const s = get()

          return lock[s.currentAction]?.includes(action) ?? false
        },
        enqueueAction(action, callback) {
          const s = get()

          console.log('enqueueAction', action, s.queue)

          if (action === s.currentAction && isNil(callback)) return

          const queue = s.queue.filter(([a]) => a !== action)

          queue.push([action, callback])

          set({ queue })

          if (action === s.currentAction && isNotNil(callback)) {
            s.resetCurrentAction()
          } else {
            void s.dequeueAction()
          }
        },
        async dequeueAction() {
          const s = get()

          if (
            s.isActiveAction() ||
            s.isRunningAction(EnumAction.None) ||
            s.queue.length < 1
          )
            return

          const [currentAction, callback] = s.queue.shift() ?? []

          if (isNil(currentAction)) {
            s.resetCurrentAction()
          } else {
            if (isNil(callback) && currentAction === EnumAction.None) {
              s.resetCurrentAction()
            } else {
              set({
                currentAction, // currentAction should not be None
                currentCallback: callback,
              })

              if (isNil(callback)) return

              try {
                await callback()
              } catch (error) {
                console.log(error)
              }

              s.resetCurrentAction()
            }
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

// Debug, should be renmoved
useStoreActionManager.subscribe(
  s => s.currentAction,
  currentAction => {
    const s = useStoreActionManager.getState()

    console.table({
      currentAction,
      queue: s.queue,
    })
  },
)
