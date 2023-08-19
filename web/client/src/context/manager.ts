import {
  isArrayEmpty,
  isArrayNotEmpty,
  isFalse,
  isNil,
  isNotNil,
} from '@utils/index'
import { create } from 'zustand'
import { persist, subscribeWithSelector } from 'zustand/middleware'

export const EnumAction = {
  None: 'none',
  Meta: 'meta',
  Audits: 'audits',
  Tests: 'tests',
  Plan: 'plan',
  PlanApply: 'planApply',
  Backfill: 'backfill',
  Diff: 'diff',
  Query: 'query',
  ModelRender: 'modelRender',
  ModelEvaluate: 'modelEvaluate',
  EditorSaveChanges: 'editorSaveChanges',
  FileExplorerCreate: 'fileExplorerCreate',
  FileExplorerDelete: 'fileExplorerDelete',
  FileExplorerRename: 'fileExplorerRename',
  FileExplorerMove: 'fileExplorerMove',
  FileExplorerModify: 'fileExplorerModify',
  FileExplorerGet: 'fileExplorerGet',
} as const

export { useStoreActionManager }

export type Action = KeyOf<typeof EnumAction> | string
type ActionGroup = Action[]
interface ActionQueue {
  currentAction: Action
  currentCallback?: Callback
  currentCancel?: Callback
  queue: ActionHandler[]
}

interface ActionHandler {
  action: Action
  callback?: AsyncCallback | Callback
  cancel?: AsyncCallback | Callback
  onCallbackSuccess?: Callback
  onCallbackError?: Callback
  onCallbackFinally?: Callback
  onCancelSuccess?: Callback
  onCancelError?: Callback
  onCancelFinally?: Callback
  rules?: Array<Callback<ActionManager, boolean>>
}

interface ActionManager {
  queues: Map<ActionGroup, ActionQueue>
  currentActions: Action[]
  results: Record<Action, any>
  addCurrentAction: (action: Action) => void
  removeCurrentAction: (action: Action) => void
  resetQueueCurrentByAction: (action: Action) => void
  resetQueueCurrentByGroup: (group: ActionGroup) => void
  isRunningActionByGroup: (group?: ActionGroup, action?: Action) => boolean
  isRunningCancelActionByGroup: (
    group?: ActionGroup,
    action?: Action,
  ) => boolean
  isActiveAction: (action?: Action) => boolean
  enqueue: (payload: ActionHandler) => void
  enqueueMany: (payloads: ActionHandler[]) => void
  dequeue: () => void
  shouldLock: (action: Action) => boolean
}

// const groupOne = [
//   EnumAction.Plan,
//   EnumAction.PlanApply,
//   EnumAction.Backfill,
//   EnumAction.ModelEvaluate,
//   EnumAction.Audits,
//   EnumAction.Tests,
//   EnumAction.Diff,
// ]

// const groupTwo = [
//   EnumAction.FileExplorerGet,
//   EnumAction.FileExplorerCreate,
//   EnumAction.FileExplorerDelete,
//   EnumAction.FileExplorerRename,
//   EnumAction.FileExplorerMove,
//   EnumAction.FileExplorerModify,
// ]

//
const lock: Record<Action, ActionGroup> = {
  [EnumAction.Plan]: [EnumAction.PlanApply, EnumAction.Backfill],
  [EnumAction.PlanApply]: [
    EnumAction.Plan,
    EnumAction.Backfill,
    EnumAction.ModelEvaluate,
  ],
  [EnumAction.Backfill]: [EnumAction.ModelEvaluate],
  [EnumAction.Query]: [EnumAction.Backfill],
  [EnumAction.ModelEvaluate]: [
    EnumAction.Plan,
    EnumAction.PlanApply,
    EnumAction.Backfill,
    EnumAction.ModelRender,
  ],
  [EnumAction.FileExplorerGet]: [],
  // [EnumAction.Audits]: groupOne,
  // [EnumAction.Tests]: groupOne,
  // [EnumAction.Diff]: groupOne,
  // [EnumAction.FileExplorerGet]: groupTwo,
  // [EnumAction.FileExplorerCreate]: groupTwo,
  // [EnumAction.FileExplorerDelete]: groupTwo,
  // [EnumAction.FileExplorerRename]: groupTwo,
  // [EnumAction.FileExplorerMove]: groupTwo,
  // [EnumAction.FileExplorerModify]: groupTwo,
}

const actionsSorted = topologicalSort(lock)

console.log(actionsSorted)

const initialQueue = new Map<ActionGroup, ActionQueue>(
  Array.from(new Set(Object.values(lock))).map(group => [
    group,
    createEmptyActionQueue(),
  ]),
)

const useStoreActionManager = create(
  subscribeWithSelector(
    persist<ActionManager, [], [], { currentActions: Action[] }>(
      (set, get) => ({
        queues: initialQueue,
        currentActions: [],
        results: {},
        addCurrentAction(action) {
          const s = get()

          set({ currentActions: [...s.currentActions, action] })
        },
        removeCurrentAction(action) {
          const s = get()
          const index = s.currentActions.indexOf(action)

          if (index > -1) {
            s.currentActions.splice(index, 1)
          }

          set({ currentActions: structuredClone(s.currentActions) })
        },
        resetQueueCurrentByGroup(group) {
          const s = get()
          const q = s.queues.get(group)

          if (isNil(q)) return

          s.removeCurrentAction(q.currentAction)

          s.queues.set(
            group,
            createEmptyActionQueue(s.queues.get(group)?.queue),
          )

          set({ queues: new Map(s.queues) })

          void s.dequeue()
        },
        resetQueueCurrentByAction(action) {
          const s = get()
          const group = lock[action]

          if (isNil(group)) {
            s.removeCurrentAction(action)

            return
          }

          s.resetQueueCurrentByGroup(group)
        },
        enqueueMany(payloads = []) {
          payloads.forEach(payload => get().enqueue(payload))
        },
        enqueue(payload) {
          const s = get()
          const group = lock[payload.action]

          if (isNil(group)) {
            s.addCurrentAction(payload.action)

            s.results[payload.action] = undefined

            const callback = toCallback(payload, set, get)

            void callback()
          } else {
            const q = s.queues.get(group)

            if (isNil(q)) return

            const queue = q.queue.filter(
              ({ action, rules }) =>
                action !== payload.action && isArrayEmpty(rules),
            )

            queue.push(payload)

            s.queues.set(group, Object.assign(q, { queue }))

            set({ queues: new Map(s.queues) })

            void s.dequeue()
          }
        },
        dequeue() {
          const s = get()
          const [group, q] = getCandidate(actionsSorted, get)

          if (isNil(group) || isNil(q) || s.isRunningActionByGroup(group))
            return

          const payload = q.queue.shift()

          if (
            isNil(payload) ||
            isNil(payload.action) ||
            (isNil(payload.callback) && payload.action === EnumAction.None) ||
            (isArrayNotEmpty(payload.rules) &&
              payload.rules.some(rule => isFalse(rule(get()))))
          )
            return

          s.results[payload.action] = undefined

          const currentAction = payload.action
          const currentCallback = toCallback(payload, set, get)
          const currentCancel = toCancelCallback(payload)

          s.queues.set(group, {
            currentAction,
            currentCallback,
            currentCancel,
            queue: q.queue,
          })

          s.addCurrentAction(payload.action)

          set({ queues: new Map(s.queues) })

          currentCallback()
        },
        isRunningActionByGroup(group, action) {
          if (isNil(group)) return false

          const s = get()
          const q = s.queues.get(group)

          if (isNil(q)) return false

          return isNil(action)
            ? isNotNil(q.currentCallback)
            : isNotNil(q.currentCallback) && q.currentAction === action
        },
        isRunningCancelActionByGroup(group, action) {
          if (isNil(group)) return false

          const s = get()
          const q = s.queues.get(group)

          if (isNil(q)) return false

          return isNil(action)
            ? isNotNil(q.currentCancel)
            : isNotNil(q.currentCancel) && q.currentAction === action
        },
        isActiveAction(action): boolean {
          const s = get()

          return isNil(action)
            ? s.currentActions.length > 0
            : s.currentActions.includes(action)
        },
        shouldLock(action) {
          const s = get()
          const group = lock[action]

          if (isNil(group)) return false

          return (
            group.some(a => s.currentActions.includes(a)) ||
            s.currentActions.includes(action)
          )
        },
      }),
      {
        name: 'action-manager',
        partialize: s => ({ currentActions: s.currentActions }),
        onRehydrateStorage() {
          return s => {
            if (isNil(s)) return

            for (const action of s.currentActions) {
              const group = lock[action]

              if (isNil(group)) continue

              const q = s.queues.get(group)

              if (isNil(q)) continue

              q.currentAction = action
            }
          }
        },
      },
    ),
  ),
)

function toCallback(
  payload: ActionHandler,
  set: (
    partial:
      | ActionManager
      | Partial<ActionManager>
      | ((state: ActionManager) => ActionManager | Partial<ActionManager>),
    replace?: boolean | undefined,
  ) => void,
  get: () => ActionManager,
): Callback {
  return () => {
    if (isNil(payload.callback)) return

    try {
      ;(payload.callback as AsyncCallback)()
        .then(data => {
          payload.onCallbackSuccess?.(data, set, get)
        })
        .catch(error => {
          payload.onCallbackError?.(error, set, get)
        })
        .finally(() => {
          payload.onCallbackFinally?.(set, get)

          get().resetQueueCurrentByAction(payload.action)
        })
    } catch {
      ;(payload.callback as Callback)?.(set, get)

      get().resetQueueCurrentByAction(payload.action)
    }
  }
}

function toCancelCallback(payload: ActionHandler): Callback {
  return () => {
    if (isNil(payload.cancel)) return

    try {
      ;(payload.cancel as AsyncCallback)()
        .then(payload.onCancelSuccess)
        .catch(payload.onCancelError)
        .finally(() => {
          payload.onCancelFinally?.()
        })
    } catch {
      ;(payload.cancel as Callback)()
    }
  }
}

function createEmptyActionQueue(queue: ActionHandler[] = []): ActionQueue {
  return {
    currentAction: EnumAction.None,
    currentCallback: undefined,
    currentCancel: undefined,
    queue,
  }
}

function topologicalSort(graph: Record<Action, ActionGroup>): Action[] {
  const counts: Record<Action, number> = {}
  const result: Action[] = []

  for (const vertex in graph) {
    counts[vertex] = 0
  }
  for (const vertex in graph) {
    const naighbors = graph[vertex] ?? []
    for (const neighbor of naighbors) {
      counts[neighbor] = (counts[neighbor] ?? 0) + 1
    }
  }

  const queue: Action[] = []

  for (const vertex in counts) {
    if (counts[vertex] === 0) {
      queue.push(vertex)
    }
  }

  while (queue.length > 0) {
    const current = queue.shift()!

    result.push(current)

    const neighbors = graph[current] ?? []

    for (const neighbor of neighbors) {
      counts[neighbor]--

      if (counts[neighbor] === 0) {
        queue.push(neighbor)
      }
    }
  }

  for (const vertex in counts) {
    if (counts[vertex]! > 0) {
      result.push(vertex)
    }
  }

  return result
}

function getCandidate(
  actions: Action[],
  get: () => ActionManager,
): [ActionGroup, ActionQueue, boolean] {
  return actions.reduce((acc: any, action) => {
    const s = get()

    const group = lock[action]

    if (isNil(group)) return acc

    const q = s.queues.get(group)

    if (isNil(q)) return acc

    if (
      isArrayEmpty(q.queue) ||
      s.isRunningActionByGroup(group) ||
      (isNotNil(acc[2]) && isFalse(acc[2]))
    )
      return acc

    let hasDependency = false

    for (const action of group) {
      if (lock[action] !== group && isNotNil(lock[action])) {
        hasDependency = true

        if (
          s.isActiveAction(action) ||
          s.isRunningActionByGroup(lock[action])
        ) {
          return acc
        }
      }
    }

    return hasDependency && isArrayNotEmpty(acc)
      ? acc
      : [group, q, hasDependency]
  }, []) as [ActionGroup, ActionQueue, boolean]
}
