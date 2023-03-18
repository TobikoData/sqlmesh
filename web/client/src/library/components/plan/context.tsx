import {
  createContext,
  type Dispatch,
  type ReactNode,
  useContext,
  useReducer,
} from 'react'
import {
  type ContextEnvironmentBackfill,
  type ContextEnvironmentEnd,
  type ContextEnvironmentStart,
  type ModelsDiff,
  type ChangeDirect,
  type ContextEnvironmentChanges,
  SnapshotChangeCategory,
} from '~/api/client'
import { type PlanProgress } from '~/context/plan'
import { isArrayNotEmpty } from '~/utils'
import { isModified } from './help'

export const EnumPlanActions = {
  ResetPlanOptions: 'reset-plan-options',
  ResetBackfills: 'reset-backfills',
  ResetChanges: 'reset-changes',
  Dates: 'dates',
  DateStart: 'date-start',
  DateEnd: 'date-end',
  Category: 'category',
  Backfills: 'backfills',
  BackfillProgress: 'backfill-progress',
  Changes: 'changes',
  PlanOptions: 'plan-options',
  External: 'external',
} as const

export const EnumPlanChangeType = {
  Add: 'add',
  Remove: 'remove',
  Direct: 'direct',
  Indirect: 'indirect',
  Metadata: 'metadata',
} as const

export const EnumCategoryType = {
  BreakingChange: 'breaking-change',
  NonBreakingChange: 'non-breaking-change',
  NoChange: 'no-change',
} as const

export type PlanActions = KeyOf<typeof EnumPlanActions>
export type PlanChangeType = KeyOf<typeof EnumPlanChangeType>
export type CategoryType = KeyOf<typeof EnumCategoryType>

export interface Category {
  id: CategoryType
  name: string
  description: string
  value: SnapshotChangeCategory
}

export interface ChangeCategory {
  change: ChangeDirect
  category: Category
}

interface PlanOptions {
  skip_tests: boolean
  no_gaps: boolean
  skip_backfill: boolean
  forward_only: boolean
  auto_apply: boolean
  no_auto_categorization: boolean
  create_from?: string
  restate_models?: string
}

interface PlanChanges extends ContextEnvironmentChanges {
  hasChanges: boolean
  hasDirect: boolean
  hasIndirect: boolean
  hasMetadata: boolean
  hasAdded: boolean
  hasRemoved: boolean
}

interface PlanBackfills {
  hasLogicalUpdate: boolean
  hasBackfills: boolean
  backfills: ContextEnvironmentBackfill[]
  activeBackfill?: PlanProgress
}

interface PlanDetails extends PlanOptions, PlanChanges, PlanBackfills {
  start?: ContextEnvironmentStart
  end?: ContextEnvironmentEnd
  logicalUpdateDescription: string
  isInitialPlanRun: boolean
  categories: Category[]
  change_categorization: Map<string, ChangeCategory>
}

type PlanAction = { type: PlanActions } & Partial<PlanDetails> &
  Partial<ChangeCategory> & { modified?: ModelsDiff }

const [defaultCategory, categories] = useCategories()
const initial = {
  start: undefined,
  end: undefined,

  skip_tests: false,
  no_gaps: false,
  skip_backfill: false,
  forward_only: false,
  auto_apply: false,
  no_auto_categorization: false,
  from: undefined,
  restate_models: undefined,
  create_from: undefined,

  categories,
  defaultCategory,
  change_categorization: new Map(),

  hasChanges: false,
  hasDirect: false,
  hasIndirect: false,
  hasMetadata: false,
  hasAdded: false,
  hasRemoved: false,

  added: [],
  removed: [],
  modified: {
    direct: [],
    indirect: [],
    metadata: [],
  },

  hasLogicalUpdate: false,
  logicalUpdateDescription:
    'All changes and their downstream dependencies can be fully previewed before they get promoted. If during plan creation no data gaps have been detected and only references to new model versions need to be updated, then such update is referred to as logical. Logical updates impose no additional runtime overhead or cost.',
  activeBackfill: undefined,
  hasBackfills: false,
  backfills: [],
  isInitialPlanRun: false,
}

export const PlanContext = createContext<PlanDetails>(initial)
export const PlanDispatchContext = createContext<
  Dispatch<PlanAction | PlanAction[]>
>(() => {})

export default function PlanProvider({
  children,
}: {
  children: ReactNode
}): JSX.Element {
  const [planDetails, dispatch] = useReducer(reducers, Object.assign(initial))

  return (
    <PlanContext.Provider value={Object.assign(planDetails)}>
      <PlanDispatchContext.Provider value={dispatch}>
        {children}
      </PlanDispatchContext.Provider>
    </PlanContext.Provider>
  )
}

export function usePlan(): PlanDetails {
  return useContext(PlanContext)
}

export function usePlanDispatch(): Dispatch<PlanAction | PlanAction[]> {
  return useContext(PlanDispatchContext)
}

function reducers(
  plan: PlanDetails,
  actions: PlanAction | PlanAction[],
): PlanDetails {
  actions = Array.isArray(actions) ? actions : [actions]

  return actions.reduce(reducer, plan)
}

function reducer(
  plan: PlanDetails = initial,
  { type, ...newState }: PlanAction,
): PlanDetails {
  switch (type) {
    case EnumPlanActions.ResetPlanOptions: {
      return Object.assign<Record<string, unknown>, PlanDetails, PlanOptions>(
        {},
        plan,
        {
          skip_tests: false,
          no_gaps: false,
          skip_backfill: false,
          forward_only: false,
          auto_apply: false,
          no_auto_categorization: false,
          create_from: undefined,
          restate_models: undefined,
        },
      )
    }
    case EnumPlanActions.PlanOptions: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Partial<PlanOptions>
      >({}, plan, newState as Partial<PlanOptions>)
    }
    case EnumPlanActions.ResetBackfills: {
      return Object.assign<Record<string, unknown>, PlanDetails, PlanBackfills>(
        {},
        plan,
        {
          hasLogicalUpdate: false,
          activeBackfill: undefined,
          hasBackfills: false,
          backfills: [],
        },
      )
    }
    case EnumPlanActions.ResetChanges: {
      return Object.assign<Record<string, unknown>, PlanDetails, PlanChanges>(
        {},
        plan,
        {
          hasChanges: false,
          hasDirect: false,
          hasIndirect: false,
          hasMetadata: false,
          hasAdded: false,
          hasRemoved: false,
          added: [],
          removed: [],
          modified: {
            direct: [],
            indirect: [],
            metadata: [],
          },
        },
      )
    }
    case EnumPlanActions.Dates: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'start' | 'end'>
      >({}, plan, {
        start: newState.start,
        end: newState.end,
      })
    }
    case EnumPlanActions.External: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'isInitialPlanRun'>
      >({}, plan, {
        isInitialPlanRun: newState.isInitialPlanRun ?? false,
      })
    }
    case EnumPlanActions.DateStart: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'start'>
      >({}, plan, {
        start: newState.start,
      })
    }
    case EnumPlanActions.DateEnd: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'end'>
      >({}, plan, {
        end: newState.end,
      })
    }
    case EnumPlanActions.BackfillProgress: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'activeBackfill'>
      >({}, plan, {
        activeBackfill: newState.activeBackfill,
      })
    }
    case EnumPlanActions.Backfills: {
      const backfills =
        (newState as { backfills: ContextEnvironmentBackfill[] }).backfills ??
        []

      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'backfills' | 'hasBackfills'>
      >({}, plan, {
        backfills,
        hasBackfills: isArrayNotEmpty(backfills),
      })
    }
    case EnumPlanActions.Category: {
      const { change, category } = newState as ChangeCategory

      if (change?.model_name != null) {
        plan.change_categorization.set(change.model_name, {
          category,
          change,
        })
      }

      console.log({ change, category }, plan.change_categorization)

      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        { change_categorization: Map<string, ChangeCategory> }
      >({}, plan, {
        change_categorization: new Map(plan.change_categorization),
      })
    }
    case EnumPlanActions.Changes: {
      const { modified, added = [], removed = [] } = newState
      const hasChanges = [
        isModified(modified),
        isArrayNotEmpty(added),
        isArrayNotEmpty(removed),
      ].some(Boolean)

      const change_categorization = new Map()

      modified?.direct?.forEach(changeDirect => {
        if (changeDirect?.model_name != null) {
          change_categorization.set(changeDirect.model_name, {
            change: changeDirect,
            category: defaultCategory,
          })
        }
      })

      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        PlanChanges,
        { change_categorization: Map<string, ChangeCategory> }
      >(
        {},
        plan,
        {
          modified: {
            direct: modified?.direct ?? [],
            indirect: modified?.indirect ?? [],
            metadata: modified?.metadata ?? [],
          },
          added,
          removed,
          hasChanges,
          hasDirect: isArrayNotEmpty(modified?.direct),
          hasIndirect: isArrayNotEmpty(modified?.indirect),
          hasMetadata: isArrayNotEmpty(modified?.metadata),
          hasAdded: isArrayNotEmpty(added),
          hasRemoved: isArrayNotEmpty(removed),
        },
        {
          change_categorization,
        },
      )
    }
    default: {
      return Object.assign({}, plan)
    }
  }
}

function useCategories(): [Category, Category[]] {
  const categoryBreakingChange: Category = {
    id: EnumCategoryType.BreakingChange,
    name: 'Breaking Change',
    description: 'This is a breaking change',
    value: SnapshotChangeCategory.NUMBER_1,
  }
  const categories = [
    categoryBreakingChange,
    {
      id: EnumCategoryType.NonBreakingChange,
      name: 'Non-Breaking Change',
      description: 'This is a non-breaking change',
      value: SnapshotChangeCategory.NUMBER_2,
    },
    {
      id: EnumCategoryType.NoChange,
      name: 'No Change',
      description: 'This is a no change',
      value: SnapshotChangeCategory.NUMBER_3,
    },
  ]

  return [categoryBreakingChange, categories]
}
