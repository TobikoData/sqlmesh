import {
  createContext,
  Dispatch,
  ReactNode,
  useContext,
  useReducer,
} from 'react'
import {
  ContextEnvironmentBackfill,
  ContextEnvironmentChanges,
  ContextEnvironmentEnd,
  ContextEnvironmentStart,
  SnapshotChangeCategory,
} from '~/api/client'
import { PlanProgress } from '~/context/plan'
import { isArrayNotEmpty } from '~/utils'
import { isModified } from './help'

export const EnumPlanActions = {
  ResetAdditionalOptions: 'reset-additional-options',
  ResetBackfills: 'reset-backfills',
  ResetChanges: 'reset-changes',
  Dates: 'dates',
  DateStart: 'date-start',
  DateEnd: 'date-end',
  Category: 'category',
  Backfills: 'backfills',
  BackfillProgress: 'backfill-progress',
  Changes: 'changes',
  AdditionalOptions: 'additional-options',
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

interface PlanAdditionalOptions {
  skip_tests: boolean
  no_gaps: boolean
  skip_backfill: boolean
  forward_only: boolean
  auto_apply: boolean
  no_auto_categorization: boolean
  create_from?: string
  restate_models?: string
}

interface PlanChanges extends Partial<ContextEnvironmentChanges> {
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
  change_category: Category
}

interface PlanDetails
  extends PlanAdditionalOptions,
    PlanChanges,
    PlanBackfills {
  start?: ContextEnvironmentStart
  end?: ContextEnvironmentEnd
  categories: Category[]
  logicalUpdateDescription: string
  isInitialPlanRun: boolean
}

type PlanAction = { type: PlanActions } & Partial<PlanDetails>

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

  change_category: defaultCategory,
  categories,

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
    case EnumPlanActions.ResetAdditionalOptions: {
      return Object.assign<{}, PlanDetails, PlanAdditionalOptions>({}, plan, {
        skip_tests: false,
        no_gaps: false,
        skip_backfill: false,
        forward_only: false,
        auto_apply: false,
        no_auto_categorization: false,
        create_from: undefined,
        restate_models: undefined,
      })
    }
    case EnumPlanActions.AdditionalOptions: {
      return Object.assign<{}, PlanDetails, Partial<PlanAdditionalOptions>>(
        {},
        plan,
        newState as Partial<PlanAdditionalOptions>,
      )
    }
    case EnumPlanActions.ResetBackfills: {
      return Object.assign<{}, PlanDetails, PlanBackfills>({}, plan, {
        hasLogicalUpdate: false,
        activeBackfill: undefined,
        hasBackfills: false,
        backfills: [],
        change_category: defaultCategory,
      })
    }
    case EnumPlanActions.ResetChanges: {
      return Object.assign<{}, PlanDetails, PlanChanges>({}, plan, {
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
      })
    }
    case EnumPlanActions.Dates: {
      return Object.assign<{}, PlanDetails, Pick<PlanDetails, 'start' | 'end'>>(
        {},
        plan,
        {
          start: newState.start,
          end: newState.end,
        },
      )
    }
    case EnumPlanActions.External: {
      return Object.assign<
        {},
        PlanDetails,
        Pick<PlanDetails, 'isInitialPlanRun'>
      >({}, plan, {
        isInitialPlanRun: newState.isInitialPlanRun ?? false,
      })
    }
    case EnumPlanActions.DateStart: {
      return Object.assign<{}, PlanDetails, Pick<PlanDetails, 'start'>>(
        {},
        plan,
        {
          start: newState.start,
        },
      )
    }
    case EnumPlanActions.DateEnd: {
      return Object.assign<{}, PlanDetails, Pick<PlanDetails, 'end'>>(
        {},
        plan,
        {
          end: newState.end,
        },
      )
    }
    case EnumPlanActions.Category: {
      return Object.assign<
        {},
        PlanDetails,
        Pick<PlanDetails, 'change_category'>
      >({}, plan, {
        change_category: newState.change_category ?? defaultCategory,
      })
    }
    case EnumPlanActions.BackfillProgress: {
      return Object.assign<
        {},
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
        {},
        PlanDetails,
        Pick<PlanDetails, 'backfills' | 'hasBackfills'>
      >({}, plan, {
        backfills,
        hasBackfills: isArrayNotEmpty(backfills),
      })
    }
    case EnumPlanActions.Changes: {
      const { modified, added, removed } = newState ?? {}
      const hasChanges = [
        isModified(modified),
        isArrayNotEmpty(added),
        isArrayNotEmpty(removed),
      ].some(Boolean)

      return Object.assign<{}, PlanDetails, PlanChanges>({}, plan, {
        modified,
        added,
        removed,
        hasChanges,
        hasDirect: isArrayNotEmpty(modified?.direct),
        hasIndirect: isArrayNotEmpty(modified?.indirect),
        hasMetadata: isArrayNotEmpty(modified?.metadata),
        hasAdded: isArrayNotEmpty(added),
        hasRemoved: isArrayNotEmpty(removed),
      })
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
