import { isNotNil } from '@utils/index'
import {
  createContext,
  type Dispatch,
  type ReactNode,
  useContext,
  useReducer,
} from 'react'
import {
  type ModelsDiff,
  type ChangeDirect,
  SnapshotChangeCategory,
  type PlanDatesStart,
  type PlanDatesEnd,
} from '~/api/client'

export const EnumPlanActions = {
  PlanOptions: 'plan-options',
  ResetPlanOptions: 'reset-plan-options',
  Dates: 'dates',
  DateStart: 'date-start',
  DateEnd: 'date-end',
  Category: 'category',
  External: 'external',
  ResetTestsReport: 'reset-tests-report',
  TestsReportErrors: 'tests-report-errors',
  TestsReportMessages: 'tests-report-messages',
} as const

export const EnumPlanChangeType = {
  Add: 'add',
  Remove: 'remove',
  Direct: 'direct',
  Indirect: 'indirect',
  Default: 'default',
} as const

export const EnumCategoryType = {
  BreakingChange: 'breaking-change',
  NonBreakingChange: 'non-breaking-change',
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
  include_unmodified: boolean
  create_from?: string
  restate_models?: string
}

export interface TestReportError {
  ok: boolean
  time: number
  title: string
  total: string
  successful: number
  failures: number
  errors: number
  dialect: string
  traceback: string
  details: Array<{ message: string; details: string }>
}

export interface TestReportMessage {
  ok: boolean
  time: number
  message: string
}

interface PlanDetails extends PlanOptions {
  start?: PlanDatesStart
  end?: PlanDatesEnd
  virtualUpdateDescription: string
  isInitialPlanRun: boolean
  categories: Category[]
  change_categorization: Map<string, ChangeCategory>
  testsReportErrors?: TestReportError
  testsReportMessages?: TestReportMessage
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
  include_unmodified: true,
  from: undefined,
  restate_models: undefined,
  create_from: undefined,

  categories,
  defaultCategory,
  change_categorization: new Map(),

  virtualUpdateDescription:
    'All changes and their downstream dependencies can be fully previewed before they get promoted. If during plan creation no data gaps have been detected and only references to new model versions need to be updated, then such an update is referred to as a Virtual Update. Virtual Updates impose no additional runtime overhead or cost.',

  isInitialPlanRun: false,
  errors: [],
  testsReportErrors: undefined,
  testsReportMessages: undefined,
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
          include_unmodified: true,
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

    case EnumPlanActions.TestsReportErrors: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'testsReportErrors'>
      >({}, plan, {
        testsReportErrors: newState.testsReportErrors,
      })
    }

    case EnumPlanActions.TestsReportMessages: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'testsReportMessages'>
      >({}, plan, {
        testsReportMessages: newState.testsReportMessages,
      })
    }

    case EnumPlanActions.ResetTestsReport: {
      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        Pick<PlanDetails, 'testsReportErrors' | 'testsReportMessages'>
      >({}, plan, {
        testsReportErrors: undefined,
        testsReportMessages: undefined,
      })
    }

    case EnumPlanActions.Category: {
      const { change, category } = newState as ChangeCategory

      if (isNotNil(change?.model_name)) {
        plan.change_categorization.set(change.model_name, {
          category,
          change,
        })
      }

      return Object.assign<
        Record<string, unknown>,
        PlanDetails,
        { change_categorization: Map<string, ChangeCategory> }
      >({}, plan, {
        change_categorization: new Map(plan.change_categorization),
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
    description: 'It will rebuild all models',
    value: SnapshotChangeCategory.NUMBER_1,
  }
  const categories = [
    categoryBreakingChange,
    {
      id: EnumCategoryType.NonBreakingChange,
      name: 'Non-Breaking Change',
      description: 'It will exclude all indirect models caused by this change',
      value: SnapshotChangeCategory.NUMBER_2,
    },
  ]

  return [categoryBreakingChange, categories]
}
