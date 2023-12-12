import { useMemo } from 'react'
import {
  type BodyInitiateApplyApiCommandsApplyPostCategoriesAnyOf,
  type BodyInitiateApplyApiCommandsApplyPostCategories,
  type PlanDates,
  type PlanOptions,
} from '~/api/client'
import { type ModelEnvironment } from '~/models/environment'
import { isStringEmptyOrNil } from '~/utils'
import { usePlan } from './context'

export function usePlanPayload({
  environment,
  isInitialPlanRun,
}: {
  environment: ModelEnvironment
  isInitialPlanRun: boolean
}): { planDates?: PlanDates; planOptions: PlanOptions } {
  const {
    start,
    end,
    skip_tests,
    no_gaps,
    skip_backfill,
    forward_only,
    no_auto_categorization,
    restate_models,
    create_from,
    include_unmodified,
  } = usePlan()

  const planDates = useMemo(() => {
    if (environment.isDefault) return

    return {
      start,
      end:
        isInitialPlanRun && isStringEmptyOrNil(restate_models)
          ? undefined
          : end,
    }
  }, [environment, start, end, isInitialPlanRun, restate_models])

  const planOptions = useMemo(() => {
    if (environment.isDefaultInitial)
      return { skip_tests, include_unmodified: true }

    return {
      no_gaps,
      skip_backfill,
      forward_only,
      create_from,
      no_auto_categorization,
      skip_tests,
      restate_models,
      include_unmodified,
    }
  }, [
    environment,
    no_gaps,
    skip_backfill,
    forward_only,
    include_unmodified,
    create_from,
    no_auto_categorization,
    skip_tests,
    restate_models,
  ])

  return {
    planOptions,
    planDates,
  }
}

export function useApplyPayload({
  isInitialPlanRun,
}: {
  isInitialPlanRun: boolean
}): {
  planDates?: PlanDates
  planOptions: PlanOptions
  categories: BodyInitiateApplyApiCommandsApplyPostCategories
} {
  const {
    start,
    end,
    skip_tests,
    no_gaps,
    skip_backfill,
    forward_only,
    include_unmodified,
    no_auto_categorization,
    restate_models,
    create_from,
    change_categorization,
  } = usePlan()

  const planDates = useMemo(() => {
    if (isInitialPlanRun) return

    return {
      start,
      end,
    }
  }, [start, end, isInitialPlanRun])

  const categories = useMemo(() => {
    return Array.from(
      change_categorization.values(),
    ).reduce<BodyInitiateApplyApiCommandsApplyPostCategoriesAnyOf>(
      (acc, { category, change }) => {
        acc[change.model_name] = category.value

        return acc
      },
      {},
    )
  }, [change_categorization])

  return {
    planDates,
    planOptions: {
      no_gaps,
      skip_backfill,
      forward_only,
      include_unmodified,
      create_from,
      no_auto_categorization,
      skip_tests,
      restate_models,
    },
    categories,
  }
}
