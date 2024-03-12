import { useMemo } from 'react'
import {
  type BodyInitiateApplyApiCommandsApplyPostCategoriesAnyOf,
  type BodyInitiateApplyApiCommandsApplyPostCategories,
  type PlanDates,
  type PlanOptions,
  SnapshotChangeCategory,
} from '~/api/client'
import { isNil, isStringEmptyOrNil, isTrue } from '~/utils'
import { usePlan } from './context'
import { useStoreContext } from '@context/context'

export function usePlanPayload(options?: PlanOptions): {
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
    auto_apply,
    create_from,
    change_categorization,
  } = usePlan()

  const environment = useStoreContext(s => s.environment)

  const isInitialPlanRun =
    isNil(environment?.isDefault) || isTrue(environment?.isDefault)

  const planDates = useMemo(() => {
    if (environment.isProd) return

    return {
      start,
      end:
        isInitialPlanRun && isStringEmptyOrNil(restate_models)
          ? undefined
          : end,
    }
  }, [environment, start, end, isInitialPlanRun, restate_models])

  const planOptions = useMemo(() => {
    return environment.isInitialProd
      ? {
          include_unmodified: true,
          no_gaps: true,
          skip_tests,
          auto_apply,
        }
      : {
          skip_tests,
          no_gaps,
          skip_backfill,
          forward_only,
          create_from,
          no_auto_categorization,
          restate_models,
          include_unmodified,
          auto_apply,
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
    auto_apply,
  ])

  const categories = useMemo(() => {
    return Array.from(
      change_categorization.values(),
    ).reduce<BodyInitiateApplyApiCommandsApplyPostCategoriesAnyOf>(
      (acc, { category, change }) => {
        acc[change.displayName] =
          category?.value ?? SnapshotChangeCategory.NUMBER_1

        return acc
      },
      {},
    )
  }, [change_categorization])

  return {
    planOptions: {
      ...planOptions,
      ...options,
    },
    planDates,
    categories,
  }
}

export function useApplyPayload(): {
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

  const environment = useStoreContext(s => s.environment)

  const isInitialPlanRun =
    isNil(environment?.isDefault) || isTrue(environment?.isDefault)

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
        acc[change.displayName] =
          category?.value ?? SnapshotChangeCategory.NUMBER_1

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
