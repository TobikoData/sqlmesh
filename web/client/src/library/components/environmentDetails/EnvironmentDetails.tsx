import { PlayCircleIcon } from '@heroicons/react/20/solid'
import clsx from 'clsx'
import { PlayCircleIcon as OutlinePlayCircleIcon } from '@heroicons/react/24/outline'
import { EnumRoutes } from '~/routes'
import { useStoreContext } from '@context/context'
import { useStorePlan } from '@context/plan'
import {
  useApiCancelPlan,
  useApiEnvironments,
  useApiPlanApply,
  useApiPlanRun,
} from '@api/index'
import { EnumSize, EnumVariant } from '~/types/enum'
import Spinner from '@components/logo/Spinner'
import ModuleLink from '@components/moduleNavigation/ModuleLink'
import EnvironmentChanges from './EnvironmentChanges'
import { SelectEnvironment } from './SelectEnvironment'
import LoadingStatus from '@components/loading/LoadingStatus'
import { useEffect } from 'react'
import { includes, isFalse } from '@utils/index'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'

let planActionUpdateTimeoutId: Optional<number>

export default function EnvironmentDetails(): JSX.Element {
  const modules = useStoreContext(s => s.modules)
  const environment = useStoreContext(s => s.environment)
  const models = useStoreContext(s => s.models)

  const planAction = useStorePlan(s => s.planAction)
  const planApply = useStorePlan(s => s.planApply)
  const planOverview = useStorePlan(s => s.planOverview)
  const planCancel = useStorePlan(s => s.planCancel)
  const setPlanApply = useStorePlan(s => s.setPlanApply)
  const setPlanOverview = useStorePlan(s => s.setPlanOverview)
  const setPlanCancel = useStorePlan(s => s.setPlanCancel)
  const setPlanAction = useStorePlan(s => s.setPlanAction)
  const resetPlanTrackers = useStorePlan(s => s.resetPlanTrackers)
  const clearPlanApply = useStorePlan(s => s.clearPlanApply)

  const { isFetching: isFetchingPlanApply } = useApiPlanApply(environment.name)
  const { isFetching: isFetchingPlanCancel } = useApiCancelPlan()
  const { isFetching: isFetchingEnvironments } = useApiEnvironments()
  const { refetch: planRun, isFetching: isFetchingPlanRun } = useApiPlanRun(
    environment.name,
    {
      planOptions: { skip_tests: true, include_unmodified: true },
    },
  )

  useEffect(() => {
    if (
      models.size > 0 &&
      isFalse(planAction.isProcessing) &&
      isFalse(planAction.isRunningTask)
    ) {
      resetPlanTrackers()
      clearPlanApply()

      void planRun()
    }
  }, [models, environment])

  useEffect(() => {
    if (isFetchingPlanRun) {
      planOverview.isFetching = true

      setPlanOverview(planOverview)
    }

    if (isFetchingPlanApply) {
      planApply.isFetching = true

      setPlanApply(planApply)
    }

    if (isFetchingPlanCancel) {
      planCancel.isFetching = true

      setPlanCancel(planCancel)
    }
  }, [isFetchingPlanRun, isFetchingPlanApply, isFetchingPlanCancel])

  useEffect(() => {
    clearTimeout(planActionUpdateTimeoutId)

    const value = ModelPlanAction.getPlanAction({
      planOverview,
      planApply,
      planCancel,
    })

    // This inconsistency can happen when we receive flag from the backend
    // that some task is runninng but not yet recived SSE event with data
    if (
      (value === EnumPlanAction.Done && planAction.isRunningTask) ||
      value === planAction.value
    )
      return

    // Delaying the update of the plan action to avoid flickering
    planActionUpdateTimeoutId = setTimeout(
      () => {
        setPlanAction(new ModelPlanAction({ value }))

        planActionUpdateTimeoutId = undefined
      },
      includes(
        [
          EnumPlanAction.Running,
          EnumPlanAction.RunningTask,
          EnumPlanAction.Cancelling,
          EnumPlanAction.Applying,
        ],
        value,
      )
        ? 0
        : 500,
    ) as unknown as number
  }, [planOverview, planApply, planCancel])

  return (
    <div className="h-8 flex w-full items-center justify-end text-neutral-500">
      {isFetchingEnvironments ? (
        <LoadingStatus>Loading Environments...</LoadingStatus>
      ) : (
        <>
          {isFalse(modules.hasOnlyPlans) &&
            isFalse(modules.hasOnlyPlansAndErrors) && (
              <ModuleLink
                title={
                  planAction.isProcessing
                    ? planAction.displayStatus(planOverview)
                    : 'Plan'
                }
                to={EnumRoutes.Plan}
                icon={
                  planAction.isProcessing ? (
                    <Spinner
                      variant={EnumVariant.Success}
                      className="w-3 py-1"
                    />
                  ) : (
                    <OutlinePlayCircleIcon
                      className={clsx(
                        'w-5',
                        planOverview.isFailed
                          ? 'text-danger-500 dark:text-danger-100'
                          : 'text-success-100',
                      )}
                    />
                  )
                }
                iconActive={
                  planAction.isProcessing ? (
                    <Spinner
                      variant={EnumVariant.Success}
                      className="w-3 py-1"
                    />
                  ) : (
                    <PlayCircleIcon
                      className={clsx(
                        'w-5',
                        planOverview.isFailed
                          ? 'text-danger-500 dark:text-danger-100'
                          : 'text-success-100',
                      )}
                    />
                  )
                }
                before={
                  <p
                    className={clsx(
                      'block mx-1 text-xs font-bold whitespace-nowrap',
                      planOverview.isFailed
                        ? 'text-danger-500 dark:text-danger-100'
                        : 'text-success-100',
                    )}
                  >
                    {planAction.isProcessing
                      ? planAction.displayStatus(planOverview)
                      : 'Plan'}
                  </p>
                }
                className={clsx(
                  'px-2 rounded-full py-0.5 mr-1',
                  planOverview.isFailed
                    ? 'bg-danger-10 dark:bg-danger-500'
                    : 'bg-success-500',
                )}
                classActive={clsx(
                  planOverview.isFailed
                    ? 'bg-danger-10 dark:bg-danger-500'
                    : 'bg-success-500',
                )}
              />
            )}
          <div className="flex justify-center items-center min-w-[6rem] max-w-full">
            <SelectEnvironment
              className="border-none mx-2 w-full"
              size={EnumSize.sm}
              showAddEnvironment={true}
              disabled={
                isFetchingPlanRun ||
                planAction.isProcessing ||
                environment.isInitialProd
              }
            />
          </div>
          <EnvironmentChanges />
        </>
      )}
    </div>
  )
}
