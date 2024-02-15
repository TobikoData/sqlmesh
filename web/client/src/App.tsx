import { useEffect, Suspense } from 'react'
import { RouterProvider } from 'react-router-dom'
import { Divider } from '@components/divider/Divider'
import Header from './library/pages/root/Header'
import Footer from './library/pages/root/Footer'
import { getBrowserRouter } from './routes'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { useApiEnvironments, useApiMeta, useApiPlanRun } from './api'
import { useStoreContext } from '@context/context'
import { useStorePlan } from '@context/plan'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { isNil, isObjectNotEmpty, isTrue } from './utils'
import { useChannelEvents } from '@api/channels'
import { type PlanOverviewTracker } from '@models/tracker-plan-overview'
import {
  type Environments,
  type EnvironmentsEnvironments,
  Status,
} from '@api/client'
import { type PlanApplyTracker } from '@models/tracker-plan-apply'
import { type PlanCancelTracker } from '@models/tracker-plan-cancel'
import { type EnvironmentName } from '@models/environment'

export default function App(): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const version = useStoreContext(s => s.version)
  const modules = useStoreContext(s => s.modules)
  const setVersion = useStoreContext(s => s.setVersion)
  const setModules = useStoreContext(s => s.setModules)
  const addLocalEnvironment = useStoreContext(s => s.addLocalEnvironment)
  const addRemoteEnvironments = useStoreContext(s => s.addRemoteEnvironments)
  const setEnvironment = useStoreContext(s => s.setEnvironment)

  const planOverview = useStorePlan(s => s.planOverview)
  const planApply = useStorePlan(s => s.planApply)
  const planCancel = useStorePlan(s => s.planCancel)
  const setPlanOverview = useStorePlan(s => s.setPlanOverview)
  const setPlanApply = useStorePlan(s => s.setPlanApply)
  const setPlanCancel = useStorePlan(s => s.setPlanCancel)
  const setPlanAction = useStorePlan(s => s.setPlanAction)

  const { refetch: getEnvironments, cancel: cancelRequestEnvironments } =
    useApiEnvironments()
  const { refetch: planRun } = useApiPlanRun(environment.name, {
    planOptions: {
      skip_tests: true,
      include_unmodified: true,
    },
  })

  const channel = useChannelEvents()

  const {
    refetch: getMeta,
    cancel: cancelRequestMeta,
    isFetching: isFetchingMeta,
  } = useApiMeta()

  useEffect(() => {
    const channelPlanOverview = channel(
      'plan-overview',
      updatePlanOverviewTracker,
    )
    const channelPlanApply = channel('plan-apply', updatePlanApplyTracker)
    const channelPlanCancel = channel('plan-cancel', updatePlanCancelTracker)

    channelPlanOverview.subscribe()
    channelPlanApply.subscribe()
    channelPlanCancel.subscribe()

    return () => {
      channelPlanApply.unsubscribe()
      channelPlanCancel.unsubscribe()
      channelPlanOverview.unsubscribe()
    }
  }, [environment, environments])

  useEffect(() => {
    void getMeta().then(({ data }) => {
      setVersion(data?.version)
      setModules(Array.from(new Set(modules.concat(data?.modules ?? []))))

      if (isTrue(data?.has_running_task)) {
        setPlanAction(
          new ModelPlanAction({ value: EnumPlanAction.RunningTask }),
        )
      } else {
        void getEnvironments().then(({ data }) => updateEnviroments(data))
      }
    })

    return () => {
      void cancelRequestMeta()
      void cancelRequestEnvironments()
    }
  }, [])

  function updatePlanOverviewTracker(data: PlanOverviewTracker): void {
    planOverview.update(data)

    if (data.environment !== environment.name) {
      synchronizeEnvironment(data.environment)
    }

    setPlanOverview(planOverview)
  }

  function updatePlanApplyTracker(data: PlanApplyTracker): void {
    planApply.update(data, planOverview)

    if (data.environment !== environment.name) {
      synchronizeEnvironment(data.environment)
    }

    const isFinished =
      isTrue(data.meta?.done) && data.meta?.status !== Status.init

    if (isFinished) {
      void getEnvironments().then(({ data }) => {
        planCancel.reset()

        updateEnviroments(data)

        void planRun()

        setPlanCancel(planCancel)
      })
    }

    setPlanApply(planApply)
  }

  function updatePlanCancelTracker(data: PlanCancelTracker): void {
    planCancel.update(data)

    if (data.environment !== environment.name) {
      synchronizeEnvironment(data.environment)
    }

    setPlanCancel(planCancel)
  }

  function updateEnviroments(data: Optional<Environments>): void {
    const { environments, default_target_environment, pinned_environments } =
      data ?? {}

    if (isObjectNotEmpty<EnvironmentsEnvironments>(environments)) {
      addRemoteEnvironments(
        Object.values(environments),
        default_target_environment,
        pinned_environments,
      )
    }
  }

  function synchronizeEnvironment(environment: EnvironmentName): void {
    const found = Array.from(environments).find(e => e.name === environment)

    if (isNil(found)) {
      addLocalEnvironment(environment)
    } else {
      setEnvironment(found)
    }
  }

  const router = getBrowserRouter(modules)
  const isLoadingMeta = isNil(version) || isFetchingMeta

  return (
    <>
      <Header />
      <Divider />
      <main className="h-full overflow-hidden">
        {isLoadingMeta ? (
          <div className="flex justify-center items-center w-full h-full">
            <Loading className="inline-block">
              <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
              <h3 className="text-md">Building Modules...</h3>
            </Loading>
          </div>
        ) : (
          <Suspense
            fallback={
              <div className="flex justify-center items-center w-full h-full">
                <Loading className="inline-block">
                  <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
                  <h3 className="text-md">Loading Page...</h3>
                </Loading>
              </div>
            }
          >
            <RouterProvider router={router} />
          </Suspense>
        )}
      </main>
      <Divider />
      <Footer />
    </>
  )
}
