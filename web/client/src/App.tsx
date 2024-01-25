import { useEffect, Suspense } from 'react'
import { RouterProvider } from 'react-router-dom'
import { Divider } from '@components/divider/Divider'
import Header from './library/pages/root/Header'
import Footer from './library/pages/root/Footer'
import { getBrowserRouter } from './routes'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { useApiMeta } from './api'
import { useStoreContext } from '@context/context'
import { useStorePlan } from '@context/plan'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { isNil, isTrue } from './utils'

export default function App(): JSX.Element {
  const version = useStoreContext(s => s.version)
  const modules = useStoreContext(s => s.modules)
  const setVersion = useStoreContext(s => s.setVersion)
  const setPlanAction = useStorePlan(s => s.setPlanAction)
  const setModules = useStoreContext(s => s.setModules)

  const {
    refetch: getMeta,
    cancel: cancelRequestMeta,
    isFetching: isFetchingMeta,
  } = useApiMeta()

  useEffect(() => {
    void getMeta().then(({ data }) => {
      setVersion(data?.version)
      setModules(Array.from(new Set(modules.concat(data?.modules ?? []))))

      if (isTrue(data?.has_running_task)) {
        setPlanAction(
          new ModelPlanAction({ value: EnumPlanAction.RunningTask }),
        )
      }
    })

    return () => {
      void cancelRequestMeta()
    }
  }, [])

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
