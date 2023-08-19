import { useEffect, Suspense } from 'react'
import { RouterProvider } from 'react-router-dom'
import { Divider } from '@components/divider/Divider'
import Header from './library/pages/root/Header'
import Footer from './library/pages/root/Footer'
import { router } from './routes'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { isFalse, isNil } from './utils'
import { useApiMeta } from './api'
import { useStoreContext } from '@context/context'
import { EnumAction, useStoreActionManager } from '@context/manager'

export default function App(): JSX.Element {
  const enqueue = useStoreActionManager(s => s.enqueue)
  const resetQueueCurrentByAction = useStoreActionManager(
    s => s.resetQueueCurrentByAction,
  )
  const isActiveAction = useStoreActionManager(s => s.isActiveAction)

  const setVersion = useStoreContext(s => s.setVersion)

  const { refetch: getMeta, cancel: cancelRequestMeta } = useApiMeta()

  useEffect(() => {
    enqueue({
      action: EnumAction.Meta,
      callback: getMeta,
      onCallbackSuccess({ data }) {
        if (isNil(data)) return

        setVersion(data.version)

        if (isFalse(data.has_running_task)) {
          if (isActiveAction(EnumAction.Plan)) {
            resetQueueCurrentByAction(EnumAction.Plan)
          } else if (isActiveAction(EnumAction.PlanApply)) {
            resetQueueCurrentByAction(EnumAction.PlanApply)
          } else if (isActiveAction(EnumAction.Backfill)) {
            resetQueueCurrentByAction(EnumAction.Backfill)
          }
        }
      },
    })

    return () => {
      void cancelRequestMeta()
    }
  }, [])

  return (
    <>
      <Header />
      <Divider />
      <main className="h-full overflow-hidden">
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
      </main>
      <Divider />
      <Footer />
    </>
  )
}
