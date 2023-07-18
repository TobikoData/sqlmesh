import { useEffect, useCallback, Suspense } from 'react'
import { RouterProvider } from 'react-router-dom'
import { Divider } from '@components/divider/Divider'
import Header from './library/pages/root/Header'
import Footer from './library/pages/root/Footer'
import { router } from './routes'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { debounceAsync } from './utils'
import { useApiMeta } from './api'
import { useStoreContext } from '@context/context'

export default function App(): JSX.Element {
  const setVersion = useStoreContext(s => s.setVersion)

  const { refetch: getMeta } = useApiMeta()

  const debouncedGetMeta = useCallback(debounceAsync(getMeta, 1000, true), [
    getMeta,
  ])

  useEffect(() => {
    void debouncedGetMeta().then(({ data }) => {
      setVersion(data?.version)
    })

    return () => {
      debouncedGetMeta.cancel()
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
