import { useEffect, Suspense } from 'react'
import { RouterProvider } from 'react-router-dom'
import { Divider } from '@components/divider/Divider'
import Header from './library/pages/root/Header'
import Footer from './library/pages/root/Footer'
import { getBrowserRouter } from './routes'
import { useApiModules } from './api'
import { useStoreContext } from '@context/context'
import LoadingSegment from '@components/loading/LoadingSegment'
import {
  EnumErrorKey,
  useNotificationCenter,
} from './library/pages/root/context/notificationCenter'

export default function App(): JSX.Element {
  const { addError } = useNotificationCenter()

  const modules = useStoreContext(s => s.modules)
  const setModules = useStoreContext(s => s.setModules)

  const { refetch: getModules, cancel, isFetching } = useApiModules()

  useEffect(() => {
    getModules()
      .then(({ data }) => {
        modules.update(data)

        setModules(modules)
      })
      .catch(error => addError(EnumErrorKey.Modules, error))

    return () => {
      void cancel()
    }
  }, [])

  const router = getBrowserRouter(modules.list)

  return (
    <>
      <Header />
      <Divider />
      <main className="h-full overflow-hidden relative">
        {isFetching && (
          <LoadingSegment className="absolute w-full h-full bg-theme z-10">
            Building Modules...
          </LoadingSegment>
        )}
        <Suspense fallback={<LoadingSegment>Loading Page...</LoadingSegment>}>
          <RouterProvider router={router} />
        </Suspense>
      </main>
      <Divider />
      <Footer />
    </>
  )
}
