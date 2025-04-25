import { useEffect, Suspense, lazy } from 'react'
import { RouterProvider } from 'react-router-dom'
import { Divider } from '@components/divider/Divider'
import { getBrowserRouter } from './routes'
import { useApiModules } from './api'
import { useStoreContext } from '@context/context'
import LoadingSegment from '@components/loading/LoadingSegment'
import {
  EnumErrorKey,
  useNotificationCenter,
} from './library/pages/root/context/notificationCenter'
import { isArrayNotEmpty, isNotNil } from './utils'
import NotFound from './library/pages/root/NotFound'

const IS_HEADLESS: boolean = Boolean((window as any).__IS_HEADLESS__ ?? false)
const Header: Optional<React.LazyExoticComponent<() => JSX.Element>> =
  IS_HEADLESS ? undefined : lazy(() => import('./library/pages/root/Header'))
const Footer: Optional<React.LazyExoticComponent<() => JSX.Element>> =
  IS_HEADLESS ? undefined : lazy(() => import('./library/pages/root/Footer'))

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

  const router = getBrowserRouter(modules)

  return (
    <>
      {isNotNil(Header) && (
        <>
          <Header />
          <Divider />
        </>
      )}
      <main className="h-full overflow-hidden relative">
        {isFetching ? (
          <LoadingSegment className="absolute w-full h-full bg-theme z-10">
            Building Modules...
          </LoadingSegment>
        ) : (
          <Suspense fallback={<LoadingSegment>Loading Page...</LoadingSegment>}>
            {isArrayNotEmpty(modules.list) ? (
              <RouterProvider router={router} />
            ) : (
              <NotFound description="No Modules Found" />
            )}
          </Suspense>
        )}
      </main>
      {isNotNil(Footer) && (
        <>
          <Divider />
          <Footer />
        </>
      )}
    </>
  )
}
