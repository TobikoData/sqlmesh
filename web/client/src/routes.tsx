import { createBrowserRouter } from 'react-router-dom'
import { Suspense, lazy } from 'react'
import NotFound from './library/pages/root/NotFound'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import IDE from './library/pages/ide/IDE'

const Editor = lazy(async () => await import('./library/pages/editor/Editor'))
const Docs = lazy(async () => await import('./library/pages/docs/Docs'))
const DocsContent = lazy(
  async () => await import('./library/pages/docs/Content'),
)
const DocsWelcome = lazy(
  async () => await import('./library/pages/docs/Welcome'),
)

export const EnumRoutes = {
  Ide: '/',
  IdeEditor: '/editor',
  IdeDocs: '/docs',
  IdeDocsModels: '/docs/models',
} as const

export const router = createBrowserRouter([
  {
    path: '/',
    element: <IDE />,
    children: [
      {
        path: 'editor',
        element: <Editor />,
      },
      {
        path: 'docs',
        element: <Docs />,
        children: [
          {
            path: '*',
            element: (
              <NotFound
                link={EnumRoutes.IdeDocs}
                message="Back To Docs"
              />
            ),
          },
          {
            index: true,
            element: <DocsWelcome />,
          },
          {
            path: 'models',
            children: [
              {
                path: '*',
                element: (
                  <NotFound
                    link={EnumRoutes.IdeDocs}
                    message="Back To Docs"
                  />
                ),
              },
              {
                index: true,
                element: <DocsWelcome />,
              },
              {
                path: ':modelName',
                element: (
                  <Suspense
                    fallback={
                      <Loading className="inline-block">
                        <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
                        <h3 className="text-xl">Loading Content...</h3>
                      </Loading>
                    }
                  >
                    <DocsContent />
                  </Suspense>
                ),
              },
            ],
          },
        ],
      },
    ],
  },
  {
    path: '*',
    element: (
      <NotFound
        link={EnumRoutes.Ide}
        message="Back To Editor"
      />
    ),
  },
])
