import { createBrowserRouter } from 'react-router-dom'
import Docs from './library/pages/docs/Docs'
import Editor from './library/pages/editor/Editor'
import IDEProvider from './library/pages/ide/context'
import { Suspense, lazy } from 'react'
import NotFound from './library/pages/root/NotFound'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'

const IDE = lazy(async () => await import('./library/pages/ide/IDE'))

export const EnumRoutes = {
  Ide: '/',
  IdeEditor: '/editor',
  IdeDocs: '/docs',
  IdeDocsModels: '/docs/models',
} as const

export const router = createBrowserRouter([
  {
    path: '/',
    element: (
      <Suspense
        fallback={
          <Loading className="inline-block ">
            <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
            <h3 className="text-xl">Starting IDE...</h3>
          </Loading>
        }
      >
        <IDEProvider>
          <IDE />
        </IDEProvider>
      </Suspense>
    ),
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
            element: <Docs.Welcome />,
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
                element: <Docs.Welcome />,
              },
              {
                path: ':modelName',
                element: <Docs.Content />,
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
