import { createBrowserRouter } from 'react-router-dom'
import Editor from './library/pages/editor/Editor'
import IDEProvider from './library/pages/ide/context'
import { Suspense, lazy } from 'react'
import NotFound from './library/pages/root/NotFound'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import DocsWelcome from './library/pages/docs/Welcome'
import DocsContent from './library/pages/docs/Content'

const IDE = lazy(async () => await import('./library/pages/ide/IDE'))
const Docs = lazy(async () => await import('./library/pages/docs/Docs'))

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
        element: (
          <Suspense
            fallback={
              <Loading className="inline-block ">
                <Spinner className="w-5 h-5 border border-neutral-10 mr-4" />
                <h3 className="text-xl">Looking...</h3>
              </Loading>
            }
          >
            <Docs />
          </Suspense>
        ),
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
                      <Loading className="inline-block ">
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
