import { createBrowserRouter } from 'react-router-dom'
import { Suspense, lazy } from 'react'
import NotFound from './library/pages/root/NotFound'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import IDE from './library/pages/ide/IDE'

const Editor = lazy(() => import('./library/pages/editor/Editor'))
const Errors = lazy(() => import('./library/pages/errors/Errors'))
const Plan = lazy(() => import('./library/pages/plan/Plan'))
const Docs = lazy(() => import('./library/pages/docs/Docs'))
const Tests = lazy(() => import('./library/pages/tests/Tests'))
const Audits = lazy(() => import('./library/pages/audits/Audits'))
const DocsContent = lazy(() => import('./library/pages/docs/Content'))
const DocsWelcome = lazy(() => import('./library/pages/docs/Welcome'))

export const EnumRoutes = {
  Ide: '/',
  IdeEditor: '/editor',
  IdeDocs: '/docs',
  Editor: '/editor',
  Docs: '/docs',
  Tests: '/tests',
  Audits: '/audits',
  Errors: '/errors',
  Plan: '/plan',
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
            index: true,
            element: <DocsWelcome />,
          },
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
            path: 'models',
            children: [
              {
                index: true,
                element: <DocsWelcome />,
              },
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
                path: ':modelName',
                element: (
                  <Suspense
                    fallback={
                      <div className="flex justify-center items-center w-full h-full">
                        <Loading className="inline-block">
                          <Spinner className="w-3 h-3 border border-neutral-10 mr-4" />
                          <h3 className="text-md">Loading Content...</h3>
                        </Loading>
                      </div>
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
      {
        path: 'errors',
        element: <Errors />,
      },
      {
        path: 'plan',
        element: <Plan />,
      },
      {
        path: 'tests',
        element: <Tests />,
      },
      {
        path: 'audits',
        element: <Audits />,
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
