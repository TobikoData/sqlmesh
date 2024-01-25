import { createBrowserRouter } from 'react-router-dom'
import { Suspense, lazy } from 'react'
import NotFound from './library/pages/root/NotFound'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import IDE from './library/pages/ide/IDE'
import { isArrayEmpty, isNil } from './utils'
import { Modules } from '@api/client'

const Editor = lazy(() => import('./library/pages/editor/Editor'))
const Plan = lazy(() => import('./library/pages/plan/Plan'))
const Docs = lazy(() => import('./library/pages/docs/Docs'))
const Tests = lazy(() => import('./library/pages/tests/Tests'))
const Audits = lazy(() => import('./library/pages/audits/Audits'))
const DocsContent = lazy(() => import('./library/pages/docs/Content'))
const PlanContent = lazy(() => import('./library/pages/plan/Content'))
const Errors = lazy(() => import('./library/pages/errors/Errors'))
const ErrorContent = lazy(() => import('./library/pages/errors/Content'))
const Welcome = lazy(() => import('./library/components/banner/Welcome'))

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

const aliases: Record<string, string[]> = {
  plan: [Modules.plans, Modules['plan-progress']],
}

export function getBrowserRouter(
  filter: string[],
): ReturnType<typeof createBrowserRouter> {
  const modules = [
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
          element: (
            <Welcome
              headline="Welcome to the documentation"
              tagline="Here you can find all the information about the models and their fields."
            />
          ),
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
              element: (
                <Welcome
                  headline="Welcome to the documentation"
                  tagline="Here you can find all the information about the models and their fields."
                />
              ),
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
      children: [
        {
          index: true,
          element: (
            <Welcome
              headline="Welcome to errors report"
              tagline="Here you can see all the errors that occurred."
            />
          ),
        },
        {
          path: '*',
          element: (
            <NotFound
              link={EnumRoutes.Errors}
              message="Back To Errors"
            />
          ),
        },
        {
          path: ':id',
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
              <ErrorContent />
            </Suspense>
          ),
        },
      ],
    },
    {
      path: 'plan',
      element: <Plan />,
      children: [
        {
          index: true,
          element: (
            <Welcome
              headline="Welcome to plan"
              tagline="Here you can run the plan and apply the changes."
            />
          ),
        },
        {
          path: '*',
          element: (
            <NotFound
              link={EnumRoutes.Plan}
              message="Back To Plan"
            />
          ),
        },
        {
          path: 'environments',
          children: [
            {
              index: true,
              element: (
                <Welcome
                  headline="Welcome to plan"
                  tagline="Here you can run the plan and apply the changes."
                />
              ),
            },
            {
              path: '*',
              element: (
                <NotFound
                  link={EnumRoutes.Plan}
                  message="Back To Plan"
                />
              ),
            },
            {
              path: ':environmentName',
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
                  <PlanContent />
                </Suspense>
              ),
            },
          ],
        },
      ],
    },
    {
      path: 'tests',
      element: <Tests />,
      children: [
        {
          index: true,
          element: (
            <Welcome
              headline="Welcome to tests report"
              tagline="Here you can run tests and see the results."
            />
          ),
        },
        {
          path: '*',
          element: (
            <NotFound
              link={EnumRoutes.Tests}
              message="Back To Tests"
            />
          ),
        },
      ],
    },
    {
      path: 'audits',
      element: <Audits />,
      children: [
        {
          index: true,
          element: (
            <Welcome
              headline="Welcome to audits report"
              tagline="Here you can run audits and see the results."
            />
          ),
        },
        {
          path: '*',
          element: (
            <NotFound
              link={EnumRoutes.Audits}
              message="Back To Audits"
            />
          ),
        },
      ],
    },
  ].filter(r =>
    isNil(aliases[r.path])
      ? filter.includes(r.path)
      : aliases[r.path]?.some(a => filter.includes(a)),
  )

  return createBrowserRouter(
    isArrayEmpty(modules)
      ? [
          {
            path: '*',
            element: <NotFound headline="Empty" />,
          },
        ]
      : [
          {
            path: '/',
            element: <IDE />,
            children: modules,
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
        ],
  )
}
