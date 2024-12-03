import { type RouteObject, createBrowserRouter } from 'react-router-dom'
import { Suspense, lazy } from 'react'
import NotFound from './library/pages/root/NotFound'
import Loading from '@components/loading/Loading'
import Spinner from '@components/logo/Spinner'
import { isArrayNotEmpty, isNil } from './utils'
import { Modules } from '@api/client'
import { getUrlWithPrefix } from '@api/instance'
import { type ModelModuleController } from '@models/module-controller'

const Root = lazy(() => import('./library/pages/root/Root'))
const Editor = lazy(() => import('./library/pages/editor/Editor'))
const Plan = lazy(() => import('./library/pages/plan/Plan'))
const PlanContent = lazy(() => import('./library/pages/plan/Content'))
const DataCatalog = lazy(
  () => import('./library/pages/data-catalog/DataCatalog'),
)
const Tests = lazy(() => import('./library/pages/tests/Tests'))
const Audits = lazy(() => import('./library/pages/audits/Audits'))
const Data = lazy(() => import('./library/pages/data/Data'))
const Models = lazy(() => import('./library/pages/models/Models'))
const Lineage = lazy(() => import('./library/pages/lineage/Lineage'))
const Errors = lazy(() => import('./library/pages/errors/Errors'))
const ErrorContent = lazy(() => import('./library/pages/errors/Content'))
const Welcome = lazy(() => import('./library/components/banner/Welcome'))

export const EnumRoutes = {
  Home: '/',
  Editor: '/editor',
  DataCatalog: '/data-catalog',
  DataCatalogModels: '/data-catalog/models',
  Data: '/data',
  DataModels: '/data/models',
  Lineage: '/lineage',
  LineageModels: '/lineage/models',
  Tests: '/tests',
  Audits: '/audits',
  Errors: '/errors',
  Plan: '/plan',
  Models: '/models',
  NotFound: '/not-found',
} as const

export type Routes = (typeof EnumRoutes)[keyof typeof EnumRoutes]

const aliases: Record<string, string[]> = {
  plan: [Modules.plans],
}

export function getBrowserRouter(
  modules: ModelModuleController,
): ReturnType<typeof createBrowserRouter> {
  const routes = [
    {
      path: '*',
      element: isArrayNotEmpty(modules.list) ? (
        <NotFound
          link={EnumRoutes.Home}
          message="Back To Main"
        />
      ) : (
        <NotFound headline="Empty" />
      ),
    },
    {
      path: EnumRoutes.Home,
      element: <Root />,
    },
    ...[
      {
        path: EnumRoutes.Editor,
        element: <Root content={<Editor />} />,
      },
      {
        path: EnumRoutes.DataCatalog,
        element: <Root content={<Models route={EnumRoutes.DataCatalog} />} />,
        children: [
          {
            index: true,
            element: (
              <Welcome
                headline="Welcome to the Data Catalog"
                tagline="Here you can find all the information about the models and their fields."
              />
            ),
          },
          {
            path: '*',
            element: (
              <NotFound
                link={EnumRoutes.DataCatalog}
                message="Back To Data Catalog"
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
                    headline="Welcome to the Data Catalog"
                    tagline="Here you can find all the information about the models and their fields."
                  />
                ),
              },
              {
                path: '*',
                element: (
                  <NotFound
                    link={EnumRoutes.DataCatalog}
                    message="Back To Data Catalog"
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
                    <DataCatalog />
                  </Suspense>
                ),
              },
            ],
          },
        ],
      },
      {
        path: EnumRoutes.Data,
        element: <Root content={<Models route={EnumRoutes.Data} />} />,
        children: [
          {
            index: true,
            element: (
              <Welcome
                headline="Welcome to the Data"
                tagline="Here you can find all the information about the models and their fields."
              />
            ),
          },
          {
            path: '*',
            element: (
              <NotFound
                link={EnumRoutes.Data}
                message="Back To Data"
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
                    headline="Welcome to the Data"
                    tagline="Here you can find all the information about the models and their fields."
                  />
                ),
              },
              {
                path: '*',
                element: (
                  <NotFound
                    link={EnumRoutes.Data}
                    message="Back To Data"
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
                    <Data />
                  </Suspense>
                ),
              },
            ],
          },
        ],
      },
      {
        path: EnumRoutes.Lineage,
        element: <Root content={<Models route={EnumRoutes.Lineage} />} />,
        children: [
          {
            index: true,
            element: (
              <Welcome
                headline="Welcome to the Lineage"
                tagline="Here you can find all the information about the models and their fields."
              />
            ),
          },
          {
            path: '*',
            element: (
              <NotFound
                link={EnumRoutes.Lineage}
                message="Back To Lineage"
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
                    headline="Welcome to the Lineage"
                    tagline="Here you can find all the information about the models and their fields."
                  />
                ),
              },
              {
                path: '*',
                element: (
                  <NotFound
                    link={EnumRoutes.Lineage}
                    message="Back To Lineage"
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
                    <Lineage />
                  </Suspense>
                ),
              },
            ],
          },
        ],
      },
      {
        path: EnumRoutes.Errors,
        element: <Root content={<Errors />} />,
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
        path: EnumRoutes.Plan,
        element: <Root content={<Plan />} />,
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
        path: EnumRoutes.Tests,
        element: <Root content={<Tests />} />,
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
        path: EnumRoutes.Audits,
        element: <Root content={<Audits />} />,
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
    ].filter(r => {
      const path = r.path.replace('/', '') as Modules
      return isNil(aliases[path])
        ? modules.list.includes(path as Modules)
        : aliases[path]?.some(a => modules.list.includes(a as Modules))
    }),
  ].filter(Boolean) as RouteObject[]

  return createBrowserRouter(routes, { basename: getUrlWithPrefix() })
}
