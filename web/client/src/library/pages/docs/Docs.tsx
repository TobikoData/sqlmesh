import ContainerPage from '@components/container/ContainerPage'
import { useStoreContext } from '@context/context'
import clsx from 'clsx'
import { NavLink, Outlet } from 'react-router-dom'
import { EnumRoutes } from '~/routes'

export default function Docs(): JSX.Element {
  const models = useStoreContext(s => s.models)

  return (
    <ContainerPage>
      <div className="p-4">
        <div className="p-2">
          <div className="py-2 px-4 bg-primary-10">Search</div>
        </div>
        <div className="p-2">
          <div className="py-2 px-4 bg-primary-10 rounded-xl">
            <ul className="m-4 flex justify-around">
              <li className="min-h-[10rem] w-full bg-primary-500 m-4 rounded-lg"></li>
              <li className="min-h-[10rem] w-full bg-primary-500 m-4 rounded-lg"></li>
              <li className="min-h-[10rem] w-full bg-primary-500 m-4 rounded-lg"></li>
            </ul>
          </div>
        </div>
        <div className="p-4 flex">
          <div className="p-4 min-w-[24vw] bg-theme-lighter">
            <ul className="m-4">
              {Array.from(new Set(models.values())).map(model => (
                <li
                  key={model.name}
                  className=""
                >
                  <NavLink to={`${EnumRoutes.IdeDocsModels}/${model.name}`}>
                    {({ isActive }) => (
                      <span
                        className={clsx(
                          isActive ? 'text-primary-500' : 'text-neutral-500',
                        )}
                      >
                        {model.name}
                      </span>
                    )}
                  </NavLink>
                </li>
              ))}
            </ul>
          </div>
          <div className="py-4 px-8 w-full">
            <Outlet />
          </div>
          <div className="p-4 min-w-[16vw]">
            <div className="p-4 border-2 border-primary-10 sticky top-0 rounded-md">
              <ul className="m-4">
                <li className="">Meta</li>
                <li className="">Description</li>
                <li className="">Columns</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </ContainerPage>
  )
}
