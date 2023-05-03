import Container from '@components/container/Container'
import { useStoreContext } from '@context/context'
import { Outlet, useLocation } from 'react-router-dom'
import { EnumRoutes } from '~/routes'
import Content from './Content'
import { useEffect, useState } from 'react'
import SplitPane from '@components/splitPane/SplitPane'
import Search from './Search'
import SourceList from './SourceList'
import TasksOverview from '@components/tasksOverview/TasksOverview'
import { EnumPlanApplyType, useStorePlan } from '@context/plan'

const Docs = function Docs(): JSX.Element {
  const location = useLocation()

  const models = useStoreContext(s => s.models)
  const environment = useStoreContext(s => s.environment)

  const activePlan = useStorePlan(s => s.activePlan)
  const planState = useStorePlan(s => s.state)

  const searchParams = new URLSearchParams(location.search)
  const modelName = searchParams.get('model') ?? undefined

  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState('')

  useEffect(() => {
    setFilter('')
    setSearch('')
  }, [location.search])

  const isActivePageDocs = location.pathname === EnumRoutes.IdeDocs

  return (
    <Container.Page>
      <div className="p-4 flex flex-col w-full h-full overflow-hidden">
        <Search
          models={models}
          search={search}
          setSearch={setSearch}
        />
        {activePlan != null && (
          <div className="w-full p-4">
            <TasksOverview tasks={activePlan.tasks}>
              {({ total, completed }) => (
                <>
                  <TasksOverview.Summary
                    environment={environment.name}
                    planState={planState}
                    headline="Most Recent Plan"
                    completed={completed}
                    total={total}
                    updateType={
                      activePlan.type === EnumPlanApplyType.Virtual
                        ? 'Virtual'
                        : 'Backfill'
                    }
                    updatedAt={activePlan.updated_at}
                  />
                </>
              )}
            </TasksOverview>
          </div>
        )}
        <SplitPane
          className="flex w-full h-full overflow-hidden mt-8"
          sizes={[25, 75]}
          minSize={0}
          snapOffset={0}
        >
          <div className="py-4 w-full">
            <SourceList
              models={models}
              filter={filter}
              setFilter={setFilter}
              activeModel={modelName}
            />
          </div>
          <div className="w-full">
            {isActivePageDocs ? <Welcome /> : <Outlet />}
          </div>
        </SplitPane>
      </div>
    </Container.Page>
  )
}

Docs.Content = Content

export default Docs

function Welcome(): JSX.Element {
  return (
    <div className="p-4">
      <h1 className="text-2xl font-bold">Welcome to the documentation</h1>
      <p className="text-lg mt-4">
        Here you can find all the information about the models and their fields.
      </p>
    </div>
  )
}
