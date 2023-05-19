import { Outlet, useLocation, useParams } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { isArrayNotEmpty } from '@utils/index'
import { useStoreContext } from '@context/context'
import { EnumPlanApplyType, useStorePlan } from '@context/plan'
import { ModelSQLMeshModel } from '@models/sqlmesh-model'
import Container from '@components/container/Container'
import SplitPane from '@components/splitPane/SplitPane'
import Search from './Search'
import SourceList from './SourceList'
import TasksOverview from '@components/tasksOverview/TasksOverview'

export default function Docs(): JSX.Element {
  const location = useLocation()
  const { modelName } = useParams()

  const models = useStoreContext(s => s.models)
  const environment = useStoreContext(s => s.environment)

  const activePlan = useStorePlan(s => s.activePlan)
  const planState = useStorePlan(s => s.state)

  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState('')

  const filtered = Array.from(models.entries()).reduce(
    (acc: ModelSQLMeshModel[], [key, model]) => {
      if (model.name === key) return acc
      if (
        modelName == null ||
        model.name !== ModelSQLMeshModel.decodeName(modelName)
      ) {
        acc.push(model)
      }

      return acc
    },
    [],
  )

  useEffect(() => {
    setFilter('')
    setSearch('')
  }, [location.pathname])

  return (
    <Container.Page>
      <div className="p-4 flex flex-col w-full h-full overflow-hidden">
        {isArrayNotEmpty(filtered) && (
          <Search
            models={filtered}
            search={search}
            setSearch={setSearch}
          />
        )}
        {activePlan != null && (
          <div className="w-full p-4">
            <TasksOverview tasks={activePlan.tasks}>
              {({ total, completed, totalBatches, completedBatches }) => (
                <>
                  <TasksOverview.Summary
                    environment={environment.name}
                    planState={planState}
                    headline="Most Recent Plan"
                    completed={completed}
                    total={total}
                    totalBatches={totalBatches}
                    completedBatches={completedBatches}
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
            {models.size > 0 && (
              <SourceList
                models={models}
                filter={filter}
                setFilter={setFilter}
              />
            )}
          </div>
          <div className="w-full">
            <Outlet />
          </div>
        </SplitPane>
      </div>
    </Container.Page>
  )
}
