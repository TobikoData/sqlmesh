import { useStoreContext } from '@context/context'
import { useApiModels } from '@api/index'
import ReportErrors from '@components/report/ReportErrors'
import EnvironmentDetails from '@components/environmentDetails/EnvironmentDetails'
import LoadingStatus from '@components/loading/LoadingStatus'
import HistoryNavigation from '@components/historyNavigation/historyNavigation'
import clsx from 'clsx'

export default function PageNavigation(): JSX.Element {
  const modules = useStoreContext(s => s.modules)

  const { isFetching: isFetchingModels } = useApiModels()

  return (
    <div
      className={clsx(
        'relative min-w-[10rem] px-2 min-h-8 max-h-8 w-full flex items-center',
        modules.showHistoryNavigation ? 'justify-between' : 'justify-end',
        isFetchingModels && 'overflow-hidden',
      )}
    >
      {modules.showHistoryNavigation && <HistoryNavigation />}
      {isFetchingModels && (
        <div className="absolute w-full h-full flex justify-center items-center z-10 bg-transparent-20 backdrop-blur-lg">
          <LoadingStatus>Loading Models...</LoadingStatus>
        </div>
      )}
      {modules.hasPlans && <EnvironmentDetails />}
      {modules.hasErrors && <ReportErrors />}
    </div>
  )
}
