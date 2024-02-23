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
        'min-w-[10rem] px-2 h-8 w-full flex items-center',
        modules.showHistoryNavigation ? 'justify-between' : 'justify-end',
      )}
    >
      {modules.showHistoryNavigation && <HistoryNavigation />}
      {isFetchingModels ? (
        <LoadingStatus>Loading Models...</LoadingStatus>
      ) : (
        modules.hasPlans && <EnvironmentDetails />
      )}
      {modules.hasErrors && <ReportErrors />}
    </div>
  )
}
