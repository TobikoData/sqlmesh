import { useParams } from 'react-router-dom'
import NotFound from '../root/NotFound'
import { EnumRoutes } from '~/routes'
import { useNotificationCenter } from '../root/context/notificationCenter'
import { isNil } from '@utils/index'
import { DisplayError } from '@components/report/ReportErrors'

export default function Content(): JSX.Element {
  const { errors } = useNotificationCenter()
  const { id } = useParams()

  const error = isNil(id)
    ? undefined
    : Array.from(errors).find(error => error.id === id)

  return (
    <div className="flex overflow-auto w-full h-full">
      {isNil(error) ? (
        <NotFound
          link={EnumRoutes.Errors}
          description={isNil(id) ? undefined : `Error ${id} Does Not Exist`}
          message="Back To Errors"
        />
      ) : (
        <div className="p-4 h-full w-full">
          <div className="w-full h-full p-4 rounded-lg bg-danger-5 text-danger-700">
            <DisplayError
              error={error}
              scope={error.key}
              withSplitPane={true}
            />
          </div>
        </div>
      )}
    </div>
  )
}
