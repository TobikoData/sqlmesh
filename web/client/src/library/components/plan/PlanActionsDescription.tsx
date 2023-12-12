import { useStoreContext } from '~/context/context'
import { usePlan } from './context'
import { isFalse, isStringEmptyOrNil } from '@utils/index'

export default function PlanActionsDescription(): JSX.Element {
  const {
    start,
    end,
    skip_backfill,
    no_gaps,
    no_auto_categorization,
    forward_only,
    restate_models,
    include_unmodified,
  } = usePlan()

  const environment = useStoreContext(s => s.environment)

  return (
    <div className="w-full flex px-4 py-2">
      <p className="ml-2 text-xs">
        <span>Plan for</span>
        <b className="text-primary-500 font-bold mx-1">{environment.name}</b>
        <span className="inline-block mr-1">environment</span>
        {
          <span className="inline-block mr-1">
            from{' '}
            <b>
              {isFalse(isStringEmptyOrNil(start))
                ? start
                : 'the begining of history'}
            </b>
          </span>
        }
        {
          <span className="inline-block mr-1">
            till <b>{isFalse(isStringEmptyOrNil(start)) ? end : 'today'}</b>
          </span>
        }
        {no_gaps && (
          <span className="inline-block mr-1">
            with <b>No Gaps</b>
          </span>
        )}
        {skip_backfill && (
          <span className="inline-block mr-1">
            without <b>Backfills</b>
          </span>
        )}
        {forward_only && (
          <span className="inline-block mr-1">
            consider as a <b>Breaking Change</b>
          </span>
        )}
        {no_auto_categorization && (
          <span className="inline-block mr-1">
            also set <b>Change Category</b> manually
          </span>
        )}
        {isFalse(isStringEmptyOrNil(restate_models)) && (
          <span className="inline-block mr-1">
            and restate the followingmodels <b>{restate_models}</b>
          </span>
        )}
        {include_unmodified && (
          <span className="inline-block mr-1">with views for all models</span>
        )}
      </p>
    </div>
  )
}
