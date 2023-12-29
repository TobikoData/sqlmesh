import { useStoreContext } from '~/context/context'
import { usePlan } from './context'
import { isFalse, isNotNil, isStringEmptyOrNil, isTrue } from '@utils/index'
import Banner from '@components/banner/Banner'
import { EnumSize, EnumVariant } from '~/types/enum'

export default function PlanActionsDescription(): JSX.Element {
  const {
    start,
    end,
    skip_tests,
    skip_backfill,
    no_gaps,
    no_auto_categorization,
    forward_only,
    restate_models,
    include_unmodified,
    auto_apply,
    create_from,
  } = usePlan()

  const environment = useStoreContext(s => s.environment)

  return (
    <div className="text-xs px-4 py-2">
      <Banner
        variant={EnumVariant.Warning}
        size={EnumSize.sm}
      >
        <p className="text-sm">
          <span>Plan for</span>
          <b className="text-primary-500 font-bold mx-1">{environment.name}</b>
          <span className="inline-block mr-1">environment</span>
          {isFalse(environment.isProd) && isNotNil(create_from) && (
            <span className="inline-block mr-1">
              <span>based of</span>
              <b className="text-primary-500 font-bold mx-1">{create_from}</b>
            </span>
          )}
          {isTrue(auto_apply) && (
            <span className="inline-block mr-1">
              and <b>Apply</b> automatically
            </span>
          )}
          {isTrue(skip_tests) && (
            <span className="inline-block mr-1">
              without <b>Tests</b>
            </span>
          )}
          {
            <span className="inline-block mr-1">
              <span>from&nbsp;</span>
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
          {isTrue(no_gaps) && (
            <span className="inline-block mr-1">
              with <b>No Gaps</b>
            </span>
          )}
          {isTrue(skip_backfill) && (
            <span className="inline-block mr-1">
              without <b>Backfills</b>
            </span>
          )}
          {isTrue(forward_only) && (
            <span className="inline-block mr-1">
              all changes will be <b>Forward Only</b>
            </span>
          )}
          {isTrue(no_auto_categorization) && (
            <span className="inline-block mr-1">
              also set <b>Change Category</b> manually
            </span>
          )}
          {isFalse(isStringEmptyOrNil(restate_models)) && (
            <span className="inline-block mr-1">
              and restate the following models <b>{restate_models}</b>
            </span>
          )}
          {isTrue(include_unmodified) && (
            <span className="inline-block mr-1">
              and with views for all models in the target development
              environment
            </span>
          )}
        </p>
      </Banner>
    </div>
  )
}
