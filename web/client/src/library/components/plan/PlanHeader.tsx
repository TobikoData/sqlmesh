import Banner from '@components/banner/Banner'
import { useStoreContext } from '~/context/context'
import { EnumVariant } from '~/types/enum'

export default function Plan({ error }: { error?: Error }): JSX.Element {
  const environment = useStoreContext(s => s.environment)

  return (
    <div className="w-full py-4 px-6">
      <h4 className="text-xl">
        <span className="font-bold">Target Environment is</span>
        <b className="ml-2 px-2 py-1 font-sm rounded-md bg-primary-10 text-primary-500">
          {environment.name}
        </b>
      </h4>
      {environment.isInitial && environment.isDefault && (
        <Banner
          variant={EnumVariant.Warning}
          headline="Initializing Prod Environment"
          description="Prod will be completely backfilled in order to ensure there are no data gaps.
          After this is applied, it is recommended to validate further changes in a dev environment before
          deploying to production."
        />
      )}
      {error != null && (
        <Banner
          variant={EnumVariant.Danger}
          headline="Error"
          description={error.message}
        />
      )}
    </div>
  )
}
