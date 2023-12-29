import Banner from '@components/banner/Banner'
import { useStoreContext } from '~/context/context'
import { EnumVariant } from '~/types/enum'
import { Disclosure } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import ReportErrors from '@components/report/ReportErrors'
import { useStorePlan } from '@context/plan'
import { isFalse } from '@utils/index'

export default function PlanHeader(): JSX.Element {
  const environment = useStoreContext(s => s.environment)

  const planApply = useStorePlan(s => s.planApply)

  return (
    <div className="flex flex-col p-4 w-full">
      <div className="flex justify-between items-center">
        <h4 className="flex items-center text-xl pb-2 font-bold whitespace-nowrap">
          <span>Target Environment: </span>
          <span className="block ml-2 px-2 py-1 font-sm rounded-md bg-primary-10 text-primary-500">
            {environment.name}
          </span>
        </h4>
        <ReportErrors />
      </div>
      {environment.isInitialProd && isFalse(planApply.isFinished) && (
        <Banner variant={EnumVariant.Warning}>
          <Disclosure defaultOpen={false}>
            {({ open }) => (
              <>
                <Disclosure.Button className="w-full flex items-center justify-between">
                  <Banner.Label className="w-full">
                    Initializing Prod Environment
                  </Banner.Label>
                  {open ? (
                    <MinusCircleIcon className="w-5 text-warning-500" />
                  ) : (
                    <PlusCircleIcon className="w-5 text-warning-500" />
                  )}
                </Disclosure.Button>
                <Disclosure.Panel className="px-2 text-sm mt-2">
                  <Banner.Description>
                    Prod will be completely backfilled in order to ensure there
                    are no data gaps. After this is applied, it is recommended
                    to validate further changes in a dev environment before
                    deploying to production.
                  </Banner.Description>
                </Disclosure.Panel>
              </>
            )}
          </Disclosure>
        </Banner>
      )}
    </div>
  )
}
