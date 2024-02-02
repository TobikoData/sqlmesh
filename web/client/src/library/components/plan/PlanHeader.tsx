import Banner from '@components/banner/Banner'
import { useStoreContext } from '~/context/context'
import { EnumVariant } from '~/types/enum'
import { Disclosure } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'

export default function PlanHeader(): JSX.Element {
  const environment = useStoreContext(s => s.environment)

  return (
    <div className="flex flex-col px-4 py-2 w-full">
      <Banner variant={EnumVariant.Warning}>
        <Disclosure defaultOpen={false}>
          {({ open }) => (
            <>
              <Disclosure.Button className="w-full flex items-center justify-between">
                <Banner.Label className="w-full">
                  {environment.isInitialProd
                    ? 'Initializing Prod Environment'
                    : 'Prod Environment'}
                </Banner.Label>
                {environment.isInitialProd && (
                  <>
                    {open ? (
                      <MinusCircleIcon className="w-5 text-warning-500" />
                    ) : (
                      <PlusCircleIcon className="w-5 text-warning-500" />
                    )}
                  </>
                )}
              </Disclosure.Button>
              {environment.isInitialProd && (
                <Disclosure.Panel className="px-2 text-sm mt-2">
                  <Banner.Description>
                    Prod will be completely backfilled in order to ensure there
                    are no data gaps. After this is applied, it is recommended
                    to validate further changes in a dev environment before
                    deploying to production.
                  </Banner.Description>
                </Disclosure.Panel>
              )}
            </>
          )}
        </Disclosure>
      </Banner>
    </div>
  )
}
