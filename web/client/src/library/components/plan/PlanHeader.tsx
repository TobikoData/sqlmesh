import Banner from '@components/banner/Banner'
import { useStoreContext } from '~/context/context'
import { EnumVariant } from '~/types/enum'
import { usePlan } from './context'
import { isObjectNotEmpty } from '@utils/index'
import { Disclosure } from '@headlessui/react'
import { MinusCircleIcon, PlusCircleIcon } from '@heroicons/react/24/solid'
import ReportTestsErrors from '@components/report/ReportTestsErrors'
import ReportErrors from '@components/report/ReportErrors'

export default function PlanHeader(): JSX.Element {
  const environment = useStoreContext(s => s.environment)

  const { testsReportErrors } = usePlan()

  return (
    <div className="flex flex-col py-2 w-full">
      <div className="flex justify-between">
        <h4 className="text-xl pb-2 px-6">
          <span className="font-bold">Target Environment is</span>
          <b className="ml-2 px-2 py-1 font-sm rounded-md bg-primary-10 text-primary-500">
            {environment.name}
          </b>
        </h4>
        <div className="px-6">
          <ReportErrors />
        </div>
      </div>
      <div className="w-full h-full overflow-auto hover:scrollbar scrollbar--vertical px-6 ">
        {environment.isInitial && environment.isDefault && (
          <Banner variant={EnumVariant.Warning}>
            <Disclosure defaultOpen={true}>
              {({ open }) => (
                <>
                  <div className="flex items-center">
                    <Banner.Headline className="w-full mr-2 text-sm mb-0">
                      Initializing Prod Environment
                    </Banner.Headline>
                    <Disclosure.Button className="flex items-center justify-between rounded-lg text-left text-sm">
                      {open ? (
                        <MinusCircleIcon className="h-6 w-6 text-warning-500" />
                      ) : (
                        <PlusCircleIcon className="h-6 w-6 text-warning-500" />
                      )}
                    </Disclosure.Button>
                  </div>
                  <Disclosure.Panel className="px-4 pb-2 text-sm mt-2">
                    <Banner.Description>
                      Prod will be completely backfilled in order to ensure
                      there are no data gaps. After this is applied, it is
                      recommended to validate further changes in a dev
                      environment before deploying to production.
                    </Banner.Description>
                  </Disclosure.Panel>
                </>
              )}
            </Disclosure>
          </Banner>
        )}
        {testsReportErrors != null && isObjectNotEmpty(testsReportErrors) && (
          <Banner variant={EnumVariant.Danger}>
            <Disclosure defaultOpen={false}>
              {({ open }) => (
                <>
                  <div className="flex items-center">
                    <p className="w-full mr-2 text-sm">
                      {testsReportErrors?.title}
                    </p>
                    <Disclosure.Button className="flex items-center justify-between rounded-lg text-left text-sm">
                      {open ? (
                        <MinusCircleIcon className="h-6 w-6 text-danger-500" />
                      ) : (
                        <PlusCircleIcon className="h-6 w-6 text-danger-500" />
                      )}
                    </Disclosure.Button>
                  </div>
                  <Disclosure.Panel className="px-4 pb-2 text-sm">
                    <ReportTestsErrors report={testsReportErrors} />
                  </Disclosure.Panel>
                </>
              )}
            </Disclosure>
          </Banner>
        )}
      </div>
    </div>
  )
}
