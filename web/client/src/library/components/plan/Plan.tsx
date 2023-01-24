import { RadioGroup } from '@headlessui/react'
import { CheckCircleIcon } from '@heroicons/react/24/solid'
import { Button } from '../button/Button'
import { Divider } from '../divider/Divider'
import { useApiPlan } from '../../../api'
import { useState } from 'react'
import { EnumSize } from '../../../types/enum'

const plans = [
  {
    title: 'Breaking Change',
    description: 'This is a breaking change',
  },
  {
    title: 'Non-Breaking Change',
    description: 'This is a non-breaking change',
  },
  {
    title: 'No Change',
    description: 'This is a no change',
  },
]

export function Plan({ onCancel: cancel }: { onCancel: any }) {
  const [selected, setSelected] = useState(plans[0])
  const [enviroment, setEnviroment] = useState('production')
  const { data: plan } = useApiPlan()

  function runPlan(e: any) {
    e.preventDefault()
    e.stopPropagation()

    const elForm = e.target
    const data = new FormData(e.target)
    const body: any = Object.fromEntries(data.entries())

    body.start_at = body.start_at
      ? new Date(body.start_at).valueOf()
      : undefined

    body.end_at = body.end_at ? new Date(body.end_at).valueOf() : undefined

    console.log(body)

    elForm.reset()
  }

  return (
    <div className="flex w-full h-full overflow-hidden">
      <div className="min-w-[15rem] h-full bg-secondary-900 text-gray-100 overflow-hidden overflow-y-auto">
        <div className="flex flex-col h-full w-100 p-4">
          <h3>Details:</h3>
          <div className="py-2">
            <h5 className="font-bold text-sm">Engine Adapter</h5>
            <small className="block px-2">{plan?.engine_adapter}</small>
          </div>
          <Divider />
          <div className="py-2">
            <h5 className="font-bold text-sm">Path</h5>
            <small className="block px-2">{plan?.path}</small>
          </div>
          <Divider />
          <div className="py-2">
            <h5 className="font-bold text-sm">Dialect</h5>
            <small className="block px-2">{plan?.dialect}</small>
          </div>
          <Divider />
          <div className="py-2">
            <h5 className="font-bold text-sm">Workers</h5>
            <small className="block px-2">{plan?.concurrent_tasks}</small>
          </div>
          <Divider />
          <div className="py-2">
            <h5 className="font-bold text-sm">Scheduler</h5>
            <small className="block px-2">{plan?.scheduler}</small>
          </div>
          <Divider />
          <div className="py-2">
            <h5 className="font-bold text-sm">Time Format</h5>
            <small className="block px-2">{plan?.time_column_format}</small>
          </div>
          <Divider />
          <div className="py-2">
            <h5 className="font-bold text-sm">Tables</h5>
            <ul className="block px-2">
              {plan?.tables.map((tableName: any) => (
                <li key={tableName} className="">
                  <small>{tableName}</small>
                </li>
              ))}
            </ul>
          </div>
          <Divider />
        </div>
      </div>
      <div className="p-4 w-full">
        <div className="flex flex-col w-full h-full">
          {plan?.changes && (
            <>
              <div className="mt-2">
                <div className="flex justify-between">
                  <div>
                    <p>Directly Modified:</p>
                    <ul>
                      <li>Model A</li>
                      <li>Model B</li>
                    </ul>
                  </div>
                  <div>
                    <p>Indirectly Modified:</p>
                    <ul>
                      <li>Model C</li>
                      <li>Model D</li>
                    </ul>
                  </div>
                </div>
              </div>
              <Divider />
            </>
          )}
          {plan?.diff && (
            <>
              <div className="mt-2">
                <h3>Diff</h3>
              </div>
              <Divider />
            </>
          )}
          {plan?.changes && (
            <>
              <div className="mt-2 bg-gray-600 p-4">
                <h3 className="mb-2 text-gray-100">Change Strategy</h3>
                <RadioGroup
                // value={selected}
                // onChange={setSelected}
                >
                  <RadioGroup.Label className="sr-only">
                    Server size
                  </RadioGroup.Label>
                  <div className="space-y-2">
                    {plans.map((plan) => (
                      <RadioGroup.Option
                        key={plan.title}
                        value={plan}
                        className={({ active, checked }) =>
                          `${active
                            ? 'ring-2 ring-white ring-opacity-60 ring-offset-2 ring-offset-sky-300'
                            : ''
                          }
                          ${checked
                            ? 'bg-sky-900 bg-opacity-75 text-white'
                            : 'bg-white'
                          }
                            relative flex cursor-pointer rounded-lg px-5 py-4 shadow-md focus:outline-none`
                        }
                      >
                        {({ active, checked }) => (
                          <>
                            <div className="flex w-full items-center justify-between">
                              <div className="flex items-center">
                                <div className="text-sm">
                                  <RadioGroup.Label
                                    as="p"
                                    className={`font-medium  ${checked ? 'text-white' : 'text-gray-900'
                                      }`}
                                  >
                                    {plan.title}
                                  </RadioGroup.Label>
                                  <RadioGroup.Description
                                    as="span"
                                    className={`inline ${checked ? 'text-sky-100' : 'text-gray-500'
                                      }`}
                                  >
                                    <span>{plan.description}</span>
                                  </RadioGroup.Description>
                                </div>
                              </div>
                              {checked && (
                                <div className="shrink-0 text-white">
                                  <CheckCircleIcon className="h-6 w-6" />
                                </div>
                              )}
                            </div>
                          </>
                        )}
                      </RadioGroup.Option>
                    ))}
                  </div>
                </RadioGroup>
              </div>
              <Divider />
            </>
          )}
          <div className="py-6">
            <form onSubmit={runPlan} id="plan">
              <fieldset>
                <div className="mb-4">
                  <label htmlFor="">
                    <small>Enviroment</small>
                    <input
                      type="text"
                      name="enviroment"
                      className="block bg-gray-100 px-2 py-1 rounded-md"
                      value={enviroment.toLowerCase()}
                      onInput={(e) => {
                        e.stopPropagation()
                        setEnviroment(e.currentTarget.value.toLowerCase())
                      }}
                    />
                  </label>
                </div>
                <div className="flex items-center">
                  <label htmlFor="" className="mr-2">
                    <small>Start Date</small>
                    <input
                      name="start_at"
                      className="block bg-gray-100 px-2 py-1 rounded-md"
                      type="date"
                    />
                  </label>
                  <label htmlFor="" className="mr-2">
                    <small>End Date</small>
                    <input
                      name="end_at"
                      className="block bg-gray-100 px-2 py-1 rounded-md"
                      type="date"
                    />
                  </label>
                </div>
              </fieldset>
              <fieldset className="py-4">
                <div className="flex justify-end">
                  <Button size={EnumSize.sm} type="submit">
                    {plan?.changes ? 'Apply' : 'Run'}
                  </Button>
                  <Button
                    size={EnumSize.sm}
                    onClick={() => cancel(false)}
                    variant="alternative"
                  >
                    Cancel
                  </Button>
                </div>
              </fieldset>
            </form>
          </div>
        </div>
      </div>
    </div>
  )
}
