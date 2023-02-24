import clsx from 'clsx'
import React, { useState, MouseEvent } from 'react'
import { Dialog, RadioGroup } from '@headlessui/react'
import { Divider } from '../divider/Divider'
import { IDE } from '../ide/IDE'
import LogoTobiko from '../logo/Tobiko'
import LogoSqlMesh from '../logo/SqlMesh'
import { EnumRelativeLocation, useStoreContext } from '~/context/context'
import { isStringEmptyOrNil } from '~/utils'
import Modal from '../modal/Modal'
import { Button } from '../button/Button'

import './Root.css'

export default function Root(): JSX.Element {
  return (
    <>
      <Header></Header>
      <Divider />
      <Main>
        <IDE />
        <ModalSetPlan />
      </Main>
      <Divider />
      <Footer></Footer>
    </>
  )
}

function Header(): JSX.Element {
  return (
    <header className="min-h-[2.5rem] px-2 flex justify-between items-center">
      <div className="flex h-full items-center">
        <LogoSqlMesh style={{ height: '32px' }} />
        <span className="inline-block mx-2 text-xs font-bold">by</span>
        <LogoTobiko
          style={{ height: '24px' }}
          mode="dark"
        />
      </div>
      <div>
        <ul className="flex">
          <li className="px-2 prose underline">Docs</li>
          <li className="px-2 prose underline">GitHub</li>
        </ul>
      </div>
    </header>
  )
}

function Main({ children }: { children: React.ReactNode }): JSX.Element {
  return (
    <main
      className="font-sans w-full h-full flex flex-col overflow-hidden"
      tabIndex={0}
    >
      {children}
    </main>
  )
}

function Footer(): JSX.Element {
  return (
    <footer className="px-2 py-1 text-xs flex justify-between">
      <small>{new Date().getFullYear()}</small>
      platform footer
    </footer>
  )
}

function ModalSetPlan(): JSX.Element {
  const CUSTOM = 'custom'

  const environment = useStoreContext(s => s.environment)
  const environments = useStoreContext(s => s.environments)
  const setEnvironment = useStoreContext(s => s.setEnvironment)

  const [selected, setSelected] = useState(CUSTOM)
  const [customValue, setCustomValue] = useState('')

  const FirstThreeEnvoirments = environments.slice(0, 3)

  return (
    <Modal
      show={isStringEmptyOrNil(environment)}
      onClose={() => undefined}
    >
      <Dialog.Panel className="w-[60%] transform overflow-hidden rounded-xl bg-white text-left align-middle shadow-xl transition-all">
        <form className="h-full p-10">
          <fieldset className="mb-4">
            <h2 className="whitespace-nowrap text-xl font-bold mb-1 text-gray-900">
              Select Environment
            </h2>
            <p>
              Plan a migration of the current context&apos;s models with the
              given environment
            </p>
            <div className="flex flex-col w-full mt-3 py-1">
              <RadioGroup
                className="rounded-lg w-full flex flex-wrap"
                value={selected}
                name="environment"
                onChange={(value: string) => {
                  setSelected(value)
                }}
              >
                {FirstThreeEnvoirments.map(environment => (
                  <div
                    key={environment.name}
                    className="w-[50%] py-2 odd:pr-2 even:pl-2"
                  >
                    <RadioGroup.Option
                      value={environment.name}
                      className={({ active, checked }) =>
                        clsx(
                          'relative flex cursor-pointer rounded-md px-3 py-3 focus:outline-none border-2 border-secondary-100',
                          active &&
                            'ring-4 ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
                          checked
                            ? 'border-secondary-500 bg-secondary-100'
                            : 'bg-secondary-100 text-gray-900',
                        )
                      }
                    >
                      {() => (
                        <RadioGroup.Label
                          as="p"
                          className={clsx(
                            'flex items-center',
                            environment.type === EnumRelativeLocation.Remote
                              ? 'text-secondary-500'
                              : 'text-gray-700',
                          )}
                        >
                          {environment.name}
                          <small className="block ml-2 text-gray-400">
                            ({environment.type})
                          </small>
                        </RadioGroup.Label>
                      )}
                    </RadioGroup.Option>
                  </div>
                ))}
                <div className="w-[50%] py-2 pl-2">
                  <RadioGroup.Option
                    value={CUSTOM}
                    className={({ active, checked }) =>
                      clsx(
                        'relative flex cursor-pointer rounded-md focus:outline-none border-2 border-secondary-100 pointer-events-auto',
                        active &&
                          'ring-4 ring-secondary-300 ring-opacity-60 ring-offset ring-offset-secondary-100',
                        checked
                          ? 'border-secondary-500 bg-secondary-100'
                          : 'bg-secondary-100 text-gray-900',
                      )
                    }
                  >
                    <input
                      type="text"
                      name="environment"
                      className="bg-gray-100 px-3 py-3 rounded-md w-full m-0  focus:outline-none"
                      placeholder="Environment Name"
                      onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
                        e.stopPropagation()

                        setCustomValue(e.target.value)
                      }}
                      value={customValue}
                    />
                  </RadioGroup.Option>
                </div>
              </RadioGroup>
              <small className="block text-xs mt-1 px-3 text-gray-500">
                Please select from suggested environments or provide custom name
              </small>
            </div>
          </fieldset>
          <fieldset>
            <div className="flex justify-end">
              <Button
                variant="primary"
                className="mx-0"
                disabled={
                  selected === CUSTOM && isStringEmptyOrNil(customValue)
                }
                onClick={(e: MouseEvent) => {
                  e.stopPropagation()
                  e.preventDefault()

                  const newEnvironment = (
                    selected === CUSTOM ? customValue : selected
                  ).toLowerCase()

                  setEnvironment(newEnvironment)
                }}
              >
                Apply
              </Button>
            </div>
          </fieldset>
        </form>
      </Dialog.Panel>
    </Modal>
  )
}
