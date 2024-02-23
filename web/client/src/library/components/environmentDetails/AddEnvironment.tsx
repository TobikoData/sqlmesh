import clsx from 'clsx'
import { useStoreContext } from '@context/context'
import { EnumSize, EnumVariant } from '~/types/enum'
import { isStringEmptyOrNil } from '@utils/index'
import { type ButtonSize, Button } from '@components/button/Button'
import { useState, type MouseEvent } from 'react'
import Input from '@components/input/Input'

export function AddEnvironment({
  onAdd,
  className,
  label = 'Add',
  size = EnumSize.sm,
}: {
  size?: ButtonSize
  onAdd?: () => void
  className?: string
  label?: string
}): JSX.Element {
  const getNextEnvironment = useStoreContext(s => s.getNextEnvironment)
  const isExistingEnvironment = useStoreContext(s => s.isExistingEnvironment)
  const addLocalEnvironment = useStoreContext(s => s.addLocalEnvironment)

  const [newEnvironment, setCustomEnvironment] = useState<string>('')
  const [createdFrom, setCreatedFrom] = useState<string>(
    getNextEnvironment().name,
  )

  function handleAddEnvironment(e: MouseEvent): void {
    e.stopPropagation()

    addLocalEnvironment(newEnvironment, createdFrom)

    setCustomEnvironment('')
    setCreatedFrom(getNextEnvironment().name)

    onAdd?.()
  }

  function handleInputEnvironment(
    e: React.ChangeEvent<HTMLInputElement>,
  ): void {
    e.stopPropagation()

    setCustomEnvironment(e.target.value)
  }

  const isDisabled =
    isStringEmptyOrNil(newEnvironment) || isExistingEnvironment(newEnvironment)

  return (
    <div className={clsx('flex w-full items-center bg-secondary-5', className)}>
      <Input
        className="my-0 mx-0 mr-4 min-w-[10rem] w-full"
        size={size}
      >
        {({ className }) => (
          <Input.Textfield
            className={clsx(className, 'w-full')}
            placeholder="Environment"
            value={newEnvironment}
            onInput={handleInputEnvironment}
          />
        )}
      </Input>
      <Button
        className="my-0 mx-0 font-bold"
        variant={EnumVariant.Secondary}
        size={size}
        disabled={isDisabled}
        onClick={handleAddEnvironment}
      >
        {label}
      </Button>
    </div>
  )
}
