import clsx from 'clsx'
import Input from '../input/Input'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'

export default function PlanBackfillDates({
  disabled = false,
  className,
}: {
  disabled?: boolean
  className?: string
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const { start, end, isInitialPlanRun } = usePlan()

  return (
    <div className={clsx('flex w-full flex-wrap md:flex-nowrap', className)}>
      <Input
        className="w-full md:w-[50%]"
        label="Start Date (UTC)"
        info="The start datetime of the interval"
        disabled={disabled || isInitialPlanRun}
      >
        {({ disabled, className }) => (
          <Input.Textfield
            className={clsx(className, 'w-full')}
            disabled={disabled}
            placeholder="2023-12-13"
            value={start}
            onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
              e.stopPropagation()

              dispatch({
                type: EnumPlanActions.DateStart,
                start: e.target.value,
              })
            }}
          />
        )}
      </Input>
      <Input
        className="w-full md:w-[50%]"
        label="End Date (UTC)"
        info="The end datetime of the interval"
        disabled={disabled || isInitialPlanRun}
      >
        {({ disabled, className }) => (
          <Input.Textfield
            className={clsx(className, 'w-full')}
            disabled={disabled}
            placeholder="2022-12-13"
            value={end}
            onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
              e.stopPropagation()

              dispatch({
                type: EnumPlanActions.DateEnd,
                end: e.target.value,
              })
            }}
          />
        )}
      </Input>
    </div>
  )
}
