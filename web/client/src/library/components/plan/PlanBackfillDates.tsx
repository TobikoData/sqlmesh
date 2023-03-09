import Input from '../input/Input'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'

export default function PlanBackfillDates({
  disabled = false,
}: {
  disabled?: boolean
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const { start, end, is_initial } = usePlan()

  return (
    <div className="flex w-full flex-wrap md:flex-nowrap">
      <Input
        className="w-full md:w-[50%]"
        label="Start Date"
        info="The start datetime of the interval"
        placeholder="01/01/2023"
        disabled={disabled || is_initial}
        value={start ?? ''}
        onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
          e.stopPropagation()

          dispatch({
            type: EnumPlanActions.DateStart,
            start: e.target.value,
          })
        }}
      />
      <Input
        className="w-full md:w-[50%]"
        label="End Date"
        info="The end datetime of the interval"
        placeholder="02/13/2023"
        value={end ?? ''}
        disabled={disabled || is_initial}
        onInput={(e: React.ChangeEvent<HTMLInputElement>) => {
          e.stopPropagation()

          dispatch({
            type: EnumPlanActions.DateEnd,
            end: e.target.value,
          })
        }}
      />
    </div>
  )
}
