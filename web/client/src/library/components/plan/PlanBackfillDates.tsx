import Input from '../input/Input'
import { EnumPlanActions, usePlan, usePlanDispatch } from './context'

export default function PlanBackfillDates({
  disabled = false,
}: {
  disabled?: boolean
}): JSX.Element {
  const dispatch = usePlanDispatch()
  const { start, end, isInitialPlanRun } = usePlan()

  return (
    <div className="flex w-full flex-wrap md:flex-nowrap">
      <Input
        className="w-full md:w-[50%]"
        label="Start Date (UTC)"
        info="The start datetime of the interval"
        placeholder="2023-12-13"
        disabled={disabled || isInitialPlanRun}
        value={start}
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
        label="End Date (UTC)"
        info="The end datetime of the interval"
        placeholder="2022-12-13"
        value={end}
        disabled={disabled || isInitialPlanRun}
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
