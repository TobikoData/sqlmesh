import { EnumPlanAction, PlanAction } from '../../../context/plan'
import { isArrayNotEmpty } from '../../../utils'

export function getActionName(
  action: PlanAction,
  options: string[] = [],
  fallback: string = 'Start',
): string {
  if (!options.includes(action)) return fallback

  let name: string

  switch (action) {
    case EnumPlanAction.Done:
      name = 'Done'
      break
    case EnumPlanAction.Running:
      name = 'Running...'
      break
    case EnumPlanAction.Applying:
      name = 'Applying...'
      break
    case EnumPlanAction.Canceling:
      name = 'Canceling...'
      break
    case EnumPlanAction.Resetting:
      name = 'Resetting...'
      break
    case EnumPlanAction.Closing:
      name = 'Closing...'
      break
    case EnumPlanAction.Run:
      name = 'Run'
      break
    case EnumPlanAction.Apply:
      name = 'Apply'
      break
    default:
      name = fallback
      break
  }

  return name
}

export function isModified<T extends object>(modified?: T): boolean {
  return Object.values(modified ?? {}).some(isArrayNotEmpty)
}
