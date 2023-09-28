import { EnumPlanAction, type PlanAction } from '../../../context/plan'
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
    case EnumPlanAction.Cancelling:
      name = 'Cancelling...'
      break
    case EnumPlanAction.Run:
      name = 'Run'
      break
    case EnumPlanAction.ApplyVirtual:
      name = 'Apply Virtual'
      break
    case EnumPlanAction.ApplyBackfill:
      name = 'Apply Backfill'
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
