import { describe, it, expect } from 'vitest'
import { EnumPlanAction, ModelPlanAction } from '@models/plan-action'
import { isModified } from './help'

describe('getActionDisplayName', () => {
  it('should return "Done" when action is EnumPlanAction.Done', () => {
    const action = new ModelPlanAction({ value: EnumPlanAction.Done })
    const options = [EnumPlanAction.Done]
    const fallback = 'Start'
    const expected = 'Done'

    const result = ModelPlanAction.getActionDisplayName(
      action,
      options,
      fallback,
    )

    expect(result).toBe(expected)
  })

  it('should return "Running..." when action is EnumPlanAction.Running', () => {
    const action = new ModelPlanAction({ value: EnumPlanAction.Running })
    const options = [EnumPlanAction.Running]
    const fallback = 'Start'
    const expected = 'Running...'

    const result = ModelPlanAction.getActionDisplayName(
      action,
      options,
      fallback,
    )

    expect(result).toBe(expected)
  })

  it('should return "Applying..." when action is EnumPlanAction.Applying', () => {
    const action = new ModelPlanAction({ value: EnumPlanAction.Applying })
    const options = [EnumPlanAction.Applying]
    const fallback = 'Start'
    const expected = 'Applying...'

    const result = ModelPlanAction.getActionDisplayName(
      action,
      options,
      fallback,
    )

    expect(result).toBe(expected)
  })

  it('should return "Cancelling..." when action is EnumPlanAction.Cancelling', () => {
    const action = new ModelPlanAction({ value: EnumPlanAction.Cancelling })
    const options = [EnumPlanAction.Cancelling]
    const fallback = 'Start'
    const expected = 'Cancelling...'

    const result = ModelPlanAction.getActionDisplayName(
      action,
      options,
      fallback,
    )

    expect(result).toBe(expected)
  })

  it('should return "Run" when action is EnumPlanAction.Run', () => {
    const action = new ModelPlanAction({ value: EnumPlanAction.Run })
    const options = [EnumPlanAction.Run]
    const fallback = 'Start'
    const expected = 'Run'

    const result = ModelPlanAction.getActionDisplayName(
      action,
      options,
      fallback,
    )

    expect(result).toBe(expected)
  })

  it('should return "Apply" when action is EnumPlanAction.ApplyBackfill', () => {
    const fallback = 'Start'
    let result = ModelPlanAction.getActionDisplayName(
      new ModelPlanAction({ value: EnumPlanAction.ApplyBackfill }),
      [EnumPlanAction.ApplyBackfill],
      fallback,
    )

    expect(result).toBe('Apply And Backfill')

    result = ModelPlanAction.getActionDisplayName(
      new ModelPlanAction({ value: EnumPlanAction.ApplyVirtual }),
      [EnumPlanAction.ApplyVirtual],
      fallback,
    )

    expect(result).toBe('Apply Virtual Update')
  })

  it('should return fallback when action is not included in options', () => {
    const action = new ModelPlanAction({ value: EnumPlanAction.Done })
    const options = [EnumPlanAction.Run]
    const fallback = 'Start'
    const expected = 'Start'

    const result = ModelPlanAction.getActionDisplayName(
      action,
      options,
      fallback,
    )

    expect(result).toBe(expected)
  })
})

describe('isModified', () => {
  it('should return true when modified is an object with at least one non-empty array', () => {
    const modified = {
      array1: [1, 2, 3],
      array2: [],
    }

    expect(isModified(modified)).toBe(true)
  })

  it('should return false when modified is an object with only empty arrays', () => {
    const modified = {
      array1: [],
      array2: [],
    }

    expect(isModified(modified)).toBe(false)
  })

  it('should return false when modified is an empty object', () => {
    expect(isModified({})).toBe(false)
  })

  it('should return false when modified is undefined', () => {
    expect(isModified(undefined)).toBe(false)
  })
})
