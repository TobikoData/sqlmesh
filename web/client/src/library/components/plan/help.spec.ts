import { EnumPlanAction } from '../../../context/plan'
import { getActionName, isModified } from './help'

describe('getActionName', () => {
  it('should return "Done" when action is EnumPlanAction.Done', () => {
    const action = EnumPlanAction.Done
    const options = [EnumPlanAction.Done]
    const fallback = 'Start'
    const expected = 'Done'

    const result = getActionName(action, options, fallback)

    expect(result).toBe(expected)
  })

  it('should return "Running..." when action is EnumPlanAction.Running', () => {
    const action = EnumPlanAction.Running
    const options = [EnumPlanAction.Running]
    const fallback = 'Start'
    const expected = 'Running...'

    const result = getActionName(action, options, fallback)

    expect(result).toBe(expected)
  })

  it('should return "Applying..." when action is EnumPlanAction.Applying', () => {
    const action = EnumPlanAction.Applying
    const options = [EnumPlanAction.Applying]
    const fallback = 'Start'
    const expected = 'Applying...'

    const result = getActionName(action, options, fallback)

    expect(result).toBe(expected)
  })

  it('should return "Cancelling..." when action is EnumPlanAction.Cancelling', () => {
    const action = EnumPlanAction.Cancelling
    const options = [EnumPlanAction.Cancelling]
    const fallback = 'Start'
    const expected = 'Cancelling...'

    const result = getActionName(action, options, fallback)

    expect(result).toBe(expected)
  })

  it('should return "Resetting..." when action is EnumPlanAction.Resetting', () => {
    const action = EnumPlanAction.Resetting
    const options = [EnumPlanAction.Resetting]
    const fallback = 'Start'
    const expected = 'Resetting...'

    const result = getActionName(action, options, fallback)

    expect(result).toBe(expected)
  })

  it('should return "Run" when action is EnumPlanAction.Run', () => {
    const action = EnumPlanAction.Run
    const options = [EnumPlanAction.Run]
    const fallback = 'Start'
    const expected = 'Run'

    const result = getActionName(action, options, fallback)

    expect(result).toBe(expected)
  })

  it('should return "Apply" when action is EnumPlanAction.Apply', () => {
    const action = EnumPlanAction.Apply
    const options = [EnumPlanAction.Apply]
    const fallback = 'Start'
    const expected = 'Apply'

    const result = getActionName(action, options, fallback)

    expect(result).toBe(expected)
  })

  it('should return fallback when action is not included in options', () => {
    const action = EnumPlanAction.Done
    const options = [EnumPlanAction.Run]
    const fallback = 'Start'
    const expected = 'Start'

    const result = getActionName(action, options, fallback)

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
