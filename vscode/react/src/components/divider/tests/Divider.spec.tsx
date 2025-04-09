import { describe, it, expect } from 'vitest'
import { render } from '../../../../tests/utils'
import { EnumSize } from '../../../../types/enum'
import { Divider, SIZE } from '../Divider'

describe('Divider', () => {
  it('should render the correct size class', () => {
    const { container } = render(<Divider size={EnumSize.lg} />)
    expect(container.firstChild).toHaveClass(
      `border-b${SIZE.get(EnumSize.lg) ?? ''}`,
    )
  })

  it('should render the correct orientation class', () => {
    const { container } = render(<Divider orientation="vertical" />)
    expect(container.firstChild).toHaveClass(
      `border-r${SIZE.get(EnumSize.sm) ?? ''}`,
    )
  })
})
