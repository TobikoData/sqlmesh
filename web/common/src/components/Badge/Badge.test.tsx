import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'

import { Badge } from './Badge'
import { badgeVariants } from './help'
import { EnumShape, EnumSize } from '@/types/enums'
import { cn } from '@/utils'

describe('Badge', () => {
  it('renders with default props and children', () => {
    render(<Badge>Test Badge</Badge>)
    expect(screen.getByText('Test Badge')).toBeInTheDocument()
  })

  it('applies the size class for each size', () => {
    Object.values(EnumSize).forEach(size => {
      const variants = cn(badgeVariants({ size }))
      render(<Badge size={size}>Size {size}</Badge>)
      expect(screen.getByText(`Size ${size}`)).toHaveClass(variants)
    })
  })

  it('applies the shape class for each shape', () => {
    Object.values(EnumShape).forEach(shape => {
      const variants = cn(badgeVariants({ shape }))
      render(<Badge shape={shape}>Shape {shape}</Badge>)
      expect(screen.getByText(`Shape ${shape}`)).toHaveClass(variants)
    })
  })

  it('supports custom size and shape', () => {
    render(
      <Badge
        size={EnumSize.XXL}
        shape={EnumShape.Square}
      >
        Custom Size and Shape
      </Badge>,
    )
    expect(screen.getByText('Custom Size and Shape')).toHaveClass(
      cn(badgeVariants({ size: EnumSize.XXL, shape: EnumShape.Square })),
    )
  })

  it('applies custom className', () => {
    render(<Badge className="custom-class">Custom Class</Badge>)
    expect(screen.getByText('Custom Class')).toHaveClass('custom-class')
  })

  it('renders as a child element when asChild is true', () => {
    render(
      <Badge asChild>
        <a href="#">Link Badge</a>
      </Badge>,
    )
    expect(screen.getByText('Link Badge').tagName).toBe('A')
  })
})
