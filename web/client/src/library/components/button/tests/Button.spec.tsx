import React from 'react'
import { vi, describe, test, expect } from 'vitest'
import { render, fireEvent, renderHook } from '../../../../tests/utils'
import { EnumSize, EnumVariant } from '../../../../types/enum'
import {
  Button,
  makeButton,
  VARIANT,
  SHAPE,
  SIZE,
  EnumButtonShape,
} from '../Button'

describe('Button', () => {
  test('renders with default variant, shape, and size', () => {
    const { getByText } = render(<Button>Click me</Button>)
    const button = getByText('Click me')

    expect(button).toHaveClass(VARIANT.get(EnumVariant.Primary) as string)
    expect(button).toHaveClass(SHAPE.get(EnumButtonShape.Rounded) as string)
    expect(button).toHaveClass(SIZE.get(EnumSize.md) as string)
  })

  test('renders with custom variant, shape, and size', () => {
    const { getByText } = render(
      <Button
        variant={EnumVariant.Secondary}
        shape={EnumButtonShape.Square}
        size={EnumSize.sm}
      >
        Click me
      </Button>,
    )
    const button = getByText('Click me')

    expect(button).toHaveClass(VARIANT.get(EnumVariant.Secondary) as string)
    expect(button).toHaveClass(SHAPE.get(EnumButtonShape.Square) as string)
    expect(button).toHaveClass(SIZE.get(EnumSize.sm) as string)
  })

  test('calls onClick when clicked', () => {
    const onClick = vi.fn()
    const { getByText } = render(<Button onClick={onClick}>Click me</Button>)
    const button = getByText('Click me')

    fireEvent.click(button)
    expect(onClick).toHaveBeenCalled()
  })
})

describe('ButtonMenu', () => {
  test('renders with default variant, shape, and size', () => {
    const ButtonMenu = createButton()
    const { getByText } = render(<ButtonMenu>Click me</ButtonMenu>)
    const button = getByText('Click me')

    expect(button).toHaveClass(VARIANT.get(EnumVariant.Primary) as string)
    expect(button).toHaveClass(SHAPE.get(EnumButtonShape.Rounded) as string)
    expect(button).toHaveClass(SIZE.get(EnumSize.md) as string)
  })

  test('renders with custom variant, shape, and size', () => {
    const ButtonMenu = createButton()
    const { getByText } = render(
      <ButtonMenu
        variant={EnumVariant.Secondary}
        shape={EnumButtonShape.Square}
        size={EnumSize.sm}
      >
        Click me
      </ButtonMenu>,
    )
    const button = getByText('Click me')

    expect(button).toHaveClass(VARIANT.get(EnumVariant.Secondary) as string)
    expect(button).toHaveClass(SHAPE.get(EnumButtonShape.Square) as string)
    expect(button).toHaveClass(SIZE.get(EnumSize.sm) as string)
  })

  test('calls onClick when clicked', () => {
    const ButtonMenu = createButton()
    const onClick = vi.fn()
    const { getByText } = render(
      <ButtonMenu onClick={onClick}>Click me</ButtonMenu>,
    )
    const button = getByText('Click me')

    fireEvent.click(button)
    expect(onClick).toHaveBeenCalled()
  })

  test('get ref from component', () => {
    const ButtonMenu = createButton()
    const { result } = renderHook(() => React.useRef<HTMLDivElement>(null))
    const { getByText } = render(
      <ButtonMenu ref={result.current}>Click me</ButtonMenu>,
    )
    const button = getByText('Click me')

    expect(result.current.current).toBe(button)
  })
})

function createButton(): any {
  return makeButton(
    React.forwardRef<any, any>(function Button({ children, ...props }, ref) {
      return (
        <div
          {...props}
          role="button"
          ref={ref}
        >
          {children}
        </div>
      )
    }),
  )
}
