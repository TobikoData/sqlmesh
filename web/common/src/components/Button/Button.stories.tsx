import type { Meta, StoryObj } from '@storybook/react-vite'
import type { Size } from '@sqlmesh-common/types'
import { Button, type ButtonVariant } from './Button'
import { fn, expect, userEvent, within } from 'storybook/test'

const buttonVariants: ButtonVariant[] = [
  'primary',
  'secondary',
  'alternative',
  'destructive',
  'danger',
  'transparent',
]

const meta: Meta<typeof Button> = {
  title: 'Components/Button',
  component: Button,
}

export default meta

type Story = StoryObj<typeof Button>

export const Default: Story = {
  args: {
    children: 'Default Button',
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByText('Default Button')).toBeInTheDocument()
  },
}

export const Variants: Story = {
  render: args => (
    <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
      {Object.values(buttonVariants).map(variant => (
        <Button
          key={variant}
          {...args}
          variant={variant}
        >
          {variant}
        </Button>
      ))}
    </div>
  ),
}

const sizes: Size[] = ['2xs', 'xs', 's', 'm', 'l', 'xl', '2xl']

export const Sizes: Story = {
  render: args => (
    <div
      style={{
        display: 'flex',
        gap: 12,
        flexWrap: 'wrap',
        alignItems: 'center',
      }}
    >
      {sizes.map(size => (
        <Button
          key={size}
          {...args}
          size={size}
        >
          {size}
        </Button>
      ))}
    </div>
  ),
}

export const Disabled: Story = {
  args: {
    children: 'Disabled Button',
    disabled: true,
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const button = canvas.getByRole('button')
    await expect(button).toBeDisabled()
    await expect(button).toHaveTextContent('Disabled Button')
  },
}

export const AsChild: Story = {
  render: args => (
    <Button
      asChild
      {...args}
    >
      <a href="#">Link as Button</a>
    </Button>
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const linkElement = canvas.getByText('Link as Button')
    await expect(linkElement.tagName).toBe('A')
    await expect(linkElement).toHaveAttribute('href', '#')
  },
}

export const InteractiveClick: Story = {
  args: {
    children: 'Click Me',
    onClick: fn(),
  },
  play: async ({ canvasElement, args }) => {
    const canvas = within(canvasElement)
    const user = userEvent.setup()
    const button = canvas.getByRole('button')
    await expect(button).toBeInTheDocument()
    await user.click(button)
    await expect(args.onClick).toHaveBeenCalledTimes(1)
    await user.click(button)
    await expect(args.onClick).toHaveBeenCalledTimes(2)
  },
}
