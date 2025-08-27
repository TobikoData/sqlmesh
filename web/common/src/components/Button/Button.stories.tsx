import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, userEvent, within } from 'storybook/test'

import { EnumSize } from '@/types/enums'
import { Button } from './Button'
import { EnumButtonVariant } from './help'

const meta: Meta<typeof Button> = {
  title: 'Components/Button',
  component: Button,
  tags: ['autodocs'],
  argTypes: {
    onClick: { action: 'clicked' },
    variant: {
      control: { type: 'select' },
      options: Object.values(EnumButtonVariant),
    },
    size: {
      control: { type: 'select' },
      options: Object.values(EnumSize),
    },
    type: {
      control: { type: 'select' },
      options: ['button', 'submit', 'reset'],
    },
    disabled: {
      control: 'boolean',
    },
  },
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
      {Object.values(EnumButtonVariant).map(variant => (
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
      {Object.values(EnumSize).map(size => (
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

export const Types: Story = {
  render: args => (
    <div style={{ display: 'flex', gap: 12 }}>
      <Button
        {...args}
        type="button"
      >
        Button
      </Button>
      <Button
        {...args}
        type="submit"
      >
        Submit
      </Button>
      <Button
        {...args}
        type="reset"
      >
        Reset
      </Button>
    </div>
  ),
}

export const InteractiveClick: Story = {
  args: {
    children: 'Click Me',
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
