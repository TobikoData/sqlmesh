import type { Meta, StoryObj } from '@storybook/react-vite'

import { EnumShape, EnumSize } from '@/types/enums'
import { Badge } from './Badge'

const meta: Meta<typeof Badge> = {
  title: 'Components/Badge',
  component: Badge,
  tags: ['autodocs'],
  argTypes: {
    size: {
      control: { type: 'select' },
      options: Object.values(EnumSize),
    },
    shape: {
      control: { type: 'select' },
      options: Object.values(EnumShape),
    },
    children: { control: 'text' },
  },
}

export default meta
type Story = StoryObj<typeof Badge>

export const Default: Story = {
  args: {
    children: 'Default Badge',
  },
}

export const Sizes: Story = {
  render: args => (
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
      {Object.values(EnumSize).map(size => (
        <Badge
          key={size}
          size={size}
          {...args}
        >
          {size} Badge
        </Badge>
      ))}
    </div>
  ),
  args: {
    children: undefined,
  },
}

export const Shapes: Story = {
  render: args => (
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
      {Object.values(EnumShape).map(shape => (
        <Badge
          key={shape}
          shape={shape}
          {...args}
        >
          {shape} Badge
        </Badge>
      ))}
    </div>
  ),
  args: {
    children: undefined,
  },
}

export const Colors: Story = {
  render: args => (
    <div
      style={{
        display: 'flex',
        gap: 8,
        flexWrap: 'wrap',
        alignItems: 'center',
      }}
    >
      <Badge
        size={EnumSize.S}
        shape={EnumShape.Pill}
        className="bg-[red] text-light"
        {...args}
      >
        Primary Badge
      </Badge>
      <Badge
        size={EnumSize.S}
        shape={EnumShape.Pill}
        className="bg-[lightblue] text-prose"
        {...args}
      >
        Secondary Badge
      </Badge>
      <Badge
        size={EnumSize.S}
        shape={EnumShape.Round}
        className="bg-[green] text-light"
        {...args}
      >
        Failed Badge
      </Badge>
      <Badge
        size={EnumSize.XXS}
        shape={EnumShape.Round}
        className="bg-[orange] text-light"
        {...args}
      >
        Success Badge
      </Badge>
    </div>
  ),
  args: {
    children: undefined,
  },
}
