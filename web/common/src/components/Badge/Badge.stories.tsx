import type { Meta, StoryObj } from '@storybook/react-vite'

import type { Shape, Size } from '@sqlmesh-common/types'
import { Badge } from './Badge'

const meta: Meta<typeof Badge> = {
  title: 'Components/Badge',
  component: Badge,
}

export default meta
type Story = StoryObj<typeof Badge>

export const Default: Story = {
  args: {
    children: 'Default Badge',
  },
}

const sizes: Size[] = ['2xs', 'xs', 's', 'm', 'l', 'xl', '2xl']
const shapes: Shape[] = ['square', 'round', 'pill']

export const Sizes: Story = {
  render: args => (
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
      {sizes.map(size => (
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
      {shapes.map(shape => (
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
        size="s"
        shape="pill"
        className="bg-[red] text-light"
        {...args}
      >
        Primary Badge
      </Badge>
      <Badge
        size="s"
        shape="pill"
        className="bg-[lightblue] text-prose"
        {...args}
      >
        Secondary Badge
      </Badge>
      <Badge
        size="s"
        shape="round"
        className="bg-[green] text-light"
        {...args}
      >
        Failed Badge
      </Badge>
      <Badge
        size="2xs"
        shape="round"
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
