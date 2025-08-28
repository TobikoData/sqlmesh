import type { Meta, StoryObj } from '@storybook/react-vite'
import { Tooltip } from '@/components/Tooltip/Tooltip'
import { Button } from '@/components/Button/Button'

const meta: Meta<typeof Tooltip> = {
  title: 'Components/Tooltip',
  component: Tooltip,
}

export default meta
type Story = StoryObj<typeof Tooltip>

export const Default: Story = {
  args: {
    trigger: <Button>Hover me</Button>,
    children: 'This is a tooltip',
  },
}
