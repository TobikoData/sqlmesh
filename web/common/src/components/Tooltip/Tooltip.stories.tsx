import type { Meta, StoryObj } from '@storybook/react-vite'
import {
  Info,
  HelpCircle,
  AlertCircle,
  Settings,
  User,
  Mail,
  Copy,
  Download,
} from 'lucide-react'

import Tooltip from './Tooltip'
import { Button } from '@/components/Button/Button'
import { Badge } from '@/components/Badge/Badge'
import { EnumButtonVariant } from '@/components/Button/help'
import { EnumSize } from '@/types/enums'

const meta: Meta<typeof Tooltip> = {
  title: 'Components/Tooltip',
  component: Tooltip,
  tags: ['autodocs'],
  argTypes: {
    side: {
      control: { type: 'select' },
      options: ['top', 'right', 'bottom', 'left'],
    },
    align: {
      control: { type: 'select' },
      options: ['start', 'center', 'end'],
    },
    delayDuration: {
      control: { type: 'number', min: 0, max: 1000, step: 100 },
    },
    sideOffset: {
      control: { type: 'number', min: 0, max: 50, step: 5 },
    },
    alignOffset: {
      control: { type: 'number', min: -50, max: 50, step: 5 },
    },
  },
}

export default meta
type Story = StoryObj<typeof Tooltip>

export const Default: Story = {
  args: {
    trigger: <Button>Hover me</Button>,
    children: 'This is a tooltip',
  },
}
