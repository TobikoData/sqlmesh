import type { Meta, StoryObj } from '@storybook/react-vite'

import { Metadata } from './Metadata'

const meta: Meta<typeof Metadata> = {
  title: 'Components/Metadata',
  component: Metadata,
  args: {
    label: 'Label',
    value: 'Value',
  },
  argTypes: {
    label: { control: 'text' },
    value: { control: 'text' },
    className: { control: 'text' },
  },
}
export default meta
type Story = StoryObj<typeof Metadata>

export const Default: Story = {}

export const CustomClassName: Story = {
  args: {
    label: 'Custom Label',
    value: 'Custom Value',
    className: 'bg-neutral-100 p-2 rounded border border-neutral-200',
  },
}

export const WithReactNode: Story = {
  args: {
    label: (
      <span>
        <b>ReactNode</b> Label
      </span>
    ),
    value: <span style={{ color: 'green' }}>ReactNode Value</span>,
  },
}
