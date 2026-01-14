import type { Meta, StoryObj } from '@storybook/react-vite'
import type { Side } from '@sqlmesh-common/types'
import { LoadingContainer } from './LoadingContainer'
import { expect, within } from 'storybook/test'

const meta: Meta<typeof LoadingContainer> = {
  title: 'Components/Containers/LoadingContainer',
  component: LoadingContainer,
}

export default meta

type Story = StoryObj<typeof LoadingContainer>

export const Default: Story = {
  args: {
    isLoading: true,
  },
}

export const WithMessage: Story = {
  args: {
    isLoading: true,
    message: 'Loading data...',
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByText('Loading data...')).toBeInTheDocument()
  },
}

export const WithContent: Story = {
  args: {
    isLoading: true,
    message: 'Processing',
    children: (
      <div
        style={{
          padding: '8px 16px',
          backgroundColor: '#f0f0f0',
          borderRadius: '4px',
        }}
      >
        Main Content
      </div>
    ),
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByText('Main Content')).toBeInTheDocument()
    await expect(canvas.getByText('Processing')).toBeInTheDocument()
  },
}

const sides: Side[] = ['left', 'right', 'both']

export const LoadingSides: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      {sides.map(side => (
        <LoadingContainer
          isLoading={true}
          side={side}
        >
          <div
            style={{
              padding: '4px 12px',
              backgroundColor: 'lightgray',
              borderRadius: '4px',
              fontWeight: 500,
            }}
          >
            Content
          </div>
        </LoadingContainer>
      ))}
    </div>
  ),
}
