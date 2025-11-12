import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, userEvent, waitFor, within, fn } from 'storybook/test'

import { CopyButton } from './CopyButton'
import { Check, Copy } from 'lucide-react'

const meta: Meta<typeof CopyButton> = {
  title: 'Components/CopyButton',
  component: CopyButton,
}

export default meta
type Story = StoryObj<typeof CopyButton>

export const Default: Story = {
  args: {
    text: 'Hello, World!',
    children: copied => (copied ? 'Copied!' : 'Copy'),
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const button = canvas.getByRole('button')
    await expect(button).toHaveTextContent('Copy')
    await expect(button).toBeEnabled()
    const writeTextSpy = fn().mockResolvedValue(undefined)
    if (navigator.clipboard) {
      navigator.clipboard.writeText = writeTextSpy
    } else {
      Object.defineProperty(navigator, 'clipboard', {
        writable: true,
        value: {
          writeText: writeTextSpy,
        },
      })
    }
    const user = userEvent.setup()
    await user.click(button)
    await expect(writeTextSpy).toHaveBeenCalledWith('Hello, World!')
    await waitFor(() => {
      expect(button).toHaveTextContent('Copied!')
    })
    await expect(button).toBeDisabled()
  },
}

export const WithIcons: Story = {
  args: {
    text: 'Copy this text with icon feedback',
    children: copied => (
      <>
        {copied ? (
          <>
            <Check size={14} />
            Copied!
          </>
        ) : (
          <>
            <Copy size={14} />
            Copy
          </>
        )}
      </>
    ),
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const button = canvas.getByRole('button')

    // Initial state with Copy icon
    await expect(button).toHaveTextContent('Copy')

    // Mock clipboard API
    Object.defineProperty(navigator, 'clipboard', {
      writable: true,
      value: {
        writeText: fn().mockResolvedValue(undefined),
      },
    })

    const user = userEvent.setup()
    await user.click(button)

    // Should switch to Check icon and Copied text
    await waitFor(() => {
      expect(button).toHaveTextContent('Copied!')
    })
  },
}

export const IconOnly: Story = {
  args: {
    text: 'This is the text to copy',
    children: copied => (copied ? <Check size={16} /> : <Copy size={16} />),
  },
}

export const CustomDelay: Story = {
  args: {
    text: 'This stays copied for 5 seconds',
    delay: 5000,
    children: copied => (copied ? 'Copied! (5s delay)' : 'Copy (5s feedback)'),
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const button = canvas.getByRole('button')

    // Mock clipboard API
    Object.defineProperty(navigator, 'clipboard', {
      writable: true,
      value: {
        writeText: fn().mockResolvedValue(undefined),
      },
    })

    const user = userEvent.setup()
    await user.click(button)

    // Should show copied state
    await waitFor(() => {
      expect(button).toHaveTextContent('Copied! (5s delay)')
    })

    // Button should remain disabled for custom delay
    await expect(button).toBeDisabled()
  },
}
