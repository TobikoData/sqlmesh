import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, userEvent, waitFor, within } from '@storybook/test'
import { Copy, Check } from 'lucide-react'

import { ClipboardCopy } from './ClipboardCopy'
import { EnumButtonVariant } from '@/components/Button/help'
import { EnumSize } from '@/types/enums'
import { vi } from 'vitest'

const meta: Meta<typeof ClipboardCopy> = {
  title: 'Components/ClipboardCopy',
  component: ClipboardCopy,
  tags: ['autodocs'],
  argTypes: {
    text: {
      control: 'text',
      description: 'The text to copy to clipboard',
    },
    delay: {
      control: { type: 'number', min: 500, max: 5000, step: 500 },
      description: 'Time in ms before resetting the copied state',
    },
    variant: {
      control: { type: 'select' },
      options: Object.values(EnumButtonVariant),
    },
    size: {
      control: { type: 'select' },
      options: Object.values(EnumSize),
    },
    disabled: {
      control: 'boolean',
    },
    title: {
      control: 'text',
    },
  },
}

export default meta
type Story = StoryObj<typeof ClipboardCopy>

export const Default: Story = {
  args: {
    text: 'Hello, World!',
    children: copied => (copied ? 'Copied!' : 'Copy'),
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const button = canvas.getByRole('button')

    // Initial state
    await expect(button).toHaveTextContent('Copy')
    await expect(button).toBeEnabled()

    // Mock clipboard API
    const writeTextSpy = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      writable: true,
      value: {
        writeText: writeTextSpy,
      },
    })

    const user = userEvent.setup()
    await user.click(button)

    // Should call clipboard API with correct text
    await expect(writeTextSpy).toHaveBeenCalledWith('Hello, World!')

    // Should show copied state
    await waitFor(() => {
      expect(button).toHaveTextContent('Copied!')
    })

    // Should be disabled while in copied state
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
        writeText: vi.fn().mockResolvedValue(undefined),
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
        writeText: vi.fn().mockResolvedValue(undefined),
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

export const Sizes: Story = {
  render: () => (
    <div
      style={{
        display: 'flex',
        gap: 12,
        alignItems: 'center',
        flexWrap: 'wrap',
      }}
    >
      {Object.values(EnumSize).map(size => (
        <ClipboardCopy
          key={size}
          text={`Size ${size} text`}
          size={size}
        >
          {copied => (copied ? 'Copied!' : `Size ${size}`)}
        </ClipboardCopy>
      ))}
    </div>
  ),
}

export const Variants: Story = {
  render: () => (
    <div
      style={{
        display: 'flex',
        gap: 12,
        alignItems: 'center',
        flexWrap: 'wrap',
      }}
    >
      {Object.values(EnumButtonVariant).map(variant => (
        <ClipboardCopy
          key={variant}
          text={`${variant} variant text`}
          variant={variant}
        >
          {copied => (
            <>
              {copied ? <Check size={14} /> : <Copy size={14} />}
              {variant}
            </>
          )}
        </ClipboardCopy>
      ))}
    </div>
  ),
}

export const DisabledState: Story = {
  args: {
    text: 'This cannot be copied',
    disabled: true,
    children: () => 'Copy (Disabled)',
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const button = canvas.getByRole('button')

    // Should be disabled
    await expect(button).toBeDisabled()
    await expect(button).toHaveTextContent('Copy (Disabled)')

    // Mock clipboard API
    const writeTextSpy = vi.fn()
    Object.defineProperty(navigator, 'clipboard', {
      writable: true,
      value: {
        writeText: writeTextSpy,
      },
    })

    // Clicking disabled button should not work
    const user = userEvent.setup()
    await user.click(button)

    // Should not call clipboard API
    await expect(writeTextSpy).not.toHaveBeenCalled()
  },
}
