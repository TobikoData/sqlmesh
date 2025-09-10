import type { Meta, StoryObj } from '@storybook/react-vite'
import { Input } from './Input'
import { fn, expect, userEvent, within } from 'storybook/test'

export default {
  title: 'Components/Input',
  component: Input,
} satisfies Meta

type Story = StoryObj<typeof Input>

export const Default: Story = {
  args: {
    placeholder: 'Enter text...',
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const input = canvas.getByPlaceholderText('Enter text...')
    await expect(input).toBeInTheDocument()
    await expect(input).toHaveAttribute('type', 'text')
  },
}

export const InputTypes: Story = {
  render: () => (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: 16,
        maxWidth: 320,
      }}
    >
      <Input
        type="text"
        placeholder="Text input"
      />
      <Input
        type="email"
        placeholder="Email input"
      />
      <Input
        type="password"
        placeholder="Password input"
      />
      <Input
        type="number"
        placeholder="Number input"
      />
      <Input
        type="search"
        placeholder="Search input"
      />
      <Input
        type="tel"
        placeholder="Phone input"
      />
      <Input
        type="url"
        placeholder="URL input"
      />
      <Input type="date" />
      <Input type="time" />
      <Input type="file" />
    </div>
  ),
}

export const States: Story = {
  render: () => (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: 16,
        maxWidth: 320,
      }}
    >
      <Input placeholder="Normal input" />
      <Input
        placeholder="Disabled input"
        disabled
      />
      <Input
        placeholder="Read-only input"
        readOnly
        value="Read-only text"
      />
      <Input
        placeholder="With default value"
        defaultValue="Default text"
      />
    </div>
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const disabledInput = canvas.getByPlaceholderText('Disabled input')
    const readOnlyInput = canvas.getByPlaceholderText('Read-only input')

    await expect(disabledInput).toBeDisabled()
    await expect(readOnlyInput).toHaveAttribute('readonly')
    await expect(readOnlyInput).toHaveValue('Read-only text')
  },
}

export const InteractiveTyping: Story = {
  args: {
    placeholder: 'Type something...',
    onChange: fn(),
  },
  play: async ({ canvasElement, args }) => {
    const canvas = within(canvasElement)
    const user = userEvent.setup()
    const input = canvas.getByPlaceholderText('Type something...')

    await expect(input).toBeInTheDocument()
    await user.type(input, 'Hello World')
    await expect(input).toHaveValue('Hello World')
    await expect(args.onChange).toHaveBeenCalled()
  },
}
