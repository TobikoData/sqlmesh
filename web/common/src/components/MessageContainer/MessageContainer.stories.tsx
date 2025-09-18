import type { Meta, StoryObj } from '@storybook/react-vite'
import { MessageContainer } from './MessageContainer'

const meta = {
  title: 'Components/Containers/MessageContainer',
  component: MessageContainer,
} satisfies Meta<typeof MessageContainer>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    children: 'This is a default message container with some content',
    isLoading: false,
    wrap: false,
  },
}

export const WithLongText: Story = {
  args: {
    children:
      'This is a very long message that demonstrates how the MessageContainer handles overflow text when the content exceeds the available width. By default, it will truncate with ellipsis.',
    isLoading: false,
    wrap: false,
  },
}

export const WithWrapping: Story = {
  args: {
    children:
      'This is a very long message that demonstrates how the MessageContainer handles overflow text when wrapping is enabled. With wrap set to true, the text will wrap to multiple lines instead of being truncated.',
    isLoading: false,
    wrap: true,
  },
}

export const Loading: Story = {
  args: {
    children: 'This content is loading...',
    isLoading: true,
    wrap: false,
  },
}

export const LoadingWithWrap: Story = {
  args: {
    children:
      'This is a longer message that is currently loading. When both loading and wrap are enabled, the skeleton animation will respect the wrapping behavior.',
    isLoading: true,
    wrap: true,
  },
}

export const WithCustomStyling: Story = {
  args: {
    children: 'Custom styled message container',
    className:
      'bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 font-bold',
    isLoading: false,
    wrap: false,
  },
}
