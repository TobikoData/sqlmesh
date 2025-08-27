import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {
  vi,
  describe,
  it,
  expect,
  afterEach,
  beforeEach,
  type MockInstance,
} from 'vitest'

import { ClipboardCopy } from './ClipboardCopy'

describe('ClipboardCopy', () => {
  let writeTextSpy: MockInstance

  beforeEach(() => {
    writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText')
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('copies text to clipboard on click', async () => {
    const user = userEvent.setup()
    const writeTextSpy = vi.spyOn(navigator.clipboard, 'writeText')
    render(
      <ClipboardCopy text="Hello, World!">
        {copied => (copied ? 'Copied!' : 'Copy')}
      </ClipboardCopy>,
    )
    const button = screen.getByRole('button')
    await user.click(button)
    expect(writeTextSpy).toHaveBeenCalledWith('Hello, World!')
    expect(writeTextSpy).toHaveBeenCalledTimes(1)
  })

  it('shows copied state after clicking', async () => {
    const user = userEvent.setup()
    render(
      <ClipboardCopy
        text="test"
        delay={1000}
      >
        {copied => (copied ? 'Copied!' : 'Copy')}
      </ClipboardCopy>,
    )
    const button = screen.getByRole('button')
    expect(button).toHaveTextContent('Copy')
    await user.click(button)
    await waitFor(() => {
      expect(button).toHaveTextContent('Copied!')
    })
  })

  it('disables button while in copied state', async () => {
    const user = userEvent.setup()
    render(
      <ClipboardCopy text="test">
        {copied => (copied ? 'Copied!' : 'Copy')}
      </ClipboardCopy>,
    )
    const button = screen.getByRole('button')
    expect(button).toBeEnabled()
    await user.click(button)
    await waitFor(() => {
      expect(button).toBeDisabled()
    })
  })

  it('resets to initial state after delay', async () => {
    const user = userEvent.setup()
    render(
      <ClipboardCopy
        text="test"
        delay={100}
      >
        {copied => (copied ? 'Copied!' : 'Copy')}
      </ClipboardCopy>,
    )
    const button = screen.getByRole('button')
    await user.click(button)
    await waitFor(() => {
      expect(button).toHaveTextContent('Copied!')
    })
    await waitFor(
      () => {
        expect(button).toHaveTextContent('Copy')
        expect(button).toBeEnabled()
      },
      { timeout: 200 },
    )
  })

  it('respects disabled prop', async () => {
    const user = userEvent.setup()
    render(
      <ClipboardCopy
        text="test"
        disabled
      >
        {() => 'Copy'}
      </ClipboardCopy>,
    )
    const button = screen.getByRole('button')
    expect(button).toBeDisabled()
    await user.click(button)
    expect(writeTextSpy).not.toHaveBeenCalled()
  })

  it('calls onClick handler if provided', async () => {
    const onClickSpy = vi.fn()
    const user = userEvent.setup({
      // Tell userEvent to skip its own clipboard setup
      writeToClipboard: false,
    })
    render(
      <ClipboardCopy
        text="test"
        onClick={onClickSpy}
      >
        {() => 'Copy'}
      </ClipboardCopy>,
    )
    await user.click(screen.getByRole('button'))
    expect(onClickSpy).toHaveBeenCalled()
    expect(writeTextSpy).toHaveBeenCalled()
  })
})
