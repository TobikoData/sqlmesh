import { userEvent } from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ErrorContainer } from './ErrorContainer'

describe('ErrorContainer', () => {
  const mockNavigate = vi.fn()
  const mockResetErrorBoundary = vi.fn()

  it('should display internal error message', async () => {
    const error = new Error('Test internal error')
    render(
      <ErrorContainer
        errorMessage={error.message}
        tryAgain={mockResetErrorBoundary}
        navigate={mockNavigate}
      />,
    )

    await screen.findByText('Internal Error')
    await screen.findByText('Test internal error')
  })

  it('should display error message with title', async () => {
    const error = new Error('Test server error')
    render(
      <ErrorContainer
        title="Server Error"
        errorMessage={error.message}
        tryAgain={mockResetErrorBoundary}
        navigate={mockNavigate}
      />,
    )

    await screen.findByText('Server Error')
    await screen.findByText('Test server error')
  })

  it('should call resetErrorBoundary on "Try again" button click', async () => {
    const user = userEvent.setup()
    const error = new Error('Test server error')
    render(
      <ErrorContainer
        errorMessage={error.message}
        tryAgain={mockResetErrorBoundary}
        navigate={mockNavigate}
      />,
    )
    await user.click(screen.getByText('Try again'))

    expect(mockResetErrorBoundary).toHaveBeenCalledTimes(1)
  })

  it('should call navigate on "Go back" button click', async () => {
    const user = userEvent.setup()
    const error = new Error('Test internal error')
    render(
      <ErrorContainer
        errorMessage={error.message}
        tryAgain={mockResetErrorBoundary}
        navigate={mockNavigate}
      />,
    )
    await user.click(screen.getByText('Go back'))

    expect(mockNavigate).toHaveBeenCalledTimes(1)
  })
})
