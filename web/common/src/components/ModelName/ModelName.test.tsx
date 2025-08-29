import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { ModelName } from './ModelName'

describe('ModelName', () => {
  it('renders full model name with catalog, schema, and model', () => {
    render(<ModelName name="cat.sch.model" />)
    expect(screen.getByText('cat')).toBeInTheDocument()
    expect(screen.getByText('sch')).toBeInTheDocument()
    expect(screen.getByText('model')).toBeInTheDocument()
  })

  it('hides catalog when hideCatalog is true', () => {
    render(
      <ModelName
        name="cat.sch.model"
        hideCatalog
      />,
    )
    expect(screen.queryByText('cat')).not.toBeInTheDocument()
    expect(screen.getByText('sch')).toBeInTheDocument()
    expect(screen.getByText('model')).toBeInTheDocument()
  })

  it('hides schema when hideSchema is true', () => {
    render(
      <ModelName
        name="cat.sch.model"
        hideSchema
      />,
    )
    expect(screen.getByText('cat')).toBeInTheDocument()
    expect(screen.queryByText('sch')).not.toBeInTheDocument()
    expect(screen.getByText('model')).toBeInTheDocument()
  })

  it('hides icon when hideIcon is true', () => {
    const { container } = render(
      <ModelName
        name="cat.sch.model"
        hideIcon
      />,
    )
    // Should not render the Box icon SVG
    expect(container.querySelector('svg')).toBeNull()
  })

  it('shows tooltip when showTooltip is true and catalog or schema is hidden', async () => {
    render(
      <ModelName
        name="cat.sch.model"
        hideCatalog
        showTooltip
      />,
    )
    // Tooltip trigger is present (icon)
    const modelName = screen.getByTestId('model-name')
    expect(modelName).toBeInTheDocument()
    await userEvent.hover(modelName)
    const tooltip = await screen.findByRole('tooltip')
    expect(tooltip).toBeInTheDocument()
    within(tooltip).getByText('cat.sch.model')
  })

  it('throws error if name is empty', () => {
    // Suppress error output for this test
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {})
    expect(() => render(<ModelName name="" />)).toThrow()
    spy.mockRestore()
  })
})
