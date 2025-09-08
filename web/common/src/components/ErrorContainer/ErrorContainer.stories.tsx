import type { Meta, StoryObj } from '@storybook/react-vite'
import { ErrorContainer } from './ErrorContainer'
import { expect, within, userEvent, fn } from 'storybook/test'

const meta: Meta<typeof ErrorContainer> = {
  title: 'Components/Containers/ErrorContainer',
  component: ErrorContainer,
  parameters: {
    layout: 'padded',
  },
  decorators: [
    Story => (
      <div style={{ height: '400px', width: '100%' }}>
        <Story />
      </div>
    ),
  ],
}

export default meta

type Story = StoryObj<typeof ErrorContainer>

export const Default: Story = {
  args: {
    errorCode: '500',
    title: 'Something went wrong',
    errorMessage: 'Unable to load the requested resource',
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByRole('alert')).toBeInTheDocument()
    await expect(canvas.getByText('Something went wrong')).toBeInTheDocument()
    await expect(
      canvas.getByText('Unable to load the requested resource'),
    ).toBeInTheDocument()
  },
}

export const WithBothButtons: Story = {
  args: {
    title: 'Server Error',
    errorMessage: 'An unexpected error occurred',
    errorDetail: 'Status Code: 500\nInternal Server Error',
    tryAgain: fn(),
    navigate: fn(),
  },
  play: async ({ canvasElement, args }) => {
    const canvas = within(canvasElement)
    const user = userEvent.setup()

    const tryAgainButton = canvas.getByRole('button', { name: 'Try again' })
    const goBackButton = canvas.getByRole('button', { name: 'Go back' })

    await expect(tryAgainButton).toBeInTheDocument()
    await expect(goBackButton).toBeInTheDocument()

    await user.click(tryAgainButton)
    await expect(args.tryAgain).toHaveBeenCalledTimes(1)

    await user.click(goBackButton)
    await expect(args.navigate).toHaveBeenCalledTimes(1)
  },
}

export const MinimalError: Story = {
  args: {
    errorMessage: 'An error occurred',
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByText('An error occurred')).toBeInTheDocument()
    const buttons = canvas.queryAllByRole('button')
    await expect(buttons).toHaveLength(0)
  },
}

export const LongErrorDetail: Story = {
  args: {
    title: 'Compilation Error',
    errorMessage: 'Failed to compile TypeScript',
    errorDetail: `ERROR in ./src/components/MyComponent.tsx:45:12
TS2322: Type 'string' is not assignable to type 'number'.
    43 |   const calculateTotal = (items: Item[]) => {
    44 |     return items.reduce((sum, item) => {
  > 45 |       return sum + item.price; // Error: price is string
       |              ^^^^^^^^^^^^^^^^^
    46 |     }, 0);
    47 |   };
    48 |

ERROR in ./src/utils/validation.ts:12:5
TS2531: Object is possibly 'null'.
    10 | export function validateUser(user: User | null) {
    11 |   // Missing null check
  > 12 |   if (user.email.includes('@')) {
       |       ^^^^^^^^^^
    13 |     return true;
    14 |   }
    15 |   return false;

ERROR in ./src/services/api.ts:88:15
TS2345: Argument of type 'undefined' is not assignable to parameter of type 'string'.
    86 |   async fetchData(endpoint?: string) {
    87 |     // endpoint might be undefined
  > 88 |     const url = buildUrl(endpoint);
       |                          ^^^^^^^^
    89 |     return fetch(url);
    90 |   }

webpack compiled with 3 errors`,
    tryAgain: fn(),
  },
}
