import { vi } from 'vitest';
import { render, fireEvent} from '../../../../tests/utils';
import { EnumSize } from '../../../../types/enum';
import { Button, ButtonMenu } from '../Button';
import { Menu } from '@headlessui/react';

describe('Button', () => {
  test('renders with default variant, shape, and size', () => {
    const { getByText } = render(<Button>Click me</Button>);
    const button = getByText('Click me');

    expect(button).toHaveClass('bg-secondary-500');
    expect(button).toHaveClass('rounded-md');
    expect(button).toHaveClass('px-3 py-2 text-base');
  });

  test('renders with custom variant, shape, and size', () => {
    const { getByText } = render(<Button variant="primary" shape="square" size={EnumSize.sm}>Click me</Button>);
    const button = getByText('Click me');

    expect(button).toHaveClass('bg-primary-500');
    expect(button).not.toHaveClass('rounded-md');
    expect(button).toHaveClass('px-2 py-1 text-xs');
  });

  test('calls onClick when clicked', () => {
    const onClick = vi.fn();
    const { getByText } = render(<Button onClick={onClick}>Click me</Button>);
    const button = getByText('Click me');

    fireEvent.click(button);
    expect(onClick).toHaveBeenCalled();
  });
});

describe('ButtonMenu', () => {
  test('renders with default variant, shape, and size', () => {
    const { getByText } = render(<Menu><ButtonMenu>Click me</ButtonMenu></Menu>);
    const button = getByText('Click me');

    expect(button).toHaveClass('bg-secondary-500');
    expect(button).toHaveClass('rounded-md');
    expect(button).toHaveClass('px-3 py-2 text-base');
  });

  test('renders with custom variant, shape, and size', () => {
    const { getByText } = render(<Menu><ButtonMenu variant="primary" shape="square" size={EnumSize.sm}>Click me</ButtonMenu></Menu>);
    const button = getByText('Click me');

    expect(button).toHaveClass('bg-primary-500');
    expect(button).not.toHaveClass('rounded-md');
    expect(button).toHaveClass('px-2 py-1 text-xs');
  });
});
