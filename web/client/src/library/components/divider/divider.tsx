import React from 'react';
import clsx from 'clsx';
import { useContext } from 'react';
import SplitPaneContext from '../../../context/SplitPaneContext';

interface PropsDivider {
  size?: 'xs' | 'md' | 'lg';
  isInteractive?: boolean;
  orientation?: 'horizontal' | 'vertical';
  className?: string;
}

const SIZE = new Map([
  ['xs', ''],
  ['md', '-2'],
  ['lg', '-4'],
]);

export function Divider({ size = 'xs', isInteractive = false, orientation = 'horizontal', className }: PropsDivider) {
  const clsOrientation = orientation === 'horizontal' ? `w-full border-b${SIZE.get(size)}` : `h-full border-r${SIZE.get(size)}`;
  const { onMouseDown } = useContext(SplitPaneContext);

  return (
    <span className={clsx(className, ['block', clsOrientation, 'border-gray-100'])} onMouseDown={isInteractive ? onMouseDown : undefined}></span>
  );
}