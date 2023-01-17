import clsx from 'clsx';

export function Button({ className, children }: any) {
  return (
    <button className={clsx(className, 'whitespace-nowrap rounded flex justify-center items-center px-4 mx-1')}>
      <small>{children}</small>
    </button>
  );
}