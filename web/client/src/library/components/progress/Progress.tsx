import clsx from 'clsx'

export interface PropsProgress extends React.HTMLAttributes<HTMLElement> {
  progress: number
  delay?: number
  duration?: number
}

export default function Progress({
  progress = 0,
  delay = 0,
  duration = 0,
  className,
}: PropsProgress): JSX.Element {
  return (
    <div
      className={clsx(
        'w-full h-1 bg-neutral-30 overflow-hidden flex items-center rounded-lg my-1',
        className,
      )}
    >
      <div
        className={`transition-[width] h-full bg-success-500 rounded-lg`}
        style={{
          width: `${progress}%`,
          transitionDelay: `${delay}ms`,
          transitionDuration: `${duration}ms`,
        }}
      ></div>
    </div>
  )
}
