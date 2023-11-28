import { isFalse } from '@utils/index'
import clsx from 'clsx'

const PROGRESS_DEFAULT_START = 3 // Start from 3% to indicate the progress is loading

export default function Progress({
  progress = 0,
  delay = 0,
  duration = 0,
  startFromZero = true,
  className,
}: {
  startFromZero?: boolean
  progress: number
  delay?: number
  duration?: number
  className?: string
}): JSX.Element {
  if (isFalse(startFromZero)) {
    progress =
      progress < PROGRESS_DEFAULT_START ? PROGRESS_DEFAULT_START : progress
  }

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
