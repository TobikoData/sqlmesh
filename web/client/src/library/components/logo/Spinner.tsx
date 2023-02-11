import clsx from 'clsx'
import React, { memo } from 'react'

interface PropsSpinner extends React.SVGAttributes<SVGAElement> {}

function Spinner({ style, className }: PropsSpinner): JSX.Element {
  return (
    <svg
      style={style}
      className={clsx('animate-spin', className)}
      viewBox="0 0 64 64"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M16 59.7128C31.3054 68.5494 50.8763 63.3054 59.7128 48C68.5494 32.6946 63.3054 13.1237 48 4.28719C32.6946 -4.54937 13.1237 0.694636 4.28719 16C-4.54937 31.3054 0.694637 50.8763 16 59.7128ZM23 47.5885C31.6093 52.559 42.6179 49.6093 47.5885 41C52.559 32.3907 49.6093 21.3821 41 16.4115C32.3907 11.441 21.3821 14.3907 16.4115 23C11.441 31.6093 14.3907 42.6179 23 47.5885Z"
        className="fill-gray-200"
      />
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M50.5827 26.5161C49.2259 21.9157 46.1691 17.8082 41.6875 15.2208C37.4263 12.7606 32.6191 12.103 28.1488 13.0114L25.1365 1.76921C32.4854 0.0988895 40.4586 1.08788 47.5 5.15321C54.7617 9.34574 59.6854 16.0326 61.8138 23.5067L50.5827 26.5161Z"
        className="fill-secondary-500"
      />
    </svg>
  )
}

export default memo(Spinner)
