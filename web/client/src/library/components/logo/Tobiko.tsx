import React, { memo } from 'react'

interface PropsLogoTobiko extends React.SVGAttributes<SVGAElement> {}

function LogoTobiko({ style, className }: PropsLogoTobiko): JSX.Element {
  return (
    <svg
      style={style}
      className={className}
      viewBox="0 0 236 72"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M204.385 40.0985C204.385 32.0027 210.325 26.1971 218.192 26.1971C226.06 26.1971 232 32.0027 232 40.0985C232 48.1411 226.06 54 218.192 54C210.325 54 204.385 48.1411 204.385 40.0985ZM218.408 47.5868C214.504 47.5868 210.9 45.0249 210.9 40.1144C210.9 35.1506 214.504 32.642 218.408 32.642C222.312 32.642 225.916 35.1506 225.916 40.1144C225.916 45.0782 222.312 47.5868 218.408 47.5868Z"
        className="fill-prose"
      />
      <path
        d="M201.591 28.6792C202.197 28.0448 201.746 26.996 200.866 26.996H194.325C194.046 26.996 193.78 27.1113 193.591 27.3142L184.79 36.743V15.6355C184.79 15.0852 184.342 14.6391 183.789 14.6391H178.673C178.12 14.6391 177.672 15.0852 177.672 15.6355V52.2047C177.672 52.755 178.12 53.2011 178.673 53.2011H183.789C184.342 53.2011 184.79 52.755 184.79 52.2047V46.277L187.734 43.1877L194.392 52.7714C194.579 53.0405 194.887 53.2011 195.216 53.2011H201.483C202.295 53.2011 202.769 52.2905 202.301 51.6305L192.657 38.0213L201.591 28.6792Z"
        className="fill-prose"
      />
      <path
        d="M170.052 52.2047C170.052 52.755 169.603 53.2011 169.051 53.2011H163.935C163.382 53.2011 162.934 52.755 162.934 52.2047V27.9923C162.934 27.4421 163.382 26.996 163.935 26.996H169.051C169.603 26.996 170.052 27.4421 170.052 27.9923V52.2047Z"
        className="fill-prose"
      />
      <path
        d="M166.466 22.735C164.058 22.735 162.078 20.7643 162.078 18.4208C162.078 15.9707 164.058 14 166.466 14C168.928 14 170.854 15.9707 170.854 18.4208C170.854 20.7643 168.928 22.735 166.466 22.735Z"
        className="fill-prose"
      />
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M136.067 53.2011C136.62 53.2011 137.068 52.755 137.068 52.2047V50.7169C137.175 51.7132 141.243 53.8402 144.882 53.8402C152.374 53.8402 158.342 47.9281 158.342 39.992C158.342 32.2157 152.856 26.3036 145.149 26.3036C141.243 26.3036 137.82 28.2997 137.175 28.7979V15.6355C137.175 15.0852 136.727 14.6391 136.174 14.6391H131.166C130.613 14.6391 130.164 15.0852 130.164 15.6355V52.2047C130.164 52.755 130.613 53.2011 131.166 53.2011H136.067ZM137.319 40.1144C137.319 45.0249 140.923 47.5868 144.827 47.5868C148.732 47.5868 152.336 45.0782 152.336 40.1144C152.336 35.1506 148.732 32.642 144.827 32.642C140.923 32.642 137.319 35.1506 137.319 40.1144Z"
        className="fill-prose"
      />
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M96.9306 40.0985C96.9306 32.0027 102.871 26.1971 110.738 26.1971C118.605 26.1971 124.546 32.0027 124.546 40.0985C124.546 48.1411 118.605 54 110.738 54C102.871 54 96.9306 48.1411 96.9306 40.0985ZM110.79 47.5868C106.885 47.5868 103.282 45.0249 103.282 40.1144C103.282 35.1506 106.885 32.642 110.79 32.642C114.694 32.642 118.298 35.1506 118.298 40.1144C118.298 45.0782 114.694 47.5868 110.79 47.5868Z"
        className="fill-prose"
      />
      <path
        d="M86.8264 19.1664C87.3793 19.1664 87.8275 19.6125 87.8275 20.1628V26.996H92.1247C92.6776 26.996 93.1258 27.4421 93.1258 27.9923V32.2846C93.1258 32.8349 92.6776 33.281 92.1247 33.281H87.8275V44.253C87.8275 46.5433 88.8979 47.2889 90.9316 47.2889C91.2897 47.2889 91.7321 47.2891 92.1257 47.2893C92.6783 47.2896 93.1258 47.7356 93.1258 48.2855V52.5789C93.1258 53.1298 92.6774 53.5761 92.124 53.5755C91.3667 53.5748 90.2638 53.5739 89.112 53.5739C83.9742 53.5739 80.7632 50.5379 80.7632 45.478V33.281H77.0011C76.4483 33.281 76 32.8349 76 32.2846V27.9923C76 27.4421 76.4483 26.996 77.0011 26.996H80.7632V20.1628C80.7632 19.6125 81.2114 19.1664 81.7643 19.1664H86.8264Z"
        className="fill-prose"
      />
      <path
        d="M42.0001 24.028C41.9926 25.6738 41.3224 27.163 40.2428 28.2427C39.157 29.3284 37.657 30 36.0001 30C34.3432 30 32.8432 29.3284 31.7574 28.2427C30.6717 27.1569 30.0001 25.6569 30.0001 24C30.0001 22.3431 30.6717 20.8431 31.7574 19.7573C32.8432 18.6716 34.3432 18 36.0001 18C37.657 18 39.157 18.6716 40.2428 19.7573C41.3224 20.837 41.9926 22.3262 42.0001 23.972C42.0076 22.3262 42.6777 20.837 43.7573 19.7573C44.8431 18.6716 46.3431 18 48 18C49.6569 18 51.1569 18.6716 52.2426 19.7573C53.3284 20.8431 54 22.3431 54 24C54 25.6569 53.3284 27.1569 52.2426 28.2427C51.1569 29.3284 49.6569 30 48 30C46.3431 30 44.8431 29.3284 43.7573 28.2427C42.6777 27.163 42.0076 25.6738 42.0001 24.028Z"
        fill="var(--color-brand-500)"
      />
      <path
        d="M30.0001 24C30.0001 25.6569 29.3284 27.157 28.2427 28.2428C27.1569 29.3285 25.6569 30.0001 24 30.0001C22.3431 30.0001 20.8431 29.3285 19.7573 28.2428C18.6716 27.157 18 25.657 18 24.0001C18 22.3432 18.6716 20.8432 19.7573 19.7574C20.8431 18.6717 22.3431 18.0001 24 18.0001C25.6569 18.0001 27.1569 18.6717 28.2427 19.7574C29.3284 20.8432 30.0001 22.3431 30.0001 24Z"
        fill="var(--color-brand-500)"
      />
      <path
        d="M30 36.0001C30 37.657 29.3284 39.157 28.2427 40.2428C27.163 41.3224 25.6739 41.9925 24.0281 42C25.6739 42.0076 27.163 42.6777 28.2427 43.7573C29.3284 44.8431 30 46.3431 30 48C30 49.6569 29.3284 51.1569 28.2427 52.2426C27.1569 53.3284 25.6569 54 24 54C22.3432 54 20.8432 53.3284 19.7575 52.2426C18.6717 51.1569 18 49.6569 18 48C18 46.3431 18.6717 44.8431 19.7575 43.7573C20.837 42.6777 22.3262 42.0076 23.972 42C22.3262 41.9925 20.837 41.3224 19.7575 40.2428C18.6717 39.157 18 37.657 18 36.0001C18 34.3432 18.6717 32.8432 19.7575 31.7574C20.8432 30.6717 22.3431 30.0001 24 30.0001C25.6569 30.0001 27.1569 30.6717 28.2427 31.7574C29.3284 32.8432 30 34.3432 30 36.0001Z"
        fill="var(--color-brand-500)"
      />
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M42.0001 48.028C41.9926 49.6738 41.3224 51.163 40.2428 52.2427C39.157 53.3284 37.657 54 36.0001 54C34.3432 54 32.8432 53.3284 31.7575 52.2427C30.6717 51.1569 30 49.6569 30 48C30 46.3431 30.6717 44.8431 31.7575 43.7573C32.8371 42.6777 34.3263 42.0076 35.9721 42C34.3263 41.9925 32.8371 41.3224 31.7575 40.2428C30.6717 39.157 30 37.657 30 36.0001C30 34.3432 30.6717 32.8432 31.7575 31.7575C32.8432 30.6717 34.3432 30 36.0001 30C37.657 30 39.157 30.6717 40.2428 31.7575C41.3224 32.8371 41.9926 34.3263 42.0001 35.9721C42.0076 34.3263 42.6777 32.8371 43.7573 31.7575C44.8431 30.6717 46.3431 30 48 30C49.6569 30 51.1569 30.6717 52.2427 31.7575C53.3284 32.8432 54 34.3432 54 36.0001C54 37.657 53.3284 39.157 52.2427 40.2428C51.163 41.3224 49.6738 41.9925 48.028 42C49.6738 42.0076 51.163 42.6777 52.2427 43.7573C53.3284 44.8431 54 46.3431 54 48C54 49.6569 53.3284 51.1569 52.2427 52.2427C51.1569 53.3284 49.6569 54 48 54C46.3431 54 44.8431 53.3284 43.7573 52.2427C42.6777 51.163 42.0076 49.6738 42.0001 48.028ZM40.2428 40.2428C41.3224 39.1631 41.9926 37.6739 42.0001 36.0281C42.0076 37.6739 42.6777 39.1631 43.7573 40.2428C44.837 41.3224 46.3262 41.9925 47.972 42C46.3262 42.0076 44.837 42.6777 43.7573 43.7573C42.6777 44.837 42.0076 46.3262 42.0001 47.972C41.9926 46.3262 41.3224 44.837 40.2428 43.7573C39.1631 42.6777 37.6739 42.0076 36.0283 42C37.6739 41.9925 39.1631 41.3224 40.2428 40.2428Z"
        fill="var(--color-brand-500)"
      />
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M4 24.3356C4 13.1046 13.1046 4 24.3356 4H47.6644C58.8954 4 68 13.1046 68 24.3356V47.6644C68 58.8954 58.8954 68 47.6644 68H24.3356C13.1046 68 4 58.8954 4 47.6644V24.3356ZM24.3356 11.3443C17.1607 11.3443 11.3443 17.1607 11.3443 24.3356V47.6644C11.3443 54.8393 17.1607 60.6557 24.3356 60.6557H47.6644C54.8393 60.6557 60.6557 54.8393 60.6557 47.6644V24.3356C60.6557 17.1607 54.8393 11.3443 47.6644 11.3443H3 56Z"
        className="fill-prose"
      />
    </svg>
  )
}

export default memo(LogoTobiko)
