export function Progress({ progress = 0, delay = 150, duration = 500 }: any) {
  return (
    <div className="w-full h-1 bg-gray-100 overflow-hidden flex items-center rounded-lg my-1">
      <div
        className={`transition-[width] h-full bg-success-500 rounded-lg`}
        style={{
          width: `${progress}%`,
          transitionDelay: `${delay}ms`,
          transitionDuration: `${duration}ms`
        }}></div>
    </div>
  )
}