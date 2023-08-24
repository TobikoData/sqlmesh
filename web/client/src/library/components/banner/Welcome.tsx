import { isNotNil } from '@utils/index'

export default function Welcome({
  headline,
  tagline,
}: {
  headline?: string
  tagline?: string
}): JSX.Element {
  return (
    <div className="w-full h-full flex justify-center items-center">
      <div className="p-4">
        {isNotNil(headline) && (
          <h1 className="text-2xl font-bold">{headline}</h1>
        )}
        {isNotNil(tagline) && <p className="text-lg mt-4">{tagline}</p>}
      </div>
    </div>
  )
}
