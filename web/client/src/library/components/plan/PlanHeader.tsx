import { EnvironmentName } from '~/context/context'

export default function Plan({
  environment,
}: {
  environment: EnvironmentName
}): JSX.Element {
  return (
    <div className="h-full w-full py-4 px-6">
      <h4 className="text-xl">
        <span className="font-bold">Target Environment is</span>
        <b className="ml-2 px-2 py-1 font-sm rounded-md bg-secondary-500 text-secondary-100">
          {environment}
        </b>
      </h4>
    </div>
  )
}
