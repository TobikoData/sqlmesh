import { useStoreContext } from '~/context/context'

export default function Plan({ error }: { error?: Error }): JSX.Element {
  const environment = useStoreContext(s => s.environment)

  return (
    <div className="h-full w-full py-4 px-6">
      <h4 className="text-xl">
        <span className="font-bold">Target Environment is</span>
        <b className="ml-2 px-2 py-1 font-sm rounded-md bg-secondary-500 text-secondary-100">
          {environment.name}
        </b>
      </h4>
      {environment.isInitial && environment.isDefault && (
        <div className="mt-4 mb-2 flex items-center w-full text-sm">
          <div className="p-4 w-full h-full border border-warning-500 bg-warning-100 rounded-lg">
            <h4 className="mb-2">Running Default Plan prod</h4>
            <p>
              Lorem ipsum dolor sit amet, consectetur adipisicing elit. Tempora
              expedita totam quis dignissimos veniam officia debitis atque
              nesciunt, voluptatem eos omnis quidem nihil error, nulla soluta?
              Saepe voluptates eaque ducimus!
            </p>
          </div>
        </div>
      )}
      {error != null && (
        <div className="mt-4 mb-2 flex items-center w-full text-sm">
          <div className="p-4 w-full h-full border border-danger-500 bg-danger-100 rounded-lg">
            <h4 className="mb-2">Erorr</h4>
            <p>{error.message}</p>
          </div>
        </div>
      )}
    </div>
  )
}
