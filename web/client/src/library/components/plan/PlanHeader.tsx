import clsx from 'clsx'
import { useStoreContext } from '~/context/context'
import { EnumVariant, type Variant } from '~/types/enum'

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
        <Banner
          variant={EnumVariant.Warning}
          headline="Running Default Plan"
          description="Lorem ipsum dolor sit amet, consectetur adipisicing elit. Tempora
          expedita totam quis dignissimos veniam officia debitis atque
          nesciunt, voluptatem eos omnis quidem nihil error, nulla soluta?
          Saepe voluptates eaque ducimus!"
        />
      )}
      {error != null && (
        <Banner
          variant={EnumVariant.Danger}
          headline="Error"
          description={error.message}
        />
      )}
    </div>
  )
}

function Banner({
  variant,
  headline,
  description,
}: {
  variant: Variant
  description?: string
  headline?: string
}): JSX.Element {
  return (
    <div className="mt-4 mb-2 flex items-center w-full text-sm">
      <div
        className={clsx(
          'p-4 w-full h-full border-2 rounded-lg',
          variant === EnumVariant.Primary &&
            'bg-primary-10 border-primary-400 text-primary-600',
          variant === EnumVariant.Secondary &&
            'bg-secondary-10 border-secondary-400 text-secondary-600',
          variant === EnumVariant.Success &&
            'bg-success-10 border-success-400 text-success-600',
          variant === EnumVariant.Warning &&
            'bg-warning-10 border-warning-400 text-warning-600',
          variant === EnumVariant.Danger &&
            'bg-danger-10 border-danger-400 text-danger-600',
          variant === EnumVariant.Info &&
            'bg-neutral-10 border-neutral-400 text-neutral-400',
        )}
      >
        {headline != null && (
          <h4 className="mb-2 font-bold text-lg">{headline}</h4>
        )}
        {description != null && <p className="text-prose">{description}</p>}
      </div>
    </div>
  )
}
