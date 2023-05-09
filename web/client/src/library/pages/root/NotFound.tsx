import Container from '@components/container/Container'
import { Link } from 'react-router-dom'
import { ButtonLink } from '@components/button/Button'
import { EnumVariant } from '~/types/enum'

export default function NotFound({
  message,
  descritpion,
  link,
}: {
  message: string
  descritpion?: string
  link: string
}): JSX.Element {
  return (
    <Container.Page>
      <div className="flex items-center justify-center w-full h-full">
        <div className="text-center">
          <h1 className="text-[4rem] md:text-[6rem] lg:text-[9rem] text-secondary-10 dark:text-primary-10 mb-4">
            Not Found
          </h1>
          {descritpion != null && (
            <p className="mb-10 text-neutral-70 dark:text-primary-70 w-full bg-primary-10 py-5 rounded-md">
              {descritpion}
            </p>
          )}
          <div className="inline-block">
            <ButtonLink variant={EnumVariant.Alternative}>
              <Link to={link}>{message}</Link>
            </ButtonLink>
          </div>
        </div>
      </div>
    </Container.Page>
  )
}
