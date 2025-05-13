import Container from '@components/container/Container'
import { Link } from 'react-router'
import { ButtonLink } from '@components/button/Button'
import { EnumVariant } from '~/types/enum'
import { isNotNil } from '@utils/index'

export default function NotFound({
  description,
  link,
  headline = 'Not Found',
  message = 'Go Back Home',
}: {
  message?: string
  link?: string
  description?: string
  headline?: string
}): JSX.Element {
  return (
    <Container.Page>
      <div className="flex items-center justify-center w-full h-full">
        <div className="text-center">
          <h1 className="text-[4rem] md:text-[6rem] lg:text-[9rem] text-secondary-10 dark:text-primary-10 mb-4">
            {headline}
          </h1>
          {isNotNil(description) && (
            <p className="mb-10 py-5 px-4 text-neutral-70 dark:text-primary-70 w-full bg-primary-10 rounded-md">
              {description}
            </p>
          )}
          {isNotNil(link) && (
            <div className="inline-block">
              <ButtonLink variant={EnumVariant.Alternative}>
                <Link to={link}>{message}</Link>
              </ButtonLink>
            </div>
          )}
        </div>
      </div>
    </Container.Page>
  )
}
