import { useIDE } from '../ide/context'
import Page from '../root/Page'
import SourceList, { SourceListItem } from '@components/sourceList/SourceList'
import { EnumRoutes } from '~/routes'
import { Outlet, useNavigate } from 'react-router-dom'
import { EnumSize, EnumVariant } from '~/types/enum'
import { useEffect, useMemo } from 'react'
import { isArrayEmpty, isNil, isNotNil } from '@utils/index'
import { Button } from '@components/button/Button'

export default function PageErrors(): JSX.Element {
  const navigate = useNavigate()

  const { errors, removeError, clearErrors } = useIDE()

  const list = useMemo(() => Array.from(errors).reverse(), [errors])

  useEffect(() => {
    if (isArrayEmpty(list)) {
      setTimeout(() => navigate(EnumRoutes.Ide))
    } else {
      const id = list[0]?.id

      if (isNil(id)) {
        navigate(EnumRoutes.Errors, { replace: true })
      } else {
        navigate(EnumRoutes.Errors + '/' + id, { replace: true })
      }
    }
  }, [list])

  function handleDelete(id: string): void {
    const error = list.find(error => error.id === id)

    if (isNotNil(error)) {
      removeError(error)
    }
  }

  return (
    <Page
      sidebar={
        <>
          {errors.size > 0 && (
            <div className="flex justify-end m-1">
              <Button
                size={EnumSize.sm}
                variant={EnumVariant.Neutral}
                onClick={() => clearErrors()}
              >
                Clear All
              </Button>
            </div>
          )}
          <SourceList
            by="id"
            byName="key"
            byDescription="message"
            to={EnumRoutes.Errors}
            items={list}
            types={list.reduce(
              (acc: Record<string, string>, it) =>
                Object.assign(acc, { [it.id]: it.status }),
              {},
            )}
            listItem={({
              id,
              to,
              name,
              description,
              text,
              disabled = false,
            }) => (
              <SourceListItem
                to={to}
                name={name}
                text={text}
                description={description}
                variant={EnumVariant.Danger}
                disabled={disabled}
                handleDelete={() => handleDelete(id)}
              />
            )}
          />
        </>
      }
      content={<Outlet />}
    />
  )
}
