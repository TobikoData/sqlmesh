import { useStorePlan } from '@context/plan'
import {
  isArrayNotEmpty,
  isFalse,
  isObjectNotEmpty,
  isTrue,
} from '@utils/index'
import { EnumPlanChangeType } from '@components/plan/context'
import React, { useState, useEffect, type MouseEvent } from 'react'
import { ModelPlanOverviewTracker } from '@models/tracker-plan-overview'
import EnvironmentChangesPreview from './EnvironmentChangesPreview'
import { useApiEnvironments, useApiPlanRun } from '@api/index'
import { Button } from '@components/button/Button'
import { useStoreContext } from '@context/context'
import { EnumSize, EnumVariant } from '~/types/enum'
import { ArrowPathIcon } from '@heroicons/react/20/solid'
import clsx from 'clsx'
import { type Environments, type EnvironmentsEnvironments } from '@api/client'

export default function EnvironmentChanges(): JSX.Element {
  const planOverview = useStorePlan(s => s.planOverview)

  const [planOverviewTracker, setPlanOverviewTracker] =
    useState<ModelPlanOverviewTracker>(planOverview)

  useEffect(() => {
    if (isFalse(planOverview.isEmpty) && planOverview.isFinished) {
      setPlanOverviewTracker(new ModelPlanOverviewTracker(planOverview))
    }
  }, [planOverview])

  const shouldShow =
    (isFalse(planOverview.isLatest) || planOverview.isFetching) &&
    planOverviewTracker.hasUpdates

  return shouldShow ? (
    <span className="flex group items-center bg-neutral-5 dark:bg-neutral-20 px-1 py-1 rounded-full">
      {(isTrue(planOverviewTracker.hasChanges) ||
        isTrue(planOverviewTracker.hasBackfills)) && (
        <ChangesToken className="mr-2">Changes</ChangesToken>
      )}
      {isArrayNotEmpty(planOverviewTracker.added) && (
        <EnvironmentChangesPreview
          headline="Added"
          type={EnumPlanChangeType.Add}
          changes={planOverviewTracker.added}
          className="-mr-2 group-hover:mr-0 z-[5]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.direct) && (
        <EnvironmentChangesPreview
          headline="Directly Modified"
          type={EnumPlanChangeType.Direct}
          changes={planOverviewTracker.direct}
          className="-mr-2 group-hover:mr-0 z-[4]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.indirect) && (
        <EnvironmentChangesPreview
          headline="Indirectly Modified"
          type={EnumPlanChangeType.Indirect}
          changes={planOverviewTracker.indirect}
          className="-mr-2 group-hover:mr-0 z-[3]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.metadata) && (
        <EnvironmentChangesPreview
          headline="Metadata"
          type={EnumPlanChangeType.Metadata}
          changes={planOverviewTracker.metadata}
          className="-mr-2 group-hover:mr-0 z-[2]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.removed) && (
        <EnvironmentChangesPreview
          headline="Removed"
          type={EnumPlanChangeType.Remove}
          changes={planOverviewTracker.removed}
          className="-mr-2 group-hover:mr-0 z-[1]"
        />
      )}
      {isArrayNotEmpty(planOverviewTracker.backfills) ? (
        <EnvironmentChangesPreview
          headline="Backfills"
          type={EnumPlanChangeType.Default}
          changes={planOverviewTracker.backfills}
          className="ml-4 group-hover:ml-2 z-[6]"
        />
      ) : (
        <div className="ml-2 group-hover:ml-2 z-[6]"></div>
      )}
    </span>
  ) : (
    <ChangesToken className="pr-2 pl-0.5 py-1 rounded-full bg-neutral-5 dark:bg-neutral-20 text-neutral-500 dark:text-neutral-300">
      No Changes
    </ChangesToken>
  )
}

function ChangesToken({
  className,
  children,
}: {
  className?: string
  children: React.ReactNode
}): JSX.Element {
  const environment = useStoreContext(s => s.environment)
  const addRemoteEnvironments = useStoreContext(s => s.addRemoteEnvironments)

  const planAction = useStorePlan(s => s.planAction)

  const { refetch: getEnvironments } = useApiEnvironments()
  const { refetch: planRun } = useApiPlanRun(environment.name, {
    planOptions: { skip_tests: true, include_unmodified: true },
  })

  function updateEnviroments(data: Optional<Environments>): void {
    const { environments, default_target_environment, pinned_environments } =
      data ?? {}

    if (isObjectNotEmpty<EnvironmentsEnvironments>(environments)) {
      addRemoteEnvironments(
        Object.values(environments),
        default_target_environment,
        pinned_environments,
      )
    }
  }

  return (
    <p
      className={clsx(
        'flex items-center text-xs text-neutral-600 dark:text-neutral-300 font-bold text-center whitespace-nowrap',
        className,
      )}
    >
      <Button
        className="w-5 h-5 !p-0 !m-0 !ml-0.5 !mr-1 border-none !rounded-full bg-neutral-10 dark:bg-neutral-20"
        variant={EnumVariant.Info}
        size={EnumSize.sm}
        disabled={planAction.isProcessing}
        onClick={(e: MouseEvent) => {
          e.stopPropagation()

          void getEnvironments().then(({ data }) => {
            void planRun()
            updateEnviroments(data)
          })
        }}
      >
        <ArrowPathIcon className="text-neutral-500 dark:text-neutral-300 w-4 h-4" />
      </Button>
      {children}
    </p>
  )
}
