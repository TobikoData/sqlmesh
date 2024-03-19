import { test } from '@playwright/test'
import {
  checkFile,
  selectEnvironment,
  addEnvironment,
  goToPlan,
  applyPlan,
  goBackTo,
  checkModelChange,
  changeFileContent,
} from './utils'
import {
  testHeader,
  testFooter,
  testModulesAPI,
  testMetaAPI,
  testEventsAPI,
  testFilesAPI,
  testEnvironmentsAPI,
  testModelsAPI,
  testPlanAPI,
  testHistoryNavigation,
  testEnvironmentDetails,
  testErrors,
  testModuleNavigation,
  testChangesAndBackfills,
  testEditor,
  testEditorTabs,
  testFileExplorer,
} from './help'

test('run wursthall demo', async ({ page }) => {
  test.setTimeout(600000)
  console.log('START')
  const apiModulesPromise = page.waitForResponse('/api/modules')
  const apiMetaPromise = page.waitForResponse('/api/meta')
  const apiEventsPromise = page.waitForResponse('/api/events')
  const apiFilesPromise = page.waitForResponse('/api/files')
  const apiEnvironmentsPromise = page.waitForResponse('/api/environments')
  const apiModelsPromise = page.waitForResponse('/api/models')
  const apiPlanPromise = page.waitForResponse('/api/plan')
  await page.goto('/')
  await testModulesAPI(
    page,
    { modules: ['docs', 'editor', 'errors', 'plans', 'files'] },
    apiModulesPromise,
  )
  await page.waitForURL('/editor')
  await testEventsAPI(page, apiEventsPromise)
  await testMetaAPI(page, apiMetaPromise)
  await testHeader(page)
  await testFooter(page)
  await testModelsAPI(page, apiModelsPromise)
  await testEnvironmentsAPI(page, apiEnvironmentsPromise)
  await testFilesAPI(page, { projectName: 'wursthall' }, apiFilesPromise)
  await testPlanAPI(page, apiPlanPromise)
  await testHistoryNavigation(page, { hasBack: true, hasForward: true })
  await testEnvironmentDetails(page, {
    env: 'prod',
    isDisabledAction: false,
    isDisabledEnv: true,
  })
  await testChangesAndBackfills(page, { added: 7, backfills: 7 })
  await testErrors(page, { label: 'No Errors' })
  await testModuleNavigation(page)
  await testEditor(page)
  await testEditorTabs(page)
  await testFileExplorer(page)
  await checkFile(page, { path: 'config.yaml' })
  console.log('1. Initial prod backfill')
  await goToPlan(page, { env: 'prod', action: 'Apply Changes And Backfill' })
  await applyPlan(page, { env: 'prod', action: 'Apply Changes And Backfill' })
  await testErrors(page, { label: 'No Errors' })
  await testEnvironmentDetails(page, { env: 'prod' })
  await testChangesAndBackfills(page, { backfills: 0 })
  await goBackTo(page, { path: '/editor' })
  console.log('2. Add dev')
  await addEnvironment(page, { env: 'dev' })
  console.log('3. Apply changes to dev')
  await testEnvironmentDetails(page, { env: 'dev' })
  await testChangesAndBackfills(page, { backfills: 0 })
  await goToPlan(page, { env: 'dev', action: 'Apply Virtual Update' })
  await applyPlan(page, { env: 'dev', action: 'Apply Virtual Update' })
  await goBackTo(page, { path: '/editor' })
  await testErrors(page, { label: 'No Errors' })
  await testEnvironmentDetails(page, { env: 'dev' })
  await testChangesAndBackfills(page, { backfills: 0 })
  console.log('4. Change model')
  await changeFileContent(page, {
    path: 'models/db/item_d.sql',
    modelName: 'db.item_d',
    content: `
      MODEL (
          name db.item_d,
          kind VIEW,
          cron '@daily',
          owner jen,
          start '2022-06-01 00:00:00+00:00'
        );
    
        SELECT
          id AS item_id,
          item_name AS item_name,
          item_group AS item_group,
          item_price AS item_price,
          ROUND(item_price) AS rounded
        FROM src.menu_item_details
    `,
  })
  console.log('5. Apply changes to dev')
  await testEnvironmentDetails(page, { env: 'dev' })
  await testChangesAndBackfills(page, { direct: 1, indirect: 2, backfills: 1 })
  await goToPlan(page, { env: 'dev', action: 'Apply Changes And Backfill' })
  await checkModelChange(page, {
    group: 'Modified Directly',
    model: 'db__dev.item_d',
    change: 'Non-Breaking Change',
  })
  await applyPlan(page, { env: 'dev', action: 'Apply Changes And Backfill' })
  await goBackTo(page, { path: '/editor' })
  await testErrors(page, { label: 'No Errors' })
  await testEnvironmentDetails(page, { env: 'dev' })
  await testChangesAndBackfills(page, { backfills: 0 })
  console.log('6. Select prod')
  await selectEnvironment(page, { env: 'prod' })
  console.log('7. Apply virtual update to prod')
  await testEnvironmentDetails(page, { env: 'prod' })
  await testChangesAndBackfills(page, { direct: 1, indirect: 2, backfills: 0 })
  await goToPlan(page, { env: 'prod', action: 'Apply Virtual Update' })
  await applyPlan(page, {
    env: 'prod',
    action: 'Apply Virtual Update',
    expectConfirmation: true,
  })
  await goBackTo(page, { path: '/editor' })
  await testErrors(page, { label: 'No Errors' })
  await testEnvironmentDetails(page, { env: 'prod' })
  await testChangesAndBackfills(page, { backfills: 0 })
  console.log('8. Select dev')
  await selectEnvironment(page, { env: 'dev' })
  await testEnvironmentDetails(page, { env: 'dev' })
  await testChangesAndBackfills(page, { backfills: 0 })
  console.log('9. Change model')
  await changeFileContent(page, {
    isSelected: true,
    path: 'models/db/item_d.sql',
    modelName: 'db.item_d',
    content: `
      MODEL (
          name db.item_d,
          kind VIEW,
          cron '@daily',
          owner jen,
          start '2022-06-01 00:00:00+00:00'
        );
  
        SELECT
          id AS item_id,
          item_name AS item_name,
          item_group AS item_group,
          item_price AS item_price,
          ROUND(item_price) AS rounded
        FROM src.menu_item_details
        WHERE item_price > 5.0
    `,
  })
  console.log('10. Apply changes to dev')
  await testEnvironmentDetails(page, { env: 'dev' })
  await testChangesAndBackfills(page, { direct: 1, indirect: 2, backfills: 3 })
  await goToPlan(page, { env: 'dev', action: 'Apply Changes And Backfill' })
  await checkModelChange(page, {
    group: 'Modified Directly',
    model: 'db__dev.item_d',
    change: 'Breaking Change',
  })
  await applyPlan(page, { env: 'dev', action: 'Apply Changes And Backfill' })
  await goBackTo(page, { path: '/editor' })
  await testErrors(page, { label: 'No Errors' })
  await testEnvironmentDetails(page, { env: 'dev' })
  await testChangesAndBackfills(page, { backfills: 0 })
  console.log('END')
})
