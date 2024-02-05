import { expect, type Page } from '@playwright/test'

const NOT_EXIST = 'NOT_EXIST'
const EMPTY_STRING = ''
const PATH_SEPARATOR = process.env.UI_TEST_BROWSER === 'webkit' ? '/' : '\\'

export async function setFileContentAndSave(
  page: Page,
  opt: {
    path: string
    content: string
  },
): Promise<void> {
  const apiFilePromise = page.waitForResponse(
    r =>
      r.url().includes(`/api/files/${opt.path}`) &&
      r.request().method() === 'POST',
  )
  const [path, fileName] = getFileNameAndParentFolder(opt.path)
  expect(path).toBeTruthy()
  expect(fileName).toBeTruthy()
  console.log(`Set new content for ${fileName} and save`)
  const elEditor = page.getByTestId('editor')
  await expect(elEditor).toBeVisible()
  const elCodeEditor = elEditor.getByTestId('code-editor')
  await expect(elCodeEditor).toBeVisible()
  const elEditorFooter = elEditor.getByTestId('editor-footer')
  await expect(elEditorFooter).toBeVisible()
  const elEditorTabs = elEditor.getByTestId('editor-tabs')
  await expect(elEditorTabs).toBeVisible()
  const elTab = elEditorTabs.getByTitle(path)
  await expect(elTab).toBeVisible()
  const elEditorTextbox = elCodeEditor.getByRole('textbox')
  await elEditorTextbox.fill(opt.content)
  await elEditorTextbox.press('Control+S')
  const apiFile = await apiFilePromise
  expect(apiFile.status()).toBe(204)
}

export async function checkModelChange(
  page: Page,
  opt: {
    group: string
    model: string
    change: string
  },
): Promise<void> {
  console.log(`Checking model ${opt.model} change to be ${opt.change}`)
  const elPlanStageTracker = page.getByTestId('plan-stage-tracker')
  await expect(elPlanStageTracker).toBeVisible()
  const elGroup = elPlanStageTracker.getByTitle(opt.group)
  await expect(elGroup).toBeVisible()
  const elModel = elGroup.getByText(opt.model)
  await expect(elModel).toBeVisible()
  const elChange = elGroup.getByText(opt.change)
  await expect(elChange).toBeVisible()
}

export async function applyPlan(
  page: Page,
  opt: {
    env: string
    action: string
    expectConfirmation?: boolean
  },
): Promise<void> {
  console.log(`Apply plan to ${opt.env}`)
  const elPlan = page.getByTestId('plan')
  await expect(elPlan).toBeVisible()
  await expect(elPlan.getByRole('button', { name: opt.action })).toBeVisible()
  await expect(elPlan.getByRole('button', { name: 'Start Over' })).toBeVisible()
  await expect(elPlan.getByRole('button', { name: 'Cancel' })).toBeHidden()
  await elPlan.getByRole('button', { name: opt.action }).click()
  if (opt.expectConfirmation === true) {
    const elModelConfirmation = page.getByTestId('modal-confirmation')
    await expect(elModelConfirmation).toBeVisible()
    await expect(
      elModelConfirmation.getByRole('button', {
        name: `Yes, Run ${opt.env}`,
      }),
    ).toBeVisible()
    await elModelConfirmation
      .getByRole('button', { name: `Yes, Run ${opt.env}` })
      .click()
  }
  await expect(elPlan.getByRole('button', { name: 'Cancel' })).toBeVisible()
  await expect(elPlan.getByRole('button', { name: opt.action })).toBeHidden()
  await expect(elPlan.getByRole('button', { name: 'Start Over' })).toBeHidden()
  await expect(elPlan.getByRole('button', { name: 'Go Back' })).toBeVisible()
  await expect(elPlan.getByRole('button', { name: 'Cancel' })).toBeHidden({
    timeout: 300000,
  })
}

export async function goBackTo(
  page: Page,
  otp: { path: string },
): Promise<void> {
  await page.getByTitle('Go Back', { exact: true }).click()
  await page.waitForURL(otp.path)
}

export async function goToPlan(
  page: Page,
  opt: { env: string; action: string },
): Promise<void> {
  console.log(`Go to plan for ${opt.env}`)
  const elEnvDetails = page.getByTestId('environment-details')
  const elAction = elEnvDetails.getByRole('link', { name: 'Plan' })
  await expect(elAction).toBeVisible()
  await elAction.click()
  await page.waitForURL(`/plan/environments/${opt.env}`)
  const elPlan = page.getByTestId('plan')
  await expect(elPlan).toBeVisible()
  await expect(elPlan.getByRole('button', { name: opt.action })).toBeVisible()
  await expect(elPlan.getByRole('button', { name: 'Start Over' })).toBeVisible()
  await expect(elPlan.getByRole('button', { name: 'Cancel' })).toBeHidden()
}

export async function addEnvironment(
  page: Page,
  opt: { env: string },
): Promise<void> {
  const elPageNavigation = page.getByTestId('page-navigation')
  const elEnvDetails = elPageNavigation.getByTestId('environment-details')
  const elSelectEnv = elEnvDetails.getByTestId('select-environment')
  await expect(elSelectEnv).toBeVisible()
  const elSelectEnvButton = elEnvDetails.getByRole('button', {
    name: `Environment:`,
  })
  await elSelectEnvButton.click()
  const elSelectEnvList = elEnvDetails.getByTestId('select-environment-list')
  const elAddEnv = elSelectEnvList.getByTestId('add-environment')
  await expect(elAddEnv).toBeVisible()
  const elAddEnvTextbox = elAddEnv.getByRole('textbox')
  await elAddEnvTextbox.fill(opt.env)
  const elAddEnvButton = elAddEnv.getByRole('button', { name: 'Add' })
  await expect(elAddEnvButton).toBeVisible()
  await elAddEnvButton.click()
  const elSelectDevEnv = elEnvDetails.getByRole('button', {
    name: `Environment:${opt.env}`,
  })
  await expect(elSelectDevEnv).toBeVisible()
}

export async function selectEnvironment(
  page: Page,
  opt: { env: string },
): Promise<void> {
  const elPageNavigation = page.getByTestId('page-navigation')
  const elEnvDetails = elPageNavigation.getByTestId('environment-details')
  const elSelectEnv = elEnvDetails.getByTestId('select-environment')
  await expect(elSelectEnv).toBeVisible()
  const elSelectEnvButton = elSelectEnv.getByRole('button')
  await elSelectEnvButton.click()
  const elSelectEnvList = elSelectEnv.getByTestId('select-environment-list')
  const elSelectEnvItem = elSelectEnvList.getByText(opt.env, { exact: true })
  await expect(elSelectEnvItem).toBeVisible()
  await elSelectEnvItem.click()
  await expect(elSelectEnvItem).toBeDisabled()
  await expect(
    elSelectEnv.getByRole('button', { name: `Environment:${opt.env}` }),
  ).toBeVisible()
  await expect(
    elSelectEnv.getByRole('button', { name: `Environment:${opt.env}` }),
  ).toBeEnabled()
}

export async function openFolders(
  page: Page,
  opt: { folders: string[]; isOpen?: boolean },
): Promise<void> {
  const elFE = page.getByTestId('file-explorer')
  await expect(elFE).toBeVisible()
  let folderName = EMPTY_STRING
  for (let i = 0; i < opt.folders.length; i++) {
    const folder = opt.folders[i] ?? NOT_EXIST
    folderName =
      folderName === EMPTY_STRING
        ? folder
        : `${folderName}${PATH_SEPARATOR}${folder}`
    console.log(`Open folder ${folderName}`)
    const elFolder = elFE.getByTitle(folderName, { exact: true })
    if (opt.isOpen != null) {
      expect(await elFolder.getAttribute('aria-expanded')).toEqual(
        String(opt.isOpen),
      )
    }
    console.log(`Folder ${folderName}`)
    const isOpen: boolean = JSON.parse(
      (await elFolder.getAttribute('aria-expanded')) ?? 'false',
    )
    if (isOpen) {
      console.log(`Folder alredy ${folderName} opened`)
    } else {
      await elFolder.click()
      console.log(`Folder ${folderName} has opened`)
      expect(await elFolder.getAttribute('aria-expanded')).toEqual('true')
    }
  }
}

export async function checkFile(
  page: Page,
  opt: { path: string },
): Promise<void> {
  const path = opt.path.replaceAll('/', PATH_SEPARATOR)
  const folders = path.split(PATH_SEPARATOR)
  const fileName = folders.pop() ?? NOT_EXIST
  const parentFolderPath =
    folders.length > 0 ? folders.join(PATH_SEPARATOR) : fileName
  console.log(
    `Check file ${fileName} located at ${
      parentFolderPath === fileName ? 'root' : parentFolderPath
    }`,
  )
  await openFolders(page, { folders })
  const elFE = page.getByTestId('file-explorer')
  await expect(elFE).toBeVisible()
  await expect(elFE.getByTitle(path, { exact: true })).toBeVisible()
}

export async function selectFile(
  page: Page,
  opt: { path: string; modelName?: string; isSelected?: boolean },
): Promise<void> {
  const path = opt.path.replaceAll('/', PATH_SEPARATOR)
  const folders = path.split(PATH_SEPARATOR)
  const fileName = folders.pop() ?? NOT_EXIST
  const parentFolderPath =
    folders.length > 0 ? folders.join(PATH_SEPARATOR) : fileName
  console.log(
    `Select file ${fileName} located at ${
      parentFolderPath === fileName ? 'root' : parentFolderPath
    }`,
  )
  const elEditor = page.getByTestId('editor')
  await expect(elEditor).toBeVisible()
  const elEditorTabs = elEditor.getByTestId('editor-tabs')
  await expect(elEditorTabs).toBeVisible()
  const elCodeEditor = elEditor.getByTestId('code-editor')
  await expect(elCodeEditor).toBeVisible()
  const elFE = page.getByTestId('file-explorer')
  await expect(elFE).toBeVisible()
  const elTab = elEditorTabs.getByTitle(path)
  if (opt.isSelected === true) {
    console.log(`File ${fileName} already selected`)
    await expect(elTab).toBeVisible()
    const elFile = elFE.getByTitle(path, { exact: true })
    await expect(elFile).toBeVisible()
  } else {
    const apiFilePromise = page.waitForResponse(
      r =>
        r.url().includes(`/api/files/${opt.path}`) &&
        r.request().method() === 'GET',
    )
    console.log(`Selecting ${fileName}`)
    await expect(elTab).toBeHidden()
    await openFolders(page, { folders })
    const elFile = elFE.getByTitle(path, { exact: true })
    await expect(elFile).toBeVisible()
    await elFile.click()
    const apiFile = await apiFilePromise
    expect(apiFile.status()).toBe(200)
    const apiFileBody = await apiFile.json()
    expect(apiFileBody.path).toEqual(opt.path)
    expect(apiFileBody.name).toEqual(fileName)
    expect(apiFileBody.content).toBeTruthy()
    await expect(
      elEditor.getByTestId('editor-tabs').getByTitle(path),
    ).toBeVisible()
    await expect(elCodeEditor.getByRole('textbox')).toHaveText(
      apiFileBody.content,
      { useInnerText: true },
    )
    console.log(`Selected ${fileName}`)
  }
  if (opt.modelName != null) {
    await expect(elCodeEditor.getByText(opt.modelName)).toBeVisible()
    console.log(`Selected file has model ${opt.modelName}`)
  }
}

export async function changeFileContent(
  page: Page,
  opt: {
    path: string
    content: string
    modelName?: string
    isSelected?: boolean
  },
): Promise<void> {
  await selectFile(page, {
    path: opt.path,
    modelName: opt.modelName,
    isSelected: opt.isSelected,
  })
  await setFileContentAndSave(page, {
    path: opt.path,
    content: opt.content,
  })
  await selectFile(page, {
    path: opt.path,
    modelName: opt.modelName,
    isSelected: true,
  })
}

function getFileNameAndParentFolder(
  path: string,
): [string, Optional<string>, Optional<string>] {
  path = path.replaceAll('/', PATH_SEPARATOR)

  const folders = path.split(PATH_SEPARATOR)
  const fileName = folders.pop() ?? undefined
  const parentFolderPath =
    folders.length > 0 ? folders.join(PATH_SEPARATOR) : fileName

  return [path, fileName, parentFolderPath]
}
