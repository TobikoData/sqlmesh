import { test, expect } from '@playwright/test'

test('has title', async ({ page }) => {
  await page.goto('/')

  await expect(page).toHaveTitle(/SQLMesh by Tobiko/)
})

test('home link', async ({ page }) => {
  await page.goto('/')

  await expect(page.getByTitle('Home')).toHaveAttribute('href', '/')
})

test('tobiko link', async ({ page }) => {
  await page.goto('/')

  await expect(page.getByTitle('Tobiko Data website')).toHaveAttribute(
    'href',
    'https://tobikodata.com/',
  )
})

test('documantation link', async ({ page }) => {
  await page.goto('/')

  await expect(page.getByText('Documentation')).toHaveAttribute(
    'href',
    'http://sqlmesh.readthedocs.io/en/stable/',
  )
})
