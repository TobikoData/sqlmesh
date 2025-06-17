import { assert, describe, it } from 'vitest'
import * as fs from 'fs'
import * as path from 'path'

describe('Commands', () => {
  it('all commands should start with "SQLMesh: " prefix', () => {
    const packageJsonPath = path.join(__dirname, '..', '..', 'package.json')
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'))

    const commands = packageJson.contributes?.commands || []

    commands.forEach((command: any) => {
      assert(
        command.title.startsWith('SQLMesh: '),
        `Command "${command.command}" title "${command.title}" should start with "SQLMesh: "`,
      )
    })
  })
})
