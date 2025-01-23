import { describe, test, expect } from 'vitest'
import { ModelFile } from './file'

describe('ModelFile', () => {
  const defaultFile = {
    name: 'test.sql',
    path: '/test/path',
    extension: '.sql',
    content: 'SELECT * FROM table;',
  }

  describe('constructor', () => {
    test('should create instance with default values', () => {
      const file = new ModelFile()
      expect(file.name).toBe('')
      expect(file.path).toBe('')
      expect(file.extension).toBe('.sql')
      expect(file.content).toBe('')
    })

    test('should create instance with provided values', () => {
      const file = new ModelFile(defaultFile)
      expect(file.name).toBe(defaultFile.name)
      expect(file.path).toBe(defaultFile.path)
      expect(file.extension).toBe(defaultFile.extension)
      expect(file.content).toBe(defaultFile.content)
    })
  })

  describe('isSQL', () => {
    test('should return true for SQL files', () => {
      const file = new ModelFile(defaultFile)
      expect(file.isSQL).toBe(true)
    })

    test('should return false for non-SQL files', () => {
      const file = new ModelFile({ ...defaultFile, extension: '.py' })
      expect(file.isSQL).toBe(false)
    })
  })

  describe('fingerprint', () => {
    test('should return concatenated string of content, name and path', () => {
      const file = new ModelFile(defaultFile)
      expect(file.fingerprint).toBe(
        `${defaultFile.content}${defaultFile.name}${defaultFile.path}`,
      )
    })
  })

  describe('removeChanges', () => {
    test('should reset content to original value', () => {
      const file = new ModelFile(defaultFile)
      file.content = 'new content'
      file.removeChanges()
      expect(file.content).toBe(defaultFile.content)
    })
  })

  describe('copyName', () => {
    test('should generate copy name with unique suffix', () => {
      const file = new ModelFile(defaultFile)
      const copyName = file.copyName()
      expect(copyName).toMatch(/^Copy of test__[\w-]+\.sql$/)
    })
  })

  describe('update', () => {
    test('should update file properties', () => {
      const file = new ModelFile()
      file.update(defaultFile)
      expect(file.isChanged).toBe(false)
      expect(file.isEmpty).toBe(false)
      expect(file.isSQL).toBe(true)
    })

    test('should retain content when file is not synced', () => {
      const file = new ModelFile(defaultFile)
      const newContent = 'SELECT * FROM table LIMIT 5;'
      file.content = newContent
      expect(file.isChanged).toBe(true)
      expect(file.isEmpty).toBe(false)
      file.update(defaultFile)
      expect(file.content).toBe(newContent)
    })
  })

  describe('isModelFile', () => {
    test('should return true for ModelFile instances', () => {
      const file = new ModelFile()
      expect(ModelFile.isModelFile(file)).toBe(true)
    })

    test('should return false for non-ModelFile objects', () => {
      expect(ModelFile.isModelFile({})).toBe(false)
      expect(ModelFile.isModelFile(null)).toBe(false)
    })
  })
})
