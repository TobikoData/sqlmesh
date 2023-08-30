import { describe, test, expect } from 'vitest'
import { ModelArtifact } from './artifact'

describe('Model Artifact', () => {
  test('should create empty artifact with ID', () => {
    const artifact = new ModelArtifact()

    expect(artifact.id).toBeTruthy()
  })
})
