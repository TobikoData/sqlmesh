import { describe, expect, test } from 'vitest'
import { Position } from '@xyflow/react'

import {
  getOnlySelectedNodes,
  getTransformedNodes,
  getTransformedModelEdgesSourceTargets,
  getTransformedModelEdgesTargetSources,
  createNode,
  calculateNodeBaseHeight,
  calculateNodeDetailsHeight,
  createEdge,
} from './help'
import type {
  LineageNode,
  LineageNodesMap,
  LineageNodeData,
  LineageDetails,
  LineageAdjacencyList,
  NodeId,
  EdgeId,
  PortId,
} from './utils'
import { toNodeID, toEdgeID } from './utils'

describe('Lineage Help Functions', () => {
  describe('getOnlySelectedNodes', () => {
    test('should return only selected nodes from the node map', () => {
      const nodesMap = {
        node1: {
          id: 'node1' as NodeId,
          position: { x: 0, y: 0 },
          data: {},
        },
        node2: {
          id: 'node2' as NodeId,
          position: { x: 100, y: 100 },
          data: {},
        },
        node3: {
          id: 'node3' as NodeId,
          position: { x: 200, y: 200 },
          data: {},
        },
      }

      const selectedNodes = new Set<NodeId>([
        'node1' as NodeId,
        'node3' as NodeId,
      ])
      const result = getOnlySelectedNodes(nodesMap, selectedNodes)

      expect(Object.keys(result)).toHaveLength(2)
      expect(result).toHaveProperty('node1')
      expect(result).toHaveProperty('node3')
      expect(result).not.toHaveProperty('node2')
    })

    test('should return empty object when no nodes are selected', () => {
      const nodesMap = {
        node1: {
          id: 'node1' as NodeId,
          position: { x: 0, y: 0 },
          data: {},
        },
      }

      const selectedNodes = new Set<NodeId>()
      const result = getOnlySelectedNodes(nodesMap, selectedNodes)

      expect(Object.keys(result)).toHaveLength(0)
    })

    test('should handle empty node map', () => {
      const nodesMap: LineageNodesMap<LineageNodeData> = {}
      const selectedNodes = new Set<NodeId>(['node1' as NodeId])
      const result = getOnlySelectedNodes(nodesMap, selectedNodes)

      expect(Object.keys(result)).toHaveLength(0)
    })
  })

  describe('getTransformedNodes', () => {
    test('should transform nodes using the provided transform function', () => {
      const adjacencyListKeys = ['model1', 'model2']
      const lineageDetails: LineageDetails<
        string,
        { name: string; type: string }
      > = {
        model1: { name: 'Model 1', type: 'table' },
        model2: { name: 'Model 2', type: 'view' },
      }

      const transformNode = (
        nodeId: NodeId,
        data: { name: string; type: string },
      ) =>
        ({
          id: nodeId,
          position: { x: 0, y: 0 },
          data: { label: data.name, nodeType: data.type },
        }) as LineageNode<{ label: string; nodeType: string }>

      const result = getTransformedNodes(
        adjacencyListKeys,
        lineageDetails,
        transformNode,
      )

      const encodedModel1 = toNodeID('model1')
      const encodedModel2 = toNodeID('model2')

      expect(Object.keys(result)).toHaveLength(2)
      expect(result[encodedModel1]).toEqual({
        id: encodedModel1,
        position: { x: 0, y: 0 },
        data: { label: 'Model 1', nodeType: 'table' },
      })
      expect(result[encodedModel2]).toEqual({
        id: encodedModel2,
        position: { x: 0, y: 0 },
        data: { label: 'Model 2', nodeType: 'view' },
      })
    })

    test('should handle empty adjacency list', () => {
      const adjacencyListKeys: string[] = []
      const lineageDetails: LineageDetails<string, { name: string }> = {}
      const transformNode = (nodeId: NodeId, data: { name: string }) =>
        ({
          id: nodeId,
          position: { x: 0, y: 0 },
          data: { label: data.name },
        }) as LineageNode<{ label: string }>

      const result = getTransformedNodes(
        adjacencyListKeys,
        lineageDetails,
        transformNode,
      )

      expect(Object.keys(result)).toHaveLength(0)
    })
  })

  describe('getTransformedModelEdgesSourceTargets', () => {
    test('should transform edges from source to targets using the provided transform function', () => {
      const adjacencyListKeys = ['model1', 'model2', 'model3']
      const lineageAdjacencyList: LineageAdjacencyList = {
        model1: ['model2', 'model3'],
        model2: ['model3'],
        model3: [],
      }

      const transformEdge = (
        type: string,
        edgeId: EdgeId,
        sourceId: NodeId,
        targetId: NodeId,
      ) => ({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type,
        zIndex: 1,
      })

      const result = getTransformedModelEdgesSourceTargets(
        adjacencyListKeys,
        lineageAdjacencyList,
        transformEdge,
      )

      expect(result).toHaveLength(3)

      const model1Id = toNodeID('model1')
      const model2Id = toNodeID('model2')
      const model3Id = toNodeID('model3')

      expect(result[0]).toEqual({
        id: toEdgeID('model1', 'model2'),
        source: model1Id,
        target: model2Id,
        type: 'edge',
        zIndex: 1,
      })
      expect(result[1]).toEqual({
        id: toEdgeID('model1', 'model3'),
        source: model1Id,
        target: model3Id,
        type: 'edge',
        zIndex: 1,
      })
      expect(result[2]).toEqual({
        id: toEdgeID('model2', 'model3'),
        source: model2Id,
        target: model3Id,
        type: 'edge',
        zIndex: 1,
      })
    })

    test('should skip edges where target is not in adjacency list', () => {
      const adjacencyListKeys = ['model1']
      const lineageAdjacencyList: LineageAdjacencyList = {
        model1: ['model2'], // model2 is not in the adjacency list
      }

      const transformEdge = (
        type: string,
        edgeId: EdgeId,
        sourceId: NodeId,
        targetId: NodeId,
      ) => ({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type,
        zIndex: 1,
      })

      const result = getTransformedModelEdgesSourceTargets(
        adjacencyListKeys,
        lineageAdjacencyList,
        transformEdge,
      )

      expect(result).toHaveLength(0)
    })

    test('should handle empty adjacency list', () => {
      const adjacencyListKeys: string[] = []
      const lineageAdjacencyList: LineageAdjacencyList = {}

      const transformEdge = (
        type: string,
        edgeId: EdgeId,
        sourceId: NodeId,
        targetId: NodeId,
      ) => ({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type,
        zIndex: 1,
      })

      const result = getTransformedModelEdgesSourceTargets(
        adjacencyListKeys,
        lineageAdjacencyList,
        transformEdge,
      )

      expect(result).toHaveLength(0)
    })

    test('should handle nodes with no targets', () => {
      const adjacencyListKeys = ['model1', 'model2']
      const lineageAdjacencyList = {
        model1: [],
        model2: null,
      } as unknown as LineageAdjacencyList

      const transformEdge = (
        type: string,
        edgeId: EdgeId,
        sourceId: NodeId,
        targetId: NodeId,
      ) => ({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type,
        zIndex: 1,
      })

      const result = getTransformedModelEdgesSourceTargets(
        adjacencyListKeys,
        lineageAdjacencyList,
        transformEdge,
      )

      expect(result).toHaveLength(0)
    })
  })

  describe('getTransformedModelEdgesTargetSources', () => {
    test('should transform edges from target to sources using the provided transform function', () => {
      const adjacencyListKeys = ['model1', 'model2', 'model3']
      const lineageAdjacencyList: LineageAdjacencyList = {
        model1: [],
        model2: ['model1'],
        model3: ['model1', 'model2'],
      }

      const transformEdge = (
        type: string,
        edgeId: EdgeId,
        sourceId: NodeId,
        targetId: NodeId,
      ) => ({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type,
        zIndex: 1,
      })

      const result = getTransformedModelEdgesTargetSources(
        adjacencyListKeys,
        lineageAdjacencyList,
        transformEdge,
      )

      expect(result).toHaveLength(3)

      const model1Id = toNodeID('model1')
      const model2Id = toNodeID('model2')
      const model3Id = toNodeID('model3')

      expect(result[0]).toEqual({
        id: toEdgeID('model1', 'model2'),
        source: model1Id,
        target: model2Id,
        type: 'edge',
        zIndex: 1,
      })
      expect(result[1]).toEqual({
        id: toEdgeID('model1', 'model3'),
        source: model1Id,
        target: model3Id,
        type: 'edge',
        zIndex: 1,
      })
      expect(result[2]).toEqual({
        id: toEdgeID('model2', 'model3'),
        source: model2Id,
        target: model3Id,
        type: 'edge',
        zIndex: 1,
      })
    })

    test('should skip edges where source is not in adjacency list', () => {
      const adjacencyListKeys = ['model2']
      const lineageAdjacencyList: LineageAdjacencyList = {
        model2: ['model1'], // model1 is not in the adjacency list
      }

      const transformEdge = (
        type: string,
        edgeId: EdgeId,
        sourceId: NodeId,
        targetId: NodeId,
      ) => ({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type,
        zIndex: 1,
      })

      const result = getTransformedModelEdgesTargetSources(
        adjacencyListKeys,
        lineageAdjacencyList,
        transformEdge,
      )

      expect(result).toHaveLength(0)
    })

    test('should handle empty adjacency list', () => {
      const adjacencyListKeys: string[] = []
      const lineageAdjacencyList: LineageAdjacencyList = {}

      const transformEdge = (
        type: string,
        edgeId: EdgeId,
        sourceId: NodeId,
        targetId: NodeId,
      ) => ({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type,
        zIndex: 1,
      })

      const result = getTransformedModelEdgesTargetSources(
        adjacencyListKeys,
        lineageAdjacencyList,
        transformEdge,
      )

      expect(result).toHaveLength(0)
    })

    test('should handle nodes with no sources', () => {
      const adjacencyListKeys = ['model1', 'model2']
      const lineageAdjacencyList = {
        model1: [],
        model2: null,
      } as unknown as LineageAdjacencyList

      const transformEdge = (
        type: string,
        edgeId: EdgeId,
        sourceId: NodeId,
        targetId: NodeId,
      ) => ({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type,
        zIndex: 1,
      })

      const result = getTransformedModelEdgesTargetSources(
        adjacencyListKeys,
        lineageAdjacencyList,
        transformEdge,
      )

      expect(result).toHaveLength(0)
    })
  })

  describe('createNode', () => {
    test('should create a node with provided data', () => {
      const nodeId = 'test-node' as NodeId
      const data = { label: 'Test Node', value: 42 }
      const node = createNode('custom', nodeId, data)

      expect(node).toEqual({
        id: nodeId,
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        width: 300, // DEFAULT_NODE_WIDTH
        height: 32, // DEFAULT_NODE_HEIGHT
        data,
        type: 'custom',
        hidden: false,
        position: { x: 0, y: 0 },
        zIndex: 10,
      })
    })

    test('should create a node with minimal data', () => {
      const nodeId = 'minimal' as NodeId
      const data = {}
      const node = createNode('default', nodeId, data)

      expect(node.id).toBe(nodeId)
      expect(node.type).toBe('default')
      expect(node.data).toEqual({})
      expect(node.hidden).toBe(false)
    })
  })

  describe('calculateNodeBaseHeight', () => {
    test('should calculate base height with no additional components', () => {
      const height = calculateNodeBaseHeight({})
      // border (2*2) + base (28) = 32
      expect(height).toBe(32)
    })

    test('should include footer height when specified', () => {
      const height = calculateNodeBaseHeight({ includeNodeFooterHeight: true })
      // border (2*2) + base (28) + footer (20) = 52
      expect(height).toBe(52)
    })

    test('should include ceiling height when specified', () => {
      const height = calculateNodeBaseHeight({ includeCeilingHeight: true })
      // border (2*2) + base (28) + ceiling (20) + ceilingGap (4) = 56
      expect(height).toBe(56)
    })

    test('should include floor height when specified', () => {
      const height = calculateNodeBaseHeight({ includeFloorHeight: true })
      // border (2*2) + base (28) + floor (20) + floorGap (4) = 56
      expect(height).toBe(56)
    })

    test('should include all components when specified', () => {
      const height = calculateNodeBaseHeight({
        includeNodeFooterHeight: true,
        includeCeilingHeight: true,
        includeFloorHeight: true,
      })
      // border (2*2) + base (28) + footer (20) + ceiling (20) + ceilingGap (4) + floor (20) + floorGap (4) = 100
      expect(height).toBe(100)
    })
  })

  describe('calculateNodeDetailsHeight', () => {
    test('should return 0 when no details', () => {
      const height = calculateNodeDetailsHeight({})
      expect(height).toBe(0)
    })

    test('should calculate height for single detail', () => {
      const height = calculateNodeDetailsHeight({ nodeDetailsCount: 1 })
      // 1 * 24 (nodeOptionHeight) = 24
      expect(height).toBe(24)
    })

    test('should calculate height for multiple details with separators', () => {
      const height = calculateNodeDetailsHeight({ nodeDetailsCount: 3 })
      // 3 * 24 (nodeOptionHeight) + 2 * 1 (separators between items) = 74
      expect(height).toBe(74)
    })

    test('should handle zero details count', () => {
      const height = calculateNodeDetailsHeight({ nodeDetailsCount: 0 })
      expect(height).toBe(0)
    })
  })

  describe('createEdge', () => {
    test('should create edge with basic parameters', () => {
      const edgeId = 'edge1' as EdgeId
      const sourceId = 'source1' as NodeId
      const targetId = 'target1' as NodeId

      const edge = createEdge('straight', edgeId, sourceId, targetId)

      expect(edge).toEqual({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type: 'straight',
        sourceHandle: undefined,
        targetHandle: undefined,
        data: undefined,
        zIndex: 1,
      })
    })

    test('should create edge with handles', () => {
      const edgeId = 'edge2' as EdgeId
      const sourceId = 'source2' as NodeId
      const targetId = 'target2' as NodeId
      const sourceHandleId = 'handle1' as PortId
      const targetHandleId = 'handle2' as PortId

      const edge = createEdge(
        'bezier',
        edgeId,
        sourceId,
        targetId,
        sourceHandleId,
        targetHandleId,
      )

      expect(edge).toEqual({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type: 'bezier',
        sourceHandle: sourceHandleId,
        targetHandle: targetHandleId,
        data: undefined,
        zIndex: 1,
      })
    })

    test('should create edge with data', () => {
      const edgeId = 'edge3' as EdgeId
      const sourceId = 'source3' as NodeId
      const targetId = 'target3' as NodeId
      const data = { label: 'Connection', weight: 5 }

      const edge = createEdge(
        'smoothstep',
        edgeId,
        sourceId,
        targetId,
        undefined,
        undefined,
        data,
      )

      expect(edge).toEqual({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type: 'smoothstep',
        sourceHandle: undefined,
        targetHandle: undefined,
        data,
        zIndex: 1,
      })
    })

    test('should create edge with all parameters', () => {
      const edgeId = 'edge4' as EdgeId
      const sourceId = 'source4' as NodeId
      const targetId = 'target4' as NodeId
      const sourceHandleId = 'handle3' as PortId
      const targetHandleId = 'handle4' as PortId
      const data = { animated: true }

      const edge = createEdge(
        'step',
        edgeId,
        sourceId,
        targetId,
        sourceHandleId,
        targetHandleId,
        data,
      )

      expect(edge).toEqual({
        id: edgeId,
        source: sourceId,
        target: targetId,
        type: 'step',
        sourceHandle: sourceHandleId,
        targetHandle: targetHandleId,
        data,
        zIndex: 1,
      })
    })
  })
})
