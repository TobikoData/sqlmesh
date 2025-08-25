#!/usr/bin/env python
"""
Analyze dbt-core usage in SQLMesh tests to identify leaf nodes for Rust translation.

This script:
1. Runs pytest tests/dbt with coverage tracking
2. Generates a call graph of dbt-core functions
3. Identifies leaf nodes (functions with no outgoing calls to other dbt functions)
4. Outputs a prioritized list of functions to translate to Rust
"""

import ast
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple
import importlib.util
import inspect
import coverage
import pytest


class DBTCallGraphAnalyzer:
    def __init__(self):
        self.call_graph = defaultdict(set)  # function -> set of functions it calls
        self.reverse_graph = defaultdict(set)  # function -> set of functions that call it
        self.function_info = {}  # function -> metadata (file, line, complexity)
        self.dbt_modules = set()
        self.coverage_data = None
        
    def find_dbt_core_path(self) -> Path:
        """Find the dbt-core installation path."""
        try:
            import dbt
            dbt_path = Path(dbt.__file__).parent
            return dbt_path
        except ImportError:
            print("ERROR: dbt-core not installed")
            sys.exit(1)
    
    def run_tests_with_coverage(self) -> coverage.Coverage:
        """Run pytest tests/dbt with coverage enabled."""
        print("Running tests with coverage...")
        
        # Create coverage instance
        cov = coverage.Coverage(source=["dbt"], branch=True)
        cov.start()
        
        # Run pytest programmatically
        exit_code = pytest.main([
            "tests/dbt",
            "-v",
            "--tb=short",
            "-x",  # Stop on first failure for faster analysis
            "--no-header",
            "--no-summary",
            "-q"
        ])
        
        cov.stop()
        cov.save()
        
        if exit_code != 0:
            print(f"WARNING: Tests exited with code {exit_code}")
        
        return cov
    
    def analyze_ast_calls(self, tree: ast.AST, module_name: str):
        """Analyze AST to extract function calls within dbt modules."""
        
        class CallVisitor(ast.NodeVisitor):
            def __init__(self, analyzer, module_name):
                self.analyzer = analyzer
                self.module_name = module_name
                self.current_function = None
                self.current_class = None
                
            def visit_ClassDef(self, node):
                old_class = self.current_class
                self.current_class = node.name
                self.generic_visit(node)
                self.current_class = old_class
                
            def visit_FunctionDef(self, node):
                old_function = self.current_function
                
                if self.current_class:
                    func_name = f"{self.module_name}.{self.current_class}.{node.name}"
                else:
                    func_name = f"{self.module_name}.{node.name}"
                
                self.current_function = func_name
                
                # Calculate complexity (rough estimate based on AST nodes)
                complexity = len(list(ast.walk(node)))
                
                self.analyzer.function_info[func_name] = {
                    'module': self.module_name,
                    'class': self.current_class,
                    'function': node.name,
                    'line': node.lineno,
                    'complexity': complexity,
                    'docstring': ast.get_docstring(node),
                    'is_private': node.name.startswith('_'),
                    'is_test': node.name.startswith('test_'),
                }
                
                self.generic_visit(node)
                self.current_function = old_function
            
            def visit_Call(self, node):
                if self.current_function:
                    # Try to resolve the call target
                    call_name = self._resolve_call(node)
                    if call_name and call_name.startswith('dbt'):
                        self.analyzer.call_graph[self.current_function].add(call_name)
                        self.analyzer.reverse_graph[call_name].add(self.current_function)
                
                self.generic_visit(node)
            
            def _resolve_call(self, node):
                """Try to resolve what function is being called."""
                if isinstance(node.func, ast.Name):
                    return node.func.id
                elif isinstance(node.func, ast.Attribute):
                    parts = []
                    current = node.func
                    while isinstance(current, ast.Attribute):
                        parts.append(current.attr)
                        current = current.value
                    if isinstance(current, ast.Name):
                        parts.append(current.id)
                    return '.'.join(reversed(parts))
                return None
        
        visitor = CallVisitor(self, module_name)
        visitor.visit(tree)
    
    def analyze_dbt_modules(self):
        """Analyze all dbt modules to build call graph."""
        dbt_path = self.find_dbt_core_path()
        print(f"Analyzing dbt-core at: {dbt_path}")
        
        for py_file in dbt_path.rglob("*.py"):
            if '__pycache__' in str(py_file):
                continue
                
            rel_path = py_file.relative_to(dbt_path.parent)
            module_name = str(rel_path).replace('/', '.').replace('\\', '.')[:-3]
            
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                tree = ast.parse(content, filename=str(py_file))
                self.analyze_ast_calls(tree, module_name)
                self.dbt_modules.add(module_name)
            except Exception as e:
                print(f"Warning: Failed to parse {py_file}: {e}")
    
    def identify_leaf_nodes(self) -> List[str]:
        """Identify functions that don't call other dbt functions."""
        leaf_nodes = []
        
        for func_name, info in self.function_info.items():
            # Skip test functions and private functions initially
            if info.get('is_test') or info.get('is_private'):
                continue
                
            # Check if this function calls any other dbt functions
            calls = self.call_graph.get(func_name, set())
            dbt_calls = [c for c in calls if c.startswith('dbt')]
            
            if not dbt_calls:
                leaf_nodes.append(func_name)
        
        return leaf_nodes
    
    def calculate_translation_priority(self, leaf_nodes: List[str]) -> List[Tuple[str, float]]:
        """Calculate priority score for translating each leaf node."""
        priorities = []
        
        for func in leaf_nodes:
            info = self.function_info.get(func, {})
            
            # Calculate priority based on:
            # 1. How many functions call this (higher = more important)
            # 2. Complexity (lower = easier to translate first)
            # 3. Coverage (if available)
            
            callers = len(self.reverse_graph.get(func, set()))
            complexity = info.get('complexity', 100)
            
            # Priority formula: more callers and less complexity = higher priority
            priority = (callers * 10) / (complexity + 1)
            
            priorities.append((func, priority, {
                'callers': callers,
                'complexity': complexity,
                'module': info.get('module', ''),
                'line': info.get('line', 0),
            }))
        
        # Sort by priority (highest first)
        priorities.sort(key=lambda x: x[1], reverse=True)
        
        return priorities
    
    def generate_report(self):
        """Generate the final report with translation recommendations."""
        print("\n" + "="*80)
        print("DBT-CORE TO RUST TRANSLATION ANALYSIS")
        print("="*80)
        
        # Analyze modules
        self.analyze_dbt_modules()
        
        # Get leaf nodes
        leaf_nodes = self.identify_leaf_nodes()
        print(f"\nFound {len(leaf_nodes)} leaf node functions")
        
        # Calculate priorities
        priorities = self.calculate_translation_priority(leaf_nodes)
        
        # Output results
        print("\n" + "-"*80)
        print("RECOMMENDED TRANSLATION ORDER (Top 50 Leaf Nodes)")
        print("-"*80)
        print(f"{'Rank':<6} {'Priority':<10} {'Callers':<8} {'Complex':<8} {'Function'}")
        print("-"*80)
        
        for i, (func, priority, meta) in enumerate(priorities[:50], 1):
            print(f"{i:<6} {priority:<10.2f} {meta['callers']:<8} {meta['complexity']:<8} {func}")
        
        # Save detailed results to JSON
        output_file = "dbt_translation_analysis.json"
        output_data = {
            'summary': {
                'total_functions': len(self.function_info),
                'total_leaf_nodes': len(leaf_nodes),
                'total_edges': sum(len(calls) for calls in self.call_graph.values()),
            },
            'leaf_nodes': [
                {
                    'function': func,
                    'priority': priority,
                    'metadata': meta
                }
                for func, priority, meta in priorities
            ],
            'call_graph': {
                func: list(calls) 
                for func, calls in self.call_graph.items()
                if calls  # Only include functions that make calls
            },
            'reverse_graph': {
                func: list(callers)
                for func, callers in self.reverse_graph.items()
                if callers  # Only include functions that are called
            }
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\n✅ Detailed analysis saved to: {output_file}")
        
        # Generate Rust translation starter template
        self.generate_rust_template(priorities[:10])
    
    def generate_rust_template(self, top_functions):
        """Generate a Rust module template for the top priority functions."""
        rust_file = "dbt_core_rust_starter.rs"
        
        content = """//! DBT-Core Rust Translation - Leaf Node Functions
//! 
//! This file contains Rust implementations of the highest-priority
//! leaf node functions from dbt-core Python codebase.
//! 
//! Translation Priority Order (based on usage frequency and complexity):
"""
        
        for i, (func, priority, meta) in enumerate(top_functions, 1):
            content += f"//! {i}. {func} (priority: {priority:.2f})\n"
        
        content += """
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Error type for DBT operations
#[derive(Debug, thiserror::Error)]
pub enum DbtError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, DbtError>;

"""
        
        # Add stub implementations for top functions
        for func, priority, meta in top_functions[:5]:
            func_name = func.split('.')[-1]
            module = '.'.join(func.split('.')[:-1])
            
            content += f"""
/// Rust implementation of {func}
/// Original module: {module}
/// Priority: {priority:.2f}, Called by {meta['callers']} functions
pub fn {self.to_rust_name(func_name)}() -> Result<()> {{
    // TODO: Implement {func_name}
    todo!("Implement {func_name}")
}}
"""
        
        content += """
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_placeholder() {
        // TODO: Add tests as functions are implemented
        assert!(true);
    }
}
"""
        
        with open(rust_file, 'w') as f:
            f.write(content)
        
        print(f"✅ Rust starter template saved to: {rust_file}")
    
    def to_rust_name(self, python_name: str) -> str:
        """Convert Python function name to Rust naming convention."""
        # Handle special cases and convert to snake_case if needed
        return python_name.lower().replace('__', '_')


def main():
    """Main entry point."""
    print("Starting DBT-Core to Rust Translation Analysis...")
    print("This will:")
    print("1. Run tests/dbt to understand dbt-core usage")
    print("2. Build a call graph of dbt-core functions")
    print("3. Identify leaf nodes for translation")
    print("4. Generate a prioritized translation plan\n")
    
    analyzer = DBTCallGraphAnalyzer()
    
    # Run tests with coverage (optional, can be commented out for faster analysis)
    try:
        print("Running tests (this may take a few minutes)...")
        cov = analyzer.run_tests_with_coverage()
        analyzer.coverage_data = cov
        print("✅ Tests completed")
    except Exception as e:
        print(f"⚠️  Could not run tests with coverage: {e}")
        print("Continuing with static analysis only...\n")
    
    # Generate the analysis report
    analyzer.generate_report()
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)
    print("\nNext steps:")
    print("1. Review dbt_translation_analysis.json for the complete call graph")
    print("2. Start translating functions from the top of the priority list")
    print("3. Use dbt_core_rust_starter.rs as a template for your Rust module")
    print("4. Run 'python analyze_dbt_for_rust.py' again to update analysis")


if __name__ == "__main__":
    main()