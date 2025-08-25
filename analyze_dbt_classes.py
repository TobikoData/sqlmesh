#!/usr/bin/env python
"""
Analyze dbt-core treating classes as whole units for Rust translation.

This script analyzes dbt-core at the class level, identifying which classes
are leaf nodes (don't depend on other dbt classes) and can be translated first.
"""

import ast
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple
import coverage
import pytest


class DBTClassAnalyzer:
    def __init__(self):
        self.class_dependencies = defaultdict(set)  # class -> set of classes it depends on
        self.class_dependents = defaultdict(set)  # class -> set of classes that depend on it
        self.class_info = {}  # class -> metadata
        self.module_classes = defaultdict(list)  # module -> list of classes
        self.coverage_data = None
        self.executed_classes = set()  # Classes that were actually executed in tests
        
    def find_dbt_core_path(self) -> Path:
        """Find the dbt-core installation path."""
        try:
            import dbt
            dbt_path = Path(dbt.__file__).parent
            return dbt_path
        except ImportError:
            # Try local dbt-core directory
            local_dbt = Path("dbt-core/core/dbt")
            if local_dbt.exists():
                return local_dbt
            print("ERROR: dbt-core not found")
            sys.exit(1)
    
    def run_tests_with_coverage(self) -> coverage.Coverage:
        """Run pytest tests/dbt with coverage enabled to see what's actually used."""
        print("Running tests with coverage to identify actually used classes...")
        
        # Create coverage instance
        cov = coverage.Coverage(source=["dbt"], branch=True)
        cov.start()
        
        # Run pytest programmatically - just a subset for speed
        exit_code = pytest.main([
            "tests/dbt",
            "-v",
            "--tb=short",
            "-x",  # Stop on first failure
            "--no-header",
            "--no-summary",
            "-q",
            "-k", "not slow",  # Skip slow tests for faster analysis
            "--maxfail=5"  # Stop after 5 failures
        ])
        
        cov.stop()
        cov.save()
        
        if exit_code != 0:
            print(f"WARNING: Tests exited with code {exit_code}")
        
        # Analyze coverage data to find executed classes
        self._analyze_coverage_data(cov)
        
        return cov
    
    def _analyze_coverage_data(self, cov: coverage.Coverage):
        """Extract which classes were actually executed from coverage data."""
        try:
            # Get executed files
            executed_files = cov.get_data().measured_files()
            
            for file_path in executed_files:
                if 'dbt' not in file_path or '__pycache__' in file_path:
                    continue
                
                # Parse the file to find class definitions
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    tree = ast.parse(content)
                    
                    # Extract module name from file path
                    if 'site-packages' in file_path:
                        parts = file_path.split('site-packages/')[-1]
                    else:
                        parts = file_path
                    
                    module_name = parts.replace('/', '.').replace('\\', '.').replace('.py', '')
                    
                    # Find classes in this executed file
                    for node in ast.walk(tree):
                        if isinstance(node, ast.ClassDef):
                            class_full_name = f"{module_name}.{node.name}"
                            self.executed_classes.add(class_full_name)
                            
                except Exception:
                    pass  # Skip files we can't parse
                    
            print(f"  Found {len(self.executed_classes)} classes executed during tests")
            
        except Exception as e:
            print(f"  Warning: Could not analyze coverage data: {e}")
    
    def analyze_class_dependencies(self, tree: ast.AST, module_name: str, file_path: Path):
        """Analyze AST to extract class definitions and their dependencies."""
        
        class ClassAnalyzer(ast.NodeVisitor):
            def __init__(self, analyzer, module_name, file_path):
                self.analyzer = analyzer
                self.module_name = module_name
                self.file_path = file_path
                self.current_class = None
                self.imports = {}  # local name -> full module path
                self.class_imports = defaultdict(set)  # class -> imported types it uses
                
            def visit_Import(self, node):
                for alias in node.names:
                    import_name = alias.name
                    local_name = alias.asname or alias.name
                    self.imports[local_name] = import_name
                
            def visit_ImportFrom(self, node):
                if node.module:
                    base_module = node.module
                    for alias in node.names:
                        if alias.name == '*':
                            # Handle star imports
                            self.imports['*'] = base_module
                        else:
                            import_path = f"{base_module}.{alias.name}"
                            local_name = alias.asname or alias.name
                            self.imports[local_name] = import_path
                
            def visit_ClassDef(self, node):
                old_class = self.current_class
                self.current_class = node.name
                class_full_name = f"{self.module_name}.{node.name}"
                
                # Extract base classes
                base_classes = []
                external_deps = set()
                dbt_deps = set()
                
                for base in node.bases:
                    base_name = self._get_name(base)
                    if base_name:
                        # Resolve the base class
                        resolved = self._resolve_type(base_name)
                        if resolved:
                            base_classes.append(resolved)
                            if 'dbt' in resolved:
                                dbt_deps.add(resolved)
                            else:
                                external_deps.add(resolved)
                
                # Calculate class metrics
                methods = []
                properties = []
                class_methods = []
                static_methods = []
                
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        methods.append(item.name)
                        
                        # Check for decorators
                        for dec in item.decorator_list:
                            dec_name = self._get_name(dec)
                            if dec_name == 'property':
                                properties.append(item.name)
                            elif dec_name == 'classmethod':
                                class_methods.append(item.name)
                            elif dec_name == 'staticmethod':
                                static_methods.append(item.name)
                
                # Store class info
                self.analyzer.class_info[class_full_name] = {
                    'module': self.module_name,
                    'name': node.name,
                    'file': str(self.file_path),
                    'line': node.lineno,
                    'base_classes': base_classes,
                    'methods': methods,
                    'properties': properties,
                    'class_methods': class_methods,
                    'static_methods': static_methods,
                    'num_methods': len(methods),
                    'lines_of_code': node.end_lineno - node.lineno + 1 if node.end_lineno else 1,
                    'complexity': len(list(ast.walk(node))),
                    'docstring': ast.get_docstring(node),
                    'is_abstract': self._is_abstract(node),
                    'is_dataclass': self._is_dataclass(node),
                    'is_enum': any('Enum' in str(b) for b in base_classes),
                    'external_deps': list(external_deps),
                    'dbt_deps': list(dbt_deps),
                }
                
                # Update module tracking
                self.analyzer.module_classes[self.module_name].append(class_full_name)
                
                # Track dependencies
                for dep in dbt_deps:
                    self.analyzer.class_dependencies[class_full_name].add(dep)
                    self.analyzer.class_dependents[dep].add(class_full_name)
                
                # Analyze the class body for type references
                self.generic_visit(node)
                
                # Check for type hints and references in the class
                self._analyze_class_body_dependencies(node, class_full_name)
                
                self.current_class = old_class
            
            def _analyze_class_body_dependencies(self, node, class_full_name):
                """Analyze the class body for dependencies through type hints and usage."""
                
                class TypeVisitor(ast.NodeVisitor):
                    def __init__(self, analyzer, imports):
                        self.dependencies = set()
                        self.analyzer = analyzer
                        self.imports = imports
                    
                    def visit_Name(self, node):
                        # Check if this name refers to a dbt class
                        name = node.id
                        if name in self.imports:
                            resolved = self.imports[name]
                            if 'dbt' in resolved:
                                self.dependencies.add(resolved)
                    
                    def visit_Attribute(self, node):
                        # Handle attribute access like dbt.models.Model
                        path = self._get_full_path(node)
                        if path and 'dbt' in path:
                            self.dependencies.add(path)
                        self.generic_visit(node)
                    
                    def _get_full_path(self, node):
                        parts = []
                        current = node
                        while isinstance(current, ast.Attribute):
                            parts.append(current.attr)
                            current = current.value
                        if isinstance(current, ast.Name):
                            parts.append(current.id)
                        return '.'.join(reversed(parts)) if parts else None
                
                type_visitor = TypeVisitor(self.analyzer, self.imports)
                type_visitor.visit(node)
                
                # Add discovered dependencies
                for dep in type_visitor.dependencies:
                    if dep != class_full_name:  # Don't add self-references
                        self.analyzer.class_dependencies[class_full_name].add(dep)
                        self.analyzer.class_dependents[dep].add(class_full_name)
            
            def _get_name(self, node):
                """Extract name from AST node."""
                if isinstance(node, ast.Name):
                    return node.id
                elif isinstance(node, ast.Attribute):
                    parts = []
                    current = node
                    while isinstance(current, ast.Attribute):
                        parts.append(current.attr)
                        current = current.value
                    if isinstance(current, ast.Name):
                        parts.append(current.id)
                    return '.'.join(reversed(parts))
                elif isinstance(node, ast.Constant):
                    return str(node.value)
                return None
            
            def _resolve_type(self, type_name: str) -> str:
                """Resolve a type name to its full module path."""
                if '.' in type_name:
                    # Already qualified
                    return type_name
                
                # Check imports
                if type_name in self.imports:
                    return self.imports[type_name]
                
                # Could be a local class in the same module
                return f"{self.module_name}.{type_name}"
            
            def _is_abstract(self, node):
                """Check if a class is abstract (has ABC base or abstractmethod)."""
                # Check for ABC base class
                for base in node.bases:
                    base_name = self._get_name(base)
                    if base_name and ('ABC' in base_name or 'Abstract' in base_name):
                        return True
                
                # Check for abstractmethod decorators
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        for dec in item.decorator_list:
                            dec_name = self._get_name(dec)
                            if dec_name and 'abstractmethod' in dec_name:
                                return True
                
                return False
            
            def _is_dataclass(self, node):
                """Check if a class is a dataclass."""
                for dec in node.decorator_list:
                    dec_name = self._get_name(dec)
                    if dec_name and 'dataclass' in dec_name:
                        return True
                return False
        
        visitor = ClassAnalyzer(self, module_name, file_path)
        visitor.visit(tree)
    
    def analyze_dbt_modules(self):
        """Analyze all dbt modules to build class dependency graph."""
        dbt_path = self.find_dbt_core_path()
        print(f"Analyzing dbt-core at: {dbt_path}")
        
        py_files = list(dbt_path.rglob("*.py"))
        print(f"Found {len(py_files)} Python files to analyze")
        
        for i, py_file in enumerate(py_files):
            if '__pycache__' in str(py_file):
                continue
            
            if i % 50 == 0:
                print(f"  Processed {i}/{len(py_files)} files...")
                
            # Calculate module name
            try:
                if 'dbt-core' in str(py_file):
                    # Local dbt-core directory
                    rel_path = py_file.relative_to(Path("dbt-core/core"))
                else:
                    # Installed dbt package
                    rel_path = py_file.relative_to(dbt_path.parent)
                    
                module_name = str(rel_path).replace('/', '.').replace('\\', '.')[:-3]
            except ValueError:
                continue
            
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                tree = ast.parse(content, filename=str(py_file))
                self.analyze_class_dependencies(tree, module_name, py_file)
            except Exception as e:
                # Silently skip files that can't be parsed
                pass
        
        print(f"  Analyzed {len(self.class_info)} classes")
    
    def identify_leaf_classes(self) -> List[str]:
        """Identify classes that don't depend on other dbt classes."""
        leaf_classes = []
        
        for class_name, info in self.class_info.items():
            # Get dbt dependencies only
            dbt_deps = [
                dep for dep in self.class_dependencies.get(class_name, set())
                if 'dbt' in dep and dep in self.class_info  # Must be a known class
            ]
            
            if not dbt_deps:
                leaf_classes.append(class_name)
        
        return leaf_classes
    
    def calculate_class_priority(self, class_name: str) -> float:
        """Calculate translation priority for a class."""
        info = self.class_info.get(class_name, {})
        
        # Priority factors
        dependents = len(self.class_dependents.get(class_name, set()))
        complexity = info.get('complexity', 1000)
        loc = info.get('lines_of_code', 100)
        num_methods = info.get('num_methods', 10)
        is_abstract = info.get('is_abstract', False)
        is_dataclass = info.get('is_dataclass', False)
        is_enum = info.get('is_enum', False)
        was_executed = class_name in self.executed_classes
        
        # Calculate priority
        priority = 0
        priority += dependents * 20  # Classes used by many others are important
        priority += (1000 / (complexity + 1))  # Simpler classes first
        priority += (500 / (loc + 1))  # Smaller classes first
        priority += 100 if is_dataclass else 0  # Dataclasses are easy to translate
        priority += 80 if is_enum else 0  # Enums are easy to translate
        priority -= 50 if is_abstract else 0  # Abstract classes need subclasses
        priority -= num_methods * 2  # Fewer methods is easier
        priority += 200 if was_executed else 0  # HUGE bonus for actually used classes
        
        return priority
    
    def generate_report(self, run_tests=True):
        """Generate the final report with class-level translation recommendations."""
        print("\n" + "="*80)
        print("DBT-CORE CLASS-LEVEL TRANSLATION ANALYSIS")
        print("="*80)
        
        # Run tests first to see what's actually used
        if run_tests:
            try:
                self.coverage_data = self.run_tests_with_coverage()
                print(f"✅ Tests completed, identified {len(self.executed_classes)} executed classes")
            except Exception as e:
                print(f"⚠️  Could not run tests with coverage: {e}")
                print("Continuing with static analysis only...\n")
        
        # Analyze modules
        self.analyze_dbt_modules()
        
        # Get leaf classes
        leaf_classes = self.identify_leaf_classes()
        
        # Calculate priorities for all classes
        all_priorities = []
        for class_name in self.class_info:
            priority = self.calculate_class_priority(class_name)
            all_priorities.append((class_name, priority))
        
        all_priorities.sort(key=lambda x: x[1], reverse=True)
        
        # Separate leaf and non-leaf classes
        leaf_priorities = [(c, p) for c, p in all_priorities if c in leaf_classes]
        non_leaf_priorities = [(c, p) for c, p in all_priorities if c not in leaf_classes]
        
        print(f"\nAnalysis Summary:")
        print(f"  Total classes analyzed: {len(self.class_info)}")
        print(f"  Leaf classes (no dbt dependencies): {len(leaf_classes)}")
        print(f"  Classes with dependencies: {len(self.class_info) - len(leaf_classes)}")
        
        # Find strongly connected components (classes that depend on each other)
        sccs = self.find_strongly_connected_components()
        print(f"  Strongly connected components: {len([s for s in sccs if len(s) > 1])}")
        
        # Output top leaf classes
        print("\n" + "-"*80)
        print("TOP 20 LEAF CLASSES TO TRANSLATE FIRST")
        print("-"*80)
        print(f"{'Rank':<6} {'Priority':<10} {'Used':<6} {'Type':<12} {'Methods':<8} {'LOC':<7} {'Class'}")
        print("-"*80)
        
        for i, (class_name, priority) in enumerate(leaf_priorities[:20], 1):
            info = self.class_info[class_name]
            class_type = 'dataclass' if info.get('is_dataclass') else \
                        'enum' if info.get('is_enum') else \
                        'abstract' if info.get('is_abstract') else \
                        'regular'
            was_executed = '✓' if class_name in self.executed_classes else ' '
            short_name = class_name.split('.')[-1]
            module = '.'.join(class_name.split('.')[:-1])
            print(f"{i:<6} {priority:<10.2f} {was_executed:<6} {class_type:<12} {info['num_methods']:<8} {info['lines_of_code']:<7} {short_name} ({module})")
        
        # Group classes by module
        module_classes = defaultdict(list)
        for class_name in leaf_classes:
            module = '.'.join(class_name.split('.')[:-1])
            module_classes[module].append(class_name)
        
        # Find modules with most leaf classes
        print("\n" + "-"*80)
        print("MODULES WITH MOST LEAF CLASSES")
        print("-"*80)
        print(f"{'Module':<40} {'Leaf Classes':<15} {'Total Classes'}")
        print("-"*80)
        
        module_counts = sorted(
            [(m, len(classes)) for m, classes in module_classes.items()],
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        for module, leaf_count in module_counts:
            total_count = len(self.module_classes.get(module, []))
            print(f"{module:<40} {leaf_count:<15} {total_count}")
        
        # Identify class clusters that should be translated together
        print("\n" + "-"*80)
        print("CLASS CLUSTERS TO TRANSLATE TOGETHER")
        print("-"*80)
        
        for scc in sccs[:5]:
            if len(scc) > 1:
                print(f"\nCluster of {len(scc)} interdependent classes:")
                for class_name in sorted(scc)[:5]:
                    print(f"  - {class_name}")
        
        # Save detailed results
        self.save_json_report(leaf_priorities, non_leaf_priorities, sccs)
        self.generate_rust_module_template(leaf_priorities[:10])
    
    def find_strongly_connected_components(self) -> List[Set[str]]:
        """Find strongly connected components in the class dependency graph."""
        # Tarjan's algorithm for finding SCCs
        index_counter = [0]
        stack = []
        lowlinks = {}
        index = {}
        on_stack = defaultdict(bool)
        sccs = []
        
        def strongconnect(node):
            index[node] = index_counter[0]
            lowlinks[node] = index_counter[0]
            index_counter[0] += 1
            stack.append(node)
            on_stack[node] = True
            
            for successor in self.class_dependencies.get(node, set()):
                if successor not in index:
                    strongconnect(successor)
                    lowlinks[node] = min(lowlinks[node], lowlinks[successor])
                elif on_stack[successor]:
                    lowlinks[node] = min(lowlinks[node], index[successor])
            
            if lowlinks[node] == index[node]:
                scc = set()
                while True:
                    w = stack.pop()
                    on_stack[w] = False
                    scc.add(w)
                    if w == node:
                        break
                sccs.append(scc)
        
        for node in self.class_info:
            if node not in index:
                strongconnect(node)
        
        return sorted(sccs, key=len, reverse=True)
    
    def save_json_report(self, leaf_priorities, non_leaf_priorities, sccs):
        """Save detailed JSON report."""
        output_file = "dbt_class_analysis.json"
        
        output_data = {
            'summary': {
                'total_classes': len(self.class_info),
                'leaf_classes': len(leaf_priorities),
                'classes_with_deps': len(non_leaf_priorities),
                'strongly_connected_components': len([s for s in sccs if len(s) > 1]),
            },
            'leaf_classes': [
                {
                    'rank': i,
                    'class': class_name,
                    'priority': priority,
                    'info': self.class_info[class_name]
                }
                for i, (class_name, priority) in enumerate(leaf_priorities[:50], 1)
            ],
            'class_dependencies': {
                class_name: list(deps)
                for class_name, deps in self.class_dependencies.items()
                if deps
            },
            'strongly_connected_components': [
                list(scc) for scc in sccs if len(scc) > 1
            ][:10],
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\n✅ Detailed class analysis saved to: {output_file}")
    
    def generate_rust_module_template(self, top_classes):
        """Generate Rust module template for top priority classes."""
        rust_file = "dbt_classes_rust.rs"
        
        content = """//! DBT-Core Rust Translation - Class-Level Implementation
//! 
//! This file contains Rust implementations of high-priority classes
//! from dbt-core, starting with leaf classes that have no dbt dependencies.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

"""
        
        for i, (class_name, priority) in enumerate(top_classes, 1):
            info = self.class_info[class_name]
            class_short = class_name.split('.')[-1]
            
            if info.get('is_enum'):
                content += f"""
/// Rust implementation of {class_name}
/// Priority: {priority:.2f}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum {class_short} {{
    // TODO: Add enum variants
}}
"""
            elif info.get('is_dataclass'):
                content += f"""
/// Rust implementation of {class_name}
/// Priority: {priority:.2f}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct {class_short} {{
    // TODO: Add fields based on Python dataclass
}}
"""
            else:
                content += f"""
/// Rust implementation of {class_name}
/// Priority: {priority:.2f}
/// Methods: {', '.join(info['methods'][:3]) if info['methods'] else 'none'}
pub struct {class_short} {{
    // TODO: Add fields
}}

impl {class_short} {{
    pub fn new() -> Self {{
        todo!("Implement constructor")
    }}
    
"""
                for method in info['methods'][:3]:
                    if not method.startswith('_'):
                        content += f"""    pub fn {method}(&self) {{
        todo!("Implement {method}")
    }}
    
"""
                content += "}\n"
        
        with open(rust_file, 'w') as f:
            f.write(content)
        
        print(f"✅ Rust class template saved to: {rust_file}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze dbt-core classes for Rust translation")
    parser.add_argument("--no-tests", action="store_true", 
                       help="Skip running tests (faster but less accurate)")
    args = parser.parse_args()
    
    print("Starting DBT-Core Class-Level Analysis for Rust Translation...")
    print("This analyzes classes as whole units for translation planning.\n")
    
    if args.no_tests:
        print("Skipping test execution (--no-tests flag provided)")
    else:
        print("Will run tests to identify actually used classes")
        print("(Use --no-tests flag for faster static-only analysis)\n")
    
    analyzer = DBTClassAnalyzer()
    analyzer.generate_report(run_tests=not args.no_tests)
    
    print("\n" + "="*80)
    print("CLASS ANALYSIS COMPLETE!")
    print("="*80)
    print("\nKey insights:")
    print("  - Leaf classes have no dbt dependencies and can be translated first")
    print("  - Classes marked with ✓ were actually executed during tests")
    print("  - Dataclasses and enums are typically easiest to translate")
    print("  - Classes in the same SCC should be translated together")
    print("  - Start with high-priority leaf classes for maximum impact")


if __name__ == "__main__":
    main()