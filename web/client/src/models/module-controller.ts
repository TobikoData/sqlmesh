import { Modules } from '@api/client'
import { ModelInitial } from './initial'
import { isFalse } from '@utils/index'
import { EnumRoutes, type Routes } from '~/routes'

export class ModelModuleController extends ModelInitial {
  modules = new Set<Modules>()

  constructor(modules: Modules[] = []) {
    super()

    this.modules = new Set(modules)
  }

  get list(): Modules[] {
    return Array.from(this.modules)
  }

  get isEmpty(): boolean {
    return this.modules.size === 0
  }

  get hasErrors(): boolean {
    return this.modules.has(Modules.errors)
  }

  get hasEditor(): boolean {
    return this.modules.has(Modules.editor)
  }

  get hasFiles(): boolean {
    return this.modules.has(Modules.files)
  }

  get hasProjectEditor(): boolean {
    return this.hasEditor && this.hasFiles
  }

  get hasOnlyProjectEditor(): boolean {
    return this.modules.size === 2 && this.hasProjectEditor
  }

  get hasProjectEditorAndModule(): boolean {
    return (
      this.modules.size === 3 &&
      this.hasProjectEditor &&
      isFalse(this.hasErrors)
    )
  }

  get hasDocs(): boolean {
    return this.modules.has(Modules.docs)
  }

  get hasPlans(): boolean {
    return this.modules.has(Modules.plans)
  }

  get hasOnlyPlans(): boolean {
    return this.modules.size === 1 && this.hasPlans
  }

  get hasOnlyPlansAndErrors(): boolean {
    return this.modules.size === 2 && this.hasPlans && this.hasErrors
  }

  get hasTests(): boolean {
    return this.modules.has(Modules.tests)
  }

  get hasAudits(): boolean {
    return this.modules.has(Modules.audits)
  }

  get hasData(): boolean {
    return this.modules.has(Modules.data)
  }

  get hasLineage(): boolean {
    return this.modules.has(Modules.lineage)
  }

  get hasModuleAndErrors(): boolean {
    return (
      this.hasErrors &&
      (this.modules.size === 2 ||
        (this.modules.size === 3 && this.hasProjectEditor))
    )
  }

  get hasSingleModule(): boolean {
    return this.modules.size === 1
  }

  get showModuleNavigation(): boolean {
    if (
      this.isEmpty ||
      this.hasSingleModule ||
      this.hasOnlyProjectEditor ||
      this.hasModuleAndErrors
    )
      return false
    if (this.hasProjectEditorAndModule) return true

    return isFalse(this.isEmpty)
  }

  get showHistoryNavigation(): boolean {
    if (this.hasProjectEditorAndModule || this.hasDocs) return true
    if (this.hasOnlyProjectEditor || this.hasModuleAndErrors) return false

    return isFalse(this.isEmpty)
  }

  get showNavigation(): boolean {
    return this.hasErrors || this.hasPlans || this.showHistoryNavigation
  }

  update(modules: Modules[] = []): void {
    this.modules = new Set(modules)
  }

  defaultNavigationRoute(): Routes {
    if (this.hasEditor) return EnumRoutes.Editor
    if (this.hasDocs) return EnumRoutes.Docs
    if (this.hasPlans) return EnumRoutes.Plan

    return EnumRoutes.NotFound
  }
}
