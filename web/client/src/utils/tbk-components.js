var f_ = Object.defineProperty
var g_ = (t, r, i) =>
  r in t
    ? f_(t, r, { enumerable: !0, configurable: !0, writable: !0, value: i })
    : (t[r] = i)
var W = (t, r, i) => g_(t, typeof r != 'symbol' ? r + '' : r, i)
var tc = ''
function cu(t) {
  tc = t
}
function m_(t = '') {
  if (!tc) {
    const r = [...document.getElementsByTagName('script')],
      i = r.find(o => o.hasAttribute('data-shoelace'))
    if (i) cu(i.getAttribute('data-shoelace'))
    else {
      const o = r.find(
        d =>
          /shoelace(\.min)?\.js($|\?)/.test(d.src) ||
          /shoelace-autoloader(\.min)?\.js($|\?)/.test(d.src),
      )
      let a = ''
      o && (a = o.getAttribute('src')), cu(a.split('/').slice(0, -1).join('/'))
    }
  }
  return tc.replace(/\/$/, '') + (t ? `/${t.replace(/^\//, '')}` : '')
}
var v_ = {
    name: 'default',
    resolver: t => m_(`assets/icons/${t}.svg`),
  },
  b_ = v_,
  du = {
    caret: `
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <polyline points="6 9 12 15 18 9"></polyline>
    </svg>
  `,
    check: `
    <svg part="checked-icon" class="checkbox__icon" viewBox="0 0 16 16">
      <g stroke="none" stroke-width="1" fill="none" fill-rule="evenodd" stroke-linecap="round">
        <g stroke="currentColor">
          <g transform="translate(3.428571, 3.428571)">
            <path d="M0,5.71428571 L3.42857143,9.14285714"></path>
            <path d="M9.14285714,0 L3.42857143,9.14285714"></path>
          </g>
        </g>
      </g>
    </svg>
  `,
    'chevron-down': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-chevron-down" viewBox="0 0 16 16">
      <path fill-rule="evenodd" d="M1.646 4.646a.5.5 0 0 1 .708 0L8 10.293l5.646-5.647a.5.5 0 0 1 .708.708l-6 6a.5.5 0 0 1-.708 0l-6-6a.5.5 0 0 1 0-.708z"/>
    </svg>
  `,
    'chevron-left': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-chevron-left" viewBox="0 0 16 16">
      <path fill-rule="evenodd" d="M11.354 1.646a.5.5 0 0 1 0 .708L5.707 8l5.647 5.646a.5.5 0 0 1-.708.708l-6-6a.5.5 0 0 1 0-.708l6-6a.5.5 0 0 1 .708 0z"/>
    </svg>
  `,
    'chevron-right': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-chevron-right" viewBox="0 0 16 16">
      <path fill-rule="evenodd" d="M4.646 1.646a.5.5 0 0 1 .708 0l6 6a.5.5 0 0 1 0 .708l-6 6a.5.5 0 0 1-.708-.708L10.293 8 4.646 2.354a.5.5 0 0 1 0-.708z"/>
    </svg>
  `,
    copy: `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-copy" viewBox="0 0 16 16">
      <path fill-rule="evenodd" d="M4 2a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V2Zm2-1a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V2a1 1 0 0 0-1-1H6ZM2 5a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-1h1v1a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h1v1H2Z"/>
    </svg>
  `,
    eye: `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-eye" viewBox="0 0 16 16">
      <path d="M16 8s-3-5.5-8-5.5S0 8 0 8s3 5.5 8 5.5S16 8 16 8zM1.173 8a13.133 13.133 0 0 1 1.66-2.043C4.12 4.668 5.88 3.5 8 3.5c2.12 0 3.879 1.168 5.168 2.457A13.133 13.133 0 0 1 14.828 8c-.058.087-.122.183-.195.288-.335.48-.83 1.12-1.465 1.755C11.879 11.332 10.119 12.5 8 12.5c-2.12 0-3.879-1.168-5.168-2.457A13.134 13.134 0 0 1 1.172 8z"/>
      <path d="M8 5.5a2.5 2.5 0 1 0 0 5 2.5 2.5 0 0 0 0-5zM4.5 8a3.5 3.5 0 1 1 7 0 3.5 3.5 0 0 1-7 0z"/>
    </svg>
  `,
    'eye-slash': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-eye-slash" viewBox="0 0 16 16">
      <path d="M13.359 11.238C15.06 9.72 16 8 16 8s-3-5.5-8-5.5a7.028 7.028 0 0 0-2.79.588l.77.771A5.944 5.944 0 0 1 8 3.5c2.12 0 3.879 1.168 5.168 2.457A13.134 13.134 0 0 1 14.828 8c-.058.087-.122.183-.195.288-.335.48-.83 1.12-1.465 1.755-.165.165-.337.328-.517.486l.708.709z"/>
      <path d="M11.297 9.176a3.5 3.5 0 0 0-4.474-4.474l.823.823a2.5 2.5 0 0 1 2.829 2.829l.822.822zm-2.943 1.299.822.822a3.5 3.5 0 0 1-4.474-4.474l.823.823a2.5 2.5 0 0 0 2.829 2.829z"/>
      <path d="M3.35 5.47c-.18.16-.353.322-.518.487A13.134 13.134 0 0 0 1.172 8l.195.288c.335.48.83 1.12 1.465 1.755C4.121 11.332 5.881 12.5 8 12.5c.716 0 1.39-.133 2.02-.36l.77.772A7.029 7.029 0 0 1 8 13.5C3 13.5 0 8 0 8s.939-1.721 2.641-3.238l.708.709zm10.296 8.884-12-12 .708-.708 12 12-.708.708z"/>
    </svg>
  `,
    eyedropper: `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-eyedropper" viewBox="0 0 16 16">
      <path d="M13.354.646a1.207 1.207 0 0 0-1.708 0L8.5 3.793l-.646-.647a.5.5 0 1 0-.708.708L8.293 5l-7.147 7.146A.5.5 0 0 0 1 12.5v1.793l-.854.853a.5.5 0 1 0 .708.707L1.707 15H3.5a.5.5 0 0 0 .354-.146L11 7.707l1.146 1.147a.5.5 0 0 0 .708-.708l-.647-.646 3.147-3.146a1.207 1.207 0 0 0 0-1.708l-2-2zM2 12.707l7-7L10.293 7l-7 7H2v-1.293z"></path>
    </svg>
  `,
    'grip-vertical': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-grip-vertical" viewBox="0 0 16 16">
      <path d="M7 2a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zM7 5a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zM7 8a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm-3 3a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm-3 3a1 1 0 1 1-2 0 1 1 0 0 1 2 0zm3 0a1 1 0 1 1-2 0 1 1 0 0 1 2 0z"></path>
    </svg>
  `,
    indeterminate: `
    <svg part="indeterminate-icon" class="checkbox__icon" viewBox="0 0 16 16">
      <g stroke="none" stroke-width="1" fill="none" fill-rule="evenodd" stroke-linecap="round">
        <g stroke="currentColor" stroke-width="2">
          <g transform="translate(2.285714, 6.857143)">
            <path d="M10.2857143,1.14285714 L1.14285714,1.14285714"></path>
          </g>
        </g>
      </g>
    </svg>
  `,
    'person-fill': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-person-fill" viewBox="0 0 16 16">
      <path d="M3 14s-1 0-1-1 1-4 6-4 6 3 6 4-1 1-1 1H3zm5-6a3 3 0 1 0 0-6 3 3 0 0 0 0 6z"/>
    </svg>
  `,
    'play-fill': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-play-fill" viewBox="0 0 16 16">
      <path d="m11.596 8.697-6.363 3.692c-.54.313-1.233-.066-1.233-.697V4.308c0-.63.692-1.01 1.233-.696l6.363 3.692a.802.802 0 0 1 0 1.393z"></path>
    </svg>
  `,
    'pause-fill': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-pause-fill" viewBox="0 0 16 16">
      <path d="M5.5 3.5A1.5 1.5 0 0 1 7 5v6a1.5 1.5 0 0 1-3 0V5a1.5 1.5 0 0 1 1.5-1.5zm5 0A1.5 1.5 0 0 1 12 5v6a1.5 1.5 0 0 1-3 0V5a1.5 1.5 0 0 1 1.5-1.5z"></path>
    </svg>
  `,
    radio: `
    <svg part="checked-icon" class="radio__icon" viewBox="0 0 16 16">
      <g stroke="none" stroke-width="1" fill="none" fill-rule="evenodd">
        <g fill="currentColor">
          <circle cx="8" cy="8" r="3.42857143"></circle>
        </g>
      </g>
    </svg>
  `,
    'star-fill': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-star-fill" viewBox="0 0 16 16">
      <path d="M3.612 15.443c-.386.198-.824-.149-.746-.592l.83-4.73L.173 6.765c-.329-.314-.158-.888.283-.95l4.898-.696L7.538.792c.197-.39.73-.39.927 0l2.184 4.327 4.898.696c.441.062.612.636.282.95l-3.522 3.356.83 4.73c.078.443-.36.79-.746.592L8 13.187l-4.389 2.256z"/>
    </svg>
  `,
    'x-lg': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-x-lg" viewBox="0 0 16 16">
      <path d="M2.146 2.854a.5.5 0 1 1 .708-.708L8 7.293l5.146-5.147a.5.5 0 0 1 .708.708L8.707 8l5.147 5.146a.5.5 0 0 1-.708.708L8 8.707l-5.146 5.147a.5.5 0 0 1-.708-.708L7.293 8 2.146 2.854Z"/>
    </svg>
  `,
    'x-circle-fill': `
    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-x-circle-fill" viewBox="0 0 16 16">
      <path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0zM5.354 4.646a.5.5 0 1 0-.708.708L7.293 8l-2.647 2.646a.5.5 0 0 0 .708.708L8 8.707l2.646 2.647a.5.5 0 0 0 .708-.708L8.707 8l2.647-2.646a.5.5 0 0 0-.708-.708L8 7.293 5.354 4.646z"></path>
    </svg>
  `,
  },
  y_ = {
    name: 'system',
    resolver: t =>
      t in du ? `data:image/svg+xml,${encodeURIComponent(du[t])}` : '',
  },
  __ = y_,
  Ws = [b_, __],
  qs = []
function w_(t) {
  qs.push(t)
}
function x_(t) {
  qs = qs.filter(r => r !== t)
}
function hu(t) {
  return Ws.find(r => r.name === t)
}
function Lp(t, r) {
  k_(t),
    Ws.push({
      name: t,
      resolver: r.resolver,
      mutator: r.mutator,
      spriteSheet: r.spriteSheet,
    }),
    qs.forEach(i => {
      i.library === t && i.setIcon()
    })
}
function k_(t) {
  Ws = Ws.filter(r => r.name !== t)
}
/**
 * @license
 * Copyright 2019 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const Ns = globalThis,
  hc =
    Ns.ShadowRoot &&
    (Ns.ShadyCSS === void 0 || Ns.ShadyCSS.nativeShadow) &&
    'adoptedStyleSheets' in Document.prototype &&
    'replace' in CSSStyleSheet.prototype,
  uc = Symbol(),
  uu = /* @__PURE__ */ new WeakMap()
let Dp = class {
  constructor(r, i, o) {
    if (((this._$cssResult$ = !0), o !== uc))
      throw Error(
        'CSSResult is not constructable. Use `unsafeCSS` or `css` instead.',
      )
    ;(this.cssText = r), (this.t = i)
  }
  get styleSheet() {
    let r = this.o
    const i = this.t
    if (hc && r === void 0) {
      const o = i !== void 0 && i.length === 1
      o && (r = uu.get(i)),
        r === void 0 &&
          ((this.o = r = new CSSStyleSheet()).replaceSync(this.cssText),
          o && uu.set(i, r))
    }
    return r
  }
  toString() {
    return this.cssText
  }
}
const ft = t => new Dp(typeof t == 'string' ? t : t + '', void 0, uc),
  st = (t, ...r) => {
    const i =
      t.length === 1
        ? t[0]
        : r.reduce(
            (o, a, d) =>
              o +
              (h => {
                if (h._$cssResult$ === !0) return h.cssText
                if (typeof h == 'number') return h
                throw Error(
                  "Value passed to 'css' function must be a 'css' function result: " +
                    h +
                    ". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.",
                )
              })(a) +
              t[d + 1],
            t[0],
          )
    return new Dp(i, t, uc)
  },
  $_ = (t, r) => {
    if (hc)
      t.adoptedStyleSheets = r.map(i =>
        i instanceof CSSStyleSheet ? i : i.styleSheet,
      )
    else
      for (const i of r) {
        const o = document.createElement('style'),
          a = Ns.litNonce
        a !== void 0 && o.setAttribute('nonce', a),
          (o.textContent = i.cssText),
          t.appendChild(o)
      }
  },
  pu = hc
    ? t => t
    : t =>
        t instanceof CSSStyleSheet
          ? (r => {
              let i = ''
              for (const o of r.cssRules) i += o.cssText
              return ft(i)
            })(t)
          : t
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const {
    is: C_,
    defineProperty: S_,
    getOwnPropertyDescriptor: z_,
    getOwnPropertyNames: A_,
    getOwnPropertySymbols: E_,
    getPrototypeOf: T_,
  } = Object,
  Ti = globalThis,
  fu = Ti.trustedTypes,
  I_ = fu ? fu.emptyScript : '',
  Dl = Ti.reactiveElementPolyfillSupport,
  yo = (t, r) => t,
  Ln = {
    toAttribute(t, r) {
      switch (r) {
        case Boolean:
          t = t ? I_ : null
          break
        case Object:
        case Array:
          t = t == null ? t : JSON.stringify(t)
      }
      return t
    },
    fromAttribute(t, r) {
      let i = t
      switch (r) {
        case Boolean:
          i = t !== null
          break
        case Number:
          i = t === null ? null : Number(t)
          break
        case Object:
        case Array:
          try {
            i = JSON.parse(t)
          } catch {
            i = null
          }
      }
      return i
    },
  },
  pc = (t, r) => !C_(t, r),
  gu = {
    attribute: !0,
    type: String,
    converter: Ln,
    reflect: !1,
    hasChanged: pc,
  }
Symbol.metadata ?? (Symbol.metadata = Symbol('metadata')),
  Ti.litPropertyMetadata ??
    (Ti.litPropertyMetadata = /* @__PURE__ */ new WeakMap())
class Tn extends HTMLElement {
  static addInitializer(r) {
    this._$Ei(), (this.l ?? (this.l = [])).push(r)
  }
  static get observedAttributes() {
    return this.finalize(), this._$Eh && [...this._$Eh.keys()]
  }
  static createProperty(r, i = gu) {
    if (
      (i.state && (i.attribute = !1),
      this._$Ei(),
      this.elementProperties.set(r, i),
      !i.noAccessor)
    ) {
      const o = Symbol(),
        a = this.getPropertyDescriptor(r, o, i)
      a !== void 0 && S_(this.prototype, r, a)
    }
  }
  static getPropertyDescriptor(r, i, o) {
    const { get: a, set: d } = z_(this.prototype, r) ?? {
      get() {
        return this[i]
      },
      set(h) {
        this[i] = h
      },
    }
    return {
      get() {
        return a == null ? void 0 : a.call(this)
      },
      set(h) {
        const m = a == null ? void 0 : a.call(this)
        d.call(this, h), this.requestUpdate(r, m, o)
      },
      configurable: !0,
      enumerable: !0,
    }
  }
  static getPropertyOptions(r) {
    return this.elementProperties.get(r) ?? gu
  }
  static _$Ei() {
    if (this.hasOwnProperty(yo('elementProperties'))) return
    const r = T_(this)
    r.finalize(),
      r.l !== void 0 && (this.l = [...r.l]),
      (this.elementProperties = new Map(r.elementProperties))
  }
  static finalize() {
    if (this.hasOwnProperty(yo('finalized'))) return
    if (
      ((this.finalized = !0),
      this._$Ei(),
      this.hasOwnProperty(yo('properties')))
    ) {
      const i = this.properties,
        o = [...A_(i), ...E_(i)]
      for (const a of o) this.createProperty(a, i[a])
    }
    const r = this[Symbol.metadata]
    if (r !== null) {
      const i = litPropertyMetadata.get(r)
      if (i !== void 0) for (const [o, a] of i) this.elementProperties.set(o, a)
    }
    this._$Eh = /* @__PURE__ */ new Map()
    for (const [i, o] of this.elementProperties) {
      const a = this._$Eu(i, o)
      a !== void 0 && this._$Eh.set(a, i)
    }
    this.elementStyles = this.finalizeStyles(this.styles)
  }
  static finalizeStyles(r) {
    const i = []
    if (Array.isArray(r)) {
      const o = new Set(r.flat(1 / 0).reverse())
      for (const a of o) i.unshift(pu(a))
    } else r !== void 0 && i.push(pu(r))
    return i
  }
  static _$Eu(r, i) {
    const o = i.attribute
    return o === !1
      ? void 0
      : typeof o == 'string'
      ? o
      : typeof r == 'string'
      ? r.toLowerCase()
      : void 0
  }
  constructor() {
    super(),
      (this._$Ep = void 0),
      (this.isUpdatePending = !1),
      (this.hasUpdated = !1),
      (this._$Em = null),
      this._$Ev()
  }
  _$Ev() {
    var r
    ;(this._$ES = new Promise(i => (this.enableUpdating = i))),
      (this._$AL = /* @__PURE__ */ new Map()),
      this._$E_(),
      this.requestUpdate(),
      (r = this.constructor.l) == null || r.forEach(i => i(this))
  }
  addController(r) {
    var i
    ;(this._$EO ?? (this._$EO = /* @__PURE__ */ new Set())).add(r),
      this.renderRoot !== void 0 &&
        this.isConnected &&
        ((i = r.hostConnected) == null || i.call(r))
  }
  removeController(r) {
    var i
    ;(i = this._$EO) == null || i.delete(r)
  }
  _$E_() {
    const r = /* @__PURE__ */ new Map(),
      i = this.constructor.elementProperties
    for (const o of i.keys())
      this.hasOwnProperty(o) && (r.set(o, this[o]), delete this[o])
    r.size > 0 && (this._$Ep = r)
  }
  createRenderRoot() {
    const r =
      this.shadowRoot ?? this.attachShadow(this.constructor.shadowRootOptions)
    return $_(r, this.constructor.elementStyles), r
  }
  connectedCallback() {
    var r
    this.renderRoot ?? (this.renderRoot = this.createRenderRoot()),
      this.enableUpdating(!0),
      (r = this._$EO) == null ||
        r.forEach(i => {
          var o
          return (o = i.hostConnected) == null ? void 0 : o.call(i)
        })
  }
  enableUpdating(r) {}
  disconnectedCallback() {
    var r
    ;(r = this._$EO) == null ||
      r.forEach(i => {
        var o
        return (o = i.hostDisconnected) == null ? void 0 : o.call(i)
      })
  }
  attributeChangedCallback(r, i, o) {
    this._$AK(r, o)
  }
  _$EC(r, i) {
    var d
    const o = this.constructor.elementProperties.get(r),
      a = this.constructor._$Eu(r, o)
    if (a !== void 0 && o.reflect === !0) {
      const h = (
        ((d = o.converter) == null ? void 0 : d.toAttribute) !== void 0
          ? o.converter
          : Ln
      ).toAttribute(i, o.type)
      ;(this._$Em = r),
        h == null ? this.removeAttribute(a) : this.setAttribute(a, h),
        (this._$Em = null)
    }
  }
  _$AK(r, i) {
    var d
    const o = this.constructor,
      a = o._$Eh.get(r)
    if (a !== void 0 && this._$Em !== a) {
      const h = o.getPropertyOptions(a),
        m =
          typeof h.converter == 'function'
            ? { fromAttribute: h.converter }
            : ((d = h.converter) == null ? void 0 : d.fromAttribute) !== void 0
            ? h.converter
            : Ln
      ;(this._$Em = a),
        (this[a] = m.fromAttribute(i, h.type)),
        (this._$Em = null)
    }
  }
  requestUpdate(r, i, o) {
    if (r !== void 0) {
      if (
        (o ?? (o = this.constructor.getPropertyOptions(r)),
        !(o.hasChanged ?? pc)(this[r], i))
      )
        return
      this.P(r, i, o)
    }
    this.isUpdatePending === !1 && (this._$ES = this._$ET())
  }
  P(r, i, o) {
    this._$AL.has(r) || this._$AL.set(r, i),
      o.reflect === !0 &&
        this._$Em !== r &&
        (this._$Ej ?? (this._$Ej = /* @__PURE__ */ new Set())).add(r)
  }
  async _$ET() {
    this.isUpdatePending = !0
    try {
      await this._$ES
    } catch (i) {
      Promise.reject(i)
    }
    const r = this.scheduleUpdate()
    return r != null && (await r), !this.isUpdatePending
  }
  scheduleUpdate() {
    return this.performUpdate()
  }
  performUpdate() {
    var o
    if (!this.isUpdatePending) return
    if (!this.hasUpdated) {
      if (
        (this.renderRoot ?? (this.renderRoot = this.createRenderRoot()),
        this._$Ep)
      ) {
        for (const [d, h] of this._$Ep) this[d] = h
        this._$Ep = void 0
      }
      const a = this.constructor.elementProperties
      if (a.size > 0)
        for (const [d, h] of a)
          h.wrapped !== !0 ||
            this._$AL.has(d) ||
            this[d] === void 0 ||
            this.P(d, this[d], h)
    }
    let r = !1
    const i = this._$AL
    try {
      ;(r = this.shouldUpdate(i)),
        r
          ? (this.willUpdate(i),
            (o = this._$EO) == null ||
              o.forEach(a => {
                var d
                return (d = a.hostUpdate) == null ? void 0 : d.call(a)
              }),
            this.update(i))
          : this._$EU()
    } catch (a) {
      throw ((r = !1), this._$EU(), a)
    }
    r && this._$AE(i)
  }
  willUpdate(r) {}
  _$AE(r) {
    var i
    ;(i = this._$EO) == null ||
      i.forEach(o => {
        var a
        return (a = o.hostUpdated) == null ? void 0 : a.call(o)
      }),
      this.hasUpdated || ((this.hasUpdated = !0), this.firstUpdated(r)),
      this.updated(r)
  }
  _$EU() {
    ;(this._$AL = /* @__PURE__ */ new Map()), (this.isUpdatePending = !1)
  }
  get updateComplete() {
    return this.getUpdateComplete()
  }
  getUpdateComplete() {
    return this._$ES
  }
  shouldUpdate(r) {
    return !0
  }
  update(r) {
    this._$Ej && (this._$Ej = this._$Ej.forEach(i => this._$EC(i, this[i]))),
      this._$EU()
  }
  updated(r) {}
  firstUpdated(r) {}
}
;(Tn.elementStyles = []),
  (Tn.shadowRootOptions = { mode: 'open' }),
  (Tn[yo('elementProperties')] = /* @__PURE__ */ new Map()),
  (Tn[yo('finalized')] = /* @__PURE__ */ new Map()),
  Dl == null || Dl({ ReactiveElement: Tn }),
  (Ti.reactiveElementVersions ?? (Ti.reactiveElementVersions = [])).push(
    '2.0.4',
  )
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const _o = globalThis,
  Ys = _o.trustedTypes,
  mu = Ys ? Ys.createPolicy('lit-html', { createHTML: t => t }) : void 0,
  Mp = '$lit$',
  zi = `lit$${Math.random().toFixed(9).slice(2)}$`,
  Rp = '?' + zi,
  O_ = `<${Rp}>`,
  Qi = document,
  Ao = () => Qi.createComment(''),
  Eo = t => t === null || (typeof t != 'object' && typeof t != 'function'),
  fc = Array.isArray,
  L_ = t =>
    fc(t) || typeof (t == null ? void 0 : t[Symbol.iterator]) == 'function',
  Ml = `[ 	
\f\r]`,
  ho = /<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,
  vu = /-->/g,
  bu = />/g,
  Ki = RegExp(
    `>|${Ml}(?:([^\\s"'>=/]+)(${Ml}*=${Ml}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,
    'g',
  ),
  yu = /'/g,
  _u = /"/g,
  Pp = /^(?:script|style|textarea|title)$/i,
  D_ =
    t =>
    (r, ...i) => ({ _$litType$: t, strings: r, values: i }),
  C = D_(1),
  lr = Symbol.for('lit-noChange'),
  Gt = Symbol.for('lit-nothing'),
  wu = /* @__PURE__ */ new WeakMap(),
  Xi = Qi.createTreeWalker(Qi, 129)
function Bp(t, r) {
  if (!fc(t) || !t.hasOwnProperty('raw'))
    throw Error('invalid template strings array')
  return mu !== void 0 ? mu.createHTML(r) : r
}
const M_ = (t, r) => {
  const i = t.length - 1,
    o = []
  let a,
    d = r === 2 ? '<svg>' : r === 3 ? '<math>' : '',
    h = ho
  for (let m = 0; m < i; m++) {
    const f = t[m]
    let b,
      A,
      y = -1,
      k = 0
    for (; k < f.length && ((h.lastIndex = k), (A = h.exec(f)), A !== null); )
      (k = h.lastIndex),
        h === ho
          ? A[1] === '!--'
            ? (h = vu)
            : A[1] !== void 0
            ? (h = bu)
            : A[2] !== void 0
            ? (Pp.test(A[2]) && (a = RegExp('</' + A[2], 'g')), (h = Ki))
            : A[3] !== void 0 && (h = Ki)
          : h === Ki
          ? A[0] === '>'
            ? ((h = a ?? ho), (y = -1))
            : A[1] === void 0
            ? (y = -2)
            : ((y = h.lastIndex - A[2].length),
              (b = A[1]),
              (h = A[3] === void 0 ? Ki : A[3] === '"' ? _u : yu))
          : h === _u || h === yu
          ? (h = Ki)
          : h === vu || h === bu
          ? (h = ho)
          : ((h = Ki), (a = void 0))
    const S = h === Ki && t[m + 1].startsWith('/>') ? ' ' : ''
    d +=
      h === ho
        ? f + O_
        : y >= 0
        ? (o.push(b), f.slice(0, y) + Mp + f.slice(y) + zi + S)
        : f + zi + (y === -2 ? m : S)
  }
  return [
    Bp(
      t,
      d + (t[i] || '<?>') + (r === 2 ? '</svg>' : r === 3 ? '</math>' : ''),
    ),
    o,
  ]
}
class To {
  constructor({ strings: r, _$litType$: i }, o) {
    let a
    this.parts = []
    let d = 0,
      h = 0
    const m = r.length - 1,
      f = this.parts,
      [b, A] = M_(r, i)
    if (
      ((this.el = To.createElement(b, o)),
      (Xi.currentNode = this.el.content),
      i === 2 || i === 3)
    ) {
      const y = this.el.content.firstChild
      y.replaceWith(...y.childNodes)
    }
    for (; (a = Xi.nextNode()) !== null && f.length < m; ) {
      if (a.nodeType === 1) {
        if (a.hasAttributes())
          for (const y of a.getAttributeNames())
            if (y.endsWith(Mp)) {
              const k = A[h++],
                S = a.getAttribute(y).split(zi),
                $ = /([.?@])?(.*)/.exec(k)
              f.push({
                type: 1,
                index: d,
                name: $[2],
                strings: S,
                ctor:
                  $[1] === '.'
                    ? P_
                    : $[1] === '?'
                    ? B_
                    : $[1] === '@'
                    ? F_
                    : ea,
              }),
                a.removeAttribute(y)
            } else
              y.startsWith(zi) &&
                (f.push({ type: 6, index: d }), a.removeAttribute(y))
        if (Pp.test(a.tagName)) {
          const y = a.textContent.split(zi),
            k = y.length - 1
          if (k > 0) {
            a.textContent = Ys ? Ys.emptyScript : ''
            for (let S = 0; S < k; S++)
              a.append(y[S], Ao()),
                Xi.nextNode(),
                f.push({ type: 2, index: ++d })
            a.append(y[k], Ao())
          }
        }
      } else if (a.nodeType === 8)
        if (a.data === Rp) f.push({ type: 2, index: d })
        else {
          let y = -1
          for (; (y = a.data.indexOf(zi, y + 1)) !== -1; )
            f.push({ type: 7, index: d }), (y += zi.length - 1)
        }
      d++
    }
  }
  static createElement(r, i) {
    const o = Qi.createElement('template')
    return (o.innerHTML = r), o
  }
}
function Dn(t, r, i = t, o) {
  var h, m
  if (r === lr) return r
  let a = o !== void 0 ? ((h = i._$Co) == null ? void 0 : h[o]) : i._$Cl
  const d = Eo(r) ? void 0 : r._$litDirective$
  return (
    (a == null ? void 0 : a.constructor) !== d &&
      ((m = a == null ? void 0 : a._$AO) == null || m.call(a, !1),
      d === void 0 ? (a = void 0) : ((a = new d(t)), a._$AT(t, i, o)),
      o !== void 0 ? ((i._$Co ?? (i._$Co = []))[o] = a) : (i._$Cl = a)),
    a !== void 0 && (r = Dn(t, a._$AS(t, r.values), a, o)),
    r
  )
}
class R_ {
  constructor(r, i) {
    ;(this._$AV = []), (this._$AN = void 0), (this._$AD = r), (this._$AM = i)
  }
  get parentNode() {
    return this._$AM.parentNode
  }
  get _$AU() {
    return this._$AM._$AU
  }
  u(r) {
    const {
        el: { content: i },
        parts: o,
      } = this._$AD,
      a = ((r == null ? void 0 : r.creationScope) ?? Qi).importNode(i, !0)
    Xi.currentNode = a
    let d = Xi.nextNode(),
      h = 0,
      m = 0,
      f = o[0]
    for (; f !== void 0; ) {
      if (h === f.index) {
        let b
        f.type === 2
          ? (b = new Oo(d, d.nextSibling, this, r))
          : f.type === 1
          ? (b = new f.ctor(d, f.name, f.strings, this, r))
          : f.type === 6 && (b = new N_(d, this, r)),
          this._$AV.push(b),
          (f = o[++m])
      }
      h !== (f == null ? void 0 : f.index) && ((d = Xi.nextNode()), h++)
    }
    return (Xi.currentNode = Qi), a
  }
  p(r) {
    let i = 0
    for (const o of this._$AV)
      o !== void 0 &&
        (o.strings !== void 0
          ? (o._$AI(r, o, i), (i += o.strings.length - 2))
          : o._$AI(r[i])),
        i++
  }
}
class Oo {
  get _$AU() {
    var r
    return ((r = this._$AM) == null ? void 0 : r._$AU) ?? this._$Cv
  }
  constructor(r, i, o, a) {
    ;(this.type = 2),
      (this._$AH = Gt),
      (this._$AN = void 0),
      (this._$AA = r),
      (this._$AB = i),
      (this._$AM = o),
      (this.options = a),
      (this._$Cv = (a == null ? void 0 : a.isConnected) ?? !0)
  }
  get parentNode() {
    let r = this._$AA.parentNode
    const i = this._$AM
    return (
      i !== void 0 &&
        (r == null ? void 0 : r.nodeType) === 11 &&
        (r = i.parentNode),
      r
    )
  }
  get startNode() {
    return this._$AA
  }
  get endNode() {
    return this._$AB
  }
  _$AI(r, i = this) {
    ;(r = Dn(this, r, i)),
      Eo(r)
        ? r === Gt || r == null || r === ''
          ? (this._$AH !== Gt && this._$AR(), (this._$AH = Gt))
          : r !== this._$AH && r !== lr && this._(r)
        : r._$litType$ !== void 0
        ? this.$(r)
        : r.nodeType !== void 0
        ? this.T(r)
        : L_(r)
        ? this.k(r)
        : this._(r)
  }
  O(r) {
    return this._$AA.parentNode.insertBefore(r, this._$AB)
  }
  T(r) {
    this._$AH !== r && (this._$AR(), (this._$AH = this.O(r)))
  }
  _(r) {
    this._$AH !== Gt && Eo(this._$AH)
      ? (this._$AA.nextSibling.data = r)
      : this.T(Qi.createTextNode(r)),
      (this._$AH = r)
  }
  $(r) {
    var d
    const { values: i, _$litType$: o } = r,
      a =
        typeof o == 'number'
          ? this._$AC(r)
          : (o.el === void 0 &&
              (o.el = To.createElement(Bp(o.h, o.h[0]), this.options)),
            o)
    if (((d = this._$AH) == null ? void 0 : d._$AD) === a) this._$AH.p(i)
    else {
      const h = new R_(a, this),
        m = h.u(this.options)
      h.p(i), this.T(m), (this._$AH = h)
    }
  }
  _$AC(r) {
    let i = wu.get(r.strings)
    return i === void 0 && wu.set(r.strings, (i = new To(r))), i
  }
  k(r) {
    fc(this._$AH) || ((this._$AH = []), this._$AR())
    const i = this._$AH
    let o,
      a = 0
    for (const d of r)
      a === i.length
        ? i.push((o = new Oo(this.O(Ao()), this.O(Ao()), this, this.options)))
        : (o = i[a]),
        o._$AI(d),
        a++
    a < i.length && (this._$AR(o && o._$AB.nextSibling, a), (i.length = a))
  }
  _$AR(r = this._$AA.nextSibling, i) {
    var o
    for (
      (o = this._$AP) == null ? void 0 : o.call(this, !1, !0, i);
      r && r !== this._$AB;

    ) {
      const a = r.nextSibling
      r.remove(), (r = a)
    }
  }
  setConnected(r) {
    var i
    this._$AM === void 0 &&
      ((this._$Cv = r), (i = this._$AP) == null || i.call(this, r))
  }
}
class ea {
  get tagName() {
    return this.element.tagName
  }
  get _$AU() {
    return this._$AM._$AU
  }
  constructor(r, i, o, a, d) {
    ;(this.type = 1),
      (this._$AH = Gt),
      (this._$AN = void 0),
      (this.element = r),
      (this.name = i),
      (this._$AM = a),
      (this.options = d),
      o.length > 2 || o[0] !== '' || o[1] !== ''
        ? ((this._$AH = Array(o.length - 1).fill(new String())),
          (this.strings = o))
        : (this._$AH = Gt)
  }
  _$AI(r, i = this, o, a) {
    const d = this.strings
    let h = !1
    if (d === void 0)
      (r = Dn(this, r, i, 0)),
        (h = !Eo(r) || (r !== this._$AH && r !== lr)),
        h && (this._$AH = r)
    else {
      const m = r
      let f, b
      for (r = d[0], f = 0; f < d.length - 1; f++)
        (b = Dn(this, m[o + f], i, f)),
          b === lr && (b = this._$AH[f]),
          h || (h = !Eo(b) || b !== this._$AH[f]),
          b === Gt ? (r = Gt) : r !== Gt && (r += (b ?? '') + d[f + 1]),
          (this._$AH[f] = b)
    }
    h && !a && this.j(r)
  }
  j(r) {
    r === Gt
      ? this.element.removeAttribute(this.name)
      : this.element.setAttribute(this.name, r ?? '')
  }
}
let P_ = class extends ea {
  constructor() {
    super(...arguments), (this.type = 3)
  }
  j(r) {
    this.element[this.name] = r === Gt ? void 0 : r
  }
}
class B_ extends ea {
  constructor() {
    super(...arguments), (this.type = 4)
  }
  j(r) {
    this.element.toggleAttribute(this.name, !!r && r !== Gt)
  }
}
class F_ extends ea {
  constructor(r, i, o, a, d) {
    super(r, i, o, a, d), (this.type = 5)
  }
  _$AI(r, i = this) {
    if ((r = Dn(this, r, i, 0) ?? Gt) === lr) return
    const o = this._$AH,
      a =
        (r === Gt && o !== Gt) ||
        r.capture !== o.capture ||
        r.once !== o.once ||
        r.passive !== o.passive,
      d = r !== Gt && (o === Gt || a)
    a && this.element.removeEventListener(this.name, this, o),
      d && this.element.addEventListener(this.name, this, r),
      (this._$AH = r)
  }
  handleEvent(r) {
    var i
    typeof this._$AH == 'function'
      ? this._$AH.call(
          ((i = this.options) == null ? void 0 : i.host) ?? this.element,
          r,
        )
      : this._$AH.handleEvent(r)
  }
}
class N_ {
  constructor(r, i, o) {
    ;(this.element = r),
      (this.type = 6),
      (this._$AN = void 0),
      (this._$AM = i),
      (this.options = o)
  }
  get _$AU() {
    return this._$AM._$AU
  }
  _$AI(r) {
    Dn(this, r)
  }
}
const Rl = _o.litHtmlPolyfillSupport
Rl == null || Rl(To, Oo),
  (_o.litHtmlVersions ?? (_o.litHtmlVersions = [])).push('3.2.1')
const U_ = (t, r, i) => {
  const o = (i == null ? void 0 : i.renderBefore) ?? r
  let a = o._$litPart$
  if (a === void 0) {
    const d = (i == null ? void 0 : i.renderBefore) ?? null
    o._$litPart$ = a = new Oo(r.insertBefore(Ao(), d), d, void 0, i ?? {})
  }
  return a._$AI(t), a
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
let Ji = class extends Tn {
  constructor() {
    super(...arguments),
      (this.renderOptions = { host: this }),
      (this._$Do = void 0)
  }
  createRenderRoot() {
    var i
    const r = super.createRenderRoot()
    return (
      (i = this.renderOptions).renderBefore ?? (i.renderBefore = r.firstChild),
      r
    )
  }
  update(r) {
    const i = this.render()
    this.hasUpdated || (this.renderOptions.isConnected = this.isConnected),
      super.update(r),
      (this._$Do = U_(i, this.renderRoot, this.renderOptions))
  }
  connectedCallback() {
    var r
    super.connectedCallback(), (r = this._$Do) == null || r.setConnected(!0)
  }
  disconnectedCallback() {
    var r
    super.disconnectedCallback(), (r = this._$Do) == null || r.setConnected(!1)
  }
  render() {
    return lr
  }
}
var Op
;(Ji._$litElement$ = !0),
  (Ji.finalized = !0),
  (Op = globalThis.litElementHydrateSupport) == null ||
    Op.call(globalThis, { LitElement: Ji })
const Pl = globalThis.litElementPolyfillSupport
Pl == null || Pl({ LitElement: Ji })
;(globalThis.litElementVersions ?? (globalThis.litElementVersions = [])).push(
  '4.1.1',
)
var H_ = st`
  :host {
    display: inline-block;
    width: 1em;
    height: 1em;
    box-sizing: content-box !important;
  }

  svg {
    display: block;
    height: 100%;
    width: 100%;
  }
`,
  Fp = Object.defineProperty,
  V_ = Object.defineProperties,
  W_ = Object.getOwnPropertyDescriptor,
  q_ = Object.getOwnPropertyDescriptors,
  xu = Object.getOwnPropertySymbols,
  Y_ = Object.prototype.hasOwnProperty,
  K_ = Object.prototype.propertyIsEnumerable,
  Bl = (t, r) => ((r = Symbol[t]) ? r : Symbol.for('Symbol.' + t)),
  ku = (t, r, i) =>
    r in t
      ? Fp(t, r, { enumerable: !0, configurable: !0, writable: !0, value: i })
      : (t[r] = i),
  pi = (t, r) => {
    for (var i in r || (r = {})) Y_.call(r, i) && ku(t, i, r[i])
    if (xu) for (var i of xu(r)) K_.call(r, i) && ku(t, i, r[i])
    return t
  },
  Lo = (t, r) => V_(t, q_(r)),
  c = (t, r, i, o) => {
    for (
      var a = o > 1 ? void 0 : o ? W_(r, i) : r, d = t.length - 1, h;
      d >= 0;
      d--
    )
      (h = t[d]) && (a = (o ? h(r, i, a) : h(a)) || a)
    return o && a && Fp(r, i, a), a
  },
  Np = (t, r, i) => {
    if (!r.has(t)) throw TypeError('Cannot ' + i)
  },
  G_ = (t, r, i) => (Np(t, r, 'read from private field'), r.get(t)),
  X_ = (t, r, i) => {
    if (r.has(t))
      throw TypeError('Cannot add the same private member more than once')
    r instanceof WeakSet ? r.add(t) : r.set(t, i)
  },
  j_ = (t, r, i, o) => (Np(t, r, 'write to private field'), r.set(t, i), i),
  Z_ = function (t, r) {
    ;(this[0] = t), (this[1] = r)
  },
  J_ = t => {
    var r = t[Bl('asyncIterator')],
      i = !1,
      o,
      a = {}
    return (
      r == null
        ? ((r = t[Bl('iterator')]()), (o = d => (a[d] = h => r[d](h))))
        : ((r = r.call(t)),
          (o = d =>
            (a[d] = h => {
              if (i) {
                if (((i = !1), d === 'throw')) throw h
                return h
              }
              return (
                (i = !0),
                {
                  done: !1,
                  value: new Z_(
                    new Promise(m => {
                      var f = r[d](h)
                      if (!(f instanceof Object))
                        throw TypeError('Object expected')
                      m(f)
                    }),
                    1,
                  ),
                }
              )
            }))),
      (a[Bl('iterator')] = () => a),
      o('next'),
      'throw' in r
        ? o('throw')
        : (a.throw = d => {
            throw d
          }),
      'return' in r && o('return'),
      a
    )
  }
function X(t, r) {
  const i = pi(
    {
      waitUntilFirstUpdate: !1,
    },
    r,
  )
  return (o, a) => {
    const { update: d } = o,
      h = Array.isArray(t) ? t : [t]
    o.update = function (m) {
      h.forEach(f => {
        const b = f
        if (m.has(b)) {
          const A = m.get(b),
            y = this[b]
          A !== y &&
            (!i.waitUntilFirstUpdate || this.hasUpdated) &&
            this[a](A, y)
        }
      }),
        d.call(this, m)
    }
  }
}
var dt = st`
  :host {
    box-sizing: border-box;
  }

  :host *,
  :host *::before,
  :host *::after {
    box-sizing: inherit;
  }

  [hidden] {
    display: none !important;
  }
`
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const Q_ = {
    attribute: !0,
    type: String,
    converter: Ln,
    reflect: !1,
    hasChanged: pc,
  },
  tw = (t = Q_, r, i) => {
    const { kind: o, metadata: a } = i
    let d = globalThis.litPropertyMetadata.get(a)
    if (
      (d === void 0 &&
        globalThis.litPropertyMetadata.set(a, (d = /* @__PURE__ */ new Map())),
      d.set(i.name, t),
      o === 'accessor')
    ) {
      const { name: h } = i
      return {
        set(m) {
          const f = r.get.call(this)
          r.set.call(this, m), this.requestUpdate(h, f, t)
        },
        init(m) {
          return m !== void 0 && this.P(h, void 0, t), m
        },
      }
    }
    if (o === 'setter') {
      const { name: h } = i
      return function (m) {
        const f = this[h]
        r.call(this, m), this.requestUpdate(h, f, t)
      }
    }
    throw Error('Unsupported decorator location: ' + o)
  }
function p(t) {
  return (r, i) =>
    typeof i == 'object'
      ? tw(t, r, i)
      : ((o, a, d) => {
          const h = a.hasOwnProperty(d)
          return (
            a.constructor.createProperty(d, h ? { ...o, wrapped: !0 } : o),
            h ? Object.getOwnPropertyDescriptor(a, d) : void 0
          )
        })(t, r, i)
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function at(t) {
  return p({ ...t, state: !0, attribute: !1 })
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function Do(t) {
  return (r, i) => {
    const o = typeof r == 'function' ? r : r[i]
    Object.assign(o, t)
  }
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const Up = (t, r, i) => (
  (i.configurable = !0),
  (i.enumerable = !0),
  Reflect.decorate && typeof r != 'object' && Object.defineProperty(t, r, i),
  i
)
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function J(t, r) {
  return (i, o, a) => {
    const d = h => {
      var m
      return ((m = h.renderRoot) == null ? void 0 : m.querySelector(t)) ?? null
    }
    return Up(i, o, {
      get() {
        return d(this)
      },
    })
  }
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function ew(t) {
  return (r, i) =>
    Up(r, i, {
      async get() {
        var o
        return (
          await this.updateComplete,
          ((o = this.renderRoot) == null ? void 0 : o.querySelector(t)) ?? null
        )
      },
    })
}
var Us,
  nt = class extends Ji {
    constructor() {
      super(),
        X_(this, Us, !1),
        (this.initialReflectedProperties = /* @__PURE__ */ new Map()),
        Object.entries(this.constructor.dependencies).forEach(([t, r]) => {
          this.constructor.define(t, r)
        })
    }
    emit(t, r) {
      const i = new CustomEvent(
        t,
        pi(
          {
            bubbles: !0,
            cancelable: !1,
            composed: !0,
            detail: {},
          },
          r,
        ),
      )
      return this.dispatchEvent(i), i
    }
    /* eslint-enable */
    static define(t, r = this, i = {}) {
      const o = customElements.get(t)
      if (!o) {
        try {
          customElements.define(t, r, i)
        } catch {
          customElements.define(t, class extends r {}, i)
        }
        return
      }
      let a = ' (unknown version)',
        d = a
      'version' in r && r.version && (a = ' v' + r.version),
        'version' in o && o.version && (d = ' v' + o.version),
        !(a && d && a === d) &&
          console.warn(
            `Attempted to register <${t}>${a}, but <${t}>${d} has already been registered.`,
          )
    }
    attributeChangedCallback(t, r, i) {
      G_(this, Us) ||
        (this.constructor.elementProperties.forEach((o, a) => {
          o.reflect &&
            this[a] != null &&
            this.initialReflectedProperties.set(a, this[a])
        }),
        j_(this, Us, !0)),
        super.attributeChangedCallback(t, r, i)
    }
    willUpdate(t) {
      super.willUpdate(t),
        this.initialReflectedProperties.forEach((r, i) => {
          t.has(i) && this[i] == null && (this[i] = r)
        })
    }
  }
Us = /* @__PURE__ */ new WeakMap()
nt.version = '2.18.0'
nt.dependencies = {}
c([p()], nt.prototype, 'dir', 2)
c([p()], nt.prototype, 'lang', 2)
/**
 * @license
 * Copyright 2020 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const rw = (t, r) => (t == null ? void 0 : t._$litType$) !== void 0,
  Hp = t => t.strings === void 0,
  iw = {},
  nw = (t, r = iw) => (t._$AH = r)
var uo = Symbol(),
  Os = Symbol(),
  Fl,
  Nl = /* @__PURE__ */ new Map(),
  Nt = class extends nt {
    constructor() {
      super(...arguments),
        (this.initialRender = !1),
        (this.svg = null),
        (this.label = ''),
        (this.library = 'default')
    }
    /** Given a URL, this function returns the resulting SVG element or an appropriate error symbol. */
    async resolveIcon(t, r) {
      var i
      let o
      if (r != null && r.spriteSheet)
        return (
          (this.svg = C`<svg part="svg">
        <use part="use" href="${t}"></use>
      </svg>`),
          this.svg
        )
      try {
        if (((o = await fetch(t, { mode: 'cors' })), !o.ok))
          return o.status === 410 ? uo : Os
      } catch {
        return Os
      }
      try {
        const a = document.createElement('div')
        a.innerHTML = await o.text()
        const d = a.firstElementChild
        if (
          ((i = d == null ? void 0 : d.tagName) == null
            ? void 0
            : i.toLowerCase()) !== 'svg'
        )
          return uo
        Fl || (Fl = new DOMParser())
        const m = Fl.parseFromString(
          d.outerHTML,
          'text/html',
        ).body.querySelector('svg')
        return m ? (m.part.add('svg'), document.adoptNode(m)) : uo
      } catch {
        return uo
      }
    }
    connectedCallback() {
      super.connectedCallback(), w_(this)
    }
    firstUpdated() {
      ;(this.initialRender = !0), this.setIcon()
    }
    disconnectedCallback() {
      super.disconnectedCallback(), x_(this)
    }
    getIconSource() {
      const t = hu(this.library)
      return this.name && t
        ? {
            url: t.resolver(this.name),
            fromLibrary: !0,
          }
        : {
            url: this.src,
            fromLibrary: !1,
          }
    }
    handleLabelChange() {
      typeof this.label == 'string' && this.label.length > 0
        ? (this.setAttribute('role', 'img'),
          this.setAttribute('aria-label', this.label),
          this.removeAttribute('aria-hidden'))
        : (this.removeAttribute('role'),
          this.removeAttribute('aria-label'),
          this.setAttribute('aria-hidden', 'true'))
    }
    async setIcon() {
      var t
      const { url: r, fromLibrary: i } = this.getIconSource(),
        o = i ? hu(this.library) : void 0
      if (!r) {
        this.svg = null
        return
      }
      let a = Nl.get(r)
      if (
        (a || ((a = this.resolveIcon(r, o)), Nl.set(r, a)), !this.initialRender)
      )
        return
      const d = await a
      if ((d === Os && Nl.delete(r), r === this.getIconSource().url)) {
        if (rw(d)) {
          if (((this.svg = d), o)) {
            await this.updateComplete
            const h = this.shadowRoot.querySelector("[part='svg']")
            typeof o.mutator == 'function' && h && o.mutator(h)
          }
          return
        }
        switch (d) {
          case Os:
          case uo:
            ;(this.svg = null), this.emit('sl-error')
            break
          default:
            ;(this.svg = d.cloneNode(!0)),
              (t = o == null ? void 0 : o.mutator) == null ||
                t.call(o, this.svg),
              this.emit('sl-load')
        }
      }
    }
    render() {
      return this.svg
    }
  }
Nt.styles = [dt, H_]
c([at()], Nt.prototype, 'svg', 2)
c([p({ reflect: !0 })], Nt.prototype, 'name', 2)
c([p()], Nt.prototype, 'src', 2)
c([p()], Nt.prototype, 'label', 2)
c([p({ reflect: !0 })], Nt.prototype, 'library', 2)
c([X('label')], Nt.prototype, 'handleLabelChange', 1)
c([X(['name', 'src', 'library'])], Nt.prototype, 'setIcon', 1)
class Ks extends Error {
  constructor(i = 'Invalid value', o) {
    super(i, o)
    W(this, 'name', 'ValueError')
  }
}
var Ge =
  typeof globalThis < 'u'
    ? globalThis
    : typeof window < 'u'
    ? window
    : typeof global < 'u'
    ? global
    : typeof self < 'u'
    ? self
    : {}
function ra(t) {
  return t && t.__esModule && Object.prototype.hasOwnProperty.call(t, 'default')
    ? t.default
    : t
}
var Vp = { exports: {} }
;(function (t, r) {
  ;(function (i, o) {
    t.exports = o()
  })(Ge, function () {
    var i = 1e3,
      o = 6e4,
      a = 36e5,
      d = 'millisecond',
      h = 'second',
      m = 'minute',
      f = 'hour',
      b = 'day',
      A = 'week',
      y = 'month',
      k = 'quarter',
      S = 'year',
      $ = 'date',
      w = 'Invalid Date',
      T =
        /^(\d{4})[-/]?(\d{1,2})?[-/]?(\d{0,2})[Tt\s]*(\d{1,2})?:?(\d{1,2})?:?(\d{1,2})?[.:]?(\d+)?$/,
      P =
        /\[([^\]]+)]|Y{1,4}|M{1,4}|D{1,2}|d{1,4}|H{1,2}|h{1,2}|a|A|m{1,2}|s{1,2}|Z{1,2}|SSS/g,
      D = {
        name: 'en',
        weekdays:
          'Sunday_Monday_Tuesday_Wednesday_Thursday_Friday_Saturday'.split('_'),
        months:
          'January_February_March_April_May_June_July_August_September_October_November_December'.split(
            '_',
          ),
        ordinal: function (Z) {
          var G = ['th', 'st', 'nd', 'rd'],
            Y = Z % 100
          return '[' + Z + (G[(Y - 20) % 10] || G[Y] || G[0]) + ']'
        },
      },
      R = function (Z, G, Y) {
        var tt = String(Z)
        return !tt || tt.length >= G
          ? Z
          : '' + Array(G + 1 - tt.length).join(Y) + Z
      },
      O = {
        s: R,
        z: function (Z) {
          var G = -Z.utcOffset(),
            Y = Math.abs(G),
            tt = Math.floor(Y / 60),
            j = Y % 60
          return (G <= 0 ? '+' : '-') + R(tt, 2, '0') + ':' + R(j, 2, '0')
        },
        m: function Z(G, Y) {
          if (G.date() < Y.date()) return -Z(Y, G)
          var tt = 12 * (Y.year() - G.year()) + (Y.month() - G.month()),
            j = G.clone().add(tt, y),
            yt = Y - j < 0,
            gt = G.clone().add(tt + (yt ? -1 : 1), y)
          return +(-(tt + (Y - j) / (yt ? j - gt : gt - j)) || 0)
        },
        a: function (Z) {
          return Z < 0 ? Math.ceil(Z) || 0 : Math.floor(Z)
        },
        p: function (Z) {
          return (
            { M: y, y: S, w: A, d: b, D: $, h: f, m, s: h, ms: d, Q: k }[Z] ||
            String(Z || '')
              .toLowerCase()
              .replace(/s$/, '')
          )
        },
        u: function (Z) {
          return Z === void 0
        },
      },
      L = 'en',
      U = {}
    U[L] = D
    var H = '$isDayjsObject',
      F = function (Z) {
        return Z instanceof mt || !(!Z || !Z[H])
      },
      K = function Z(G, Y, tt) {
        var j
        if (!G) return L
        if (typeof G == 'string') {
          var yt = G.toLowerCase()
          U[yt] && (j = yt), Y && ((U[yt] = Y), (j = yt))
          var gt = G.split('-')
          if (!j && gt.length > 1) return Z(gt[0])
        } else {
          var St = G.name
          ;(U[St] = G), (j = St)
        }
        return !tt && j && (L = j), j || (!tt && L)
      },
      q = function (Z, G) {
        if (F(Z)) return Z.clone()
        var Y = typeof G == 'object' ? G : {}
        return (Y.date = Z), (Y.args = arguments), new mt(Y)
      },
      et = O
    ;(et.l = K),
      (et.i = F),
      (et.w = function (Z, G) {
        return q(Z, { locale: G.$L, utc: G.$u, x: G.$x, $offset: G.$offset })
      })
    var mt = (function () {
        function Z(Y) {
          ;(this.$L = K(Y.locale, null, !0)),
            this.parse(Y),
            (this.$x = this.$x || Y.x || {}),
            (this[H] = !0)
        }
        var G = Z.prototype
        return (
          (G.parse = function (Y) {
            ;(this.$d = (function (tt) {
              var j = tt.date,
                yt = tt.utc
              if (j === null) return /* @__PURE__ */ new Date(NaN)
              if (et.u(j)) return /* @__PURE__ */ new Date()
              if (j instanceof Date) return new Date(j)
              if (typeof j == 'string' && !/Z$/i.test(j)) {
                var gt = j.match(T)
                if (gt) {
                  var St = gt[2] - 1 || 0,
                    jt = (gt[7] || '0').substring(0, 3)
                  return yt
                    ? new Date(
                        Date.UTC(
                          gt[1],
                          St,
                          gt[3] || 1,
                          gt[4] || 0,
                          gt[5] || 0,
                          gt[6] || 0,
                          jt,
                        ),
                      )
                    : new Date(
                        gt[1],
                        St,
                        gt[3] || 1,
                        gt[4] || 0,
                        gt[5] || 0,
                        gt[6] || 0,
                        jt,
                      )
                }
              }
              return new Date(j)
            })(Y)),
              this.init()
          }),
          (G.init = function () {
            var Y = this.$d
            ;(this.$y = Y.getFullYear()),
              (this.$M = Y.getMonth()),
              (this.$D = Y.getDate()),
              (this.$W = Y.getDay()),
              (this.$H = Y.getHours()),
              (this.$m = Y.getMinutes()),
              (this.$s = Y.getSeconds()),
              (this.$ms = Y.getMilliseconds())
          }),
          (G.$utils = function () {
            return et
          }),
          (G.isValid = function () {
            return this.$d.toString() !== w
          }),
          (G.isSame = function (Y, tt) {
            var j = q(Y)
            return this.startOf(tt) <= j && j <= this.endOf(tt)
          }),
          (G.isAfter = function (Y, tt) {
            return q(Y) < this.startOf(tt)
          }),
          (G.isBefore = function (Y, tt) {
            return this.endOf(tt) < q(Y)
          }),
          (G.$g = function (Y, tt, j) {
            return et.u(Y) ? this[tt] : this.set(j, Y)
          }),
          (G.unix = function () {
            return Math.floor(this.valueOf() / 1e3)
          }),
          (G.valueOf = function () {
            return this.$d.getTime()
          }),
          (G.startOf = function (Y, tt) {
            var j = this,
              yt = !!et.u(tt) || tt,
              gt = et.p(Y),
              St = function (Je, Te) {
                var Qe = et.w(
                  j.$u ? Date.UTC(j.$y, Te, Je) : new Date(j.$y, Te, Je),
                  j,
                )
                return yt ? Qe : Qe.endOf(b)
              },
              jt = function (Je, Te) {
                return et.w(
                  j
                    .toDate()
                    [Je].apply(
                      j.toDate('s'),
                      (yt ? [0, 0, 0, 0] : [23, 59, 59, 999]).slice(Te),
                    ),
                  j,
                )
              },
              he = this.$W,
              we = this.$M,
              me = this.$D,
              Cr = 'set' + (this.$u ? 'UTC' : '')
            switch (gt) {
              case S:
                return yt ? St(1, 0) : St(31, 11)
              case y:
                return yt ? St(1, we) : St(0, we + 1)
              case A:
                var ti = this.$locale().weekStart || 0,
                  Sr = (he < ti ? he + 7 : he) - ti
                return St(yt ? me - Sr : me + (6 - Sr), we)
              case b:
              case $:
                return jt(Cr + 'Hours', 0)
              case f:
                return jt(Cr + 'Minutes', 1)
              case m:
                return jt(Cr + 'Seconds', 2)
              case h:
                return jt(Cr + 'Milliseconds', 3)
              default:
                return this.clone()
            }
          }),
          (G.endOf = function (Y) {
            return this.startOf(Y, !1)
          }),
          (G.$set = function (Y, tt) {
            var j,
              yt = et.p(Y),
              gt = 'set' + (this.$u ? 'UTC' : ''),
              St = ((j = {}),
              (j[b] = gt + 'Date'),
              (j[$] = gt + 'Date'),
              (j[y] = gt + 'Month'),
              (j[S] = gt + 'FullYear'),
              (j[f] = gt + 'Hours'),
              (j[m] = gt + 'Minutes'),
              (j[h] = gt + 'Seconds'),
              (j[d] = gt + 'Milliseconds'),
              j)[yt],
              jt = yt === b ? this.$D + (tt - this.$W) : tt
            if (yt === y || yt === S) {
              var he = this.clone().set($, 1)
              he.$d[St](jt),
                he.init(),
                (this.$d = he.set($, Math.min(this.$D, he.daysInMonth())).$d)
            } else St && this.$d[St](jt)
            return this.init(), this
          }),
          (G.set = function (Y, tt) {
            return this.clone().$set(Y, tt)
          }),
          (G.get = function (Y) {
            return this[et.p(Y)]()
          }),
          (G.add = function (Y, tt) {
            var j,
              yt = this
            Y = Number(Y)
            var gt = et.p(tt),
              St = function (we) {
                var me = q(yt)
                return et.w(me.date(me.date() + Math.round(we * Y)), yt)
              }
            if (gt === y) return this.set(y, this.$M + Y)
            if (gt === S) return this.set(S, this.$y + Y)
            if (gt === b) return St(1)
            if (gt === A) return St(7)
            var jt = ((j = {}), (j[m] = o), (j[f] = a), (j[h] = i), j)[gt] || 1,
              he = this.$d.getTime() + Y * jt
            return et.w(he, this)
          }),
          (G.subtract = function (Y, tt) {
            return this.add(-1 * Y, tt)
          }),
          (G.format = function (Y) {
            var tt = this,
              j = this.$locale()
            if (!this.isValid()) return j.invalidDate || w
            var yt = Y || 'YYYY-MM-DDTHH:mm:ssZ',
              gt = et.z(this),
              St = this.$H,
              jt = this.$m,
              he = this.$M,
              we = j.weekdays,
              me = j.months,
              Cr = j.meridiem,
              ti = function (Te, Qe, Nr, Pi) {
                return (Te && (Te[Qe] || Te(tt, yt))) || Nr[Qe].slice(0, Pi)
              },
              Sr = function (Te) {
                return et.s(St % 12 || 12, Te, '0')
              },
              Je =
                Cr ||
                function (Te, Qe, Nr) {
                  var Pi = Te < 12 ? 'AM' : 'PM'
                  return Nr ? Pi.toLowerCase() : Pi
                }
            return yt.replace(P, function (Te, Qe) {
              return (
                Qe ||
                (function (Nr) {
                  switch (Nr) {
                    case 'YY':
                      return String(tt.$y).slice(-2)
                    case 'YYYY':
                      return et.s(tt.$y, 4, '0')
                    case 'M':
                      return he + 1
                    case 'MM':
                      return et.s(he + 1, 2, '0')
                    case 'MMM':
                      return ti(j.monthsShort, he, me, 3)
                    case 'MMMM':
                      return ti(me, he)
                    case 'D':
                      return tt.$D
                    case 'DD':
                      return et.s(tt.$D, 2, '0')
                    case 'd':
                      return String(tt.$W)
                    case 'dd':
                      return ti(j.weekdaysMin, tt.$W, we, 2)
                    case 'ddd':
                      return ti(j.weekdaysShort, tt.$W, we, 3)
                    case 'dddd':
                      return we[tt.$W]
                    case 'H':
                      return String(St)
                    case 'HH':
                      return et.s(St, 2, '0')
                    case 'h':
                      return Sr(1)
                    case 'hh':
                      return Sr(2)
                    case 'a':
                      return Je(St, jt, !0)
                    case 'A':
                      return Je(St, jt, !1)
                    case 'm':
                      return String(jt)
                    case 'mm':
                      return et.s(jt, 2, '0')
                    case 's':
                      return String(tt.$s)
                    case 'ss':
                      return et.s(tt.$s, 2, '0')
                    case 'SSS':
                      return et.s(tt.$ms, 3, '0')
                    case 'Z':
                      return gt
                  }
                  return null
                })(Te) ||
                gt.replace(':', '')
              )
            })
          }),
          (G.utcOffset = function () {
            return 15 * -Math.round(this.$d.getTimezoneOffset() / 15)
          }),
          (G.diff = function (Y, tt, j) {
            var yt,
              gt = this,
              St = et.p(tt),
              jt = q(Y),
              he = (jt.utcOffset() - this.utcOffset()) * o,
              we = this - jt,
              me = function () {
                return et.m(gt, jt)
              }
            switch (St) {
              case S:
                yt = me() / 12
                break
              case y:
                yt = me()
                break
              case k:
                yt = me() / 3
                break
              case A:
                yt = (we - he) / 6048e5
                break
              case b:
                yt = (we - he) / 864e5
                break
              case f:
                yt = we / a
                break
              case m:
                yt = we / o
                break
              case h:
                yt = we / i
                break
              default:
                yt = we
            }
            return j ? yt : et.a(yt)
          }),
          (G.daysInMonth = function () {
            return this.endOf(y).$D
          }),
          (G.$locale = function () {
            return U[this.$L]
          }),
          (G.locale = function (Y, tt) {
            if (!Y) return this.$L
            var j = this.clone(),
              yt = K(Y, tt, !0)
            return yt && (j.$L = yt), j
          }),
          (G.clone = function () {
            return et.w(this.$d, this)
          }),
          (G.toDate = function () {
            return new Date(this.valueOf())
          }),
          (G.toJSON = function () {
            return this.isValid() ? this.toISOString() : null
          }),
          (G.toISOString = function () {
            return this.$d.toISOString()
          }),
          (G.toString = function () {
            return this.$d.toUTCString()
          }),
          Z
        )
      })(),
      Ot = mt.prototype
    return (
      (q.prototype = Ot),
      [
        ['$ms', d],
        ['$s', h],
        ['$m', m],
        ['$H', f],
        ['$W', b],
        ['$M', y],
        ['$y', S],
        ['$D', $],
      ].forEach(function (Z) {
        Ot[Z[1]] = function (G) {
          return this.$g(G, Z[0], Z[1])
        }
      }),
      (q.extend = function (Z, G) {
        return Z.$i || (Z(G, mt, q), (Z.$i = !0)), q
      }),
      (q.locale = K),
      (q.isDayjs = F),
      (q.unix = function (Z) {
        return q(1e3 * Z)
      }),
      (q.en = U[L]),
      (q.Ls = U),
      (q.p = {}),
      q
    )
  })
})(Vp)
var ow = Vp.exports
const Mo = /* @__PURE__ */ ra(ow)
var Wp = { exports: {} }
;(function (t, r) {
  ;(function (i, o) {
    t.exports = o()
  })(Ge, function () {
    var i = 'minute',
      o = /[+-]\d\d(?::?\d\d)?/g,
      a = /([+-]|\d\d)/g
    return function (d, h, m) {
      var f = h.prototype
      ;(m.utc = function (w) {
        var T = { date: w, utc: !0, args: arguments }
        return new h(T)
      }),
        (f.utc = function (w) {
          var T = m(this.toDate(), { locale: this.$L, utc: !0 })
          return w ? T.add(this.utcOffset(), i) : T
        }),
        (f.local = function () {
          return m(this.toDate(), { locale: this.$L, utc: !1 })
        })
      var b = f.parse
      f.parse = function (w) {
        w.utc && (this.$u = !0),
          this.$utils().u(w.$offset) || (this.$offset = w.$offset),
          b.call(this, w)
      }
      var A = f.init
      f.init = function () {
        if (this.$u) {
          var w = this.$d
          ;(this.$y = w.getUTCFullYear()),
            (this.$M = w.getUTCMonth()),
            (this.$D = w.getUTCDate()),
            (this.$W = w.getUTCDay()),
            (this.$H = w.getUTCHours()),
            (this.$m = w.getUTCMinutes()),
            (this.$s = w.getUTCSeconds()),
            (this.$ms = w.getUTCMilliseconds())
        } else A.call(this)
      }
      var y = f.utcOffset
      f.utcOffset = function (w, T) {
        var P = this.$utils().u
        if (P(w))
          return this.$u ? 0 : P(this.$offset) ? y.call(this) : this.$offset
        if (
          typeof w == 'string' &&
          ((w = (function (L) {
            L === void 0 && (L = '')
            var U = L.match(o)
            if (!U) return null
            var H = ('' + U[0]).match(a) || ['-', 0, 0],
              F = H[0],
              K = 60 * +H[1] + +H[2]
            return K === 0 ? 0 : F === '+' ? K : -K
          })(w)),
          w === null)
        )
          return this
        var D = Math.abs(w) <= 16 ? 60 * w : w,
          R = this
        if (T) return (R.$offset = D), (R.$u = w === 0), R
        if (w !== 0) {
          var O = this.$u
            ? this.toDate().getTimezoneOffset()
            : -1 * this.utcOffset()
          ;((R = this.local().add(D + O, i)).$offset = D),
            (R.$x.$localOffset = O)
        } else R = this.utc()
        return R
      }
      var k = f.format
      ;(f.format = function (w) {
        var T = w || (this.$u ? 'YYYY-MM-DDTHH:mm:ss[Z]' : '')
        return k.call(this, T)
      }),
        (f.valueOf = function () {
          var w = this.$utils().u(this.$offset)
            ? 0
            : this.$offset +
              (this.$x.$localOffset || this.$d.getTimezoneOffset())
          return this.$d.valueOf() - 6e4 * w
        }),
        (f.isUTC = function () {
          return !!this.$u
        }),
        (f.toISOString = function () {
          return this.toDate().toISOString()
        }),
        (f.toString = function () {
          return this.toDate().toUTCString()
        })
      var S = f.toDate
      f.toDate = function (w) {
        return w === 's' && this.$offset
          ? m(this.format('YYYY-MM-DD HH:mm:ss:SSS')).toDate()
          : S.call(this)
      }
      var $ = f.diff
      f.diff = function (w, T, P) {
        if (w && this.$u === w.$u) return $.call(this, w, T, P)
        var D = this.local(),
          R = m(w).local()
        return $.call(D, R, T, P)
      }
    }
  })
})(Wp)
var sw = Wp.exports
const aw = /* @__PURE__ */ ra(sw)
var qp = { exports: {} }
;(function (t, r) {
  ;(function (i, o) {
    t.exports = o()
  })(Ge, function () {
    var i,
      o,
      a = 1e3,
      d = 6e4,
      h = 36e5,
      m = 864e5,
      f =
        /\[([^\]]+)]|Y{1,4}|M{1,4}|D{1,2}|d{1,4}|H{1,2}|h{1,2}|a|A|m{1,2}|s{1,2}|Z{1,2}|SSS/g,
      b = 31536e6,
      A = 2628e6,
      y =
        /^(-|\+)?P(?:([-+]?[0-9,.]*)Y)?(?:([-+]?[0-9,.]*)M)?(?:([-+]?[0-9,.]*)W)?(?:([-+]?[0-9,.]*)D)?(?:T(?:([-+]?[0-9,.]*)H)?(?:([-+]?[0-9,.]*)M)?(?:([-+]?[0-9,.]*)S)?)?$/,
      k = {
        years: b,
        months: A,
        days: m,
        hours: h,
        minutes: d,
        seconds: a,
        milliseconds: 1,
        weeks: 6048e5,
      },
      S = function (U) {
        return U instanceof O
      },
      $ = function (U, H, F) {
        return new O(U, F, H.$l)
      },
      w = function (U) {
        return o.p(U) + 's'
      },
      T = function (U) {
        return U < 0
      },
      P = function (U) {
        return T(U) ? Math.ceil(U) : Math.floor(U)
      },
      D = function (U) {
        return Math.abs(U)
      },
      R = function (U, H) {
        return U
          ? T(U)
            ? { negative: !0, format: '' + D(U) + H }
            : { negative: !1, format: '' + U + H }
          : { negative: !1, format: '' }
      },
      O = (function () {
        function U(F, K, q) {
          var et = this
          if (
            ((this.$d = {}),
            (this.$l = q),
            F === void 0 && ((this.$ms = 0), this.parseFromMilliseconds()),
            K)
          )
            return $(F * k[w(K)], this)
          if (typeof F == 'number')
            return (this.$ms = F), this.parseFromMilliseconds(), this
          if (typeof F == 'object')
            return (
              Object.keys(F).forEach(function (Z) {
                et.$d[w(Z)] = F[Z]
              }),
              this.calMilliseconds(),
              this
            )
          if (typeof F == 'string') {
            var mt = F.match(y)
            if (mt) {
              var Ot = mt.slice(2).map(function (Z) {
                return Z != null ? Number(Z) : 0
              })
              return (
                (this.$d.years = Ot[0]),
                (this.$d.months = Ot[1]),
                (this.$d.weeks = Ot[2]),
                (this.$d.days = Ot[3]),
                (this.$d.hours = Ot[4]),
                (this.$d.minutes = Ot[5]),
                (this.$d.seconds = Ot[6]),
                this.calMilliseconds(),
                this
              )
            }
          }
          return this
        }
        var H = U.prototype
        return (
          (H.calMilliseconds = function () {
            var F = this
            this.$ms = Object.keys(this.$d).reduce(function (K, q) {
              return K + (F.$d[q] || 0) * k[q]
            }, 0)
          }),
          (H.parseFromMilliseconds = function () {
            var F = this.$ms
            ;(this.$d.years = P(F / b)),
              (F %= b),
              (this.$d.months = P(F / A)),
              (F %= A),
              (this.$d.days = P(F / m)),
              (F %= m),
              (this.$d.hours = P(F / h)),
              (F %= h),
              (this.$d.minutes = P(F / d)),
              (F %= d),
              (this.$d.seconds = P(F / a)),
              (F %= a),
              (this.$d.milliseconds = F)
          }),
          (H.toISOString = function () {
            var F = R(this.$d.years, 'Y'),
              K = R(this.$d.months, 'M'),
              q = +this.$d.days || 0
            this.$d.weeks && (q += 7 * this.$d.weeks)
            var et = R(q, 'D'),
              mt = R(this.$d.hours, 'H'),
              Ot = R(this.$d.minutes, 'M'),
              Z = this.$d.seconds || 0
            this.$d.milliseconds &&
              ((Z += this.$d.milliseconds / 1e3),
              (Z = Math.round(1e3 * Z) / 1e3))
            var G = R(Z, 'S'),
              Y =
                F.negative ||
                K.negative ||
                et.negative ||
                mt.negative ||
                Ot.negative ||
                G.negative,
              tt = mt.format || Ot.format || G.format ? 'T' : '',
              j =
                (Y ? '-' : '') +
                'P' +
                F.format +
                K.format +
                et.format +
                tt +
                mt.format +
                Ot.format +
                G.format
            return j === 'P' || j === '-P' ? 'P0D' : j
          }),
          (H.toJSON = function () {
            return this.toISOString()
          }),
          (H.format = function (F) {
            var K = F || 'YYYY-MM-DDTHH:mm:ss',
              q = {
                Y: this.$d.years,
                YY: o.s(this.$d.years, 2, '0'),
                YYYY: o.s(this.$d.years, 4, '0'),
                M: this.$d.months,
                MM: o.s(this.$d.months, 2, '0'),
                D: this.$d.days,
                DD: o.s(this.$d.days, 2, '0'),
                H: this.$d.hours,
                HH: o.s(this.$d.hours, 2, '0'),
                m: this.$d.minutes,
                mm: o.s(this.$d.minutes, 2, '0'),
                s: this.$d.seconds,
                ss: o.s(this.$d.seconds, 2, '0'),
                SSS: o.s(this.$d.milliseconds, 3, '0'),
              }
            return K.replace(f, function (et, mt) {
              return mt || String(q[et])
            })
          }),
          (H.as = function (F) {
            return this.$ms / k[w(F)]
          }),
          (H.get = function (F) {
            var K = this.$ms,
              q = w(F)
            return (
              q === 'milliseconds'
                ? (K %= 1e3)
                : (K = q === 'weeks' ? P(K / k[q]) : this.$d[q]),
              K || 0
            )
          }),
          (H.add = function (F, K, q) {
            var et
            return (
              (et = K ? F * k[w(K)] : S(F) ? F.$ms : $(F, this).$ms),
              $(this.$ms + et * (q ? -1 : 1), this)
            )
          }),
          (H.subtract = function (F, K) {
            return this.add(F, K, !0)
          }),
          (H.locale = function (F) {
            var K = this.clone()
            return (K.$l = F), K
          }),
          (H.clone = function () {
            return $(this.$ms, this)
          }),
          (H.humanize = function (F) {
            return i().add(this.$ms, 'ms').locale(this.$l).fromNow(!F)
          }),
          (H.valueOf = function () {
            return this.asMilliseconds()
          }),
          (H.milliseconds = function () {
            return this.get('milliseconds')
          }),
          (H.asMilliseconds = function () {
            return this.as('milliseconds')
          }),
          (H.seconds = function () {
            return this.get('seconds')
          }),
          (H.asSeconds = function () {
            return this.as('seconds')
          }),
          (H.minutes = function () {
            return this.get('minutes')
          }),
          (H.asMinutes = function () {
            return this.as('minutes')
          }),
          (H.hours = function () {
            return this.get('hours')
          }),
          (H.asHours = function () {
            return this.as('hours')
          }),
          (H.days = function () {
            return this.get('days')
          }),
          (H.asDays = function () {
            return this.as('days')
          }),
          (H.weeks = function () {
            return this.get('weeks')
          }),
          (H.asWeeks = function () {
            return this.as('weeks')
          }),
          (H.months = function () {
            return this.get('months')
          }),
          (H.asMonths = function () {
            return this.as('months')
          }),
          (H.years = function () {
            return this.get('years')
          }),
          (H.asYears = function () {
            return this.as('years')
          }),
          U
        )
      })(),
      L = function (U, H, F) {
        return U.add(H.years() * F, 'y')
          .add(H.months() * F, 'M')
          .add(H.days() * F, 'd')
          .add(H.hours() * F, 'h')
          .add(H.minutes() * F, 'm')
          .add(H.seconds() * F, 's')
          .add(H.milliseconds() * F, 'ms')
      }
    return function (U, H, F) {
      ;(i = F),
        (o = F().$utils()),
        (F.duration = function (et, mt) {
          var Ot = F.locale()
          return $(et, { $l: Ot }, mt)
        }),
        (F.isDuration = S)
      var K = H.prototype.add,
        q = H.prototype.subtract
      ;(H.prototype.add = function (et, mt) {
        return S(et) ? L(this, et, 1) : K.bind(this)(et, mt)
      }),
        (H.prototype.subtract = function (et, mt) {
          return S(et) ? L(this, et, -1) : q.bind(this)(et, mt)
        })
    }
  })
})(qp)
var lw = qp.exports
const cw = /* @__PURE__ */ ra(lw)
var Yp = { exports: {} }
;(function (t, r) {
  ;(function (i, o) {
    t.exports = o()
  })(Ge, function () {
    return function (i, o, a) {
      i = i || {}
      var d = o.prototype,
        h = {
          future: 'in %s',
          past: '%s ago',
          s: 'a few seconds',
          m: 'a minute',
          mm: '%d minutes',
          h: 'an hour',
          hh: '%d hours',
          d: 'a day',
          dd: '%d days',
          M: 'a month',
          MM: '%d months',
          y: 'a year',
          yy: '%d years',
        }
      function m(b, A, y, k) {
        return d.fromToBase(b, A, y, k)
      }
      ;(a.en.relativeTime = h),
        (d.fromToBase = function (b, A, y, k, S) {
          for (
            var $,
              w,
              T,
              P = y.$locale().relativeTime || h,
              D = i.thresholds || [
                { l: 's', r: 44, d: 'second' },
                { l: 'm', r: 89 },
                { l: 'mm', r: 44, d: 'minute' },
                { l: 'h', r: 89 },
                { l: 'hh', r: 21, d: 'hour' },
                { l: 'd', r: 35 },
                { l: 'dd', r: 25, d: 'day' },
                { l: 'M', r: 45 },
                { l: 'MM', r: 10, d: 'month' },
                { l: 'y', r: 17 },
                { l: 'yy', d: 'year' },
              ],
              R = D.length,
              O = 0;
            O < R;
            O += 1
          ) {
            var L = D[O]
            L.d && ($ = k ? a(b).diff(y, L.d, !0) : y.diff(b, L.d, !0))
            var U = (i.rounding || Math.round)(Math.abs($))
            if (((T = $ > 0), U <= L.r || !L.r)) {
              U <= 1 && O > 0 && (L = D[O - 1])
              var H = P[L.l]
              S && (U = S('' + U)),
                (w =
                  typeof H == 'string' ? H.replace('%d', U) : H(U, A, L.l, T))
              break
            }
          }
          if (A) return w
          var F = T ? P.future : P.past
          return typeof F == 'function' ? F(w) : F.replace('%s', w)
        }),
        (d.to = function (b, A) {
          return m(b, A, this, !0)
        }),
        (d.from = function (b, A) {
          return m(b, A, this)
        })
      var f = function (b) {
        return b.$u ? a.utc() : a()
      }
      ;(d.toNow = function (b) {
        return this.to(f(this), b)
      }),
        (d.fromNow = function (b) {
          return this.from(f(this), b)
        })
    }
  })
})(Yp)
var dw = Yp.exports
const hw = /* @__PURE__ */ ra(dw)
function uw(t) {
  throw new Error(
    'Could not dynamically require "' +
      t +
      '". Please configure the dynamicRequireTargets or/and ignoreDynamicRequires option of @rollup/plugin-commonjs appropriately for this require call to work.',
  )
}
var pw = { exports: {} }
;(function (t, r) {
  ;(function (i, o) {
    typeof uw == 'function' ? (t.exports = o()) : (i.pluralize = o())
  })(Ge, function () {
    var i = [],
      o = [],
      a = {},
      d = {},
      h = {}
    function m(w) {
      return typeof w == 'string' ? new RegExp('^' + w + '$', 'i') : w
    }
    function f(w, T) {
      return w === T
        ? T
        : w === w.toLowerCase()
        ? T.toLowerCase()
        : w === w.toUpperCase()
        ? T.toUpperCase()
        : w[0] === w[0].toUpperCase()
        ? T.charAt(0).toUpperCase() + T.substr(1).toLowerCase()
        : T.toLowerCase()
    }
    function b(w, T) {
      return w.replace(/\$(\d{1,2})/g, function (P, D) {
        return T[D] || ''
      })
    }
    function A(w, T) {
      return w.replace(T[0], function (P, D) {
        var R = b(T[1], arguments)
        return f(P === '' ? w[D - 1] : P, R)
      })
    }
    function y(w, T, P) {
      if (!w.length || a.hasOwnProperty(w)) return T
      for (var D = P.length; D--; ) {
        var R = P[D]
        if (R[0].test(T)) return A(T, R)
      }
      return T
    }
    function k(w, T, P) {
      return function (D) {
        var R = D.toLowerCase()
        return T.hasOwnProperty(R)
          ? f(D, R)
          : w.hasOwnProperty(R)
          ? f(D, w[R])
          : y(R, D, P)
      }
    }
    function S(w, T, P, D) {
      return function (R) {
        var O = R.toLowerCase()
        return T.hasOwnProperty(O)
          ? !0
          : w.hasOwnProperty(O)
          ? !1
          : y(O, O, P) === O
      }
    }
    function $(w, T, P) {
      var D = T === 1 ? $.singular(w) : $.plural(w)
      return (P ? T + ' ' : '') + D
    }
    return (
      ($.plural = k(h, d, i)),
      ($.isPlural = S(h, d, i)),
      ($.singular = k(d, h, o)),
      ($.isSingular = S(d, h, o)),
      ($.addPluralRule = function (w, T) {
        i.push([m(w), T])
      }),
      ($.addSingularRule = function (w, T) {
        o.push([m(w), T])
      }),
      ($.addUncountableRule = function (w) {
        if (typeof w == 'string') {
          a[w.toLowerCase()] = !0
          return
        }
        $.addPluralRule(w, '$0'), $.addSingularRule(w, '$0')
      }),
      ($.addIrregularRule = function (w, T) {
        ;(T = T.toLowerCase()), (w = w.toLowerCase()), (h[w] = T), (d[T] = w)
      }),
      [
        // Pronouns.
        ['I', 'we'],
        ['me', 'us'],
        ['he', 'they'],
        ['she', 'they'],
        ['them', 'them'],
        ['myself', 'ourselves'],
        ['yourself', 'yourselves'],
        ['itself', 'themselves'],
        ['herself', 'themselves'],
        ['himself', 'themselves'],
        ['themself', 'themselves'],
        ['is', 'are'],
        ['was', 'were'],
        ['has', 'have'],
        ['this', 'these'],
        ['that', 'those'],
        // Words ending in with a consonant and `o`.
        ['echo', 'echoes'],
        ['dingo', 'dingoes'],
        ['volcano', 'volcanoes'],
        ['tornado', 'tornadoes'],
        ['torpedo', 'torpedoes'],
        // Ends with `us`.
        ['genus', 'genera'],
        ['viscus', 'viscera'],
        // Ends with `ma`.
        ['stigma', 'stigmata'],
        ['stoma', 'stomata'],
        ['dogma', 'dogmata'],
        ['lemma', 'lemmata'],
        ['schema', 'schemata'],
        ['anathema', 'anathemata'],
        // Other irregular rules.
        ['ox', 'oxen'],
        ['axe', 'axes'],
        ['die', 'dice'],
        ['yes', 'yeses'],
        ['foot', 'feet'],
        ['eave', 'eaves'],
        ['goose', 'geese'],
        ['tooth', 'teeth'],
        ['quiz', 'quizzes'],
        ['human', 'humans'],
        ['proof', 'proofs'],
        ['carve', 'carves'],
        ['valve', 'valves'],
        ['looey', 'looies'],
        ['thief', 'thieves'],
        ['groove', 'grooves'],
        ['pickaxe', 'pickaxes'],
        ['passerby', 'passersby'],
      ].forEach(function (w) {
        return $.addIrregularRule(w[0], w[1])
      }),
      [
        [/s?$/i, 's'],
        [/[^\u0000-\u007F]$/i, '$0'],
        [/([^aeiou]ese)$/i, '$1'],
        [/(ax|test)is$/i, '$1es'],
        [/(alias|[^aou]us|t[lm]as|gas|ris)$/i, '$1es'],
        [/(e[mn]u)s?$/i, '$1s'],
        [/([^l]ias|[aeiou]las|[ejzr]as|[iu]am)$/i, '$1'],
        [
          /(alumn|syllab|vir|radi|nucle|fung|cact|stimul|termin|bacill|foc|uter|loc|strat)(?:us|i)$/i,
          '$1i',
        ],
        [/(alumn|alg|vertebr)(?:a|ae)$/i, '$1ae'],
        [/(seraph|cherub)(?:im)?$/i, '$1im'],
        [/(her|at|gr)o$/i, '$1oes'],
        [
          /(agend|addend|millenni|dat|extrem|bacteri|desiderat|strat|candelabr|errat|ov|symposi|curricul|automat|quor)(?:a|um)$/i,
          '$1a',
        ],
        [
          /(apheli|hyperbat|periheli|asyndet|noumen|phenomen|criteri|organ|prolegomen|hedr|automat)(?:a|on)$/i,
          '$1a',
        ],
        [/sis$/i, 'ses'],
        [/(?:(kni|wi|li)fe|(ar|l|ea|eo|oa|hoo)f)$/i, '$1$2ves'],
        [/([^aeiouy]|qu)y$/i, '$1ies'],
        [/([^ch][ieo][ln])ey$/i, '$1ies'],
        [/(x|ch|ss|sh|zz)$/i, '$1es'],
        [/(matr|cod|mur|sil|vert|ind|append)(?:ix|ex)$/i, '$1ices'],
        [/\b((?:tit)?m|l)(?:ice|ouse)$/i, '$1ice'],
        [/(pe)(?:rson|ople)$/i, '$1ople'],
        [/(child)(?:ren)?$/i, '$1ren'],
        [/eaux$/i, '$0'],
        [/m[ae]n$/i, 'men'],
        ['thou', 'you'],
      ].forEach(function (w) {
        return $.addPluralRule(w[0], w[1])
      }),
      [
        [/s$/i, ''],
        [/(ss)$/i, '$1'],
        [
          /(wi|kni|(?:after|half|high|low|mid|non|night|[^\w]|^)li)ves$/i,
          '$1fe',
        ],
        [/(ar|(?:wo|[ae])l|[eo][ao])ves$/i, '$1f'],
        [/ies$/i, 'y'],
        [
          /\b([pl]|zomb|(?:neck|cross)?t|coll|faer|food|gen|goon|group|lass|talk|goal|cut)ies$/i,
          '$1ie',
        ],
        [/\b(mon|smil)ies$/i, '$1ey'],
        [/\b((?:tit)?m|l)ice$/i, '$1ouse'],
        [/(seraph|cherub)im$/i, '$1'],
        [
          /(x|ch|ss|sh|zz|tto|go|cho|alias|[^aou]us|t[lm]as|gas|(?:her|at|gr)o|[aeiou]ris)(?:es)?$/i,
          '$1',
        ],
        [
          /(analy|diagno|parenthe|progno|synop|the|empha|cri|ne)(?:sis|ses)$/i,
          '$1sis',
        ],
        [/(movie|twelve|abuse|e[mn]u)s$/i, '$1'],
        [/(test)(?:is|es)$/i, '$1is'],
        [
          /(alumn|syllab|vir|radi|nucle|fung|cact|stimul|termin|bacill|foc|uter|loc|strat)(?:us|i)$/i,
          '$1us',
        ],
        [
          /(agend|addend|millenni|dat|extrem|bacteri|desiderat|strat|candelabr|errat|ov|symposi|curricul|quor)a$/i,
          '$1um',
        ],
        [
          /(apheli|hyperbat|periheli|asyndet|noumen|phenomen|criteri|organ|prolegomen|hedr|automat)a$/i,
          '$1on',
        ],
        [/(alumn|alg|vertebr)ae$/i, '$1a'],
        [/(cod|mur|sil|vert|ind)ices$/i, '$1ex'],
        [/(matr|append)ices$/i, '$1ix'],
        [/(pe)(rson|ople)$/i, '$1rson'],
        [/(child)ren$/i, '$1'],
        [/(eau)x?$/i, '$1'],
        [/men$/i, 'man'],
      ].forEach(function (w) {
        return $.addSingularRule(w[0], w[1])
      }),
      [
        // Singular words with no plurals.
        'adulthood',
        'advice',
        'agenda',
        'aid',
        'aircraft',
        'alcohol',
        'ammo',
        'analytics',
        'anime',
        'athletics',
        'audio',
        'bison',
        'blood',
        'bream',
        'buffalo',
        'butter',
        'carp',
        'cash',
        'chassis',
        'chess',
        'clothing',
        'cod',
        'commerce',
        'cooperation',
        'corps',
        'debris',
        'diabetes',
        'digestion',
        'elk',
        'energy',
        'equipment',
        'excretion',
        'expertise',
        'firmware',
        'flounder',
        'fun',
        'gallows',
        'garbage',
        'graffiti',
        'hardware',
        'headquarters',
        'health',
        'herpes',
        'highjinks',
        'homework',
        'housework',
        'information',
        'jeans',
        'justice',
        'kudos',
        'labour',
        'literature',
        'machinery',
        'mackerel',
        'mail',
        'media',
        'mews',
        'moose',
        'music',
        'mud',
        'manga',
        'news',
        'only',
        'personnel',
        'pike',
        'plankton',
        'pliers',
        'police',
        'pollution',
        'premises',
        'rain',
        'research',
        'rice',
        'salmon',
        'scissors',
        'series',
        'sewage',
        'shambles',
        'shrimp',
        'software',
        'species',
        'staff',
        'swine',
        'tennis',
        'traffic',
        'transportation',
        'trout',
        'tuna',
        'wealth',
        'welfare',
        'whiting',
        'wildebeest',
        'wildlife',
        'you',
        /pok[eé]mon$/i,
        // Regexes.
        /[^aeiou]ese$/i,
        // "chinese", "japanese"
        /deer$/i,
        // "deer", "reindeer"
        /fish$/i,
        // "fish", "blowfish", "angelfish"
        /measles$/i,
        /o[iu]s$/i,
        // "carnivorous"
        /pox$/i,
        // "chickpox", "smallpox"
        /sheep$/i,
      ].forEach($.addUncountableRule),
      $
    )
  })
})(pw)
Mo.extend(aw)
Mo.extend(cw)
Mo.extend(hw)
function kr(t = kr('"message" is required')) {
  throw new Ks(t)
}
function It(t) {
  return t === !1
}
function Qt(t) {
  return [null, void 0].includes(t)
}
function Kr(t) {
  return mc(Qt, It)(t)
}
function Kp(t) {
  return t instanceof Element
}
function gc(t) {
  return typeof t == 'string'
}
function Gp(t) {
  return typeof t == 'function'
}
function Xp(t) {
  return Array.isArray(t)
}
function ia(t) {
  return Xp(t) && t.length > 0
}
function mc(...t) {
  return function (i) {
    return t.reduce((o, a) => a(o), i)
  }
}
function jp(t) {
  return typeof t == 'number' && Number.isFinite(t)
}
function fw(t) {
  return mc(jp, It)(t)
}
function gw(t) {
  return ['string', 'number', 'boolean', 'symbol'].includes(typeof t)
}
function Zp(t) {
  return typeof t == 'object' && Kr(t) && t.constructor === Object
}
function mw(t) {
  return Zp(t) && ia(Object.keys(t))
}
function vw(t = 9) {
  const r = new Uint8Array(t)
  return (
    window.crypto.getRandomValues(r),
    Array.from(r, i => i.toString(36))
      .join('')
      .slice(0, t)
  )
}
function $u(t = 0, r = 'YYYY-MM-DD HH:mm:ss') {
  return Mo.utc(t).format(r)
}
function Cu(t = 0, r = 'YYYY-MM-DD HH:mm:ss') {
  return Mo(t).format(r)
}
function lt(t, r = '') {
  return t ? r : ''
}
function bw(t, r = Error) {
  return t instanceof r
}
function Fe(t, r = 'Invalid value') {
  if (Qt(t) || It(t)) throw new Ks(r, bw(r) ? { cause: r } : void 0)
  return !0
}
function yw(t) {
  return Qt(t) ? [] : Xp(t) ? t : [t]
}
function _w(t = kr('"element" is required'), r = kr('"parent" is required')) {
  return {
    top: Math.round(
      t.getBoundingClientRect().top - r.getBoundingClientRect().top,
    ),
    left: Math.round(
      t.getBoundingClientRect().left - r.getBoundingClientRect().left,
    ),
  }
}
function ww(
  t = kr('"element" is required'),
  r = kr('"parent" is required'),
  i = 'vertical',
  o = 'smooth',
) {
  Fe(i in ['horizontal', 'vertical', 'both'], 'Invalid direction'),
    Fe(o in ['smooth', 'auto'], 'Invalid behavior')
  const a = _w(t, r),
    d = a.top + r.scrollTop,
    h = a.left + r.scrollLeft,
    m = r.scrollLeft,
    f = r.scrollLeft + r.offsetWidth,
    b = r.scrollTop,
    A = r.scrollTop + r.offsetHeight
  ;(i === 'horizontal' || i === 'both') &&
    (h < m
      ? r.scrollTo({ left: h, behavior: o })
      : h + t.clientWidth > f &&
        r.scrollTo({
          left: h - r.offsetWidth + t.clientWidth,
          behavior: o,
        })),
    (i === 'vertical' || i === 'both') &&
      (d < b
        ? r.scrollTo({ top: d, behavior: o })
        : d + t.clientHeight > A &&
          r.scrollTo({
            top: d - r.offsetHeight + t.clientHeight,
            behavior: o,
          }))
}
class xw {
  constructor(r = kr('EnumValue "key" is required'), i, o) {
    ;(this.key = r), (this.value = i), (this.title = o ?? i ?? this.value)
  }
}
function ce(t = kr('"obj" is required to create a new Enum')) {
  Fe(mw(t), 'Enum values cannot be empty')
  const r = Object.assign({}, t),
    i = {
      includes: a,
      throwOnMiss: d,
      title: h,
      forEach: A,
      value: m,
      keys: y,
      values: k,
      item: b,
      key: f,
      items: S,
      entries: $,
      getValue: w,
    }
  for (const [T, P] of Object.entries(r)) {
    Fe(gc(T) && fw(parseInt(T)), `Key "${T}" is invalid`)
    const D = yw(P)
    Fe(
      D.every(R => Qt(R) || gw(R)),
      `Value "${P}" is invalid`,
    ) && (r[T] = new xw(T, ...D))
  }
  const o = new Proxy(Object.preventExtensions(r), {
    get(T, P) {
      return P in i ? i[P] : Reflect.get(T, P).value
    },
    set() {
      throw new Ks('Cannot change enum property')
    },
    deleteProperty() {
      throw new Ks('Cannot delete enum property')
    },
  })
  function a(T) {
    return !!y().find(P => m(P) === T)
  }
  function d(T) {
    Fe(a(T), `Value "${T}" does not exist in enum`)
  }
  function h(T) {
    var P
    return (P = S().find(D => D.value === T)) == null ? void 0 : P.title
  }
  function m(T) {
    return o[T]
  }
  function f(T) {
    var P
    return (P = S().find(D => D.value === T || D.title === T)) == null
      ? void 0
      : P.key
  }
  function b(T) {
    return r[T]
  }
  function A(T) {
    y().forEach(P => T(r[P]))
  }
  function y() {
    return Object.keys(r)
  }
  function k() {
    return y().map(T => m(T))
  }
  function S() {
    return y().map(T => b(T))
  }
  function $() {
    return y().map((T, P) => [T, m(T), b(T), P])
  }
  function w(T) {
    return m(f(T))
  }
  return o
}
ce({
  Complete: ['complete', 'Complete'],
  Failed: ['failed', 'Failed'],
  Behind: ['behind', 'Behind'],
  Progress: ['progress', 'Progress'],
  InProgress: ['in progress', 'In Progress'],
  Pending: ['pending', 'Pending'],
  Skipped: ['skipped', 'Skipped'],
  Undefined: ['undefined', 'Undefined'],
})
const An = ce({
    Open: 'open',
    Close: 'close',
    Click: 'click',
    Keydown: 'keydown',
    Keyup: 'keyup',
    Focus: 'focus',
    Blur: 'blur',
    Submit: 'submit',
    Change: 'change',
  }),
  kw = ce({
    Base: 'base',
    Content: 'content',
    Tagline: 'tagline',
    Before: 'before',
    After: 'after',
    Info: 'info',
    Nav: 'nav',
    Default: void 0,
  }),
  Su = ce({
    Active: 'active',
    Disabled: 'disabled',
    Open: 'open',
    Closed: 'closed',
  })
ce({
  Form: 'FORM',
})
const Ls = ce({
    Tab: 'Tab',
    Enter: 'Enter',
    Shift: 'Shift',
    Escape: 'Escape',
    Space: 'Space',
    End: 'End',
    Home: 'Home',
    Left: 'ArrowLeft',
    Up: 'ArrowUp',
    Right: 'ArrowRight',
    Down: 'ArrowDown',
  }),
  vc = ce({
    Horizontal: 'horizontal',
    Vertical: 'vertical',
  }),
  Lt = ce({
    XXS: '2xs',
    XS: 'xs',
    S: 's',
    M: 'm',
    L: 'l',
    XL: 'xl',
    XXL: '2xl',
  }),
  Jt = ce({
    Left: 'left',
    Right: 'right',
    Center: 'center',
  }),
  qr = ce({
    Left: 'left',
    Right: 'right',
    Top: 'top',
    Bottom: 'bottom',
  }),
  Be = ce({
    Round: 'round',
    Pill: 'pill',
    Square: 'square',
    Circle: 'circle',
    Rect: 'rect',
  }),
  $t = ce({
    Neutral: 'neutral',
    Undefined: 'undefined',
    Primary: 'primary',
    Translucent: 'translucent',
    Success: 'success',
    Danger: 'danger',
    Warning: 'warning',
    Gradient: 'gradient',
    Transparent: 'transparent',
    /* ------------------------------ */
    Action: 'action',
    Secondary: 'secondary-action',
    Alternative: 'alternative',
    Cancel: 'cancel',
    /* ------------------------------ */
    Failed: 'failed',
    InProgress: 'in-progress',
    Progress: 'progress',
    Skipped: 'skipped',
    Preempted: 'preempted',
    Pending: 'pending',
    Complete: 'complete',
    Behind: 'behind',
    /* ------------------------------ */
    IDE: 'ide',
    Lineage: 'lineage',
    Editor: 'editor',
    Folder: 'folder',
    File: 'file',
    Error: 'error',
    Changes: 'changes',
    ChangeAdd: 'change-add',
    ChangeRemove: 'change-remove',
    ChangeDirectly: 'change-directly',
    ChangeIndirectly: 'change-indirectly',
    ChangeMetadata: 'change-metadata',
    Backfill: 'backfill',
    Restatement: 'restatement',
    Audit: 'audit',
    Test: 'test',
    Macro: 'macro',
    Model: 'model',
    ModelVersion: 'model-version',
    ModelSQL: 'model-sql',
    ModelPython: 'model-python',
    ModelExternal: 'model-external',
    ModelSeed: 'model-seed',
    ModelUnknown: 'model-unknown',
    Column: 'column',
    Upstream: 'upstream',
    Downstream: 'downstream',
    Plan: 'plan',
    Run: 'run',
    Environment: 'environment',
    Breaking: 'breaking',
    NonBreaking: 'non-breaking',
    ForwardOnly: 'forward-only',
    Measure: 'measure',
  }),
  zu = ce({
    Auto: 'auto',
    Full: 'full',
    Wide: 'wide',
    Compact: 'compact',
  }),
  $w = ce({
    Auto: 'auto',
    Full: 'full',
    Tall: 'tall',
    Short: 'short',
  }),
  ji = Object.freeze({
    ...Object.entries(kw).reduce(
      (t, [r, i]) => ((t[`Part${r}`] = Au('part', i)), t),
      {},
    ),
    SlotDefault: Au('slot:not([name])'),
  })
function Au(t = kr('"name" is required to create a selector'), r) {
  return Qt(r) ? t : `[${t}="${r}"]`
}
var Cw = typeof Ge == 'object' && Ge && Ge.Object === Object && Ge,
  Sw = Cw,
  zw = Sw,
  Aw = typeof self == 'object' && self && self.Object === Object && self,
  Ew = zw || Aw || Function('return this')(),
  bc = Ew,
  Tw = bc,
  Iw = Tw.Symbol,
  yc = Iw,
  Eu = yc,
  Jp = Object.prototype,
  Ow = Jp.hasOwnProperty,
  Lw = Jp.toString,
  po = Eu ? Eu.toStringTag : void 0
function Dw(t) {
  var r = Ow.call(t, po),
    i = t[po]
  try {
    t[po] = void 0
    var o = !0
  } catch {}
  var a = Lw.call(t)
  return o && (r ? (t[po] = i) : delete t[po]), a
}
var Mw = Dw,
  Rw = Object.prototype,
  Pw = Rw.toString
function Bw(t) {
  return Pw.call(t)
}
var Fw = Bw,
  Tu = yc,
  Nw = Mw,
  Uw = Fw,
  Hw = '[object Null]',
  Vw = '[object Undefined]',
  Iu = Tu ? Tu.toStringTag : void 0
function Ww(t) {
  return t == null
    ? t === void 0
      ? Vw
      : Hw
    : Iu && Iu in Object(t)
    ? Nw(t)
    : Uw(t)
}
var qw = Ww
function Yw(t) {
  var r = typeof t
  return t != null && (r == 'object' || r == 'function')
}
var Qp = Yw,
  Kw = qw,
  Gw = Qp,
  Xw = '[object AsyncFunction]',
  jw = '[object Function]',
  Zw = '[object GeneratorFunction]',
  Jw = '[object Proxy]'
function Qw(t) {
  if (!Gw(t)) return !1
  var r = Kw(t)
  return r == jw || r == Zw || r == Xw || r == Jw
}
var tx = Qw,
  ex = bc,
  rx = ex['__core-js_shared__'],
  ix = rx,
  Ul = ix,
  Ou = (function () {
    var t = /[^.]+$/.exec((Ul && Ul.keys && Ul.keys.IE_PROTO) || '')
    return t ? 'Symbol(src)_1.' + t : ''
  })()
function nx(t) {
  return !!Ou && Ou in t
}
var ox = nx,
  sx = Function.prototype,
  ax = sx.toString
function lx(t) {
  if (t != null) {
    try {
      return ax.call(t)
    } catch {}
    try {
      return t + ''
    } catch {}
  }
  return ''
}
var cx = lx,
  dx = tx,
  hx = ox,
  ux = Qp,
  px = cx,
  fx = /[\\^$.*+?()[\]{}|]/g,
  gx = /^\[object .+?Constructor\]$/,
  mx = Function.prototype,
  vx = Object.prototype,
  bx = mx.toString,
  yx = vx.hasOwnProperty,
  _x = RegExp(
    '^' +
      bx
        .call(yx)
        .replace(fx, '\\$&')
        .replace(
          /hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g,
          '$1.*?',
        ) +
      '$',
  )
function wx(t) {
  if (!ux(t) || hx(t)) return !1
  var r = dx(t) ? _x : gx
  return r.test(px(t))
}
var xx = wx
function kx(t, r) {
  return t == null ? void 0 : t[r]
}
var $x = kx,
  Cx = xx,
  Sx = $x
function zx(t, r) {
  var i = Sx(t, r)
  return Cx(i) ? i : void 0
}
var _c = zx,
  Ax = _c
;(function () {
  try {
    var t = Ax(Object, 'defineProperty')
    return t({}, '', {}), t
  } catch {}
})()
function Ex(t, r) {
  return t === r || (t !== t && r !== r)
}
var Tx = Ex,
  Ix = _c,
  Ox = Ix(Object, 'create'),
  na = Ox,
  Lu = na
function Lx() {
  ;(this.__data__ = Lu ? Lu(null) : {}), (this.size = 0)
}
var Dx = Lx
function Mx(t) {
  var r = this.has(t) && delete this.__data__[t]
  return (this.size -= r ? 1 : 0), r
}
var Rx = Mx,
  Px = na,
  Bx = '__lodash_hash_undefined__',
  Fx = Object.prototype,
  Nx = Fx.hasOwnProperty
function Ux(t) {
  var r = this.__data__
  if (Px) {
    var i = r[t]
    return i === Bx ? void 0 : i
  }
  return Nx.call(r, t) ? r[t] : void 0
}
var Hx = Ux,
  Vx = na,
  Wx = Object.prototype,
  qx = Wx.hasOwnProperty
function Yx(t) {
  var r = this.__data__
  return Vx ? r[t] !== void 0 : qx.call(r, t)
}
var Kx = Yx,
  Gx = na,
  Xx = '__lodash_hash_undefined__'
function jx(t, r) {
  var i = this.__data__
  return (
    (this.size += this.has(t) ? 0 : 1),
    (i[t] = Gx && r === void 0 ? Xx : r),
    this
  )
}
var Zx = jx,
  Jx = Dx,
  Qx = Rx,
  t2 = Hx,
  e2 = Kx,
  r2 = Zx
function Rn(t) {
  var r = -1,
    i = t == null ? 0 : t.length
  for (this.clear(); ++r < i; ) {
    var o = t[r]
    this.set(o[0], o[1])
  }
}
Rn.prototype.clear = Jx
Rn.prototype.delete = Qx
Rn.prototype.get = t2
Rn.prototype.has = e2
Rn.prototype.set = r2
var i2 = Rn
function n2() {
  ;(this.__data__ = []), (this.size = 0)
}
var o2 = n2,
  s2 = Tx
function a2(t, r) {
  for (var i = t.length; i--; ) if (s2(t[i][0], r)) return i
  return -1
}
var oa = a2,
  l2 = oa,
  c2 = Array.prototype,
  d2 = c2.splice
function h2(t) {
  var r = this.__data__,
    i = l2(r, t)
  if (i < 0) return !1
  var o = r.length - 1
  return i == o ? r.pop() : d2.call(r, i, 1), --this.size, !0
}
var u2 = h2,
  p2 = oa
function f2(t) {
  var r = this.__data__,
    i = p2(r, t)
  return i < 0 ? void 0 : r[i][1]
}
var g2 = f2,
  m2 = oa
function v2(t) {
  return m2(this.__data__, t) > -1
}
var b2 = v2,
  y2 = oa
function _2(t, r) {
  var i = this.__data__,
    o = y2(i, t)
  return o < 0 ? (++this.size, i.push([t, r])) : (i[o][1] = r), this
}
var w2 = _2,
  x2 = o2,
  k2 = u2,
  $2 = g2,
  C2 = b2,
  S2 = w2
function Pn(t) {
  var r = -1,
    i = t == null ? 0 : t.length
  for (this.clear(); ++r < i; ) {
    var o = t[r]
    this.set(o[0], o[1])
  }
}
Pn.prototype.clear = x2
Pn.prototype.delete = k2
Pn.prototype.get = $2
Pn.prototype.has = C2
Pn.prototype.set = S2
var z2 = Pn,
  A2 = _c,
  E2 = bc,
  T2 = A2(E2, 'Map'),
  I2 = T2,
  Du = i2,
  O2 = z2,
  L2 = I2
function D2() {
  ;(this.size = 0),
    (this.__data__ = {
      hash: new Du(),
      map: new (L2 || O2)(),
      string: new Du(),
    })
}
var M2 = D2
function R2(t) {
  var r = typeof t
  return r == 'string' || r == 'number' || r == 'symbol' || r == 'boolean'
    ? t !== '__proto__'
    : t === null
}
var P2 = R2,
  B2 = P2
function F2(t, r) {
  var i = t.__data__
  return B2(r) ? i[typeof r == 'string' ? 'string' : 'hash'] : i.map
}
var sa = F2,
  N2 = sa
function U2(t) {
  var r = N2(this, t).delete(t)
  return (this.size -= r ? 1 : 0), r
}
var H2 = U2,
  V2 = sa
function W2(t) {
  return V2(this, t).get(t)
}
var q2 = W2,
  Y2 = sa
function K2(t) {
  return Y2(this, t).has(t)
}
var G2 = K2,
  X2 = sa
function j2(t, r) {
  var i = X2(this, t),
    o = i.size
  return i.set(t, r), (this.size += i.size == o ? 0 : 1), this
}
var Z2 = j2,
  J2 = M2,
  Q2 = H2,
  tk = q2,
  ek = G2,
  rk = Z2
function Bn(t) {
  var r = -1,
    i = t == null ? 0 : t.length
  for (this.clear(); ++r < i; ) {
    var o = t[r]
    this.set(o[0], o[1])
  }
}
Bn.prototype.clear = J2
Bn.prototype.delete = Q2
Bn.prototype.get = tk
Bn.prototype.has = ek
Bn.prototype.set = rk
var ik = Bn,
  tf = ik,
  nk = 'Expected a function'
function wc(t, r) {
  if (typeof t != 'function' || (r != null && typeof r != 'function'))
    throw new TypeError(nk)
  var i = function () {
    var o = arguments,
      a = r ? r.apply(this, o) : o[0],
      d = i.cache
    if (d.has(a)) return d.get(a)
    var h = t.apply(this, o)
    return (i.cache = d.set(a, h) || d), h
  }
  return (i.cache = new (wc.Cache || tf)()), i
}
wc.Cache = tf
var ok = wc,
  sk = ok,
  ak = 500
function lk(t) {
  var r = sk(t, function (o) {
      return i.size === ak && i.clear(), o
    }),
    i = r.cache
  return r
}
var ck = lk,
  dk = ck,
  hk =
    /[^.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\\]|\\.)*?)\2)\]|(?=(?:\.|\[\])(?:\.|\[\]|$))/g,
  uk = /\\(\\)?/g
dk(function (t) {
  var r = []
  return (
    t.charCodeAt(0) === 46 && r.push(''),
    t.replace(hk, function (i, o, a, d) {
      r.push(a ? d.replace(uk, '$1') : o || i)
    }),
    r
  )
})
var Mu = yc,
  Ru = Mu ? Mu.prototype : void 0
Ru && Ru.toString
const Dr = ce({
  UTC: ['utc', 'UTC'],
  Local: ['local', 'LOCAL'],
})
function nn(t) {
  var r
  return (
    (r = class extends t {}),
    W(r, 'shadowRootOptions', { ...t.shadowRootOptions, delegatesFocus: !0 }),
    r
  )
}
function pk(t) {
  var r
  return (
    (r = class extends t {
      connectedCallback() {
        super.connectedCallback(), this.setTimezone(this.timezone)
      }
      get isLocal() {
        return this.timezone === Dr.Local
      }
      get isUTC() {
        return this.timezone === Dr.UTC
      }
      constructor() {
        super(), (this.timezone = Dr.UTC)
      }
      setTimezone(o = Dr.UTC) {
        this.timezone = Dr.includes(o) ? Dr.getValue(o) : Dr.UTC
      }
    }),
    W(r, 'properties', {
      timezone: { type: Dr, converter: Dr.getValue },
    }),
    r
  )
}
function dr(t = Pu(Ji), ...r) {
  return (
    Qt(t._$litElement$) && (r.push(t), (t = Pu(Ji))), mc(...r.flat())(fk(t))
  )
}
function Pu(t) {
  return class extends t {}
}
class Ai {
  constructor(r, i, o = {}) {
    W(this, '_event')
    ;(this.original = i), (this.value = r), (this.meta = o)
  }
  get event() {
    return this._event
  }
  setEvent(r = kr('"event" is required to set event')) {
    this._event = r
  }
  static assert(r) {
    Fe(r instanceof Ai, 'Event "detail" should be instance of "EventDetail"')
  }
  static assertHandler(r) {
    return (
      Fe(Gp(r), '"eventHandler" should be a function'),
      function (i = kr('"event" is required')) {
        return Ai.assert(i.detail), r(i)
      }
    )
  }
}
function fk(t) {
  var r
  return (
    (r = class extends t {
      constructor() {
        super(),
          (this.uid = vw()),
          (this.disabled = !1),
          (this.emit.EventDetail = Ai)
      }
      firstUpdated() {
        var o
        if (
          (super.firstUpdated(),
          (o = this.elsSlots) == null ||
            o.forEach(a =>
              a.addEventListener(
                'slotchange',
                this._handleSlotChange.bind(this),
              ),
            ),
          Kr(window.htmx))
        ) {
          const a = Array.from(this.renderRoot.querySelectorAll('a'))
          ;(this.closest('[hx-boost="true"]') ||
            this.closest('[data-hx-boost="true"]')) &&
            a.forEach(h => {
              It(h.hasAttribute('hx-boost')) &&
                h.setAttribute(
                  'hx-boost',
                  this.hasAttribute('[hx-boost="false"]') ? 'false' : 'true',
                )
            }),
            window.htmx.process(this),
            window.htmx.process(this.renderRoot)
        }
      }
      get elSlot() {
        var o
        return (o = this.renderRoot) == null
          ? void 0
          : o.querySelector(ji.SlotDefault)
      }
      get elsSlots() {
        var o
        return (o = this.renderRoot) == null
          ? void 0
          : o.querySelectorAll('slot')
      }
      get elBase() {
        var o
        return (o = this.renderRoot) == null
          ? void 0
          : o.querySelector(ji.PartBase)
      }
      get elsSlotted() {
        return []
          .concat(
            Array.from(this.elsSlots).map(o =>
              o.assignedElements({ flatten: !0 }),
            ),
          )
          .flat()
      }
      clear() {
        r.clear(this)
      }
      emit(o = 'event', a) {
        if (
          ((a = Object.assign(
            {
              detail: void 0,
              bubbles: !0,
              cancelable: !1,
              composed: !0,
            },
            a,
          )),
          Kr(a.detail))
        ) {
          if (It(a.detail instanceof Ai) && Zp(a.detail))
            if ('value' in a.detail) {
              const { value: d, ...h } = a.detail
              a.detail = new Ai(d, void 0, h)
            } else a.detail = new Ai(a.detail)
          Fe(
            a.detail instanceof Ai,
            'event "detail" must be instance of "EventDetail"',
          ),
            a.detail.setEvent(o)
        }
        return this.dispatchEvent(new CustomEvent(o, a))
      }
      setHidden(o = !1, a = 0) {
        setTimeout(() => {
          this.hidden = o
        }, a)
      }
      setDisabled(o = !1, a = 0) {
        setTimeout(() => {
          this.disabled = o
        }, a)
      }
      notify(o, a, d) {
        var h
        ;(h = o == null ? void 0 : o.emit) == null || h.call(o, a, d)
      }
      // We may want to ensure event detail when sending custom events from children components
      assertEventHandler(o = kr('"eventHandler" is required')) {
        return (
          Fe(Gp(o), '"eventHandler" should be a function'),
          this.emit.EventDetail.assertHandler(o.bind(this))
        )
      }
      getShadowRoot() {
        return this.renderRoot
      }
      _handleSlotChange(o) {
        Kr(o.target) && (o.target.style.position = 'initial')
      }
      static clear(o) {
        Kp(o) && (o.innerHTML = '')
      }
      static defineAs(
        o = kr('"tagName" is required to define custom element'),
      ) {
        Qt(customElements.get(o)) && customElements.define(o, this)
      }
    }),
    W(r, 'properties', {
      uid: { type: String },
      disabled: { type: Boolean, reflect: !0 },
      tabindex: { type: Number, reflect: !0 },
    }),
    r
  )
}
function gk() {
  return ft(`
        :host([disabled]) { cursor: not-allowed; }
        :host([disabled]) > *,
        :host([disabled])::slotted(*) {
            pointer-events: none;
            opacity: 0.75;
        }
        :host(:focus),
        :host([disabled]),
        :host([disabled]) * { outline: none; }
    `)
}
function ef() {
  return ft(`
        :host { box-sizing: border-box; }
        :host *,
        :host *::before,
        :host *::after { box-sizing: inherit; }
        :host[hidden] { display: none !important; }
    `)
}
function Bt() {
  return ft(`
        ${ef()}
        ${gk()}
    `)
}
function on(t) {
  const r = `
        outline: var(--half) solid var(--color-outline);
        outline-offset: var(--half);
        z-index: 1;
    `
  return ft(
    t
      ? `
            ${t}:focus,
            ${t}[disabled] { outline: none; }
            ${t},
            ${t}:focus-visible:not([disabled]) {
                ${r}
            }
        `
      : `
            :host(:focus-visible:not([disabled])) {
                ${r}
            }
    `,
  )
}
function mk(t = ':host') {
  return ft(`
    ${t} {
        scroll-behavior: smooth;
        scrollbar-width: var(--size-scrollbar);
        scrollbar-width: thin;
        scrollbar-color: var(--color-scrollbar) transparent;
        scrollbar-base-color: var(--color-scrollbar);
        scrollbar-face-color: var(--color-scrollbar);
        scrollbar-track-color: transparent;
    }
    
    ${t}::-webkit-scrollbar {
        height: var(--size-scrollbar);
        width: var(--size-scrollbar);
        border-radius: var(--step-4);
        overflow: hidden;
    }
    
    ${t}::-webkit-scrollbar-track {
        background-color: transparent;
    }
    
    ${t}::-webkit-scrollbar-thumb {
        background: var(--color-scrollbar);
        border-radius: var(--step-4);
    }
  `)
}
function aa(t = 'from-input') {
  return ft(`
        :host {
            --from-input-padding: var(--${t}-padding-y) var(--${t}-padding-x);
            --from-input-placeholder-color: var(--color-gray-300);
            --from-input-color: var(--color-gray-700);
            --from-input-color-error: var(--color-scarlet);
            --from-input-background: var(--color-scarlet-5);
            --from-input-font-size: var(--font-size);
            --from-input-text-align: var(--text-align);
            --from-input-box-shadow: inset 0 0 0 1px var(--color-gray-200);
            --from-input-radius: var(--radius-xs);
            --from-input-font-family: var(--font-sans);
            --form-input-box-shadow-hover: inset 0 0 0 1px var(--color-gray-500);
            --form-input-box-shadow-focus: inset 0 0 0 1px var(--color-gray-500);

            --${t}-padding: var(--from-input-padding);
            --${t}-placeholder-color: var(--from-input-placeholder-color);
            --${t}-color: var(--from-input-color);
            --${t}-font-size: var(--from-input-font-size);
            --${t}-text-align: var(--from-input-text-align);
            --${t}-box-shadow: var(--from-input-box-shadow);
            --${t}-radius: var(--from-input-radius);
            --${t}-background: transparent;
            --${t}-font-family: var(--from-input-font-family);
            --${t}-box-shadow-hover: var(--form-input-box-shadow-hover);
            --${t}-box-shadow-focus: var(--form-input-box-shadow-focus);
        }
        :host([error]) {
            --${t}-box-shadow: inset 0 0 0 1px var(--color-scarlet);
        }
      `)
}
function Xr(t = 'color') {
  return ft(`
          :host([variant="${$t.Neutral}"]),
          :host([variant="${$t.Undefined}"]) {
            --${t}-variant-10: var(--color-gray-10);
            --${t}-variant-15: var(--color-gray-15);
            --${t}-variant-125: var(--color-gray-125);
            --${t}-variant-150: var(--color-gray-150);
            --${t}-variant-525: var(--color-gray-525);
            --${t}-variant-550: var(--color-gray-550);
            --${t}-variant-725: var(--color-gray-725);
            --${t}-variant-750: var(--color-gray-750);

            --${t}-variant-shadow: var(--color-gray-200);
            --${t}-variant: var(--color-gray-700);
            --${t}-variant-lucid: var(--color-gray-5);
            --${t}-variant-light: var(--color-gray-100);
            --${t}-variant-dark: var(--color-gray-800);
          }
            :host-context([mode='dark']):host([variant="${$t.Neutral}"]),
            :host-context([mode='dark']):host([variant="${$t.Undefined}"]) {
                --${t}-variant: var(--color-gray-200);
                --${t}-variant-lucid: var(--color-gray-10);
            }
          :host([variant="${$t.Undefined}"]) {
            --${t}-variant-shadow: var(--color-gray-100);
            --${t}-variant: var(--color-gray-200);
            --${t}-variant-lucid: var(--color-gray-5);
            --${t}-variant-light: var(--color-gray-100);
            --${t}-variant-dark: var(--color-gray-500);
          }
          :host([variant="${$t.Success}"]),
          :host([variant="${$t.Complete}"]) {
            --${t}-variant-5: var(--color-emerald-5);
            --${t}-variant-10: var(--color-emerald-10);
            --${t}-variant: var(--color-emerald-500);
            --${t}-variant-lucid: var(--color-emerald-5);
            --${t}-variant-light: var(--color-emerald-100);
            --${t}-variant-dark: var(--color-emerald-800);
          }
          :host([variant="${$t.Warning}"]),
          :host([variant="${$t.Skipped}"]),
          :host([variant="${$t.Pending}"]) {
            --${t}-variant: var(--color-mandarin-500);
            --${t}-variant-lucid: var(--color-mandarin-5);
            --${t}-variant-light: var(--color-mandarin-100);
            --${t}-variant-dark: var(--color-mandarin-800);
          }
          :host([variant="${$t.Danger}"]),
          :host([variant="${$t.Behind}"]),
          :host([variant="${$t.Failed}"]) {
            --${t}-variant-5: var(--color-scarlet-5);
            --${t}-variant-10: var(--color-scarlet-10);
            --${t}-variant: var(--color-scarlet-500);
            --${t}-variant-lucid: var(--color-scarlet-5);
            --${t}-variant-lucid: var(--color-scarlet-5);
            --${t}-variant-light: var(--color-scarlet-100);
            --${t}-variant-dark: var(--color-scarlet-800);
          }
          :host([variant="${$t.ChangeAdd}"]) {
            --${t}-variant: var(--color-change-add);
            --${t}-variant-lucid: var(--color-change-add-5);
            --${t}-variant-light: var(--color-change-add-100);
            --${t}-variant-dark: var(--color-change-add-800);
          }
          :host([variant="${$t.ChangeRemove}"]) {
            --${t}-variant: var(--color-change-remove);
            --${t}-variant-lucid: var(--color-change-remove-5);
            --${t}-variant-light: var(--color-change-remove-100);
            --${t}-variant-dark: var(--color-change-remove-800);
          }
          :host([variant="${$t.ChangeDirectly}"]) {
            --${t}-variant: var(--color-change-directly-modified);
            --${t}-variant-lucid: var(--color-change-directly-modified-5);
            --${t}-variant-light: var(--color-change-directly-modified-100);
            --${t}-variant-dark: var(--color-change-directly-modified-800);
          }
          :host([variant="${$t.ChangeIndirectly}"]) {
            --${t}-variant: var(--color-change-indirectly-modified);
            --${t}-variant-lucid: var(--color-change-indirectly-modified-5);
            --${t}-variant-light: var(--color-change-indirectly-modified-100);
            --${t}-variant-dark: var(--color-change-indirectly-modified-800);
          }
          :host([variant="${$t.ChangeMetadata}"]) {
            --${t}-variant: var(--color-change-metadata);
            --${t}-variant-lucid: var(--color-change-metadata-5);
            --${t}-variant-light: var(--color-change-metadata-100);
            --${t}-variant-dark: var(--color-change-metadata-800);
          }
          :host([variant="${$t.Backfill}"]) {
            --${t}-variant: var(--color-backfill);
            --${t}-variant-lucid: var(--color-backfill-5);
            --${t}-variant-light: var(--color-backfill-100);
            --${t}-variant-dark: var(--color-backfill-800);
          }
          :host([variant="${$t.Model}"]),
          :host([variant="${$t.Primary}"]) {
            --${t}-variant: var(--color-deep-blue);
            --${t}-variant-lucid: var(--color-deep-blue-5);
            --${t}-variant-light: var(--color-deep-blue-100);
            --${t}-variant-dark: var(--color-deep-blue-800);
          }
          :host([variant="${$t.Plan}"]) {
            --${t}-variant: var(--color-plan);
            --${t}-variant-lucid: var(--color-plan-5);
            --${t}-variant-light: var(--color-plan-100);
            --${t}-variant-dark: var(--color-plan-800);
          }
          :host([variant="${$t.Run}"]) {
            --${t}-variant: var(--color-run);
            --${t}-variant-lucid: var(--color-run-5);
            --${t}-variant-light: var(--color-run-100);
            --${t}-variant-dark: var(--color-run-800);
          }
          :host([variant="${$t.Environment}"]) {
            --${t}-variant: var(--color-environment);
            --${t}-variant-lucid: var(--color-environment-5);
            --${t}-variant-light: var(--color-environment-100);
            --${t}-variant-dark: var(--color-environment-800);
          }
        :host([variant="${$t.Progress}"]),
        :host([variant="${$t.InProgress}"]) {
            --${t}-variant: var(--color-status-progress);
            --${t}-variant-lucid: var(--color-status-progress-5);
            --${t}-variant-light: var(--color-status-progress-100);
            --${t}-variant-dark: var(--color-status-progress-800);
        }
      `)
}
function vk(t = '') {
  return ft(`
        :host([inverse]) {
            --${t}-background: var(--color-variant);
            --${t}-color: var(--color-variant-light);
        }
    `)
}
function bk(t = '') {
  return ft(`
        :host([ghost]:not([inverse])) {
            --${t}-background: transparent;
        }
        :host([disabled][ghost]:not([inverse])) {
            --${t}-background: var(--color-variant-light);
        }
    `)
}
function yk(t = '') {
  return ft(`
        :host(:hover:not([disabled])) {
            --${t}-background: var(--color-variant-125);
        }
        :host(:active:not([disabled])) {
            --${t}-background: var(--color-variant-150);
        }
        :host([inverse]:hover:not([disabled])) {
            --${t}-background: var(--color-variant-525);
        }
        :host([inverse]:active:not([disabled])) {
            --${t}-background: var(--color-variant-550);
        }
    `)
}
function fi(t = '', r) {
  return ft(`
        :host([shape="${Be.Rect}"]) {
            --${t}-radius: 0;
        }        
        :host([shape="${Be.Round}"]) {
            --${t}-radius: var(--from-input-radius, var(--radius-xs));
        }
        :host([shape="${Be.Pill}"]) {
            --${t}-radius: calc(var(--${t}-font-size) * 2);
        }
        :host([shape="${Be.Circle}"]) {
            --${t}-width: calc(var(--${t}-font-size) * 2);
            --${t}-height: calc(var(--${t}-font-size) * 2);
            --${t}-padding-y: 0;
            --${t}-padding-x: 0;
            --${t}-radius: 100%;
        }
        :host([shape="${Be.Square}"]) {
            --${t}-width: calc(var(--${t}-font-size) * 2);
            --${t}-height: calc(var(--${t}-font-size) * 2);
            --${t}-padding-y: 0;
            --${t}-padding-x: 0;
            --${t}-radius: 0;
        }
    `)
}
function xc() {
  return ft(`
        :host([side="${Jt.Left}"]) {
            --text-align: left;
        }
        :host([side="${Jt.Center}"]) {
            --text-align: center;
        }
        :host([side="${Jt.Right}"]) {
            --text-align: right;
        }
    `)
}
function _k() {
  return ft(`
        :host([shadow]) {
            --shadow: 0 1px var(--half) 0 var(--color-variant-shadow);
        }
    `)
}
function wk() {
  return ft(`
        :host([outline]) {
            --shadow-inset: inset 0 0 0 var(--half) var(--color-variant);
        }
    `)
}
function xk(t = 'label', r = '') {
  return ft(`
        ${t} {
            font-weight: var(--text-semibold);
            color: var(${r ? `--${r}-color` : '--color-gray-700'});
        }
    `)
}
function kk(t = 'p', r = '') {
  return ft(`
        ${t} {
            font-weight: var(--text-normal);
            color: var(${r ? `--${r}-color` : '--color-gray-700'});
        }
    `)
}
function je(t = 'item', r = 1.25, i = 4) {
  return ft(`
        :host {
            --${t}-padding-x: round(up, calc(var(--${t}-font-size) / ${r} * var(--padding-x-factor, 1)), var(--half));
            --${t}-padding-y: round(up, calc(var(--${t}-font-size) / ${i} * var(--padding-y-factor, 1)), var(--half));
        }
    `)
}
function $k(t = '') {
  return ft(`
        :host([horizontal="compact"]) {
            --padding-x-factor: 0.5;
        }
        :host([horizontal="wide"]) {
            --padding-x-factor: 1.5;
        }
        :host([horizontal="full"]) {
            --${t}-width: 100%;
            width: var(--${t}-width);
        }
        :host([vertical="tall"]) {
            --padding-y-factor: 1.25;
        }
        :host([vertical="short"]) {
            --padding-y-factor: 0.75;
        }
        :host([vertical="full"]) {
            --${t}-height: 100%;
            height: var(--${t}-height);
        }
    `)
}
function ge(t) {
  const r = t ? `--${t}-font-size` : '--font-size',
    i = t ? `--${t}-font-weight` : '--font-size'
  return ft(`
        :host {
            ${i}: var(--text-medium);
            ${r}: var(--text-s);
        }
        :host([size="${Lt.XXS}"]) {
            ${i}: var(--text-semibold);
            ${r}: var(--text-2xs);
        }
        :host([size="${Lt.XS}"]) {
            ${i}: var(--text-semibold);
            ${r}: var(--text-xs);
        }
        :host([size="${Lt.S}"]) {
            ${i}: var(--text-medium);
            ${r}: var(--text-s);
        }
        :host([size="${Lt.M}"]) {
            ${i}: var(--text-medium);
            ${r}: var(--text-m);
        }
        :host([size="${Lt.L}"]) {
            ${i}: var(--text-normal);
            ${r}: var(--text-l);
        }
        :host([size="${Lt.XL}"]) {
            ${i}: var(--text-normal);
            ${r}: var(--text-xl);
        }
        :host([size="${Lt.XXL}"]) {
            ${i}: var(--text-normal);
            ${r}: var(--text-2xl);
        }
    `)
}
Lp('heroicons', {
  resolver: t =>
    `https://cdn.jsdelivr.net/npm/heroicons@2.1.5/24/outline/${t}.svg`,
})
Lp('heroicons-micro', {
  resolver: t =>
    `https://cdn.jsdelivr.net/npm/heroicons@2.1.5/16/solid/${t}.svg`,
})
class Bu extends dr(Nt, nn) {}
W(Bu, 'styles', [Nt.styles, Bt()]),
  W(Bu, 'properties', {
    ...Nt.properties,
  })
const de = dr(),
  Ck = `:host {
  --badge-font-size: var(--font-size);
  --badge-font-family: var(--font-mono);
  --badge-color: var(--badge-variant);
  --badge-background: var(--badge-variant-lucid);

  display: inline-flex;
  border-radius: var(--badge-radius);
  background: var(--badge-background);
  color: var(--badge-color);
  font-family: var(--badge-font-family);
}
[part='base'] {
  margin: 0;
  padding: var(--badge-padding-y) var(--badge-padding-x);
  display: flex;
  align-items: center;
  font-size: var(--badge-font-size);
  line-height: 1;
  border-radius: var(--badge-radius);
  text-align: center;
  white-space: nowrap;
}
`
class Fu extends de {
  constructor() {
    super(),
      (this.size = Lt.M),
      (this.variant = $t.Neutral),
      (this.shape = Be.Round)
  }
  render() {
    return C`
      <span part="base">
        <slot></slot>
      </span>
    `
  }
}
W(Fu, 'styles', [
  Bt(),
  ge(),
  Xr('badge'),
  xk('[part="base"]', 'badge'),
  vk('badge'),
  bk('badge'),
  fi('badge'),
  je('badge', 1.75, 4),
  _k(),
  wk(),
  ft(Ck),
]),
  W(Fu, 'properties', {
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
  })
var Sk = st`
  :host {
    --max-width: 20rem;
    --hide-delay: 0ms;
    --show-delay: 150ms;

    display: contents;
  }

  .tooltip {
    --arrow-size: var(--sl-tooltip-arrow-size);
    --arrow-color: var(--sl-tooltip-background-color);
  }

  .tooltip::part(popup) {
    z-index: var(--sl-z-index-tooltip);
  }

  .tooltip[placement^='top']::part(popup) {
    transform-origin: bottom;
  }

  .tooltip[placement^='bottom']::part(popup) {
    transform-origin: top;
  }

  .tooltip[placement^='left']::part(popup) {
    transform-origin: right;
  }

  .tooltip[placement^='right']::part(popup) {
    transform-origin: left;
  }

  .tooltip__body {
    display: block;
    width: max-content;
    max-width: var(--max-width);
    border-radius: var(--sl-tooltip-border-radius);
    background-color: var(--sl-tooltip-background-color);
    font-family: var(--sl-tooltip-font-family);
    font-size: var(--sl-tooltip-font-size);
    font-weight: var(--sl-tooltip-font-weight);
    line-height: var(--sl-tooltip-line-height);
    text-align: start;
    white-space: normal;
    color: var(--sl-tooltip-color);
    padding: var(--sl-tooltip-padding);
    pointer-events: none;
    user-select: none;
    -webkit-user-select: none;
  }
`,
  zk = st`
  :host {
    --arrow-color: var(--sl-color-neutral-1000);
    --arrow-size: 6px;

    /*
     * These properties are computed to account for the arrow's dimensions after being rotated 45º. The constant
     * 0.7071 is derived from sin(45), which is the diagonal size of the arrow's container after rotating.
     */
    --arrow-size-diagonal: calc(var(--arrow-size) * 0.7071);
    --arrow-padding-offset: calc(var(--arrow-size-diagonal) - var(--arrow-size));

    display: contents;
  }

  .popup {
    position: absolute;
    isolation: isolate;
    max-width: var(--auto-size-available-width, none);
    max-height: var(--auto-size-available-height, none);
  }

  .popup--fixed {
    position: fixed;
  }

  .popup:not(.popup--active) {
    display: none;
  }

  .popup__arrow {
    position: absolute;
    width: calc(var(--arrow-size-diagonal) * 2);
    height: calc(var(--arrow-size-diagonal) * 2);
    rotate: 45deg;
    background: var(--arrow-color);
    z-index: -1;
  }

  /* Hover bridge */
  .popup-hover-bridge:not(.popup-hover-bridge--visible) {
    display: none;
  }

  .popup-hover-bridge {
    position: fixed;
    z-index: calc(var(--sl-z-index-dropdown) - 1);
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    clip-path: polygon(
      var(--hover-bridge-top-left-x, 0) var(--hover-bridge-top-left-y, 0),
      var(--hover-bridge-top-right-x, 0) var(--hover-bridge-top-right-y, 0),
      var(--hover-bridge-bottom-right-x, 0) var(--hover-bridge-bottom-right-y, 0),
      var(--hover-bridge-bottom-left-x, 0) var(--hover-bridge-bottom-left-y, 0)
    );
  }
`
const ec = /* @__PURE__ */ new Set(),
  Ak = new MutationObserver(sf),
  In = /* @__PURE__ */ new Map()
let rf = document.documentElement.dir || 'ltr',
  nf = document.documentElement.lang || navigator.language,
  Gi
Ak.observe(document.documentElement, {
  attributes: !0,
  attributeFilter: ['dir', 'lang'],
})
function of(...t) {
  t.map(r => {
    const i = r.$code.toLowerCase()
    In.has(i)
      ? In.set(i, Object.assign(Object.assign({}, In.get(i)), r))
      : In.set(i, r),
      Gi || (Gi = r)
  }),
    sf()
}
function sf() {
  ;(rf = document.documentElement.dir || 'ltr'),
    (nf = document.documentElement.lang || navigator.language),
    [...ec.keys()].map(t => {
      typeof t.requestUpdate == 'function' && t.requestUpdate()
    })
}
let Ek = class {
  constructor(r) {
    ;(this.host = r), this.host.addController(this)
  }
  hostConnected() {
    ec.add(this.host)
  }
  hostDisconnected() {
    ec.delete(this.host)
  }
  dir() {
    return `${this.host.dir || rf}`.toLowerCase()
  }
  lang() {
    return `${this.host.lang || nf}`.toLowerCase()
  }
  getTranslationData(r) {
    var i, o
    const a = new Intl.Locale(r.replace(/_/g, '-')),
      d = a == null ? void 0 : a.language.toLowerCase(),
      h =
        (o =
          (i = a == null ? void 0 : a.region) === null || i === void 0
            ? void 0
            : i.toLowerCase()) !== null && o !== void 0
          ? o
          : '',
      m = In.get(`${d}-${h}`),
      f = In.get(d)
    return { locale: a, language: d, region: h, primary: m, secondary: f }
  }
  exists(r, i) {
    var o
    const { primary: a, secondary: d } = this.getTranslationData(
      (o = i.lang) !== null && o !== void 0 ? o : this.lang(),
    )
    return (
      (i = Object.assign({ includeFallback: !1 }, i)),
      !!((a && a[r]) || (d && d[r]) || (i.includeFallback && Gi && Gi[r]))
    )
  }
  term(r, ...i) {
    const { primary: o, secondary: a } = this.getTranslationData(this.lang())
    let d
    if (o && o[r]) d = o[r]
    else if (a && a[r]) d = a[r]
    else if (Gi && Gi[r]) d = Gi[r]
    else
      return console.error(`No translation found for: ${String(r)}`), String(r)
    return typeof d == 'function' ? d(...i) : d
  }
  date(r, i) {
    return (r = new Date(r)), new Intl.DateTimeFormat(this.lang(), i).format(r)
  }
  number(r, i) {
    return (
      (r = Number(r)),
      isNaN(r) ? '' : new Intl.NumberFormat(this.lang(), i).format(r)
    )
  }
  relativeTime(r, i, o) {
    return new Intl.RelativeTimeFormat(this.lang(), o).format(r, i)
  }
}
var af = {
  $code: 'en',
  $name: 'English',
  $dir: 'ltr',
  carousel: 'Carousel',
  clearEntry: 'Clear entry',
  close: 'Close',
  copied: 'Copied',
  copy: 'Copy',
  currentValue: 'Current value',
  error: 'Error',
  goToSlide: (t, r) => `Go to slide ${t} of ${r}`,
  hidePassword: 'Hide password',
  loading: 'Loading',
  nextSlide: 'Next slide',
  numOptionsSelected: t =>
    t === 0
      ? 'No options selected'
      : t === 1
      ? '1 option selected'
      : `${t} options selected`,
  previousSlide: 'Previous slide',
  progress: 'Progress',
  remove: 'Remove',
  resize: 'Resize',
  scrollToEnd: 'Scroll to end',
  scrollToStart: 'Scroll to start',
  selectAColorFromTheScreen: 'Select a color from the screen',
  showPassword: 'Show password',
  slideNum: t => `Slide ${t}`,
  toggleColorFormat: 'Toggle color format',
}
of(af)
var Tk = af,
  Dt = class extends Ek {}
of(Tk)
const Oi = Math.min,
  ar = Math.max,
  Gs = Math.round,
  Ds = Math.floor,
  Li = t => ({
    x: t,
    y: t,
  }),
  Ik = {
    left: 'right',
    right: 'left',
    bottom: 'top',
    top: 'bottom',
  },
  Ok = {
    start: 'end',
    end: 'start',
  }
function rc(t, r, i) {
  return ar(t, Oi(r, i))
}
function Fn(t, r) {
  return typeof t == 'function' ? t(r) : t
}
function Di(t) {
  return t.split('-')[0]
}
function Nn(t) {
  return t.split('-')[1]
}
function lf(t) {
  return t === 'x' ? 'y' : 'x'
}
function kc(t) {
  return t === 'y' ? 'height' : 'width'
}
function Ro(t) {
  return ['top', 'bottom'].includes(Di(t)) ? 'y' : 'x'
}
function $c(t) {
  return lf(Ro(t))
}
function Lk(t, r, i) {
  i === void 0 && (i = !1)
  const o = Nn(t),
    a = $c(t),
    d = kc(a)
  let h =
    a === 'x'
      ? o === (i ? 'end' : 'start')
        ? 'right'
        : 'left'
      : o === 'start'
      ? 'bottom'
      : 'top'
  return r.reference[d] > r.floating[d] && (h = Xs(h)), [h, Xs(h)]
}
function Dk(t) {
  const r = Xs(t)
  return [ic(t), r, ic(r)]
}
function ic(t) {
  return t.replace(/start|end/g, r => Ok[r])
}
function Mk(t, r, i) {
  const o = ['left', 'right'],
    a = ['right', 'left'],
    d = ['top', 'bottom'],
    h = ['bottom', 'top']
  switch (t) {
    case 'top':
    case 'bottom':
      return i ? (r ? a : o) : r ? o : a
    case 'left':
    case 'right':
      return r ? d : h
    default:
      return []
  }
}
function Rk(t, r, i, o) {
  const a = Nn(t)
  let d = Mk(Di(t), i === 'start', o)
  return a && ((d = d.map(h => h + '-' + a)), r && (d = d.concat(d.map(ic)))), d
}
function Xs(t) {
  return t.replace(/left|right|bottom|top/g, r => Ik[r])
}
function Pk(t) {
  return {
    top: 0,
    right: 0,
    bottom: 0,
    left: 0,
    ...t,
  }
}
function cf(t) {
  return typeof t != 'number'
    ? Pk(t)
    : {
        top: t,
        right: t,
        bottom: t,
        left: t,
      }
}
function js(t) {
  return {
    ...t,
    top: t.y,
    left: t.x,
    right: t.x + t.width,
    bottom: t.y + t.height,
  }
}
function Nu(t, r, i) {
  let { reference: o, floating: a } = t
  const d = Ro(r),
    h = $c(r),
    m = kc(h),
    f = Di(r),
    b = d === 'y',
    A = o.x + o.width / 2 - a.width / 2,
    y = o.y + o.height / 2 - a.height / 2,
    k = o[m] / 2 - a[m] / 2
  let S
  switch (f) {
    case 'top':
      S = {
        x: A,
        y: o.y - a.height,
      }
      break
    case 'bottom':
      S = {
        x: A,
        y: o.y + o.height,
      }
      break
    case 'right':
      S = {
        x: o.x + o.width,
        y,
      }
      break
    case 'left':
      S = {
        x: o.x - a.width,
        y,
      }
      break
    default:
      S = {
        x: o.x,
        y: o.y,
      }
  }
  switch (Nn(r)) {
    case 'start':
      S[h] -= k * (i && b ? -1 : 1)
      break
    case 'end':
      S[h] += k * (i && b ? -1 : 1)
      break
  }
  return S
}
const Bk = async (t, r, i) => {
  const {
      placement: o = 'bottom',
      strategy: a = 'absolute',
      middleware: d = [],
      platform: h,
    } = i,
    m = d.filter(Boolean),
    f = await (h.isRTL == null ? void 0 : h.isRTL(r))
  let b = await h.getElementRects({
      reference: t,
      floating: r,
      strategy: a,
    }),
    { x: A, y } = Nu(b, o, f),
    k = o,
    S = {},
    $ = 0
  for (let w = 0; w < m.length; w++) {
    const { name: T, fn: P } = m[w],
      {
        x: D,
        y: R,
        data: O,
        reset: L,
      } = await P({
        x: A,
        y,
        initialPlacement: o,
        placement: k,
        strategy: a,
        middlewareData: S,
        rects: b,
        platform: h,
        elements: {
          reference: t,
          floating: r,
        },
      })
    if (
      ((A = D ?? A),
      (y = R ?? y),
      (S = {
        ...S,
        [T]: {
          ...S[T],
          ...O,
        },
      }),
      L && $ <= 50)
    ) {
      $++,
        typeof L == 'object' &&
          (L.placement && (k = L.placement),
          L.rects &&
            (b =
              L.rects === !0
                ? await h.getElementRects({
                    reference: t,
                    floating: r,
                    strategy: a,
                  })
                : L.rects),
          ({ x: A, y } = Nu(b, k, f))),
        (w = -1)
      continue
    }
  }
  return {
    x: A,
    y,
    placement: k,
    strategy: a,
    middlewareData: S,
  }
}
async function Cc(t, r) {
  var i
  r === void 0 && (r = {})
  const { x: o, y: a, platform: d, rects: h, elements: m, strategy: f } = t,
    {
      boundary: b = 'clippingAncestors',
      rootBoundary: A = 'viewport',
      elementContext: y = 'floating',
      altBoundary: k = !1,
      padding: S = 0,
    } = Fn(r, t),
    $ = cf(S),
    T = m[k ? (y === 'floating' ? 'reference' : 'floating') : y],
    P = js(
      await d.getClippingRect({
        element:
          (i = await (d.isElement == null ? void 0 : d.isElement(T))) == null ||
          i
            ? T
            : T.contextElement ||
              (await (d.getDocumentElement == null
                ? void 0
                : d.getDocumentElement(m.floating))),
        boundary: b,
        rootBoundary: A,
        strategy: f,
      }),
    ),
    D =
      y === 'floating'
        ? {
            ...h.floating,
            x: o,
            y: a,
          }
        : h.reference,
    R = await (d.getOffsetParent == null
      ? void 0
      : d.getOffsetParent(m.floating)),
    O = (await (d.isElement == null ? void 0 : d.isElement(R)))
      ? (await (d.getScale == null ? void 0 : d.getScale(R))) || {
          x: 1,
          y: 1,
        }
      : {
          x: 1,
          y: 1,
        },
    L = js(
      d.convertOffsetParentRelativeRectToViewportRelativeRect
        ? await d.convertOffsetParentRelativeRectToViewportRelativeRect({
            rect: D,
            offsetParent: R,
            strategy: f,
          })
        : D,
    )
  return {
    top: (P.top - L.top + $.top) / O.y,
    bottom: (L.bottom - P.bottom + $.bottom) / O.y,
    left: (P.left - L.left + $.left) / O.x,
    right: (L.right - P.right + $.right) / O.x,
  }
}
const Fk = t => ({
    name: 'arrow',
    options: t,
    async fn(r) {
      const {
          x: i,
          y: o,
          placement: a,
          rects: d,
          platform: h,
          elements: m,
          middlewareData: f,
        } = r,
        { element: b, padding: A = 0 } = Fn(t, r) || {}
      if (b == null) return {}
      const y = cf(A),
        k = {
          x: i,
          y: o,
        },
        S = $c(a),
        $ = kc(S),
        w = await h.getDimensions(b),
        T = S === 'y',
        P = T ? 'top' : 'left',
        D = T ? 'bottom' : 'right',
        R = T ? 'clientHeight' : 'clientWidth',
        O = d.reference[$] + d.reference[S] - k[S] - d.floating[$],
        L = k[S] - d.reference[S],
        U = await (h.getOffsetParent == null ? void 0 : h.getOffsetParent(b))
      let H = U ? U[R] : 0
      ;(!H || !(await (h.isElement == null ? void 0 : h.isElement(U)))) &&
        (H = m.floating[R] || d.floating[$])
      const F = O / 2 - L / 2,
        K = H / 2 - w[$] / 2 - 1,
        q = Oi(y[P], K),
        et = Oi(y[D], K),
        mt = q,
        Ot = H - w[$] - et,
        Z = H / 2 - w[$] / 2 + F,
        G = rc(mt, Z, Ot),
        Y =
          !f.arrow &&
          Nn(a) != null &&
          Z != G &&
          d.reference[$] / 2 - (Z < mt ? q : et) - w[$] / 2 < 0,
        tt = Y ? (Z < mt ? Z - mt : Z - Ot) : 0
      return {
        [S]: k[S] + tt,
        data: {
          [S]: G,
          centerOffset: Z - G - tt,
          ...(Y && {
            alignmentOffset: tt,
          }),
        },
        reset: Y,
      }
    },
  }),
  Nk = function (t) {
    return (
      t === void 0 && (t = {}),
      {
        name: 'flip',
        options: t,
        async fn(r) {
          var i, o
          const {
              placement: a,
              middlewareData: d,
              rects: h,
              initialPlacement: m,
              platform: f,
              elements: b,
            } = r,
            {
              mainAxis: A = !0,
              crossAxis: y = !0,
              fallbackPlacements: k,
              fallbackStrategy: S = 'bestFit',
              fallbackAxisSideDirection: $ = 'none',
              flipAlignment: w = !0,
              ...T
            } = Fn(t, r)
          if ((i = d.arrow) != null && i.alignmentOffset) return {}
          const P = Di(a),
            D = Di(m) === m,
            R = await (f.isRTL == null ? void 0 : f.isRTL(b.floating)),
            O = k || (D || !w ? [Xs(m)] : Dk(m))
          !k && $ !== 'none' && O.push(...Rk(m, w, $, R))
          const L = [m, ...O],
            U = await Cc(r, T),
            H = []
          let F = ((o = d.flip) == null ? void 0 : o.overflows) || []
          if ((A && H.push(U[P]), y)) {
            const mt = Lk(a, h, R)
            H.push(U[mt[0]], U[mt[1]])
          }
          if (
            ((F = [
              ...F,
              {
                placement: a,
                overflows: H,
              },
            ]),
            !H.every(mt => mt <= 0))
          ) {
            var K, q
            const mt = (((K = d.flip) == null ? void 0 : K.index) || 0) + 1,
              Ot = L[mt]
            if (Ot)
              return {
                data: {
                  index: mt,
                  overflows: F,
                },
                reset: {
                  placement: Ot,
                },
              }
            let Z =
              (q = F.filter(G => G.overflows[0] <= 0).sort(
                (G, Y) => G.overflows[1] - Y.overflows[1],
              )[0]) == null
                ? void 0
                : q.placement
            if (!Z)
              switch (S) {
                case 'bestFit': {
                  var et
                  const G =
                    (et = F.map(Y => [
                      Y.placement,
                      Y.overflows
                        .filter(tt => tt > 0)
                        .reduce((tt, j) => tt + j, 0),
                    ]).sort((Y, tt) => Y[1] - tt[1])[0]) == null
                      ? void 0
                      : et[0]
                  G && (Z = G)
                  break
                }
                case 'initialPlacement':
                  Z = m
                  break
              }
            if (a !== Z)
              return {
                reset: {
                  placement: Z,
                },
              }
          }
          return {}
        },
      }
    )
  }
async function Uk(t, r) {
  const { placement: i, platform: o, elements: a } = t,
    d = await (o.isRTL == null ? void 0 : o.isRTL(a.floating)),
    h = Di(i),
    m = Nn(i),
    f = Ro(i) === 'y',
    b = ['left', 'top'].includes(h) ? -1 : 1,
    A = d && f ? -1 : 1,
    y = Fn(r, t)
  let {
    mainAxis: k,
    crossAxis: S,
    alignmentAxis: $,
  } = typeof y == 'number'
    ? {
        mainAxis: y,
        crossAxis: 0,
        alignmentAxis: null,
      }
    : {
        mainAxis: 0,
        crossAxis: 0,
        alignmentAxis: null,
        ...y,
      }
  return (
    m && typeof $ == 'number' && (S = m === 'end' ? $ * -1 : $),
    f
      ? {
          x: S * A,
          y: k * b,
        }
      : {
          x: k * b,
          y: S * A,
        }
  )
}
const Hk = function (t) {
    return (
      t === void 0 && (t = 0),
      {
        name: 'offset',
        options: t,
        async fn(r) {
          const { x: i, y: o } = r,
            a = await Uk(r, t)
          return {
            x: i + a.x,
            y: o + a.y,
            data: a,
          }
        },
      }
    )
  },
  Vk = function (t) {
    return (
      t === void 0 && (t = {}),
      {
        name: 'shift',
        options: t,
        async fn(r) {
          const { x: i, y: o, placement: a } = r,
            {
              mainAxis: d = !0,
              crossAxis: h = !1,
              limiter: m = {
                fn: T => {
                  let { x: P, y: D } = T
                  return {
                    x: P,
                    y: D,
                  }
                },
              },
              ...f
            } = Fn(t, r),
            b = {
              x: i,
              y: o,
            },
            A = await Cc(r, f),
            y = Ro(Di(a)),
            k = lf(y)
          let S = b[k],
            $ = b[y]
          if (d) {
            const T = k === 'y' ? 'top' : 'left',
              P = k === 'y' ? 'bottom' : 'right',
              D = S + A[T],
              R = S - A[P]
            S = rc(D, S, R)
          }
          if (h) {
            const T = y === 'y' ? 'top' : 'left',
              P = y === 'y' ? 'bottom' : 'right',
              D = $ + A[T],
              R = $ - A[P]
            $ = rc(D, $, R)
          }
          const w = m.fn({
            ...r,
            [k]: S,
            [y]: $,
          })
          return {
            ...w,
            data: {
              x: w.x - i,
              y: w.y - o,
            },
          }
        },
      }
    )
  },
  Uu = function (t) {
    return (
      t === void 0 && (t = {}),
      {
        name: 'size',
        options: t,
        async fn(r) {
          const { placement: i, rects: o, platform: a, elements: d } = r,
            { apply: h = () => {}, ...m } = Fn(t, r),
            f = await Cc(r, m),
            b = Di(i),
            A = Nn(i),
            y = Ro(i) === 'y',
            { width: k, height: S } = o.floating
          let $, w
          b === 'top' || b === 'bottom'
            ? (($ = b),
              (w =
                A ===
                ((await (a.isRTL == null ? void 0 : a.isRTL(d.floating)))
                  ? 'start'
                  : 'end')
                  ? 'left'
                  : 'right'))
            : ((w = b), ($ = A === 'end' ? 'top' : 'bottom'))
          const T = S - f[$],
            P = k - f[w],
            D = !r.middlewareData.shift
          let R = T,
            O = P
          if (y) {
            const U = k - f.left - f.right
            O = A || D ? Oi(P, U) : U
          } else {
            const U = S - f.top - f.bottom
            R = A || D ? Oi(T, U) : U
          }
          if (D && !A) {
            const U = ar(f.left, 0),
              H = ar(f.right, 0),
              F = ar(f.top, 0),
              K = ar(f.bottom, 0)
            y
              ? (O = k - 2 * (U !== 0 || H !== 0 ? U + H : ar(f.left, f.right)))
              : (R = S - 2 * (F !== 0 || K !== 0 ? F + K : ar(f.top, f.bottom)))
          }
          await h({
            ...r,
            availableWidth: O,
            availableHeight: R,
          })
          const L = await a.getDimensions(d.floating)
          return k !== L.width || S !== L.height
            ? {
                reset: {
                  rects: !0,
                },
              }
            : {}
        },
      }
    )
  }
function Mi(t) {
  return df(t) ? (t.nodeName || '').toLowerCase() : '#document'
}
function cr(t) {
  var r
  return (
    (t == null || (r = t.ownerDocument) == null ? void 0 : r.defaultView) ||
    window
  )
}
function gi(t) {
  var r
  return (r = (df(t) ? t.ownerDocument : t.document) || window.document) == null
    ? void 0
    : r.documentElement
}
function df(t) {
  return t instanceof Node || t instanceof cr(t).Node
}
function ui(t) {
  return t instanceof Element || t instanceof cr(t).Element
}
function Gr(t) {
  return t instanceof HTMLElement || t instanceof cr(t).HTMLElement
}
function Hu(t) {
  return typeof ShadowRoot > 'u'
    ? !1
    : t instanceof ShadowRoot || t instanceof cr(t).ShadowRoot
}
function Po(t) {
  const { overflow: r, overflowX: i, overflowY: o, display: a } = $r(t)
  return (
    /auto|scroll|overlay|hidden|clip/.test(r + o + i) &&
    !['inline', 'contents'].includes(a)
  )
}
function Wk(t) {
  return ['table', 'td', 'th'].includes(Mi(t))
}
function Sc(t) {
  const r = zc(),
    i = $r(t)
  return (
    i.transform !== 'none' ||
    i.perspective !== 'none' ||
    (i.containerType ? i.containerType !== 'normal' : !1) ||
    (!r && (i.backdropFilter ? i.backdropFilter !== 'none' : !1)) ||
    (!r && (i.filter ? i.filter !== 'none' : !1)) ||
    ['transform', 'perspective', 'filter'].some(o =>
      (i.willChange || '').includes(o),
    ) ||
    ['paint', 'layout', 'strict', 'content'].some(o =>
      (i.contain || '').includes(o),
    )
  )
}
function qk(t) {
  let r = Mn(t)
  for (; Gr(r) && !la(r); ) {
    if (Sc(r)) return r
    r = Mn(r)
  }
  return null
}
function zc() {
  return typeof CSS > 'u' || !CSS.supports
    ? !1
    : CSS.supports('-webkit-backdrop-filter', 'none')
}
function la(t) {
  return ['html', 'body', '#document'].includes(Mi(t))
}
function $r(t) {
  return cr(t).getComputedStyle(t)
}
function ca(t) {
  return ui(t)
    ? {
        scrollLeft: t.scrollLeft,
        scrollTop: t.scrollTop,
      }
    : {
        scrollLeft: t.pageXOffset,
        scrollTop: t.pageYOffset,
      }
}
function Mn(t) {
  if (Mi(t) === 'html') return t
  const r =
    // Step into the shadow DOM of the parent of a slotted node.
    t.assignedSlot || // DOM Element detected.
    t.parentNode || // ShadowRoot detected.
    (Hu(t) && t.host) || // Fallback.
    gi(t)
  return Hu(r) ? r.host : r
}
function hf(t) {
  const r = Mn(t)
  return la(r)
    ? t.ownerDocument
      ? t.ownerDocument.body
      : t.body
    : Gr(r) && Po(r)
    ? r
    : hf(r)
}
function Io(t, r, i) {
  var o
  r === void 0 && (r = []), i === void 0 && (i = !0)
  const a = hf(t),
    d = a === ((o = t.ownerDocument) == null ? void 0 : o.body),
    h = cr(a)
  return d
    ? r.concat(
        h,
        h.visualViewport || [],
        Po(a) ? a : [],
        h.frameElement && i ? Io(h.frameElement) : [],
      )
    : r.concat(a, Io(a, [], i))
}
function uf(t) {
  const r = $r(t)
  let i = parseFloat(r.width) || 0,
    o = parseFloat(r.height) || 0
  const a = Gr(t),
    d = a ? t.offsetWidth : i,
    h = a ? t.offsetHeight : o,
    m = Gs(i) !== d || Gs(o) !== h
  return (
    m && ((i = d), (o = h)),
    {
      width: i,
      height: o,
      $: m,
    }
  )
}
function Ac(t) {
  return ui(t) ? t : t.contextElement
}
function On(t) {
  const r = Ac(t)
  if (!Gr(r)) return Li(1)
  const i = r.getBoundingClientRect(),
    { width: o, height: a, $: d } = uf(r)
  let h = (d ? Gs(i.width) : i.width) / o,
    m = (d ? Gs(i.height) : i.height) / a
  return (
    (!h || !Number.isFinite(h)) && (h = 1),
    (!m || !Number.isFinite(m)) && (m = 1),
    {
      x: h,
      y: m,
    }
  )
}
const Yk = /* @__PURE__ */ Li(0)
function pf(t) {
  const r = cr(t)
  return !zc() || !r.visualViewport
    ? Yk
    : {
        x: r.visualViewport.offsetLeft,
        y: r.visualViewport.offsetTop,
      }
}
function Kk(t, r, i) {
  return r === void 0 && (r = !1), !i || (r && i !== cr(t)) ? !1 : r
}
function tn(t, r, i, o) {
  r === void 0 && (r = !1), i === void 0 && (i = !1)
  const a = t.getBoundingClientRect(),
    d = Ac(t)
  let h = Li(1)
  r && (o ? ui(o) && (h = On(o)) : (h = On(t)))
  const m = Kk(d, i, o) ? pf(d) : Li(0)
  let f = (a.left + m.x) / h.x,
    b = (a.top + m.y) / h.y,
    A = a.width / h.x,
    y = a.height / h.y
  if (d) {
    const k = cr(d),
      S = o && ui(o) ? cr(o) : o
    let $ = k.frameElement
    for (; $ && o && S !== k; ) {
      const w = On($),
        T = $.getBoundingClientRect(),
        P = $r($),
        D = T.left + ($.clientLeft + parseFloat(P.paddingLeft)) * w.x,
        R = T.top + ($.clientTop + parseFloat(P.paddingTop)) * w.y
      ;(f *= w.x),
        (b *= w.y),
        (A *= w.x),
        (y *= w.y),
        (f += D),
        (b += R),
        ($ = cr($).frameElement)
    }
  }
  return js({
    width: A,
    height: y,
    x: f,
    y: b,
  })
}
function Gk(t) {
  let { rect: r, offsetParent: i, strategy: o } = t
  const a = Gr(i),
    d = gi(i)
  if (i === d) return r
  let h = {
      scrollLeft: 0,
      scrollTop: 0,
    },
    m = Li(1)
  const f = Li(0)
  if (
    (a || (!a && o !== 'fixed')) &&
    ((Mi(i) !== 'body' || Po(d)) && (h = ca(i)), Gr(i))
  ) {
    const b = tn(i)
    ;(m = On(i)), (f.x = b.x + i.clientLeft), (f.y = b.y + i.clientTop)
  }
  return {
    width: r.width * m.x,
    height: r.height * m.y,
    x: r.x * m.x - h.scrollLeft * m.x + f.x,
    y: r.y * m.y - h.scrollTop * m.y + f.y,
  }
}
function Xk(t) {
  return Array.from(t.getClientRects())
}
function ff(t) {
  return tn(gi(t)).left + ca(t).scrollLeft
}
function jk(t) {
  const r = gi(t),
    i = ca(t),
    o = t.ownerDocument.body,
    a = ar(r.scrollWidth, r.clientWidth, o.scrollWidth, o.clientWidth),
    d = ar(r.scrollHeight, r.clientHeight, o.scrollHeight, o.clientHeight)
  let h = -i.scrollLeft + ff(t)
  const m = -i.scrollTop
  return (
    $r(o).direction === 'rtl' && (h += ar(r.clientWidth, o.clientWidth) - a),
    {
      width: a,
      height: d,
      x: h,
      y: m,
    }
  )
}
function Zk(t, r) {
  const i = cr(t),
    o = gi(t),
    a = i.visualViewport
  let d = o.clientWidth,
    h = o.clientHeight,
    m = 0,
    f = 0
  if (a) {
    ;(d = a.width), (h = a.height)
    const b = zc()
    ;(!b || (b && r === 'fixed')) && ((m = a.offsetLeft), (f = a.offsetTop))
  }
  return {
    width: d,
    height: h,
    x: m,
    y: f,
  }
}
function Jk(t, r) {
  const i = tn(t, !0, r === 'fixed'),
    o = i.top + t.clientTop,
    a = i.left + t.clientLeft,
    d = Gr(t) ? On(t) : Li(1),
    h = t.clientWidth * d.x,
    m = t.clientHeight * d.y,
    f = a * d.x,
    b = o * d.y
  return {
    width: h,
    height: m,
    x: f,
    y: b,
  }
}
function Vu(t, r, i) {
  let o
  if (r === 'viewport') o = Zk(t, i)
  else if (r === 'document') o = jk(gi(t))
  else if (ui(r)) o = Jk(r, i)
  else {
    const a = pf(t)
    o = {
      ...r,
      x: r.x - a.x,
      y: r.y - a.y,
    }
  }
  return js(o)
}
function gf(t, r) {
  const i = Mn(t)
  return i === r || !ui(i) || la(i)
    ? !1
    : $r(i).position === 'fixed' || gf(i, r)
}
function Qk(t, r) {
  const i = r.get(t)
  if (i) return i
  let o = Io(t, [], !1).filter(m => ui(m) && Mi(m) !== 'body'),
    a = null
  const d = $r(t).position === 'fixed'
  let h = d ? Mn(t) : t
  for (; ui(h) && !la(h); ) {
    const m = $r(h),
      f = Sc(h)
    !f && m.position === 'fixed' && (a = null),
      (
        d
          ? !f && !a
          : (!f &&
              m.position === 'static' &&
              !!a &&
              ['absolute', 'fixed'].includes(a.position)) ||
            (Po(h) && !f && gf(t, h))
      )
        ? (o = o.filter(A => A !== h))
        : (a = m),
      (h = Mn(h))
  }
  return r.set(t, o), o
}
function t$(t) {
  let { element: r, boundary: i, rootBoundary: o, strategy: a } = t
  const h = [...(i === 'clippingAncestors' ? Qk(r, this._c) : [].concat(i)), o],
    m = h[0],
    f = h.reduce(
      (b, A) => {
        const y = Vu(r, A, a)
        return (
          (b.top = ar(y.top, b.top)),
          (b.right = Oi(y.right, b.right)),
          (b.bottom = Oi(y.bottom, b.bottom)),
          (b.left = ar(y.left, b.left)),
          b
        )
      },
      Vu(r, m, a),
    )
  return {
    width: f.right - f.left,
    height: f.bottom - f.top,
    x: f.left,
    y: f.top,
  }
}
function e$(t) {
  return uf(t)
}
function r$(t, r, i) {
  const o = Gr(r),
    a = gi(r),
    d = i === 'fixed',
    h = tn(t, !0, d, r)
  let m = {
    scrollLeft: 0,
    scrollTop: 0,
  }
  const f = Li(0)
  if (o || (!o && !d))
    if (((Mi(r) !== 'body' || Po(a)) && (m = ca(r)), o)) {
      const b = tn(r, !0, d, r)
      ;(f.x = b.x + r.clientLeft), (f.y = b.y + r.clientTop)
    } else a && (f.x = ff(a))
  return {
    x: h.left + m.scrollLeft - f.x,
    y: h.top + m.scrollTop - f.y,
    width: h.width,
    height: h.height,
  }
}
function Wu(t, r) {
  return !Gr(t) || $r(t).position === 'fixed' ? null : r ? r(t) : t.offsetParent
}
function mf(t, r) {
  const i = cr(t)
  if (!Gr(t)) return i
  let o = Wu(t, r)
  for (; o && Wk(o) && $r(o).position === 'static'; ) o = Wu(o, r)
  return o &&
    (Mi(o) === 'html' ||
      (Mi(o) === 'body' && $r(o).position === 'static' && !Sc(o)))
    ? i
    : o || qk(t) || i
}
const i$ = async function (t) {
  let { reference: r, floating: i, strategy: o } = t
  const a = this.getOffsetParent || mf,
    d = this.getDimensions
  return {
    reference: r$(r, await a(i), o),
    floating: {
      x: 0,
      y: 0,
      ...(await d(i)),
    },
  }
}
function n$(t) {
  return $r(t).direction === 'rtl'
}
const Hs = {
  convertOffsetParentRelativeRectToViewportRelativeRect: Gk,
  getDocumentElement: gi,
  getClippingRect: t$,
  getOffsetParent: mf,
  getElementRects: i$,
  getClientRects: Xk,
  getDimensions: e$,
  getScale: On,
  isElement: ui,
  isRTL: n$,
}
function o$(t, r) {
  let i = null,
    o
  const a = gi(t)
  function d() {
    clearTimeout(o), i && i.disconnect(), (i = null)
  }
  function h(m, f) {
    m === void 0 && (m = !1), f === void 0 && (f = 1), d()
    const { left: b, top: A, width: y, height: k } = t.getBoundingClientRect()
    if ((m || r(), !y || !k)) return
    const S = Ds(A),
      $ = Ds(a.clientWidth - (b + y)),
      w = Ds(a.clientHeight - (A + k)),
      T = Ds(b),
      D = {
        rootMargin: -S + 'px ' + -$ + 'px ' + -w + 'px ' + -T + 'px',
        threshold: ar(0, Oi(1, f)) || 1,
      }
    let R = !0
    function O(L) {
      const U = L[0].intersectionRatio
      if (U !== f) {
        if (!R) return h()
        U
          ? h(!1, U)
          : (o = setTimeout(() => {
              h(!1, 1e-7)
            }, 100))
      }
      R = !1
    }
    try {
      i = new IntersectionObserver(O, {
        ...D,
        // Handle <iframe>s
        root: a.ownerDocument,
      })
    } catch {
      i = new IntersectionObserver(O, D)
    }
    i.observe(t)
  }
  return h(!0), d
}
function s$(t, r, i, o) {
  o === void 0 && (o = {})
  const {
      ancestorScroll: a = !0,
      ancestorResize: d = !0,
      elementResize: h = typeof ResizeObserver == 'function',
      layoutShift: m = typeof IntersectionObserver == 'function',
      animationFrame: f = !1,
    } = o,
    b = Ac(t),
    A = a || d ? [...(b ? Io(b) : []), ...Io(r)] : []
  A.forEach(P => {
    a &&
      P.addEventListener('scroll', i, {
        passive: !0,
      }),
      d && P.addEventListener('resize', i)
  })
  const y = b && m ? o$(b, i) : null
  let k = -1,
    S = null
  h &&
    ((S = new ResizeObserver(P => {
      let [D] = P
      D &&
        D.target === b &&
        S &&
        (S.unobserve(r),
        cancelAnimationFrame(k),
        (k = requestAnimationFrame(() => {
          S && S.observe(r)
        }))),
        i()
    })),
    b && !f && S.observe(b),
    S.observe(r))
  let $,
    w = f ? tn(t) : null
  f && T()
  function T() {
    const P = tn(t)
    w &&
      (P.x !== w.x ||
        P.y !== w.y ||
        P.width !== w.width ||
        P.height !== w.height) &&
      i(),
      (w = P),
      ($ = requestAnimationFrame(T))
  }
  return (
    i(),
    () => {
      A.forEach(P => {
        a && P.removeEventListener('scroll', i),
          d && P.removeEventListener('resize', i)
      }),
        y && y(),
        S && S.disconnect(),
        (S = null),
        f && cancelAnimationFrame($)
    }
  )
}
const a$ = (t, r, i) => {
  const o = /* @__PURE__ */ new Map(),
    a = {
      platform: Hs,
      ...i,
    },
    d = {
      ...a.platform,
      _c: o,
    }
  return Bk(t, r, {
    ...a,
    platform: d,
  })
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const Yr = {
    ATTRIBUTE: 1,
    CHILD: 2,
    PROPERTY: 3,
    BOOLEAN_ATTRIBUTE: 4,
    EVENT: 5,
    ELEMENT: 6,
  },
  Bo =
    t =>
    (...r) => ({ _$litDirective$: t, values: r })
let Fo = class {
  constructor(r) {}
  get _$AU() {
    return this._$AM._$AU
  }
  _$AT(r, i, o) {
    ;(this._$Ct = r), (this._$AM = i), (this._$Ci = o)
  }
  _$AS(r, i) {
    return this.update(r, i)
  }
  update(r, i) {
    return this.render(...i)
  }
}
/**
 * @license
 * Copyright 2018 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const ct = Bo(
  class extends Fo {
    constructor(t) {
      var r
      if (
        (super(t),
        t.type !== Yr.ATTRIBUTE ||
          t.name !== 'class' ||
          ((r = t.strings) == null ? void 0 : r.length) > 2)
      )
        throw Error(
          '`classMap()` can only be used in the `class` attribute and must be the only part in the attribute.',
        )
    }
    render(t) {
      return (
        ' ' +
        Object.keys(t)
          .filter(r => t[r])
          .join(' ') +
        ' '
      )
    }
    update(t, [r]) {
      var o, a
      if (this.st === void 0) {
        ;(this.st = /* @__PURE__ */ new Set()),
          t.strings !== void 0 &&
            (this.nt = new Set(
              t.strings
                .join(' ')
                .split(/\s/)
                .filter(d => d !== ''),
            ))
        for (const d in r)
          r[d] && !((o = this.nt) != null && o.has(d)) && this.st.add(d)
        return this.render(r)
      }
      const i = t.element.classList
      for (const d of this.st) d in r || (i.remove(d), this.st.delete(d))
      for (const d in r) {
        const h = !!r[d]
        h === this.st.has(d) ||
          ((a = this.nt) != null && a.has(d)) ||
          (h ? (i.add(d), this.st.add(d)) : (i.remove(d), this.st.delete(d)))
      }
      return lr
    }
  },
)
function l$(t) {
  return c$(t)
}
function Hl(t) {
  return t.assignedSlot
    ? t.assignedSlot
    : t.parentNode instanceof ShadowRoot
    ? t.parentNode.host
    : t.parentNode
}
function c$(t) {
  for (let r = t; r; r = Hl(r))
    if (r instanceof Element && getComputedStyle(r).display === 'none')
      return null
  for (let r = Hl(t); r; r = Hl(r)) {
    if (!(r instanceof Element)) continue
    const i = getComputedStyle(r)
    if (
      i.display !== 'contents' &&
      (i.position !== 'static' || i.filter !== 'none' || r.tagName === 'BODY')
    )
      return r
  }
  return null
}
function d$(t) {
  return (
    t !== null &&
    typeof t == 'object' &&
    'getBoundingClientRect' in t &&
    ('contextElement' in t ? t instanceof Element : !0)
  )
}
var Ut = class extends nt {
  constructor() {
    super(...arguments),
      (this.localize = new Dt(this)),
      (this.active = !1),
      (this.placement = 'top'),
      (this.strategy = 'absolute'),
      (this.distance = 0),
      (this.skidding = 0),
      (this.arrow = !1),
      (this.arrowPlacement = 'anchor'),
      (this.arrowPadding = 10),
      (this.flip = !1),
      (this.flipFallbackPlacements = ''),
      (this.flipFallbackStrategy = 'best-fit'),
      (this.flipPadding = 0),
      (this.shift = !1),
      (this.shiftPadding = 0),
      (this.autoSizePadding = 0),
      (this.hoverBridge = !1),
      (this.updateHoverBridge = () => {
        if (this.hoverBridge && this.anchorEl) {
          const t = this.anchorEl.getBoundingClientRect(),
            r = this.popup.getBoundingClientRect(),
            i =
              this.placement.includes('top') ||
              this.placement.includes('bottom')
          let o = 0,
            a = 0,
            d = 0,
            h = 0,
            m = 0,
            f = 0,
            b = 0,
            A = 0
          i
            ? t.top < r.top
              ? ((o = t.left),
                (a = t.bottom),
                (d = t.right),
                (h = t.bottom),
                (m = r.left),
                (f = r.top),
                (b = r.right),
                (A = r.top))
              : ((o = r.left),
                (a = r.bottom),
                (d = r.right),
                (h = r.bottom),
                (m = t.left),
                (f = t.top),
                (b = t.right),
                (A = t.top))
            : t.left < r.left
            ? ((o = t.right),
              (a = t.top),
              (d = r.left),
              (h = r.top),
              (m = t.right),
              (f = t.bottom),
              (b = r.left),
              (A = r.bottom))
            : ((o = r.right),
              (a = r.top),
              (d = t.left),
              (h = t.top),
              (m = r.right),
              (f = r.bottom),
              (b = t.left),
              (A = t.bottom)),
            this.style.setProperty('--hover-bridge-top-left-x', `${o}px`),
            this.style.setProperty('--hover-bridge-top-left-y', `${a}px`),
            this.style.setProperty('--hover-bridge-top-right-x', `${d}px`),
            this.style.setProperty('--hover-bridge-top-right-y', `${h}px`),
            this.style.setProperty('--hover-bridge-bottom-left-x', `${m}px`),
            this.style.setProperty('--hover-bridge-bottom-left-y', `${f}px`),
            this.style.setProperty('--hover-bridge-bottom-right-x', `${b}px`),
            this.style.setProperty('--hover-bridge-bottom-right-y', `${A}px`)
        }
      })
  }
  async connectedCallback() {
    super.connectedCallback(), await this.updateComplete, this.start()
  }
  disconnectedCallback() {
    super.disconnectedCallback(), this.stop()
  }
  async updated(t) {
    super.updated(t),
      t.has('active') && (this.active ? this.start() : this.stop()),
      t.has('anchor') && this.handleAnchorChange(),
      this.active && (await this.updateComplete, this.reposition())
  }
  async handleAnchorChange() {
    if ((await this.stop(), this.anchor && typeof this.anchor == 'string')) {
      const t = this.getRootNode()
      this.anchorEl = t.getElementById(this.anchor)
    } else
      this.anchor instanceof Element || d$(this.anchor)
        ? (this.anchorEl = this.anchor)
        : (this.anchorEl = this.querySelector('[slot="anchor"]'))
    this.anchorEl instanceof HTMLSlotElement &&
      (this.anchorEl = this.anchorEl.assignedElements({ flatten: !0 })[0]),
      this.anchorEl && this.active && this.start()
  }
  start() {
    this.anchorEl &&
      (this.cleanup = s$(this.anchorEl, this.popup, () => {
        this.reposition()
      }))
  }
  async stop() {
    return new Promise(t => {
      this.cleanup
        ? (this.cleanup(),
          (this.cleanup = void 0),
          this.removeAttribute('data-current-placement'),
          this.style.removeProperty('--auto-size-available-width'),
          this.style.removeProperty('--auto-size-available-height'),
          requestAnimationFrame(() => t()))
        : t()
    })
  }
  /** Forces the popup to recalculate and reposition itself. */
  reposition() {
    if (!this.active || !this.anchorEl) return
    const t = [
      // The offset middleware goes first
      Hk({ mainAxis: this.distance, crossAxis: this.skidding }),
    ]
    this.sync
      ? t.push(
          Uu({
            apply: ({ rects: i }) => {
              const o = this.sync === 'width' || this.sync === 'both',
                a = this.sync === 'height' || this.sync === 'both'
              ;(this.popup.style.width = o ? `${i.reference.width}px` : ''),
                (this.popup.style.height = a ? `${i.reference.height}px` : '')
            },
          }),
        )
      : ((this.popup.style.width = ''), (this.popup.style.height = '')),
      this.flip &&
        t.push(
          Nk({
            boundary: this.flipBoundary,
            // @ts-expect-error - We're converting a string attribute to an array here
            fallbackPlacements: this.flipFallbackPlacements,
            fallbackStrategy:
              this.flipFallbackStrategy === 'best-fit'
                ? 'bestFit'
                : 'initialPlacement',
            padding: this.flipPadding,
          }),
        ),
      this.shift &&
        t.push(
          Vk({
            boundary: this.shiftBoundary,
            padding: this.shiftPadding,
          }),
        ),
      this.autoSize
        ? t.push(
            Uu({
              boundary: this.autoSizeBoundary,
              padding: this.autoSizePadding,
              apply: ({ availableWidth: i, availableHeight: o }) => {
                this.autoSize === 'vertical' || this.autoSize === 'both'
                  ? this.style.setProperty(
                      '--auto-size-available-height',
                      `${o}px`,
                    )
                  : this.style.removeProperty('--auto-size-available-height'),
                  this.autoSize === 'horizontal' || this.autoSize === 'both'
                    ? this.style.setProperty(
                        '--auto-size-available-width',
                        `${i}px`,
                      )
                    : this.style.removeProperty('--auto-size-available-width')
              },
            }),
          )
        : (this.style.removeProperty('--auto-size-available-width'),
          this.style.removeProperty('--auto-size-available-height')),
      this.arrow &&
        t.push(
          Fk({
            element: this.arrowEl,
            padding: this.arrowPadding,
          }),
        )
    const r =
      this.strategy === 'absolute'
        ? i => Hs.getOffsetParent(i, l$)
        : Hs.getOffsetParent
    a$(this.anchorEl, this.popup, {
      placement: this.placement,
      middleware: t,
      strategy: this.strategy,
      platform: Lo(pi({}, Hs), {
        getOffsetParent: r,
      }),
    }).then(({ x: i, y: o, middlewareData: a, placement: d }) => {
      const h = this.localize.dir() === 'rtl',
        m = { top: 'bottom', right: 'left', bottom: 'top', left: 'right' }[
          d.split('-')[0]
        ]
      if (
        (this.setAttribute('data-current-placement', d),
        Object.assign(this.popup.style, {
          left: `${i}px`,
          top: `${o}px`,
        }),
        this.arrow)
      ) {
        const f = a.arrow.x,
          b = a.arrow.y
        let A = '',
          y = '',
          k = '',
          S = ''
        if (this.arrowPlacement === 'start') {
          const $ =
            typeof f == 'number'
              ? `calc(${this.arrowPadding}px - var(--arrow-padding-offset))`
              : ''
          ;(A =
            typeof b == 'number'
              ? `calc(${this.arrowPadding}px - var(--arrow-padding-offset))`
              : ''),
            (y = h ? $ : ''),
            (S = h ? '' : $)
        } else if (this.arrowPlacement === 'end') {
          const $ =
            typeof f == 'number'
              ? `calc(${this.arrowPadding}px - var(--arrow-padding-offset))`
              : ''
          ;(y = h ? '' : $),
            (S = h ? $ : ''),
            (k =
              typeof b == 'number'
                ? `calc(${this.arrowPadding}px - var(--arrow-padding-offset))`
                : '')
        } else
          this.arrowPlacement === 'center'
            ? ((S =
                typeof f == 'number'
                  ? 'calc(50% - var(--arrow-size-diagonal))'
                  : ''),
              (A =
                typeof b == 'number'
                  ? 'calc(50% - var(--arrow-size-diagonal))'
                  : ''))
            : ((S = typeof f == 'number' ? `${f}px` : ''),
              (A = typeof b == 'number' ? `${b}px` : ''))
        Object.assign(this.arrowEl.style, {
          top: A,
          right: y,
          bottom: k,
          left: S,
          [m]: 'calc(var(--arrow-size-diagonal) * -1)',
        })
      }
    }),
      requestAnimationFrame(() => this.updateHoverBridge()),
      this.emit('sl-reposition')
  }
  render() {
    return C`
      <slot name="anchor" @slotchange=${this.handleAnchorChange}></slot>

      <span
        part="hover-bridge"
        class=${ct({
          'popup-hover-bridge': !0,
          'popup-hover-bridge--visible': this.hoverBridge && this.active,
        })}
      ></span>

      <div
        part="popup"
        class=${ct({
          popup: !0,
          'popup--active': this.active,
          'popup--fixed': this.strategy === 'fixed',
          'popup--has-arrow': this.arrow,
        })}
      >
        <slot></slot>
        ${
          this.arrow
            ? C`<div part="arrow" class="popup__arrow" role="presentation"></div>`
            : ''
        }
      </div>
    `
  }
}
Ut.styles = [dt, zk]
c([J('.popup')], Ut.prototype, 'popup', 2)
c([J('.popup__arrow')], Ut.prototype, 'arrowEl', 2)
c([p()], Ut.prototype, 'anchor', 2)
c([p({ type: Boolean, reflect: !0 })], Ut.prototype, 'active', 2)
c([p({ reflect: !0 })], Ut.prototype, 'placement', 2)
c([p({ reflect: !0 })], Ut.prototype, 'strategy', 2)
c([p({ type: Number })], Ut.prototype, 'distance', 2)
c([p({ type: Number })], Ut.prototype, 'skidding', 2)
c([p({ type: Boolean })], Ut.prototype, 'arrow', 2)
c([p({ attribute: 'arrow-placement' })], Ut.prototype, 'arrowPlacement', 2)
c(
  [p({ attribute: 'arrow-padding', type: Number })],
  Ut.prototype,
  'arrowPadding',
  2,
)
c([p({ type: Boolean })], Ut.prototype, 'flip', 2)
c(
  [
    p({
      attribute: 'flip-fallback-placements',
      converter: {
        fromAttribute: t =>
          t
            .split(' ')
            .map(r => r.trim())
            .filter(r => r !== ''),
        toAttribute: t => t.join(' '),
      },
    }),
  ],
  Ut.prototype,
  'flipFallbackPlacements',
  2,
)
c(
  [p({ attribute: 'flip-fallback-strategy' })],
  Ut.prototype,
  'flipFallbackStrategy',
  2,
)
c([p({ type: Object })], Ut.prototype, 'flipBoundary', 2)
c(
  [p({ attribute: 'flip-padding', type: Number })],
  Ut.prototype,
  'flipPadding',
  2,
)
c([p({ type: Boolean })], Ut.prototype, 'shift', 2)
c([p({ type: Object })], Ut.prototype, 'shiftBoundary', 2)
c(
  [p({ attribute: 'shift-padding', type: Number })],
  Ut.prototype,
  'shiftPadding',
  2,
)
c([p({ attribute: 'auto-size' })], Ut.prototype, 'autoSize', 2)
c([p()], Ut.prototype, 'sync', 2)
c([p({ type: Object })], Ut.prototype, 'autoSizeBoundary', 2)
c(
  [p({ attribute: 'auto-size-padding', type: Number })],
  Ut.prototype,
  'autoSizePadding',
  2,
)
c(
  [p({ attribute: 'hover-bridge', type: Boolean })],
  Ut.prototype,
  'hoverBridge',
  2,
)
var vf = /* @__PURE__ */ new Map(),
  h$ = /* @__PURE__ */ new WeakMap()
function u$(t) {
  return t ?? { keyframes: [], options: { duration: 0 } }
}
function qu(t, r) {
  return r.toLowerCase() === 'rtl'
    ? {
        keyframes: t.rtlKeyframes || t.keyframes,
        options: t.options,
      }
    : t
}
function Mt(t, r) {
  vf.set(t, u$(r))
}
function Xt(t, r, i) {
  const o = h$.get(t)
  if (o != null && o[r]) return qu(o[r], i.dir)
  const a = vf.get(r)
  return a
    ? qu(a, i.dir)
    : {
        keyframes: [],
        options: { duration: 0 },
      }
}
function Ue(t, r) {
  return new Promise(i => {
    function o(a) {
      a.target === t && (t.removeEventListener(r, o), i())
    }
    t.addEventListener(r, o)
  })
}
function ie(t, r, i) {
  return new Promise(o => {
    if ((i == null ? void 0 : i.duration) === 1 / 0)
      throw new Error('Promise-based animations must be finite.')
    const a = t.animate(
      r,
      Lo(pi({}, i), {
        duration: nc() ? 0 : i.duration,
      }),
    )
    a.addEventListener('cancel', o, { once: !0 }),
      a.addEventListener('finish', o, { once: !0 })
  })
}
function Yu(t) {
  return (
    (t = t.toString().toLowerCase()),
    t.indexOf('ms') > -1
      ? parseFloat(t)
      : t.indexOf('s') > -1
      ? parseFloat(t) * 1e3
      : parseFloat(t)
  )
}
function nc() {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches
}
function pe(t) {
  return Promise.all(
    t.getAnimations().map(
      r =>
        new Promise(i => {
          r.cancel(), requestAnimationFrame(i)
        }),
    ),
  )
}
function Zs(t, r) {
  return t.map(i =>
    Lo(pi({}, i), {
      height: i.height === 'auto' ? `${r}px` : i.height,
    }),
  )
}
var le = class extends nt {
  constructor() {
    super(),
      (this.localize = new Dt(this)),
      (this.content = ''),
      (this.placement = 'top'),
      (this.disabled = !1),
      (this.distance = 8),
      (this.open = !1),
      (this.skidding = 0),
      (this.trigger = 'hover focus'),
      (this.hoist = !1),
      (this.handleBlur = () => {
        this.hasTrigger('focus') && this.hide()
      }),
      (this.handleClick = () => {
        this.hasTrigger('click') && (this.open ? this.hide() : this.show())
      }),
      (this.handleFocus = () => {
        this.hasTrigger('focus') && this.show()
      }),
      (this.handleDocumentKeyDown = t => {
        t.key === 'Escape' && (t.stopPropagation(), this.hide())
      }),
      (this.handleMouseOver = () => {
        if (this.hasTrigger('hover')) {
          const t = Yu(getComputedStyle(this).getPropertyValue('--show-delay'))
          clearTimeout(this.hoverTimeout),
            (this.hoverTimeout = window.setTimeout(() => this.show(), t))
        }
      }),
      (this.handleMouseOut = () => {
        if (this.hasTrigger('hover')) {
          const t = Yu(getComputedStyle(this).getPropertyValue('--hide-delay'))
          clearTimeout(this.hoverTimeout),
            (this.hoverTimeout = window.setTimeout(() => this.hide(), t))
        }
      }),
      this.addEventListener('blur', this.handleBlur, !0),
      this.addEventListener('focus', this.handleFocus, !0),
      this.addEventListener('click', this.handleClick),
      this.addEventListener('mouseover', this.handleMouseOver),
      this.addEventListener('mouseout', this.handleMouseOut)
  }
  disconnectedCallback() {
    var t
    super.disconnectedCallback(),
      (t = this.closeWatcher) == null || t.destroy(),
      document.removeEventListener('keydown', this.handleDocumentKeyDown)
  }
  firstUpdated() {
    ;(this.body.hidden = !this.open),
      this.open && ((this.popup.active = !0), this.popup.reposition())
  }
  hasTrigger(t) {
    return this.trigger.split(' ').includes(t)
  }
  async handleOpenChange() {
    var t, r
    if (this.open) {
      if (this.disabled) return
      this.emit('sl-show'),
        'CloseWatcher' in window
          ? ((t = this.closeWatcher) == null || t.destroy(),
            (this.closeWatcher = new CloseWatcher()),
            (this.closeWatcher.onclose = () => {
              this.hide()
            }))
          : document.addEventListener('keydown', this.handleDocumentKeyDown),
        await pe(this.body),
        (this.body.hidden = !1),
        (this.popup.active = !0)
      const { keyframes: i, options: o } = Xt(this, 'tooltip.show', {
        dir: this.localize.dir(),
      })
      await ie(this.popup.popup, i, o),
        this.popup.reposition(),
        this.emit('sl-after-show')
    } else {
      this.emit('sl-hide'),
        (r = this.closeWatcher) == null || r.destroy(),
        document.removeEventListener('keydown', this.handleDocumentKeyDown),
        await pe(this.body)
      const { keyframes: i, options: o } = Xt(this, 'tooltip.hide', {
        dir: this.localize.dir(),
      })
      await ie(this.popup.popup, i, o),
        (this.popup.active = !1),
        (this.body.hidden = !0),
        this.emit('sl-after-hide')
    }
  }
  async handleOptionsChange() {
    this.hasUpdated && (await this.updateComplete, this.popup.reposition())
  }
  handleDisabledChange() {
    this.disabled && this.open && this.hide()
  }
  /** Shows the tooltip. */
  async show() {
    if (!this.open) return (this.open = !0), Ue(this, 'sl-after-show')
  }
  /** Hides the tooltip */
  async hide() {
    if (this.open) return (this.open = !1), Ue(this, 'sl-after-hide')
  }
  //
  // NOTE: Tooltip is a bit unique in that we're using aria-live instead of aria-labelledby to trick screen readers into
  // announcing the content. It works really well, but it violates an accessibility rule. We're also adding the
  // aria-describedby attribute to a slot, which is required by <sl-popup> to correctly locate the first assigned
  // element, otherwise positioning is incorrect.
  //
  render() {
    return C`
      <sl-popup
        part="base"
        exportparts="
          popup:base__popup,
          arrow:base__arrow
        "
        class=${ct({
          tooltip: !0,
          'tooltip--open': this.open,
        })}
        placement=${this.placement}
        distance=${this.distance}
        skidding=${this.skidding}
        strategy=${this.hoist ? 'fixed' : 'absolute'}
        flip
        shift
        arrow
        hover-bridge
      >
        ${''}
        <slot slot="anchor" aria-describedby="tooltip"></slot>

        ${''}
        <div part="body" id="tooltip" class="tooltip__body" role="tooltip" aria-live=${
          this.open ? 'polite' : 'off'
        }>
          <slot name="content">${this.content}</slot>
        </div>
      </sl-popup>
    `
  }
}
le.styles = [dt, Sk]
le.dependencies = { 'sl-popup': Ut }
c([J('slot:not([name])')], le.prototype, 'defaultSlot', 2)
c([J('.tooltip__body')], le.prototype, 'body', 2)
c([J('sl-popup')], le.prototype, 'popup', 2)
c([p()], le.prototype, 'content', 2)
c([p()], le.prototype, 'placement', 2)
c([p({ type: Boolean, reflect: !0 })], le.prototype, 'disabled', 2)
c([p({ type: Number })], le.prototype, 'distance', 2)
c([p({ type: Boolean, reflect: !0 })], le.prototype, 'open', 2)
c([p({ type: Number })], le.prototype, 'skidding', 2)
c([p()], le.prototype, 'trigger', 2)
c([p({ type: Boolean })], le.prototype, 'hoist', 2)
c(
  [X('open', { waitUntilFirstUpdate: !0 })],
  le.prototype,
  'handleOpenChange',
  1,
)
c(
  [X(['content', 'distance', 'hoist', 'placement', 'skidding'])],
  le.prototype,
  'handleOptionsChange',
  1,
)
c([X('disabled')], le.prototype, 'handleDisabledChange', 1)
Mt('tooltip.show', {
  keyframes: [
    { opacity: 0, scale: 0.8 },
    { opacity: 1, scale: 1 },
  ],
  options: { duration: 150, easing: 'ease' },
})
Mt('tooltip.hide', {
  keyframes: [
    { opacity: 1, scale: 1 },
    { opacity: 0, scale: 0.8 },
  ],
  options: { duration: 150, easing: 'ease' },
})
const p$ = `:host {
  --max-width: 100%;
  --show-delay: 0;
  --hide-delay: 0;
}
sl-popup::part(popup) {
  pointer-events: none;
}
:host([enterable]) sl-popup::part(popup) {
  pointer-events: all;
}
`
Mt('tooltip.show', {
  keyframes: [{ opacity: 0 }, { opacity: 1 }],
  options: { duration: 150, easing: 'ease-in-out' },
})
Mt('tooltip.hide', {
  keyframes: [{ opacity: 1 }, { opacity: 0 }],
  options: { duration: 200, transorm: '', easing: 'ease-in-out' },
})
class Ku extends dr(le, nn) {
  constructor() {
    super(), (this.enterable = !1)
  }
}
W(Ku, 'styles', [Bt(), le.styles, ft(p$)]),
  W(Ku, 'properties', {
    ...le.properties,
    enterable: { type: Boolean, reflect: !0 },
  })
const f$ = `:host {
  --information-font-size: var(--font-size);
  --information-color: var(--color-header);

  display: block;
}
:host-context([mode='dark']) {
  --information-color: var(--color-gray-200);
}
[part='base'] {
  color: var(--information-color);
  font-weight: var(--text-semibold);
  display: flex;
  gap: var(--step);
  align-items: center;
  white-space: nowrap;
  font-size: var(--information-font-size);
}
[part='info'] tbk-icon {
  cursor: pointer;
}
tbk-tooltip::part(base__popup) {
  color: var(--tooltip-text);
  font-size: var(--text-xs);
  font-weight: var(--font-weight);
  background: var(--tooltip-background);
  padding: var(--step-2) var(--step-3);
  white-space: wrap;
  border-radius: var(--tooltip-border-radius);
  box-shadow: var(--tooltip-shadow);
  z-index: var(--layer-high);
}
`
class Gu extends de {
  constructor() {
    super(),
      (this.text = ''),
      (this.side = Jt.Right),
      (this.size = Lt.S),
      (this._hasTooltip = !1)
  }
  _renderInfo() {
    return this._hasTooltip || this.text
      ? C`
        <tbk-tooltip
          part="info"
          placement="right"
          hoist
        >
          <div slot="content">
            <slot
              @slotchange="${this.handleSlotchange}"
              name="content"
            ></slot>
            ${this.text}
          </div>

          <tbk-icon
            library="heroicons"
            name="information-circle"
          ></tbk-icon>
        </tbk-tooltip>
      `
      : C`<slot
        @slotchange="${this.handleSlotchange}"
        name="content"
      ></slot>`
  }
  handleSlotchange() {
    const r = this.shadowRoot
      .querySelector('slot[name="content"]')
      .assignedElements()
    r.length > 0
      ? (this._hasTooltip = r.some(
          i => i.tagName !== 'SLOT' || i.assignedElements().length > 0,
        ))
      : (this._hasTooltip = !1)
  }
  render() {
    return C`
      <span part="base">
        ${lt(this.side === Jt.Left, this._renderInfo())}
        <slot></slot>
        ${lt(this.side === Jt.Right, this._renderInfo())}
      </span>
    `
  }
}
W(Gu, 'styles', [Bt(), ge(), ft(f$)]),
  W(Gu, 'properties', {
    text: { type: String },
    side: { type: String, reflect: !0 },
    size: { type: String, reflect: !0 },
    _hasTooltip: { type: Boolean, state: !0 },
  })
const g$ = `:host {
  --text-block-font-size: var(--font-size);
  --text-block-color: var(--color-variant);
  --text-block-color-content: var(--color-gray-400);

  display: block;
}
:host-context([mode='dark']) {
  --text-block-color: var(--color-gray-200);
  --text-block-color-content: var(--color-gray-300);
}
:host([inherit]) {
  --text-block-color: inherit;
}
slot:empty {
  position: absolute;
}
[part='base'] {
  display: flex;
  align-items: center;
  gap: var(--step-2);
}
[part='headline'] {
  display: flex;
  align-items: center;
  gap: var(--step);
}
[part='headline'] > tbk-information::part(base) {
  display: inline-flex;
  color: var(--text-block-color);
  font-size: var(--text-block-font-size);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
[part='content'] {
  display: flex;
  flex-direction: column;
  gap: var(--step);
  overflow: hidden;
}
[part='content'] > small {
  color: var(--text-block-color-content);
  font-size: var(--text-xs);
  font-weight: normal;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
}
[part='text'] {
  display: block;
  color: var(--text-block-color);
}
`
class Xu extends de {
  constructor() {
    super(),
      (this.size = Lt.M),
      (this.variant = $t.Neutral),
      (this.inherit = !1)
  }
  render() {
    return C`
      <div part="base">
        <slot name="before"></slot>
        <div part="content">
          ${lt(
            this.headline,
            C`
              <span part="headline">
                <slot name="badge"></slot>
                <tbk-information
                  .text="${this.description}"
                  .size="${this.size}"
                  >${this.headline}</tbk-information
                >
              </span>
            `,
          )}
          ${lt(this.tagline, C`<small>${this.tagline}</small>`)}
          <slot part="text"></slot>
        </div>
        <slot name="after"></slot>
      </div>
    `
  }
}
W(Xu, 'styles', [Bt(), ge(), Xr(), ft(g$)]),
  W(Xu, 'properties', {
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    headline: { type: String },
    tagline: { type: String },
    description: { type: String },
    inherit: { type: Boolean, reflect: !0 },
  })
const m$ = `:host {
  --model-name-font-size: var(--font-size);
  --model-name-font-weight: var(--font-weight);
  --model-name-font-family: var(--font-accent);
  --model-name-color-icon: var(--color-gray-500);
  --model-name-color-icon-main: var(--color-deep-blue-800);
  --model-name-background-highlighted: var(--color-gray-5);
  --model-name-background-highlighted-hover: var(--color-gray-10);
  --model-name-color-model: var(--color-deep-blue-500);
  --model-name-color-schema: var(--color-deep-blue-600);
  --model-name-color-catalog: var(--color-deep-blue-800);
  --model-name-color-reduced: var(--color-gray);

  display: inline-flex;
  align-items: center;
  gap: var(--step);
  min-height: var(--step-2);
  line-height: 1;
  white-space: nowrap;
  font-family: var(--model-name-font-family);
  font-weight: var(--model-name-font-weight);
}
:host-context([mode='dark']) {
  --model-name-color-icon-main: var(--color-deep-blue-100);
  --model-name-background-highlighted: var(--color-gray-10);
  --model-name-background-highlighted-hover: var(--color-gray-5);
  --model-name-color-model: var(--color-deep-blue-200);
  --model-name-color-schema: var(--color-deep-blue-300);
  --model-name-color-catalog: var(--color-deep-blue-400);
  --model-name-color-reduced: var(--color-gray);
}
[part='icon'] {
  display: flex;
  flex-shrink: 0;
  font-size: calc(var(--model-name-font-size) * 1.25);
  color: var(--model-name-color-icon-main);
}
[part='text'] {
  width: 100%;
  display: inline-flex;
  overflow: hidden;
  border-radius: var(--radius-2xs);
}
[part='catalog'] > span,
[part='schema'] > span,
[part='model'] > span {
  display: inline-flex;
  align-items: center;
  font-size: var(--model-name-font-size);
  background: transparent;
  padding: 0;
  border-radius: none;
}
:host([highlighted]) [part='catalog'] > span,
:host([highlighted]) [part='schema'] > span,
:host([highlighted]) [part='model'] > span {
  background: var(--model-name-background-highlighted);
  padding: var(--half) var(--step);
  border-radius: var(--radius-2xs);
}
[part='model'] > span {
  display: inline-block;
  text-overflow: ellipsis;
  overflow: hidden;
}
[part='catalog'],
[part='schema'] {
  display: flex;
  align-items: center;
}
[part='icon'],
[part='model'] {
  overflow: hidden;
  display: flex;
  align-items: center;
  color: var(--model-name-color-model);
}
[part='schema'] {
  color: var(--model-name-color-schema);
}
[part='catalog'] {
  color: var(--model-name-color-catalog);
}
:host([reduce-color]) [part='schema'],
:host([reduce-color]) [part='catalog'] {
  color: var(--model-name-color-reduced);
}
[part='schema'] > small,
[part='catalog'] > small {
  line-height: 1;
}
[part='hidden'] {
  display: inline-flex;
  z-index: -1;
  opacity: 0;
  visibility: hidden;
  position: fixed;
  bottom: 0;
  top: 0;
  font-size: var(--model-name-font-size);
}
tbk-icon[part='ellipsis'] {
  background: var(--model-name-background-highlighted);
  flex-shrink: 0;
  padding: 0 var(--step);
  border-radius: var(--radius-2xs);
}
tbk-icon[part='ellipsis']:hover {
  background: var(--model-name-background-highlighted-hover);
}
tbk-tooltip::part(base__popup) {
  color: var(--tooltip-text);
  font-size: var(--text-xs);
  font-weight: var(--font-weight);
  background: var(--tooltip-background);
  padding: var(--step) var(--step-2);
  white-space: wrap;
  border-radius: var(--radius-2xs);
  box-shadow: var(--tooltip-shadow);
  z-index: var(--layer-high);
  overflow: hidden;
  max-width: 80%;
  overflow-wrap: break-word;
}
small[part='ellipsis'] {
  display: flex;
  justify-content: center;
  align-items: center;
  padding: 0 var(--step);
  background: var(--model-name-background-highlighted);
  border-radius: var(--radius-2xs);
}
`
var Js = { exports: {} }
/**
 * @license
 * Lodash <https://lodash.com/>
 * Copyright OpenJS Foundation and other contributors <https://openjsf.org/>
 * Released under MIT license <https://lodash.com/license>
 * Based on Underscore.js 1.8.3 <http://underscorejs.org/LICENSE>
 * Copyright Jeremy Ashkenas, DocumentCloud and Investigative Reporters & Editors
 */
Js.exports
;(function (t, r) {
  ;(function () {
    var i,
      o = '4.17.21',
      a = 200,
      d = 'Unsupported core-js use. Try https://npms.io/search?q=ponyfill.',
      h = 'Expected a function',
      m = 'Invalid `variable` option passed into `_.template`',
      f = '__lodash_hash_undefined__',
      b = 500,
      A = '__lodash_placeholder__',
      y = 1,
      k = 2,
      S = 4,
      $ = 1,
      w = 2,
      T = 1,
      P = 2,
      D = 4,
      R = 8,
      O = 16,
      L = 32,
      U = 64,
      H = 128,
      F = 256,
      K = 512,
      q = 30,
      et = '...',
      mt = 800,
      Ot = 16,
      Z = 1,
      G = 2,
      Y = 3,
      tt = 1 / 0,
      j = 9007199254740991,
      yt = 17976931348623157e292,
      gt = NaN,
      St = 4294967295,
      jt = St - 1,
      he = St >>> 1,
      we = [
        ['ary', H],
        ['bind', T],
        ['bindKey', P],
        ['curry', R],
        ['curryRight', O],
        ['flip', K],
        ['partial', L],
        ['partialRight', U],
        ['rearg', F],
      ],
      me = '[object Arguments]',
      Cr = '[object Array]',
      ti = '[object AsyncFunction]',
      Sr = '[object Boolean]',
      Je = '[object Date]',
      Te = '[object DOMException]',
      Qe = '[object Error]',
      Nr = '[object Function]',
      Pi = '[object GeneratorFunction]',
      zr = '[object Map]',
      Wn = '[object Number]',
      If = '[object Null]',
      ei = '[object Object]',
      Lc = '[object Promise]',
      Of = '[object Proxy]',
      qn = '[object RegExp]',
      Ar = '[object Set]',
      Yn = '[object String]',
      Vo = '[object Symbol]',
      Lf = '[object Undefined]',
      Kn = '[object WeakMap]',
      Df = '[object WeakSet]',
      Gn = '[object ArrayBuffer]',
      pn = '[object DataView]',
      pa = '[object Float32Array]',
      fa = '[object Float64Array]',
      ga = '[object Int8Array]',
      ma = '[object Int16Array]',
      va = '[object Int32Array]',
      ba = '[object Uint8Array]',
      ya = '[object Uint8ClampedArray]',
      _a = '[object Uint16Array]',
      wa = '[object Uint32Array]',
      Mf = /\b__p \+= '';/g,
      Rf = /\b(__p \+=) '' \+/g,
      Pf = /(__e\(.*?\)|\b__t\)) \+\n'';/g,
      Dc = /&(?:amp|lt|gt|quot|#39);/g,
      Mc = /[&<>"']/g,
      Bf = RegExp(Dc.source),
      Ff = RegExp(Mc.source),
      Nf = /<%-([\s\S]+?)%>/g,
      Uf = /<%([\s\S]+?)%>/g,
      Rc = /<%=([\s\S]+?)%>/g,
      Hf = /\.|\[(?:[^[\]]*|(["'])(?:(?!\1)[^\\]|\\.)*?\1)\]/,
      Vf = /^\w*$/,
      Wf =
        /[^.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\\]|\\.)*?)\2)\]|(?=(?:\.|\[\])(?:\.|\[\]|$))/g,
      xa = /[\\^$.*+?()[\]{}|]/g,
      qf = RegExp(xa.source),
      ka = /^\s+/,
      Yf = /\s/,
      Kf = /\{(?:\n\/\* \[wrapped with .+\] \*\/)?\n?/,
      Gf = /\{\n\/\* \[wrapped with (.+)\] \*/,
      Xf = /,? & /,
      jf = /[^\x00-\x2f\x3a-\x40\x5b-\x60\x7b-\x7f]+/g,
      Zf = /[()=,{}\[\]\/\s]/,
      Jf = /\\(\\)?/g,
      Qf = /\$\{([^\\}]*(?:\\.[^\\}]*)*)\}/g,
      Pc = /\w*$/,
      tg = /^[-+]0x[0-9a-f]+$/i,
      eg = /^0b[01]+$/i,
      rg = /^\[object .+?Constructor\]$/,
      ig = /^0o[0-7]+$/i,
      ng = /^(?:0|[1-9]\d*)$/,
      og = /[\xc0-\xd6\xd8-\xf6\xf8-\xff\u0100-\u017f]/g,
      Wo = /($^)/,
      sg = /['\n\r\u2028\u2029\\]/g,
      qo = '\\ud800-\\udfff',
      ag = '\\u0300-\\u036f',
      lg = '\\ufe20-\\ufe2f',
      cg = '\\u20d0-\\u20ff',
      Bc = ag + lg + cg,
      Fc = '\\u2700-\\u27bf',
      Nc = 'a-z\\xdf-\\xf6\\xf8-\\xff',
      dg = '\\xac\\xb1\\xd7\\xf7',
      hg = '\\x00-\\x2f\\x3a-\\x40\\x5b-\\x60\\x7b-\\xbf',
      ug = '\\u2000-\\u206f',
      pg =
        ' \\t\\x0b\\f\\xa0\\ufeff\\n\\r\\u2028\\u2029\\u1680\\u180e\\u2000\\u2001\\u2002\\u2003\\u2004\\u2005\\u2006\\u2007\\u2008\\u2009\\u200a\\u202f\\u205f\\u3000',
      Uc = 'A-Z\\xc0-\\xd6\\xd8-\\xde',
      Hc = '\\ufe0e\\ufe0f',
      Vc = dg + hg + ug + pg,
      $a = "['’]",
      fg = '[' + qo + ']',
      Wc = '[' + Vc + ']',
      Yo = '[' + Bc + ']',
      qc = '\\d+',
      gg = '[' + Fc + ']',
      Yc = '[' + Nc + ']',
      Kc = '[^' + qo + Vc + qc + Fc + Nc + Uc + ']',
      Ca = '\\ud83c[\\udffb-\\udfff]',
      mg = '(?:' + Yo + '|' + Ca + ')',
      Gc = '[^' + qo + ']',
      Sa = '(?:\\ud83c[\\udde6-\\uddff]){2}',
      za = '[\\ud800-\\udbff][\\udc00-\\udfff]',
      fn = '[' + Uc + ']',
      Xc = '\\u200d',
      jc = '(?:' + Yc + '|' + Kc + ')',
      vg = '(?:' + fn + '|' + Kc + ')',
      Zc = '(?:' + $a + '(?:d|ll|m|re|s|t|ve))?',
      Jc = '(?:' + $a + '(?:D|LL|M|RE|S|T|VE))?',
      Qc = mg + '?',
      td = '[' + Hc + ']?',
      bg = '(?:' + Xc + '(?:' + [Gc, Sa, za].join('|') + ')' + td + Qc + ')*',
      yg = '\\d*(?:1st|2nd|3rd|(?![123])\\dth)(?=\\b|[A-Z_])',
      _g = '\\d*(?:1ST|2ND|3RD|(?![123])\\dTH)(?=\\b|[a-z_])',
      ed = td + Qc + bg,
      wg = '(?:' + [gg, Sa, za].join('|') + ')' + ed,
      xg = '(?:' + [Gc + Yo + '?', Yo, Sa, za, fg].join('|') + ')',
      kg = RegExp($a, 'g'),
      $g = RegExp(Yo, 'g'),
      Aa = RegExp(Ca + '(?=' + Ca + ')|' + xg + ed, 'g'),
      Cg = RegExp(
        [
          fn + '?' + Yc + '+' + Zc + '(?=' + [Wc, fn, '$'].join('|') + ')',
          vg + '+' + Jc + '(?=' + [Wc, fn + jc, '$'].join('|') + ')',
          fn + '?' + jc + '+' + Zc,
          fn + '+' + Jc,
          _g,
          yg,
          qc,
          wg,
        ].join('|'),
        'g',
      ),
      Sg = RegExp('[' + Xc + qo + Bc + Hc + ']'),
      zg = /[a-z][A-Z]|[A-Z]{2}[a-z]|[0-9][a-zA-Z]|[a-zA-Z][0-9]|[^a-zA-Z0-9 ]/,
      Ag = [
        'Array',
        'Buffer',
        'DataView',
        'Date',
        'Error',
        'Float32Array',
        'Float64Array',
        'Function',
        'Int8Array',
        'Int16Array',
        'Int32Array',
        'Map',
        'Math',
        'Object',
        'Promise',
        'RegExp',
        'Set',
        'String',
        'Symbol',
        'TypeError',
        'Uint8Array',
        'Uint8ClampedArray',
        'Uint16Array',
        'Uint32Array',
        'WeakMap',
        '_',
        'clearTimeout',
        'isFinite',
        'parseInt',
        'setTimeout',
      ],
      Eg = -1,
      Yt = {}
    ;(Yt[pa] =
      Yt[fa] =
      Yt[ga] =
      Yt[ma] =
      Yt[va] =
      Yt[ba] =
      Yt[ya] =
      Yt[_a] =
      Yt[wa] =
        !0),
      (Yt[me] =
        Yt[Cr] =
        Yt[Gn] =
        Yt[Sr] =
        Yt[pn] =
        Yt[Je] =
        Yt[Qe] =
        Yt[Nr] =
        Yt[zr] =
        Yt[Wn] =
        Yt[ei] =
        Yt[qn] =
        Yt[Ar] =
        Yt[Yn] =
        Yt[Kn] =
          !1)
    var qt = {}
    ;(qt[me] =
      qt[Cr] =
      qt[Gn] =
      qt[pn] =
      qt[Sr] =
      qt[Je] =
      qt[pa] =
      qt[fa] =
      qt[ga] =
      qt[ma] =
      qt[va] =
      qt[zr] =
      qt[Wn] =
      qt[ei] =
      qt[qn] =
      qt[Ar] =
      qt[Yn] =
      qt[Vo] =
      qt[ba] =
      qt[ya] =
      qt[_a] =
      qt[wa] =
        !0),
      (qt[Qe] = qt[Nr] = qt[Kn] = !1)
    var Tg = {
        // Latin-1 Supplement block.
        À: 'A',
        Á: 'A',
        Â: 'A',
        Ã: 'A',
        Ä: 'A',
        Å: 'A',
        à: 'a',
        á: 'a',
        â: 'a',
        ã: 'a',
        ä: 'a',
        å: 'a',
        Ç: 'C',
        ç: 'c',
        Ð: 'D',
        ð: 'd',
        È: 'E',
        É: 'E',
        Ê: 'E',
        Ë: 'E',
        è: 'e',
        é: 'e',
        ê: 'e',
        ë: 'e',
        Ì: 'I',
        Í: 'I',
        Î: 'I',
        Ï: 'I',
        ì: 'i',
        í: 'i',
        î: 'i',
        ï: 'i',
        Ñ: 'N',
        ñ: 'n',
        Ò: 'O',
        Ó: 'O',
        Ô: 'O',
        Õ: 'O',
        Ö: 'O',
        Ø: 'O',
        ò: 'o',
        ó: 'o',
        ô: 'o',
        õ: 'o',
        ö: 'o',
        ø: 'o',
        Ù: 'U',
        Ú: 'U',
        Û: 'U',
        Ü: 'U',
        ù: 'u',
        ú: 'u',
        û: 'u',
        ü: 'u',
        Ý: 'Y',
        ý: 'y',
        ÿ: 'y',
        Æ: 'Ae',
        æ: 'ae',
        Þ: 'Th',
        þ: 'th',
        ß: 'ss',
        // Latin Extended-A block.
        Ā: 'A',
        Ă: 'A',
        Ą: 'A',
        ā: 'a',
        ă: 'a',
        ą: 'a',
        Ć: 'C',
        Ĉ: 'C',
        Ċ: 'C',
        Č: 'C',
        ć: 'c',
        ĉ: 'c',
        ċ: 'c',
        č: 'c',
        Ď: 'D',
        Đ: 'D',
        ď: 'd',
        đ: 'd',
        Ē: 'E',
        Ĕ: 'E',
        Ė: 'E',
        Ę: 'E',
        Ě: 'E',
        ē: 'e',
        ĕ: 'e',
        ė: 'e',
        ę: 'e',
        ě: 'e',
        Ĝ: 'G',
        Ğ: 'G',
        Ġ: 'G',
        Ģ: 'G',
        ĝ: 'g',
        ğ: 'g',
        ġ: 'g',
        ģ: 'g',
        Ĥ: 'H',
        Ħ: 'H',
        ĥ: 'h',
        ħ: 'h',
        Ĩ: 'I',
        Ī: 'I',
        Ĭ: 'I',
        Į: 'I',
        İ: 'I',
        ĩ: 'i',
        ī: 'i',
        ĭ: 'i',
        į: 'i',
        ı: 'i',
        Ĵ: 'J',
        ĵ: 'j',
        Ķ: 'K',
        ķ: 'k',
        ĸ: 'k',
        Ĺ: 'L',
        Ļ: 'L',
        Ľ: 'L',
        Ŀ: 'L',
        Ł: 'L',
        ĺ: 'l',
        ļ: 'l',
        ľ: 'l',
        ŀ: 'l',
        ł: 'l',
        Ń: 'N',
        Ņ: 'N',
        Ň: 'N',
        Ŋ: 'N',
        ń: 'n',
        ņ: 'n',
        ň: 'n',
        ŋ: 'n',
        Ō: 'O',
        Ŏ: 'O',
        Ő: 'O',
        ō: 'o',
        ŏ: 'o',
        ő: 'o',
        Ŕ: 'R',
        Ŗ: 'R',
        Ř: 'R',
        ŕ: 'r',
        ŗ: 'r',
        ř: 'r',
        Ś: 'S',
        Ŝ: 'S',
        Ş: 'S',
        Š: 'S',
        ś: 's',
        ŝ: 's',
        ş: 's',
        š: 's',
        Ţ: 'T',
        Ť: 'T',
        Ŧ: 'T',
        ţ: 't',
        ť: 't',
        ŧ: 't',
        Ũ: 'U',
        Ū: 'U',
        Ŭ: 'U',
        Ů: 'U',
        Ű: 'U',
        Ų: 'U',
        ũ: 'u',
        ū: 'u',
        ŭ: 'u',
        ů: 'u',
        ű: 'u',
        ų: 'u',
        Ŵ: 'W',
        ŵ: 'w',
        Ŷ: 'Y',
        ŷ: 'y',
        Ÿ: 'Y',
        Ź: 'Z',
        Ż: 'Z',
        Ž: 'Z',
        ź: 'z',
        ż: 'z',
        ž: 'z',
        Ĳ: 'IJ',
        ĳ: 'ij',
        Œ: 'Oe',
        œ: 'oe',
        ŉ: "'n",
        ſ: 's',
      },
      Ig = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
      },
      Og = {
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&#39;': "'",
      },
      Lg = {
        '\\': '\\',
        "'": "'",
        '\n': 'n',
        '\r': 'r',
        '\u2028': 'u2028',
        '\u2029': 'u2029',
      },
      Dg = parseFloat,
      Mg = parseInt,
      rd = typeof Ge == 'object' && Ge && Ge.Object === Object && Ge,
      Rg = typeof self == 'object' && self && self.Object === Object && self,
      Ce = rd || Rg || Function('return this')(),
      Ea = r && !r.nodeType && r,
      Bi = Ea && !0 && t && !t.nodeType && t,
      id = Bi && Bi.exports === Ea,
      Ta = id && rd.process,
      gr = (function () {
        try {
          var z = Bi && Bi.require && Bi.require('util').types
          return z || (Ta && Ta.binding && Ta.binding('util'))
        } catch {}
      })(),
      nd = gr && gr.isArrayBuffer,
      od = gr && gr.isDate,
      sd = gr && gr.isMap,
      ad = gr && gr.isRegExp,
      ld = gr && gr.isSet,
      cd = gr && gr.isTypedArray
    function tr(z, M, I) {
      switch (I.length) {
        case 0:
          return z.call(M)
        case 1:
          return z.call(M, I[0])
        case 2:
          return z.call(M, I[0], I[1])
        case 3:
          return z.call(M, I[0], I[1], I[2])
      }
      return z.apply(M, I)
    }
    function Pg(z, M, I, rt) {
      for (var vt = -1, Pt = z == null ? 0 : z.length; ++vt < Pt; ) {
        var ve = z[vt]
        M(rt, ve, I(ve), z)
      }
      return rt
    }
    function mr(z, M) {
      for (
        var I = -1, rt = z == null ? 0 : z.length;
        ++I < rt && M(z[I], I, z) !== !1;

      );
      return z
    }
    function Bg(z, M) {
      for (var I = z == null ? 0 : z.length; I-- && M(z[I], I, z) !== !1; );
      return z
    }
    function dd(z, M) {
      for (var I = -1, rt = z == null ? 0 : z.length; ++I < rt; )
        if (!M(z[I], I, z)) return !1
      return !0
    }
    function bi(z, M) {
      for (
        var I = -1, rt = z == null ? 0 : z.length, vt = 0, Pt = [];
        ++I < rt;

      ) {
        var ve = z[I]
        M(ve, I, z) && (Pt[vt++] = ve)
      }
      return Pt
    }
    function Ko(z, M) {
      var I = z == null ? 0 : z.length
      return !!I && gn(z, M, 0) > -1
    }
    function Ia(z, M, I) {
      for (var rt = -1, vt = z == null ? 0 : z.length; ++rt < vt; )
        if (I(M, z[rt])) return !0
      return !1
    }
    function Kt(z, M) {
      for (
        var I = -1, rt = z == null ? 0 : z.length, vt = Array(rt);
        ++I < rt;

      )
        vt[I] = M(z[I], I, z)
      return vt
    }
    function yi(z, M) {
      for (var I = -1, rt = M.length, vt = z.length; ++I < rt; )
        z[vt + I] = M[I]
      return z
    }
    function Oa(z, M, I, rt) {
      var vt = -1,
        Pt = z == null ? 0 : z.length
      for (rt && Pt && (I = z[++vt]); ++vt < Pt; ) I = M(I, z[vt], vt, z)
      return I
    }
    function Fg(z, M, I, rt) {
      var vt = z == null ? 0 : z.length
      for (rt && vt && (I = z[--vt]); vt--; ) I = M(I, z[vt], vt, z)
      return I
    }
    function La(z, M) {
      for (var I = -1, rt = z == null ? 0 : z.length; ++I < rt; )
        if (M(z[I], I, z)) return !0
      return !1
    }
    var Ng = Da('length')
    function Ug(z) {
      return z.split('')
    }
    function Hg(z) {
      return z.match(jf) || []
    }
    function hd(z, M, I) {
      var rt
      return (
        I(z, function (vt, Pt, ve) {
          if (M(vt, Pt, ve)) return (rt = Pt), !1
        }),
        rt
      )
    }
    function Go(z, M, I, rt) {
      for (var vt = z.length, Pt = I + (rt ? 1 : -1); rt ? Pt-- : ++Pt < vt; )
        if (M(z[Pt], Pt, z)) return Pt
      return -1
    }
    function gn(z, M, I) {
      return M === M ? tm(z, M, I) : Go(z, ud, I)
    }
    function Vg(z, M, I, rt) {
      for (var vt = I - 1, Pt = z.length; ++vt < Pt; )
        if (rt(z[vt], M)) return vt
      return -1
    }
    function ud(z) {
      return z !== z
    }
    function pd(z, M) {
      var I = z == null ? 0 : z.length
      return I ? Ra(z, M) / I : gt
    }
    function Da(z) {
      return function (M) {
        return M == null ? i : M[z]
      }
    }
    function Ma(z) {
      return function (M) {
        return z == null ? i : z[M]
      }
    }
    function fd(z, M, I, rt, vt) {
      return (
        vt(z, function (Pt, ve, Wt) {
          I = rt ? ((rt = !1), Pt) : M(I, Pt, ve, Wt)
        }),
        I
      )
    }
    function Wg(z, M) {
      var I = z.length
      for (z.sort(M); I--; ) z[I] = z[I].value
      return z
    }
    function Ra(z, M) {
      for (var I, rt = -1, vt = z.length; ++rt < vt; ) {
        var Pt = M(z[rt])
        Pt !== i && (I = I === i ? Pt : I + Pt)
      }
      return I
    }
    function Pa(z, M) {
      for (var I = -1, rt = Array(z); ++I < z; ) rt[I] = M(I)
      return rt
    }
    function qg(z, M) {
      return Kt(M, function (I) {
        return [I, z[I]]
      })
    }
    function gd(z) {
      return z && z.slice(0, yd(z) + 1).replace(ka, '')
    }
    function er(z) {
      return function (M) {
        return z(M)
      }
    }
    function Ba(z, M) {
      return Kt(M, function (I) {
        return z[I]
      })
    }
    function Xn(z, M) {
      return z.has(M)
    }
    function md(z, M) {
      for (var I = -1, rt = z.length; ++I < rt && gn(M, z[I], 0) > -1; );
      return I
    }
    function vd(z, M) {
      for (var I = z.length; I-- && gn(M, z[I], 0) > -1; );
      return I
    }
    function Yg(z, M) {
      for (var I = z.length, rt = 0; I--; ) z[I] === M && ++rt
      return rt
    }
    var Kg = Ma(Tg),
      Gg = Ma(Ig)
    function Xg(z) {
      return '\\' + Lg[z]
    }
    function jg(z, M) {
      return z == null ? i : z[M]
    }
    function mn(z) {
      return Sg.test(z)
    }
    function Zg(z) {
      return zg.test(z)
    }
    function Jg(z) {
      for (var M, I = []; !(M = z.next()).done; ) I.push(M.value)
      return I
    }
    function Fa(z) {
      var M = -1,
        I = Array(z.size)
      return (
        z.forEach(function (rt, vt) {
          I[++M] = [vt, rt]
        }),
        I
      )
    }
    function bd(z, M) {
      return function (I) {
        return z(M(I))
      }
    }
    function _i(z, M) {
      for (var I = -1, rt = z.length, vt = 0, Pt = []; ++I < rt; ) {
        var ve = z[I]
        ;(ve === M || ve === A) && ((z[I] = A), (Pt[vt++] = I))
      }
      return Pt
    }
    function Xo(z) {
      var M = -1,
        I = Array(z.size)
      return (
        z.forEach(function (rt) {
          I[++M] = rt
        }),
        I
      )
    }
    function Qg(z) {
      var M = -1,
        I = Array(z.size)
      return (
        z.forEach(function (rt) {
          I[++M] = [rt, rt]
        }),
        I
      )
    }
    function tm(z, M, I) {
      for (var rt = I - 1, vt = z.length; ++rt < vt; )
        if (z[rt] === M) return rt
      return -1
    }
    function em(z, M, I) {
      for (var rt = I + 1; rt--; ) if (z[rt] === M) return rt
      return rt
    }
    function vn(z) {
      return mn(z) ? im(z) : Ng(z)
    }
    function Er(z) {
      return mn(z) ? nm(z) : Ug(z)
    }
    function yd(z) {
      for (var M = z.length; M-- && Yf.test(z.charAt(M)); );
      return M
    }
    var rm = Ma(Og)
    function im(z) {
      for (var M = (Aa.lastIndex = 0); Aa.test(z); ) ++M
      return M
    }
    function nm(z) {
      return z.match(Aa) || []
    }
    function om(z) {
      return z.match(Cg) || []
    }
    var sm = function z(M) {
        M = M == null ? Ce : bn.defaults(Ce.Object(), M, bn.pick(Ce, Ag))
        var I = M.Array,
          rt = M.Date,
          vt = M.Error,
          Pt = M.Function,
          ve = M.Math,
          Wt = M.Object,
          Na = M.RegExp,
          am = M.String,
          vr = M.TypeError,
          jo = I.prototype,
          lm = Pt.prototype,
          yn = Wt.prototype,
          Zo = M['__core-js_shared__'],
          Jo = lm.toString,
          Vt = yn.hasOwnProperty,
          cm = 0,
          _d = (function () {
            var e = /[^.]+$/.exec((Zo && Zo.keys && Zo.keys.IE_PROTO) || '')
            return e ? 'Symbol(src)_1.' + e : ''
          })(),
          Qo = yn.toString,
          dm = Jo.call(Wt),
          hm = Ce._,
          um = Na(
            '^' +
              Jo.call(Vt)
                .replace(xa, '\\$&')
                .replace(
                  /hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g,
                  '$1.*?',
                ) +
              '$',
          ),
          ts = id ? M.Buffer : i,
          wi = M.Symbol,
          es = M.Uint8Array,
          wd = ts ? ts.allocUnsafe : i,
          rs = bd(Wt.getPrototypeOf, Wt),
          xd = Wt.create,
          kd = yn.propertyIsEnumerable,
          is = jo.splice,
          $d = wi ? wi.isConcatSpreadable : i,
          jn = wi ? wi.iterator : i,
          Fi = wi ? wi.toStringTag : i,
          ns = (function () {
            try {
              var e = Wi(Wt, 'defineProperty')
              return e({}, '', {}), e
            } catch {}
          })(),
          pm = M.clearTimeout !== Ce.clearTimeout && M.clearTimeout,
          fm = rt && rt.now !== Ce.Date.now && rt.now,
          gm = M.setTimeout !== Ce.setTimeout && M.setTimeout,
          os = ve.ceil,
          ss = ve.floor,
          Ua = Wt.getOwnPropertySymbols,
          mm = ts ? ts.isBuffer : i,
          Cd = M.isFinite,
          vm = jo.join,
          bm = bd(Wt.keys, Wt),
          be = ve.max,
          Ie = ve.min,
          ym = rt.now,
          _m = M.parseInt,
          Sd = ve.random,
          wm = jo.reverse,
          Ha = Wi(M, 'DataView'),
          Zn = Wi(M, 'Map'),
          Va = Wi(M, 'Promise'),
          _n = Wi(M, 'Set'),
          Jn = Wi(M, 'WeakMap'),
          Qn = Wi(Wt, 'create'),
          as = Jn && new Jn(),
          wn = {},
          xm = qi(Ha),
          km = qi(Zn),
          $m = qi(Va),
          Cm = qi(_n),
          Sm = qi(Jn),
          ls = wi ? wi.prototype : i,
          to = ls ? ls.valueOf : i,
          zd = ls ? ls.toString : i
        function g(e) {
          if (ee(e) && !_t(e) && !(e instanceof At)) {
            if (e instanceof br) return e
            if (Vt.call(e, '__wrapped__')) return Ah(e)
          }
          return new br(e)
        }
        var xn = /* @__PURE__ */ (function () {
          function e() {}
          return function (n) {
            if (!Zt(n)) return {}
            if (xd) return xd(n)
            e.prototype = n
            var s = new e()
            return (e.prototype = i), s
          }
        })()
        function cs() {}
        function br(e, n) {
          ;(this.__wrapped__ = e),
            (this.__actions__ = []),
            (this.__chain__ = !!n),
            (this.__index__ = 0),
            (this.__values__ = i)
        }
        ;(g.templateSettings = {
          /**
           * Used to detect `data` property values to be HTML-escaped.
           *
           * @memberOf _.templateSettings
           * @type {RegExp}
           */
          escape: Nf,
          /**
           * Used to detect code to be evaluated.
           *
           * @memberOf _.templateSettings
           * @type {RegExp}
           */
          evaluate: Uf,
          /**
           * Used to detect `data` property values to inject.
           *
           * @memberOf _.templateSettings
           * @type {RegExp}
           */
          interpolate: Rc,
          /**
           * Used to reference the data object in the template text.
           *
           * @memberOf _.templateSettings
           * @type {string}
           */
          variable: '',
          /**
           * Used to import variables into the compiled template.
           *
           * @memberOf _.templateSettings
           * @type {Object}
           */
          imports: {
            /**
             * A reference to the `lodash` function.
             *
             * @memberOf _.templateSettings.imports
             * @type {Function}
             */
            _: g,
          },
        }),
          (g.prototype = cs.prototype),
          (g.prototype.constructor = g),
          (br.prototype = xn(cs.prototype)),
          (br.prototype.constructor = br)
        function At(e) {
          ;(this.__wrapped__ = e),
            (this.__actions__ = []),
            (this.__dir__ = 1),
            (this.__filtered__ = !1),
            (this.__iteratees__ = []),
            (this.__takeCount__ = St),
            (this.__views__ = [])
        }
        function zm() {
          var e = new At(this.__wrapped__)
          return (
            (e.__actions__ = Ve(this.__actions__)),
            (e.__dir__ = this.__dir__),
            (e.__filtered__ = this.__filtered__),
            (e.__iteratees__ = Ve(this.__iteratees__)),
            (e.__takeCount__ = this.__takeCount__),
            (e.__views__ = Ve(this.__views__)),
            e
          )
        }
        function Am() {
          if (this.__filtered__) {
            var e = new At(this)
            ;(e.__dir__ = -1), (e.__filtered__ = !0)
          } else (e = this.clone()), (e.__dir__ *= -1)
          return e
        }
        function Em() {
          var e = this.__wrapped__.value(),
            n = this.__dir__,
            s = _t(e),
            l = n < 0,
            u = s ? e.length : 0,
            v = Uv(0, u, this.__views__),
            _ = v.start,
            x = v.end,
            E = x - _,
            B = l ? x : _ - 1,
            N = this.__iteratees__,
            V = N.length,
            Q = 0,
            ot = Ie(E, this.__takeCount__)
          if (!s || (!l && u == E && ot == E)) return Zd(e, this.__actions__)
          var ut = []
          t: for (; E-- && Q < ot; ) {
            B += n
            for (var xt = -1, pt = e[B]; ++xt < V; ) {
              var zt = N[xt],
                Et = zt.iteratee,
                nr = zt.type,
                Pe = Et(pt)
              if (nr == G) pt = Pe
              else if (!Pe) {
                if (nr == Z) continue t
                break t
              }
            }
            ut[Q++] = pt
          }
          return ut
        }
        ;(At.prototype = xn(cs.prototype)), (At.prototype.constructor = At)
        function Ni(e) {
          var n = -1,
            s = e == null ? 0 : e.length
          for (this.clear(); ++n < s; ) {
            var l = e[n]
            this.set(l[0], l[1])
          }
        }
        function Tm() {
          ;(this.__data__ = Qn ? Qn(null) : {}), (this.size = 0)
        }
        function Im(e) {
          var n = this.has(e) && delete this.__data__[e]
          return (this.size -= n ? 1 : 0), n
        }
        function Om(e) {
          var n = this.__data__
          if (Qn) {
            var s = n[e]
            return s === f ? i : s
          }
          return Vt.call(n, e) ? n[e] : i
        }
        function Lm(e) {
          var n = this.__data__
          return Qn ? n[e] !== i : Vt.call(n, e)
        }
        function Dm(e, n) {
          var s = this.__data__
          return (
            (this.size += this.has(e) ? 0 : 1),
            (s[e] = Qn && n === i ? f : n),
            this
          )
        }
        ;(Ni.prototype.clear = Tm),
          (Ni.prototype.delete = Im),
          (Ni.prototype.get = Om),
          (Ni.prototype.has = Lm),
          (Ni.prototype.set = Dm)
        function ri(e) {
          var n = -1,
            s = e == null ? 0 : e.length
          for (this.clear(); ++n < s; ) {
            var l = e[n]
            this.set(l[0], l[1])
          }
        }
        function Mm() {
          ;(this.__data__ = []), (this.size = 0)
        }
        function Rm(e) {
          var n = this.__data__,
            s = ds(n, e)
          if (s < 0) return !1
          var l = n.length - 1
          return s == l ? n.pop() : is.call(n, s, 1), --this.size, !0
        }
        function Pm(e) {
          var n = this.__data__,
            s = ds(n, e)
          return s < 0 ? i : n[s][1]
        }
        function Bm(e) {
          return ds(this.__data__, e) > -1
        }
        function Fm(e, n) {
          var s = this.__data__,
            l = ds(s, e)
          return l < 0 ? (++this.size, s.push([e, n])) : (s[l][1] = n), this
        }
        ;(ri.prototype.clear = Mm),
          (ri.prototype.delete = Rm),
          (ri.prototype.get = Pm),
          (ri.prototype.has = Bm),
          (ri.prototype.set = Fm)
        function ii(e) {
          var n = -1,
            s = e == null ? 0 : e.length
          for (this.clear(); ++n < s; ) {
            var l = e[n]
            this.set(l[0], l[1])
          }
        }
        function Nm() {
          ;(this.size = 0),
            (this.__data__ = {
              hash: new Ni(),
              map: new (Zn || ri)(),
              string: new Ni(),
            })
        }
        function Um(e) {
          var n = xs(this, e).delete(e)
          return (this.size -= n ? 1 : 0), n
        }
        function Hm(e) {
          return xs(this, e).get(e)
        }
        function Vm(e) {
          return xs(this, e).has(e)
        }
        function Wm(e, n) {
          var s = xs(this, e),
            l = s.size
          return s.set(e, n), (this.size += s.size == l ? 0 : 1), this
        }
        ;(ii.prototype.clear = Nm),
          (ii.prototype.delete = Um),
          (ii.prototype.get = Hm),
          (ii.prototype.has = Vm),
          (ii.prototype.set = Wm)
        function Ui(e) {
          var n = -1,
            s = e == null ? 0 : e.length
          for (this.__data__ = new ii(); ++n < s; ) this.add(e[n])
        }
        function qm(e) {
          return this.__data__.set(e, f), this
        }
        function Ym(e) {
          return this.__data__.has(e)
        }
        ;(Ui.prototype.add = Ui.prototype.push = qm), (Ui.prototype.has = Ym)
        function Tr(e) {
          var n = (this.__data__ = new ri(e))
          this.size = n.size
        }
        function Km() {
          ;(this.__data__ = new ri()), (this.size = 0)
        }
        function Gm(e) {
          var n = this.__data__,
            s = n.delete(e)
          return (this.size = n.size), s
        }
        function Xm(e) {
          return this.__data__.get(e)
        }
        function jm(e) {
          return this.__data__.has(e)
        }
        function Zm(e, n) {
          var s = this.__data__
          if (s instanceof ri) {
            var l = s.__data__
            if (!Zn || l.length < a - 1)
              return l.push([e, n]), (this.size = ++s.size), this
            s = this.__data__ = new ii(l)
          }
          return s.set(e, n), (this.size = s.size), this
        }
        ;(Tr.prototype.clear = Km),
          (Tr.prototype.delete = Gm),
          (Tr.prototype.get = Xm),
          (Tr.prototype.has = jm),
          (Tr.prototype.set = Zm)
        function Ad(e, n) {
          var s = _t(e),
            l = !s && Yi(e),
            u = !s && !l && Si(e),
            v = !s && !l && !u && Sn(e),
            _ = s || l || u || v,
            x = _ ? Pa(e.length, am) : [],
            E = x.length
          for (var B in e)
            (n || Vt.call(e, B)) &&
              !(
                _ && // Safari 9 has enumerable `arguments.length` in strict mode.
                (B == 'length' || // Node.js 0.10 has enumerable non-index properties on buffers.
                  (u && (B == 'offset' || B == 'parent')) || // PhantomJS 2 has enumerable non-index properties on typed arrays.
                  (v &&
                    (B == 'buffer' ||
                      B == 'byteLength' ||
                      B == 'byteOffset')) || // Skip index properties.
                  ai(B, E))
              ) &&
              x.push(B)
          return x
        }
        function Ed(e) {
          var n = e.length
          return n ? e[tl(0, n - 1)] : i
        }
        function Jm(e, n) {
          return ks(Ve(e), Hi(n, 0, e.length))
        }
        function Qm(e) {
          return ks(Ve(e))
        }
        function Wa(e, n, s) {
          ;((s !== i && !Ir(e[n], s)) || (s === i && !(n in e))) && ni(e, n, s)
        }
        function eo(e, n, s) {
          var l = e[n]
          ;(!(Vt.call(e, n) && Ir(l, s)) || (s === i && !(n in e))) &&
            ni(e, n, s)
        }
        function ds(e, n) {
          for (var s = e.length; s--; ) if (Ir(e[s][0], n)) return s
          return -1
        }
        function tv(e, n, s, l) {
          return (
            xi(e, function (u, v, _) {
              n(l, u, s(u), _)
            }),
            l
          )
        }
        function Td(e, n) {
          return e && Hr(n, xe(n), e)
        }
        function ev(e, n) {
          return e && Hr(n, qe(n), e)
        }
        function ni(e, n, s) {
          n == '__proto__' && ns
            ? ns(e, n, {
                configurable: !0,
                enumerable: !0,
                value: s,
                writable: !0,
              })
            : (e[n] = s)
        }
        function qa(e, n) {
          for (var s = -1, l = n.length, u = I(l), v = e == null; ++s < l; )
            u[s] = v ? i : Cl(e, n[s])
          return u
        }
        function Hi(e, n, s) {
          return (
            e === e &&
              (s !== i && (e = e <= s ? e : s),
              n !== i && (e = e >= n ? e : n)),
            e
          )
        }
        function yr(e, n, s, l, u, v) {
          var _,
            x = n & y,
            E = n & k,
            B = n & S
          if ((s && (_ = u ? s(e, l, u, v) : s(e)), _ !== i)) return _
          if (!Zt(e)) return e
          var N = _t(e)
          if (N) {
            if (((_ = Vv(e)), !x)) return Ve(e, _)
          } else {
            var V = Oe(e),
              Q = V == Nr || V == Pi
            if (Si(e)) return th(e, x)
            if (V == ei || V == me || (Q && !u)) {
              if (((_ = E || Q ? {} : yh(e)), !x))
                return E ? Ov(e, ev(_, e)) : Iv(e, Td(_, e))
            } else {
              if (!qt[V]) return u ? e : {}
              _ = Wv(e, V, x)
            }
          }
          v || (v = new Tr())
          var ot = v.get(e)
          if (ot) return ot
          v.set(e, _),
            Gh(e)
              ? e.forEach(function (pt) {
                  _.add(yr(pt, n, s, pt, e, v))
                })
              : Yh(e) &&
                e.forEach(function (pt, zt) {
                  _.set(zt, yr(pt, n, s, zt, e, v))
                })
          var ut = B ? (E ? hl : dl) : E ? qe : xe,
            xt = N ? i : ut(e)
          return (
            mr(xt || e, function (pt, zt) {
              xt && ((zt = pt), (pt = e[zt])), eo(_, zt, yr(pt, n, s, zt, e, v))
            }),
            _
          )
        }
        function rv(e) {
          var n = xe(e)
          return function (s) {
            return Id(s, e, n)
          }
        }
        function Id(e, n, s) {
          var l = s.length
          if (e == null) return !l
          for (e = Wt(e); l--; ) {
            var u = s[l],
              v = n[u],
              _ = e[u]
            if ((_ === i && !(u in e)) || !v(_)) return !1
          }
          return !0
        }
        function Od(e, n, s) {
          if (typeof e != 'function') throw new vr(h)
          return lo(function () {
            e.apply(i, s)
          }, n)
        }
        function ro(e, n, s, l) {
          var u = -1,
            v = Ko,
            _ = !0,
            x = e.length,
            E = [],
            B = n.length
          if (!x) return E
          s && (n = Kt(n, er(s))),
            l
              ? ((v = Ia), (_ = !1))
              : n.length >= a && ((v = Xn), (_ = !1), (n = new Ui(n)))
          t: for (; ++u < x; ) {
            var N = e[u],
              V = s == null ? N : s(N)
            if (((N = l || N !== 0 ? N : 0), _ && V === V)) {
              for (var Q = B; Q--; ) if (n[Q] === V) continue t
              E.push(N)
            } else v(n, V, l) || E.push(N)
          }
          return E
        }
        var xi = oh(Ur),
          Ld = oh(Ka, !0)
        function iv(e, n) {
          var s = !0
          return (
            xi(e, function (l, u, v) {
              return (s = !!n(l, u, v)), s
            }),
            s
          )
        }
        function hs(e, n, s) {
          for (var l = -1, u = e.length; ++l < u; ) {
            var v = e[l],
              _ = n(v)
            if (_ != null && (x === i ? _ === _ && !ir(_) : s(_, x)))
              var x = _,
                E = v
          }
          return E
        }
        function nv(e, n, s, l) {
          var u = e.length
          for (
            s = wt(s),
              s < 0 && (s = -s > u ? 0 : u + s),
              l = l === i || l > u ? u : wt(l),
              l < 0 && (l += u),
              l = s > l ? 0 : jh(l);
            s < l;

          )
            e[s++] = n
          return e
        }
        function Dd(e, n) {
          var s = []
          return (
            xi(e, function (l, u, v) {
              n(l, u, v) && s.push(l)
            }),
            s
          )
        }
        function Se(e, n, s, l, u) {
          var v = -1,
            _ = e.length
          for (s || (s = Yv), u || (u = []); ++v < _; ) {
            var x = e[v]
            n > 0 && s(x)
              ? n > 1
                ? Se(x, n - 1, s, l, u)
                : yi(u, x)
              : l || (u[u.length] = x)
          }
          return u
        }
        var Ya = sh(),
          Md = sh(!0)
        function Ur(e, n) {
          return e && Ya(e, n, xe)
        }
        function Ka(e, n) {
          return e && Md(e, n, xe)
        }
        function us(e, n) {
          return bi(n, function (s) {
            return li(e[s])
          })
        }
        function Vi(e, n) {
          n = $i(n, e)
          for (var s = 0, l = n.length; e != null && s < l; ) e = e[Vr(n[s++])]
          return s && s == l ? e : i
        }
        function Rd(e, n, s) {
          var l = n(e)
          return _t(e) ? l : yi(l, s(e))
        }
        function Me(e) {
          return e == null
            ? e === i
              ? Lf
              : If
            : Fi && Fi in Wt(e)
            ? Nv(e)
            : Qv(e)
        }
        function Ga(e, n) {
          return e > n
        }
        function ov(e, n) {
          return e != null && Vt.call(e, n)
        }
        function sv(e, n) {
          return e != null && n in Wt(e)
        }
        function av(e, n, s) {
          return e >= Ie(n, s) && e < be(n, s)
        }
        function Xa(e, n, s) {
          for (
            var l = s ? Ia : Ko,
              u = e[0].length,
              v = e.length,
              _ = v,
              x = I(v),
              E = 1 / 0,
              B = [];
            _--;

          ) {
            var N = e[_]
            _ && n && (N = Kt(N, er(n))),
              (E = Ie(N.length, E)),
              (x[_] =
                !s && (n || (u >= 120 && N.length >= 120)) ? new Ui(_ && N) : i)
          }
          N = e[0]
          var V = -1,
            Q = x[0]
          t: for (; ++V < u && B.length < E; ) {
            var ot = N[V],
              ut = n ? n(ot) : ot
            if (
              ((ot = s || ot !== 0 ? ot : 0), !(Q ? Xn(Q, ut) : l(B, ut, s)))
            ) {
              for (_ = v; --_; ) {
                var xt = x[_]
                if (!(xt ? Xn(xt, ut) : l(e[_], ut, s))) continue t
              }
              Q && Q.push(ut), B.push(ot)
            }
          }
          return B
        }
        function lv(e, n, s, l) {
          return (
            Ur(e, function (u, v, _) {
              n(l, s(u), v, _)
            }),
            l
          )
        }
        function io(e, n, s) {
          ;(n = $i(n, e)), (e = kh(e, n))
          var l = e == null ? e : e[Vr(wr(n))]
          return l == null ? i : tr(l, e, s)
        }
        function Pd(e) {
          return ee(e) && Me(e) == me
        }
        function cv(e) {
          return ee(e) && Me(e) == Gn
        }
        function dv(e) {
          return ee(e) && Me(e) == Je
        }
        function no(e, n, s, l, u) {
          return e === n
            ? !0
            : e == null || n == null || (!ee(e) && !ee(n))
            ? e !== e && n !== n
            : hv(e, n, s, l, no, u)
        }
        function hv(e, n, s, l, u, v) {
          var _ = _t(e),
            x = _t(n),
            E = _ ? Cr : Oe(e),
            B = x ? Cr : Oe(n)
          ;(E = E == me ? ei : E), (B = B == me ? ei : B)
          var N = E == ei,
            V = B == ei,
            Q = E == B
          if (Q && Si(e)) {
            if (!Si(n)) return !1
            ;(_ = !0), (N = !1)
          }
          if (Q && !N)
            return (
              v || (v = new Tr()),
              _ || Sn(e) ? mh(e, n, s, l, u, v) : Bv(e, n, E, s, l, u, v)
            )
          if (!(s & $)) {
            var ot = N && Vt.call(e, '__wrapped__'),
              ut = V && Vt.call(n, '__wrapped__')
            if (ot || ut) {
              var xt = ot ? e.value() : e,
                pt = ut ? n.value() : n
              return v || (v = new Tr()), u(xt, pt, s, l, v)
            }
          }
          return Q ? (v || (v = new Tr()), Fv(e, n, s, l, u, v)) : !1
        }
        function uv(e) {
          return ee(e) && Oe(e) == zr
        }
        function ja(e, n, s, l) {
          var u = s.length,
            v = u,
            _ = !l
          if (e == null) return !v
          for (e = Wt(e); u--; ) {
            var x = s[u]
            if (_ && x[2] ? x[1] !== e[x[0]] : !(x[0] in e)) return !1
          }
          for (; ++u < v; ) {
            x = s[u]
            var E = x[0],
              B = e[E],
              N = x[1]
            if (_ && x[2]) {
              if (B === i && !(E in e)) return !1
            } else {
              var V = new Tr()
              if (l) var Q = l(B, N, E, e, n, V)
              if (!(Q === i ? no(N, B, $ | w, l, V) : Q)) return !1
            }
          }
          return !0
        }
        function Bd(e) {
          if (!Zt(e) || Gv(e)) return !1
          var n = li(e) ? um : rg
          return n.test(qi(e))
        }
        function pv(e) {
          return ee(e) && Me(e) == qn
        }
        function fv(e) {
          return ee(e) && Oe(e) == Ar
        }
        function gv(e) {
          return ee(e) && Es(e.length) && !!Yt[Me(e)]
        }
        function Fd(e) {
          return typeof e == 'function'
            ? e
            : e == null
            ? Ye
            : typeof e == 'object'
            ? _t(e)
              ? Hd(e[0], e[1])
              : Ud(e)
            : au(e)
        }
        function Za(e) {
          if (!ao(e)) return bm(e)
          var n = []
          for (var s in Wt(e)) Vt.call(e, s) && s != 'constructor' && n.push(s)
          return n
        }
        function mv(e) {
          if (!Zt(e)) return Jv(e)
          var n = ao(e),
            s = []
          for (var l in e)
            (l == 'constructor' && (n || !Vt.call(e, l))) || s.push(l)
          return s
        }
        function Ja(e, n) {
          return e < n
        }
        function Nd(e, n) {
          var s = -1,
            l = We(e) ? I(e.length) : []
          return (
            xi(e, function (u, v, _) {
              l[++s] = n(u, v, _)
            }),
            l
          )
        }
        function Ud(e) {
          var n = pl(e)
          return n.length == 1 && n[0][2]
            ? wh(n[0][0], n[0][1])
            : function (s) {
                return s === e || ja(s, e, n)
              }
        }
        function Hd(e, n) {
          return gl(e) && _h(n)
            ? wh(Vr(e), n)
            : function (s) {
                var l = Cl(s, e)
                return l === i && l === n ? Sl(s, e) : no(n, l, $ | w)
              }
        }
        function ps(e, n, s, l, u) {
          e !== n &&
            Ya(
              n,
              function (v, _) {
                if ((u || (u = new Tr()), Zt(v))) vv(e, n, _, s, ps, l, u)
                else {
                  var x = l ? l(vl(e, _), v, _ + '', e, n, u) : i
                  x === i && (x = v), Wa(e, _, x)
                }
              },
              qe,
            )
        }
        function vv(e, n, s, l, u, v, _) {
          var x = vl(e, s),
            E = vl(n, s),
            B = _.get(E)
          if (B) {
            Wa(e, s, B)
            return
          }
          var N = v ? v(x, E, s + '', e, n, _) : i,
            V = N === i
          if (V) {
            var Q = _t(E),
              ot = !Q && Si(E),
              ut = !Q && !ot && Sn(E)
            ;(N = E),
              Q || ot || ut
                ? _t(x)
                  ? (N = x)
                  : se(x)
                  ? (N = Ve(x))
                  : ot
                  ? ((V = !1), (N = th(E, !0)))
                  : ut
                  ? ((V = !1), (N = eh(E, !0)))
                  : (N = [])
                : co(E) || Yi(E)
                ? ((N = x),
                  Yi(x) ? (N = Zh(x)) : (!Zt(x) || li(x)) && (N = yh(E)))
                : (V = !1)
          }
          V && (_.set(E, N), u(N, E, l, v, _), _.delete(E)), Wa(e, s, N)
        }
        function Vd(e, n) {
          var s = e.length
          if (s) return (n += n < 0 ? s : 0), ai(n, s) ? e[n] : i
        }
        function Wd(e, n, s) {
          n.length
            ? (n = Kt(n, function (v) {
                return _t(v)
                  ? function (_) {
                      return Vi(_, v.length === 1 ? v[0] : v)
                    }
                  : v
              }))
            : (n = [Ye])
          var l = -1
          n = Kt(n, er(ht()))
          var u = Nd(e, function (v, _, x) {
            var E = Kt(n, function (B) {
              return B(v)
            })
            return { criteria: E, index: ++l, value: v }
          })
          return Wg(u, function (v, _) {
            return Tv(v, _, s)
          })
        }
        function bv(e, n) {
          return qd(e, n, function (s, l) {
            return Sl(e, l)
          })
        }
        function qd(e, n, s) {
          for (var l = -1, u = n.length, v = {}; ++l < u; ) {
            var _ = n[l],
              x = Vi(e, _)
            s(x, _) && oo(v, $i(_, e), x)
          }
          return v
        }
        function yv(e) {
          return function (n) {
            return Vi(n, e)
          }
        }
        function Qa(e, n, s, l) {
          var u = l ? Vg : gn,
            v = -1,
            _ = n.length,
            x = e
          for (e === n && (n = Ve(n)), s && (x = Kt(e, er(s))); ++v < _; )
            for (
              var E = 0, B = n[v], N = s ? s(B) : B;
              (E = u(x, N, E, l)) > -1;

            )
              x !== e && is.call(x, E, 1), is.call(e, E, 1)
          return e
        }
        function Yd(e, n) {
          for (var s = e ? n.length : 0, l = s - 1; s--; ) {
            var u = n[s]
            if (s == l || u !== v) {
              var v = u
              ai(u) ? is.call(e, u, 1) : il(e, u)
            }
          }
          return e
        }
        function tl(e, n) {
          return e + ss(Sd() * (n - e + 1))
        }
        function _v(e, n, s, l) {
          for (var u = -1, v = be(os((n - e) / (s || 1)), 0), _ = I(v); v--; )
            (_[l ? v : ++u] = e), (e += s)
          return _
        }
        function el(e, n) {
          var s = ''
          if (!e || n < 1 || n > j) return s
          do n % 2 && (s += e), (n = ss(n / 2)), n && (e += e)
          while (n)
          return s
        }
        function kt(e, n) {
          return bl(xh(e, n, Ye), e + '')
        }
        function wv(e) {
          return Ed(zn(e))
        }
        function xv(e, n) {
          var s = zn(e)
          return ks(s, Hi(n, 0, s.length))
        }
        function oo(e, n, s, l) {
          if (!Zt(e)) return e
          n = $i(n, e)
          for (
            var u = -1, v = n.length, _ = v - 1, x = e;
            x != null && ++u < v;

          ) {
            var E = Vr(n[u]),
              B = s
            if (E === '__proto__' || E === 'constructor' || E === 'prototype')
              return e
            if (u != _) {
              var N = x[E]
              ;(B = l ? l(N, E, x) : i),
                B === i && (B = Zt(N) ? N : ai(n[u + 1]) ? [] : {})
            }
            eo(x, E, B), (x = x[E])
          }
          return e
        }
        var Kd = as
            ? function (e, n) {
                return as.set(e, n), e
              }
            : Ye,
          kv = ns
            ? function (e, n) {
                return ns(e, 'toString', {
                  configurable: !0,
                  enumerable: !1,
                  value: Al(n),
                  writable: !0,
                })
              }
            : Ye
        function $v(e) {
          return ks(zn(e))
        }
        function _r(e, n, s) {
          var l = -1,
            u = e.length
          n < 0 && (n = -n > u ? 0 : u + n),
            (s = s > u ? u : s),
            s < 0 && (s += u),
            (u = n > s ? 0 : (s - n) >>> 0),
            (n >>>= 0)
          for (var v = I(u); ++l < u; ) v[l] = e[l + n]
          return v
        }
        function Cv(e, n) {
          var s
          return (
            xi(e, function (l, u, v) {
              return (s = n(l, u, v)), !s
            }),
            !!s
          )
        }
        function fs(e, n, s) {
          var l = 0,
            u = e == null ? l : e.length
          if (typeof n == 'number' && n === n && u <= he) {
            for (; l < u; ) {
              var v = (l + u) >>> 1,
                _ = e[v]
              _ !== null && !ir(_) && (s ? _ <= n : _ < n)
                ? (l = v + 1)
                : (u = v)
            }
            return u
          }
          return rl(e, n, Ye, s)
        }
        function rl(e, n, s, l) {
          var u = 0,
            v = e == null ? 0 : e.length
          if (v === 0) return 0
          n = s(n)
          for (
            var _ = n !== n, x = n === null, E = ir(n), B = n === i;
            u < v;

          ) {
            var N = ss((u + v) / 2),
              V = s(e[N]),
              Q = V !== i,
              ot = V === null,
              ut = V === V,
              xt = ir(V)
            if (_) var pt = l || ut
            else
              B
                ? (pt = ut && (l || Q))
                : x
                ? (pt = ut && Q && (l || !ot))
                : E
                ? (pt = ut && Q && !ot && (l || !xt))
                : ot || xt
                ? (pt = !1)
                : (pt = l ? V <= n : V < n)
            pt ? (u = N + 1) : (v = N)
          }
          return Ie(v, jt)
        }
        function Gd(e, n) {
          for (var s = -1, l = e.length, u = 0, v = []; ++s < l; ) {
            var _ = e[s],
              x = n ? n(_) : _
            if (!s || !Ir(x, E)) {
              var E = x
              v[u++] = _ === 0 ? 0 : _
            }
          }
          return v
        }
        function Xd(e) {
          return typeof e == 'number' ? e : ir(e) ? gt : +e
        }
        function rr(e) {
          if (typeof e == 'string') return e
          if (_t(e)) return Kt(e, rr) + ''
          if (ir(e)) return zd ? zd.call(e) : ''
          var n = e + ''
          return n == '0' && 1 / e == -tt ? '-0' : n
        }
        function ki(e, n, s) {
          var l = -1,
            u = Ko,
            v = e.length,
            _ = !0,
            x = [],
            E = x
          if (s) (_ = !1), (u = Ia)
          else if (v >= a) {
            var B = n ? null : Rv(e)
            if (B) return Xo(B)
            ;(_ = !1), (u = Xn), (E = new Ui())
          } else E = n ? [] : x
          t: for (; ++l < v; ) {
            var N = e[l],
              V = n ? n(N) : N
            if (((N = s || N !== 0 ? N : 0), _ && V === V)) {
              for (var Q = E.length; Q--; ) if (E[Q] === V) continue t
              n && E.push(V), x.push(N)
            } else u(E, V, s) || (E !== x && E.push(V), x.push(N))
          }
          return x
        }
        function il(e, n) {
          return (
            (n = $i(n, e)), (e = kh(e, n)), e == null || delete e[Vr(wr(n))]
          )
        }
        function jd(e, n, s, l) {
          return oo(e, n, s(Vi(e, n)), l)
        }
        function gs(e, n, s, l) {
          for (
            var u = e.length, v = l ? u : -1;
            (l ? v-- : ++v < u) && n(e[v], v, e);

          );
          return s
            ? _r(e, l ? 0 : v, l ? v + 1 : u)
            : _r(e, l ? v + 1 : 0, l ? u : v)
        }
        function Zd(e, n) {
          var s = e
          return (
            s instanceof At && (s = s.value()),
            Oa(
              n,
              function (l, u) {
                return u.func.apply(u.thisArg, yi([l], u.args))
              },
              s,
            )
          )
        }
        function nl(e, n, s) {
          var l = e.length
          if (l < 2) return l ? ki(e[0]) : []
          for (var u = -1, v = I(l); ++u < l; )
            for (var _ = e[u], x = -1; ++x < l; )
              x != u && (v[u] = ro(v[u] || _, e[x], n, s))
          return ki(Se(v, 1), n, s)
        }
        function Jd(e, n, s) {
          for (var l = -1, u = e.length, v = n.length, _ = {}; ++l < u; ) {
            var x = l < v ? n[l] : i
            s(_, e[l], x)
          }
          return _
        }
        function ol(e) {
          return se(e) ? e : []
        }
        function sl(e) {
          return typeof e == 'function' ? e : Ye
        }
        function $i(e, n) {
          return _t(e) ? e : gl(e, n) ? [e] : zh(Ft(e))
        }
        var Sv = kt
        function Ci(e, n, s) {
          var l = e.length
          return (s = s === i ? l : s), !n && s >= l ? e : _r(e, n, s)
        }
        var Qd =
          pm ||
          function (e) {
            return Ce.clearTimeout(e)
          }
        function th(e, n) {
          if (n) return e.slice()
          var s = e.length,
            l = wd ? wd(s) : new e.constructor(s)
          return e.copy(l), l
        }
        function al(e) {
          var n = new e.constructor(e.byteLength)
          return new es(n).set(new es(e)), n
        }
        function zv(e, n) {
          var s = n ? al(e.buffer) : e.buffer
          return new e.constructor(s, e.byteOffset, e.byteLength)
        }
        function Av(e) {
          var n = new e.constructor(e.source, Pc.exec(e))
          return (n.lastIndex = e.lastIndex), n
        }
        function Ev(e) {
          return to ? Wt(to.call(e)) : {}
        }
        function eh(e, n) {
          var s = n ? al(e.buffer) : e.buffer
          return new e.constructor(s, e.byteOffset, e.length)
        }
        function rh(e, n) {
          if (e !== n) {
            var s = e !== i,
              l = e === null,
              u = e === e,
              v = ir(e),
              _ = n !== i,
              x = n === null,
              E = n === n,
              B = ir(n)
            if (
              (!x && !B && !v && e > n) ||
              (v && _ && E && !x && !B) ||
              (l && _ && E) ||
              (!s && E) ||
              !u
            )
              return 1
            if (
              (!l && !v && !B && e < n) ||
              (B && s && u && !l && !v) ||
              (x && s && u) ||
              (!_ && u) ||
              !E
            )
              return -1
          }
          return 0
        }
        function Tv(e, n, s) {
          for (
            var l = -1,
              u = e.criteria,
              v = n.criteria,
              _ = u.length,
              x = s.length;
            ++l < _;

          ) {
            var E = rh(u[l], v[l])
            if (E) {
              if (l >= x) return E
              var B = s[l]
              return E * (B == 'desc' ? -1 : 1)
            }
          }
          return e.index - n.index
        }
        function ih(e, n, s, l) {
          for (
            var u = -1,
              v = e.length,
              _ = s.length,
              x = -1,
              E = n.length,
              B = be(v - _, 0),
              N = I(E + B),
              V = !l;
            ++x < E;

          )
            N[x] = n[x]
          for (; ++u < _; ) (V || u < v) && (N[s[u]] = e[u])
          for (; B--; ) N[x++] = e[u++]
          return N
        }
        function nh(e, n, s, l) {
          for (
            var u = -1,
              v = e.length,
              _ = -1,
              x = s.length,
              E = -1,
              B = n.length,
              N = be(v - x, 0),
              V = I(N + B),
              Q = !l;
            ++u < N;

          )
            V[u] = e[u]
          for (var ot = u; ++E < B; ) V[ot + E] = n[E]
          for (; ++_ < x; ) (Q || u < v) && (V[ot + s[_]] = e[u++])
          return V
        }
        function Ve(e, n) {
          var s = -1,
            l = e.length
          for (n || (n = I(l)); ++s < l; ) n[s] = e[s]
          return n
        }
        function Hr(e, n, s, l) {
          var u = !s
          s || (s = {})
          for (var v = -1, _ = n.length; ++v < _; ) {
            var x = n[v],
              E = l ? l(s[x], e[x], x, s, e) : i
            E === i && (E = e[x]), u ? ni(s, x, E) : eo(s, x, E)
          }
          return s
        }
        function Iv(e, n) {
          return Hr(e, fl(e), n)
        }
        function Ov(e, n) {
          return Hr(e, vh(e), n)
        }
        function ms(e, n) {
          return function (s, l) {
            var u = _t(s) ? Pg : tv,
              v = n ? n() : {}
            return u(s, e, ht(l, 2), v)
          }
        }
        function kn(e) {
          return kt(function (n, s) {
            var l = -1,
              u = s.length,
              v = u > 1 ? s[u - 1] : i,
              _ = u > 2 ? s[2] : i
            for (
              v = e.length > 3 && typeof v == 'function' ? (u--, v) : i,
                _ && Re(s[0], s[1], _) && ((v = u < 3 ? i : v), (u = 1)),
                n = Wt(n);
              ++l < u;

            ) {
              var x = s[l]
              x && e(n, x, l, v)
            }
            return n
          })
        }
        function oh(e, n) {
          return function (s, l) {
            if (s == null) return s
            if (!We(s)) return e(s, l)
            for (
              var u = s.length, v = n ? u : -1, _ = Wt(s);
              (n ? v-- : ++v < u) && l(_[v], v, _) !== !1;

            );
            return s
          }
        }
        function sh(e) {
          return function (n, s, l) {
            for (var u = -1, v = Wt(n), _ = l(n), x = _.length; x--; ) {
              var E = _[e ? x : ++u]
              if (s(v[E], E, v) === !1) break
            }
            return n
          }
        }
        function Lv(e, n, s) {
          var l = n & T,
            u = so(e)
          function v() {
            var _ = this && this !== Ce && this instanceof v ? u : e
            return _.apply(l ? s : this, arguments)
          }
          return v
        }
        function ah(e) {
          return function (n) {
            n = Ft(n)
            var s = mn(n) ? Er(n) : i,
              l = s ? s[0] : n.charAt(0),
              u = s ? Ci(s, 1).join('') : n.slice(1)
            return l[e]() + u
          }
        }
        function $n(e) {
          return function (n) {
            return Oa(ou(nu(n).replace(kg, '')), e, '')
          }
        }
        function so(e) {
          return function () {
            var n = arguments
            switch (n.length) {
              case 0:
                return new e()
              case 1:
                return new e(n[0])
              case 2:
                return new e(n[0], n[1])
              case 3:
                return new e(n[0], n[1], n[2])
              case 4:
                return new e(n[0], n[1], n[2], n[3])
              case 5:
                return new e(n[0], n[1], n[2], n[3], n[4])
              case 6:
                return new e(n[0], n[1], n[2], n[3], n[4], n[5])
              case 7:
                return new e(n[0], n[1], n[2], n[3], n[4], n[5], n[6])
            }
            var s = xn(e.prototype),
              l = e.apply(s, n)
            return Zt(l) ? l : s
          }
        }
        function Dv(e, n, s) {
          var l = so(e)
          function u() {
            for (var v = arguments.length, _ = I(v), x = v, E = Cn(u); x--; )
              _[x] = arguments[x]
            var B = v < 3 && _[0] !== E && _[v - 1] !== E ? [] : _i(_, E)
            if (((v -= B.length), v < s))
              return uh(e, n, vs, u.placeholder, i, _, B, i, i, s - v)
            var N = this && this !== Ce && this instanceof u ? l : e
            return tr(N, this, _)
          }
          return u
        }
        function lh(e) {
          return function (n, s, l) {
            var u = Wt(n)
            if (!We(n)) {
              var v = ht(s, 3)
              ;(n = xe(n)),
                (s = function (x) {
                  return v(u[x], x, u)
                })
            }
            var _ = e(n, s, l)
            return _ > -1 ? u[v ? n[_] : _] : i
          }
        }
        function ch(e) {
          return si(function (n) {
            var s = n.length,
              l = s,
              u = br.prototype.thru
            for (e && n.reverse(); l--; ) {
              var v = n[l]
              if (typeof v != 'function') throw new vr(h)
              if (u && !_ && ws(v) == 'wrapper') var _ = new br([], !0)
            }
            for (l = _ ? l : s; ++l < s; ) {
              v = n[l]
              var x = ws(v),
                E = x == 'wrapper' ? ul(v) : i
              E &&
              ml(E[0]) &&
              E[1] == (H | R | L | F) &&
              !E[4].length &&
              E[9] == 1
                ? (_ = _[ws(E[0])].apply(_, E[3]))
                : (_ = v.length == 1 && ml(v) ? _[x]() : _.thru(v))
            }
            return function () {
              var B = arguments,
                N = B[0]
              if (_ && B.length == 1 && _t(N)) return _.plant(N).value()
              for (var V = 0, Q = s ? n[V].apply(this, B) : N; ++V < s; )
                Q = n[V].call(this, Q)
              return Q
            }
          })
        }
        function vs(e, n, s, l, u, v, _, x, E, B) {
          var N = n & H,
            V = n & T,
            Q = n & P,
            ot = n & (R | O),
            ut = n & K,
            xt = Q ? i : so(e)
          function pt() {
            for (var zt = arguments.length, Et = I(zt), nr = zt; nr--; )
              Et[nr] = arguments[nr]
            if (ot)
              var Pe = Cn(pt),
                or = Yg(Et, Pe)
            if (
              (l && (Et = ih(Et, l, u, ot)),
              v && (Et = nh(Et, v, _, ot)),
              (zt -= or),
              ot && zt < B)
            ) {
              var ae = _i(Et, Pe)
              return uh(e, n, vs, pt.placeholder, s, Et, ae, x, E, B - zt)
            }
            var Or = V ? s : this,
              di = Q ? Or[e] : e
            return (
              (zt = Et.length),
              x ? (Et = tb(Et, x)) : ut && zt > 1 && Et.reverse(),
              N && E < zt && (Et.length = E),
              this && this !== Ce && this instanceof pt && (di = xt || so(di)),
              di.apply(Or, Et)
            )
          }
          return pt
        }
        function dh(e, n) {
          return function (s, l) {
            return lv(s, e, n(l), {})
          }
        }
        function bs(e, n) {
          return function (s, l) {
            var u
            if (s === i && l === i) return n
            if ((s !== i && (u = s), l !== i)) {
              if (u === i) return l
              typeof s == 'string' || typeof l == 'string'
                ? ((s = rr(s)), (l = rr(l)))
                : ((s = Xd(s)), (l = Xd(l))),
                (u = e(s, l))
            }
            return u
          }
        }
        function ll(e) {
          return si(function (n) {
            return (
              (n = Kt(n, er(ht()))),
              kt(function (s) {
                var l = this
                return e(n, function (u) {
                  return tr(u, l, s)
                })
              })
            )
          })
        }
        function ys(e, n) {
          n = n === i ? ' ' : rr(n)
          var s = n.length
          if (s < 2) return s ? el(n, e) : n
          var l = el(n, os(e / vn(n)))
          return mn(n) ? Ci(Er(l), 0, e).join('') : l.slice(0, e)
        }
        function Mv(e, n, s, l) {
          var u = n & T,
            v = so(e)
          function _() {
            for (
              var x = -1,
                E = arguments.length,
                B = -1,
                N = l.length,
                V = I(N + E),
                Q = this && this !== Ce && this instanceof _ ? v : e;
              ++B < N;

            )
              V[B] = l[B]
            for (; E--; ) V[B++] = arguments[++x]
            return tr(Q, u ? s : this, V)
          }
          return _
        }
        function hh(e) {
          return function (n, s, l) {
            return (
              l && typeof l != 'number' && Re(n, s, l) && (s = l = i),
              (n = ci(n)),
              s === i ? ((s = n), (n = 0)) : (s = ci(s)),
              (l = l === i ? (n < s ? 1 : -1) : ci(l)),
              _v(n, s, l, e)
            )
          }
        }
        function _s(e) {
          return function (n, s) {
            return (
              (typeof n == 'string' && typeof s == 'string') ||
                ((n = xr(n)), (s = xr(s))),
              e(n, s)
            )
          }
        }
        function uh(e, n, s, l, u, v, _, x, E, B) {
          var N = n & R,
            V = N ? _ : i,
            Q = N ? i : _,
            ot = N ? v : i,
            ut = N ? i : v
          ;(n |= N ? L : U), (n &= ~(N ? U : L)), n & D || (n &= ~(T | P))
          var xt = [e, n, u, ot, V, ut, Q, x, E, B],
            pt = s.apply(i, xt)
          return ml(e) && $h(pt, xt), (pt.placeholder = l), Ch(pt, e, n)
        }
        function cl(e) {
          var n = ve[e]
          return function (s, l) {
            if (
              ((s = xr(s)), (l = l == null ? 0 : Ie(wt(l), 292)), l && Cd(s))
            ) {
              var u = (Ft(s) + 'e').split('e'),
                v = n(u[0] + 'e' + (+u[1] + l))
              return (u = (Ft(v) + 'e').split('e')), +(u[0] + 'e' + (+u[1] - l))
            }
            return n(s)
          }
        }
        var Rv =
          _n && 1 / Xo(new _n([, -0]))[1] == tt
            ? function (e) {
                return new _n(e)
              }
            : Il
        function ph(e) {
          return function (n) {
            var s = Oe(n)
            return s == zr ? Fa(n) : s == Ar ? Qg(n) : qg(n, e(n))
          }
        }
        function oi(e, n, s, l, u, v, _, x) {
          var E = n & P
          if (!E && typeof e != 'function') throw new vr(h)
          var B = l ? l.length : 0
          if (
            (B || ((n &= ~(L | U)), (l = u = i)),
            (_ = _ === i ? _ : be(wt(_), 0)),
            (x = x === i ? x : wt(x)),
            (B -= u ? u.length : 0),
            n & U)
          ) {
            var N = l,
              V = u
            l = u = i
          }
          var Q = E ? i : ul(e),
            ot = [e, n, s, l, u, N, V, v, _, x]
          if (
            (Q && Zv(ot, Q),
            (e = ot[0]),
            (n = ot[1]),
            (s = ot[2]),
            (l = ot[3]),
            (u = ot[4]),
            (x = ot[9] = ot[9] === i ? (E ? 0 : e.length) : be(ot[9] - B, 0)),
            !x && n & (R | O) && (n &= ~(R | O)),
            !n || n == T)
          )
            var ut = Lv(e, n, s)
          else
            n == R || n == O
              ? (ut = Dv(e, n, x))
              : (n == L || n == (T | L)) && !u.length
              ? (ut = Mv(e, n, s, l))
              : (ut = vs.apply(i, ot))
          var xt = Q ? Kd : $h
          return Ch(xt(ut, ot), e, n)
        }
        function fh(e, n, s, l) {
          return e === i || (Ir(e, yn[s]) && !Vt.call(l, s)) ? n : e
        }
        function gh(e, n, s, l, u, v) {
          return (
            Zt(e) && Zt(n) && (v.set(n, e), ps(e, n, i, gh, v), v.delete(n)), e
          )
        }
        function Pv(e) {
          return co(e) ? i : e
        }
        function mh(e, n, s, l, u, v) {
          var _ = s & $,
            x = e.length,
            E = n.length
          if (x != E && !(_ && E > x)) return !1
          var B = v.get(e),
            N = v.get(n)
          if (B && N) return B == n && N == e
          var V = -1,
            Q = !0,
            ot = s & w ? new Ui() : i
          for (v.set(e, n), v.set(n, e); ++V < x; ) {
            var ut = e[V],
              xt = n[V]
            if (l) var pt = _ ? l(xt, ut, V, n, e, v) : l(ut, xt, V, e, n, v)
            if (pt !== i) {
              if (pt) continue
              Q = !1
              break
            }
            if (ot) {
              if (
                !La(n, function (zt, Et) {
                  if (!Xn(ot, Et) && (ut === zt || u(ut, zt, s, l, v)))
                    return ot.push(Et)
                })
              ) {
                Q = !1
                break
              }
            } else if (!(ut === xt || u(ut, xt, s, l, v))) {
              Q = !1
              break
            }
          }
          return v.delete(e), v.delete(n), Q
        }
        function Bv(e, n, s, l, u, v, _) {
          switch (s) {
            case pn:
              if (e.byteLength != n.byteLength || e.byteOffset != n.byteOffset)
                return !1
              ;(e = e.buffer), (n = n.buffer)
            case Gn:
              return !(e.byteLength != n.byteLength || !v(new es(e), new es(n)))
            case Sr:
            case Je:
            case Wn:
              return Ir(+e, +n)
            case Qe:
              return e.name == n.name && e.message == n.message
            case qn:
            case Yn:
              return e == n + ''
            case zr:
              var x = Fa
            case Ar:
              var E = l & $
              if ((x || (x = Xo), e.size != n.size && !E)) return !1
              var B = _.get(e)
              if (B) return B == n
              ;(l |= w), _.set(e, n)
              var N = mh(x(e), x(n), l, u, v, _)
              return _.delete(e), N
            case Vo:
              if (to) return to.call(e) == to.call(n)
          }
          return !1
        }
        function Fv(e, n, s, l, u, v) {
          var _ = s & $,
            x = dl(e),
            E = x.length,
            B = dl(n),
            N = B.length
          if (E != N && !_) return !1
          for (var V = E; V--; ) {
            var Q = x[V]
            if (!(_ ? Q in n : Vt.call(n, Q))) return !1
          }
          var ot = v.get(e),
            ut = v.get(n)
          if (ot && ut) return ot == n && ut == e
          var xt = !0
          v.set(e, n), v.set(n, e)
          for (var pt = _; ++V < E; ) {
            Q = x[V]
            var zt = e[Q],
              Et = n[Q]
            if (l) var nr = _ ? l(Et, zt, Q, n, e, v) : l(zt, Et, Q, e, n, v)
            if (!(nr === i ? zt === Et || u(zt, Et, s, l, v) : nr)) {
              xt = !1
              break
            }
            pt || (pt = Q == 'constructor')
          }
          if (xt && !pt) {
            var Pe = e.constructor,
              or = n.constructor
            Pe != or &&
              'constructor' in e &&
              'constructor' in n &&
              !(
                typeof Pe == 'function' &&
                Pe instanceof Pe &&
                typeof or == 'function' &&
                or instanceof or
              ) &&
              (xt = !1)
          }
          return v.delete(e), v.delete(n), xt
        }
        function si(e) {
          return bl(xh(e, i, Ih), e + '')
        }
        function dl(e) {
          return Rd(e, xe, fl)
        }
        function hl(e) {
          return Rd(e, qe, vh)
        }
        var ul = as
          ? function (e) {
              return as.get(e)
            }
          : Il
        function ws(e) {
          for (
            var n = e.name + '', s = wn[n], l = Vt.call(wn, n) ? s.length : 0;
            l--;

          ) {
            var u = s[l],
              v = u.func
            if (v == null || v == e) return u.name
          }
          return n
        }
        function Cn(e) {
          var n = Vt.call(g, 'placeholder') ? g : e
          return n.placeholder
        }
        function ht() {
          var e = g.iteratee || El
          return (
            (e = e === El ? Fd : e),
            arguments.length ? e(arguments[0], arguments[1]) : e
          )
        }
        function xs(e, n) {
          var s = e.__data__
          return Kv(n) ? s[typeof n == 'string' ? 'string' : 'hash'] : s.map
        }
        function pl(e) {
          for (var n = xe(e), s = n.length; s--; ) {
            var l = n[s],
              u = e[l]
            n[s] = [l, u, _h(u)]
          }
          return n
        }
        function Wi(e, n) {
          var s = jg(e, n)
          return Bd(s) ? s : i
        }
        function Nv(e) {
          var n = Vt.call(e, Fi),
            s = e[Fi]
          try {
            e[Fi] = i
            var l = !0
          } catch {}
          var u = Qo.call(e)
          return l && (n ? (e[Fi] = s) : delete e[Fi]), u
        }
        var fl = Ua
            ? function (e) {
                return e == null
                  ? []
                  : ((e = Wt(e)),
                    bi(Ua(e), function (n) {
                      return kd.call(e, n)
                    }))
              }
            : Ol,
          vh = Ua
            ? function (e) {
                for (var n = []; e; ) yi(n, fl(e)), (e = rs(e))
                return n
              }
            : Ol,
          Oe = Me
        ;((Ha && Oe(new Ha(new ArrayBuffer(1))) != pn) ||
          (Zn && Oe(new Zn()) != zr) ||
          (Va && Oe(Va.resolve()) != Lc) ||
          (_n && Oe(new _n()) != Ar) ||
          (Jn && Oe(new Jn()) != Kn)) &&
          (Oe = function (e) {
            var n = Me(e),
              s = n == ei ? e.constructor : i,
              l = s ? qi(s) : ''
            if (l)
              switch (l) {
                case xm:
                  return pn
                case km:
                  return zr
                case $m:
                  return Lc
                case Cm:
                  return Ar
                case Sm:
                  return Kn
              }
            return n
          })
        function Uv(e, n, s) {
          for (var l = -1, u = s.length; ++l < u; ) {
            var v = s[l],
              _ = v.size
            switch (v.type) {
              case 'drop':
                e += _
                break
              case 'dropRight':
                n -= _
                break
              case 'take':
                n = Ie(n, e + _)
                break
              case 'takeRight':
                e = be(e, n - _)
                break
            }
          }
          return { start: e, end: n }
        }
        function Hv(e) {
          var n = e.match(Gf)
          return n ? n[1].split(Xf) : []
        }
        function bh(e, n, s) {
          n = $i(n, e)
          for (var l = -1, u = n.length, v = !1; ++l < u; ) {
            var _ = Vr(n[l])
            if (!(v = e != null && s(e, _))) break
            e = e[_]
          }
          return v || ++l != u
            ? v
            : ((u = e == null ? 0 : e.length),
              !!u && Es(u) && ai(_, u) && (_t(e) || Yi(e)))
        }
        function Vv(e) {
          var n = e.length,
            s = new e.constructor(n)
          return (
            n &&
              typeof e[0] == 'string' &&
              Vt.call(e, 'index') &&
              ((s.index = e.index), (s.input = e.input)),
            s
          )
        }
        function yh(e) {
          return typeof e.constructor == 'function' && !ao(e) ? xn(rs(e)) : {}
        }
        function Wv(e, n, s) {
          var l = e.constructor
          switch (n) {
            case Gn:
              return al(e)
            case Sr:
            case Je:
              return new l(+e)
            case pn:
              return zv(e, s)
            case pa:
            case fa:
            case ga:
            case ma:
            case va:
            case ba:
            case ya:
            case _a:
            case wa:
              return eh(e, s)
            case zr:
              return new l()
            case Wn:
            case Yn:
              return new l(e)
            case qn:
              return Av(e)
            case Ar:
              return new l()
            case Vo:
              return Ev(e)
          }
        }
        function qv(e, n) {
          var s = n.length
          if (!s) return e
          var l = s - 1
          return (
            (n[l] = (s > 1 ? '& ' : '') + n[l]),
            (n = n.join(s > 2 ? ', ' : ' ')),
            e.replace(
              Kf,
              `{
/* [wrapped with ` +
                n +
                `] */
`,
            )
          )
        }
        function Yv(e) {
          return _t(e) || Yi(e) || !!($d && e && e[$d])
        }
        function ai(e, n) {
          var s = typeof e
          return (
            (n = n ?? j),
            !!n &&
              (s == 'number' || (s != 'symbol' && ng.test(e))) &&
              e > -1 &&
              e % 1 == 0 &&
              e < n
          )
        }
        function Re(e, n, s) {
          if (!Zt(s)) return !1
          var l = typeof n
          return (
            l == 'number' ? We(s) && ai(n, s.length) : l == 'string' && n in s
          )
            ? Ir(s[n], e)
            : !1
        }
        function gl(e, n) {
          if (_t(e)) return !1
          var s = typeof e
          return s == 'number' ||
            s == 'symbol' ||
            s == 'boolean' ||
            e == null ||
            ir(e)
            ? !0
            : Vf.test(e) || !Hf.test(e) || (n != null && e in Wt(n))
        }
        function Kv(e) {
          var n = typeof e
          return n == 'string' ||
            n == 'number' ||
            n == 'symbol' ||
            n == 'boolean'
            ? e !== '__proto__'
            : e === null
        }
        function ml(e) {
          var n = ws(e),
            s = g[n]
          if (typeof s != 'function' || !(n in At.prototype)) return !1
          if (e === s) return !0
          var l = ul(s)
          return !!l && e === l[0]
        }
        function Gv(e) {
          return !!_d && _d in e
        }
        var Xv = Zo ? li : Ll
        function ao(e) {
          var n = e && e.constructor,
            s = (typeof n == 'function' && n.prototype) || yn
          return e === s
        }
        function _h(e) {
          return e === e && !Zt(e)
        }
        function wh(e, n) {
          return function (s) {
            return s == null ? !1 : s[e] === n && (n !== i || e in Wt(s))
          }
        }
        function jv(e) {
          var n = zs(e, function (l) {
              return s.size === b && s.clear(), l
            }),
            s = n.cache
          return n
        }
        function Zv(e, n) {
          var s = e[1],
            l = n[1],
            u = s | l,
            v = u < (T | P | H),
            _ =
              (l == H && s == R) ||
              (l == H && s == F && e[7].length <= n[8]) ||
              (l == (H | F) && n[7].length <= n[8] && s == R)
          if (!(v || _)) return e
          l & T && ((e[2] = n[2]), (u |= s & T ? 0 : D))
          var x = n[3]
          if (x) {
            var E = e[3]
            ;(e[3] = E ? ih(E, x, n[4]) : x), (e[4] = E ? _i(e[3], A) : n[4])
          }
          return (
            (x = n[5]),
            x &&
              ((E = e[5]),
              (e[5] = E ? nh(E, x, n[6]) : x),
              (e[6] = E ? _i(e[5], A) : n[6])),
            (x = n[7]),
            x && (e[7] = x),
            l & H && (e[8] = e[8] == null ? n[8] : Ie(e[8], n[8])),
            e[9] == null && (e[9] = n[9]),
            (e[0] = n[0]),
            (e[1] = u),
            e
          )
        }
        function Jv(e) {
          var n = []
          if (e != null) for (var s in Wt(e)) n.push(s)
          return n
        }
        function Qv(e) {
          return Qo.call(e)
        }
        function xh(e, n, s) {
          return (
            (n = be(n === i ? e.length - 1 : n, 0)),
            function () {
              for (
                var l = arguments, u = -1, v = be(l.length - n, 0), _ = I(v);
                ++u < v;

              )
                _[u] = l[n + u]
              u = -1
              for (var x = I(n + 1); ++u < n; ) x[u] = l[u]
              return (x[n] = s(_)), tr(e, this, x)
            }
          )
        }
        function kh(e, n) {
          return n.length < 2 ? e : Vi(e, _r(n, 0, -1))
        }
        function tb(e, n) {
          for (var s = e.length, l = Ie(n.length, s), u = Ve(e); l--; ) {
            var v = n[l]
            e[l] = ai(v, s) ? u[v] : i
          }
          return e
        }
        function vl(e, n) {
          if (
            !(n === 'constructor' && typeof e[n] == 'function') &&
            n != '__proto__'
          )
            return e[n]
        }
        var $h = Sh(Kd),
          lo =
            gm ||
            function (e, n) {
              return Ce.setTimeout(e, n)
            },
          bl = Sh(kv)
        function Ch(e, n, s) {
          var l = n + ''
          return bl(e, qv(l, eb(Hv(l), s)))
        }
        function Sh(e) {
          var n = 0,
            s = 0
          return function () {
            var l = ym(),
              u = Ot - (l - s)
            if (((s = l), u > 0)) {
              if (++n >= mt) return arguments[0]
            } else n = 0
            return e.apply(i, arguments)
          }
        }
        function ks(e, n) {
          var s = -1,
            l = e.length,
            u = l - 1
          for (n = n === i ? l : n; ++s < n; ) {
            var v = tl(s, u),
              _ = e[v]
            ;(e[v] = e[s]), (e[s] = _)
          }
          return (e.length = n), e
        }
        var zh = jv(function (e) {
          var n = []
          return (
            e.charCodeAt(0) === 46 && n.push(''),
            e.replace(Wf, function (s, l, u, v) {
              n.push(u ? v.replace(Jf, '$1') : l || s)
            }),
            n
          )
        })
        function Vr(e) {
          if (typeof e == 'string' || ir(e)) return e
          var n = e + ''
          return n == '0' && 1 / e == -tt ? '-0' : n
        }
        function qi(e) {
          if (e != null) {
            try {
              return Jo.call(e)
            } catch {}
            try {
              return e + ''
            } catch {}
          }
          return ''
        }
        function eb(e, n) {
          return (
            mr(we, function (s) {
              var l = '_.' + s[0]
              n & s[1] && !Ko(e, l) && e.push(l)
            }),
            e.sort()
          )
        }
        function Ah(e) {
          if (e instanceof At) return e.clone()
          var n = new br(e.__wrapped__, e.__chain__)
          return (
            (n.__actions__ = Ve(e.__actions__)),
            (n.__index__ = e.__index__),
            (n.__values__ = e.__values__),
            n
          )
        }
        function rb(e, n, s) {
          ;(s ? Re(e, n, s) : n === i) ? (n = 1) : (n = be(wt(n), 0))
          var l = e == null ? 0 : e.length
          if (!l || n < 1) return []
          for (var u = 0, v = 0, _ = I(os(l / n)); u < l; )
            _[v++] = _r(e, u, (u += n))
          return _
        }
        function ib(e) {
          for (
            var n = -1, s = e == null ? 0 : e.length, l = 0, u = [];
            ++n < s;

          ) {
            var v = e[n]
            v && (u[l++] = v)
          }
          return u
        }
        function nb() {
          var e = arguments.length
          if (!e) return []
          for (var n = I(e - 1), s = arguments[0], l = e; l--; )
            n[l - 1] = arguments[l]
          return yi(_t(s) ? Ve(s) : [s], Se(n, 1))
        }
        var ob = kt(function (e, n) {
            return se(e) ? ro(e, Se(n, 1, se, !0)) : []
          }),
          sb = kt(function (e, n) {
            var s = wr(n)
            return (
              se(s) && (s = i), se(e) ? ro(e, Se(n, 1, se, !0), ht(s, 2)) : []
            )
          }),
          ab = kt(function (e, n) {
            var s = wr(n)
            return se(s) && (s = i), se(e) ? ro(e, Se(n, 1, se, !0), i, s) : []
          })
        function lb(e, n, s) {
          var l = e == null ? 0 : e.length
          return l
            ? ((n = s || n === i ? 1 : wt(n)), _r(e, n < 0 ? 0 : n, l))
            : []
        }
        function cb(e, n, s) {
          var l = e == null ? 0 : e.length
          return l
            ? ((n = s || n === i ? 1 : wt(n)),
              (n = l - n),
              _r(e, 0, n < 0 ? 0 : n))
            : []
        }
        function db(e, n) {
          return e && e.length ? gs(e, ht(n, 3), !0, !0) : []
        }
        function hb(e, n) {
          return e && e.length ? gs(e, ht(n, 3), !0) : []
        }
        function ub(e, n, s, l) {
          var u = e == null ? 0 : e.length
          return u
            ? (s && typeof s != 'number' && Re(e, n, s) && ((s = 0), (l = u)),
              nv(e, n, s, l))
            : []
        }
        function Eh(e, n, s) {
          var l = e == null ? 0 : e.length
          if (!l) return -1
          var u = s == null ? 0 : wt(s)
          return u < 0 && (u = be(l + u, 0)), Go(e, ht(n, 3), u)
        }
        function Th(e, n, s) {
          var l = e == null ? 0 : e.length
          if (!l) return -1
          var u = l - 1
          return (
            s !== i && ((u = wt(s)), (u = s < 0 ? be(l + u, 0) : Ie(u, l - 1))),
            Go(e, ht(n, 3), u, !0)
          )
        }
        function Ih(e) {
          var n = e == null ? 0 : e.length
          return n ? Se(e, 1) : []
        }
        function pb(e) {
          var n = e == null ? 0 : e.length
          return n ? Se(e, tt) : []
        }
        function fb(e, n) {
          var s = e == null ? 0 : e.length
          return s ? ((n = n === i ? 1 : wt(n)), Se(e, n)) : []
        }
        function gb(e) {
          for (var n = -1, s = e == null ? 0 : e.length, l = {}; ++n < s; ) {
            var u = e[n]
            l[u[0]] = u[1]
          }
          return l
        }
        function Oh(e) {
          return e && e.length ? e[0] : i
        }
        function mb(e, n, s) {
          var l = e == null ? 0 : e.length
          if (!l) return -1
          var u = s == null ? 0 : wt(s)
          return u < 0 && (u = be(l + u, 0)), gn(e, n, u)
        }
        function vb(e) {
          var n = e == null ? 0 : e.length
          return n ? _r(e, 0, -1) : []
        }
        var bb = kt(function (e) {
            var n = Kt(e, ol)
            return n.length && n[0] === e[0] ? Xa(n) : []
          }),
          yb = kt(function (e) {
            var n = wr(e),
              s = Kt(e, ol)
            return (
              n === wr(s) ? (n = i) : s.pop(),
              s.length && s[0] === e[0] ? Xa(s, ht(n, 2)) : []
            )
          }),
          _b = kt(function (e) {
            var n = wr(e),
              s = Kt(e, ol)
            return (
              (n = typeof n == 'function' ? n : i),
              n && s.pop(),
              s.length && s[0] === e[0] ? Xa(s, i, n) : []
            )
          })
        function wb(e, n) {
          return e == null ? '' : vm.call(e, n)
        }
        function wr(e) {
          var n = e == null ? 0 : e.length
          return n ? e[n - 1] : i
        }
        function xb(e, n, s) {
          var l = e == null ? 0 : e.length
          if (!l) return -1
          var u = l
          return (
            s !== i && ((u = wt(s)), (u = u < 0 ? be(l + u, 0) : Ie(u, l - 1))),
            n === n ? em(e, n, u) : Go(e, ud, u, !0)
          )
        }
        function kb(e, n) {
          return e && e.length ? Vd(e, wt(n)) : i
        }
        var $b = kt(Lh)
        function Lh(e, n) {
          return e && e.length && n && n.length ? Qa(e, n) : e
        }
        function Cb(e, n, s) {
          return e && e.length && n && n.length ? Qa(e, n, ht(s, 2)) : e
        }
        function Sb(e, n, s) {
          return e && e.length && n && n.length ? Qa(e, n, i, s) : e
        }
        var zb = si(function (e, n) {
          var s = e == null ? 0 : e.length,
            l = qa(e, n)
          return (
            Yd(
              e,
              Kt(n, function (u) {
                return ai(u, s) ? +u : u
              }).sort(rh),
            ),
            l
          )
        })
        function Ab(e, n) {
          var s = []
          if (!(e && e.length)) return s
          var l = -1,
            u = [],
            v = e.length
          for (n = ht(n, 3); ++l < v; ) {
            var _ = e[l]
            n(_, l, e) && (s.push(_), u.push(l))
          }
          return Yd(e, u), s
        }
        function yl(e) {
          return e == null ? e : wm.call(e)
        }
        function Eb(e, n, s) {
          var l = e == null ? 0 : e.length
          return l
            ? (s && typeof s != 'number' && Re(e, n, s)
                ? ((n = 0), (s = l))
                : ((n = n == null ? 0 : wt(n)), (s = s === i ? l : wt(s))),
              _r(e, n, s))
            : []
        }
        function Tb(e, n) {
          return fs(e, n)
        }
        function Ib(e, n, s) {
          return rl(e, n, ht(s, 2))
        }
        function Ob(e, n) {
          var s = e == null ? 0 : e.length
          if (s) {
            var l = fs(e, n)
            if (l < s && Ir(e[l], n)) return l
          }
          return -1
        }
        function Lb(e, n) {
          return fs(e, n, !0)
        }
        function Db(e, n, s) {
          return rl(e, n, ht(s, 2), !0)
        }
        function Mb(e, n) {
          var s = e == null ? 0 : e.length
          if (s) {
            var l = fs(e, n, !0) - 1
            if (Ir(e[l], n)) return l
          }
          return -1
        }
        function Rb(e) {
          return e && e.length ? Gd(e) : []
        }
        function Pb(e, n) {
          return e && e.length ? Gd(e, ht(n, 2)) : []
        }
        function Bb(e) {
          var n = e == null ? 0 : e.length
          return n ? _r(e, 1, n) : []
        }
        function Fb(e, n, s) {
          return e && e.length
            ? ((n = s || n === i ? 1 : wt(n)), _r(e, 0, n < 0 ? 0 : n))
            : []
        }
        function Nb(e, n, s) {
          var l = e == null ? 0 : e.length
          return l
            ? ((n = s || n === i ? 1 : wt(n)),
              (n = l - n),
              _r(e, n < 0 ? 0 : n, l))
            : []
        }
        function Ub(e, n) {
          return e && e.length ? gs(e, ht(n, 3), !1, !0) : []
        }
        function Hb(e, n) {
          return e && e.length ? gs(e, ht(n, 3)) : []
        }
        var Vb = kt(function (e) {
            return ki(Se(e, 1, se, !0))
          }),
          Wb = kt(function (e) {
            var n = wr(e)
            return se(n) && (n = i), ki(Se(e, 1, se, !0), ht(n, 2))
          }),
          qb = kt(function (e) {
            var n = wr(e)
            return (
              (n = typeof n == 'function' ? n : i), ki(Se(e, 1, se, !0), i, n)
            )
          })
        function Yb(e) {
          return e && e.length ? ki(e) : []
        }
        function Kb(e, n) {
          return e && e.length ? ki(e, ht(n, 2)) : []
        }
        function Gb(e, n) {
          return (
            (n = typeof n == 'function' ? n : i),
            e && e.length ? ki(e, i, n) : []
          )
        }
        function _l(e) {
          if (!(e && e.length)) return []
          var n = 0
          return (
            (e = bi(e, function (s) {
              if (se(s)) return (n = be(s.length, n)), !0
            })),
            Pa(n, function (s) {
              return Kt(e, Da(s))
            })
          )
        }
        function Dh(e, n) {
          if (!(e && e.length)) return []
          var s = _l(e)
          return n == null
            ? s
            : Kt(s, function (l) {
                return tr(n, i, l)
              })
        }
        var Xb = kt(function (e, n) {
            return se(e) ? ro(e, n) : []
          }),
          jb = kt(function (e) {
            return nl(bi(e, se))
          }),
          Zb = kt(function (e) {
            var n = wr(e)
            return se(n) && (n = i), nl(bi(e, se), ht(n, 2))
          }),
          Jb = kt(function (e) {
            var n = wr(e)
            return (n = typeof n == 'function' ? n : i), nl(bi(e, se), i, n)
          }),
          Qb = kt(_l)
        function t0(e, n) {
          return Jd(e || [], n || [], eo)
        }
        function e0(e, n) {
          return Jd(e || [], n || [], oo)
        }
        var r0 = kt(function (e) {
          var n = e.length,
            s = n > 1 ? e[n - 1] : i
          return (s = typeof s == 'function' ? (e.pop(), s) : i), Dh(e, s)
        })
        function Mh(e) {
          var n = g(e)
          return (n.__chain__ = !0), n
        }
        function i0(e, n) {
          return n(e), e
        }
        function $s(e, n) {
          return n(e)
        }
        var n0 = si(function (e) {
          var n = e.length,
            s = n ? e[0] : 0,
            l = this.__wrapped__,
            u = function (v) {
              return qa(v, e)
            }
          return n > 1 ||
            this.__actions__.length ||
            !(l instanceof At) ||
            !ai(s)
            ? this.thru(u)
            : ((l = l.slice(s, +s + (n ? 1 : 0))),
              l.__actions__.push({
                func: $s,
                args: [u],
                thisArg: i,
              }),
              new br(l, this.__chain__).thru(function (v) {
                return n && !v.length && v.push(i), v
              }))
        })
        function o0() {
          return Mh(this)
        }
        function s0() {
          return new br(this.value(), this.__chain__)
        }
        function a0() {
          this.__values__ === i && (this.__values__ = Xh(this.value()))
          var e = this.__index__ >= this.__values__.length,
            n = e ? i : this.__values__[this.__index__++]
          return { done: e, value: n }
        }
        function l0() {
          return this
        }
        function c0(e) {
          for (var n, s = this; s instanceof cs; ) {
            var l = Ah(s)
            ;(l.__index__ = 0),
              (l.__values__ = i),
              n ? (u.__wrapped__ = l) : (n = l)
            var u = l
            s = s.__wrapped__
          }
          return (u.__wrapped__ = e), n
        }
        function d0() {
          var e = this.__wrapped__
          if (e instanceof At) {
            var n = e
            return (
              this.__actions__.length && (n = new At(this)),
              (n = n.reverse()),
              n.__actions__.push({
                func: $s,
                args: [yl],
                thisArg: i,
              }),
              new br(n, this.__chain__)
            )
          }
          return this.thru(yl)
        }
        function h0() {
          return Zd(this.__wrapped__, this.__actions__)
        }
        var u0 = ms(function (e, n, s) {
          Vt.call(e, s) ? ++e[s] : ni(e, s, 1)
        })
        function p0(e, n, s) {
          var l = _t(e) ? dd : iv
          return s && Re(e, n, s) && (n = i), l(e, ht(n, 3))
        }
        function f0(e, n) {
          var s = _t(e) ? bi : Dd
          return s(e, ht(n, 3))
        }
        var g0 = lh(Eh),
          m0 = lh(Th)
        function v0(e, n) {
          return Se(Cs(e, n), 1)
        }
        function b0(e, n) {
          return Se(Cs(e, n), tt)
        }
        function y0(e, n, s) {
          return (s = s === i ? 1 : wt(s)), Se(Cs(e, n), s)
        }
        function Rh(e, n) {
          var s = _t(e) ? mr : xi
          return s(e, ht(n, 3))
        }
        function Ph(e, n) {
          var s = _t(e) ? Bg : Ld
          return s(e, ht(n, 3))
        }
        var _0 = ms(function (e, n, s) {
          Vt.call(e, s) ? e[s].push(n) : ni(e, s, [n])
        })
        function w0(e, n, s, l) {
          ;(e = We(e) ? e : zn(e)), (s = s && !l ? wt(s) : 0)
          var u = e.length
          return (
            s < 0 && (s = be(u + s, 0)),
            Ts(e) ? s <= u && e.indexOf(n, s) > -1 : !!u && gn(e, n, s) > -1
          )
        }
        var x0 = kt(function (e, n, s) {
            var l = -1,
              u = typeof n == 'function',
              v = We(e) ? I(e.length) : []
            return (
              xi(e, function (_) {
                v[++l] = u ? tr(n, _, s) : io(_, n, s)
              }),
              v
            )
          }),
          k0 = ms(function (e, n, s) {
            ni(e, s, n)
          })
        function Cs(e, n) {
          var s = _t(e) ? Kt : Nd
          return s(e, ht(n, 3))
        }
        function $0(e, n, s, l) {
          return e == null
            ? []
            : (_t(n) || (n = n == null ? [] : [n]),
              (s = l ? i : s),
              _t(s) || (s = s == null ? [] : [s]),
              Wd(e, n, s))
        }
        var C0 = ms(
          function (e, n, s) {
            e[s ? 0 : 1].push(n)
          },
          function () {
            return [[], []]
          },
        )
        function S0(e, n, s) {
          var l = _t(e) ? Oa : fd,
            u = arguments.length < 3
          return l(e, ht(n, 4), s, u, xi)
        }
        function z0(e, n, s) {
          var l = _t(e) ? Fg : fd,
            u = arguments.length < 3
          return l(e, ht(n, 4), s, u, Ld)
        }
        function A0(e, n) {
          var s = _t(e) ? bi : Dd
          return s(e, As(ht(n, 3)))
        }
        function E0(e) {
          var n = _t(e) ? Ed : wv
          return n(e)
        }
        function T0(e, n, s) {
          ;(s ? Re(e, n, s) : n === i) ? (n = 1) : (n = wt(n))
          var l = _t(e) ? Jm : xv
          return l(e, n)
        }
        function I0(e) {
          var n = _t(e) ? Qm : $v
          return n(e)
        }
        function O0(e) {
          if (e == null) return 0
          if (We(e)) return Ts(e) ? vn(e) : e.length
          var n = Oe(e)
          return n == zr || n == Ar ? e.size : Za(e).length
        }
        function L0(e, n, s) {
          var l = _t(e) ? La : Cv
          return s && Re(e, n, s) && (n = i), l(e, ht(n, 3))
        }
        var D0 = kt(function (e, n) {
            if (e == null) return []
            var s = n.length
            return (
              s > 1 && Re(e, n[0], n[1])
                ? (n = [])
                : s > 2 && Re(n[0], n[1], n[2]) && (n = [n[0]]),
              Wd(e, Se(n, 1), [])
            )
          }),
          Ss =
            fm ||
            function () {
              return Ce.Date.now()
            }
        function M0(e, n) {
          if (typeof n != 'function') throw new vr(h)
          return (
            (e = wt(e)),
            function () {
              if (--e < 1) return n.apply(this, arguments)
            }
          )
        }
        function Bh(e, n, s) {
          return (
            (n = s ? i : n),
            (n = e && n == null ? e.length : n),
            oi(e, H, i, i, i, i, n)
          )
        }
        function Fh(e, n) {
          var s
          if (typeof n != 'function') throw new vr(h)
          return (
            (e = wt(e)),
            function () {
              return (
                --e > 0 && (s = n.apply(this, arguments)), e <= 1 && (n = i), s
              )
            }
          )
        }
        var wl = kt(function (e, n, s) {
            var l = T
            if (s.length) {
              var u = _i(s, Cn(wl))
              l |= L
            }
            return oi(e, l, n, s, u)
          }),
          Nh = kt(function (e, n, s) {
            var l = T | P
            if (s.length) {
              var u = _i(s, Cn(Nh))
              l |= L
            }
            return oi(n, l, e, s, u)
          })
        function Uh(e, n, s) {
          n = s ? i : n
          var l = oi(e, R, i, i, i, i, i, n)
          return (l.placeholder = Uh.placeholder), l
        }
        function Hh(e, n, s) {
          n = s ? i : n
          var l = oi(e, O, i, i, i, i, i, n)
          return (l.placeholder = Hh.placeholder), l
        }
        function Vh(e, n, s) {
          var l,
            u,
            v,
            _,
            x,
            E,
            B = 0,
            N = !1,
            V = !1,
            Q = !0
          if (typeof e != 'function') throw new vr(h)
          ;(n = xr(n) || 0),
            Zt(s) &&
              ((N = !!s.leading),
              (V = 'maxWait' in s),
              (v = V ? be(xr(s.maxWait) || 0, n) : v),
              (Q = 'trailing' in s ? !!s.trailing : Q))
          function ot(ae) {
            var Or = l,
              di = u
            return (l = u = i), (B = ae), (_ = e.apply(di, Or)), _
          }
          function ut(ae) {
            return (B = ae), (x = lo(zt, n)), N ? ot(ae) : _
          }
          function xt(ae) {
            var Or = ae - E,
              di = ae - B,
              lu = n - Or
            return V ? Ie(lu, v - di) : lu
          }
          function pt(ae) {
            var Or = ae - E,
              di = ae - B
            return E === i || Or >= n || Or < 0 || (V && di >= v)
          }
          function zt() {
            var ae = Ss()
            if (pt(ae)) return Et(ae)
            x = lo(zt, xt(ae))
          }
          function Et(ae) {
            return (x = i), Q && l ? ot(ae) : ((l = u = i), _)
          }
          function nr() {
            x !== i && Qd(x), (B = 0), (l = E = u = x = i)
          }
          function Pe() {
            return x === i ? _ : Et(Ss())
          }
          function or() {
            var ae = Ss(),
              Or = pt(ae)
            if (((l = arguments), (u = this), (E = ae), Or)) {
              if (x === i) return ut(E)
              if (V) return Qd(x), (x = lo(zt, n)), ot(E)
            }
            return x === i && (x = lo(zt, n)), _
          }
          return (or.cancel = nr), (or.flush = Pe), or
        }
        var R0 = kt(function (e, n) {
            return Od(e, 1, n)
          }),
          P0 = kt(function (e, n, s) {
            return Od(e, xr(n) || 0, s)
          })
        function B0(e) {
          return oi(e, K)
        }
        function zs(e, n) {
          if (typeof e != 'function' || (n != null && typeof n != 'function'))
            throw new vr(h)
          var s = function () {
            var l = arguments,
              u = n ? n.apply(this, l) : l[0],
              v = s.cache
            if (v.has(u)) return v.get(u)
            var _ = e.apply(this, l)
            return (s.cache = v.set(u, _) || v), _
          }
          return (s.cache = new (zs.Cache || ii)()), s
        }
        zs.Cache = ii
        function As(e) {
          if (typeof e != 'function') throw new vr(h)
          return function () {
            var n = arguments
            switch (n.length) {
              case 0:
                return !e.call(this)
              case 1:
                return !e.call(this, n[0])
              case 2:
                return !e.call(this, n[0], n[1])
              case 3:
                return !e.call(this, n[0], n[1], n[2])
            }
            return !e.apply(this, n)
          }
        }
        function F0(e) {
          return Fh(2, e)
        }
        var N0 = Sv(function (e, n) {
            n =
              n.length == 1 && _t(n[0])
                ? Kt(n[0], er(ht()))
                : Kt(Se(n, 1), er(ht()))
            var s = n.length
            return kt(function (l) {
              for (var u = -1, v = Ie(l.length, s); ++u < v; )
                l[u] = n[u].call(this, l[u])
              return tr(e, this, l)
            })
          }),
          xl = kt(function (e, n) {
            var s = _i(n, Cn(xl))
            return oi(e, L, i, n, s)
          }),
          Wh = kt(function (e, n) {
            var s = _i(n, Cn(Wh))
            return oi(e, U, i, n, s)
          }),
          U0 = si(function (e, n) {
            return oi(e, F, i, i, i, n)
          })
        function H0(e, n) {
          if (typeof e != 'function') throw new vr(h)
          return (n = n === i ? n : wt(n)), kt(e, n)
        }
        function V0(e, n) {
          if (typeof e != 'function') throw new vr(h)
          return (
            (n = n == null ? 0 : be(wt(n), 0)),
            kt(function (s) {
              var l = s[n],
                u = Ci(s, 0, n)
              return l && yi(u, l), tr(e, this, u)
            })
          )
        }
        function W0(e, n, s) {
          var l = !0,
            u = !0
          if (typeof e != 'function') throw new vr(h)
          return (
            Zt(s) &&
              ((l = 'leading' in s ? !!s.leading : l),
              (u = 'trailing' in s ? !!s.trailing : u)),
            Vh(e, n, {
              leading: l,
              maxWait: n,
              trailing: u,
            })
          )
        }
        function q0(e) {
          return Bh(e, 1)
        }
        function Y0(e, n) {
          return xl(sl(n), e)
        }
        function K0() {
          if (!arguments.length) return []
          var e = arguments[0]
          return _t(e) ? e : [e]
        }
        function G0(e) {
          return yr(e, S)
        }
        function X0(e, n) {
          return (n = typeof n == 'function' ? n : i), yr(e, S, n)
        }
        function j0(e) {
          return yr(e, y | S)
        }
        function Z0(e, n) {
          return (n = typeof n == 'function' ? n : i), yr(e, y | S, n)
        }
        function J0(e, n) {
          return n == null || Id(e, n, xe(n))
        }
        function Ir(e, n) {
          return e === n || (e !== e && n !== n)
        }
        var Q0 = _s(Ga),
          ty = _s(function (e, n) {
            return e >= n
          }),
          Yi = Pd(
            /* @__PURE__ */ (function () {
              return arguments
            })(),
          )
            ? Pd
            : function (e) {
                return ee(e) && Vt.call(e, 'callee') && !kd.call(e, 'callee')
              },
          _t = I.isArray,
          ey = nd ? er(nd) : cv
        function We(e) {
          return e != null && Es(e.length) && !li(e)
        }
        function se(e) {
          return ee(e) && We(e)
        }
        function ry(e) {
          return e === !0 || e === !1 || (ee(e) && Me(e) == Sr)
        }
        var Si = mm || Ll,
          iy = od ? er(od) : dv
        function ny(e) {
          return ee(e) && e.nodeType === 1 && !co(e)
        }
        function oy(e) {
          if (e == null) return !0
          if (
            We(e) &&
            (_t(e) ||
              typeof e == 'string' ||
              typeof e.splice == 'function' ||
              Si(e) ||
              Sn(e) ||
              Yi(e))
          )
            return !e.length
          var n = Oe(e)
          if (n == zr || n == Ar) return !e.size
          if (ao(e)) return !Za(e).length
          for (var s in e) if (Vt.call(e, s)) return !1
          return !0
        }
        function sy(e, n) {
          return no(e, n)
        }
        function ay(e, n, s) {
          s = typeof s == 'function' ? s : i
          var l = s ? s(e, n) : i
          return l === i ? no(e, n, i, s) : !!l
        }
        function kl(e) {
          if (!ee(e)) return !1
          var n = Me(e)
          return (
            n == Qe ||
            n == Te ||
            (typeof e.message == 'string' &&
              typeof e.name == 'string' &&
              !co(e))
          )
        }
        function ly(e) {
          return typeof e == 'number' && Cd(e)
        }
        function li(e) {
          if (!Zt(e)) return !1
          var n = Me(e)
          return n == Nr || n == Pi || n == ti || n == Of
        }
        function qh(e) {
          return typeof e == 'number' && e == wt(e)
        }
        function Es(e) {
          return typeof e == 'number' && e > -1 && e % 1 == 0 && e <= j
        }
        function Zt(e) {
          var n = typeof e
          return e != null && (n == 'object' || n == 'function')
        }
        function ee(e) {
          return e != null && typeof e == 'object'
        }
        var Yh = sd ? er(sd) : uv
        function cy(e, n) {
          return e === n || ja(e, n, pl(n))
        }
        function dy(e, n, s) {
          return (s = typeof s == 'function' ? s : i), ja(e, n, pl(n), s)
        }
        function hy(e) {
          return Kh(e) && e != +e
        }
        function uy(e) {
          if (Xv(e)) throw new vt(d)
          return Bd(e)
        }
        function py(e) {
          return e === null
        }
        function fy(e) {
          return e == null
        }
        function Kh(e) {
          return typeof e == 'number' || (ee(e) && Me(e) == Wn)
        }
        function co(e) {
          if (!ee(e) || Me(e) != ei) return !1
          var n = rs(e)
          if (n === null) return !0
          var s = Vt.call(n, 'constructor') && n.constructor
          return typeof s == 'function' && s instanceof s && Jo.call(s) == dm
        }
        var $l = ad ? er(ad) : pv
        function gy(e) {
          return qh(e) && e >= -j && e <= j
        }
        var Gh = ld ? er(ld) : fv
        function Ts(e) {
          return typeof e == 'string' || (!_t(e) && ee(e) && Me(e) == Yn)
        }
        function ir(e) {
          return typeof e == 'symbol' || (ee(e) && Me(e) == Vo)
        }
        var Sn = cd ? er(cd) : gv
        function my(e) {
          return e === i
        }
        function vy(e) {
          return ee(e) && Oe(e) == Kn
        }
        function by(e) {
          return ee(e) && Me(e) == Df
        }
        var yy = _s(Ja),
          _y = _s(function (e, n) {
            return e <= n
          })
        function Xh(e) {
          if (!e) return []
          if (We(e)) return Ts(e) ? Er(e) : Ve(e)
          if (jn && e[jn]) return Jg(e[jn]())
          var n = Oe(e),
            s = n == zr ? Fa : n == Ar ? Xo : zn
          return s(e)
        }
        function ci(e) {
          if (!e) return e === 0 ? e : 0
          if (((e = xr(e)), e === tt || e === -tt)) {
            var n = e < 0 ? -1 : 1
            return n * yt
          }
          return e === e ? e : 0
        }
        function wt(e) {
          var n = ci(e),
            s = n % 1
          return n === n ? (s ? n - s : n) : 0
        }
        function jh(e) {
          return e ? Hi(wt(e), 0, St) : 0
        }
        function xr(e) {
          if (typeof e == 'number') return e
          if (ir(e)) return gt
          if (Zt(e)) {
            var n = typeof e.valueOf == 'function' ? e.valueOf() : e
            e = Zt(n) ? n + '' : n
          }
          if (typeof e != 'string') return e === 0 ? e : +e
          e = gd(e)
          var s = eg.test(e)
          return s || ig.test(e)
            ? Mg(e.slice(2), s ? 2 : 8)
            : tg.test(e)
            ? gt
            : +e
        }
        function Zh(e) {
          return Hr(e, qe(e))
        }
        function wy(e) {
          return e ? Hi(wt(e), -j, j) : e === 0 ? e : 0
        }
        function Ft(e) {
          return e == null ? '' : rr(e)
        }
        var xy = kn(function (e, n) {
            if (ao(n) || We(n)) {
              Hr(n, xe(n), e)
              return
            }
            for (var s in n) Vt.call(n, s) && eo(e, s, n[s])
          }),
          Jh = kn(function (e, n) {
            Hr(n, qe(n), e)
          }),
          Is = kn(function (e, n, s, l) {
            Hr(n, qe(n), e, l)
          }),
          ky = kn(function (e, n, s, l) {
            Hr(n, xe(n), e, l)
          }),
          $y = si(qa)
        function Cy(e, n) {
          var s = xn(e)
          return n == null ? s : Td(s, n)
        }
        var Sy = kt(function (e, n) {
            e = Wt(e)
            var s = -1,
              l = n.length,
              u = l > 2 ? n[2] : i
            for (u && Re(n[0], n[1], u) && (l = 1); ++s < l; )
              for (var v = n[s], _ = qe(v), x = -1, E = _.length; ++x < E; ) {
                var B = _[x],
                  N = e[B]
                ;(N === i || (Ir(N, yn[B]) && !Vt.call(e, B))) && (e[B] = v[B])
              }
            return e
          }),
          zy = kt(function (e) {
            return e.push(i, gh), tr(Qh, i, e)
          })
        function Ay(e, n) {
          return hd(e, ht(n, 3), Ur)
        }
        function Ey(e, n) {
          return hd(e, ht(n, 3), Ka)
        }
        function Ty(e, n) {
          return e == null ? e : Ya(e, ht(n, 3), qe)
        }
        function Iy(e, n) {
          return e == null ? e : Md(e, ht(n, 3), qe)
        }
        function Oy(e, n) {
          return e && Ur(e, ht(n, 3))
        }
        function Ly(e, n) {
          return e && Ka(e, ht(n, 3))
        }
        function Dy(e) {
          return e == null ? [] : us(e, xe(e))
        }
        function My(e) {
          return e == null ? [] : us(e, qe(e))
        }
        function Cl(e, n, s) {
          var l = e == null ? i : Vi(e, n)
          return l === i ? s : l
        }
        function Ry(e, n) {
          return e != null && bh(e, n, ov)
        }
        function Sl(e, n) {
          return e != null && bh(e, n, sv)
        }
        var Py = dh(function (e, n, s) {
            n != null && typeof n.toString != 'function' && (n = Qo.call(n)),
              (e[n] = s)
          }, Al(Ye)),
          By = dh(function (e, n, s) {
            n != null && typeof n.toString != 'function' && (n = Qo.call(n)),
              Vt.call(e, n) ? e[n].push(s) : (e[n] = [s])
          }, ht),
          Fy = kt(io)
        function xe(e) {
          return We(e) ? Ad(e) : Za(e)
        }
        function qe(e) {
          return We(e) ? Ad(e, !0) : mv(e)
        }
        function Ny(e, n) {
          var s = {}
          return (
            (n = ht(n, 3)),
            Ur(e, function (l, u, v) {
              ni(s, n(l, u, v), l)
            }),
            s
          )
        }
        function Uy(e, n) {
          var s = {}
          return (
            (n = ht(n, 3)),
            Ur(e, function (l, u, v) {
              ni(s, u, n(l, u, v))
            }),
            s
          )
        }
        var Hy = kn(function (e, n, s) {
            ps(e, n, s)
          }),
          Qh = kn(function (e, n, s, l) {
            ps(e, n, s, l)
          }),
          Vy = si(function (e, n) {
            var s = {}
            if (e == null) return s
            var l = !1
            ;(n = Kt(n, function (v) {
              return (v = $i(v, e)), l || (l = v.length > 1), v
            })),
              Hr(e, hl(e), s),
              l && (s = yr(s, y | k | S, Pv))
            for (var u = n.length; u--; ) il(s, n[u])
            return s
          })
        function Wy(e, n) {
          return tu(e, As(ht(n)))
        }
        var qy = si(function (e, n) {
          return e == null ? {} : bv(e, n)
        })
        function tu(e, n) {
          if (e == null) return {}
          var s = Kt(hl(e), function (l) {
            return [l]
          })
          return (
            (n = ht(n)),
            qd(e, s, function (l, u) {
              return n(l, u[0])
            })
          )
        }
        function Yy(e, n, s) {
          n = $i(n, e)
          var l = -1,
            u = n.length
          for (u || ((u = 1), (e = i)); ++l < u; ) {
            var v = e == null ? i : e[Vr(n[l])]
            v === i && ((l = u), (v = s)), (e = li(v) ? v.call(e) : v)
          }
          return e
        }
        function Ky(e, n, s) {
          return e == null ? e : oo(e, n, s)
        }
        function Gy(e, n, s, l) {
          return (
            (l = typeof l == 'function' ? l : i), e == null ? e : oo(e, n, s, l)
          )
        }
        var eu = ph(xe),
          ru = ph(qe)
        function Xy(e, n, s) {
          var l = _t(e),
            u = l || Si(e) || Sn(e)
          if (((n = ht(n, 4)), s == null)) {
            var v = e && e.constructor
            u
              ? (s = l ? new v() : [])
              : Zt(e)
              ? (s = li(v) ? xn(rs(e)) : {})
              : (s = {})
          }
          return (
            (u ? mr : Ur)(e, function (_, x, E) {
              return n(s, _, x, E)
            }),
            s
          )
        }
        function jy(e, n) {
          return e == null ? !0 : il(e, n)
        }
        function Zy(e, n, s) {
          return e == null ? e : jd(e, n, sl(s))
        }
        function Jy(e, n, s, l) {
          return (
            (l = typeof l == 'function' ? l : i),
            e == null ? e : jd(e, n, sl(s), l)
          )
        }
        function zn(e) {
          return e == null ? [] : Ba(e, xe(e))
        }
        function Qy(e) {
          return e == null ? [] : Ba(e, qe(e))
        }
        function t1(e, n, s) {
          return (
            s === i && ((s = n), (n = i)),
            s !== i && ((s = xr(s)), (s = s === s ? s : 0)),
            n !== i && ((n = xr(n)), (n = n === n ? n : 0)),
            Hi(xr(e), n, s)
          )
        }
        function e1(e, n, s) {
          return (
            (n = ci(n)),
            s === i ? ((s = n), (n = 0)) : (s = ci(s)),
            (e = xr(e)),
            av(e, n, s)
          )
        }
        function r1(e, n, s) {
          if (
            (s && typeof s != 'boolean' && Re(e, n, s) && (n = s = i),
            s === i &&
              (typeof n == 'boolean'
                ? ((s = n), (n = i))
                : typeof e == 'boolean' && ((s = e), (e = i))),
            e === i && n === i
              ? ((e = 0), (n = 1))
              : ((e = ci(e)), n === i ? ((n = e), (e = 0)) : (n = ci(n))),
            e > n)
          ) {
            var l = e
            ;(e = n), (n = l)
          }
          if (s || e % 1 || n % 1) {
            var u = Sd()
            return Ie(e + u * (n - e + Dg('1e-' + ((u + '').length - 1))), n)
          }
          return tl(e, n)
        }
        var i1 = $n(function (e, n, s) {
          return (n = n.toLowerCase()), e + (s ? iu(n) : n)
        })
        function iu(e) {
          return zl(Ft(e).toLowerCase())
        }
        function nu(e) {
          return (e = Ft(e)), e && e.replace(og, Kg).replace($g, '')
        }
        function n1(e, n, s) {
          ;(e = Ft(e)), (n = rr(n))
          var l = e.length
          s = s === i ? l : Hi(wt(s), 0, l)
          var u = s
          return (s -= n.length), s >= 0 && e.slice(s, u) == n
        }
        function o1(e) {
          return (e = Ft(e)), e && Ff.test(e) ? e.replace(Mc, Gg) : e
        }
        function s1(e) {
          return (e = Ft(e)), e && qf.test(e) ? e.replace(xa, '\\$&') : e
        }
        var a1 = $n(function (e, n, s) {
            return e + (s ? '-' : '') + n.toLowerCase()
          }),
          l1 = $n(function (e, n, s) {
            return e + (s ? ' ' : '') + n.toLowerCase()
          }),
          c1 = ah('toLowerCase')
        function d1(e, n, s) {
          ;(e = Ft(e)), (n = wt(n))
          var l = n ? vn(e) : 0
          if (!n || l >= n) return e
          var u = (n - l) / 2
          return ys(ss(u), s) + e + ys(os(u), s)
        }
        function h1(e, n, s) {
          ;(e = Ft(e)), (n = wt(n))
          var l = n ? vn(e) : 0
          return n && l < n ? e + ys(n - l, s) : e
        }
        function u1(e, n, s) {
          ;(e = Ft(e)), (n = wt(n))
          var l = n ? vn(e) : 0
          return n && l < n ? ys(n - l, s) + e : e
        }
        function p1(e, n, s) {
          return (
            s || n == null ? (n = 0) : n && (n = +n),
            _m(Ft(e).replace(ka, ''), n || 0)
          )
        }
        function f1(e, n, s) {
          return (
            (s ? Re(e, n, s) : n === i) ? (n = 1) : (n = wt(n)), el(Ft(e), n)
          )
        }
        function g1() {
          var e = arguments,
            n = Ft(e[0])
          return e.length < 3 ? n : n.replace(e[1], e[2])
        }
        var m1 = $n(function (e, n, s) {
          return e + (s ? '_' : '') + n.toLowerCase()
        })
        function v1(e, n, s) {
          return (
            s && typeof s != 'number' && Re(e, n, s) && (n = s = i),
            (s = s === i ? St : s >>> 0),
            s
              ? ((e = Ft(e)),
                e &&
                (typeof n == 'string' || (n != null && !$l(n))) &&
                ((n = rr(n)), !n && mn(e))
                  ? Ci(Er(e), 0, s)
                  : e.split(n, s))
              : []
          )
        }
        var b1 = $n(function (e, n, s) {
          return e + (s ? ' ' : '') + zl(n)
        })
        function y1(e, n, s) {
          return (
            (e = Ft(e)),
            (s = s == null ? 0 : Hi(wt(s), 0, e.length)),
            (n = rr(n)),
            e.slice(s, s + n.length) == n
          )
        }
        function _1(e, n, s) {
          var l = g.templateSettings
          s && Re(e, n, s) && (n = i), (e = Ft(e)), (n = Is({}, n, l, fh))
          var u = Is({}, n.imports, l.imports, fh),
            v = xe(u),
            _ = Ba(u, v),
            x,
            E,
            B = 0,
            N = n.interpolate || Wo,
            V = "__p += '",
            Q = Na(
              (n.escape || Wo).source +
                '|' +
                N.source +
                '|' +
                (N === Rc ? Qf : Wo).source +
                '|' +
                (n.evaluate || Wo).source +
                '|$',
              'g',
            ),
            ot =
              '//# sourceURL=' +
              (Vt.call(n, 'sourceURL')
                ? (n.sourceURL + '').replace(/\s/g, ' ')
                : 'lodash.templateSources[' + ++Eg + ']') +
              `
`
          e.replace(Q, function (pt, zt, Et, nr, Pe, or) {
            return (
              Et || (Et = nr),
              (V += e.slice(B, or).replace(sg, Xg)),
              zt &&
                ((x = !0),
                (V +=
                  `' +
__e(` +
                  zt +
                  `) +
'`)),
              Pe &&
                ((E = !0),
                (V +=
                  `';
` +
                  Pe +
                  `;
__p += '`)),
              Et &&
                (V +=
                  `' +
((__t = (` +
                  Et +
                  `)) == null ? '' : __t) +
'`),
              (B = or + pt.length),
              pt
            )
          }),
            (V += `';
`)
          var ut = Vt.call(n, 'variable') && n.variable
          if (!ut)
            V =
              `with (obj) {
` +
              V +
              `
}
`
          else if (Zf.test(ut)) throw new vt(m)
          ;(V = (E ? V.replace(Mf, '') : V)
            .replace(Rf, '$1')
            .replace(Pf, '$1;')),
            (V =
              'function(' +
              (ut || 'obj') +
              `) {
` +
              (ut
                ? ''
                : `obj || (obj = {});
`) +
              "var __t, __p = ''" +
              (x ? ', __e = _.escape' : '') +
              (E
                ? `, __j = Array.prototype.join;
function print() { __p += __j.call(arguments, '') }
`
                : `;
`) +
              V +
              `return __p
}`)
          var xt = su(function () {
            return Pt(v, ot + 'return ' + V).apply(i, _)
          })
          if (((xt.source = V), kl(xt))) throw xt
          return xt
        }
        function w1(e) {
          return Ft(e).toLowerCase()
        }
        function x1(e) {
          return Ft(e).toUpperCase()
        }
        function k1(e, n, s) {
          if (((e = Ft(e)), e && (s || n === i))) return gd(e)
          if (!e || !(n = rr(n))) return e
          var l = Er(e),
            u = Er(n),
            v = md(l, u),
            _ = vd(l, u) + 1
          return Ci(l, v, _).join('')
        }
        function $1(e, n, s) {
          if (((e = Ft(e)), e && (s || n === i))) return e.slice(0, yd(e) + 1)
          if (!e || !(n = rr(n))) return e
          var l = Er(e),
            u = vd(l, Er(n)) + 1
          return Ci(l, 0, u).join('')
        }
        function C1(e, n, s) {
          if (((e = Ft(e)), e && (s || n === i))) return e.replace(ka, '')
          if (!e || !(n = rr(n))) return e
          var l = Er(e),
            u = md(l, Er(n))
          return Ci(l, u).join('')
        }
        function S1(e, n) {
          var s = q,
            l = et
          if (Zt(n)) {
            var u = 'separator' in n ? n.separator : u
            ;(s = 'length' in n ? wt(n.length) : s),
              (l = 'omission' in n ? rr(n.omission) : l)
          }
          e = Ft(e)
          var v = e.length
          if (mn(e)) {
            var _ = Er(e)
            v = _.length
          }
          if (s >= v) return e
          var x = s - vn(l)
          if (x < 1) return l
          var E = _ ? Ci(_, 0, x).join('') : e.slice(0, x)
          if (u === i) return E + l
          if ((_ && (x += E.length - x), $l(u))) {
            if (e.slice(x).search(u)) {
              var B,
                N = E
              for (
                u.global || (u = Na(u.source, Ft(Pc.exec(u)) + 'g')),
                  u.lastIndex = 0;
                (B = u.exec(N));

              )
                var V = B.index
              E = E.slice(0, V === i ? x : V)
            }
          } else if (e.indexOf(rr(u), x) != x) {
            var Q = E.lastIndexOf(u)
            Q > -1 && (E = E.slice(0, Q))
          }
          return E + l
        }
        function z1(e) {
          return (e = Ft(e)), e && Bf.test(e) ? e.replace(Dc, rm) : e
        }
        var A1 = $n(function (e, n, s) {
            return e + (s ? ' ' : '') + n.toUpperCase()
          }),
          zl = ah('toUpperCase')
        function ou(e, n, s) {
          return (
            (e = Ft(e)),
            (n = s ? i : n),
            n === i ? (Zg(e) ? om(e) : Hg(e)) : e.match(n) || []
          )
        }
        var su = kt(function (e, n) {
            try {
              return tr(e, i, n)
            } catch (s) {
              return kl(s) ? s : new vt(s)
            }
          }),
          E1 = si(function (e, n) {
            return (
              mr(n, function (s) {
                ;(s = Vr(s)), ni(e, s, wl(e[s], e))
              }),
              e
            )
          })
        function T1(e) {
          var n = e == null ? 0 : e.length,
            s = ht()
          return (
            (e = n
              ? Kt(e, function (l) {
                  if (typeof l[1] != 'function') throw new vr(h)
                  return [s(l[0]), l[1]]
                })
              : []),
            kt(function (l) {
              for (var u = -1; ++u < n; ) {
                var v = e[u]
                if (tr(v[0], this, l)) return tr(v[1], this, l)
              }
            })
          )
        }
        function I1(e) {
          return rv(yr(e, y))
        }
        function Al(e) {
          return function () {
            return e
          }
        }
        function O1(e, n) {
          return e == null || e !== e ? n : e
        }
        var L1 = ch(),
          D1 = ch(!0)
        function Ye(e) {
          return e
        }
        function El(e) {
          return Fd(typeof e == 'function' ? e : yr(e, y))
        }
        function M1(e) {
          return Ud(yr(e, y))
        }
        function R1(e, n) {
          return Hd(e, yr(n, y))
        }
        var P1 = kt(function (e, n) {
            return function (s) {
              return io(s, e, n)
            }
          }),
          B1 = kt(function (e, n) {
            return function (s) {
              return io(e, s, n)
            }
          })
        function Tl(e, n, s) {
          var l = xe(n),
            u = us(n, l)
          s == null &&
            !(Zt(n) && (u.length || !l.length)) &&
            ((s = n), (n = e), (e = this), (u = us(n, xe(n))))
          var v = !(Zt(s) && 'chain' in s) || !!s.chain,
            _ = li(e)
          return (
            mr(u, function (x) {
              var E = n[x]
              ;(e[x] = E),
                _ &&
                  (e.prototype[x] = function () {
                    var B = this.__chain__
                    if (v || B) {
                      var N = e(this.__wrapped__),
                        V = (N.__actions__ = Ve(this.__actions__))
                      return (
                        V.push({ func: E, args: arguments, thisArg: e }),
                        (N.__chain__ = B),
                        N
                      )
                    }
                    return E.apply(e, yi([this.value()], arguments))
                  })
            }),
            e
          )
        }
        function F1() {
          return Ce._ === this && (Ce._ = hm), this
        }
        function Il() {}
        function N1(e) {
          return (
            (e = wt(e)),
            kt(function (n) {
              return Vd(n, e)
            })
          )
        }
        var U1 = ll(Kt),
          H1 = ll(dd),
          V1 = ll(La)
        function au(e) {
          return gl(e) ? Da(Vr(e)) : yv(e)
        }
        function W1(e) {
          return function (n) {
            return e == null ? i : Vi(e, n)
          }
        }
        var q1 = hh(),
          Y1 = hh(!0)
        function Ol() {
          return []
        }
        function Ll() {
          return !1
        }
        function K1() {
          return {}
        }
        function G1() {
          return ''
        }
        function X1() {
          return !0
        }
        function j1(e, n) {
          if (((e = wt(e)), e < 1 || e > j)) return []
          var s = St,
            l = Ie(e, St)
          ;(n = ht(n)), (e -= St)
          for (var u = Pa(l, n); ++s < e; ) n(s)
          return u
        }
        function Z1(e) {
          return _t(e) ? Kt(e, Vr) : ir(e) ? [e] : Ve(zh(Ft(e)))
        }
        function J1(e) {
          var n = ++cm
          return Ft(e) + n
        }
        var Q1 = bs(function (e, n) {
            return e + n
          }, 0),
          t_ = cl('ceil'),
          e_ = bs(function (e, n) {
            return e / n
          }, 1),
          r_ = cl('floor')
        function i_(e) {
          return e && e.length ? hs(e, Ye, Ga) : i
        }
        function n_(e, n) {
          return e && e.length ? hs(e, ht(n, 2), Ga) : i
        }
        function o_(e) {
          return pd(e, Ye)
        }
        function s_(e, n) {
          return pd(e, ht(n, 2))
        }
        function a_(e) {
          return e && e.length ? hs(e, Ye, Ja) : i
        }
        function l_(e, n) {
          return e && e.length ? hs(e, ht(n, 2), Ja) : i
        }
        var c_ = bs(function (e, n) {
            return e * n
          }, 1),
          d_ = cl('round'),
          h_ = bs(function (e, n) {
            return e - n
          }, 0)
        function u_(e) {
          return e && e.length ? Ra(e, Ye) : 0
        }
        function p_(e, n) {
          return e && e.length ? Ra(e, ht(n, 2)) : 0
        }
        return (
          (g.after = M0),
          (g.ary = Bh),
          (g.assign = xy),
          (g.assignIn = Jh),
          (g.assignInWith = Is),
          (g.assignWith = ky),
          (g.at = $y),
          (g.before = Fh),
          (g.bind = wl),
          (g.bindAll = E1),
          (g.bindKey = Nh),
          (g.castArray = K0),
          (g.chain = Mh),
          (g.chunk = rb),
          (g.compact = ib),
          (g.concat = nb),
          (g.cond = T1),
          (g.conforms = I1),
          (g.constant = Al),
          (g.countBy = u0),
          (g.create = Cy),
          (g.curry = Uh),
          (g.curryRight = Hh),
          (g.debounce = Vh),
          (g.defaults = Sy),
          (g.defaultsDeep = zy),
          (g.defer = R0),
          (g.delay = P0),
          (g.difference = ob),
          (g.differenceBy = sb),
          (g.differenceWith = ab),
          (g.drop = lb),
          (g.dropRight = cb),
          (g.dropRightWhile = db),
          (g.dropWhile = hb),
          (g.fill = ub),
          (g.filter = f0),
          (g.flatMap = v0),
          (g.flatMapDeep = b0),
          (g.flatMapDepth = y0),
          (g.flatten = Ih),
          (g.flattenDeep = pb),
          (g.flattenDepth = fb),
          (g.flip = B0),
          (g.flow = L1),
          (g.flowRight = D1),
          (g.fromPairs = gb),
          (g.functions = Dy),
          (g.functionsIn = My),
          (g.groupBy = _0),
          (g.initial = vb),
          (g.intersection = bb),
          (g.intersectionBy = yb),
          (g.intersectionWith = _b),
          (g.invert = Py),
          (g.invertBy = By),
          (g.invokeMap = x0),
          (g.iteratee = El),
          (g.keyBy = k0),
          (g.keys = xe),
          (g.keysIn = qe),
          (g.map = Cs),
          (g.mapKeys = Ny),
          (g.mapValues = Uy),
          (g.matches = M1),
          (g.matchesProperty = R1),
          (g.memoize = zs),
          (g.merge = Hy),
          (g.mergeWith = Qh),
          (g.method = P1),
          (g.methodOf = B1),
          (g.mixin = Tl),
          (g.negate = As),
          (g.nthArg = N1),
          (g.omit = Vy),
          (g.omitBy = Wy),
          (g.once = F0),
          (g.orderBy = $0),
          (g.over = U1),
          (g.overArgs = N0),
          (g.overEvery = H1),
          (g.overSome = V1),
          (g.partial = xl),
          (g.partialRight = Wh),
          (g.partition = C0),
          (g.pick = qy),
          (g.pickBy = tu),
          (g.property = au),
          (g.propertyOf = W1),
          (g.pull = $b),
          (g.pullAll = Lh),
          (g.pullAllBy = Cb),
          (g.pullAllWith = Sb),
          (g.pullAt = zb),
          (g.range = q1),
          (g.rangeRight = Y1),
          (g.rearg = U0),
          (g.reject = A0),
          (g.remove = Ab),
          (g.rest = H0),
          (g.reverse = yl),
          (g.sampleSize = T0),
          (g.set = Ky),
          (g.setWith = Gy),
          (g.shuffle = I0),
          (g.slice = Eb),
          (g.sortBy = D0),
          (g.sortedUniq = Rb),
          (g.sortedUniqBy = Pb),
          (g.split = v1),
          (g.spread = V0),
          (g.tail = Bb),
          (g.take = Fb),
          (g.takeRight = Nb),
          (g.takeRightWhile = Ub),
          (g.takeWhile = Hb),
          (g.tap = i0),
          (g.throttle = W0),
          (g.thru = $s),
          (g.toArray = Xh),
          (g.toPairs = eu),
          (g.toPairsIn = ru),
          (g.toPath = Z1),
          (g.toPlainObject = Zh),
          (g.transform = Xy),
          (g.unary = q0),
          (g.union = Vb),
          (g.unionBy = Wb),
          (g.unionWith = qb),
          (g.uniq = Yb),
          (g.uniqBy = Kb),
          (g.uniqWith = Gb),
          (g.unset = jy),
          (g.unzip = _l),
          (g.unzipWith = Dh),
          (g.update = Zy),
          (g.updateWith = Jy),
          (g.values = zn),
          (g.valuesIn = Qy),
          (g.without = Xb),
          (g.words = ou),
          (g.wrap = Y0),
          (g.xor = jb),
          (g.xorBy = Zb),
          (g.xorWith = Jb),
          (g.zip = Qb),
          (g.zipObject = t0),
          (g.zipObjectDeep = e0),
          (g.zipWith = r0),
          (g.entries = eu),
          (g.entriesIn = ru),
          (g.extend = Jh),
          (g.extendWith = Is),
          Tl(g, g),
          (g.add = Q1),
          (g.attempt = su),
          (g.camelCase = i1),
          (g.capitalize = iu),
          (g.ceil = t_),
          (g.clamp = t1),
          (g.clone = G0),
          (g.cloneDeep = j0),
          (g.cloneDeepWith = Z0),
          (g.cloneWith = X0),
          (g.conformsTo = J0),
          (g.deburr = nu),
          (g.defaultTo = O1),
          (g.divide = e_),
          (g.endsWith = n1),
          (g.eq = Ir),
          (g.escape = o1),
          (g.escapeRegExp = s1),
          (g.every = p0),
          (g.find = g0),
          (g.findIndex = Eh),
          (g.findKey = Ay),
          (g.findLast = m0),
          (g.findLastIndex = Th),
          (g.findLastKey = Ey),
          (g.floor = r_),
          (g.forEach = Rh),
          (g.forEachRight = Ph),
          (g.forIn = Ty),
          (g.forInRight = Iy),
          (g.forOwn = Oy),
          (g.forOwnRight = Ly),
          (g.get = Cl),
          (g.gt = Q0),
          (g.gte = ty),
          (g.has = Ry),
          (g.hasIn = Sl),
          (g.head = Oh),
          (g.identity = Ye),
          (g.includes = w0),
          (g.indexOf = mb),
          (g.inRange = e1),
          (g.invoke = Fy),
          (g.isArguments = Yi),
          (g.isArray = _t),
          (g.isArrayBuffer = ey),
          (g.isArrayLike = We),
          (g.isArrayLikeObject = se),
          (g.isBoolean = ry),
          (g.isBuffer = Si),
          (g.isDate = iy),
          (g.isElement = ny),
          (g.isEmpty = oy),
          (g.isEqual = sy),
          (g.isEqualWith = ay),
          (g.isError = kl),
          (g.isFinite = ly),
          (g.isFunction = li),
          (g.isInteger = qh),
          (g.isLength = Es),
          (g.isMap = Yh),
          (g.isMatch = cy),
          (g.isMatchWith = dy),
          (g.isNaN = hy),
          (g.isNative = uy),
          (g.isNil = fy),
          (g.isNull = py),
          (g.isNumber = Kh),
          (g.isObject = Zt),
          (g.isObjectLike = ee),
          (g.isPlainObject = co),
          (g.isRegExp = $l),
          (g.isSafeInteger = gy),
          (g.isSet = Gh),
          (g.isString = Ts),
          (g.isSymbol = ir),
          (g.isTypedArray = Sn),
          (g.isUndefined = my),
          (g.isWeakMap = vy),
          (g.isWeakSet = by),
          (g.join = wb),
          (g.kebabCase = a1),
          (g.last = wr),
          (g.lastIndexOf = xb),
          (g.lowerCase = l1),
          (g.lowerFirst = c1),
          (g.lt = yy),
          (g.lte = _y),
          (g.max = i_),
          (g.maxBy = n_),
          (g.mean = o_),
          (g.meanBy = s_),
          (g.min = a_),
          (g.minBy = l_),
          (g.stubArray = Ol),
          (g.stubFalse = Ll),
          (g.stubObject = K1),
          (g.stubString = G1),
          (g.stubTrue = X1),
          (g.multiply = c_),
          (g.nth = kb),
          (g.noConflict = F1),
          (g.noop = Il),
          (g.now = Ss),
          (g.pad = d1),
          (g.padEnd = h1),
          (g.padStart = u1),
          (g.parseInt = p1),
          (g.random = r1),
          (g.reduce = S0),
          (g.reduceRight = z0),
          (g.repeat = f1),
          (g.replace = g1),
          (g.result = Yy),
          (g.round = d_),
          (g.runInContext = z),
          (g.sample = E0),
          (g.size = O0),
          (g.snakeCase = m1),
          (g.some = L0),
          (g.sortedIndex = Tb),
          (g.sortedIndexBy = Ib),
          (g.sortedIndexOf = Ob),
          (g.sortedLastIndex = Lb),
          (g.sortedLastIndexBy = Db),
          (g.sortedLastIndexOf = Mb),
          (g.startCase = b1),
          (g.startsWith = y1),
          (g.subtract = h_),
          (g.sum = u_),
          (g.sumBy = p_),
          (g.template = _1),
          (g.times = j1),
          (g.toFinite = ci),
          (g.toInteger = wt),
          (g.toLength = jh),
          (g.toLower = w1),
          (g.toNumber = xr),
          (g.toSafeInteger = wy),
          (g.toString = Ft),
          (g.toUpper = x1),
          (g.trim = k1),
          (g.trimEnd = $1),
          (g.trimStart = C1),
          (g.truncate = S1),
          (g.unescape = z1),
          (g.uniqueId = J1),
          (g.upperCase = A1),
          (g.upperFirst = zl),
          (g.each = Rh),
          (g.eachRight = Ph),
          (g.first = Oh),
          Tl(
            g,
            (function () {
              var e = {}
              return (
                Ur(g, function (n, s) {
                  Vt.call(g.prototype, s) || (e[s] = n)
                }),
                e
              )
            })(),
            { chain: !1 },
          ),
          (g.VERSION = o),
          mr(
            [
              'bind',
              'bindKey',
              'curry',
              'curryRight',
              'partial',
              'partialRight',
            ],
            function (e) {
              g[e].placeholder = g
            },
          ),
          mr(['drop', 'take'], function (e, n) {
            ;(At.prototype[e] = function (s) {
              s = s === i ? 1 : be(wt(s), 0)
              var l = this.__filtered__ && !n ? new At(this) : this.clone()
              return (
                l.__filtered__
                  ? (l.__takeCount__ = Ie(s, l.__takeCount__))
                  : l.__views__.push({
                      size: Ie(s, St),
                      type: e + (l.__dir__ < 0 ? 'Right' : ''),
                    }),
                l
              )
            }),
              (At.prototype[e + 'Right'] = function (s) {
                return this.reverse()[e](s).reverse()
              })
          }),
          mr(['filter', 'map', 'takeWhile'], function (e, n) {
            var s = n + 1,
              l = s == Z || s == Y
            At.prototype[e] = function (u) {
              var v = this.clone()
              return (
                v.__iteratees__.push({
                  iteratee: ht(u, 3),
                  type: s,
                }),
                (v.__filtered__ = v.__filtered__ || l),
                v
              )
            }
          }),
          mr(['head', 'last'], function (e, n) {
            var s = 'take' + (n ? 'Right' : '')
            At.prototype[e] = function () {
              return this[s](1).value()[0]
            }
          }),
          mr(['initial', 'tail'], function (e, n) {
            var s = 'drop' + (n ? '' : 'Right')
            At.prototype[e] = function () {
              return this.__filtered__ ? new At(this) : this[s](1)
            }
          }),
          (At.prototype.compact = function () {
            return this.filter(Ye)
          }),
          (At.prototype.find = function (e) {
            return this.filter(e).head()
          }),
          (At.prototype.findLast = function (e) {
            return this.reverse().find(e)
          }),
          (At.prototype.invokeMap = kt(function (e, n) {
            return typeof e == 'function'
              ? new At(this)
              : this.map(function (s) {
                  return io(s, e, n)
                })
          })),
          (At.prototype.reject = function (e) {
            return this.filter(As(ht(e)))
          }),
          (At.prototype.slice = function (e, n) {
            e = wt(e)
            var s = this
            return s.__filtered__ && (e > 0 || n < 0)
              ? new At(s)
              : (e < 0 ? (s = s.takeRight(-e)) : e && (s = s.drop(e)),
                n !== i &&
                  ((n = wt(n)), (s = n < 0 ? s.dropRight(-n) : s.take(n - e))),
                s)
          }),
          (At.prototype.takeRightWhile = function (e) {
            return this.reverse().takeWhile(e).reverse()
          }),
          (At.prototype.toArray = function () {
            return this.take(St)
          }),
          Ur(At.prototype, function (e, n) {
            var s = /^(?:filter|find|map|reject)|While$/.test(n),
              l = /^(?:head|last)$/.test(n),
              u = g[l ? 'take' + (n == 'last' ? 'Right' : '') : n],
              v = l || /^find/.test(n)
            u &&
              (g.prototype[n] = function () {
                var _ = this.__wrapped__,
                  x = l ? [1] : arguments,
                  E = _ instanceof At,
                  B = x[0],
                  N = E || _t(_),
                  V = function (zt) {
                    var Et = u.apply(g, yi([zt], x))
                    return l && Q ? Et[0] : Et
                  }
                N &&
                  s &&
                  typeof B == 'function' &&
                  B.length != 1 &&
                  (E = N = !1)
                var Q = this.__chain__,
                  ot = !!this.__actions__.length,
                  ut = v && !Q,
                  xt = E && !ot
                if (!v && N) {
                  _ = xt ? _ : new At(this)
                  var pt = e.apply(_, x)
                  return (
                    pt.__actions__.push({ func: $s, args: [V], thisArg: i }),
                    new br(pt, Q)
                  )
                }
                return ut && xt
                  ? e.apply(this, x)
                  : ((pt = this.thru(V)),
                    ut ? (l ? pt.value()[0] : pt.value()) : pt)
              })
          }),
          mr(
            ['pop', 'push', 'shift', 'sort', 'splice', 'unshift'],
            function (e) {
              var n = jo[e],
                s = /^(?:push|sort|unshift)$/.test(e) ? 'tap' : 'thru',
                l = /^(?:pop|shift)$/.test(e)
              g.prototype[e] = function () {
                var u = arguments
                if (l && !this.__chain__) {
                  var v = this.value()
                  return n.apply(_t(v) ? v : [], u)
                }
                return this[s](function (_) {
                  return n.apply(_t(_) ? _ : [], u)
                })
              }
            },
          ),
          Ur(At.prototype, function (e, n) {
            var s = g[n]
            if (s) {
              var l = s.name + ''
              Vt.call(wn, l) || (wn[l] = []), wn[l].push({ name: n, func: s })
            }
          }),
          (wn[vs(i, P).name] = [
            {
              name: 'wrapper',
              func: i,
            },
          ]),
          (At.prototype.clone = zm),
          (At.prototype.reverse = Am),
          (At.prototype.value = Em),
          (g.prototype.at = n0),
          (g.prototype.chain = o0),
          (g.prototype.commit = s0),
          (g.prototype.next = a0),
          (g.prototype.plant = c0),
          (g.prototype.reverse = d0),
          (g.prototype.toJSON = g.prototype.valueOf = g.prototype.value = h0),
          (g.prototype.first = g.prototype.head),
          jn && (g.prototype[jn] = l0),
          g
        )
      },
      bn = sm()
    Bi ? (((Bi.exports = bn)._ = bn), (Ea._ = bn)) : (Ce._ = bn)
  }).call(Ge)
})(Js, Js.exports)
var sn = Js.exports
const Wr = ce({
  Ellipsis: 'ellipsis',
  Short: 'short',
  None: 'none',
})
class ju extends de {
  constructor() {
    super()
    W(this, '_catalog')
    W(this, '_schema', '')
    W(this, '_model', '')
    W(this, '_widthCatalog', 0)
    W(this, '_widthSchema', 0)
    W(this, '_widthModel', 0)
    W(this, '_widthOriginal', 0)
    W(this, '_widthAdditional', 0)
    W(this, '_widthIconEllipsis', 26)
    W(this, '_widthIcon', 0)
    W(
      this,
      '_toggleNamePartsDebounced',
      sn.debounce(i => {
        const a =
          (this._hideCatalog
            ? 0
            : this._collapseCatalog
            ? this._widthIconEllipsis
            : this._widthCatalog) +
          this._widthSchema +
          this._widthModel +
          this._widthAdditional
        ;(this._hasCollapsedParts = i < this._widthOriginal),
          this._hasCollapsedParts
            ? (this.mode === Wr.None
                ? (this._hideCatalog = !0)
                : (this._collapseCatalog = !0),
              i < a
                ? this.mode === Wr.None
                  ? (this._hideSchema = !0)
                  : (this._collapseSchema = !0)
                : this.mode === Wr.None
                ? (this._hideSchema = !1)
                : (this._collapseSchema = this.collapseSchema))
            : this.mode === Wr.None
            ? ((this._hideCatalog = !1), (this._hideSchema = !1))
            : ((this._collapseCatalog = this.collapseCatalog),
              (this._collapseSchema = this.collapseSchema))
      }, 300),
    )
    ;(this._hasCollapsedParts = !1),
      (this._collapseCatalog = !1),
      (this._collapseSchema = !1),
      (this._hideCatalog = !1),
      (this._hideSchema = !1),
      (this.size = Lt.S),
      (this.hideCatalog = !1),
      (this.hideSchema = !1),
      (this.hideIcon = !1),
      (this.collapseCatalog = !1),
      (this.collapseSchema = !1),
      (this.hideTooltip = !1),
      (this.highlightModel = !1),
      (this.reduceColor = !1),
      (this.highlighted = !1),
      (this.shortCatalog = 'cat'),
      (this.shortSchema = 'sch'),
      (this.mode = Wr.None)
  }
  async firstUpdated() {
    await super.firstUpdated()
    const i = 28
    ;(this._widthIcon = It(this.hideIcon) ? 24 : 0),
      (this._widthAdditional = this._widthIcon + i)
    const o = this.shadowRoot.querySelector('[part="hidden"]')
    if (this.mode === Wr.None) return o.parentElement.removeChild(o)
    setTimeout(() => {
      const a = this.shadowRoot.querySelector('[part="hidden"]'),
        [d, h, m] = Array.from(a.children)
      ;(this._widthCatalog = d.clientWidth),
        (this._widthSchema = h.clientWidth),
        (this._widthModel = m.clientWidth),
        (this._widthOriginal =
          this._widthCatalog +
          this._widthSchema +
          this._widthModel +
          this._widthAdditional),
        setTimeout(() => {
          a.parentElement.removeChild(a)
        }),
        this.resize()
    })
  }
  willUpdate(i) {
    i.has('text')
      ? this._setNameParts()
      : i.has('collapse-catalog')
      ? (this._collapseCatalog = this.collapseCatalog)
      : i.has('collapse-schema')
      ? (this._collapseSchema = this.collapseSchema)
      : i.has('mode')
      ? this.mode === Wr.None &&
        ((this._hideCatalog = !0), (this._hideSchema = !0))
      : i.has('hide-catalog')
      ? (this._hideCatalog = this.mode === Wr.None ? !0 : this.hideCatalog)
      : i.has('hide-schema') &&
        (this._hideSchema = this.mode === Wr.None ? !0 : this.hideSchema)
  }
  async resize() {
    this.hideCatalog && this.hideSchema
      ? ((this._hideCatalog = !0),
        (this._hideSchema = !0),
        (this._collapseCatalog = !0),
        (this._collapseSchema = !0),
        (this._hasCollapsedParts = !0))
      : this._toggleNamePartsDebounced(this.parentElement.clientWidth)
  }
  _setNameParts() {
    this.text = decodeURI(this.text)
    const i = this.text.split('.')
    ;(this._model = i.pop()),
      (this._schema = i.pop()),
      (this._catalog = i.pop()),
      Qt(this._catalog) && (this.hideCatalog = !0),
      Fe(
        this._model,
        `Model Name ${this.text} does not satisfy the pattern: catalog.schema.model or schema.model`,
      ),
      Fe(
        this._schema,
        `Model Name ${this.text} does not satisfy the pattern: catalog.schema.model or schema.model`,
      )
  }
  _renderCatalog() {
    return lt(
      It(this._hideCatalog) && It(this.hideCatalog),
      C`
        <span part="catalog">
          ${
            this._collapseCatalog
              ? this._renderIconEllipsis(this.shortCatalog)
              : C`<span>${this._catalog}</span>`
          }
          .
        </span>
      `,
    )
  }
  _renderSchema() {
    return lt(
      It(this._hideSchema) && It(this.hideSchema),
      C`
        <span part="schema">
          ${
            this._collapseSchema
              ? this._renderIconEllipsis(this.shortSchema)
              : C`<span>${this._schema}</span>`
          }
          .
        </span>
      `,
    )
  }
  _renderModel() {
    return C`
      <span
        title="${this._model}"
        part="model"
      >
        <span>${this._model}</span>
      </span>
    `
  }
  _renderIconEllipsis(i = '') {
    return this.mode === Wr.Ellipsis
      ? C`
          <tbk-icon
            part="ellipsis"
            library="heroicons-micro"
            name="ellipsis-horizontal"
          ></tbk-icon>
        `
      : C`<small part="ellipsis">${i}</small>`
  }
  _renderIconModel() {
    if (this.hideIcon) return ''
    const i = C`
      <tbk-icon
        part="icon"
        library="heroicons"
        name="cube"
      ></tbk-icon>
    `
    return this.hideTooltip
      ? C`<span title="${this.text}">${i}</span>`
      : this._hasCollapsedParts
      ? C`
          <tbk-tooltip
            content="${this.text}"
            placement="right"
            distance="0"
          >
            ${i}
          </tbk-tooltip>
        `
      : i
  }
  render() {
    return C`
      <slot name="before"></slot>
      ${this._renderIconModel()}
      <div part="text">
        <span part="hidden">
          <span>${this._catalog}</span>
          <span>${this._schema}</span>
          <span>${this._model}</span>
        </span>
        ${this._renderCatalog()} ${this._renderSchema()} ${this._renderModel()}
      </div>
      <slot name="after"></slot>
    `
  }
  static categorize(i = []) {
    return i.reduce((o, a) => {
      Fe(gc(a.name), 'Model name must be present')
      const d = a.name.split('.')
      d.pop(),
        Fe(
          ia(d),
          `Model Name ${a.name} does not satisfy the pattern: catalog.schema.model or schema.model`,
        )
      const h = decodeURI(d.join('.'))
      return Qt(o[h]) && (o[h] = []), o[h].push(a), o
    }, {})
  }
}
W(ju, 'styles', [Bt(), ge(), ft(m$)]),
  W(ju, 'properties', {
    size: { type: String, reflect: !0 },
    text: { type: String },
    hideCatalog: { type: Boolean, reflect: !0, attribute: 'hide-catalog' },
    hideSchema: { type: Boolean, reflect: !0, attribute: 'hide-schema' },
    hideIcon: { type: Boolean, reflect: !0, attribute: 'hide-icon' },
    hideTooltip: { type: Boolean, reflect: !0, attribute: 'hide-tooltip' },
    collapseCatalog: {
      type: Boolean,
      reflect: !0,
      attribute: 'collapse-catalog',
    },
    collapseSchema: {
      type: Boolean,
      reflect: !0,
      attribute: 'collapse-schema',
    },
    highlighted: { type: Boolean, reflect: !0 },
    highlightedModel: {
      type: Boolean,
      reflect: !0,
      attribute: 'highlighted-model',
    },
    reduceColor: { type: Boolean, reflect: !0, attribute: 'reduce-color' },
    mode: { type: String, reflect: !0 },
    shortCatalog: { type: String, attribute: 'short-catalog' },
    shortSchema: { type: String, attribute: 'short-schema' },
    _hasCollapsedParts: { type: Boolean, state: !0 },
    _collapseCatalog: { type: Boolean, state: !0 },
    _collapseSchema: { type: Boolean, state: !0 },
    _hideCatalog: { type: Boolean, state: !0 },
    _hideSchema: { type: Boolean, state: !0 },
  })
var v$ = st`
  :host {
    display: contents;
  }
`,
  en = class extends nt {
    constructor() {
      super(...arguments), (this.observedElements = []), (this.disabled = !1)
    }
    connectedCallback() {
      super.connectedCallback(),
        (this.resizeObserver = new ResizeObserver(t => {
          this.emit('sl-resize', { detail: { entries: t } })
        })),
        this.disabled || this.startObserver()
    }
    disconnectedCallback() {
      super.disconnectedCallback(), this.stopObserver()
    }
    handleSlotChange() {
      this.disabled || this.startObserver()
    }
    startObserver() {
      const t = this.shadowRoot.querySelector('slot')
      if (t !== null) {
        const r = t.assignedElements({ flatten: !0 })
        this.observedElements.forEach(i => this.resizeObserver.unobserve(i)),
          (this.observedElements = []),
          r.forEach(i => {
            this.resizeObserver.observe(i), this.observedElements.push(i)
          })
      }
    }
    stopObserver() {
      this.resizeObserver.disconnect()
    }
    handleDisabledChange() {
      this.disabled ? this.stopObserver() : this.startObserver()
    }
    render() {
      return C` <slot @slotchange=${this.handleSlotChange}></slot> `
    }
  }
en.styles = [dt, v$]
c([p({ type: Boolean, reflect: !0 })], en.prototype, 'disabled', 2)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  en.prototype,
  'handleDisabledChange',
  1,
)
var Fs
let Oz =
  ((Fs = class extends dr(en) {
    constructor() {
      super(...arguments)
      W(this, '_items', [])
      W(
        this,
        '_handleResize',
        sn.debounce(i => {
          if (
            (i.stopPropagation(), Qt(this.updateSelector) || Qt(i.detail.value))
          )
            return
          const o = i.detail.value.entries[0]
          ;(this._items = Array.from(
            this.querySelectorAll(this.updateSelector),
          )),
            this._items.forEach(a => {
              var d
              return (d = a.resize) == null ? void 0 : d.call(a, o)
            }),
            this.emit('resize', {
              detail: new this.emit.EventDetail(void 0, i),
            })
        }, 300),
      )
    }
    firstUpdated() {
      super.firstUpdated(),
        this.addEventListener('sl-resize', this._handleResize.bind(this))
    }
  }),
  W(Fs, 'styles', [Bt()]),
  W(Fs, 'properties', {
    ...en.properties,
    updateSelector: { type: String, reflect: !0, attribute: 'update-selector' },
  }),
  Fs)
const b$ = `:host {
  display: inline-flex;
  flex-direction: column;
  gap: var(--step-2);
  width: 100%;
  height: 100%;
  overflow: hidden;
  font-family: var(--font-accent);
}
[part='content'] {
  width: 100%;
  display: flex;
  flex-direction: column;
  gap: var(--step-2);
}
`,
  y$ = `:host {
  min-height: inherit;
  height: inherit;
  max-height: inherit;
  min-width: inherit;
  width: inherit;
  max-width: inherit;
  display: block;
  overflow: auto;
}
`
class _$ extends de {
  render() {
    return C`<slot></slot>`
  }
}
W(_$, 'styles', [Bt(), mk(), ft(y$)])
const w$ = `:host {
  --source-list-item-gap: var(--step-2);
  --source-list-item-border-radius: var(--source-list-item-radius);
  --source-list-item-background: transparent;
  --source-list-item-background-active: var(--source-list-item-variant-5, var(--color-pacific-10));
  --source-list-item-background-hover: var(--source-list-item-variant-5, var(--color-pacific-5));
  --source-list-item-color: var(--color-gray-700);
  --source-list-item-color-hover: var(--source-list-item-variant, var(--color-pacific-600));
  --source-list-item-color-active: var(--source-list-item-variant, var(--color-pacific-600));
  --source-list-item-color-toggle: var(--color-gray-200);
  --source-list-item-background-toggle: var(--color-gray-200);

  display: inline-flex;
  flex-direction: column;
  border-radius: var(--source-list-item-border-radius);
  overflow: hidden;
  cursor: pointer;
  width: 100%;
  color: var(--source-list-item-color);
  font-weight: var(--text-medium);
}
:host-context([mode='dark']) {
  --source-list-item-color: var(--color-gray-200);
}
::slotted(a:focus-visible:not([disabled])) {
  display: inline-flex;
  outline: var(--half) solid var(--color-outline);
  outline-offset: var(--half);
  z-index: 1;
  border-radius: var(--radius-2xs);
}
::slotted(*) {
  color: var(--source-list-item-color);
  text-decoration: none;
}
:host(:focus-visible:not([disabled])) {
  outline: var(--half) solid var(--color-outline);
  outline-offset: var(--half);
  z-index: 1;
}
:host([compact]) {
  --source-list-item-gap: var(--half);
  --source-list-item-padding-y: var(--half);
  --source-list-item-padding-x: var(--step-2);
}
:host([compact]) [part='label'] tbk-icon {
  margin-left: var(--step);
}
:host([active]) {
  --source-list-item-color: var(--source-list-item-color-active);
  --source-list-background: var(--source-list-item-background-active);
}
:host(:hover) {
  --source-list-item-color: var(--source-list-item-color-hover);
  --source-list-item-background: var(--source-list-item-background-hover);
}
:host([open]),
:host([open]):hover {
  --source-list-item-color: var(--source-list-item-color-active);
}
:host([active]),
:host([active]:hover) {
  --source-list-item-background: var(--source-list-item-background-active);
  --source-list-item-color: var(--source-list-item-color-active);
}
[part='base'] {
  width: 100%;
  display: flex;
  flex-direction: column;
  border-radius: var(--source-list-item-border-radius);
  gap: var(--source-list-item-gap);
  font-size: var(--source-list-item-font-size);
  line-height: 1;
  padding: var(--source-list-item-padding-y) calc(var(--source-list-item-padding-x) * 0.7);
  position: relative;
  background: var(--source-list-item-background);
  text-decoration: none;
}
[part='header'] {
  width: 100%;
  display: flex;
  align-items: center;
  gap: var(--step-2);
}
[part='label'] {
  min-width: var(--step-4);
  width: 100%;
  display: flex;
  gap: var(--step-3);
  font-weight: inherit;
  align-items: center;
}
[part='text'] {
  width: 100%;
  display: flex;
  flex-direction: column;
  white-space: nowrap;
  padding: var(--step) 0;
  overflow: hidden;
  text-overflow: ellipsis;
}
:host([short]) [part='text'] {
  display: block;
  font-size: 0.7em;
  text-align: center;
  margin-bottom: var(--step-2);
  padding: 0;
}
[part='items'] {
  display: none;
  flex-direction: column;
  gap: var(--step);
}
[part='badge'] {
  display: flex;
  align-items: center;
  gap: var(--step);
  flex-shrink: 0;
}
[part='toggle'] {
  display: flex;
  align-items: center;
  gap: var(--half);
  cursor: pointer;
  padding-right: var(--step);
}
[part='toggle']:hover {
  background: var(--color-gray-5);
  border-radius: var(--radius-s);
}
:host([short]) [name='icon-active'],
:host([short]) [part='badge'],
:host([short]) [part='toggle'] {
  display: none;
}
:host([open]:not([short])) [part='items'] {
  display: flex;
  padding: var(--step) var(--step-4);
}
[part='content'] {
  display: flex;
  flex-direction: column;
  gap: var(--step);
}
tbk-icon {
  flex-shrink: 0;
  padding: var(--step) 0;
  font-size: var(--source-list-item-font-size);
}
:host([short]:not([open])) [part='base'] {
  display: inline-flex;
  min-width: var(--source-list-item-font-size);
  min-height: var(--source-list-item-font-size);
  justify-content: center;
  align-items: center;
  border-radius: var(--source-list-item-border-radius);
}
:host([short]:not([open])) [part='itmes'],
:host([short]:not([open])) [part='header'] > *:not([part='label']) {
  display: none;
}
:host([short]:not([open])) tbk-icon {
  margin: 0;
}
[part='icon-active'] {
  color: var(--source-list-item-color);
  font-size: calc(var(--font-size) * 0.9);
  display: inline-block;
  margin-left: var(--step);
  opacity: 0;
}
:host([active]) [part='icon-active'] {
  opacity: 1;
}
::slotted([slot='items']) {
  --source-list-item-font-size: calc(var(--font-size) * 0.9);
}
`,
  Vs = ce({
    Select: 'select-source-list-item',
    Open: 'open-source-list-item',
  })
class Zu extends de {
  constructor() {
    super()
    W(this, '_items', [])
    ;(this.size = Lt.S),
      (this.shape = Be.Round),
      (this.icon = 'rocket-launch'),
      (this.short = !1),
      (this.open = !1),
      (this.active = !1),
      (this.hideIcon = !1),
      (this.hasActiveIcon = !1),
      (this.hideItemsCounter = !1),
      (this.hideActiveIcon = !1),
      (this.compact = !1),
      (this.tabindex = 0),
      (this._hasItems = !1)
  }
  connectedCallback() {
    super.connectedCallback(),
      (this.role = 'listitem'),
      this.addEventListener('mousedown', this._handleMouseDown.bind(this)),
      this.addEventListener('keydown', this._handleKeyDown.bind(this)),
      this.addEventListener(Vs.Select, this._handleSelect.bind(this))
  }
  toggle(i) {
    this.open = Qt(i) ? It(this.open) : i
  }
  setActive(i) {
    this.active = Qt(i) ? It(this.active) : i
  }
  _handleMouseDown(i) {
    i.preventDefault(),
      i.stopPropagation(),
      this._hasItems
        ? (this.toggle(),
          this.emit(Vs.Open, {
            detail: new this.emit.EventDetail(this.value, i, {
              id: this.id,
              open: this.open,
              active: this.active,
              name: this.name,
              value: this.value,
            }),
          }))
        : this.selectable &&
          this.emit(Vs.Select, {
            detail: new this.emit.EventDetail(this.value, i, {
              id: this.id,
              open: this.open,
              active: this.active,
              name: this.name,
              value: this.value,
            }),
          })
  }
  _handleKeyDown(i) {
    ;(i.key === 'Enter' || i.key === ' ') && this._handleMouseDown(i)
  }
  _handleSelect(i) {
    i.target !== this &&
      requestAnimationFrame(() => {
        this.active = this._items.some(o => o.active) || this.active
      })
  }
  _handleSlotChange(i) {
    i.stopPropagation(),
      (this._items = []
        .concat(
          Array.from(
            this.renderRoot.querySelectorAll('slot[name="items"]'),
          ).map(o => o.assignedElements({ flatten: !0 })),
        )
        .flat()),
      (this._hasItems = ia(this._items))
  }
  render() {
    return C`
      <div part="base">
        <span part="header">
          ${lt(
            this.hasActiveIcon && It(this.hideActiveIcon),
            C`
              <slot name="icon-active">
                <tbk-icon
                  part="icon-active"
                  library="heroicons-micro"
                  name="check-circle"
                ></tbk-icon>
              </slot>
            `,
          )}
          <span part="label">
            ${lt(
              It(this.hideIcon),
              C`
                <slot name="icon">
                  <tbk-icon
                    library="heroicons"
                    name="${this.icon}"
                  ></tbk-icon>
                </slot>
              `,
            )}
            ${lt(
              It(this.short),
              C`
                <span part="text">
                  <slot></slot>
                </span>
              `,
            )}
          </span>
          <span part="badge">
            <slot name="badge"></slot>
          </span>
          ${lt(
            this._hasItems,
            C`
              <span part="toggle">
                ${lt(
                  It(this.hideItemsCounter),
                  C`<tbk-badge .size="${Lt.XS}">${this._items.length}</tbk-badge> `,
                )}
                <tbk-icon
                  library="heroicons-micro"
                  name="chevron-${this.open ? 'up' : 'down'}"
                ></tbk-icon>
              </span>
            `,
          )}
        </span>
        <slot name="extra"></slot>
      </div>
      <div part="items">
        <slot
          name="items"
          @slotchange="${sn.debounce(this._handleSlotChange, 200)}"
        ></slot>
      </div>
    `
  }
}
W(Zu, 'styles', [
  Bt(),
  fi('source-list-item'),
  Xr('source-list-item'),
  ge('source-list-item'),
  je('source-list-item'),
  ft(w$),
]),
  W(Zu, 'properties', {
    size: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    open: { type: Boolean, reflect: !0 },
    active: { type: Boolean, reflect: !0 },
    short: { type: Boolean, reflect: !0 },
    compact: { type: Boolean, reflect: !0 },
    selectable: { type: Boolean, reflect: !0 },
    name: { type: String },
    value: { type: String },
    icon: { type: String },
    hideIcon: { type: Boolean, reflect: !0, attribute: 'hide-icon' },
    hasActiveIcon: { type: Boolean, reflect: !0, attribute: 'has-active-icon' },
    hideItemsCounter: {
      type: Boolean,
      reflect: !0,
      attribute: 'hide-items-counter',
    },
    hideActiveIcon: {
      type: Boolean,
      reflect: !0,
      attribute: 'hide-items-counter',
    },
    _hasItems: { type: Boolean, state: !0 },
  })
class Ju extends de {
  constructor() {
    super()
    W(this, '_sections', [])
    W(this, '_items', [])
    ;(this.short = !1),
      (this.selectable = !1),
      (this.allowUnselect = !1),
      (this.hasActiveIcon = !1)
  }
  connectedCallback() {
    super.connectedCallback(),
      (this.role = 'list'),
      this.addEventListener(Vs.Select, i => {
        i.stopPropagation(),
          this._items.forEach(o => {
            o !== i.target
              ? o.setActive(!1)
              : this.allowUnselect
              ? o.setActive()
              : o.setActive(!0)
          }),
          this.emit('change', { detail: i.detail })
      })
  }
  willUpdate(i) {
    super.willUpdate(i),
      (i.has('short') || i.has('size')) && this._toggleChildren()
  }
  toggle(i) {
    this.short = Qt(i) ? It(this.short) : i
  }
  _toggleChildren() {
    this._sections.forEach(i => {
      i.short = this.short
    }),
      this._items.forEach(i => {
        ;(i.short = this.short),
          (i.size = this.size ?? i.size),
          this.selectable &&
            ((i.hasActiveIcon = this.hasActiveIcon ? It(i.hideActiveIcon) : !1),
            (i.selectable = Qt(i.selectable) ? this.selectable : i.selectable))
      })
  }
  _handleSlotChange(i) {
    i.stopPropagation(),
      (this._sections = Array.from(this.querySelectorAll('[role="group"]'))),
      (this._items = Array.from(this.querySelectorAll('[role="listitem"]'))),
      this._toggleChildren(),
      ia(this._sections) && (this._sections[0].open = !0)
  }
  render() {
    return C`
      <tbk-scroll part="content">
        <slot @slotchange="${sn.debounce(
          this._handleSlotChange.bind(this),
          200,
        )}"></slot>
      </tbk-scroll>
    `
  }
}
W(Ju, 'styles', [Bt(), on(), ge('source-list'), je('source-list'), ft(b$)]),
  W(Ju, 'properties', {
    short: { type: Boolean, reflect: !0 },
    size: { type: String, reflect: !0 },
    selectable: { type: Boolean, reflect: !0 },
    allowUnselect: { type: Boolean, reflect: !0, attribute: 'allow-unselect' },
    hasActiveIcon: { type: Boolean, reflect: !0, attribute: 'has-active-icon' },
  })
const x$ = `:host {
  --source-list-section-color: var(--color-gray-500);
  --source-list-section-background-icon: var(--color-gray-5);

  display: block;
}
:host-context([mode='dark']) {
  --source-list-section-color: var(--color-gray-200);
  --source-list-section-color-icon: var(--color-gray-200);
  --source-list-section-background-icon: var(--color-gray-700);
}
[part='base'] {
  width: 100%;
  margin-top: var(--step);
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--step);
}
[part='headline'] {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--step);
}
[part='headline'] > small {
  font-size: var(--text-xs);
  color: var(--source-list-section-color);
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
}
[part='icon'] {
  cursor: pointer;
  font-size: var(--text-xs);
  display: inline-flex;
  align-items: center;
  border-radius: var(--radius-xs);
  padding-right: var(--half);
}
[part='icon'] > tbk-icon {
  opacity: 0;
  position: absolute;
}
[part='base']:hover [part='icon'] {
  color: var(--source-list-section-color-icon);
  background: var(--source-list-section-background-icon);
}
[part='base']:hover [part='icon'] > tbk-icon {
  opacity: 1;
  position: initial;
}
[part='items'] {
  width: 100%;
  display: none;
  flex-direction: column;
  gap: var(--step);
}
:host([open]) [part='items'] {
  display: flex;
}
[part='actions'] {
  display: flex;
  gap: var(--step-2);
  justify-content: center;
  padding: var(--step-2) var(--step);
}
`,
  k$ = `:host {
  --button-height: auto;
  --button-width: 100%;
  --button-font-size: var(--font-size);
  --button-text-align: var(--text-align);
  --button-box-shadow: none;
  --button-opacity: 1;

  display: inline-flex;
  border-radius: var(--button-radius);
  opacity: var(--button-opacity);
}
:host([disabled]),
:host([disabled]:hover) {
  --button-background: var(--color-gray-125) !important;
  --button-color: var(--color-gray-600) !important;
  --button-box-shadow: none !important;
  --button-opacity: 1 !important;
}
:host([disabled]) {
  ::slotted(*) {
    color: inherit;
    text-decoration: none !important;
  }
}
:host([variant='primary']) {
  --button-background: var(--color-accent);
  --button-color: var(--color-light);
}
:host([link][variant='primary']) {
  --button-background: transparent;
  --button-color: var(--color-accent);
}
:host([link][variant='primary']:hover) {
  --button-background: var(--color-gray-100);
}
:host([link][variant='primary']:active),
:host([link][variant='primary'].active) {
  --button-background: var(--color-gray-125);
}
:host([variant='primary']:hover) {
  --button-background: var(--color-accent);
  --button-opacity: 0.85;
}
:host([variant='primary']:active),
:host([variant='primary'].active) {
  --button-background: var(--color-accent);
  --button-opacity: 0.95;
}
:host([variant='secondary']) {
  --button-background: var(--color-gray-150);
  --button-color: var(--color-gray-700);
}
:host([variant='secondary']:hover) {
  --button-background: var(--color-gray-125);
}
:host([variant='secondary']:active),
:host([variant='secondary'][active]) {
  --button-background: var(--color-gray-200);
}
:host([variant='alternative']) {
  --button-color: var(--color-gray-700);
  --button-box-shadow: inset 0 0 0 var(--one) var(--color-gray-200);
}
:host([variant='alternative']:hover) {
  --button-background: var(--color-gray-125);
}
:host([variant='alternative']:active) {
  --button-background: var(--color-gray-200);
}
:host([variant='destructive']) {
  --button-background: var(--color-gray-150);
  --button-color: var(--color-scarlet-600);
}
:host([variant='destructive']:hover) {
  --button-background: var(--color-gray-125);
}
:host([variant='destructive']:active) {
  --button-background: var(--color-gray-200);
}
:host([variant='danger']) {
  --button-background: var(--color-scarlet);
  --button-color: var(--color-scarlet-100);
}
:host([variant='danger']:hover) {
  --button-background: var(--color-scarlet-550);
}
:host([variant='danger']:active) {
  --button-background: var(--color-scarlet-600);
}
:host([variant='transparent']) {
  --button-background: transparent;
  --button-color: var(--color-gray-700);
}
:host([variant='transparent']:hover) {
  --button-background: var(--color-gray-125);
}
:host([overlay]) [part='base'] {
  display: none;
}
[part='overlay'],
[part='base'] {
  display: flex;
  align-items: center;
  justify-items: center;
  appearance: none;
  -webkit-appearance: none;
  text-decoration: none;
  border: 0;
  white-space: nowrap;
  cursor: pointer;
  position: relative;
  font-weight: var(--text-bold);
  text-align: var(--button-text-align, inherit);
  width: var(--button-width);
  height: var(--button-height);
  font-size: var(--button-font-size);
  border-radius: var(--button-radius);
  background: var(--button-background);
  color: var(--button-color);
  padding: var(--button-padding-y) var(--button-padding-x);
  box-shadow: var(--button-box-shadow);
  line-height: 1;
}
[part='content'] {
  text-align: var(--button-text-align, inherit);
  width: inherit;
  height: inherit;
  overflow: hidden;
  text-overflow: ellipsis;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
}
:host([icon]) [part='content'] {
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  position: relative;
  min-height: var(--button-font-size);
  min-width: var(--button-font-size);
}
[part='base']::-moz-focus-inner {
  border: 0;
  padding: 0;
}
:host([icon]) {
  ::slotted(*) {
    display: flex;
    font-size: calc(var(--button-font-size) * 2);
    position: absolute;
  }
}
:host([link]),
:host([link]:hover) {
  ::slotted(*) {
    margin-right: var(--step);
    color: inherit;
    text-decoration: none !important;
    box-shadow: none !important;
  }
}
:host([link]) [name='after'] {
  color: inherit;
}
slot[name='tagline'] {
  display: block;
  width: 100%;
  font-size: var(--text-xs);
  opacity: 0.85;
}
::slotted([slot='tagline']) {
  margin-top: var(--step);
}
::slotted([slot='before']),
::slotted([slot='after']) {
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 0;
  font-size: calc(var(--button-font-size));
  line-height: 1;
}
::slotted([slot='before']) {
  margin-right: var(--step-2);
}
::slotted([slot='after']) {
  margin-left: var(--step-2);
}
`,
  Vl = ce({
    Button: 'button',
    Submit: 'submit',
    Reset: 'reset',
  }),
  $$ = ce({
    Primary: 'primary',
    Secondary: 'secondary',
    Alternative: 'alternative',
    Destructive: 'destructive',
    Danger: 'danger',
    Transparent: 'transparent',
  })
class Wl extends de {
  constructor() {
    super(),
      (this.type = Vl.Button),
      (this.size = Lt.M),
      (this.side = Jt.Left),
      (this.variant = $t.Primary),
      (this.shape = Be.Round),
      (this.horizontal = zu.Auto),
      (this.vertical = $w.Auto),
      (this.disabled = !1),
      (this.readonly = !1),
      (this.overlay = !1),
      (this.link = !1),
      (this.icon = !1),
      (this.autofocus = !1),
      (this.popovertarget = ''),
      (this.internals = this.attachInternals()),
      (this.tabindex = 0)
  }
  get elContent() {
    var r
    return (r = this.renderRoot) == null
      ? void 0
      : r.querySelector(ji.PartContent)
  }
  get elTagline() {
    var r
    return (r = this.renderRoot) == null
      ? void 0
      : r.querySelector(ji.PartTagline)
  }
  get elBefore() {
    var r
    return (r = this.renderRoot) == null
      ? void 0
      : r.querySelector(ji.PartBefore)
  }
  get elAfter() {
    var r
    return (r = this.renderRoot) == null
      ? void 0
      : r.querySelector(ji.PartAfter)
  }
  connectedCallback() {
    super.connectedCallback(),
      this.addEventListener(An.Click, this._onClick.bind(this)),
      this.addEventListener(An.Keydown, this._onKeyDown.bind(this)),
      this.addEventListener(An.Keyup, this._onKeyUp.bind(this))
  }
  disconnectedCallback() {
    super.disconnectedCallback(),
      this.removeEventListener(An.Click, this._onClick),
      this.removeEventListener(An.Keydown, this._onKeyDown),
      this.removeEventListener(An.Keyup, this._onKeyUp)
  }
  firstUpdated() {
    super.firstUpdated(), this.autofocus && this.setFocus()
  }
  willUpdate(r) {
    return (
      r.has('link') &&
        (this.horizontal = this.link ? zu.Compact : this.horizontal),
      super.willUpdate(r)
    )
  }
  click() {
    const r = this.getForm()
    Kp(r) &&
      [Vl.Submit, Vl.Reset].includes(this.type) &&
      r.reportValidity() &&
      this.handleFormSubmit(r)
  }
  getForm() {
    return this.internals.form
  }
  _onClick(r) {
    var i
    if (this.readonly) {
      r.preventDefault(), r.stopPropagation(), r.stopImmediatePropagation()
      return
    }
    if (this.link)
      return (
        r.stopPropagation(),
        r.stopImmediatePropagation(),
        (i = this.querySelector('a')) == null ? void 0 : i.click()
      )
    if ((r.preventDefault(), this.disabled)) {
      r.stopPropagation(), r.stopImmediatePropagation()
      return
    }
    this.click()
  }
  _onKeyDown(r) {
    ;[Ls.Enter, Ls.Space].includes(r.code) &&
      (r.preventDefault(), r.stopPropagation(), this.classList.add(Su.Active))
  }
  _onKeyUp(r) {
    var i
    ;[Ls.Enter, Ls.Space].includes(r.code) &&
      (r.preventDefault(),
      r.stopPropagation(),
      this.classList.remove(Su.Active),
      (i = this.elBase) == null || i.click())
  }
  handleFormSubmit(r) {
    if (Qt(r)) return
    const i = document.createElement('input')
    ;(i.type = this.type),
      (i.style.position = 'absolute'),
      (i.style.width = '0'),
      (i.style.height = '0'),
      (i.style.clipPath = 'inset(50%)'),
      (i.style.overflow = 'hidden'),
      (i.style.whiteSpace = 'nowrap'),
      [
        'name',
        'value',
        'formaction',
        'formenctype',
        'formmethod',
        'formnovalidate',
        'formtarget',
      ].forEach(o => {
        Kr(this[o]) && i.setAttribute(o, this[o])
      }),
      r.append(i),
      i.click(),
      i.remove()
  }
  setOverlayText(r = '') {
    this._overlayText = r
  }
  showOverlay(r = 0) {
    setTimeout(() => {
      this.overlay = !0
    }, r)
  }
  hideOverlay(r = 0) {
    setTimeout(() => {
      ;(this.overlay = !1), (this._overlayText = '')
    }, r)
  }
  setFocus(r = 200) {
    setTimeout(() => {
      this.focus()
    }, r)
  }
  setBlur(r = 200) {
    setTimeout(() => {
      this.blur()
    }, r)
  }
  render() {
    return C`
      ${lt(
        this.overlay && this._overlayText,
        C`<span part="overlay">${this._overlayText}</span>`,
      )}
      <div part="base">
        <slot name="before"></slot>
        <div part="content">
          <slot tabindex="-1"></slot>
          <slot name="tagline">${this.tagline}</slot>
        </div>
        <slot name="after">
          ${lt(
            this.link,
            C`<tbk-icon
              library="heroicons"
              name="arrow-up-right"
            ></tbk-icon>`,
          )}
        </slot>
      </div>
    `
  }
}
W(Wl, 'formAssociated', !0),
  W(Wl, 'styles', [
    Bt(),
    on(),
    ge(),
    aa('button'),
    yk('button'),
    fi('button'),
    $k('button'),
    je('button', 1.25, 2),
    xc(),
    ft(k$),
  ]),
  W(Wl, 'properties', {
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
    side: { type: String, reflect: !0 },
    horizontal: { type: String, reflect: !0 },
    vertical: { type: String, reflect: !0 },
    disabled: { type: Boolean, reflect: !0 },
    link: { type: Boolean, reflect: !0 },
    icon: { type: Boolean, reflect: !0 },
    readonly: { type: Boolean, reflect: !0 },
    name: { type: String },
    value: { type: String },
    type: { type: String },
    form: { type: String },
    formaction: { type: String },
    formenctype: { type: String },
    formmethod: { type: String },
    formnovalidate: { type: Boolean },
    formtarget: { type: String },
    autofocus: { type: Boolean },
    popovertarget: { type: String },
    popovertargetaction: { type: String },
    tagline: { type: String },
    overlay: { type: Boolean, reflect: !0 },
    _overlayText: { type: String, state: !0 },
  })
class Qu extends de {
  constructor() {
    super()
    W(this, '_open', !1)
    W(this, '_cache', /* @__PURE__ */ new WeakMap())
    ;(this._childrenCount = 0),
      (this._showMore = !1),
      (this.open = !1),
      (this.inert = !1),
      (this.short = !1),
      (this.limit = 1 / 0)
  }
  connectedCallback() {
    super.connectedCallback(), (this.role = 'group')
  }
  willUpdate(i) {
    super.willUpdate(i),
      i.has('short') &&
        (this.short
          ? ((this._open = this.open), (this.open = !0))
          : (this.open = this._open))
  }
  toggle(i) {
    this.open = Qt(i) ? It(this.open) : i
  }
  _handleClick(i) {
    i.preventDefault(), i.stopPropagation(), this.toggle()
  }
  _toggleChildren() {
    this.elsSlotted.forEach((i, o) => {
      It(this._cache.has(i)) && this._cache.set(i, i.style.display),
        this._showMore || o < this.limit
          ? (i.style.display = this._cache.get(i, i.style.display))
          : (i.style.display = 'none')
    })
  }
  _renderShowMore() {
    return this.short
      ? C`
          <div part="actions">
            <tbk-icon
              library="heroicons-micro"
              name="ellipsis-horizontal"
            ></tbk-icon>
          </div>
        `
      : C`
          <div part="actions">
            <tbk-button
              shape="pill"
              size="2xs"
              variant="secondary"
              @click="${() => {
                ;(this._showMore = It(this._showMore)), this._toggleChildren()
              }}"
            >
              ${
                this._showMore
                  ? 'Show Less'
                  : `Show ${this._childrenCount - this.limit} More`
              }
            </tbk-button>
          </div>
        `
  }
  _handleSlotChange(i) {
    i.stopPropagation(),
      (this._childrenCount = this.elsSlotted.length),
      this._toggleChildren()
  }
  render() {
    return C`
      <div part="base">
        ${lt(
          this.headline && It(this.short),
          C`
            <span part="headline">
              <small>${this.headline}</small>
              <span
                part="icon"
                @click=${this._handleClick.bind(this)}
              >
                <tbk-badge size="${Lt.XXS}">${this._childrenCount}</tbk-badge>
                <tbk-icon
                  library="heroicons-micro"
                  name="chevron-${this.open ? 'down' : 'right'}"
                ></tbk-icon>
              </span>
            </span>
          `,
        )}
        <div part="items">
          <slot @slotchange="${sn.debounce(
            this._handleSlotChange,
            200,
          )}"></slot>
          ${lt(this._childrenCount > this.limit, this._renderShowMore())}
        </div>
      </div>
    `
  }
}
W(Qu, 'styles', [
  Bt(),
  ge('source-list-section'),
  je('source-list-section'),
  ft(x$),
]),
  W(Qu, 'properties', {
    headline: { type: String },
    open: { type: Boolean, reflect: !0 },
    inert: { type: Boolean, reflect: !0 },
    short: { type: Boolean, reflect: !0 },
    limit: { type: Number },
    _showMore: { type: String, state: !0 },
    _childrenCount: { type: Number, state: !0 },
  })
const C$ = `:host {
  --metadata-font-size: var(--font-size);

  display: flex;
  height: 100%;
  width: 100%;
  overflow: hidden;
}
[part='base'] {
  display: flex;
  flex-direction: column;
  gap: var(--step-2);
  font-size: var(--metadata-font-size);
}
`
class tp extends de {
  constructor() {
    super(), (this.size = Lt.S)
  }
  render() {
    return C`
      <tbk-scroll>
        <div part="base">
          <slot></slot>
        </div>
      </tbk-scroll>
    `
  }
}
W(tp, 'styles', [Bt(), ge(), ft(C$)]),
  W(tp, 'properties', {
    size: { type: String, reflect: !0 },
  })
const S$ = `:host {
  --metadata-item-background: var(--color-gray-3);
  --metadata-item-color-key: var(--color-gray-500);
  --metadata-item-color-description: var(--color-gray-500);
  --metadata-item-color-value: var(--color-gray-700);

  display: flex;
  flex-direction: column;
  gap: var(--step);
  width: 100%;
  padding: var(--step) var(--step-2);
  white-space: nowrap;
  overflow: hidden;
}
:host-context([mode='dark']) {
  --metadata-item-background: var(--color-gray-10);
  --metadata-item-color-key: var(--color-gray-300);
  --metadata-item-color-description: var(--color-gray-300);
  --metadata-item-color-value: var(--color-gray-200);
}
:host(:hover) {
  background-color: var(--metadata-item-background);
}

[part='base'] {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: inherit;
  overflow: hidden;
  gap: var(--step-4);
}
slot[name='key'],
::slotted([slot='key']) {
  display: inline-flex;
  color: var(--metadata-item-color-key);
  font-family: var(--font-mono);
  font-size: inherit;
  overflow: hidden;
  flex-shrink: 0;
}
slot[name='value'],
[part='value'],
::slotted([slot='value']) {
  display: inline-flex;
  color: var(--metadata-item-color-value);
  font-family: var(--font-sans);
  font-weight: var(--text-bold);
  font-size: inherit;
  overflow: hidden;
}
[part='value'] {
  color: var(--color-link);
  text-decoration: none;
}
[part='value']:hover {
  text-decoration: underline;
}
[part='description'] {
  width: 100%;
  display: block;
  color: var(--metadata-item-color-description);
  font-family: var(--font-sans);
  font-size: inherit;
}
::slotted(*) {
  width: 100%;
  padding-top: 0;
  padding-bottom: 0;
}
`
class ep extends de {
  constructor() {
    super(), (this.size = Lt.M)
  }
  _renderValue() {
    return this.href
      ? C`<a
          href="${this.href}"
          part="value"
          >${this.value}</a
        >`
      : C`<slot name="value">${this.value}</slot>`
  }
  render() {
    return C`
      ${lt(
        this.label || this.value,
        C`
          <div part="base">
            ${lt(this.label, C`<slot name="key">${this.label}</slot>`)}
            ${this._renderValue()}
          </div>
        `,
      )}
      ${lt(
        this.description,
        C`<span part="description">${this.description}</span>`,
      )}
      <slot></slot>
    `
  }
}
W(ep, 'styles', [Bt(), ge(), ft(S$)]),
  W(ep, 'properties', {
    size: { type: String, reflect: !0 },
    label: { type: String },
    value: { type: String },
    href: { type: String },
    description: { type: String },
  })
const z$ = `:host {
  font-family: var(--font-sans);

  display: flex;
  width: 100%;
  flex-direction: column;
}
[part='base'] {
  width: 100%;
  display: flex;
  align-items: flex-start;
  gap: var(--step-2);
  padding: var(--step-2);
  border-radius: var(--radius-s);
}
:host([orientation='horizontal']) [part='base'] {
  flex-direction: row;
}
:host([orientation='vertical']) [part='base'] {
  flex-direction: column;
}
[part='label'] {
  margin: 0;
  padding: var(--step) 0;
  color: var(--color-gray-500);
  text-transform: uppercase;
  letter-spacing: 1px;
  font-size: var(--text-xs);
  font-weight: var(--text-bold);
}
:host-context([mode='dark']) [part='label'] {
  color: var(--color-gray-500);
}
:host([orientation='horizontal']) [part='content'] {
  padding: 0 var(--step-4);
}
:host([orientation='vertical']) [part='content'] {
  padding: var(--step) 0;
}
[part='content'] {
  width: 100%;
}
[part='actions'] {
  display: flex;
  gap: var(--step-2);
  justify-content: center;
  padding: var(--step-2) var(--step);
}
::slotted(*) {
  margin: 0;
  display: flex;
  box-shadow: inset 0 -1px 0 0 var(--color-gray-200);
  margin-bottom: var(--step);
  align-items: baseline;
  justify-content: space-between;
}
::slotted(:last-child) {
  box-shadow: none;
}
:host-context([mode='dark']) {
  ::slotted(*) {
    box-shadow: inset 0 -1px 0 0 var(--color-gray-700);
  }
}
`
class rp extends de {
  constructor() {
    super()
    W(this, '_cache', /* @__PURE__ */ new WeakMap())
    ;(this._children = []),
      (this._showMore = !1),
      (this.orientation = vc.Vertical),
      (this.limit = 1 / 0),
      (this.hideActions = !1)
  }
  _handleSlotChange(i) {
    i.stopPropagation(),
      (this._children = this.elsSlotted),
      this._toggleChildren()
  }
  _toggleChildren() {
    this._children.forEach((i, o) => {
      It(this._cache.has(i)) && this._cache.set(i, i.style.display),
        this._showMore || o < this.limit
          ? (i.style.display = this._cache.get(i, i.style.display))
          : (i.style.display = 'none')
    })
  }
  _renderShowMore() {
    return C`
      <div part="actions">
        <tbk-button
          shape="${Be.Pill}"
          size="${Lt.XXS}"
          variant="${$$.Secondary}"
          @click="${() => {
            ;(this._showMore = It(this._showMore)), this._toggleChildren()
          }}"
        >
          ${`${
            this._showMore
              ? 'Show Less'
              : `Show ${this._children.length - this.limit} More`
          }`}
        </tbk-button>
      </div>
    `
  }
  render() {
    return C`
      <div part="base">
        ${lt(this.label, C`<p part="label">${this.label}</p>`)}
        <div part="content">
          <slot @slotchange="${sn.debounce(
            this._handleSlotChange,
            200,
          )}"></slot>
          ${lt(
            this._children.length > this.limit && It(this.hideActions),
            this._renderShowMore(),
          )}
        </div>
      </div>
    `
  }
}
W(rp, 'styles', [Bt(), ft(z$)]),
  W(rp, 'properties', {
    orientation: { type: String, reflect: !0 },
    label: { type: String },
    limit: { type: Number },
    hideActions: { type: Boolean, reflect: !0, attribute: 'hide-actions' },
    _showMore: { type: String, state: !0 },
    _children: { type: Array, state: !0 },
  })
var A$ = st`
  :host {
    --divider-width: 4px;
    --divider-hit-area: 12px;
    --min: 0%;
    --max: 100%;

    display: grid;
  }

  .start,
  .end {
    overflow: hidden;
  }

  .divider {
    flex: 0 0 var(--divider-width);
    display: flex;
    position: relative;
    align-items: center;
    justify-content: center;
    background-color: var(--sl-color-neutral-200);
    color: var(--sl-color-neutral-900);
    z-index: 1;
  }

  .divider:focus {
    outline: none;
  }

  :host(:not([disabled])) .divider:focus-visible {
    background-color: var(--sl-color-primary-600);
    color: var(--sl-color-neutral-0);
  }

  :host([disabled]) .divider {
    cursor: not-allowed;
  }

  /* Horizontal */
  :host(:not([vertical], [disabled])) .divider {
    cursor: col-resize;
  }

  :host(:not([vertical])) .divider::after {
    display: flex;
    content: '';
    position: absolute;
    height: 100%;
    left: calc(var(--divider-hit-area) / -2 + var(--divider-width) / 2);
    width: var(--divider-hit-area);
  }

  /* Vertical */
  :host([vertical]) {
    flex-direction: column;
  }

  :host([vertical]:not([disabled])) .divider {
    cursor: row-resize;
  }

  :host([vertical]) .divider::after {
    content: '';
    position: absolute;
    width: 100%;
    top: calc(var(--divider-hit-area) / -2 + var(--divider-width) / 2);
    height: var(--divider-hit-area);
  }

  @media (forced-colors: active) {
    .divider {
      outline: solid 1px transparent;
    }
  }
`
function wo(t, r) {
  function i(a) {
    const d = t.getBoundingClientRect(),
      h = t.ownerDocument.defaultView,
      m = d.left + h.scrollX,
      f = d.top + h.scrollY,
      b = a.pageX - m,
      A = a.pageY - f
    r != null && r.onMove && r.onMove(b, A)
  }
  function o() {
    document.removeEventListener('pointermove', i),
      document.removeEventListener('pointerup', o),
      r != null && r.onStop && r.onStop()
  }
  document.addEventListener('pointermove', i, { passive: !0 }),
    document.addEventListener('pointerup', o),
    (r == null ? void 0 : r.initialEvent) instanceof PointerEvent &&
      i(r.initialEvent)
}
function ue(t, r, i) {
  const o = a => (Object.is(a, -0) ? 0 : a)
  return t < r ? o(r) : t > i ? o(i) : o(t)
}
/**
 * @license
 * Copyright 2018 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const it = t => t ?? Gt
var Ze = class extends nt {
  constructor() {
    super(...arguments),
      (this.localize = new Dt(this)),
      (this.position = 50),
      (this.vertical = !1),
      (this.disabled = !1),
      (this.snapThreshold = 12)
  }
  connectedCallback() {
    super.connectedCallback(),
      (this.resizeObserver = new ResizeObserver(t => this.handleResize(t))),
      this.updateComplete.then(() => this.resizeObserver.observe(this)),
      this.detectSize(),
      (this.cachedPositionInPixels = this.percentageToPixels(this.position))
  }
  disconnectedCallback() {
    var t
    super.disconnectedCallback(),
      (t = this.resizeObserver) == null || t.unobserve(this)
  }
  detectSize() {
    const { width: t, height: r } = this.getBoundingClientRect()
    this.size = this.vertical ? r : t
  }
  percentageToPixels(t) {
    return this.size * (t / 100)
  }
  pixelsToPercentage(t) {
    return (t / this.size) * 100
  }
  handleDrag(t) {
    const r = this.localize.dir() === 'rtl'
    this.disabled ||
      (t.cancelable && t.preventDefault(),
      wo(this, {
        onMove: (i, o) => {
          let a = this.vertical ? o : i
          this.primary === 'end' && (a = this.size - a),
            this.snap &&
              this.snap.split(' ').forEach(h => {
                let m
                h.endsWith('%')
                  ? (m = this.size * (parseFloat(h) / 100))
                  : (m = parseFloat(h)),
                  r && !this.vertical && (m = this.size - m),
                  a >= m - this.snapThreshold &&
                    a <= m + this.snapThreshold &&
                    (a = m)
              }),
            (this.position = ue(this.pixelsToPercentage(a), 0, 100))
        },
        initialEvent: t,
      }))
  }
  handleKeyDown(t) {
    if (
      !this.disabled &&
      [
        'ArrowLeft',
        'ArrowRight',
        'ArrowUp',
        'ArrowDown',
        'Home',
        'End',
      ].includes(t.key)
    ) {
      let r = this.position
      const i = (t.shiftKey ? 10 : 1) * (this.primary === 'end' ? -1 : 1)
      t.preventDefault(),
        ((t.key === 'ArrowLeft' && !this.vertical) ||
          (t.key === 'ArrowUp' && this.vertical)) &&
          (r -= i),
        ((t.key === 'ArrowRight' && !this.vertical) ||
          (t.key === 'ArrowDown' && this.vertical)) &&
          (r += i),
        t.key === 'Home' && (r = this.primary === 'end' ? 100 : 0),
        t.key === 'End' && (r = this.primary === 'end' ? 0 : 100),
        (this.position = ue(r, 0, 100))
    }
  }
  handleResize(t) {
    const { width: r, height: i } = t[0].contentRect
    ;(this.size = this.vertical ? i : r),
      (isNaN(this.cachedPositionInPixels) || this.position === 1 / 0) &&
        ((this.cachedPositionInPixels = Number(
          this.getAttribute('position-in-pixels'),
        )),
        (this.positionInPixels = Number(
          this.getAttribute('position-in-pixels'),
        )),
        (this.position = this.pixelsToPercentage(this.positionInPixels))),
      this.primary &&
        (this.position = this.pixelsToPercentage(this.cachedPositionInPixels))
  }
  handlePositionChange() {
    ;(this.cachedPositionInPixels = this.percentageToPixels(this.position)),
      (this.positionInPixels = this.percentageToPixels(this.position)),
      this.emit('sl-reposition')
  }
  handlePositionInPixelsChange() {
    this.position = this.pixelsToPercentage(this.positionInPixels)
  }
  handleVerticalChange() {
    this.detectSize()
  }
  render() {
    const t = this.vertical ? 'gridTemplateRows' : 'gridTemplateColumns',
      r = this.vertical ? 'gridTemplateColumns' : 'gridTemplateRows',
      i = this.localize.dir() === 'rtl',
      o = `
      clamp(
        0%,
        clamp(
          var(--min),
          ${this.position}% - var(--divider-width) / 2,
          var(--max)
        ),
        calc(100% - var(--divider-width))
      )
    `,
      a = 'auto'
    return (
      this.primary === 'end'
        ? i && !this.vertical
          ? (this.style[t] = `${o} var(--divider-width) ${a}`)
          : (this.style[t] = `${a} var(--divider-width) ${o}`)
        : i && !this.vertical
        ? (this.style[t] = `${a} var(--divider-width) ${o}`)
        : (this.style[t] = `${o} var(--divider-width) ${a}`),
      (this.style[r] = ''),
      C`
      <slot name="start" part="panel start" class="start"></slot>

      <div
        part="divider"
        class="divider"
        tabindex=${it(this.disabled ? void 0 : '0')}
        role="separator"
        aria-valuenow=${this.position}
        aria-valuemin="0"
        aria-valuemax="100"
        aria-label=${this.localize.term('resize')}
        @keydown=${this.handleKeyDown}
        @mousedown=${this.handleDrag}
        @touchstart=${this.handleDrag}
      >
        <slot name="divider"></slot>
      </div>

      <slot name="end" part="panel end" class="end"></slot>
    `
    )
  }
}
Ze.styles = [dt, A$]
c([J('.divider')], Ze.prototype, 'divider', 2)
c([p({ type: Number, reflect: !0 })], Ze.prototype, 'position', 2)
c(
  [p({ attribute: 'position-in-pixels', type: Number })],
  Ze.prototype,
  'positionInPixels',
  2,
)
c([p({ type: Boolean, reflect: !0 })], Ze.prototype, 'vertical', 2)
c([p({ type: Boolean, reflect: !0 })], Ze.prototype, 'disabled', 2)
c([p()], Ze.prototype, 'primary', 2)
c([p()], Ze.prototype, 'snap', 2)
c(
  [p({ type: Number, attribute: 'snap-threshold' })],
  Ze.prototype,
  'snapThreshold',
  2,
)
c([X('position')], Ze.prototype, 'handlePositionChange', 1)
c([X('positionInPixels')], Ze.prototype, 'handlePositionInPixelsChange', 1)
c([X('vertical')], Ze.prototype, 'handleVerticalChange', 1)
var ql = Ze
Ze.define('sl-split-panel')
const E$ = `:host {
  --divider-width: var(--step-6);
  --divider-hit-area: var(--step-8);

  --split-pane-knob-width: calc(var(--step) + 1px);
  --split-pane-knob-height: var(--step-12);
  --split-pane-width: 1px;
  --split-pane-height: 100%;
  --split-pane-padding: var(--step-8) 1px;
  --split-pane-margin: 0 var(--step-3);

  width: 100%;
  height: 100%;
}
:host([vertical]) {
  --split-pane-knob-width: var(--step-12);
  --split-pane-knob-height: calc(var(--step) + 1px);
  --split-pane-width: 100%;
  --split-pane-height: 1px;
  --split-pane-padding: 1px var(--step-8);
  --split-pane-margin: var(--step-3) 0;
}
[part='divider'] {
  margin: var(--split-pane-margin);
  padding: var(--split-pane-padding);
  width: var(--split-pane-width);
  height: var(--split-pane-height);
  background: var(--color-gray-100);
}
::slotted([slot='divider']) {
  position: absolute;
  border-radius: var(--radius-s);
  background: var(--color-gray-200);
  width: var(--split-pane-knob-width);
  height: var(--split-pane-knob-height);
}
`
class ip extends dr(ql) {}
W(ip, 'styles', [Bt(), ql.styles, ft(E$)]),
  W(ip, 'properties', {
    ...ql.properties,
  })
const T$ = `:host {
  --details-background: var(--color-variant-lucid);
  --details-color: var(--color-variant);
  --details-summary-color: var(--color-variant);
  --details-summary-background: var(--color-variant-lucid);
  --details-summary-background-hover: var(--color-variant-10);
  --details-border: none;
  --details-summary-shadow: none;
}
:host([outline]) {
  --details-border: 1px solid var(--color-gray-200);
}
:host([ghost]) {
  --details-summary-background: transparent;
  --details-background: transparent;
  --details-summary-background-hover: var(--color-variant-lucid);
}
:host([shadow]) {
  --details-summary-shadow: var(--shadow-s);
}
:host(:hover) {
  --details-summary-background: var(--details-summary-background-hover);
}
details {
  border-radius: var(--details-radius);
  background: var(--details-background);
}
summary {
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--details-padding-y) var(--details-padding-x);
  background: var(--details-summary-background);
  color: var(--details-summary-color);
  font-size: var(--details-font-size);
  border: var(--details-border);
  border-radius: var(--details-radius);
  overflow: hidden;
  box-shadow: var(--details-summary-shadow);
}
summary::marker {
  content: none;
}
summary > [name='plus'],
[open] summary > [name='minus'] {
  display: block;
}
summary > [name='minus'],
[open] summary > [name='plus'] {
  display: none;
}
[part='content'] {
  padding: calc(var(--details-padding-y) * 2) var(--details-padding-x);
  color: var(--details-color);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
`
class np extends de {
  constructor() {
    super(),
      (this.variant = $t.Neutral),
      (this.shape = Be.Round),
      (this.size = Lt.M),
      (this.summary = 'Details'),
      (this.outline = !1),
      (this.ghost = !1),
      (this.open = !1),
      (this.shadow = !1)
  }
  connectedCallback() {
    super.connectedCallback(),
      Fe(gc(this.summary), '"summary" must be a string')
  }
  render() {
    return C`
      <details
        part="base"
        .open="${this.open}"
      >
        <summary>
          <span>${this.summary}</span>
          <tbk-icon
            library="heroicons-micro"
            name="plus"
          ></tbk-icon>
          <tbk-icon
            library="heroicons-micro"
            name="minus"
          ></tbk-icon>
        </summary>
        <div part="content">
          <slot></slot>
        </div>
      </details>
    `
  }
}
W(np, 'styles', [
  Bt(),
  on(),
  Xr(),
  ge('details'),
  fi('details'),
  je('details', 1.25, 2),
  ft(T$),
]),
  W(np, 'properties', {
    summary: { type: String },
    outline: { type: Boolean, reflect: !0 },
    ghost: { type: Boolean, reflect: !0 },
    open: { type: Boolean, reflect: !0 },
    shape: { type: String, reflect: !0 },
    size: { type: String, reflect: !0 },
    shadow: { type: Boolean, reflect: !0 },
    variant: { type: String, reflect: !0 },
  })
var I$ = st`
  :host {
    display: inline-block;
  }

  .tab {
    display: inline-flex;
    align-items: center;
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-small);
    font-weight: var(--sl-font-weight-semibold);
    border-radius: var(--sl-border-radius-medium);
    color: var(--sl-color-neutral-600);
    padding: var(--sl-spacing-medium) var(--sl-spacing-large);
    white-space: nowrap;
    user-select: none;
    -webkit-user-select: none;
    cursor: pointer;
    transition:
      var(--transition-speed) box-shadow,
      var(--transition-speed) color;
  }

  .tab:hover:not(.tab--disabled) {
    color: var(--sl-color-primary-600);
  }

  :host(:focus) {
    outline: transparent;
  }

  :host(:focus-visible):not([disabled]) {
    color: var(--sl-color-primary-600);
  }

  :host(:focus-visible) {
    outline: var(--sl-focus-ring);
    outline-offset: calc(-1 * var(--sl-focus-ring-width) - var(--sl-focus-ring-offset));
  }

  .tab.tab--active:not(.tab--disabled) {
    color: var(--sl-color-primary-600);
  }

  .tab.tab--closable {
    padding-inline-end: var(--sl-spacing-small);
  }

  .tab.tab--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .tab__close-button {
    font-size: var(--sl-font-size-small);
    margin-inline-start: var(--sl-spacing-small);
  }

  .tab__close-button::part(base) {
    padding: var(--sl-spacing-3x-small);
  }

  @media (forced-colors: active) {
    .tab.tab--active:not(.tab--disabled) {
      outline: solid 1px transparent;
      outline-offset: -3px;
    }
  }
`,
  O$ = st`
  :host {
    display: inline-block;
    color: var(--sl-color-neutral-600);
  }

  .icon-button {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    background: none;
    border: none;
    border-radius: var(--sl-border-radius-medium);
    font-size: inherit;
    color: inherit;
    padding: var(--sl-spacing-x-small);
    cursor: pointer;
    transition: var(--sl-transition-x-fast) color;
    -webkit-appearance: none;
  }

  .icon-button:hover:not(.icon-button--disabled),
  .icon-button:focus-visible:not(.icon-button--disabled) {
    color: var(--sl-color-primary-600);
  }

  .icon-button:active:not(.icon-button--disabled) {
    color: var(--sl-color-primary-700);
  }

  .icon-button:focus {
    outline: none;
  }

  .icon-button--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .icon-button:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .icon-button__icon {
    pointer-events: none;
  }
`
/**
 * @license
 * Copyright 2020 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const bf = Symbol.for(''),
  L$ = t => {
    if ((t == null ? void 0 : t.r) === bf)
      return t == null ? void 0 : t._$litStatic$
  },
  Qs = (t, ...r) => ({
    _$litStatic$: r.reduce(
      (i, o, a) =>
        i +
        (d => {
          if (d._$litStatic$ !== void 0) return d._$litStatic$
          throw Error(`Value passed to 'literal' function must be a 'literal' result: ${d}. Use 'unsafeStatic' to pass non-literal values, but
            take care to ensure page security.`)
        })(o) +
        t[a + 1],
      t[0],
    ),
    r: bf,
  }),
  op = /* @__PURE__ */ new Map(),
  D$ =
    t =>
    (r, ...i) => {
      const o = i.length
      let a, d
      const h = [],
        m = []
      let f,
        b = 0,
        A = !1
      for (; b < o; ) {
        for (f = r[b]; b < o && ((d = i[b]), (a = L$(d)) !== void 0); )
          (f += a + r[++b]), (A = !0)
        b !== o && m.push(d), h.push(f), b++
      }
      if ((b === o && h.push(r[o]), A)) {
        const y = h.join('$$lit$$')
        ;(r = op.get(y)) === void 0 && ((h.raw = h), op.set(y, (r = h))),
          (i = m)
      }
      return t(r, ...i)
    },
  xo = D$(C)
var ye = class extends nt {
  constructor() {
    super(...arguments),
      (this.hasFocus = !1),
      (this.label = ''),
      (this.disabled = !1)
  }
  handleBlur() {
    ;(this.hasFocus = !1), this.emit('sl-blur')
  }
  handleFocus() {
    ;(this.hasFocus = !0), this.emit('sl-focus')
  }
  handleClick(t) {
    this.disabled && (t.preventDefault(), t.stopPropagation())
  }
  /** Simulates a click on the icon button. */
  click() {
    this.button.click()
  }
  /** Sets focus on the icon button. */
  focus(t) {
    this.button.focus(t)
  }
  /** Removes focus from the icon button. */
  blur() {
    this.button.blur()
  }
  render() {
    const t = !!this.href,
      r = t ? Qs`a` : Qs`button`
    return xo`
      <${r}
        part="base"
        class=${ct({
          'icon-button': !0,
          'icon-button--disabled': !t && this.disabled,
          'icon-button--focused': this.hasFocus,
        })}
        ?disabled=${it(t ? void 0 : this.disabled)}
        type=${it(t ? void 0 : 'button')}
        href=${it(t ? this.href : void 0)}
        target=${it(t ? this.target : void 0)}
        download=${it(t ? this.download : void 0)}
        rel=${it(t && this.target ? 'noreferrer noopener' : void 0)}
        role=${it(t ? void 0 : 'button')}
        aria-disabled=${this.disabled ? 'true' : 'false'}
        aria-label="${this.label}"
        tabindex=${this.disabled ? '-1' : '0'}
        @blur=${this.handleBlur}
        @focus=${this.handleFocus}
        @click=${this.handleClick}
      >
        <sl-icon
          class="icon-button__icon"
          name=${it(this.name)}
          library=${it(this.library)}
          src=${it(this.src)}
          aria-hidden="true"
        ></sl-icon>
      </${r}>
    `
  }
}
ye.styles = [dt, O$]
ye.dependencies = { 'sl-icon': Nt }
c([J('.icon-button')], ye.prototype, 'button', 2)
c([at()], ye.prototype, 'hasFocus', 2)
c([p()], ye.prototype, 'name', 2)
c([p()], ye.prototype, 'library', 2)
c([p()], ye.prototype, 'src', 2)
c([p()], ye.prototype, 'href', 2)
c([p()], ye.prototype, 'target', 2)
c([p()], ye.prototype, 'download', 2)
c([p()], ye.prototype, 'label', 2)
c([p({ type: Boolean, reflect: !0 })], ye.prototype, 'disabled', 2)
var M$ = 0,
  Xe = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.attrId = ++M$),
        (this.componentId = `sl-tab-${this.attrId}`),
        (this.panel = ''),
        (this.active = !1),
        (this.closable = !1),
        (this.disabled = !1),
        (this.tabIndex = 0)
    }
    connectedCallback() {
      super.connectedCallback(), this.setAttribute('role', 'tab')
    }
    handleCloseClick(t) {
      t.stopPropagation(), this.emit('sl-close')
    }
    handleActiveChange() {
      this.setAttribute('aria-selected', this.active ? 'true' : 'false')
    }
    handleDisabledChange() {
      this.setAttribute('aria-disabled', this.disabled ? 'true' : 'false'),
        this.disabled && !this.active
          ? (this.tabIndex = -1)
          : (this.tabIndex = 0)
    }
    render() {
      return (
        (this.id = this.id.length > 0 ? this.id : this.componentId),
        C`
      <div
        part="base"
        class=${ct({
          tab: !0,
          'tab--active': this.active,
          'tab--closable': this.closable,
          'tab--disabled': this.disabled,
        })}
      >
        <slot></slot>
        ${
          this.closable
            ? C`
              <sl-icon-button
                part="close-button"
                exportparts="base:close-button__base"
                name="x-lg"
                library="system"
                label=${this.localize.term('close')}
                class="tab__close-button"
                @click=${this.handleCloseClick}
                tabindex="-1"
              ></sl-icon-button>
            `
            : ''
        }
      </div>
    `
      )
    }
  }
Xe.styles = [dt, I$]
Xe.dependencies = { 'sl-icon-button': ye }
c([J('.tab')], Xe.prototype, 'tab', 2)
c([p({ reflect: !0 })], Xe.prototype, 'panel', 2)
c([p({ type: Boolean, reflect: !0 })], Xe.prototype, 'active', 2)
c([p({ type: Boolean, reflect: !0 })], Xe.prototype, 'closable', 2)
c([p({ type: Boolean, reflect: !0 })], Xe.prototype, 'disabled', 2)
c([p({ type: Number, reflect: !0 })], Xe.prototype, 'tabIndex', 2)
c([X('active')], Xe.prototype, 'handleActiveChange', 1)
c([X('disabled')], Xe.prototype, 'handleDisabledChange', 1)
const R$ = `:host {
  --tab-font-size: var(--font-size);
  --tab-background: var(--color-variant-lucid);
  --tab-color: var(--color-variant);

  display: block;
  font-family: var(--font-mono);
  font-weight: var(--text-bold);
  margin: 0;
  border-style: solid;
  border-width: 0;
  border-color: transparent;
}
:host([active]) [part='base'],
:host([active]:hover) [part='base'] {
  background: var(--tab-background);
  color: var(--tab-color);
  cursor: default;
}
:host([active]) {
  border-color: var(--color-variant);
}
[part='base'] {
  width: 100%;
  padding: var(--tab-padding-y) var(--tab-padding-x);
  border-radius: var(--radius-2xs);
  outline: none;
  color: var(--color-gray-400);
}
:host(:hover) [part='base'] {
  background: var(--color-gray-5);
  color: var(--color-gray-400);
}
:host([active][inverse]) [part='base'] {
  --tab-background: var(--color-variant);
  --tab-color: var(--color-variant-light);
}
`
class sp extends dr(Xe, nn) {
  constructor() {
    super()
    W(this, 'componentId', `tbk-tab-${this.attrId}`)
    ;(this.size = Lt.M), (this.shape = Be.Round), (this.inverse = !1)
  }
}
W(sp, 'styles', [
  Xe.styles,
  Bt(),
  ge(),
  Xr(),
  fi('tab'),
  je('tab', 1.75, 4),
  ft(R$),
]),
  W(sp, 'properties', {
    ...Xe.properties,
    size: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
    inverse: { type: Boolean, reflect: !0 },
    variant: { type: String },
  })
const P$ = `:host {
  --indicator-color: transparent;
  --track-color: var(--color-gray-5);
  --track-width: var(--half);
  --tabs-font-size: var(--font-size);
  --tabs-color: var(--color-variant);

  width: 100%;
  height: 100%;
}
:host-context([mode='dark']) {
  --tabs-color: var(--color-variant-light);
}
[part='nav'].tab-group__nav-container {
  padding-bottom: 0;
  padding-top: 0;
  margin-bottom: var(--step-2);
  padding: 0;
}
:host([placement='bottom']) [part='nav'].tab-group__nav-container {
  margin-top: var(--step);
  margin-bottom: 0;
}
:host([placement='start']) [part='nav'].tab-group__nav-container,
:host([placement='end']) [part='nav'].tab-group__nav-container {
  margin-bottom: 0;
}
[part='base'] {
  width: 100%;
  height: 100%;
  font-size: var(--tabs-font-size);
  font-weight: var(--text-bold);
  padding: 0 var(--step);
}
:host([fullwidth]) ::slotted(tbk-tab) {
  flex: 1;
}
::slotted(tbk-tab) {
  padding: var(--step-3) 0;
  border-bottom-width: var(--half);
}
:host([placement='start']) ::slotted(tbk-tab) {
  padding: 0 var(--step-3);
  border-width: 0;
  border-right-width: var(--half);
}
:host([placement='end']) ::slotted(tbk-tab) {
  padding: 0 var(--step-3);
  border-width: 0;
  border-left-width: var(--half);
}
:host([placement='bottom']) ::slotted(tbk-tab) {
  padding: var(--step-3) 0;
  border-width: 0;
  border-top-width: var(--half);
}
[part='tabs'] {
  gap: var(--step);
}
.tab-group--start .tab-group__tabs {
  border: 0;
  box-shadow: inset calc(var(--track-width) * -1) 0 0 0 var(--track-color);
}
.tab-group--end .tab-group__tabs {
  border: 0;
  box-shadow: inset calc(var(--track-width)) 0 0 0 var(--track-color);
}
.tab-group--bottom .tab-group__tabs {
  border: 0;
  box-shadow: inset 0 calc(var(--track-width)) 0 0 var(--track-color);
}
.tab-group--top .tab-group__tabs {
  border: 0;
  box-shadow: inset 0 calc(var(--track-width) * -1) 0 0 var(--track-color);
}
[part='nav'].tab-group__nav-container tbk-tab {
  padding: 0;
}
`
var B$ = st`
  :host {
    --indicator-color: var(--sl-color-primary-600);
    --track-color: var(--sl-color-neutral-200);
    --track-width: 2px;

    display: block;
  }

  .tab-group {
    display: flex;
    border-radius: 0;
  }

  .tab-group__tabs {
    display: flex;
    position: relative;
  }

  .tab-group__indicator {
    position: absolute;
    transition:
      var(--sl-transition-fast) translate ease,
      var(--sl-transition-fast) width ease;
  }

  .tab-group--has-scroll-controls .tab-group__nav-container {
    position: relative;
    padding: 0 var(--sl-spacing-x-large);
  }

  .tab-group--has-scroll-controls .tab-group__scroll-button--start--hidden,
  .tab-group--has-scroll-controls .tab-group__scroll-button--end--hidden {
    visibility: hidden;
  }

  .tab-group__body {
    display: block;
    overflow: auto;
  }

  .tab-group__scroll-button {
    display: flex;
    align-items: center;
    justify-content: center;
    position: absolute;
    top: 0;
    bottom: 0;
    width: var(--sl-spacing-x-large);
  }

  .tab-group__scroll-button--start {
    left: 0;
  }

  .tab-group__scroll-button--end {
    right: 0;
  }

  .tab-group--rtl .tab-group__scroll-button--start {
    left: auto;
    right: 0;
  }

  .tab-group--rtl .tab-group__scroll-button--end {
    left: 0;
    right: auto;
  }

  /*
   * Top
   */

  .tab-group--top {
    flex-direction: column;
  }

  .tab-group--top .tab-group__nav-container {
    order: 1;
  }

  .tab-group--top .tab-group__nav {
    display: flex;
    overflow-x: auto;

    /* Hide scrollbar in Firefox */
    scrollbar-width: none;
  }

  /* Hide scrollbar in Chrome/Safari */
  .tab-group--top .tab-group__nav::-webkit-scrollbar {
    width: 0;
    height: 0;
  }

  .tab-group--top .tab-group__tabs {
    flex: 1 1 auto;
    position: relative;
    flex-direction: row;
    border-bottom: solid var(--track-width) var(--track-color);
  }

  .tab-group--top .tab-group__indicator {
    bottom: calc(-1 * var(--track-width));
    border-bottom: solid var(--track-width) var(--indicator-color);
  }

  .tab-group--top .tab-group__body {
    order: 2;
  }

  .tab-group--top ::slotted(sl-tab-panel) {
    --padding: var(--sl-spacing-medium) 0;
  }

  /*
   * Bottom
   */

  .tab-group--bottom {
    flex-direction: column;
  }

  .tab-group--bottom .tab-group__nav-container {
    order: 2;
  }

  .tab-group--bottom .tab-group__nav {
    display: flex;
    overflow-x: auto;

    /* Hide scrollbar in Firefox */
    scrollbar-width: none;
  }

  /* Hide scrollbar in Chrome/Safari */
  .tab-group--bottom .tab-group__nav::-webkit-scrollbar {
    width: 0;
    height: 0;
  }

  .tab-group--bottom .tab-group__tabs {
    flex: 1 1 auto;
    position: relative;
    flex-direction: row;
    border-top: solid var(--track-width) var(--track-color);
  }

  .tab-group--bottom .tab-group__indicator {
    top: calc(-1 * var(--track-width));
    border-top: solid var(--track-width) var(--indicator-color);
  }

  .tab-group--bottom .tab-group__body {
    order: 1;
  }

  .tab-group--bottom ::slotted(sl-tab-panel) {
    --padding: var(--sl-spacing-medium) 0;
  }

  /*
   * Start
   */

  .tab-group--start {
    flex-direction: row;
  }

  .tab-group--start .tab-group__nav-container {
    order: 1;
  }

  .tab-group--start .tab-group__tabs {
    flex: 0 0 auto;
    flex-direction: column;
    border-inline-end: solid var(--track-width) var(--track-color);
  }

  .tab-group--start .tab-group__indicator {
    right: calc(-1 * var(--track-width));
    border-right: solid var(--track-width) var(--indicator-color);
  }

  .tab-group--start.tab-group--rtl .tab-group__indicator {
    right: auto;
    left: calc(-1 * var(--track-width));
  }

  .tab-group--start .tab-group__body {
    flex: 1 1 auto;
    order: 2;
  }

  .tab-group--start ::slotted(sl-tab-panel) {
    --padding: 0 var(--sl-spacing-medium);
  }

  /*
   * End
   */

  .tab-group--end {
    flex-direction: row;
  }

  .tab-group--end .tab-group__nav-container {
    order: 2;
  }

  .tab-group--end .tab-group__tabs {
    flex: 0 0 auto;
    flex-direction: column;
    border-left: solid var(--track-width) var(--track-color);
  }

  .tab-group--end .tab-group__indicator {
    left: calc(-1 * var(--track-width));
    border-inline-start: solid var(--track-width) var(--indicator-color);
  }

  .tab-group--end.tab-group--rtl .tab-group__indicator {
    right: calc(-1 * var(--track-width));
    left: auto;
  }

  .tab-group--end .tab-group__body {
    flex: 1 1 auto;
    order: 1;
  }

  .tab-group--end ::slotted(sl-tab-panel) {
    --padding: 0 var(--sl-spacing-medium);
  }
`
function F$(t, r) {
  return {
    top: Math.round(
      t.getBoundingClientRect().top - r.getBoundingClientRect().top,
    ),
    left: Math.round(
      t.getBoundingClientRect().left - r.getBoundingClientRect().left,
    ),
  }
}
var oc = /* @__PURE__ */ new Set()
function N$() {
  const t = document.documentElement.clientWidth
  return Math.abs(window.innerWidth - t)
}
function U$() {
  const t = Number(
    getComputedStyle(document.body).paddingRight.replace(/px/, ''),
  )
  return isNaN(t) || !t ? 0 : t
}
function ko(t) {
  if (
    (oc.add(t), !document.documentElement.classList.contains('sl-scroll-lock'))
  ) {
    const r = N$() + U$()
    let i = getComputedStyle(document.documentElement).scrollbarGutter
    ;(!i || i === 'auto') && (i = 'stable'),
      r < 2 && (i = ''),
      document.documentElement.style.setProperty('--sl-scroll-lock-gutter', i),
      document.documentElement.classList.add('sl-scroll-lock'),
      document.documentElement.style.setProperty(
        '--sl-scroll-lock-size',
        `${r}px`,
      )
  }
}
function $o(t) {
  oc.delete(t),
    oc.size === 0 &&
      (document.documentElement.classList.remove('sl-scroll-lock'),
      document.documentElement.style.removeProperty('--sl-scroll-lock-size'))
}
function sc(t, r, i = 'vertical', o = 'smooth') {
  const a = F$(t, r),
    d = a.top + r.scrollTop,
    h = a.left + r.scrollLeft,
    m = r.scrollLeft,
    f = r.scrollLeft + r.offsetWidth,
    b = r.scrollTop,
    A = r.scrollTop + r.offsetHeight
  ;(i === 'horizontal' || i === 'both') &&
    (h < m
      ? r.scrollTo({ left: h, behavior: o })
      : h + t.clientWidth > f &&
        r.scrollTo({ left: h - r.offsetWidth + t.clientWidth, behavior: o })),
    (i === 'vertical' || i === 'both') &&
      (d < b
        ? r.scrollTo({ top: d, behavior: o })
        : d + t.clientHeight > A &&
          r.scrollTo({ top: d - r.offsetHeight + t.clientHeight, behavior: o }))
}
var fe = class extends nt {
  constructor() {
    super(...arguments),
      (this.tabs = []),
      (this.focusableTabs = []),
      (this.panels = []),
      (this.localize = new Dt(this)),
      (this.hasScrollControls = !1),
      (this.shouldHideScrollStartButton = !1),
      (this.shouldHideScrollEndButton = !1),
      (this.placement = 'top'),
      (this.activation = 'auto'),
      (this.noScrollControls = !1),
      (this.fixedScrollControls = !1),
      (this.scrollOffset = 1)
  }
  connectedCallback() {
    const t = Promise.all([
      customElements.whenDefined('sl-tab'),
      customElements.whenDefined('sl-tab-panel'),
    ])
    super.connectedCallback(),
      (this.resizeObserver = new ResizeObserver(() => {
        this.repositionIndicator(), this.updateScrollControls()
      })),
      (this.mutationObserver = new MutationObserver(r => {
        r.some(
          i => !['aria-labelledby', 'aria-controls'].includes(i.attributeName),
        ) && setTimeout(() => this.setAriaLabels()),
          r.some(i => i.attributeName === 'disabled') &&
            this.syncTabsAndPanels()
      })),
      this.updateComplete.then(() => {
        this.syncTabsAndPanels(),
          this.mutationObserver.observe(this, {
            attributes: !0,
            childList: !0,
            subtree: !0,
          }),
          this.resizeObserver.observe(this.nav),
          t.then(() => {
            new IntersectionObserver((i, o) => {
              var a
              i[0].intersectionRatio > 0 &&
                (this.setAriaLabels(),
                this.setActiveTab(
                  (a = this.getActiveTab()) != null ? a : this.tabs[0],
                  { emitEvents: !1 },
                ),
                o.unobserve(i[0].target))
            }).observe(this.tabGroup)
          })
      })
  }
  disconnectedCallback() {
    var t, r
    super.disconnectedCallback(),
      (t = this.mutationObserver) == null || t.disconnect(),
      this.nav && ((r = this.resizeObserver) == null || r.unobserve(this.nav))
  }
  getAllTabs() {
    return this.shadowRoot.querySelector('slot[name="nav"]').assignedElements()
  }
  getAllPanels() {
    return [...this.body.assignedElements()].filter(
      t => t.tagName.toLowerCase() === 'sl-tab-panel',
    )
  }
  getActiveTab() {
    return this.tabs.find(t => t.active)
  }
  handleClick(t) {
    const i = t.target.closest('sl-tab')
    ;(i == null ? void 0 : i.closest('sl-tab-group')) === this &&
      i !== null &&
      this.setActiveTab(i, { scrollBehavior: 'smooth' })
  }
  handleKeyDown(t) {
    const i = t.target.closest('sl-tab')
    if (
      (i == null ? void 0 : i.closest('sl-tab-group')) === this &&
      (['Enter', ' '].includes(t.key) &&
        i !== null &&
        (this.setActiveTab(i, { scrollBehavior: 'smooth' }),
        t.preventDefault()),
      [
        'ArrowLeft',
        'ArrowRight',
        'ArrowUp',
        'ArrowDown',
        'Home',
        'End',
      ].includes(t.key))
    ) {
      const a = this.tabs.find(m => m.matches(':focus')),
        d = this.localize.dir() === 'rtl'
      let h = null
      if ((a == null ? void 0 : a.tagName.toLowerCase()) === 'sl-tab') {
        if (t.key === 'Home') h = this.focusableTabs[0]
        else if (t.key === 'End')
          h = this.focusableTabs[this.focusableTabs.length - 1]
        else if (
          (['top', 'bottom'].includes(this.placement) &&
            t.key === (d ? 'ArrowRight' : 'ArrowLeft')) ||
          (['start', 'end'].includes(this.placement) && t.key === 'ArrowUp')
        ) {
          const m = this.tabs.findIndex(f => f === a)
          h = this.findNextFocusableTab(m, 'backward')
        } else if (
          (['top', 'bottom'].includes(this.placement) &&
            t.key === (d ? 'ArrowLeft' : 'ArrowRight')) ||
          (['start', 'end'].includes(this.placement) && t.key === 'ArrowDown')
        ) {
          const m = this.tabs.findIndex(f => f === a)
          h = this.findNextFocusableTab(m, 'forward')
        }
        if (!h) return
        ;(h.tabIndex = 0),
          h.focus({ preventScroll: !0 }),
          this.activation === 'auto'
            ? this.setActiveTab(h, { scrollBehavior: 'smooth' })
            : this.tabs.forEach(m => {
                m.tabIndex = m === h ? 0 : -1
              }),
          ['top', 'bottom'].includes(this.placement) &&
            sc(h, this.nav, 'horizontal'),
          t.preventDefault()
      }
    }
  }
  handleScrollToStart() {
    this.nav.scroll({
      left:
        this.localize.dir() === 'rtl'
          ? this.nav.scrollLeft + this.nav.clientWidth
          : this.nav.scrollLeft - this.nav.clientWidth,
      behavior: 'smooth',
    })
  }
  handleScrollToEnd() {
    this.nav.scroll({
      left:
        this.localize.dir() === 'rtl'
          ? this.nav.scrollLeft - this.nav.clientWidth
          : this.nav.scrollLeft + this.nav.clientWidth,
      behavior: 'smooth',
    })
  }
  setActiveTab(t, r) {
    if (
      ((r = pi(
        {
          emitEvents: !0,
          scrollBehavior: 'auto',
        },
        r,
      )),
      t !== this.activeTab && !t.disabled)
    ) {
      const i = this.activeTab
      ;(this.activeTab = t),
        this.tabs.forEach(o => {
          ;(o.active = o === this.activeTab),
            (o.tabIndex = o === this.activeTab ? 0 : -1)
        }),
        this.panels.forEach(o => {
          var a
          return (o.active =
            o.name === ((a = this.activeTab) == null ? void 0 : a.panel))
        }),
        this.syncIndicator(),
        ['top', 'bottom'].includes(this.placement) &&
          sc(this.activeTab, this.nav, 'horizontal', r.scrollBehavior),
        r.emitEvents &&
          (i && this.emit('sl-tab-hide', { detail: { name: i.panel } }),
          this.emit('sl-tab-show', { detail: { name: this.activeTab.panel } }))
    }
  }
  setAriaLabels() {
    this.tabs.forEach(t => {
      const r = this.panels.find(i => i.name === t.panel)
      r &&
        (t.setAttribute('aria-controls', r.getAttribute('id')),
        r.setAttribute('aria-labelledby', t.getAttribute('id')))
    })
  }
  repositionIndicator() {
    const t = this.getActiveTab()
    if (!t) return
    const r = t.clientWidth,
      i = t.clientHeight,
      o = this.localize.dir() === 'rtl',
      a = this.getAllTabs(),
      h = a.slice(0, a.indexOf(t)).reduce(
        (m, f) => ({
          left: m.left + f.clientWidth,
          top: m.top + f.clientHeight,
        }),
        { left: 0, top: 0 },
      )
    switch (this.placement) {
      case 'top':
      case 'bottom':
        ;(this.indicator.style.width = `${r}px`),
          (this.indicator.style.height = 'auto'),
          (this.indicator.style.translate = o
            ? `${-1 * h.left}px`
            : `${h.left}px`)
        break
      case 'start':
      case 'end':
        ;(this.indicator.style.width = 'auto'),
          (this.indicator.style.height = `${i}px`),
          (this.indicator.style.translate = `0 ${h.top}px`)
        break
    }
  }
  // This stores tabs and panels so we can refer to a cache instead of calling querySelectorAll() multiple times.
  syncTabsAndPanels() {
    ;(this.tabs = this.getAllTabs()),
      (this.focusableTabs = this.tabs.filter(t => !t.disabled)),
      (this.panels = this.getAllPanels()),
      this.syncIndicator(),
      this.updateComplete.then(() => this.updateScrollControls())
  }
  findNextFocusableTab(t, r) {
    let i = null
    const o = r === 'forward' ? 1 : -1
    let a = t + o
    for (; t < this.tabs.length; ) {
      if (((i = this.tabs[a] || null), i === null)) {
        r === 'forward'
          ? (i = this.focusableTabs[0])
          : (i = this.focusableTabs[this.focusableTabs.length - 1])
        break
      }
      if (!i.disabled) break
      a += o
    }
    return i
  }
  updateScrollButtons() {
    this.hasScrollControls &&
      !this.fixedScrollControls &&
      ((this.shouldHideScrollStartButton =
        this.scrollFromStart() <= this.scrollOffset),
      (this.shouldHideScrollEndButton = this.isScrolledToEnd()))
  }
  isScrolledToEnd() {
    return (
      this.scrollFromStart() + this.nav.clientWidth >=
      this.nav.scrollWidth - this.scrollOffset
    )
  }
  scrollFromStart() {
    return this.localize.dir() === 'rtl'
      ? -this.nav.scrollLeft
      : this.nav.scrollLeft
  }
  updateScrollControls() {
    this.noScrollControls
      ? (this.hasScrollControls = !1)
      : (this.hasScrollControls =
          ['top', 'bottom'].includes(this.placement) &&
          this.nav.scrollWidth > this.nav.clientWidth + 1),
      this.updateScrollButtons()
  }
  syncIndicator() {
    this.getActiveTab()
      ? ((this.indicator.style.display = 'block'), this.repositionIndicator())
      : (this.indicator.style.display = 'none')
  }
  /** Shows the specified tab panel. */
  show(t) {
    const r = this.tabs.find(i => i.panel === t)
    r && this.setActiveTab(r, { scrollBehavior: 'smooth' })
  }
  render() {
    const t = this.localize.dir() === 'rtl'
    return C`
      <div
        part="base"
        class=${ct({
          'tab-group': !0,
          'tab-group--top': this.placement === 'top',
          'tab-group--bottom': this.placement === 'bottom',
          'tab-group--start': this.placement === 'start',
          'tab-group--end': this.placement === 'end',
          'tab-group--rtl': this.localize.dir() === 'rtl',
          'tab-group--has-scroll-controls': this.hasScrollControls,
        })}
        @click=${this.handleClick}
        @keydown=${this.handleKeyDown}
      >
        <div class="tab-group__nav-container" part="nav">
          ${
            this.hasScrollControls
              ? C`
                <sl-icon-button
                  part="scroll-button scroll-button--start"
                  exportparts="base:scroll-button__base"
                  class=${ct({
                    'tab-group__scroll-button': !0,
                    'tab-group__scroll-button--start': !0,
                    'tab-group__scroll-button--start--hidden':
                      this.shouldHideScrollStartButton,
                  })}
                  name=${t ? 'chevron-right' : 'chevron-left'}
                  library="system"
                  tabindex="-1"
                  aria-hidden="true"
                  label=${this.localize.term('scrollToStart')}
                  @click=${this.handleScrollToStart}
                ></sl-icon-button>
              `
              : ''
          }

          <div class="tab-group__nav" @scrollend=${this.updateScrollButtons}>
            <div part="tabs" class="tab-group__tabs" role="tablist">
              <div part="active-tab-indicator" class="tab-group__indicator"></div>
              <sl-resize-observer @sl-resize=${this.syncIndicator}>
                <slot name="nav" @slotchange=${this.syncTabsAndPanels}></slot>
              </sl-resize-observer>
            </div>
          </div>

          ${
            this.hasScrollControls
              ? C`
                <sl-icon-button
                  part="scroll-button scroll-button--end"
                  exportparts="base:scroll-button__base"
                  class=${ct({
                    'tab-group__scroll-button': !0,
                    'tab-group__scroll-button--end': !0,
                    'tab-group__scroll-button--end--hidden':
                      this.shouldHideScrollEndButton,
                  })}
                  name=${t ? 'chevron-left' : 'chevron-right'}
                  library="system"
                  tabindex="-1"
                  aria-hidden="true"
                  label=${this.localize.term('scrollToEnd')}
                  @click=${this.handleScrollToEnd}
                ></sl-icon-button>
              `
              : ''
          }
        </div>

        <slot part="body" class="tab-group__body" @slotchange=${
          this.syncTabsAndPanels
        }></slot>
      </div>
    `
  }
}
fe.styles = [dt, B$]
fe.dependencies = { 'sl-icon-button': ye, 'sl-resize-observer': en }
c([J('.tab-group')], fe.prototype, 'tabGroup', 2)
c([J('.tab-group__body')], fe.prototype, 'body', 2)
c([J('.tab-group__nav')], fe.prototype, 'nav', 2)
c([J('.tab-group__indicator')], fe.prototype, 'indicator', 2)
c([at()], fe.prototype, 'hasScrollControls', 2)
c([at()], fe.prototype, 'shouldHideScrollStartButton', 2)
c([at()], fe.prototype, 'shouldHideScrollEndButton', 2)
c([p()], fe.prototype, 'placement', 2)
c([p()], fe.prototype, 'activation', 2)
c(
  [p({ attribute: 'no-scroll-controls', type: Boolean })],
  fe.prototype,
  'noScrollControls',
  2,
)
c(
  [p({ attribute: 'fixed-scroll-controls', type: Boolean })],
  fe.prototype,
  'fixedScrollControls',
  2,
)
c([Do({ passive: !0 })], fe.prototype, 'updateScrollButtons', 1)
c(
  [X('noScrollControls', { waitUntilFirstUpdate: !0 })],
  fe.prototype,
  'updateScrollControls',
  1,
)
c(
  [X('placement', { waitUntilFirstUpdate: !0 })],
  fe.prototype,
  'syncIndicator',
  1,
)
var H$ = (t, r) => {
    let i = 0
    return function (...o) {
      window.clearTimeout(i),
        (i = window.setTimeout(() => {
          t.call(this, ...o)
        }, r))
    }
  },
  ap = (t, r, i) => {
    const o = t[r]
    t[r] = function (...a) {
      o.call(this, ...a), i.call(this, o, ...a)
    }
  },
  V$ = 'onscrollend' in window
if (!V$) {
  const t = /* @__PURE__ */ new Set(),
    r = /* @__PURE__ */ new WeakMap(),
    i = a => {
      for (const d of a.changedTouches) t.add(d.identifier)
    },
    o = a => {
      for (const d of a.changedTouches) t.delete(d.identifier)
    }
  document.addEventListener('touchstart', i, !0),
    document.addEventListener('touchend', o, !0),
    document.addEventListener('touchcancel', o, !0),
    ap(EventTarget.prototype, 'addEventListener', function (a, d) {
      if (d !== 'scrollend') return
      const h = H$(() => {
        t.size ? h() : this.dispatchEvent(new Event('scrollend'))
      }, 100)
      a.call(this, 'scroll', h, { passive: !0 }), r.set(this, h)
    }),
    ap(EventTarget.prototype, 'removeEventListener', function (a, d) {
      if (d !== 'scrollend') return
      const h = r.get(this)
      h && a.call(this, 'scroll', h, { passive: !0 })
    })
}
class lp extends dr(fe) {
  constructor() {
    super(),
      (this.size = Lt.M),
      (this.inverse = !1),
      (this.placement = qr.Top),
      (this.variant = $t.Neutral),
      (this.fullwidth = !1)
  }
  get elSlotNav() {
    var r
    return (r = this.renderRoot) == null
      ? void 0
      : r.querySelector('slot[name="nav"]')
  }
  async firstUpdated() {
    ;(this.resizeObserver = new ResizeObserver(() => {
      this.repositionIndicator(), this.updateScrollControls()
    })),
      (this.mutationObserver = new MutationObserver(i => {
        i.some(o =>
          It(['aria-labelledby', 'aria-controls'].includes(o.attributeName)),
        ) && setTimeout(() => this.setAriaLabels()),
          i.some(o => o.attributeName === 'disabled') &&
            this.syncTabsAndPanels()
      }))
    const r = Promise.all([
      customElements.whenDefined('tbk-tab'),
      customElements.whenDefined('tbk-tab-panel'),
    ])
    await super.firstUpdated(),
      this.updateComplete.then(() => {
        this.syncTabsAndPanels(),
          this.mutationObserver.observe(this, {
            attributes: !0,
            childList: !0,
            subtree: !0,
          }),
          this.resizeObserver.observe(this.nav),
          r.then(() => {
            setTimeout(() => {
              new IntersectionObserver((o, a) => {
                o[0].intersectionRatio > 0 &&
                  (this.setAriaLabels(),
                  this.setActiveTab(this.getActiveTab() ?? this.tabs[0], {
                    emitEvents: !1,
                  }),
                  a.unobserve(o[0].target))
              }).observe(this.tabGroup),
                this.getAllTabs().forEach(o => {
                  o.setAttribute('size', this.size),
                    Qt(o.variant) && (o.variant = this.variant),
                    this.inverse && (o.inverse = this.inverse)
                }),
                this.getAllPanels().forEach(o => {
                  o.setAttribute('size', this.size),
                    Qt(o.getAttribute('variant')) &&
                      o.setAttribute('variant', this.variant)
                })
            }, 500)
          })
      })
  }
  getAllTabs(r = { includeDisabled: !0 }) {
    return (
      this.elSlotNav ? [...this.elSlotNav.assignedElements()] : []
    ).filter(o =>
      r.includeDisabled
        ? o.tagName.toLowerCase() === 'tbk-tab'
        : o.tagName.toLowerCase() === 'tbk-tab' && !o.disabled,
    )
  }
  getAllPanels() {
    return [...this.body.assignedElements()].filter(
      r => r.tagName.toLowerCase() === 'tbk-tab-panel',
    )
  }
  handleClick(r) {
    const o = r.target.closest('tbk-tab')
    ;(o == null ? void 0 : o.closest('tbk-tabs')) === this &&
      o !== null &&
      (this.setActiveTab(o, { scrollBehavior: 'smooth' }),
      this.emit('tab-change', {
        detail: new this.emit.EventDetail(o.panel, r),
      }))
  }
  handleKeyDown(r) {
    const o = r.target.closest('tbk-tab')
    if (
      (o == null ? void 0 : o.closest('tbk-tabs')) === this &&
      (['Enter', ' '].includes(r.key) &&
        o !== null &&
        (this.setActiveTab(o, { scrollBehavior: 'smooth' }),
        r.preventDefault()),
      [
        'ArrowLeft',
        'ArrowRight',
        'ArrowUp',
        'ArrowDown',
        'Home',
        'End',
      ].includes(r.key))
    ) {
      const d = this.tabs.find(m => m.matches(':focus')),
        h = this.localize.dir() === 'rtl'
      if ((d == null ? void 0 : d.tagName.toLowerCase()) === 'tbk-tab') {
        let m = this.tabs.indexOf(d)
        r.key === 'Home'
          ? (m = 0)
          : r.key === 'End'
          ? (m = this.tabs.length - 1)
          : (['top', 'bottom'].includes(this.placement) &&
              r.key === (h ? 'ArrowRight' : 'ArrowLeft')) ||
            (['start', 'end'].includes(this.placement) && r.key === 'ArrowUp')
          ? m--
          : ((['top', 'bottom'].includes(this.placement) &&
              r.key === (h ? 'ArrowLeft' : 'ArrowRight')) ||
              (['start', 'end'].includes(this.placement) &&
                r.key === 'ArrowDown')) &&
            m++,
          m < 0 && (m = this.tabs.length - 1),
          m > this.tabs.length - 1 && (m = 0),
          this.tabs[m].focus({ preventScroll: !0 }),
          this.activation === 'auto' &&
            this.setActiveTab(this.tabs[m], { scrollBehavior: 'smooth' }),
          ['top', 'bottom'].includes(this.placement) &&
            ww(this.tabs[m], this.nav, 'horizontal'),
          r.preventDefault()
      }
    }
  }
}
W(lp, 'styles', [fe.styles, Bt(), ge(), Xr(), je('tabs', 1.25, 4), ft(P$)]),
  W(lp, 'properties', {
    ...fe.properties,
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    inverse: { type: Boolean, reflect: !0 },
    fullwidth: { type: Boolean, reflect: !0 },
  })
var W$ = st`
  :host {
    --padding: 0;

    display: none;
  }

  :host([active]) {
    display: block;
  }

  .tab-panel {
    display: block;
    padding: var(--padding);
  }
`,
  q$ = 0,
  Ii = class extends nt {
    constructor() {
      super(...arguments),
        (this.attrId = ++q$),
        (this.componentId = `sl-tab-panel-${this.attrId}`),
        (this.name = ''),
        (this.active = !1)
    }
    connectedCallback() {
      super.connectedCallback(),
        (this.id = this.id.length > 0 ? this.id : this.componentId),
        this.setAttribute('role', 'tabpanel')
    }
    handleActiveChange() {
      this.setAttribute('aria-hidden', this.active ? 'false' : 'true')
    }
    render() {
      return C`
      <slot
        part="base"
        class=${ct({
          'tab-panel': !0,
          'tab-panel--active': this.active,
        })}
      ></slot>
    `
    }
  }
Ii.styles = [dt, W$]
c([p({ reflect: !0 })], Ii.prototype, 'name', 2)
c([p({ type: Boolean, reflect: !0 })], Ii.prototype, 'active', 2)
c([X('active')], Ii.prototype, 'handleActiveChange', 1)
const Y$ = `:host {
  --tab-panel-font-size: var(--font-size);

  width: 100%;
  height: 100%;
}
[part='base'] {
  width: 100%;
  height: 100%;
  padding-top: 0;
  padding-bottom: 0;
}
`
class cp extends dr(Ii, nn) {
  constructor() {
    super()
    W(this, 'componentId', `tbk-tab-panel-${this.attrId}`)
    this.size = Lt.M
  }
}
W(cp, 'styles', [Ii.styles, Bt(), ge(), je('tab-panel', 1.5, 3), ft(Y$)]),
  W(cp, 'properties', {
    ...Ii.properties,
    size: { type: String, reflect: !0 },
  })
const K$ = `:host {
  --datetime-color: var(--color-gray-800);
  --datetime-background-timezone: var(--color-gray-10);

  display: inline-flex;
  align-items: center;
  white-space: nowrap;
}
:host-context([mode='dark']) {
  --datetime-color: var(--color-gray-200);
}
[part='timezone'] {
  line-height: 1;
  background: var(--datetime-background-timezone);
  border-radius: calc(var(--font-size) / 3);
  font-weight: var(--text-black);
  padding: calc(var(--step)) calc(var(--step) + var(--step-3) / 4) calc(var(--step) - var(--half))
    var(--step-2);
  text-transform: uppercase;
  font-size: calc(var(--font-size) * 0.8);
}
[part='date'] {
  padding: calc(var(--step)) var(--step-2) calc(var(--step) - var(--half)) var(--step-2);
  font-weight: var(--text-semibold);
}
[part='time'] {
  margin: 0;
  padding: calc(var(--step)) var(--step-2) calc(var(--step) - var(--half)) 0;
}
tbk-badge::part(base) {
  color: var(--datetime-color);
  padding: 0;
  font-variant-numeric: slashed-zero;
  font-weight: var(--text-light);
}
`
function G$(t) {
  const r = {
    timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    timeZoneName: 'short',
  }
  return new Intl.DateTimeFormat(X$(), r)
    .formatToParts(t)
    .find(i => i.type === 'timeZoneName').value
}
function X$() {
  var t
  return (
    (typeof window < 'u' &&
      window.navigator &&
      (((t = window.navigator.languages) == null ? void 0 : t[0]) ||
        window.navigator.language ||
        window.navigator.userLanguage ||
        window.navigator.browserLanguage)) ||
    'en-US'
  )
}
class dp extends dr(de, pk) {
  constructor() {
    super()
    W(this, '_defaultFormat', 'YYYY-MM-DD HH:mm:ss')
    ;(this.label = ''),
      (this.date = Date.now()),
      (this.format = this._defaultFormat),
      (this.size = Lt.XXS),
      (this.side = Jt.Right),
      (this.realtime = !1),
      (this.hideTimezone = !1),
      (this.hideDate = !1)
  }
  get hasTime() {
    const i = this.format.toLowerCase()
    return i.includes('h') || i.includes('m') || i.includes('s')
  }
  firstUpdated() {
    super.firstUpdated()
    try {
      this._timestamp = jp(+this.date)
        ? +this.date
        : new Date(this.date).getTime()
    } catch {
      throw new Error('Invalid date format')
    }
    this.realtime &&
      (this._interval = setInterval(() => {
        this._timestamp = Date.now()
      }, 1e3))
  }
  willUpdate(i) {
    i.has('timezone') &&
      (this._displayTimezone = this.isUTC
        ? Dr.title(Dr.UTC)
        : G$(new Date(this._timestamp)))
  }
  renderTimezone() {
    return lt(
      It(this.hideTimezone),
      C`<span part="timezone">${this._displayTimezone}</span>`,
    )
  }
  renderContent() {
    let i = []
    if (this.format === this._defaultFormat) {
      const [o, a] = (
        this.isUTC
          ? $u(this._timestamp, this.format)
          : Cu(this._timestamp, this.format)
      ).split(' ')
      i = [
        C`<span part="date">${o}</span>`,
        a && C`<span part="time">${a}</span>`,
      ].filter(Boolean)
    } else {
      const o = this.isUTC
        ? $u(this._timestamp, this.format)
        : Cu(this._timestamp, this.format)
      i = [C`<span part="date">${o}</span>`]
    }
    return C`${i}`
  }
  render() {
    return C`<tbk-badge
      title="${this._timestamp}"
      size="${this.size}"
      variant="${$t.Neutral}"
    >
      ${lt(this.side === Jt.Left, this.renderTimezone())}
      ${lt(It(this.hideDate), this.renderContent())}
      ${lt(this.side === Jt.Right, this.renderTimezone())}
    </tbk-badge>`
  }
}
W(dp, 'styles', [Bt(), ft(K$)]),
  W(dp, 'properties', {
    date: { type: [String, Number], reflect: !0 },
    size: { type: String, reflect: !0 },
    side: { type: String, reflect: !0 },
    realtime: { type: Boolean, reflect: !0 },
    format: { type: String },
    hideTimezone: { type: Boolean, attribute: 'hide-timezone' },
    hideDate: { type: Boolean, attribute: 'hide-date' },
    _timestamp: { type: Number, state: !0 },
    _displayTimezone: { type: String, state: !0 },
  })
const j$ = `:host {
  margin-right: calc(var(--side-navigation-font-size) * 3.25);

  display: flex;
  height: 100%;
}
:host([short]:not(:focus)) [part='base'] {
  max-width: calc(var(--side-navigation-font-size) * 3.25);
}
[part='base'] {
  width: 100%;
  height: 100%;
  overflow: hidden;
  backdrop-filter: blur(var(--step-6));
  max-width: var(--media-xs);
  position: absolute;
  z-index: var(--layer-high);
  transition: max-width 0.25s cubic-bezier(0.19, 1, 0.22, 1);
}
::slotted(*) {
  padding: var(--step-2) 0;
  background: var(--color-pacific-5);
}
`
class hp extends de {
  constructor() {
    super(), (this.short = !1)
  }
  connectedCallback() {
    super.connectedCallback(),
      this.addEventListener('mouseover', () => this.toggle(!1)),
      this.addEventListener('mouseleave', () => this.toggle(!0)),
      requestAnimationFrame(() => this.toggle(!0))
  }
  willUpdate(r) {
    super.willUpdate(r),
      (r.has('short') || r.has('size')) && this._toggleChildren()
  }
  toggle(r) {
    this.short = Qt(r) ? It(this.short) : r
  }
  _toggleChildren() {
    for (const r of this.elsSlotted)
      (r.short = this.short), (r.size = this.size ?? r.size)
  }
  _handleSlotChange(r) {
    r.stopPropagation(), this._toggleChildren()
  }
  render() {
    return C`
      ${lt(It(this.short), C`<slot name="header"></slot>`)}
      <div part="base">
        <slot @slotchange="${sn.debounce(
          this._handleSlotChange.bind(this),
          200,
        )}"></slot>
      </div>
      ${lt(It(this.short), C`<slot name="footer"></slot>`)}
    `
  }
}
W(hp, 'styles', [
  Bt(),
  on(),
  ge('side-navigation'),
  je('side-navigation'),
  ft(j$),
]),
  W(hp, 'properties', {
    short: { type: Boolean, reflect: !0 },
    size: { type: String, reflect: !0 },
  })
const Z$ = `:host([outline]) {
  background: transparent;
  border: var(--metric-border-width) solid var(--color-border) !important;
}
:host([size='xs']) {
  --metric-padding: var(--step) var(--step-4) var(--step-2);
  --metric-border-radius: var(--radius-s);
  --metric-value-size: var(--text-header);
  --metric-tagline-size: var(--text-subtitle);
  --metric-delta-size: var(--text-xxs);
  --metric-delta-padding: var(--half) var(--step-2);
  --metric-delta-radius: var(--radius-xs);
  --metric-badge-size: var(--text-xs);
}
:host([size='s']) {
  --metric-padding: var(--step-2) var(--step-4);
  --metric-border-radius: var(--radius-s);
  --metric-value-size: var(--text-header);
  --metric-tagline-size: var(--text-title);
  --metric-delta-size: var(--text-xxs);
  --metric-delta-padding: var(--half) var(--step-2);
  --metric-delta-radius: var(--radius-xxs);
  --metric-badge-size: var(--text-xs);
}
:host([size='m']) {
  --metric-padding: var(--step) var(--step-5) var(--step-2);
  --metric-border-radius: var(--radius-m);
  --metric-value-size: var(--text-display);
  --metric-tagline-size: var(--text-tagline);
  --metric-delta-size: var(--text-xs);
  --metric-delta-padding: var(--step) var(--step-2);
  --metric-delta-radius: var(--radius-xs);
  --metric-badge-size: var(--text-m);
}
:host([size='l']) {
  --metric-padding: var(--step-2) var(--step-5) var(--step-3);
  --metric-border-radius: var(--radius-l);
  --metric-value-size: var(--text-headline);
  --metric-tagline-size: var(--text-tagline);
  --metric-delta-size: var(--text-xs);
  --metric-delta-padding: var(--step) var(--step-2);
  --metric-delta-radius: var(--radius-xs);
  --metric-badge-size: var(--text-l);
}
:host([size='xl']) {
  --metric-padding: var(--step-2) var(--step-6) var(--step-4);
  --metric-border-radius: var(--radius-xl);
  --metric-value-size: var(--text-headline);
  --metric-tagline-size: var(--text-header);
  --metric-delta-size: var(--text-m);
  --metric-delta-padding: var(--step) var(--step-3);
  --metric-delta-radius: var(--radius-s);
  --metric-badge-size: var(--text-xl);
}
:host([shadow]) {
  box-shadow: var(--shadow-s);
}
:host {
  --metric-color-value: var(--color-variant);
  --metric-border-width: var(--half);
  --metric-margin: 0;
  --metric-color-text: var(--color-text);

  display: inline-flex;
  flex-direction: column;
  background: var(--metric-background, inherit);
  padding: var(--metric-padding);
  border-radius: var(--metric-border-radius);
  color: var(--metric-color-text);
  border: var(--metric-border-width) solid transparent !important;
  margin: var(--metric-margin);
  overflow: hidden;
}
[part='header'] {
  display: flex;
  align-items: baseline;
  margin-top: var(--step-2);
}
[part='title'] {
  flex: 1;
}
[part='base'] {
  width: 100%;
  display: flex;
  align-items: center;
  font-weight: var(--font-weight);
  overflow: hidden;
}
[part='container'] {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
}
[part='tagline'] {
  font-size: var(--text-xs);
  color: var(--color-tagline);
  white-space: wrap;
  height: var(--text-xs);
}
[part='section'] {
  width: 100%;
  height: 100%;
  position: relative;
  color: var(--color-title);
  margin-top: var(--step-2);
  display: flex;
  flex-direction: column;
}
[part='content'] {
  display: flex;
  align-items: center;
}
[part='value'] {
  color: var(--metric-color-value);
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
  font-size: var(--metric-value-size);
  margin-right: var(--step-2);
}
[part='value-container'] {
  display: flex;
  align-items: baseline;
}
[part='prefix-suffix'] {
  font-size: var(--font-xs);
  margin: 0 var(--step);
}
[part='text'] {
  font-size: var(--metric-tagline-size);
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
[part='explore'] {
  display: flex;
  align-items: center;
  color: var(--color-link);
  margin-left: var(--step-2);
}
[part='explore'] a {
  color: inherit;
  font-size: clamp(var(--text-s), calc(var(--metric-tagline-size) * 0.5), var(--text-m));
  padding: var(--half) 0;
  text-decoration: none;
  border-bottom: var(--one) solid transparent;
}
[part='explore'] span {
  font-size: var(--text-s);
  margin-left: var(--step);
}
[part='explore'] a:hover {
  border-bottom: var(--one) solid var(--color-link);
}
[part='delta'] {
  position: absolute;
  font-weight: var(--text-black);
  top: calc(var(--step-3) * -1);
  right: 0;
  background: var(--color-gray-5);
  padding: var(--metric-delta-padding);
  border-radius: var(--metric-delta-radius);
  font-size: var(--metric-delta-size);
  display: flex;
  justify-content: center;
  align-items: center;
}
[part='delta'].--up {
  color: var(--color-success);
  background: var(--color-success-light);
}
[part='delta'].--down {
  color: var(--color-danger);
  background: var(--color-danger-light);
}
[part='delta-value'] {
  margin: 0 var(--step);
}
[part='points'] {
  padding: 0;
  white-space: nowrap;
  border-collapse: collapse;
}
[part='points'] span {
  font-size: var(--text-xs);
  color: var(--color-tagline);
}
[part='subtitle'] {
  margin-top: var(--step);
  font-size: var(--text-xs);
  color: var(--color-tagline);
  white-space: nowrap;
}
slot[name='description'],
::slotted([slot='description']) {
  white-space: normal;
  overflow: hidden;
}
::slotted([slot='badge']) {
  margin: var(--step) 0;
}
[part='footer'] {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  border-top: var(--one) solid var(--color-divider);
  margin-top: var(--step-2);
  padding: var(--step-2) 0 0;
}
[part='action'] {
  font-size: var(--text-xs);
}
[part='action'] a {
  text-decoration: none;
  padding: var(--half) 0;
  margin: 0;
}
[part='action'] a:hover {
  color: var(--color-text-hover);
  border-bottom: var(--one) solid var(--color-divider);
}
`
class up extends de {
  constructor() {
    super(),
      (this.size = Lt.L),
      (this.variant = $t.Neutral),
      (this.outline = !1),
      (this.shadow = !1)
  }
  render() {
    return C`
      <div part="base">
        ${lt(
          this.value || this.text || this.points || this.name,
          C`
            <div part="container">
              ${this._renderHeader()}
              <div part="section">
                <div part="content">
                  ${this._renderValueContainer()}
                  <slot name="badge"></slot>
                  ${this._renderText()} ${this._renderDelta()}
                </div>
                ${this._renderPoints()} ${this._renderSubtitle()}
              </div>
            </div>
          `,
        )}
        <slot name="visual"></slot>
      </div>
      ${this._renderDescription()} ${this._renderFooter()}
    `
  }
  _renderHeader() {
    return lt(
      this.name || this.tagline,
      C`
        <div part="header">
          <div part="title">${this._renderName()} ${this._renderTagline()}</div>
          <slot name="controls"></slot>
        </div>
      `,
    )
  }
  _renderName() {
    return lt(
      this.name,
      C`<tbk-information
        part="name"
        text="${this.info}"
      >
        <slot
          name="tooltip-content"
          slot="content"
        ></slot>
        ${this.name}
      </tbk-information>`,
    )
  }
  _renderTagline() {
    return lt(this.tagline, C` <div part="tagline">${this.tagline}</div> `)
  }
  _renderValueContainer() {
    return C`
      <div part="value-container">
        ${lt(
          this.valuePrefix,
          C`<span part="prefix-suffix">${this.valuePrefix}</span>`,
        )}
        ${lt(this.value, C`<div part="value">${this.value}</div>`)}
        ${lt(
          this.valueSuffix,
          C`<span part="prefix-suffix">${this.valueSuffix}</span>`,
        )}
      </div>
    `
  }
  _renderText() {
    return lt(
      this.text || this.explore,
      C`
        <div part="text">
          <span>${this.text}</span>
          ${this._renderExplore()}
        </div>
      `,
    )
  }
  _renderExplore() {
    return lt(
      this.explore,
      C`
        <div part="explore">
          <a href="${this.explore}">Explore</a>
          <span>&rarr;</span>
        </div>
      `,
    )
  }
  _renderDelta() {
    return lt(
      Kr(this.delta),
      C`
        <div
          part="delta"
          class="${this.delta === 0 ? '' : this.delta > 0 ? '--up' : '--down'}"
        >
          ${lt(this.deltaPrefix, C`<span>${this.deltaPrefix}</span>`)}
          ${lt(this.delta === 0, C`<span part="delta-value">No Change</span>`)}
          ${lt(
            this.delta > 0,
            C`
              <tbk-icon
                library="heroicons"
                name="chevron-up"
              ></tbk-icon>
              <span part="delta-value">${this.delta}</span>
            `,
          )}
          ${lt(
            this.delta < 0,
            C`
              <tbk-icon
                library="heroicons"
                name="chevron-down"
              ></tbk-icon>
              <span part="delta-value">${this.delta}</span>
            `,
          )}
          ${lt(this.deltaSuffix, C`<span>${this.deltaSuffix}</span>`)}
        </div>
      `,
    )
  }
  _renderPoints() {
    var r
    return C`
      <slot name="points">
        ${lt(
          this.points,
          C`
            <table part="points">
              ${
                (r = this.points) == null
                  ? void 0
                  : r.map(
                      ([i, o]) => C`
                  <tr>
                    <td><span>${i}:&nbsp;</span></td>
                    <td><b>${o}</b></td>
                  </tr>
                `,
                    )
              }
            </table>
          `,
        )}
      </slot>
    `
  }
  _renderSubtitle() {
    return lt(this.subtitle, C`<div part="subtitle">${this.subtitle}</div>`)
  }
  _renderDescription() {
    return C`<slot name="description">${this.description}</slot>`
  }
  _renderFooter() {
    return lt(
      this.action_href,
      C`
        <div part="footer">
          <span part="action">
            <a href="${this.action_href}">${this.action ?? 'Explore'}</a>
            <span>&rarr;</span>
          </span>
        </div>
      `,
    )
  }
}
W(up, 'styles', [Bt(), Xr(), ft(Z$)]),
  W(up, 'properties', {
    value: { type: String },
    valuePrefix: { type: String, attribute: 'value-prefix' },
    valueSuffix: { type: String, attribute: 'value-suffix' },
    delta: { type: Number },
    deltaPrefix: { type: String, attribute: 'delta-prefix' },
    deltaSuffix: { type: String, attribute: 'delta-suffix' },
    info: { type: String },
    name: { type: String },
    tagline: { type: String },
    text: { type: String },
    subtitle: { type: String },
    action: { type: String },
    action_href: { type: String },
    explore: { type: String },
    points: { type: Array },
    description: { type: String },
    variant: { type: String, reflect: !0 },
    outline: { type: Boolean, reflect: !0 },
    shadow: { type: Boolean, reflect: !0 },
    size: { type: String, reflect: !0 },
  })
const J$ = `:host {
  display: inline-flex;
}
tbk-metric {
  display: inline-block;
  width: 100%;
  height: 100%;
}
tbk-metric::part(base) {
  flex-direction: column;
}
[part='visual'] {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: var(--step-2) 0;
}
[part='legend'] {
  display: flex;
  width: 100%;
  margin: 0;
  padding: 0;
  list-style: none;
  font-size: var(--text-xs);
  margin-top: var(--step);
  white-space: nowrap;
}
[part='legend'] li {
  margin-right: var(--step-2);
}
[part='legend'] a,
[part='visual'] a {
  text-decoration: none;
}
[part='legend'] a:hover,
[part='visual'] a:hover {
  text-decoration: underline;
}
[part='visual'] a {
  text-decoration: none;
  width: 100%;
  height: var(--step-3);
  background: var(--color-gray-200);
  transition: transform 0.2s ease-in;
}
[part='visual'] a:nth-child(1) {
  background: var(--color-scarlet-500);
}
[part='visual'] a:nth-child(2) {
  background: var(--color-mandarin-500);
}
[part='visual'] a:nth-child(3) {
  background: var(--color-emerald-500);
}
[part='visual'] a:hover {
  transform: scale(1.05);
}
`
class pp extends de {
  get total() {
    return this.complete + this.pending + this.behind
  }
  constructor() {
    super(),
      (this.size = Lt.M),
      (this.complete = 0),
      (this.pending = 0),
      (this.behind = 0)
  }
  render() {
    const r = `${this.url}#behind`,
      i = `${this.url}#pending`,
      o = `${this.url}#complete`
    return C`
      <tbk-metric
        part="base"
        name="${this.name}"
        tagline="${this.tagline}"
        info="${this.info}"
        size="${this.size}"
        subtitle="${this.subtitle}"
        action_href="${this.explore}"
        outline
      >
        <ul
          slot="visual"
          part="legend"
        >
          <li>Behind: <a href="${r}">${this.behind}</a></li>
          <li>Pending: <a href="${i}">${this.pending}</a></li>
          <li>Complete: <a href="${o}">${this.complete}</a></li>
        </ul>
        <div
          slot="visual"
          part="visual"
        >
          <a
            href="${r}"
            title="Behind: ${this.behind}"
            style="width:${(this.behind / this.total) * 100}%"
          ></a>
          <a
            href="${i}"
            title="Pending: ${this.pending}"
            style="width:${(this.pending / this.total) * 100}%"
          ></a>
          <a
            href="${o}"
            title="Complete: ${this.complete}"
            style="width:${(this.complete / this.total) * 100}%"
          ></a>
        </div>
        <slot
          name="description"
          slot="description"
        ></slot>
      </tbk-metric>
    `
  }
}
W(pp, 'styles', [Bt(), Xr(), ft(J$)]),
  W(pp, 'properties', {
    url: { type: String },
    info: { type: String },
    name: { type: String },
    tagline: { type: String },
    subtitle: { type: String },
    explore: { type: String },
    complete: { type: Number },
    pending: { type: Number },
    behind: { type: Number },
    size: { type: String, reflect: !0 },
  })
const Q$ = `:host {
  width: 100%;
  display: block;
}
:host([disabled]) [part='label'] {
  opacity: 0.75;
}
:host(:focus),
:host([disabled]),
:host([disabled]) * {
  outline: none;
}
[part='tooltip'] > tbk-button,
tbk-icon-loading {
  position: absolute;
}
:host([side='right']) [part='tooltip'] > tbk-button,
:host([side='left'][loading]:not([info=''])) tbk-icon-loading,
:host([side='right'][loading][info='']) tbk-icon-loading {
  right: 0;
  transform: translateX(115%);
}
:host([side='left']) [part='tooltip'] > tbk-button,
:host([side='right'][loading]:not([info=''])) tbk-icon-loading,
:host([side='left'][loading][info='']) tbk-icon-loading {
  left: 0;
  transform: translateX(-115%);
}
[part='base'] {
  display: flex;
}
:host([placement='right']) [part='base'],
:host([placement='left']) [part='base'] {
  flex-direction: row;
  gap: var(--step);

  ::slotted(*:not([slot='label'])) {
    width: 100%;
    margin: var(--half) 0;
  }
}
:host([placement='top']) [part='base'],
:host([placement='bottom']) [part='base'] {
  flex-direction: column;
}
[part='label'] {
  display: flex;
  flex-direction: column;
  gap: var(--step);
  padding: 0 var(--step);
}
:host([placement='right']) [part='label'],
:host([placement='left']) [part='label'] {
  gap: var(--step);
}
[part='content'] {
  display: flex;
  gap: var(--step);
  align-items: center;
  position: relative;
}
[part='extra'] {
  display: flex;
  flex-direction: column;
  gap: var(--step);
}
slot:empty {
  position: absolute;
}
::slotted(*:not([slot='label'])) {
  width: 100%;
  margin: var(--step) 0;
}
slot[name='description'],
::slotted([slot='description']),
slot[name='tagline'],
::slotted([slot='tagline']) {
  white-space: wrap;
  display: inline-block;
  font-size: var(--text-xs);
  color: var(--color-gray-400);
  line-height: 1;
}
slot[name='description'],
::slotted([slot='description']) {
  padding: 0 var(--step);
}
[part='errors'],
[part='requirements'] {
  display: flex;
  border-radius: var(--radius-xs);
  font-size: var(--text-xs);
}
[part='errors'] {
  background: var(--color-scarlet-5);
  color: var(--color-scarlet-600);
}
[part='requirements'] {
  background: var(--color-gray-5);
  color: var(--color-gray-600);
}
::slotted([slot='errors']),
::slotted([slot='requirements']) {
  display: inline-flex;
  flex-direction: column;
  padding: var(--step-2) var(--step-3);
  gap: var(--step);
  margin: 0;
  list-style: none;
}
tbk-tooltip::part(base__popup) {
  color: var(--tooltip-text);
  font-size: var(--text-xs);
  font-weight: var(--font-weight);
  background: var(--tooltip-background);
  padding: var(--step-2) var(--step-3);
  white-space: wrap;
  border-radius: var(--tooltip-border-radius);
  box-shadow: var(--tooltip-shadow);
  z-index: var(--layer-high);
}
`
class fp extends de {
  constructor() {
    super(), (this.width = 20), (this.inverse = !1)
  }
  render() {
    return C`
      <svg
        xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 100 100"
        width="${this.width}"
        height="${this.width}"
      >
        <g fill="${
          this.inverse ? 'var(--color-light)' : 'var(--color-gray-500)'
        }">
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(0 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(30 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.083s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(60 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.167s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(90 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.25s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(120 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.333s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(150 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.417s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(180 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.5s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(210 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.583s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(240 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.667s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(270 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.75s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(300 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.833s"
            />
          </rect>
          <rect
            x="45"
            y="8"
            width="10"
            height="24"
            rx="2"
            ry="2"
            transform="rotate(330 50 50)"
          >
            <animate
              attributeName="opacity"
              from="1"
              to="0.1"
              dur="1s"
              repeatCount="indefinite"
              begin="0.917s"
            />
          </rect>
        </g>
      </svg>
    `
  }
}
W(fp, 'styles', [
  Bt(),
  st`
      :host {
        display: inline-block;
      }
      svg {
        display: block;
      }
    `,
]),
  W(fp, 'properties', {
    width: { type: Number },
    inverse: { type: Boolean, reflect: !0 },
  })
class gp extends de {
  constructor() {
    super(),
      (this.description = ''),
      (this.tagline = ''),
      (this.info = ''),
      (this.side = Jt.Left),
      (this.size = Lt.S),
      (this.placement = qr.Top),
      (this.loading = !1),
      (this.disabled = void 0),
      (this.required = void 0)
  }
  willUpdate(r) {
    super.willUpdate(r),
      (r.has('disabled') || r.has('required') || r.has('size')) &&
        Array.from(this.children).forEach(i => {
          ;(i.disabled = Qt(this.disabled) ? i.disabled : this.disabled),
            (i.required = Qt(this.required) ? i.required : this.required),
            i.nodeName.startsWith('TBK-') && (i.size = this.size)
        })
  }
  showLoading(r = 0) {
    setTimeout(() => {
      this.loading = !0
    }, r)
  }
  hideLoading(r = 0) {
    setTimeout(() => {
      this.loading = !1
    }, r)
  }
  _renderInfo() {
    return lt(
      this.info,
      C`
        <tbk-tooltip
          part="tooltip"
          trigger="focus hover"
          skidding="0"
          distance="0"
          content="${this.info}"
          placement="right"
          tabindex="${this.disabled ? -1 : 0}"
          hoist
        >
          <tbk-button
            icon
            size="2xs"
            shape="circle"
            variant="transparent"
            readonly
          >
            <tbk-icon
              library="heroicons"
              name="question-mark-circle"
            ></tbk-icon>
          </tbk-button>
        </tbk-tooltip>
      `,
    )
  }
  _renderLabel() {
    return C`
      <div part="label">
        <slot name="label"></slot>
        <slot name="tagline">${this.tagline}</slot>
        ${lt(
          [qr.Left, qr.Right].includes(this.placement),
          C`
            <div part="extra">
              <slot name="description">${this.description}</slot>
              <div part="errors">
                <slot name="errors"></slot>
              </div>
              <div part="requirements">
                <slot name="requirements"></slot>
              </div>
            </div>
            <slot name="additional"></slot>
          `,
        )}
      </div>
    `
  }
  _renderLoading() {
    return lt(this.loading, C`<tbk-icon-loading></tbk-icon-loading>`)
  }
  render() {
    const r = tC(this.side, !!this.info)
    return C`
      <div part="base">
        ${lt([qr.Left, qr.Top].includes(this.placement), this._renderLabel())}
        <div part="container">
          <div part="content">
            ${lt(this.side === Jt.Left, this._renderInfo())}
            ${lt(r === Jt.Left, this._renderLoading())}
            <slot></slot>
            ${lt(r === Jt.Right, this._renderLoading())}
            ${lt(this.side === Jt.Right, this._renderInfo())}
          </div>
          ${lt(
            [qr.Top, qr.Bottom].includes(this.placement),
            C`
              <div part="extra">
                <slot name="description">${this.description}</slot>
                <div part="errors">
                  <slot name="errors"></slot>
                </div>
                <div part="requirements">
                  <slot name="requirements"></slot>
                </div>
              </div>
              <slot name="additional"></slot>
            `,
          )}
        </div>
        ${lt(
          [qr.Right, qr.Bottom].includes(this.placement),
          this._renderLabel(),
        )}
      </div>
    `
  }
}
W(gp, 'styles', [ef(), xc(), ft(Q$)]),
  W(gp, 'properties', {
    description: { type: String },
    tagline: { type: String },
    info: { type: String, reflect: !0 },
    placement: { type: String, reflect: !0 },
    side: { type: String, reflect: !0 },
    size: { type: String, reflect: !0 },
    disabled: { type: Boolean, reflect: !0 },
    required: { type: Boolean, reflect: !0 },
    loading: { type: Boolean, reflect: !0 },
  })
function tC(t = Jt.Left, r = !1) {
  return t === Jt.Left && r ? Jt.Right : t === Jt.Right && r ? Jt.Left : t
}
const eC = `:host {
  --fieldset-background: var(--color-variant-lucid);
}
:host([orientation='horizontal']) [part='base'] {
  flex-direction: row;
}
:host([orientation='horizontal']) [part='legend'] {
  display: block;
  flex: 1 2 100%;
}
:host([orientation='horizontal']) [part='content'] {
  flex: 1 1 100%;
}
:host([orientation='vertical']) [part='base'] {
  flex-direction: column;
}
:host([side='left']) [part='content'] {
  align-items: flex-start;
}
:host([side='right']) [part='content'] {
  align-items: flex-end;
}
:host([side='center']) [part='content'] {
  align-items: flex-start;
}
:host([side='left']) [part='content'] {
  justify-content: flex-start;
}
:host([side='right']) [part='content'] {
  justify-content: flex-end;
}
:host([side='center']) [part='content'] {
  justify-content: center;
}
:host([inert]) {
  --fieldset-background: var(--color-gray-5);
}
[part='base'] {
  display: flex;
  border-radius: var(--radius-s);
  background: var(--fieldset-background);
  padding: var(--step-2);
  cursor: unset;
}
[part='legend'],
[part='content'] {
  display: flex;
  flex-direction: column;
  gap: var(--step);
  width: 100%;
}
slot:empty {
  position: absolute;
}
:host([size='2xs']) [part='base'] {
  gap: var(--half);
}
:host([size='xs']) [part='base'] {
  gap: var(--step);
}
:host([size='s']) [part='base'] {
  gap: var(--step-2);
}
:host([size='m']) [part='base'] {
  gap: var(--step-4);
}
:host([size='l']) [part='base'] {
  gap: var(--step-6);
}
:host([size='xl']) [part='base'] {
  gap: var(--step-8);
}
:host([size='2xl']) [part='base'] {
  gap: var(--step-10);
}
slot[name='description'],
::slotted([slot='description']) {
  display: block;
  white-space: wrap;
  font-size: var(--text-xs);
  color: var(--color-gray-400);
  line-height: 1;
}
`
class mp extends de {
  constructor() {
    super(),
      (this.legend = ''),
      (this.orientation = vc.Vertical),
      (this.side = Jt.Left),
      (this.size = Lt.S),
      (this.variant = $t.Transparent),
      (this.inert = void 0)
  }
  willUpdate(r) {
    super.willUpdate(r),
      (r.has('inert') || r.has('required')) &&
        (Kr(this.inert) || Kr(this.required)) &&
        Array.from(this.children).forEach(i => {
          i.disabled = Qt(this.inert) ? i.disabled : this.inert
        })
  }
  render() {
    return C`
      <div part="base">
        <div part="legend">
          <slot name="legend">${this.legend}</slot>
          <slot name="description">${this.description}</slot>
        </div>
        <div part="content">
          <slot></slot>
        </div>
      </div>
    `
  }
}
W(mp, 'styles', [Bt(), Xr(), ft(eC)]),
  W(mp, 'properties', {
    legend: { type: String },
    description: { type: String },
    orientation: { type: String, reflect: !0 },
    side: { type: String, reflect: !0 },
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    inert: { type: Boolean, reflect: !0 },
  })
var rC = st`
  :host {
    display: block;
  }

  .input {
    flex: 1 1 auto;
    display: inline-flex;
    align-items: stretch;
    justify-content: start;
    position: relative;
    width: 100%;
    font-family: var(--sl-input-font-family);
    font-weight: var(--sl-input-font-weight);
    letter-spacing: var(--sl-input-letter-spacing);
    vertical-align: middle;
    overflow: hidden;
    cursor: text;
    transition:
      var(--sl-transition-fast) color,
      var(--sl-transition-fast) border,
      var(--sl-transition-fast) box-shadow,
      var(--sl-transition-fast) background-color;
  }

  /* Standard inputs */
  .input--standard {
    background-color: var(--sl-input-background-color);
    border: solid var(--sl-input-border-width) var(--sl-input-border-color);
  }

  .input--standard:hover:not(.input--disabled) {
    background-color: var(--sl-input-background-color-hover);
    border-color: var(--sl-input-border-color-hover);
  }

  .input--standard.input--focused:not(.input--disabled) {
    background-color: var(--sl-input-background-color-focus);
    border-color: var(--sl-input-border-color-focus);
    box-shadow: 0 0 0 var(--sl-focus-ring-width) var(--sl-input-focus-ring-color);
  }

  .input--standard.input--focused:not(.input--disabled) .input__control {
    color: var(--sl-input-color-focus);
  }

  .input--standard.input--disabled {
    background-color: var(--sl-input-background-color-disabled);
    border-color: var(--sl-input-border-color-disabled);
    opacity: 0.5;
    cursor: not-allowed;
  }

  .input--standard.input--disabled .input__control {
    color: var(--sl-input-color-disabled);
  }

  .input--standard.input--disabled .input__control::placeholder {
    color: var(--sl-input-placeholder-color-disabled);
  }

  /* Filled inputs */
  .input--filled {
    border: none;
    background-color: var(--sl-input-filled-background-color);
    color: var(--sl-input-color);
  }

  .input--filled:hover:not(.input--disabled) {
    background-color: var(--sl-input-filled-background-color-hover);
  }

  .input--filled.input--focused:not(.input--disabled) {
    background-color: var(--sl-input-filled-background-color-focus);
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .input--filled.input--disabled {
    background-color: var(--sl-input-filled-background-color-disabled);
    opacity: 0.5;
    cursor: not-allowed;
  }

  .input__control {
    flex: 1 1 auto;
    font-family: inherit;
    font-size: inherit;
    font-weight: inherit;
    min-width: 0;
    height: 100%;
    color: var(--sl-input-color);
    border: none;
    background: inherit;
    box-shadow: none;
    padding: 0;
    margin: 0;
    cursor: inherit;
    -webkit-appearance: none;
  }

  .input__control::-webkit-search-decoration,
  .input__control::-webkit-search-cancel-button,
  .input__control::-webkit-search-results-button,
  .input__control::-webkit-search-results-decoration {
    -webkit-appearance: none;
  }

  .input__control:-webkit-autofill,
  .input__control:-webkit-autofill:hover,
  .input__control:-webkit-autofill:focus,
  .input__control:-webkit-autofill:active {
    box-shadow: 0 0 0 var(--sl-input-height-large) var(--sl-input-background-color-hover) inset !important;
    -webkit-text-fill-color: var(--sl-color-primary-500);
    caret-color: var(--sl-input-color);
  }

  .input--filled .input__control:-webkit-autofill,
  .input--filled .input__control:-webkit-autofill:hover,
  .input--filled .input__control:-webkit-autofill:focus,
  .input--filled .input__control:-webkit-autofill:active {
    box-shadow: 0 0 0 var(--sl-input-height-large) var(--sl-input-filled-background-color) inset !important;
  }

  .input__control::placeholder {
    color: var(--sl-input-placeholder-color);
    user-select: none;
    -webkit-user-select: none;
  }

  .input:hover:not(.input--disabled) .input__control {
    color: var(--sl-input-color-hover);
  }

  .input__control:focus {
    outline: none;
  }

  .input__prefix,
  .input__suffix {
    display: inline-flex;
    flex: 0 0 auto;
    align-items: center;
    cursor: default;
  }

  .input__prefix ::slotted(sl-icon),
  .input__suffix ::slotted(sl-icon) {
    color: var(--sl-input-icon-color);
  }

  /*
   * Size modifiers
   */

  .input--small {
    border-radius: var(--sl-input-border-radius-small);
    font-size: var(--sl-input-font-size-small);
    height: var(--sl-input-height-small);
  }

  .input--small .input__control {
    height: calc(var(--sl-input-height-small) - var(--sl-input-border-width) * 2);
    padding: 0 var(--sl-input-spacing-small);
  }

  .input--small .input__clear,
  .input--small .input__password-toggle {
    width: calc(1em + var(--sl-input-spacing-small) * 2);
  }

  .input--small .input__prefix ::slotted(*) {
    margin-inline-start: var(--sl-input-spacing-small);
  }

  .input--small .input__suffix ::slotted(*) {
    margin-inline-end: var(--sl-input-spacing-small);
  }

  .input--medium {
    border-radius: var(--sl-input-border-radius-medium);
    font-size: var(--sl-input-font-size-medium);
    height: var(--sl-input-height-medium);
  }

  .input--medium .input__control {
    height: calc(var(--sl-input-height-medium) - var(--sl-input-border-width) * 2);
    padding: 0 var(--sl-input-spacing-medium);
  }

  .input--medium .input__clear,
  .input--medium .input__password-toggle {
    width: calc(1em + var(--sl-input-spacing-medium) * 2);
  }

  .input--medium .input__prefix ::slotted(*) {
    margin-inline-start: var(--sl-input-spacing-medium);
  }

  .input--medium .input__suffix ::slotted(*) {
    margin-inline-end: var(--sl-input-spacing-medium);
  }

  .input--large {
    border-radius: var(--sl-input-border-radius-large);
    font-size: var(--sl-input-font-size-large);
    height: var(--sl-input-height-large);
  }

  .input--large .input__control {
    height: calc(var(--sl-input-height-large) - var(--sl-input-border-width) * 2);
    padding: 0 var(--sl-input-spacing-large);
  }

  .input--large .input__clear,
  .input--large .input__password-toggle {
    width: calc(1em + var(--sl-input-spacing-large) * 2);
  }

  .input--large .input__prefix ::slotted(*) {
    margin-inline-start: var(--sl-input-spacing-large);
  }

  .input--large .input__suffix ::slotted(*) {
    margin-inline-end: var(--sl-input-spacing-large);
  }

  /*
   * Pill modifier
   */

  .input--pill.input--small {
    border-radius: var(--sl-input-height-small);
  }

  .input--pill.input--medium {
    border-radius: var(--sl-input-height-medium);
  }

  .input--pill.input--large {
    border-radius: var(--sl-input-height-large);
  }

  /*
   * Clearable + Password Toggle
   */

  .input__clear,
  .input__password-toggle {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: inherit;
    color: var(--sl-input-icon-color);
    border: none;
    background: none;
    padding: 0;
    transition: var(--sl-transition-fast) color;
    cursor: pointer;
  }

  .input__clear:hover,
  .input__password-toggle:hover {
    color: var(--sl-input-icon-color-hover);
  }

  .input__clear:focus,
  .input__password-toggle:focus {
    outline: none;
  }

  /* Don't show the browser's password toggle in Edge */
  ::-ms-reveal {
    display: none;
  }

  /* Hide the built-in number spinner */
  .input--no-spin-buttons input[type='number']::-webkit-outer-spin-button,
  .input--no-spin-buttons input[type='number']::-webkit-inner-spin-button {
    -webkit-appearance: none;
    display: none;
  }

  .input--no-spin-buttons input[type='number'] {
    -moz-appearance: textfield;
  }
`,
  an =
    (t = 'value') =>
    (r, i) => {
      const o = r.constructor,
        a = o.prototype.attributeChangedCallback
      o.prototype.attributeChangedCallback = function (d, h, m) {
        var f
        const b = o.getPropertyOptions(t),
          A = typeof b.attribute == 'string' ? b.attribute : t
        if (d === A) {
          const y = b.converter || Ln,
            S = (
              typeof y == 'function'
                ? y
                : (f = y == null ? void 0 : y.fromAttribute) != null
                ? f
                : Ln.fromAttribute
            )(m, b.type)
          this[t] !== S && (this[i] = S)
        }
        a.call(this, d, h, m)
      }
    },
  ln = st`
  .form-control .form-control__label {
    display: none;
  }

  .form-control .form-control__help-text {
    display: none;
  }

  /* Label */
  .form-control--has-label .form-control__label {
    display: inline-block;
    color: var(--sl-input-label-color);
    margin-bottom: var(--sl-spacing-3x-small);
  }

  .form-control--has-label.form-control--small .form-control__label {
    font-size: var(--sl-input-label-font-size-small);
  }

  .form-control--has-label.form-control--medium .form-control__label {
    font-size: var(--sl-input-label-font-size-medium);
  }

  .form-control--has-label.form-control--large .form-control__label {
    font-size: var(--sl-input-label-font-size-large);
  }

  :host([required]) .form-control--has-label .form-control__label::after {
    content: var(--sl-input-required-content);
    margin-inline-start: var(--sl-input-required-content-offset);
    color: var(--sl-input-required-content-color);
  }

  /* Help text */
  .form-control--has-help-text .form-control__help-text {
    display: block;
    color: var(--sl-input-help-text-color);
    margin-top: var(--sl-spacing-3x-small);
  }

  .form-control--has-help-text.form-control--small .form-control__help-text {
    font-size: var(--sl-input-help-text-font-size-small);
  }

  .form-control--has-help-text.form-control--medium .form-control__help-text {
    font-size: var(--sl-input-help-text-font-size-medium);
  }

  .form-control--has-help-text.form-control--large .form-control__help-text {
    font-size: var(--sl-input-help-text-font-size-large);
  }

  .form-control--has-help-text.form-control--radio-group .form-control__help-text {
    margin-top: var(--sl-spacing-2x-small);
  }
`,
  fo = /* @__PURE__ */ new WeakMap(),
  go = /* @__PURE__ */ new WeakMap(),
  mo = /* @__PURE__ */ new WeakMap(),
  Yl = /* @__PURE__ */ new WeakSet(),
  Ms = /* @__PURE__ */ new WeakMap(),
  mi = class {
    constructor(t, r) {
      ;(this.handleFormData = i => {
        const o = this.options.disabled(this.host),
          a = this.options.name(this.host),
          d = this.options.value(this.host),
          h = this.host.tagName.toLowerCase() === 'sl-button'
        this.host.isConnected &&
          !o &&
          !h &&
          typeof a == 'string' &&
          a.length > 0 &&
          typeof d < 'u' &&
          (Array.isArray(d)
            ? d.forEach(m => {
                i.formData.append(a, m.toString())
              })
            : i.formData.append(a, d.toString()))
      }),
        (this.handleFormSubmit = i => {
          var o
          const a = this.options.disabled(this.host),
            d = this.options.reportValidity
          this.form &&
            !this.form.noValidate &&
            ((o = fo.get(this.form)) == null ||
              o.forEach(h => {
                this.setUserInteracted(h, !0)
              })),
            this.form &&
              !this.form.noValidate &&
              !a &&
              !d(this.host) &&
              (i.preventDefault(), i.stopImmediatePropagation())
        }),
        (this.handleFormReset = () => {
          this.options.setValue(
            this.host,
            this.options.defaultValue(this.host),
          ),
            this.setUserInteracted(this.host, !1),
            Ms.set(this.host, [])
        }),
        (this.handleInteraction = i => {
          const o = Ms.get(this.host)
          o.includes(i.type) || o.push(i.type),
            o.length === this.options.assumeInteractionOn.length &&
              this.setUserInteracted(this.host, !0)
        }),
        (this.checkFormValidity = () => {
          if (this.form && !this.form.noValidate) {
            const i = this.form.querySelectorAll('*')
            for (const o of i)
              if (typeof o.checkValidity == 'function' && !o.checkValidity())
                return !1
          }
          return !0
        }),
        (this.reportFormValidity = () => {
          if (this.form && !this.form.noValidate) {
            const i = this.form.querySelectorAll('*')
            for (const o of i)
              if (typeof o.reportValidity == 'function' && !o.reportValidity())
                return !1
          }
          return !0
        }),
        (this.host = t).addController(this),
        (this.options = pi(
          {
            form: i => {
              const o = i.form
              if (o) {
                const d = i.getRootNode().querySelector(`#${o}`)
                if (d) return d
              }
              return i.closest('form')
            },
            name: i => i.name,
            value: i => i.value,
            defaultValue: i => i.defaultValue,
            disabled: i => {
              var o
              return (o = i.disabled) != null ? o : !1
            },
            reportValidity: i =>
              typeof i.reportValidity == 'function' ? i.reportValidity() : !0,
            checkValidity: i =>
              typeof i.checkValidity == 'function' ? i.checkValidity() : !0,
            setValue: (i, o) => (i.value = o),
            assumeInteractionOn: ['sl-input'],
          },
          r,
        ))
    }
    hostConnected() {
      const t = this.options.form(this.host)
      t && this.attachForm(t),
        Ms.set(this.host, []),
        this.options.assumeInteractionOn.forEach(r => {
          this.host.addEventListener(r, this.handleInteraction)
        })
    }
    hostDisconnected() {
      this.detachForm(),
        Ms.delete(this.host),
        this.options.assumeInteractionOn.forEach(t => {
          this.host.removeEventListener(t, this.handleInteraction)
        })
    }
    hostUpdated() {
      const t = this.options.form(this.host)
      t || this.detachForm(),
        t && this.form !== t && (this.detachForm(), this.attachForm(t)),
        this.host.hasUpdated && this.setValidity(this.host.validity.valid)
    }
    attachForm(t) {
      t
        ? ((this.form = t),
          fo.has(this.form)
            ? fo.get(this.form).add(this.host)
            : fo.set(this.form, /* @__PURE__ */ new Set([this.host])),
          this.form.addEventListener('formdata', this.handleFormData),
          this.form.addEventListener('submit', this.handleFormSubmit),
          this.form.addEventListener('reset', this.handleFormReset),
          go.has(this.form) ||
            (go.set(this.form, this.form.reportValidity),
            (this.form.reportValidity = () => this.reportFormValidity())),
          mo.has(this.form) ||
            (mo.set(this.form, this.form.checkValidity),
            (this.form.checkValidity = () => this.checkFormValidity())))
        : (this.form = void 0)
    }
    detachForm() {
      if (!this.form) return
      const t = fo.get(this.form)
      t &&
        (t.delete(this.host),
        t.size <= 0 &&
          (this.form.removeEventListener('formdata', this.handleFormData),
          this.form.removeEventListener('submit', this.handleFormSubmit),
          this.form.removeEventListener('reset', this.handleFormReset),
          go.has(this.form) &&
            ((this.form.reportValidity = go.get(this.form)),
            go.delete(this.form)),
          mo.has(this.form) &&
            ((this.form.checkValidity = mo.get(this.form)),
            mo.delete(this.form)),
          (this.form = void 0)))
    }
    setUserInteracted(t, r) {
      r ? Yl.add(t) : Yl.delete(t), t.requestUpdate()
    }
    doAction(t, r) {
      if (this.form) {
        const i = document.createElement('button')
        ;(i.type = t),
          (i.style.position = 'absolute'),
          (i.style.width = '0'),
          (i.style.height = '0'),
          (i.style.clipPath = 'inset(50%)'),
          (i.style.overflow = 'hidden'),
          (i.style.whiteSpace = 'nowrap'),
          r &&
            ((i.name = r.name),
            (i.value = r.value),
            [
              'formaction',
              'formenctype',
              'formmethod',
              'formnovalidate',
              'formtarget',
            ].forEach(o => {
              r.hasAttribute(o) && i.setAttribute(o, r.getAttribute(o))
            })),
          this.form.append(i),
          i.click(),
          i.remove()
      }
    }
    /** Returns the associated `<form>` element, if one exists. */
    getForm() {
      var t
      return (t = this.form) != null ? t : null
    }
    /** Resets the form, restoring all the control to their default value */
    reset(t) {
      this.doAction('reset', t)
    }
    /** Submits the form, triggering validation and form data injection. */
    submit(t) {
      this.doAction('submit', t)
    }
    /**
     * Synchronously sets the form control's validity. Call this when you know the future validity but need to update
     * the host element immediately, i.e. before Lit updates the component in the next update.
     */
    setValidity(t) {
      const r = this.host,
        i = !!Yl.has(r),
        o = !!r.required
      r.toggleAttribute('data-required', o),
        r.toggleAttribute('data-optional', !o),
        r.toggleAttribute('data-invalid', !t),
        r.toggleAttribute('data-valid', t),
        r.toggleAttribute('data-user-invalid', !t && i),
        r.toggleAttribute('data-user-valid', t && i)
    }
    /**
     * Updates the form control's validity based on the current value of `host.validity.valid`. Call this when anything
     * that affects constraint validation changes so the component receives the correct validity states.
     */
    updateValidity() {
      const t = this.host
      this.setValidity(t.validity.valid)
    }
    /**
     * Dispatches a non-bubbling, cancelable custom event of type `sl-invalid`.
     * If the `sl-invalid` event will be cancelled then the original `invalid`
     * event (which may have been passed as argument) will also be cancelled.
     * If no original `invalid` event has been passed then the `sl-invalid`
     * event will be cancelled before being dispatched.
     */
    emitInvalidEvent(t) {
      const r = new CustomEvent('sl-invalid', {
        bubbles: !1,
        composed: !1,
        cancelable: !0,
        detail: {},
      })
      t || r.preventDefault(),
        this.host.dispatchEvent(r) || t == null || t.preventDefault()
    }
  },
  da = Object.freeze({
    badInput: !1,
    customError: !1,
    patternMismatch: !1,
    rangeOverflow: !1,
    rangeUnderflow: !1,
    stepMismatch: !1,
    tooLong: !1,
    tooShort: !1,
    typeMismatch: !1,
    valid: !0,
    valueMissing: !1,
  }),
  iC = Object.freeze(
    Lo(pi({}, da), {
      valid: !1,
      valueMissing: !0,
    }),
  ),
  nC = Object.freeze(
    Lo(pi({}, da), {
      valid: !1,
      customError: !0,
    }),
  ),
  He = class {
    constructor(t, ...r) {
      ;(this.slotNames = []),
        (this.handleSlotChange = i => {
          const o = i.target
          ;((this.slotNames.includes('[default]') && !o.name) ||
            (o.name && this.slotNames.includes(o.name))) &&
            this.host.requestUpdate()
        }),
        (this.host = t).addController(this),
        (this.slotNames = r)
    }
    hasDefaultSlot() {
      return [...this.host.childNodes].some(t => {
        if (t.nodeType === t.TEXT_NODE && t.textContent.trim() !== '') return !0
        if (t.nodeType === t.ELEMENT_NODE) {
          const r = t
          if (r.tagName.toLowerCase() === 'sl-visually-hidden') return !1
          if (!r.hasAttribute('slot')) return !0
        }
        return !1
      })
    }
    hasNamedSlot(t) {
      return this.host.querySelector(`:scope > [slot="${t}"]`) !== null
    }
    test(t) {
      return t === '[default]' ? this.hasDefaultSlot() : this.hasNamedSlot(t)
    }
    hostConnected() {
      this.host.shadowRoot.addEventListener('slotchange', this.handleSlotChange)
    }
    hostDisconnected() {
      this.host.shadowRoot.removeEventListener(
        'slotchange',
        this.handleSlotChange,
      )
    }
  }
function oC(t) {
  if (!t) return ''
  const r = t.assignedNodes({ flatten: !0 })
  let i = ''
  return (
    [...r].forEach(o => {
      o.nodeType === Node.TEXT_NODE && (i += o.textContent)
    }),
    i
  )
}
/**
 * @license
 * Copyright 2020 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const rn = Bo(
  class extends Fo {
    constructor(t) {
      if (
        (super(t),
        t.type !== Yr.PROPERTY &&
          t.type !== Yr.ATTRIBUTE &&
          t.type !== Yr.BOOLEAN_ATTRIBUTE)
      )
        throw Error(
          'The `live` directive is not allowed on child or event bindings',
        )
      if (!Hp(t))
        throw Error('`live` bindings can only contain a single expression')
    }
    render(t) {
      return t
    }
    update(t, [r]) {
      if (r === lr || r === Gt) return r
      const i = t.element,
        o = t.name
      if (t.type === Yr.PROPERTY) {
        if (r === i[o]) return lr
      } else if (t.type === Yr.BOOLEAN_ATTRIBUTE) {
        if (!!r === i.hasAttribute(o)) return lr
      } else if (t.type === Yr.ATTRIBUTE && i.getAttribute(o) === r + '')
        return lr
      return nw(t), r
    }
  },
)
var bt = class extends nt {
  constructor() {
    super(...arguments),
      (this.formControlController = new mi(this, {
        assumeInteractionOn: ['sl-blur', 'sl-input'],
      })),
      (this.hasSlotController = new He(this, 'help-text', 'label')),
      (this.localize = new Dt(this)),
      (this.hasFocus = !1),
      (this.title = ''),
      (this.__numberInput = Object.assign(document.createElement('input'), {
        type: 'number',
      })),
      (this.__dateInput = Object.assign(document.createElement('input'), {
        type: 'date',
      })),
      (this.type = 'text'),
      (this.name = ''),
      (this.value = ''),
      (this.defaultValue = ''),
      (this.size = 'medium'),
      (this.filled = !1),
      (this.pill = !1),
      (this.label = ''),
      (this.helpText = ''),
      (this.clearable = !1),
      (this.disabled = !1),
      (this.placeholder = ''),
      (this.readonly = !1),
      (this.passwordToggle = !1),
      (this.passwordVisible = !1),
      (this.noSpinButtons = !1),
      (this.form = ''),
      (this.required = !1),
      (this.spellcheck = !0)
  }
  //
  // NOTE: We use an in-memory input for these getters/setters instead of the one in the template because the properties
  // can be set before the component is rendered.
  //
  /**
   * Gets or sets the current value as a `Date` object. Returns `null` if the value can't be converted. This will use the native `<input type="{{type}}">` implementation and may result in an error.
   */
  get valueAsDate() {
    var t
    return (
      (this.__dateInput.type = this.type),
      (this.__dateInput.value = this.value),
      ((t = this.input) == null ? void 0 : t.valueAsDate) ||
        this.__dateInput.valueAsDate
    )
  }
  set valueAsDate(t) {
    ;(this.__dateInput.type = this.type),
      (this.__dateInput.valueAsDate = t),
      (this.value = this.__dateInput.value)
  }
  /** Gets or sets the current value as a number. Returns `NaN` if the value can't be converted. */
  get valueAsNumber() {
    var t
    return (
      (this.__numberInput.value = this.value),
      ((t = this.input) == null ? void 0 : t.valueAsNumber) ||
        this.__numberInput.valueAsNumber
    )
  }
  set valueAsNumber(t) {
    ;(this.__numberInput.valueAsNumber = t),
      (this.value = this.__numberInput.value)
  }
  /** Gets the validity state object */
  get validity() {
    return this.input.validity
  }
  /** Gets the validation message */
  get validationMessage() {
    return this.input.validationMessage
  }
  firstUpdated() {
    this.formControlController.updateValidity()
  }
  handleBlur() {
    ;(this.hasFocus = !1), this.emit('sl-blur')
  }
  handleChange() {
    ;(this.value = this.input.value), this.emit('sl-change')
  }
  handleClearClick(t) {
    t.preventDefault(),
      this.value !== '' &&
        ((this.value = ''),
        this.emit('sl-clear'),
        this.emit('sl-input'),
        this.emit('sl-change')),
      this.input.focus()
  }
  handleFocus() {
    ;(this.hasFocus = !0), this.emit('sl-focus')
  }
  handleInput() {
    ;(this.value = this.input.value),
      this.formControlController.updateValidity(),
      this.emit('sl-input')
  }
  handleInvalid(t) {
    this.formControlController.setValidity(!1),
      this.formControlController.emitInvalidEvent(t)
  }
  handleKeyDown(t) {
    const r = t.metaKey || t.ctrlKey || t.shiftKey || t.altKey
    t.key === 'Enter' &&
      !r &&
      setTimeout(() => {
        !t.defaultPrevented &&
          !t.isComposing &&
          this.formControlController.submit()
      })
  }
  handlePasswordToggle() {
    this.passwordVisible = !this.passwordVisible
  }
  handleDisabledChange() {
    this.formControlController.setValidity(this.disabled)
  }
  handleStepChange() {
    ;(this.input.step = String(this.step)),
      this.formControlController.updateValidity()
  }
  async handleValueChange() {
    await this.updateComplete, this.formControlController.updateValidity()
  }
  /** Sets focus on the input. */
  focus(t) {
    this.input.focus(t)
  }
  /** Removes focus from the input. */
  blur() {
    this.input.blur()
  }
  /** Selects all the text in the input. */
  select() {
    this.input.select()
  }
  /** Sets the start and end positions of the text selection (0-based). */
  setSelectionRange(t, r, i = 'none') {
    this.input.setSelectionRange(t, r, i)
  }
  /** Replaces a range of text with a new string. */
  setRangeText(t, r, i, o = 'preserve') {
    const a = r ?? this.input.selectionStart,
      d = i ?? this.input.selectionEnd
    this.input.setRangeText(t, a, d, o),
      this.value !== this.input.value && (this.value = this.input.value)
  }
  /** Displays the browser picker for an input element (only works if the browser supports it for the input type). */
  showPicker() {
    'showPicker' in HTMLInputElement.prototype && this.input.showPicker()
  }
  /** Increments the value of a numeric input type by the value of the step attribute. */
  stepUp() {
    this.input.stepUp(),
      this.value !== this.input.value && (this.value = this.input.value)
  }
  /** Decrements the value of a numeric input type by the value of the step attribute. */
  stepDown() {
    this.input.stepDown(),
      this.value !== this.input.value && (this.value = this.input.value)
  }
  /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
  checkValidity() {
    return this.input.checkValidity()
  }
  /** Gets the associated form, if one exists. */
  getForm() {
    return this.formControlController.getForm()
  }
  /** Checks for validity and shows the browser's validation message if the control is invalid. */
  reportValidity() {
    return this.input.reportValidity()
  }
  /** Sets a custom validation message. Pass an empty string to restore validity. */
  setCustomValidity(t) {
    this.input.setCustomValidity(t), this.formControlController.updateValidity()
  }
  render() {
    const t = this.hasSlotController.test('label'),
      r = this.hasSlotController.test('help-text'),
      i = this.label ? !0 : !!t,
      o = this.helpText ? !0 : !!r,
      d =
        this.clearable &&
        !this.disabled &&
        !this.readonly &&
        (typeof this.value == 'number' || this.value.length > 0)
    return C`
      <div
        part="form-control"
        class=${ct({
          'form-control': !0,
          'form-control--small': this.size === 'small',
          'form-control--medium': this.size === 'medium',
          'form-control--large': this.size === 'large',
          'form-control--has-label': i,
          'form-control--has-help-text': o,
        })}
      >
        <label
          part="form-control-label"
          class="form-control__label"
          for="input"
          aria-hidden=${i ? 'false' : 'true'}
        >
          <slot name="label">${this.label}</slot>
        </label>

        <div part="form-control-input" class="form-control-input">
          <div
            part="base"
            class=${ct({
              input: !0,
              // Sizes
              'input--small': this.size === 'small',
              'input--medium': this.size === 'medium',
              'input--large': this.size === 'large',
              // States
              'input--pill': this.pill,
              'input--standard': !this.filled,
              'input--filled': this.filled,
              'input--disabled': this.disabled,
              'input--focused': this.hasFocus,
              'input--empty': !this.value,
              'input--no-spin-buttons': this.noSpinButtons,
            })}
          >
            <span part="prefix" class="input__prefix">
              <slot name="prefix"></slot>
            </span>

            <input
              part="input"
              id="input"
              class="input__control"
              type=${
                this.type === 'password' && this.passwordVisible
                  ? 'text'
                  : this.type
              }
              title=${this.title}
              name=${it(this.name)}
              ?disabled=${this.disabled}
              ?readonly=${this.readonly}
              ?required=${this.required}
              placeholder=${it(this.placeholder)}
              minlength=${it(this.minlength)}
              maxlength=${it(this.maxlength)}
              min=${it(this.min)}
              max=${it(this.max)}
              step=${it(this.step)}
              .value=${rn(this.value)}
              autocapitalize=${it(this.autocapitalize)}
              autocomplete=${it(this.autocomplete)}
              autocorrect=${it(this.autocorrect)}
              ?autofocus=${this.autofocus}
              spellcheck=${this.spellcheck}
              pattern=${it(this.pattern)}
              enterkeyhint=${it(this.enterkeyhint)}
              inputmode=${it(this.inputmode)}
              aria-describedby="help-text"
              @change=${this.handleChange}
              @input=${this.handleInput}
              @invalid=${this.handleInvalid}
              @keydown=${this.handleKeyDown}
              @focus=${this.handleFocus}
              @blur=${this.handleBlur}
            />

            ${
              d
                ? C`
                  <button
                    part="clear-button"
                    class="input__clear"
                    type="button"
                    aria-label=${this.localize.term('clearEntry')}
                    @click=${this.handleClearClick}
                    tabindex="-1"
                  >
                    <slot name="clear-icon">
                      <sl-icon name="x-circle-fill" library="system"></sl-icon>
                    </slot>
                  </button>
                `
                : ''
            }
            ${
              this.passwordToggle && !this.disabled
                ? C`
                  <button
                    part="password-toggle-button"
                    class="input__password-toggle"
                    type="button"
                    aria-label=${this.localize.term(
                      this.passwordVisible ? 'hidePassword' : 'showPassword',
                    )}
                    @click=${this.handlePasswordToggle}
                    tabindex="-1"
                  >
                    ${
                      this.passwordVisible
                        ? C`
                          <slot name="show-password-icon">
                            <sl-icon name="eye-slash" library="system"></sl-icon>
                          </slot>
                        `
                        : C`
                          <slot name="hide-password-icon">
                            <sl-icon name="eye" library="system"></sl-icon>
                          </slot>
                        `
                    }
                  </button>
                `
                : ''
            }

            <span part="suffix" class="input__suffix">
              <slot name="suffix"></slot>
            </span>
          </div>
        </div>

        <div
          part="form-control-help-text"
          id="help-text"
          class="form-control__help-text"
          aria-hidden=${o ? 'false' : 'true'}
        >
          <slot name="help-text">${this.helpText}</slot>
        </div>
      </div>
    `
  }
}
bt.styles = [dt, ln, rC]
bt.dependencies = { 'sl-icon': Nt }
c([J('.input__control')], bt.prototype, 'input', 2)
c([at()], bt.prototype, 'hasFocus', 2)
c([p()], bt.prototype, 'title', 2)
c([p({ reflect: !0 })], bt.prototype, 'type', 2)
c([p()], bt.prototype, 'name', 2)
c([p()], bt.prototype, 'value', 2)
c([an()], bt.prototype, 'defaultValue', 2)
c([p({ reflect: !0 })], bt.prototype, 'size', 2)
c([p({ type: Boolean, reflect: !0 })], bt.prototype, 'filled', 2)
c([p({ type: Boolean, reflect: !0 })], bt.prototype, 'pill', 2)
c([p()], bt.prototype, 'label', 2)
c([p({ attribute: 'help-text' })], bt.prototype, 'helpText', 2)
c([p({ type: Boolean })], bt.prototype, 'clearable', 2)
c([p({ type: Boolean, reflect: !0 })], bt.prototype, 'disabled', 2)
c([p()], bt.prototype, 'placeholder', 2)
c([p({ type: Boolean, reflect: !0 })], bt.prototype, 'readonly', 2)
c(
  [p({ attribute: 'password-toggle', type: Boolean })],
  bt.prototype,
  'passwordToggle',
  2,
)
c(
  [p({ attribute: 'password-visible', type: Boolean })],
  bt.prototype,
  'passwordVisible',
  2,
)
c(
  [p({ attribute: 'no-spin-buttons', type: Boolean })],
  bt.prototype,
  'noSpinButtons',
  2,
)
c([p({ reflect: !0 })], bt.prototype, 'form', 2)
c([p({ type: Boolean, reflect: !0 })], bt.prototype, 'required', 2)
c([p()], bt.prototype, 'pattern', 2)
c([p({ type: Number })], bt.prototype, 'minlength', 2)
c([p({ type: Number })], bt.prototype, 'maxlength', 2)
c([p()], bt.prototype, 'min', 2)
c([p()], bt.prototype, 'max', 2)
c([p()], bt.prototype, 'step', 2)
c([p()], bt.prototype, 'autocapitalize', 2)
c([p()], bt.prototype, 'autocorrect', 2)
c([p()], bt.prototype, 'autocomplete', 2)
c([p({ type: Boolean })], bt.prototype, 'autofocus', 2)
c([p()], bt.prototype, 'enterkeyhint', 2)
c(
  [
    p({
      type: Boolean,
      converter: {
        // Allow "true|false" attribute values but keep the property boolean
        fromAttribute: t => !(!t || t === 'false'),
        toAttribute: t => (t ? 'true' : 'false'),
      },
    }),
  ],
  bt.prototype,
  'spellcheck',
  2,
)
c([p()], bt.prototype, 'inputmode', 2)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  bt.prototype,
  'handleDisabledChange',
  1,
)
c(
  [X('step', { waitUntilFirstUpdate: !0 })],
  bt.prototype,
  'handleStepChange',
  1,
)
c(
  [X('value', { waitUntilFirstUpdate: !0 })],
  bt.prototype,
  'handleValueChange',
  1,
)
const sC = `:host {
  --sl-input-placeholder-color: var(--input-placeholder-color);
  --input-height: auto;
  --input-width: 100%;
  --input-background: var(--color-white);

  display: inline-flex;
  border-radius: var(--input-radius);
  font-family: var(--input-font-family);
  text-align: var(--input-text-align, inherit);
  color: var(--input-color);
  background: var(--input-background);
  box-shadow: var(--input-box-shadow);
}
:host([error]) {
  color: var(--from-input-color-error);
}
:host([disabled]) {
  --input-background: var(--color-gray-125) !important;
  --input-color: var(--color-gray-600) !important;
  --input-box-shadow: none !important;
}
[part='base'] {
  display: flex;
  align-items: center;
  border: 0;
  line-height: 1;
  background: inherit;
  color: inherit;
  box-shadow: inherit;
  text-align: inherit;
  white-space: nowrap;
  width: var(--input-width);
  height: var(--input-height);
  font-size: var(--input-font-size);
  border-radius: var(--input-radius);
  padding: var(--input-padding-y) var(--input-padding-x);
}
[part='form-control'] {
  width: 100%;
  text-align: inherit;
  font-family: inherit;
}
[part='base'].input--standard.input--disabled {
  border: inherit;
  opacity: 1;
  cursor: inherit;
  background-color: inherit;
}
[part='base']:hover:not(.input--disabled) .input__control,
[part='base']:focus:not(.input--disabled) .input__control,
[part='input'] {
  height: auto;
}
[part='base'].input--standard:hover:not(.input--disabled),
[part='base']:hover:not(.input--disabled) {
  box-shadow: var(--input-box-shadow-hover);
  color: var(--input-color);
}
[part='base'].input--focused:not(.input--disabled),
[part='base']:focus:not(.input--disabled) {
  box-shadow: var(--input-box-shadow-focus);
  color: var(--input-color);
}
[part='base'] .input__control {
  background: transparent;
  font-weight: inherit;
  color: inherit;
}
::slotted([slot='prefix']) {
  margin-right: var(--step-2);
}
::slotted([slot='suffix']) {
  margin-left: var(--step-2);
}
`,
  vp = ce({
    Text: 'text',
    Password: 'password',
    Email: 'email',
    Number: 'number',
    Search: 'search',
    Tel: 'tel',
    Url: 'url',
    Date: 'date',
    Time: 'time',
    DatetimeLocal: 'datetime-local',
    Month: 'month',
    Week: 'week',
    File: 'file',
    Color: 'color',
  })
class bp extends dr(bt, nn) {
  constructor() {
    super(),
      (this.size = Lt.M),
      (this.type = vp.Text),
      (this.shape = Be.Round),
      (this.variant = $t.Neutral),
      (this.outline = !1),
      (this.shadow = !1),
      (this.disabled = !1),
      (this.required = !1),
      (this.error = !1)
  }
  connectedCallback() {
    super.connectedCallback(),
      this.addEventListener('sl-clear', () => {
        this.emit('clear')
      }),
      this.addEventListener('click', r => {
        r.stopPropagation(), r.stopImmediatePropagation()
      }),
      this.addEventListener('input', r => {
        if (this.type === vp.Number) {
          const i = Number(r.target.value)
          this.required &&
            (Kr(this.min) && i < this.min
              ? (this.value = this.min)
              : Kr(this.max) && i > this.max && (this.value = this.max))
        }
      })
  }
  get elFormControlInputBase() {
    return this.renderRoot.querySelector(ji.PartBase)
  }
  get elFocusable() {
    return this.renderRoot.querySelector('[part="input"]')
  }
  focus(r) {
    var i
    ;(i = this.input) == null || i.focus(r)
  }
  blur() {
    var r
    ;(r = this.input) == null || r.blur()
  }
}
W(bp, 'styles', [
  bt.styles,
  Bt(),
  on('.input--focused'),
  aa('input'),
  ge(),
  fi('input'),
  je('input', 1.25, 2),
  ft(sC),
]),
  W(bp, 'properties', {
    ...bt.properties,
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
    side: { type: String, reflect: !0 },
    outline: { type: Boolean, reflect: !0 },
    shadow: { type: Boolean, reflect: !0 },
    ghost: { type: Boolean, reflect: !0 },
    disabled: { type: Boolean, reflect: !0 },
    required: { type: Boolean, reflect: !0 },
    error: { type: Boolean, reflect: !0 },
  })
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
let ac = class extends Fo {
  constructor(r) {
    if ((super(r), (this.it = Gt), r.type !== Yr.CHILD))
      throw Error(
        this.constructor.directiveName +
          '() can only be used in child bindings',
      )
  }
  render(r) {
    if (r === Gt || r == null) return (this._t = void 0), (this.it = r)
    if (r === lr) return r
    if (typeof r != 'string')
      throw Error(
        this.constructor.directiveName + '() called with a non-string value',
      )
    if (r === this.it) return this._t
    this.it = r
    const i = [r]
    return (
      (i.raw = i),
      (this._t = {
        _$litType$: this.constructor.resultType,
        strings: i,
        values: [],
      })
    )
  }
}
;(ac.directiveName = 'unsafeHTML'), (ac.resultType = 1)
const Co = Bo(ac)
var aC = st`
  :host {
    display: inline-block;
  }

  .tag {
    display: flex;
    align-items: center;
    border: solid 1px;
    line-height: 1;
    white-space: nowrap;
    user-select: none;
    -webkit-user-select: none;
  }

  .tag__remove::part(base) {
    color: inherit;
    padding: 0;
  }

  /*
   * Variant modifiers
   */

  .tag--primary {
    background-color: var(--sl-color-primary-50);
    border-color: var(--sl-color-primary-200);
    color: var(--sl-color-primary-800);
  }

  .tag--primary:active > sl-icon-button {
    color: var(--sl-color-primary-600);
  }

  .tag--success {
    background-color: var(--sl-color-success-50);
    border-color: var(--sl-color-success-200);
    color: var(--sl-color-success-800);
  }

  .tag--success:active > sl-icon-button {
    color: var(--sl-color-success-600);
  }

  .tag--neutral {
    background-color: var(--sl-color-neutral-50);
    border-color: var(--sl-color-neutral-200);
    color: var(--sl-color-neutral-800);
  }

  .tag--neutral:active > sl-icon-button {
    color: var(--sl-color-neutral-600);
  }

  .tag--warning {
    background-color: var(--sl-color-warning-50);
    border-color: var(--sl-color-warning-200);
    color: var(--sl-color-warning-800);
  }

  .tag--warning:active > sl-icon-button {
    color: var(--sl-color-warning-600);
  }

  .tag--danger {
    background-color: var(--sl-color-danger-50);
    border-color: var(--sl-color-danger-200);
    color: var(--sl-color-danger-800);
  }

  .tag--danger:active > sl-icon-button {
    color: var(--sl-color-danger-600);
  }

  /*
   * Size modifiers
   */

  .tag--small {
    font-size: var(--sl-button-font-size-small);
    height: calc(var(--sl-input-height-small) * 0.8);
    line-height: calc(var(--sl-input-height-small) - var(--sl-input-border-width) * 2);
    border-radius: var(--sl-input-border-radius-small);
    padding: 0 var(--sl-spacing-x-small);
  }

  .tag--medium {
    font-size: var(--sl-button-font-size-medium);
    height: calc(var(--sl-input-height-medium) * 0.8);
    line-height: calc(var(--sl-input-height-medium) - var(--sl-input-border-width) * 2);
    border-radius: var(--sl-input-border-radius-medium);
    padding: 0 var(--sl-spacing-small);
  }

  .tag--large {
    font-size: var(--sl-button-font-size-large);
    height: calc(var(--sl-input-height-large) * 0.8);
    line-height: calc(var(--sl-input-height-large) - var(--sl-input-border-width) * 2);
    border-radius: var(--sl-input-border-radius-large);
    padding: 0 var(--sl-spacing-medium);
  }

  .tag__remove {
    margin-inline-start: var(--sl-spacing-x-small);
  }

  /*
   * Pill modifier
   */

  .tag--pill {
    border-radius: var(--sl-border-radius-pill);
  }
`,
  vi = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.variant = 'neutral'),
        (this.size = 'medium'),
        (this.pill = !1),
        (this.removable = !1)
    }
    handleRemoveClick() {
      this.emit('sl-remove')
    }
    render() {
      return C`
      <span
        part="base"
        class=${ct({
          tag: !0,
          // Types
          'tag--primary': this.variant === 'primary',
          'tag--success': this.variant === 'success',
          'tag--neutral': this.variant === 'neutral',
          'tag--warning': this.variant === 'warning',
          'tag--danger': this.variant === 'danger',
          'tag--text': this.variant === 'text',
          // Sizes
          'tag--small': this.size === 'small',
          'tag--medium': this.size === 'medium',
          'tag--large': this.size === 'large',
          // Modifiers
          'tag--pill': this.pill,
          'tag--removable': this.removable,
        })}
      >
        <slot part="content" class="tag__content"></slot>

        ${
          this.removable
            ? C`
              <sl-icon-button
                part="remove-button"
                exportparts="base:remove-button__base"
                name="x-lg"
                library="system"
                label=${this.localize.term('remove')}
                class="tag__remove"
                @click=${this.handleRemoveClick}
                tabindex="-1"
              ></sl-icon-button>
            `
            : ''
        }
      </span>
    `
    }
  }
vi.styles = [dt, aC]
vi.dependencies = { 'sl-icon-button': ye }
c([p({ reflect: !0 })], vi.prototype, 'variant', 2)
c([p({ reflect: !0 })], vi.prototype, 'size', 2)
c([p({ type: Boolean, reflect: !0 })], vi.prototype, 'pill', 2)
c([p({ type: Boolean })], vi.prototype, 'removable', 2)
var lC = st`
  :host {
    display: block;
  }

  /** The popup */
  .select {
    flex: 1 1 auto;
    display: inline-flex;
    width: 100%;
    position: relative;
    vertical-align: middle;
  }

  .select::part(popup) {
    z-index: var(--sl-z-index-dropdown);
  }

  .select[data-current-placement^='top']::part(popup) {
    transform-origin: bottom;
  }

  .select[data-current-placement^='bottom']::part(popup) {
    transform-origin: top;
  }

  /* Combobox */
  .select__combobox {
    flex: 1;
    display: flex;
    width: 100%;
    min-width: 0;
    position: relative;
    align-items: center;
    justify-content: start;
    font-family: var(--sl-input-font-family);
    font-weight: var(--sl-input-font-weight);
    letter-spacing: var(--sl-input-letter-spacing);
    vertical-align: middle;
    overflow: hidden;
    cursor: pointer;
    transition:
      var(--sl-transition-fast) color,
      var(--sl-transition-fast) border,
      var(--sl-transition-fast) box-shadow,
      var(--sl-transition-fast) background-color;
  }

  .select__display-input {
    position: relative;
    width: 100%;
    font: inherit;
    border: none;
    background: none;
    color: var(--sl-input-color);
    cursor: inherit;
    overflow: hidden;
    padding: 0;
    margin: 0;
    -webkit-appearance: none;
  }

  .select__display-input::placeholder {
    color: var(--sl-input-placeholder-color);
  }

  .select:not(.select--disabled):hover .select__display-input {
    color: var(--sl-input-color-hover);
  }

  .select__display-input:focus {
    outline: none;
  }

  /* Visually hide the display input when multiple is enabled */
  .select--multiple:not(.select--placeholder-visible) .select__display-input {
    position: absolute;
    z-index: -1;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    opacity: 0;
  }

  .select__value-input {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    padding: 0;
    margin: 0;
    opacity: 0;
    z-index: -1;
  }

  .select__tags {
    display: flex;
    flex: 1;
    align-items: center;
    flex-wrap: wrap;
    margin-inline-start: var(--sl-spacing-2x-small);
  }

  .select__tags::slotted(sl-tag) {
    cursor: pointer !important;
  }

  .select--disabled .select__tags,
  .select--disabled .select__tags::slotted(sl-tag) {
    cursor: not-allowed !important;
  }

  /* Standard selects */
  .select--standard .select__combobox {
    background-color: var(--sl-input-background-color);
    border: solid var(--sl-input-border-width) var(--sl-input-border-color);
  }

  .select--standard.select--disabled .select__combobox {
    background-color: var(--sl-input-background-color-disabled);
    border-color: var(--sl-input-border-color-disabled);
    color: var(--sl-input-color-disabled);
    opacity: 0.5;
    cursor: not-allowed;
    outline: none;
  }

  .select--standard:not(.select--disabled).select--open .select__combobox,
  .select--standard:not(.select--disabled).select--focused .select__combobox {
    background-color: var(--sl-input-background-color-focus);
    border-color: var(--sl-input-border-color-focus);
    box-shadow: 0 0 0 var(--sl-focus-ring-width) var(--sl-input-focus-ring-color);
  }

  /* Filled selects */
  .select--filled .select__combobox {
    border: none;
    background-color: var(--sl-input-filled-background-color);
    color: var(--sl-input-color);
  }

  .select--filled:hover:not(.select--disabled) .select__combobox {
    background-color: var(--sl-input-filled-background-color-hover);
  }

  .select--filled.select--disabled .select__combobox {
    background-color: var(--sl-input-filled-background-color-disabled);
    opacity: 0.5;
    cursor: not-allowed;
  }

  .select--filled:not(.select--disabled).select--open .select__combobox,
  .select--filled:not(.select--disabled).select--focused .select__combobox {
    background-color: var(--sl-input-filled-background-color-focus);
    outline: var(--sl-focus-ring);
  }

  /* Sizes */
  .select--small .select__combobox {
    border-radius: var(--sl-input-border-radius-small);
    font-size: var(--sl-input-font-size-small);
    min-height: var(--sl-input-height-small);
    padding-block: 0;
    padding-inline: var(--sl-input-spacing-small);
  }

  .select--small .select__clear {
    margin-inline-start: var(--sl-input-spacing-small);
  }

  .select--small .select__prefix::slotted(*) {
    margin-inline-end: var(--sl-input-spacing-small);
  }

  .select--small.select--multiple .select__prefix::slotted(*) {
    margin-inline-start: var(--sl-input-spacing-small);
  }

  .select--small.select--multiple:not(.select--placeholder-visible) .select__combobox {
    padding-block: 2px;
    padding-inline-start: 0;
  }

  .select--small .select__tags {
    gap: 2px;
  }

  .select--medium .select__combobox {
    border-radius: var(--sl-input-border-radius-medium);
    font-size: var(--sl-input-font-size-medium);
    min-height: var(--sl-input-height-medium);
    padding-block: 0;
    padding-inline: var(--sl-input-spacing-medium);
  }

  .select--medium .select__clear {
    margin-inline-start: var(--sl-input-spacing-medium);
  }

  .select--medium .select__prefix::slotted(*) {
    margin-inline-end: var(--sl-input-spacing-medium);
  }

  .select--medium.select--multiple .select__prefix::slotted(*) {
    margin-inline-start: var(--sl-input-spacing-medium);
  }

  .select--medium.select--multiple .select__combobox {
    padding-inline-start: 0;
    padding-block: 3px;
  }

  .select--medium .select__tags {
    gap: 3px;
  }

  .select--large .select__combobox {
    border-radius: var(--sl-input-border-radius-large);
    font-size: var(--sl-input-font-size-large);
    min-height: var(--sl-input-height-large);
    padding-block: 0;
    padding-inline: var(--sl-input-spacing-large);
  }

  .select--large .select__clear {
    margin-inline-start: var(--sl-input-spacing-large);
  }

  .select--large .select__prefix::slotted(*) {
    margin-inline-end: var(--sl-input-spacing-large);
  }

  .select--large.select--multiple .select__prefix::slotted(*) {
    margin-inline-start: var(--sl-input-spacing-large);
  }

  .select--large.select--multiple .select__combobox {
    padding-inline-start: 0;
    padding-block: 4px;
  }

  .select--large .select__tags {
    gap: 4px;
  }

  /* Pills */
  .select--pill.select--small .select__combobox {
    border-radius: var(--sl-input-height-small);
  }

  .select--pill.select--medium .select__combobox {
    border-radius: var(--sl-input-height-medium);
  }

  .select--pill.select--large .select__combobox {
    border-radius: var(--sl-input-height-large);
  }

  /* Prefix and Suffix */
  .select__prefix,
  .select__suffix {
    flex: 0;
    display: inline-flex;
    align-items: center;
    color: var(--sl-input-placeholder-color);
  }

  .select__suffix::slotted(*) {
    margin-inline-start: var(--sl-spacing-small);
  }

  /* Clear button */
  .select__clear {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: inherit;
    color: var(--sl-input-icon-color);
    border: none;
    background: none;
    padding: 0;
    transition: var(--sl-transition-fast) color;
    cursor: pointer;
  }

  .select__clear:hover {
    color: var(--sl-input-icon-color-hover);
  }

  .select__clear:focus {
    outline: none;
  }

  /* Expand icon */
  .select__expand-icon {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    transition: var(--sl-transition-medium) rotate ease;
    rotate: 0;
    margin-inline-start: var(--sl-spacing-small);
  }

  .select--open .select__expand-icon {
    rotate: -180deg;
  }

  /* Listbox */
  .select__listbox {
    display: block;
    position: relative;
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-medium);
    font-weight: var(--sl-font-weight-normal);
    box-shadow: var(--sl-shadow-large);
    background: var(--sl-panel-background-color);
    border: solid var(--sl-panel-border-width) var(--sl-panel-border-color);
    border-radius: var(--sl-border-radius-medium);
    padding-block: var(--sl-spacing-x-small);
    padding-inline: 0;
    overflow: auto;
    overscroll-behavior: none;

    /* Make sure it adheres to the popup's auto size */
    max-width: var(--auto-size-available-width);
    max-height: var(--auto-size-available-height);
  }

  .select__listbox ::slotted(sl-divider) {
    --spacing: var(--sl-spacing-x-small);
  }

  .select__listbox ::slotted(small) {
    display: block;
    font-size: var(--sl-font-size-small);
    font-weight: var(--sl-font-weight-semibold);
    color: var(--sl-color-neutral-500);
    padding-block: var(--sl-spacing-2x-small);
    padding-inline: var(--sl-spacing-x-large);
  }
`,
  Ct = class extends nt {
    constructor() {
      super(...arguments),
        (this.formControlController = new mi(this, {
          assumeInteractionOn: ['sl-blur', 'sl-input'],
        })),
        (this.hasSlotController = new He(this, 'help-text', 'label')),
        (this.localize = new Dt(this)),
        (this.typeToSelectString = ''),
        (this.hasFocus = !1),
        (this.displayLabel = ''),
        (this.selectedOptions = []),
        (this.valueHasChanged = !1),
        (this.name = ''),
        (this.value = ''),
        (this.defaultValue = ''),
        (this.size = 'medium'),
        (this.placeholder = ''),
        (this.multiple = !1),
        (this.maxOptionsVisible = 3),
        (this.disabled = !1),
        (this.clearable = !1),
        (this.open = !1),
        (this.hoist = !1),
        (this.filled = !1),
        (this.pill = !1),
        (this.label = ''),
        (this.placement = 'bottom'),
        (this.helpText = ''),
        (this.form = ''),
        (this.required = !1),
        (this.getTag = t => C`
      <sl-tag
        part="tag"
        exportparts="
              base:tag__base,
              content:tag__content,
              remove-button:tag__remove-button,
              remove-button__base:tag__remove-button__base
            "
        ?pill=${this.pill}
        size=${this.size}
        removable
        @sl-remove=${r => this.handleTagRemove(r, t)}
      >
        ${t.getTextLabel()}
      </sl-tag>
    `),
        (this.handleDocumentFocusIn = t => {
          const r = t.composedPath()
          this && !r.includes(this) && this.hide()
        }),
        (this.handleDocumentKeyDown = t => {
          const r = t.target,
            i = r.closest('.select__clear') !== null,
            o = r.closest('sl-icon-button') !== null
          if (!(i || o)) {
            if (
              (t.key === 'Escape' &&
                this.open &&
                !this.closeWatcher &&
                (t.preventDefault(),
                t.stopPropagation(),
                this.hide(),
                this.displayInput.focus({ preventScroll: !0 })),
              t.key === 'Enter' ||
                (t.key === ' ' && this.typeToSelectString === ''))
            ) {
              if (
                (t.preventDefault(), t.stopImmediatePropagation(), !this.open)
              ) {
                this.show()
                return
              }
              this.currentOption &&
                !this.currentOption.disabled &&
                ((this.valueHasChanged = !0),
                this.multiple
                  ? this.toggleOptionSelection(this.currentOption)
                  : this.setSelectedOptions(this.currentOption),
                this.updateComplete.then(() => {
                  this.emit('sl-input'), this.emit('sl-change')
                }),
                this.multiple ||
                  (this.hide(), this.displayInput.focus({ preventScroll: !0 })))
              return
            }
            if (['ArrowUp', 'ArrowDown', 'Home', 'End'].includes(t.key)) {
              const a = this.getAllOptions(),
                d = a.indexOf(this.currentOption)
              let h = Math.max(0, d)
              if (
                (t.preventDefault(),
                !this.open && (this.show(), this.currentOption))
              )
                return
              t.key === 'ArrowDown'
                ? ((h = d + 1), h > a.length - 1 && (h = 0))
                : t.key === 'ArrowUp'
                ? ((h = d - 1), h < 0 && (h = a.length - 1))
                : t.key === 'Home'
                ? (h = 0)
                : t.key === 'End' && (h = a.length - 1),
                this.setCurrentOption(a[h])
            }
            if ((t.key && t.key.length === 1) || t.key === 'Backspace') {
              const a = this.getAllOptions()
              if (t.metaKey || t.ctrlKey || t.altKey) return
              if (!this.open) {
                if (t.key === 'Backspace') return
                this.show()
              }
              t.stopPropagation(),
                t.preventDefault(),
                clearTimeout(this.typeToSelectTimeout),
                (this.typeToSelectTimeout = window.setTimeout(
                  () => (this.typeToSelectString = ''),
                  1e3,
                )),
                t.key === 'Backspace'
                  ? (this.typeToSelectString = this.typeToSelectString.slice(
                      0,
                      -1,
                    ))
                  : (this.typeToSelectString += t.key.toLowerCase())
              for (const d of a)
                if (
                  d
                    .getTextLabel()
                    .toLowerCase()
                    .startsWith(this.typeToSelectString)
                ) {
                  this.setCurrentOption(d)
                  break
                }
            }
          }
        }),
        (this.handleDocumentMouseDown = t => {
          const r = t.composedPath()
          this && !r.includes(this) && this.hide()
        })
    }
    /** Gets the validity state object */
    get validity() {
      return this.valueInput.validity
    }
    /** Gets the validation message */
    get validationMessage() {
      return this.valueInput.validationMessage
    }
    connectedCallback() {
      super.connectedCallback(),
        setTimeout(() => {
          this.handleDefaultSlotChange()
        }),
        (this.open = !1)
    }
    addOpenListeners() {
      var t
      document.addEventListener('focusin', this.handleDocumentFocusIn),
        document.addEventListener('keydown', this.handleDocumentKeyDown),
        document.addEventListener('mousedown', this.handleDocumentMouseDown),
        this.getRootNode() !== document &&
          this.getRootNode().addEventListener(
            'focusin',
            this.handleDocumentFocusIn,
          ),
        'CloseWatcher' in window &&
          ((t = this.closeWatcher) == null || t.destroy(),
          (this.closeWatcher = new CloseWatcher()),
          (this.closeWatcher.onclose = () => {
            this.open &&
              (this.hide(), this.displayInput.focus({ preventScroll: !0 }))
          }))
    }
    removeOpenListeners() {
      var t
      document.removeEventListener('focusin', this.handleDocumentFocusIn),
        document.removeEventListener('keydown', this.handleDocumentKeyDown),
        document.removeEventListener('mousedown', this.handleDocumentMouseDown),
        this.getRootNode() !== document &&
          this.getRootNode().removeEventListener(
            'focusin',
            this.handleDocumentFocusIn,
          ),
        (t = this.closeWatcher) == null || t.destroy()
    }
    handleFocus() {
      ;(this.hasFocus = !0),
        this.displayInput.setSelectionRange(0, 0),
        this.emit('sl-focus')
    }
    handleBlur() {
      ;(this.hasFocus = !1), this.emit('sl-blur')
    }
    handleLabelClick() {
      this.displayInput.focus()
    }
    handleComboboxMouseDown(t) {
      const i = t
        .composedPath()
        .some(
          o =>
            o instanceof Element &&
            o.tagName.toLowerCase() === 'sl-icon-button',
        )
      this.disabled ||
        i ||
        (t.preventDefault(),
        this.displayInput.focus({ preventScroll: !0 }),
        (this.open = !this.open))
    }
    handleComboboxKeyDown(t) {
      t.key !== 'Tab' && (t.stopPropagation(), this.handleDocumentKeyDown(t))
    }
    handleClearClick(t) {
      t.stopPropagation(),
        this.value !== '' &&
          (this.setSelectedOptions([]),
          this.displayInput.focus({ preventScroll: !0 }),
          this.updateComplete.then(() => {
            this.emit('sl-clear'), this.emit('sl-input'), this.emit('sl-change')
          }))
    }
    handleClearMouseDown(t) {
      t.stopPropagation(), t.preventDefault()
    }
    handleOptionClick(t) {
      const i = t.target.closest('sl-option'),
        o = this.value
      i &&
        !i.disabled &&
        ((this.valueHasChanged = !0),
        this.multiple
          ? this.toggleOptionSelection(i)
          : this.setSelectedOptions(i),
        this.updateComplete.then(() =>
          this.displayInput.focus({ preventScroll: !0 }),
        ),
        this.value !== o &&
          this.updateComplete.then(() => {
            this.emit('sl-input'), this.emit('sl-change')
          }),
        this.multiple ||
          (this.hide(), this.displayInput.focus({ preventScroll: !0 })))
    }
    handleDefaultSlotChange() {
      customElements.get('wa-option') ||
        customElements
          .whenDefined('wa-option')
          .then(() => this.handleDefaultSlotChange())
      const t = this.getAllOptions(),
        r = this.valueHasChanged ? this.value : this.defaultValue,
        i = Array.isArray(r) ? r : [r],
        o = []
      t.forEach(a => o.push(a.value)),
        this.setSelectedOptions(t.filter(a => i.includes(a.value)))
    }
    handleTagRemove(t, r) {
      t.stopPropagation(),
        this.disabled ||
          (this.toggleOptionSelection(r, !1),
          this.updateComplete.then(() => {
            this.emit('sl-input'), this.emit('sl-change')
          }))
    }
    // Gets an array of all <sl-option> elements
    getAllOptions() {
      return [...this.querySelectorAll('sl-option')]
    }
    // Gets the first <sl-option> element
    getFirstOption() {
      return this.querySelector('sl-option')
    }
    // Sets the current option, which is the option the user is currently interacting with (e.g. via keyboard). Only one
    // option may be "current" at a time.
    setCurrentOption(t) {
      this.getAllOptions().forEach(i => {
        ;(i.current = !1), (i.tabIndex = -1)
      }),
        t &&
          ((this.currentOption = t),
          (t.current = !0),
          (t.tabIndex = 0),
          t.focus())
    }
    // Sets the selected option(s)
    setSelectedOptions(t) {
      const r = this.getAllOptions(),
        i = Array.isArray(t) ? t : [t]
      r.forEach(o => (o.selected = !1)),
        i.length && i.forEach(o => (o.selected = !0)),
        this.selectionChanged()
    }
    // Toggles an option's selected state
    toggleOptionSelection(t, r) {
      r === !0 || r === !1 ? (t.selected = r) : (t.selected = !t.selected),
        this.selectionChanged()
    }
    // This method must be called whenever the selection changes. It will update the selected options cache, the current
    // value, and the display value
    selectionChanged() {
      var t, r, i
      const o = this.getAllOptions()
      if (((this.selectedOptions = o.filter(a => a.selected)), this.multiple))
        (this.value = this.selectedOptions.map(a => a.value)),
          this.placeholder && this.value.length === 0
            ? (this.displayLabel = '')
            : (this.displayLabel = this.localize.term(
                'numOptionsSelected',
                this.selectedOptions.length,
              ))
      else {
        const a = this.selectedOptions[0]
        ;(this.value = (t = a == null ? void 0 : a.value) != null ? t : ''),
          (this.displayLabel =
            (i =
              (r = a == null ? void 0 : a.getTextLabel) == null
                ? void 0
                : r.call(a)) != null
              ? i
              : '')
      }
      this.updateComplete.then(() => {
        this.formControlController.updateValidity()
      })
    }
    get tags() {
      return this.selectedOptions.map((t, r) => {
        if (r < this.maxOptionsVisible || this.maxOptionsVisible <= 0) {
          const i = this.getTag(t, r)
          return C`<div @sl-remove=${o => this.handleTagRemove(o, t)}>
          ${typeof i == 'string' ? Co(i) : i}
        </div>`
        } else if (r === this.maxOptionsVisible)
          return C`<sl-tag size=${this.size}>+${
            this.selectedOptions.length - r
          }</sl-tag>`
        return C``
      })
    }
    handleInvalid(t) {
      this.formControlController.setValidity(!1),
        this.formControlController.emitInvalidEvent(t)
    }
    handleDisabledChange() {
      this.disabled && ((this.open = !1), this.handleOpenChange())
    }
    handleValueChange() {
      const t = this.getAllOptions(),
        r = Array.isArray(this.value) ? this.value : [this.value]
      this.setSelectedOptions(t.filter(i => r.includes(i.value)))
    }
    async handleOpenChange() {
      if (this.open && !this.disabled) {
        this.setCurrentOption(this.selectedOptions[0] || this.getFirstOption()),
          this.emit('sl-show'),
          this.addOpenListeners(),
          await pe(this),
          (this.listbox.hidden = !1),
          (this.popup.active = !0),
          requestAnimationFrame(() => {
            this.setCurrentOption(this.currentOption)
          })
        const { keyframes: t, options: r } = Xt(this, 'select.show', {
          dir: this.localize.dir(),
        })
        await ie(this.popup.popup, t, r),
          this.currentOption &&
            sc(this.currentOption, this.listbox, 'vertical', 'auto'),
          this.emit('sl-after-show')
      } else {
        this.emit('sl-hide'), this.removeOpenListeners(), await pe(this)
        const { keyframes: t, options: r } = Xt(this, 'select.hide', {
          dir: this.localize.dir(),
        })
        await ie(this.popup.popup, t, r),
          (this.listbox.hidden = !0),
          (this.popup.active = !1),
          this.emit('sl-after-hide')
      }
    }
    /** Shows the listbox. */
    async show() {
      if (this.open || this.disabled) {
        this.open = !1
        return
      }
      return (this.open = !0), Ue(this, 'sl-after-show')
    }
    /** Hides the listbox. */
    async hide() {
      if (!this.open || this.disabled) {
        this.open = !1
        return
      }
      return (this.open = !1), Ue(this, 'sl-after-hide')
    }
    /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
    checkValidity() {
      return this.valueInput.checkValidity()
    }
    /** Gets the associated form, if one exists. */
    getForm() {
      return this.formControlController.getForm()
    }
    /** Checks for validity and shows the browser's validation message if the control is invalid. */
    reportValidity() {
      return this.valueInput.reportValidity()
    }
    /** Sets a custom validation message. Pass an empty string to restore validity. */
    setCustomValidity(t) {
      this.valueInput.setCustomValidity(t),
        this.formControlController.updateValidity()
    }
    /** Sets focus on the control. */
    focus(t) {
      this.displayInput.focus(t)
    }
    /** Removes focus from the control. */
    blur() {
      this.displayInput.blur()
    }
    render() {
      const t = this.hasSlotController.test('label'),
        r = this.hasSlotController.test('help-text'),
        i = this.label ? !0 : !!t,
        o = this.helpText ? !0 : !!r,
        a = this.clearable && !this.disabled && this.value.length > 0,
        d = this.placeholder && this.value && this.value.length <= 0
      return C`
      <div
        part="form-control"
        class=${ct({
          'form-control': !0,
          'form-control--small': this.size === 'small',
          'form-control--medium': this.size === 'medium',
          'form-control--large': this.size === 'large',
          'form-control--has-label': i,
          'form-control--has-help-text': o,
        })}
      >
        <label
          id="label"
          part="form-control-label"
          class="form-control__label"
          aria-hidden=${i ? 'false' : 'true'}
          @click=${this.handleLabelClick}
        >
          <slot name="label">${this.label}</slot>
        </label>

        <div part="form-control-input" class="form-control-input">
          <sl-popup
            class=${ct({
              select: !0,
              'select--standard': !0,
              'select--filled': this.filled,
              'select--pill': this.pill,
              'select--open': this.open,
              'select--disabled': this.disabled,
              'select--multiple': this.multiple,
              'select--focused': this.hasFocus,
              'select--placeholder-visible': d,
              'select--top': this.placement === 'top',
              'select--bottom': this.placement === 'bottom',
              'select--small': this.size === 'small',
              'select--medium': this.size === 'medium',
              'select--large': this.size === 'large',
            })}
            placement=${this.placement}
            strategy=${this.hoist ? 'fixed' : 'absolute'}
            flip
            shift
            sync="width"
            auto-size="vertical"
            auto-size-padding="10"
          >
            <div
              part="combobox"
              class="select__combobox"
              slot="anchor"
              @keydown=${this.handleComboboxKeyDown}
              @mousedown=${this.handleComboboxMouseDown}
            >
              <slot part="prefix" name="prefix" class="select__prefix"></slot>

              <input
                part="display-input"
                class="select__display-input"
                type="text"
                placeholder=${this.placeholder}
                .disabled=${this.disabled}
                .value=${this.displayLabel}
                autocomplete="off"
                spellcheck="false"
                autocapitalize="off"
                readonly
                aria-controls="listbox"
                aria-expanded=${this.open ? 'true' : 'false'}
                aria-haspopup="listbox"
                aria-labelledby="label"
                aria-disabled=${this.disabled ? 'true' : 'false'}
                aria-describedby="help-text"
                role="combobox"
                tabindex="0"
                @focus=${this.handleFocus}
                @blur=${this.handleBlur}
              />

              ${
                this.multiple
                  ? C`<div part="tags" class="select__tags">${this.tags}</div>`
                  : ''
              }

              <input
                class="select__value-input"
                type="text"
                ?disabled=${this.disabled}
                ?required=${this.required}
                .value=${
                  Array.isArray(this.value) ? this.value.join(', ') : this.value
                }
                tabindex="-1"
                aria-hidden="true"
                @focus=${() => this.focus()}
                @invalid=${this.handleInvalid}
              />

              ${
                a
                  ? C`
                    <button
                      part="clear-button"
                      class="select__clear"
                      type="button"
                      aria-label=${this.localize.term('clearEntry')}
                      @mousedown=${this.handleClearMouseDown}
                      @click=${this.handleClearClick}
                      tabindex="-1"
                    >
                      <slot name="clear-icon">
                        <sl-icon name="x-circle-fill" library="system"></sl-icon>
                      </slot>
                    </button>
                  `
                  : ''
              }

              <slot name="suffix" part="suffix" class="select__suffix"></slot>

              <slot name="expand-icon" part="expand-icon" class="select__expand-icon">
                <sl-icon library="system" name="chevron-down"></sl-icon>
              </slot>
            </div>

            <div
              id="listbox"
              role="listbox"
              aria-expanded=${this.open ? 'true' : 'false'}
              aria-multiselectable=${this.multiple ? 'true' : 'false'}
              aria-labelledby="label"
              part="listbox"
              class="select__listbox"
              tabindex="-1"
              @mouseup=${this.handleOptionClick}
              @slotchange=${this.handleDefaultSlotChange}
            >
              <slot></slot>
            </div>
          </sl-popup>
        </div>

        <div
          part="form-control-help-text"
          id="help-text"
          class="form-control__help-text"
          aria-hidden=${o ? 'false' : 'true'}
        >
          <slot name="help-text">${this.helpText}</slot>
        </div>
      </div>
    `
    }
  }
Ct.styles = [dt, ln, lC]
Ct.dependencies = {
  'sl-icon': Nt,
  'sl-popup': Ut,
  'sl-tag': vi,
}
c([J('.select')], Ct.prototype, 'popup', 2)
c([J('.select__combobox')], Ct.prototype, 'combobox', 2)
c([J('.select__display-input')], Ct.prototype, 'displayInput', 2)
c([J('.select__value-input')], Ct.prototype, 'valueInput', 2)
c([J('.select__listbox')], Ct.prototype, 'listbox', 2)
c([at()], Ct.prototype, 'hasFocus', 2)
c([at()], Ct.prototype, 'displayLabel', 2)
c([at()], Ct.prototype, 'currentOption', 2)
c([at()], Ct.prototype, 'selectedOptions', 2)
c([at()], Ct.prototype, 'valueHasChanged', 2)
c([p()], Ct.prototype, 'name', 2)
c(
  [
    p({
      converter: {
        fromAttribute: t => t.split(' '),
        toAttribute: t => t.join(' '),
      },
    }),
  ],
  Ct.prototype,
  'value',
  2,
)
c([an()], Ct.prototype, 'defaultValue', 2)
c([p({ reflect: !0 })], Ct.prototype, 'size', 2)
c([p()], Ct.prototype, 'placeholder', 2)
c([p({ type: Boolean, reflect: !0 })], Ct.prototype, 'multiple', 2)
c(
  [p({ attribute: 'max-options-visible', type: Number })],
  Ct.prototype,
  'maxOptionsVisible',
  2,
)
c([p({ type: Boolean, reflect: !0 })], Ct.prototype, 'disabled', 2)
c([p({ type: Boolean })], Ct.prototype, 'clearable', 2)
c([p({ type: Boolean, reflect: !0 })], Ct.prototype, 'open', 2)
c([p({ type: Boolean })], Ct.prototype, 'hoist', 2)
c([p({ type: Boolean, reflect: !0 })], Ct.prototype, 'filled', 2)
c([p({ type: Boolean, reflect: !0 })], Ct.prototype, 'pill', 2)
c([p()], Ct.prototype, 'label', 2)
c([p({ reflect: !0 })], Ct.prototype, 'placement', 2)
c([p({ attribute: 'help-text' })], Ct.prototype, 'helpText', 2)
c([p({ reflect: !0 })], Ct.prototype, 'form', 2)
c([p({ type: Boolean, reflect: !0 })], Ct.prototype, 'required', 2)
c([p()], Ct.prototype, 'getTag', 2)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  Ct.prototype,
  'handleDisabledChange',
  1,
)
c(
  [X('value', { waitUntilFirstUpdate: !0 })],
  Ct.prototype,
  'handleValueChange',
  1,
)
c(
  [X('open', { waitUntilFirstUpdate: !0 })],
  Ct.prototype,
  'handleOpenChange',
  1,
)
Mt('select.show', {
  keyframes: [
    { opacity: 0, scale: 0.9 },
    { opacity: 1, scale: 1 },
  ],
  options: { duration: 100, easing: 'ease' },
})
Mt('select.hide', {
  keyframes: [
    { opacity: 1, scale: 1 },
    { opacity: 0, scale: 0.9 },
  ],
  options: { duration: 100, easing: 'ease' },
})
var cC = st`
  :host(:not(:focus-within)) {
    position: absolute !important;
    width: 1px !important;
    height: 1px !important;
    clip: rect(0 0 0 0) !important;
    clip-path: inset(50%) !important;
    border: none !important;
    overflow: hidden !important;
    white-space: nowrap !important;
    padding: 0 !important;
  }
`,
  Ec = class extends nt {
    render() {
      return C` <slot></slot> `
    }
  }
Ec.styles = [dt, cC]
Ec.define('sl-visually-hidden')
le.define('sl-tooltip')
var dC = st`
  :host {
    /*
     * These are actually used by tree item, but we define them here so they can more easily be set and all tree items
     * stay consistent.
     */
    --indent-guide-color: var(--sl-color-neutral-200);
    --indent-guide-offset: 0;
    --indent-guide-style: solid;
    --indent-guide-width: 0;
    --indent-size: var(--sl-spacing-large);

    display: block;

    /*
     * Tree item indentation uses the "em" unit to increment its width on each level, so setting the font size to zero
     * here removes the indentation for all the nodes on the first level.
     */
    font-size: 0;
  }
`,
  hC = st`
  :host {
    display: block;
    outline: 0;
    z-index: 0;
  }

  :host(:focus) {
    outline: none;
  }

  slot:not([name])::slotted(sl-icon) {
    margin-inline-end: var(--sl-spacing-x-small);
  }

  .tree-item {
    position: relative;
    display: flex;
    align-items: stretch;
    flex-direction: column;
    color: var(--sl-color-neutral-700);
    cursor: pointer;
    user-select: none;
    -webkit-user-select: none;
  }

  .tree-item__checkbox {
    pointer-events: none;
  }

  .tree-item__expand-button,
  .tree-item__checkbox,
  .tree-item__label {
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-medium);
    font-weight: var(--sl-font-weight-normal);
    line-height: var(--sl-line-height-dense);
    letter-spacing: var(--sl-letter-spacing-normal);
  }

  .tree-item__checkbox::part(base) {
    display: flex;
    align-items: center;
  }

  .tree-item__indentation {
    display: block;
    width: 1em;
    flex-shrink: 0;
  }

  .tree-item__expand-button {
    display: flex;
    align-items: center;
    justify-content: center;
    box-sizing: content-box;
    color: var(--sl-color-neutral-500);
    padding: var(--sl-spacing-x-small);
    width: 1rem;
    height: 1rem;
    flex-shrink: 0;
    cursor: pointer;
  }

  .tree-item__expand-button {
    transition: var(--sl-transition-medium) rotate ease;
  }

  .tree-item--expanded .tree-item__expand-button {
    rotate: 90deg;
  }

  .tree-item--expanded.tree-item--rtl .tree-item__expand-button {
    rotate: -90deg;
  }

  .tree-item--expanded slot[name='expand-icon'],
  .tree-item:not(.tree-item--expanded) slot[name='collapse-icon'] {
    display: none;
  }

  .tree-item:not(.tree-item--has-expand-button) .tree-item__expand-icon-slot {
    display: none;
  }

  .tree-item__expand-button--visible {
    cursor: pointer;
  }

  .tree-item__item {
    display: flex;
    align-items: center;
    border-inline-start: solid 3px transparent;
  }

  .tree-item--disabled .tree-item__item {
    opacity: 0.5;
    outline: none;
    cursor: not-allowed;
  }

  :host(:focus-visible) .tree-item__item {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
    z-index: 2;
  }

  :host(:not([aria-disabled='true'])) .tree-item--selected .tree-item__item {
    background-color: var(--sl-color-neutral-100);
    border-inline-start-color: var(--sl-color-primary-600);
  }

  :host(:not([aria-disabled='true'])) .tree-item__expand-button {
    color: var(--sl-color-neutral-600);
  }

  .tree-item__label {
    display: flex;
    align-items: center;
    transition: var(--sl-transition-fast) color;
  }

  .tree-item__children {
    display: block;
    font-size: calc(1em + var(--indent-size, var(--sl-spacing-medium)));
  }

  /* Indentation lines */
  .tree-item__children {
    position: relative;
  }

  .tree-item__children::before {
    content: '';
    position: absolute;
    top: var(--indent-guide-offset);
    bottom: var(--indent-guide-offset);
    left: calc(1em - (var(--indent-guide-width) / 2) - 1px);
    border-inline-end: var(--indent-guide-width) var(--indent-guide-style) var(--indent-guide-color);
    z-index: 1;
  }

  .tree-item--rtl .tree-item__children::before {
    left: auto;
    right: 1em;
  }

  @media (forced-colors: active) {
    :host(:not([aria-disabled='true'])) .tree-item--selected .tree-item__item {
      outline: dashed 1px SelectedItem;
    }
  }
`,
  uC = st`
  :host {
    display: inline-block;
  }

  .checkbox {
    position: relative;
    display: inline-flex;
    align-items: flex-start;
    font-family: var(--sl-input-font-family);
    font-weight: var(--sl-input-font-weight);
    color: var(--sl-input-label-color);
    vertical-align: middle;
    cursor: pointer;
  }

  .checkbox--small {
    --toggle-size: var(--sl-toggle-size-small);
    font-size: var(--sl-input-font-size-small);
  }

  .checkbox--medium {
    --toggle-size: var(--sl-toggle-size-medium);
    font-size: var(--sl-input-font-size-medium);
  }

  .checkbox--large {
    --toggle-size: var(--sl-toggle-size-large);
    font-size: var(--sl-input-font-size-large);
  }

  .checkbox__control {
    flex: 0 0 auto;
    position: relative;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: var(--toggle-size);
    height: var(--toggle-size);
    border: solid var(--sl-input-border-width) var(--sl-input-border-color);
    border-radius: 2px;
    background-color: var(--sl-input-background-color);
    color: var(--sl-color-neutral-0);
    transition:
      var(--sl-transition-fast) border-color,
      var(--sl-transition-fast) background-color,
      var(--sl-transition-fast) color,
      var(--sl-transition-fast) box-shadow;
  }

  .checkbox__input {
    position: absolute;
    opacity: 0;
    padding: 0;
    margin: 0;
    pointer-events: none;
  }

  .checkbox__checked-icon,
  .checkbox__indeterminate-icon {
    display: inline-flex;
    width: var(--toggle-size);
    height: var(--toggle-size);
  }

  /* Hover */
  .checkbox:not(.checkbox--checked):not(.checkbox--disabled) .checkbox__control:hover {
    border-color: var(--sl-input-border-color-hover);
    background-color: var(--sl-input-background-color-hover);
  }

  /* Focus */
  .checkbox:not(.checkbox--checked):not(.checkbox--disabled) .checkbox__input:focus-visible ~ .checkbox__control {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  /* Checked/indeterminate */
  .checkbox--checked .checkbox__control,
  .checkbox--indeterminate .checkbox__control {
    border-color: var(--sl-color-primary-600);
    background-color: var(--sl-color-primary-600);
  }

  /* Checked/indeterminate + hover */
  .checkbox.checkbox--checked:not(.checkbox--disabled) .checkbox__control:hover,
  .checkbox.checkbox--indeterminate:not(.checkbox--disabled) .checkbox__control:hover {
    border-color: var(--sl-color-primary-500);
    background-color: var(--sl-color-primary-500);
  }

  /* Checked/indeterminate + focus */
  .checkbox.checkbox--checked:not(.checkbox--disabled) .checkbox__input:focus-visible ~ .checkbox__control,
  .checkbox.checkbox--indeterminate:not(.checkbox--disabled) .checkbox__input:focus-visible ~ .checkbox__control {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  /* Disabled */
  .checkbox--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .checkbox__label {
    display: inline-block;
    color: var(--sl-input-label-color);
    line-height: var(--toggle-size);
    margin-inline-start: 0.5em;
    user-select: none;
    -webkit-user-select: none;
  }

  :host([required]) .checkbox__label::after {
    content: var(--sl-input-required-content);
    color: var(--sl-input-required-content-color);
    margin-inline-start: var(--sl-input-required-content-offset);
  }
`,
  _e = class extends nt {
    constructor() {
      super(...arguments),
        (this.formControlController = new mi(this, {
          value: t => (t.checked ? t.value || 'on' : void 0),
          defaultValue: t => t.defaultChecked,
          setValue: (t, r) => (t.checked = r),
        })),
        (this.hasSlotController = new He(this, 'help-text')),
        (this.hasFocus = !1),
        (this.title = ''),
        (this.name = ''),
        (this.size = 'medium'),
        (this.disabled = !1),
        (this.checked = !1),
        (this.indeterminate = !1),
        (this.defaultChecked = !1),
        (this.form = ''),
        (this.required = !1),
        (this.helpText = '')
    }
    /** Gets the validity state object */
    get validity() {
      return this.input.validity
    }
    /** Gets the validation message */
    get validationMessage() {
      return this.input.validationMessage
    }
    firstUpdated() {
      this.formControlController.updateValidity()
    }
    handleClick() {
      ;(this.checked = !this.checked),
        (this.indeterminate = !1),
        this.emit('sl-change')
    }
    handleBlur() {
      ;(this.hasFocus = !1), this.emit('sl-blur')
    }
    handleInput() {
      this.emit('sl-input')
    }
    handleInvalid(t) {
      this.formControlController.setValidity(!1),
        this.formControlController.emitInvalidEvent(t)
    }
    handleFocus() {
      ;(this.hasFocus = !0), this.emit('sl-focus')
    }
    handleDisabledChange() {
      this.formControlController.setValidity(this.disabled)
    }
    handleStateChange() {
      ;(this.input.checked = this.checked),
        (this.input.indeterminate = this.indeterminate),
        this.formControlController.updateValidity()
    }
    /** Simulates a click on the checkbox. */
    click() {
      this.input.click()
    }
    /** Sets focus on the checkbox. */
    focus(t) {
      this.input.focus(t)
    }
    /** Removes focus from the checkbox. */
    blur() {
      this.input.blur()
    }
    /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
    checkValidity() {
      return this.input.checkValidity()
    }
    /** Gets the associated form, if one exists. */
    getForm() {
      return this.formControlController.getForm()
    }
    /** Checks for validity and shows the browser's validation message if the control is invalid. */
    reportValidity() {
      return this.input.reportValidity()
    }
    /**
     * Sets a custom validation message. The value provided will be shown to the user when the form is submitted. To clear
     * the custom validation message, call this method with an empty string.
     */
    setCustomValidity(t) {
      this.input.setCustomValidity(t),
        this.formControlController.updateValidity()
    }
    render() {
      const t = this.hasSlotController.test('help-text'),
        r = this.helpText ? !0 : !!t
      return C`
      <div
        class=${ct({
          'form-control': !0,
          'form-control--small': this.size === 'small',
          'form-control--medium': this.size === 'medium',
          'form-control--large': this.size === 'large',
          'form-control--has-help-text': r,
        })}
      >
        <label
          part="base"
          class=${ct({
            checkbox: !0,
            'checkbox--checked': this.checked,
            'checkbox--disabled': this.disabled,
            'checkbox--focused': this.hasFocus,
            'checkbox--indeterminate': this.indeterminate,
            'checkbox--small': this.size === 'small',
            'checkbox--medium': this.size === 'medium',
            'checkbox--large': this.size === 'large',
          })}
        >
          <input
            class="checkbox__input"
            type="checkbox"
            title=${this.title}
            name=${this.name}
            value=${it(this.value)}
            .indeterminate=${rn(this.indeterminate)}
            .checked=${rn(this.checked)}
            .disabled=${this.disabled}
            .required=${this.required}
            aria-checked=${this.checked ? 'true' : 'false'}
            aria-describedby="help-text"
            @click=${this.handleClick}
            @input=${this.handleInput}
            @invalid=${this.handleInvalid}
            @blur=${this.handleBlur}
            @focus=${this.handleFocus}
          />

          <span
            part="control${this.checked ? ' control--checked' : ''}${
              this.indeterminate ? ' control--indeterminate' : ''
            }"
            class="checkbox__control"
          >
            ${
              this.checked
                ? C`
                  <sl-icon part="checked-icon" class="checkbox__checked-icon" library="system" name="check"></sl-icon>
                `
                : ''
            }
            ${
              !this.checked && this.indeterminate
                ? C`
                  <sl-icon
                    part="indeterminate-icon"
                    class="checkbox__indeterminate-icon"
                    library="system"
                    name="indeterminate"
                  ></sl-icon>
                `
                : ''
            }
          </span>

          <div part="label" class="checkbox__label">
            <slot></slot>
          </div>
        </label>

        <div
          aria-hidden=${r ? 'false' : 'true'}
          class="form-control__help-text"
          id="help-text"
          part="form-control-help-text"
        >
          <slot name="help-text">${this.helpText}</slot>
        </div>
      </div>
    `
    }
  }
_e.styles = [dt, ln, uC]
_e.dependencies = { 'sl-icon': Nt }
c([J('input[type="checkbox"]')], _e.prototype, 'input', 2)
c([at()], _e.prototype, 'hasFocus', 2)
c([p()], _e.prototype, 'title', 2)
c([p()], _e.prototype, 'name', 2)
c([p()], _e.prototype, 'value', 2)
c([p({ reflect: !0 })], _e.prototype, 'size', 2)
c([p({ type: Boolean, reflect: !0 })], _e.prototype, 'disabled', 2)
c([p({ type: Boolean, reflect: !0 })], _e.prototype, 'checked', 2)
c([p({ type: Boolean, reflect: !0 })], _e.prototype, 'indeterminate', 2)
c([an('checked')], _e.prototype, 'defaultChecked', 2)
c([p({ reflect: !0 })], _e.prototype, 'form', 2)
c([p({ type: Boolean, reflect: !0 })], _e.prototype, 'required', 2)
c([p({ attribute: 'help-text' })], _e.prototype, 'helpText', 2)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  _e.prototype,
  'handleDisabledChange',
  1,
)
c(
  [X(['checked', 'indeterminate'], { waitUntilFirstUpdate: !0 })],
  _e.prototype,
  'handleStateChange',
  1,
)
var pC = st`
  :host {
    --track-width: 2px;
    --track-color: rgb(128 128 128 / 25%);
    --indicator-color: var(--sl-color-primary-600);
    --speed: 2s;

    display: inline-flex;
    width: 1em;
    height: 1em;
    flex: none;
  }

  .spinner {
    flex: 1 1 auto;
    height: 100%;
    width: 100%;
  }

  .spinner__track,
  .spinner__indicator {
    fill: none;
    stroke-width: var(--track-width);
    r: calc(0.5em - var(--track-width) / 2);
    cx: 0.5em;
    cy: 0.5em;
    transform-origin: 50% 50%;
  }

  .spinner__track {
    stroke: var(--track-color);
    transform-origin: 0% 0%;
  }

  .spinner__indicator {
    stroke: var(--indicator-color);
    stroke-linecap: round;
    stroke-dasharray: 150% 75%;
    animation: spin var(--speed) linear infinite;
  }

  @keyframes spin {
    0% {
      transform: rotate(0deg);
      stroke-dasharray: 0.05em, 3em;
    }

    50% {
      transform: rotate(450deg);
      stroke-dasharray: 1.375em, 1.375em;
    }

    100% {
      transform: rotate(1080deg);
      stroke-dasharray: 0.05em, 3em;
    }
  }
`,
  No = class extends nt {
    constructor() {
      super(...arguments), (this.localize = new Dt(this))
    }
    render() {
      return C`
      <svg part="base" class="spinner" role="progressbar" aria-label=${this.localize.term(
        'loading',
      )}>
        <circle class="spinner__track"></circle>
        <circle class="spinner__indicator"></circle>
      </svg>
    `
    }
  }
No.styles = [dt, pC]
/**
 * @license
 * Copyright 2021 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function yp(t, r, i) {
  return t ? r(t) : i == null ? void 0 : i(t)
}
var ne = class lc extends nt {
  constructor() {
    super(...arguments),
      (this.localize = new Dt(this)),
      (this.indeterminate = !1),
      (this.isLeaf = !1),
      (this.loading = !1),
      (this.selectable = !1),
      (this.expanded = !1),
      (this.selected = !1),
      (this.disabled = !1),
      (this.lazy = !1)
  }
  static isTreeItem(r) {
    return r instanceof Element && r.getAttribute('role') === 'treeitem'
  }
  connectedCallback() {
    super.connectedCallback(),
      this.setAttribute('role', 'treeitem'),
      this.setAttribute('tabindex', '-1'),
      this.isNestedItem() && (this.slot = 'children')
  }
  firstUpdated() {
    ;(this.childrenContainer.hidden = !this.expanded),
      (this.childrenContainer.style.height = this.expanded ? 'auto' : '0'),
      (this.isLeaf = !this.lazy && this.getChildrenItems().length === 0),
      this.handleExpandedChange()
  }
  async animateCollapse() {
    this.emit('sl-collapse'), await pe(this.childrenContainer)
    const { keyframes: r, options: i } = Xt(this, 'tree-item.collapse', {
      dir: this.localize.dir(),
    })
    await ie(
      this.childrenContainer,
      Zs(r, this.childrenContainer.scrollHeight),
      i,
    ),
      (this.childrenContainer.hidden = !0),
      this.emit('sl-after-collapse')
  }
  // Checks whether the item is nested into an item
  isNestedItem() {
    const r = this.parentElement
    return !!r && lc.isTreeItem(r)
  }
  handleChildrenSlotChange() {
    ;(this.loading = !1),
      (this.isLeaf = !this.lazy && this.getChildrenItems().length === 0)
  }
  willUpdate(r) {
    r.has('selected') && !r.has('indeterminate') && (this.indeterminate = !1)
  }
  async animateExpand() {
    this.emit('sl-expand'),
      await pe(this.childrenContainer),
      (this.childrenContainer.hidden = !1)
    const { keyframes: r, options: i } = Xt(this, 'tree-item.expand', {
      dir: this.localize.dir(),
    })
    await ie(
      this.childrenContainer,
      Zs(r, this.childrenContainer.scrollHeight),
      i,
    ),
      (this.childrenContainer.style.height = 'auto'),
      this.emit('sl-after-expand')
  }
  handleLoadingChange() {
    this.setAttribute('aria-busy', this.loading ? 'true' : 'false'),
      this.loading || this.animateExpand()
  }
  handleDisabledChange() {
    this.setAttribute('aria-disabled', this.disabled ? 'true' : 'false')
  }
  handleSelectedChange() {
    this.setAttribute('aria-selected', this.selected ? 'true' : 'false')
  }
  handleExpandedChange() {
    this.isLeaf
      ? this.removeAttribute('aria-expanded')
      : this.setAttribute('aria-expanded', this.expanded ? 'true' : 'false')
  }
  handleExpandAnimation() {
    this.expanded
      ? this.lazy
        ? ((this.loading = !0), this.emit('sl-lazy-load'))
        : this.animateExpand()
      : this.animateCollapse()
  }
  handleLazyChange() {
    this.emit('sl-lazy-change')
  }
  /** Gets all the nested tree items in this node. */
  getChildrenItems({ includeDisabled: r = !0 } = {}) {
    return this.childrenSlot
      ? [...this.childrenSlot.assignedElements({ flatten: !0 })].filter(
          i => lc.isTreeItem(i) && (r || !i.disabled),
        )
      : []
  }
  render() {
    const r = this.localize.dir() === 'rtl',
      i = !this.loading && (!this.isLeaf || this.lazy)
    return C`
      <div
        part="base"
        class="${ct({
          'tree-item': !0,
          'tree-item--expanded': this.expanded,
          'tree-item--selected': this.selected,
          'tree-item--disabled': this.disabled,
          'tree-item--leaf': this.isLeaf,
          'tree-item--has-expand-button': i,
          'tree-item--rtl': this.localize.dir() === 'rtl',
        })}"
      >
        <div
          class="tree-item__item"
          part="
            item
            ${this.disabled ? 'item--disabled' : ''}
            ${this.expanded ? 'item--expanded' : ''}
            ${this.indeterminate ? 'item--indeterminate' : ''}
            ${this.selected ? 'item--selected' : ''}
          "
        >
          <div class="tree-item__indentation" part="indentation"></div>

          <div
            part="expand-button"
            class=${ct({
              'tree-item__expand-button': !0,
              'tree-item__expand-button--visible': i,
            })}
            aria-hidden="true"
          >
            ${yp(
              this.loading,
              () =>
                C` <sl-spinner part="spinner" exportparts="base:spinner__base"></sl-spinner> `,
            )}
            <slot class="tree-item__expand-icon-slot" name="expand-icon">
              <sl-icon library="system" name=${
                r ? 'chevron-left' : 'chevron-right'
              }></sl-icon>
            </slot>
            <slot class="tree-item__expand-icon-slot" name="collapse-icon">
              <sl-icon library="system" name=${
                r ? 'chevron-left' : 'chevron-right'
              }></sl-icon>
            </slot>
          </div>

          ${yp(
            this.selectable,
            () => C`
              <sl-checkbox
                part="checkbox"
                exportparts="
                    base:checkbox__base,
                    control:checkbox__control,
                    control--checked:checkbox__control--checked,
                    control--indeterminate:checkbox__control--indeterminate,
                    checked-icon:checkbox__checked-icon,
                    indeterminate-icon:checkbox__indeterminate-icon,
                    label:checkbox__label
                  "
                class="tree-item__checkbox"
                ?disabled="${this.disabled}"
                ?checked="${rn(this.selected)}"
                ?indeterminate="${this.indeterminate}"
                tabindex="-1"
              ></sl-checkbox>
            `,
          )}

          <slot class="tree-item__label" part="label"></slot>
        </div>

        <div class="tree-item__children" part="children" role="group">
          <slot name="children" @slotchange="${
            this.handleChildrenSlotChange
          }"></slot>
        </div>
      </div>
    `
  }
}
ne.styles = [dt, hC]
ne.dependencies = {
  'sl-checkbox': _e,
  'sl-icon': Nt,
  'sl-spinner': No,
}
c([at()], ne.prototype, 'indeterminate', 2)
c([at()], ne.prototype, 'isLeaf', 2)
c([at()], ne.prototype, 'loading', 2)
c([at()], ne.prototype, 'selectable', 2)
c([p({ type: Boolean, reflect: !0 })], ne.prototype, 'expanded', 2)
c([p({ type: Boolean, reflect: !0 })], ne.prototype, 'selected', 2)
c([p({ type: Boolean, reflect: !0 })], ne.prototype, 'disabled', 2)
c([p({ type: Boolean, reflect: !0 })], ne.prototype, 'lazy', 2)
c([J('slot:not([name])')], ne.prototype, 'defaultSlot', 2)
c([J('slot[name=children]')], ne.prototype, 'childrenSlot', 2)
c([J('.tree-item__item')], ne.prototype, 'itemElement', 2)
c([J('.tree-item__children')], ne.prototype, 'childrenContainer', 2)
c([J('.tree-item__expand-button slot')], ne.prototype, 'expandButtonSlot', 2)
c(
  [X('loading', { waitUntilFirstUpdate: !0 })],
  ne.prototype,
  'handleLoadingChange',
  1,
)
c([X('disabled')], ne.prototype, 'handleDisabledChange', 1)
c([X('selected')], ne.prototype, 'handleSelectedChange', 1)
c(
  [X('expanded', { waitUntilFirstUpdate: !0 })],
  ne.prototype,
  'handleExpandedChange',
  1,
)
c(
  [X('expanded', { waitUntilFirstUpdate: !0 })],
  ne.prototype,
  'handleExpandAnimation',
  1,
)
c(
  [X('lazy', { waitUntilFirstUpdate: !0 })],
  ne.prototype,
  'handleLazyChange',
  1,
)
var So = ne
Mt('tree-item.expand', {
  keyframes: [
    { height: '0', opacity: '0', overflow: 'hidden' },
    { height: 'auto', opacity: '1', overflow: 'hidden' },
  ],
  options: { duration: 250, easing: 'cubic-bezier(0.4, 0.0, 0.2, 1)' },
})
Mt('tree-item.collapse', {
  keyframes: [
    { height: 'auto', opacity: '1', overflow: 'hidden' },
    { height: '0', opacity: '0', overflow: 'hidden' },
  ],
  options: { duration: 200, easing: 'cubic-bezier(0.4, 0.0, 0.2, 1)' },
})
function _p(t, r = !1) {
  function i(d) {
    const h = d.getChildrenItems({ includeDisabled: !1 })
    if (h.length) {
      const m = h.every(b => b.selected),
        f = h.every(b => !b.selected && !b.indeterminate)
      ;(d.selected = m), (d.indeterminate = !m && !f)
    }
  }
  function o(d) {
    const h = d.parentElement
    So.isTreeItem(h) && (i(h), o(h))
  }
  function a(d) {
    for (const h of d.getChildrenItems())
      (h.selected = r ? d.selected || h.selected : !h.disabled && d.selected),
        a(h)
    r && i(d)
  }
  a(t), o(t)
}
var cn = class extends nt {
  constructor() {
    super(),
      (this.selection = 'single'),
      (this.clickTarget = null),
      (this.localize = new Dt(this)),
      (this.initTreeItem = t => {
        ;(t.selectable = this.selection === 'multiple'),
          ['expand', 'collapse']
            .filter(r => !!this.querySelector(`[slot="${r}-icon"]`))
            .forEach(r => {
              const i = t.querySelector(`[slot="${r}-icon"]`),
                o = this.getExpandButtonIcon(r)
              o &&
                (i === null
                  ? t.append(o)
                  : i.hasAttribute('data-default') && i.replaceWith(o))
            })
      }),
      (this.handleTreeChanged = t => {
        for (const r of t) {
          const i = [...r.addedNodes].filter(So.isTreeItem),
            o = [...r.removedNodes].filter(So.isTreeItem)
          i.forEach(this.initTreeItem),
            this.lastFocusedItem &&
              o.includes(this.lastFocusedItem) &&
              (this.lastFocusedItem = null)
        }
      }),
      (this.handleFocusOut = t => {
        const r = t.relatedTarget
        ;(!r || !this.contains(r)) && (this.tabIndex = 0)
      }),
      (this.handleFocusIn = t => {
        const r = t.target
        t.target === this &&
          this.focusItem(this.lastFocusedItem || this.getAllTreeItems()[0]),
          So.isTreeItem(r) &&
            !r.disabled &&
            (this.lastFocusedItem && (this.lastFocusedItem.tabIndex = -1),
            (this.lastFocusedItem = r),
            (this.tabIndex = -1),
            (r.tabIndex = 0))
      }),
      this.addEventListener('focusin', this.handleFocusIn),
      this.addEventListener('focusout', this.handleFocusOut),
      this.addEventListener('sl-lazy-change', this.handleSlotChange)
  }
  async connectedCallback() {
    super.connectedCallback(),
      this.setAttribute('role', 'tree'),
      this.setAttribute('tabindex', '0'),
      await this.updateComplete,
      (this.mutationObserver = new MutationObserver(this.handleTreeChanged)),
      this.mutationObserver.observe(this, { childList: !0, subtree: !0 })
  }
  disconnectedCallback() {
    var t
    super.disconnectedCallback(),
      (t = this.mutationObserver) == null || t.disconnect()
  }
  // Generates a clone of the expand icon element to use for each tree item
  getExpandButtonIcon(t) {
    const i = (
      t === 'expand' ? this.expandedIconSlot : this.collapsedIconSlot
    ).assignedElements({ flatten: !0 })[0]
    if (i) {
      const o = i.cloneNode(!0)
      return (
        [o, ...o.querySelectorAll('[id]')].forEach(a =>
          a.removeAttribute('id'),
        ),
        o.setAttribute('data-default', ''),
        (o.slot = `${t}-icon`),
        o
      )
    }
    return null
  }
  selectItem(t) {
    const r = [...this.selectedItems]
    if (this.selection === 'multiple')
      (t.selected = !t.selected), t.lazy && (t.expanded = !0), _p(t)
    else if (this.selection === 'single' || t.isLeaf) {
      const o = this.getAllTreeItems()
      for (const a of o) a.selected = a === t
    } else this.selection === 'leaf' && (t.expanded = !t.expanded)
    const i = this.selectedItems
    ;(r.length !== i.length || i.some(o => !r.includes(o))) &&
      Promise.all(i.map(o => o.updateComplete)).then(() => {
        this.emit('sl-selection-change', { detail: { selection: i } })
      })
  }
  getAllTreeItems() {
    return [...this.querySelectorAll('sl-tree-item')]
  }
  focusItem(t) {
    t == null || t.focus()
  }
  handleKeyDown(t) {
    if (
      ![
        'ArrowDown',
        'ArrowUp',
        'ArrowRight',
        'ArrowLeft',
        'Home',
        'End',
        'Enter',
        ' ',
      ].includes(t.key) ||
      t.composedPath().some(a => {
        var d
        return ['input', 'textarea'].includes(
          (d = a == null ? void 0 : a.tagName) == null
            ? void 0
            : d.toLowerCase(),
        )
      })
    )
      return
    const r = this.getFocusableItems(),
      i = this.localize.dir() === 'ltr',
      o = this.localize.dir() === 'rtl'
    if (r.length > 0) {
      t.preventDefault()
      const a = r.findIndex(f => f.matches(':focus')),
        d = r[a],
        h = f => {
          const b = r[ue(f, 0, r.length - 1)]
          this.focusItem(b)
        },
        m = f => {
          d.expanded = f
        }
      t.key === 'ArrowDown'
        ? h(a + 1)
        : t.key === 'ArrowUp'
        ? h(a - 1)
        : (i && t.key === 'ArrowRight') || (o && t.key === 'ArrowLeft')
        ? !d || d.disabled || d.expanded || (d.isLeaf && !d.lazy)
          ? h(a + 1)
          : m(!0)
        : (i && t.key === 'ArrowLeft') || (o && t.key === 'ArrowRight')
        ? !d || d.disabled || d.isLeaf || !d.expanded
          ? h(a - 1)
          : m(!1)
        : t.key === 'Home'
        ? h(0)
        : t.key === 'End'
        ? h(r.length - 1)
        : (t.key === 'Enter' || t.key === ' ') &&
          (d.disabled || this.selectItem(d))
    }
  }
  handleClick(t) {
    const r = t.target,
      i = r.closest('sl-tree-item'),
      o = t.composedPath().some(a => {
        var d
        return (d = a == null ? void 0 : a.classList) == null
          ? void 0
          : d.contains('tree-item__expand-button')
      })
    !i ||
      i.disabled ||
      r !== this.clickTarget ||
      (o ? (i.expanded = !i.expanded) : this.selectItem(i))
  }
  handleMouseDown(t) {
    this.clickTarget = t.target
  }
  handleSlotChange() {
    this.getAllTreeItems().forEach(this.initTreeItem)
  }
  async handleSelectionChange() {
    const t = this.selection === 'multiple',
      r = this.getAllTreeItems()
    this.setAttribute('aria-multiselectable', t ? 'true' : 'false')
    for (const i of r) i.selectable = t
    t &&
      (await this.updateComplete,
      [...this.querySelectorAll(':scope > sl-tree-item')].forEach(i =>
        _p(i, !0),
      ))
  }
  /** @internal Returns the list of tree items that are selected in the tree. */
  get selectedItems() {
    const t = this.getAllTreeItems(),
      r = i => i.selected
    return t.filter(r)
  }
  /** @internal Gets focusable tree items in the tree. */
  getFocusableItems() {
    const t = this.getAllTreeItems(),
      r = /* @__PURE__ */ new Set()
    return t.filter(i => {
      var o
      if (i.disabled) return !1
      const a =
        (o = i.parentElement) == null ? void 0 : o.closest('[role=treeitem]')
      return a && (!a.expanded || a.loading || r.has(a)) && r.add(i), !r.has(i)
    })
  }
  render() {
    return C`
      <div
        part="base"
        class="tree"
        @click=${this.handleClick}
        @keydown=${this.handleKeyDown}
        @mousedown=${this.handleMouseDown}
      >
        <slot @slotchange=${this.handleSlotChange}></slot>
        <span hidden aria-hidden="true"><slot name="expand-icon"></slot></span>
        <span hidden aria-hidden="true"><slot name="collapse-icon"></slot></span>
      </div>
    `
  }
}
cn.styles = [dt, dC]
c([J('slot:not([name])')], cn.prototype, 'defaultSlot', 2)
c([J('slot[name=expand-icon]')], cn.prototype, 'expandedIconSlot', 2)
c([J('slot[name=collapse-icon]')], cn.prototype, 'collapsedIconSlot', 2)
c([p()], cn.prototype, 'selection', 2)
c([X('selection')], cn.prototype, 'handleSelectionChange', 1)
cn.define('sl-tree')
So.define('sl-tree-item')
var Kl = vi
vi.define('sl-tag')
var fC = st`
  :host {
    display: block;
  }

  .textarea {
    display: grid;
    align-items: center;
    position: relative;
    width: 100%;
    font-family: var(--sl-input-font-family);
    font-weight: var(--sl-input-font-weight);
    line-height: var(--sl-line-height-normal);
    letter-spacing: var(--sl-input-letter-spacing);
    vertical-align: middle;
    transition:
      var(--sl-transition-fast) color,
      var(--sl-transition-fast) border,
      var(--sl-transition-fast) box-shadow,
      var(--sl-transition-fast) background-color;
    cursor: text;
  }

  /* Standard textareas */
  .textarea--standard {
    background-color: var(--sl-input-background-color);
    border: solid var(--sl-input-border-width) var(--sl-input-border-color);
  }

  .textarea--standard:hover:not(.textarea--disabled) {
    background-color: var(--sl-input-background-color-hover);
    border-color: var(--sl-input-border-color-hover);
  }
  .textarea--standard:hover:not(.textarea--disabled) .textarea__control {
    color: var(--sl-input-color-hover);
  }

  .textarea--standard.textarea--focused:not(.textarea--disabled) {
    background-color: var(--sl-input-background-color-focus);
    border-color: var(--sl-input-border-color-focus);
    color: var(--sl-input-color-focus);
    box-shadow: 0 0 0 var(--sl-focus-ring-width) var(--sl-input-focus-ring-color);
  }

  .textarea--standard.textarea--focused:not(.textarea--disabled) .textarea__control {
    color: var(--sl-input-color-focus);
  }

  .textarea--standard.textarea--disabled {
    background-color: var(--sl-input-background-color-disabled);
    border-color: var(--sl-input-border-color-disabled);
    opacity: 0.5;
    cursor: not-allowed;
  }

  .textarea__control,
  .textarea__size-adjuster {
    grid-area: 1 / 1 / 2 / 2;
  }

  .textarea__size-adjuster {
    visibility: hidden;
    pointer-events: none;
    opacity: 0;
  }

  .textarea--standard.textarea--disabled .textarea__control {
    color: var(--sl-input-color-disabled);
  }

  .textarea--standard.textarea--disabled .textarea__control::placeholder {
    color: var(--sl-input-placeholder-color-disabled);
  }

  /* Filled textareas */
  .textarea--filled {
    border: none;
    background-color: var(--sl-input-filled-background-color);
    color: var(--sl-input-color);
  }

  .textarea--filled:hover:not(.textarea--disabled) {
    background-color: var(--sl-input-filled-background-color-hover);
  }

  .textarea--filled.textarea--focused:not(.textarea--disabled) {
    background-color: var(--sl-input-filled-background-color-focus);
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .textarea--filled.textarea--disabled {
    background-color: var(--sl-input-filled-background-color-disabled);
    opacity: 0.5;
    cursor: not-allowed;
  }

  .textarea__control {
    font-family: inherit;
    font-size: inherit;
    font-weight: inherit;
    line-height: 1.4;
    color: var(--sl-input-color);
    border: none;
    background: none;
    box-shadow: none;
    cursor: inherit;
    -webkit-appearance: none;
  }

  .textarea__control::-webkit-search-decoration,
  .textarea__control::-webkit-search-cancel-button,
  .textarea__control::-webkit-search-results-button,
  .textarea__control::-webkit-search-results-decoration {
    -webkit-appearance: none;
  }

  .textarea__control::placeholder {
    color: var(--sl-input-placeholder-color);
    user-select: none;
    -webkit-user-select: none;
  }

  .textarea__control:focus {
    outline: none;
  }

  /*
   * Size modifiers
   */

  .textarea--small {
    border-radius: var(--sl-input-border-radius-small);
    font-size: var(--sl-input-font-size-small);
  }

  .textarea--small .textarea__control {
    padding: 0.5em var(--sl-input-spacing-small);
  }

  .textarea--medium {
    border-radius: var(--sl-input-border-radius-medium);
    font-size: var(--sl-input-font-size-medium);
  }

  .textarea--medium .textarea__control {
    padding: 0.5em var(--sl-input-spacing-medium);
  }

  .textarea--large {
    border-radius: var(--sl-input-border-radius-large);
    font-size: var(--sl-input-font-size-large);
  }

  .textarea--large .textarea__control {
    padding: 0.5em var(--sl-input-spacing-large);
  }

  /*
   * Resize types
   */

  .textarea--resize-none .textarea__control {
    resize: none;
  }

  .textarea--resize-vertical .textarea__control {
    resize: vertical;
  }

  .textarea--resize-auto .textarea__control {
    height: auto;
    resize: none;
    overflow-y: hidden;
  }
`,
  Rt = class extends nt {
    constructor() {
      super(...arguments),
        (this.formControlController = new mi(this, {
          assumeInteractionOn: ['sl-blur', 'sl-input'],
        })),
        (this.hasSlotController = new He(this, 'help-text', 'label')),
        (this.hasFocus = !1),
        (this.title = ''),
        (this.name = ''),
        (this.value = ''),
        (this.size = 'medium'),
        (this.filled = !1),
        (this.label = ''),
        (this.helpText = ''),
        (this.placeholder = ''),
        (this.rows = 4),
        (this.resize = 'vertical'),
        (this.disabled = !1),
        (this.readonly = !1),
        (this.form = ''),
        (this.required = !1),
        (this.spellcheck = !0),
        (this.defaultValue = '')
    }
    /** Gets the validity state object */
    get validity() {
      return this.input.validity
    }
    /** Gets the validation message */
    get validationMessage() {
      return this.input.validationMessage
    }
    connectedCallback() {
      super.connectedCallback(),
        (this.resizeObserver = new ResizeObserver(() =>
          this.setTextareaHeight(),
        )),
        this.updateComplete.then(() => {
          this.setTextareaHeight(), this.resizeObserver.observe(this.input)
        })
    }
    firstUpdated() {
      this.formControlController.updateValidity()
    }
    disconnectedCallback() {
      var t
      super.disconnectedCallback(),
        this.input &&
          ((t = this.resizeObserver) == null || t.unobserve(this.input))
    }
    handleBlur() {
      ;(this.hasFocus = !1), this.emit('sl-blur')
    }
    handleChange() {
      ;(this.value = this.input.value),
        this.setTextareaHeight(),
        this.emit('sl-change')
    }
    handleFocus() {
      ;(this.hasFocus = !0), this.emit('sl-focus')
    }
    handleInput() {
      ;(this.value = this.input.value), this.emit('sl-input')
    }
    handleInvalid(t) {
      this.formControlController.setValidity(!1),
        this.formControlController.emitInvalidEvent(t)
    }
    setTextareaHeight() {
      this.resize === 'auto'
        ? ((this.sizeAdjuster.style.height = `${this.input.clientHeight}px`),
          (this.input.style.height = 'auto'),
          (this.input.style.height = `${this.input.scrollHeight}px`))
        : (this.input.style.height = void 0)
    }
    handleDisabledChange() {
      this.formControlController.setValidity(this.disabled)
    }
    handleRowsChange() {
      this.setTextareaHeight()
    }
    async handleValueChange() {
      await this.updateComplete,
        this.formControlController.updateValidity(),
        this.setTextareaHeight()
    }
    /** Sets focus on the textarea. */
    focus(t) {
      this.input.focus(t)
    }
    /** Removes focus from the textarea. */
    blur() {
      this.input.blur()
    }
    /** Selects all the text in the textarea. */
    select() {
      this.input.select()
    }
    /** Gets or sets the textarea's scroll position. */
    scrollPosition(t) {
      if (t) {
        typeof t.top == 'number' && (this.input.scrollTop = t.top),
          typeof t.left == 'number' && (this.input.scrollLeft = t.left)
        return
      }
      return {
        top: this.input.scrollTop,
        left: this.input.scrollTop,
      }
    }
    /** Sets the start and end positions of the text selection (0-based). */
    setSelectionRange(t, r, i = 'none') {
      this.input.setSelectionRange(t, r, i)
    }
    /** Replaces a range of text with a new string. */
    setRangeText(t, r, i, o = 'preserve') {
      const a = r ?? this.input.selectionStart,
        d = i ?? this.input.selectionEnd
      this.input.setRangeText(t, a, d, o),
        this.value !== this.input.value &&
          ((this.value = this.input.value), this.setTextareaHeight())
    }
    /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
    checkValidity() {
      return this.input.checkValidity()
    }
    /** Gets the associated form, if one exists. */
    getForm() {
      return this.formControlController.getForm()
    }
    /** Checks for validity and shows the browser's validation message if the control is invalid. */
    reportValidity() {
      return this.input.reportValidity()
    }
    /** Sets a custom validation message. Pass an empty string to restore validity. */
    setCustomValidity(t) {
      this.input.setCustomValidity(t),
        this.formControlController.updateValidity()
    }
    render() {
      const t = this.hasSlotController.test('label'),
        r = this.hasSlotController.test('help-text'),
        i = this.label ? !0 : !!t,
        o = this.helpText ? !0 : !!r
      return C`
      <div
        part="form-control"
        class=${ct({
          'form-control': !0,
          'form-control--small': this.size === 'small',
          'form-control--medium': this.size === 'medium',
          'form-control--large': this.size === 'large',
          'form-control--has-label': i,
          'form-control--has-help-text': o,
        })}
      >
        <label
          part="form-control-label"
          class="form-control__label"
          for="input"
          aria-hidden=${i ? 'false' : 'true'}
        >
          <slot name="label">${this.label}</slot>
        </label>

        <div part="form-control-input" class="form-control-input">
          <div
            part="base"
            class=${ct({
              textarea: !0,
              'textarea--small': this.size === 'small',
              'textarea--medium': this.size === 'medium',
              'textarea--large': this.size === 'large',
              'textarea--standard': !this.filled,
              'textarea--filled': this.filled,
              'textarea--disabled': this.disabled,
              'textarea--focused': this.hasFocus,
              'textarea--empty': !this.value,
              'textarea--resize-none': this.resize === 'none',
              'textarea--resize-vertical': this.resize === 'vertical',
              'textarea--resize-auto': this.resize === 'auto',
            })}
          >
            <textarea
              part="textarea"
              id="input"
              class="textarea__control"
              title=${this.title}
              name=${it(this.name)}
              .value=${rn(this.value)}
              ?disabled=${this.disabled}
              ?readonly=${this.readonly}
              ?required=${this.required}
              placeholder=${it(this.placeholder)}
              rows=${it(this.rows)}
              minlength=${it(this.minlength)}
              maxlength=${it(this.maxlength)}
              autocapitalize=${it(this.autocapitalize)}
              autocorrect=${it(this.autocorrect)}
              ?autofocus=${this.autofocus}
              spellcheck=${it(this.spellcheck)}
              enterkeyhint=${it(this.enterkeyhint)}
              inputmode=${it(this.inputmode)}
              aria-describedby="help-text"
              @change=${this.handleChange}
              @input=${this.handleInput}
              @invalid=${this.handleInvalid}
              @focus=${this.handleFocus}
              @blur=${this.handleBlur}
            ></textarea>
            <!-- This "adjuster" exists to prevent layout shifting. https://github.com/shoelace-style/shoelace/issues/2180 -->
            <div part="textarea-adjuster" class="textarea__size-adjuster" ?hidden=${
              this.resize !== 'auto'
            }></div>
          </div>
        </div>

        <div
          part="form-control-help-text"
          id="help-text"
          class="form-control__help-text"
          aria-hidden=${o ? 'false' : 'true'}
        >
          <slot name="help-text">${this.helpText}</slot>
        </div>
      </div>
    `
    }
  }
Rt.styles = [dt, ln, fC]
c([J('.textarea__control')], Rt.prototype, 'input', 2)
c([J('.textarea__size-adjuster')], Rt.prototype, 'sizeAdjuster', 2)
c([at()], Rt.prototype, 'hasFocus', 2)
c([p()], Rt.prototype, 'title', 2)
c([p()], Rt.prototype, 'name', 2)
c([p()], Rt.prototype, 'value', 2)
c([p({ reflect: !0 })], Rt.prototype, 'size', 2)
c([p({ type: Boolean, reflect: !0 })], Rt.prototype, 'filled', 2)
c([p()], Rt.prototype, 'label', 2)
c([p({ attribute: 'help-text' })], Rt.prototype, 'helpText', 2)
c([p()], Rt.prototype, 'placeholder', 2)
c([p({ type: Number })], Rt.prototype, 'rows', 2)
c([p()], Rt.prototype, 'resize', 2)
c([p({ type: Boolean, reflect: !0 })], Rt.prototype, 'disabled', 2)
c([p({ type: Boolean, reflect: !0 })], Rt.prototype, 'readonly', 2)
c([p({ reflect: !0 })], Rt.prototype, 'form', 2)
c([p({ type: Boolean, reflect: !0 })], Rt.prototype, 'required', 2)
c([p({ type: Number })], Rt.prototype, 'minlength', 2)
c([p({ type: Number })], Rt.prototype, 'maxlength', 2)
c([p()], Rt.prototype, 'autocapitalize', 2)
c([p()], Rt.prototype, 'autocorrect', 2)
c([p()], Rt.prototype, 'autocomplete', 2)
c([p({ type: Boolean })], Rt.prototype, 'autofocus', 2)
c([p()], Rt.prototype, 'enterkeyhint', 2)
c(
  [
    p({
      type: Boolean,
      converter: {
        // Allow "true|false" attribute values but keep the property boolean
        fromAttribute: t => !(!t || t === 'false'),
        toAttribute: t => (t ? 'true' : 'false'),
      },
    }),
  ],
  Rt.prototype,
  'spellcheck',
  2,
)
c([p()], Rt.prototype, 'inputmode', 2)
c([an()], Rt.prototype, 'defaultValue', 2)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  Rt.prototype,
  'handleDisabledChange',
  1,
)
c(
  [X('rows', { waitUntilFirstUpdate: !0 })],
  Rt.prototype,
  'handleRowsChange',
  1,
)
c(
  [X('value', { waitUntilFirstUpdate: !0 })],
  Rt.prototype,
  'handleValueChange',
  1,
)
Rt.define('sl-textarea')
Ii.define('sl-tab-panel')
fe.define('sl-tab-group')
No.define('sl-spinner')
Xe.define('sl-tab')
var gC = st`
  :host {
    display: inline-block;
  }

  :host([size='small']) {
    --height: var(--sl-toggle-size-small);
    --thumb-size: calc(var(--sl-toggle-size-small) + 4px);
    --width: calc(var(--height) * 2);

    font-size: var(--sl-input-font-size-small);
  }

  :host([size='medium']) {
    --height: var(--sl-toggle-size-medium);
    --thumb-size: calc(var(--sl-toggle-size-medium) + 4px);
    --width: calc(var(--height) * 2);

    font-size: var(--sl-input-font-size-medium);
  }

  :host([size='large']) {
    --height: var(--sl-toggle-size-large);
    --thumb-size: calc(var(--sl-toggle-size-large) + 4px);
    --width: calc(var(--height) * 2);

    font-size: var(--sl-input-font-size-large);
  }

  .switch {
    position: relative;
    display: inline-flex;
    align-items: center;
    font-family: var(--sl-input-font-family);
    font-size: inherit;
    font-weight: var(--sl-input-font-weight);
    color: var(--sl-input-label-color);
    vertical-align: middle;
    cursor: pointer;
  }

  .switch__control {
    flex: 0 0 auto;
    position: relative;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: var(--width);
    height: var(--height);
    background-color: var(--sl-color-neutral-400);
    border: solid var(--sl-input-border-width) var(--sl-color-neutral-400);
    border-radius: var(--height);
    transition:
      var(--sl-transition-fast) border-color,
      var(--sl-transition-fast) background-color;
  }

  .switch__control .switch__thumb {
    width: var(--thumb-size);
    height: var(--thumb-size);
    background-color: var(--sl-color-neutral-0);
    border-radius: 50%;
    border: solid var(--sl-input-border-width) var(--sl-color-neutral-400);
    translate: calc((var(--width) - var(--height)) / -2);
    transition:
      var(--sl-transition-fast) translate ease,
      var(--sl-transition-fast) background-color,
      var(--sl-transition-fast) border-color,
      var(--sl-transition-fast) box-shadow;
  }

  .switch__input {
    position: absolute;
    opacity: 0;
    padding: 0;
    margin: 0;
    pointer-events: none;
  }

  /* Hover */
  .switch:not(.switch--checked):not(.switch--disabled) .switch__control:hover {
    background-color: var(--sl-color-neutral-400);
    border-color: var(--sl-color-neutral-400);
  }

  .switch:not(.switch--checked):not(.switch--disabled) .switch__control:hover .switch__thumb {
    background-color: var(--sl-color-neutral-0);
    border-color: var(--sl-color-neutral-400);
  }

  /* Focus */
  .switch:not(.switch--checked):not(.switch--disabled) .switch__input:focus-visible ~ .switch__control {
    background-color: var(--sl-color-neutral-400);
    border-color: var(--sl-color-neutral-400);
  }

  .switch:not(.switch--checked):not(.switch--disabled) .switch__input:focus-visible ~ .switch__control .switch__thumb {
    background-color: var(--sl-color-neutral-0);
    border-color: var(--sl-color-primary-600);
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  /* Checked */
  .switch--checked .switch__control {
    background-color: var(--sl-color-primary-600);
    border-color: var(--sl-color-primary-600);
  }

  .switch--checked .switch__control .switch__thumb {
    background-color: var(--sl-color-neutral-0);
    border-color: var(--sl-color-primary-600);
    translate: calc((var(--width) - var(--height)) / 2);
  }

  /* Checked + hover */
  .switch.switch--checked:not(.switch--disabled) .switch__control:hover {
    background-color: var(--sl-color-primary-600);
    border-color: var(--sl-color-primary-600);
  }

  .switch.switch--checked:not(.switch--disabled) .switch__control:hover .switch__thumb {
    background-color: var(--sl-color-neutral-0);
    border-color: var(--sl-color-primary-600);
  }

  /* Checked + focus */
  .switch.switch--checked:not(.switch--disabled) .switch__input:focus-visible ~ .switch__control {
    background-color: var(--sl-color-primary-600);
    border-color: var(--sl-color-primary-600);
  }

  .switch.switch--checked:not(.switch--disabled) .switch__input:focus-visible ~ .switch__control .switch__thumb {
    background-color: var(--sl-color-neutral-0);
    border-color: var(--sl-color-primary-600);
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  /* Disabled */
  .switch--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .switch__label {
    display: inline-block;
    line-height: var(--height);
    margin-inline-start: 0.5em;
    user-select: none;
    -webkit-user-select: none;
  }

  :host([required]) .switch__label::after {
    content: var(--sl-input-required-content);
    color: var(--sl-input-required-content-color);
    margin-inline-start: var(--sl-input-required-content-offset);
  }

  @media (forced-colors: active) {
    .switch.switch--checked:not(.switch--disabled) .switch__control:hover .switch__thumb,
    .switch--checked .switch__control .switch__thumb {
      background-color: ButtonText;
    }
  }
`,
  Le = class extends nt {
    constructor() {
      super(...arguments),
        (this.formControlController = new mi(this, {
          value: t => (t.checked ? t.value || 'on' : void 0),
          defaultValue: t => t.defaultChecked,
          setValue: (t, r) => (t.checked = r),
        })),
        (this.hasSlotController = new He(this, 'help-text')),
        (this.hasFocus = !1),
        (this.title = ''),
        (this.name = ''),
        (this.size = 'medium'),
        (this.disabled = !1),
        (this.checked = !1),
        (this.defaultChecked = !1),
        (this.form = ''),
        (this.required = !1),
        (this.helpText = '')
    }
    /** Gets the validity state object */
    get validity() {
      return this.input.validity
    }
    /** Gets the validation message */
    get validationMessage() {
      return this.input.validationMessage
    }
    firstUpdated() {
      this.formControlController.updateValidity()
    }
    handleBlur() {
      ;(this.hasFocus = !1), this.emit('sl-blur')
    }
    handleInput() {
      this.emit('sl-input')
    }
    handleInvalid(t) {
      this.formControlController.setValidity(!1),
        this.formControlController.emitInvalidEvent(t)
    }
    handleClick() {
      ;(this.checked = !this.checked), this.emit('sl-change')
    }
    handleFocus() {
      ;(this.hasFocus = !0), this.emit('sl-focus')
    }
    handleKeyDown(t) {
      t.key === 'ArrowLeft' &&
        (t.preventDefault(),
        (this.checked = !1),
        this.emit('sl-change'),
        this.emit('sl-input')),
        t.key === 'ArrowRight' &&
          (t.preventDefault(),
          (this.checked = !0),
          this.emit('sl-change'),
          this.emit('sl-input'))
    }
    handleCheckedChange() {
      ;(this.input.checked = this.checked),
        this.formControlController.updateValidity()
    }
    handleDisabledChange() {
      this.formControlController.setValidity(!0)
    }
    /** Simulates a click on the switch. */
    click() {
      this.input.click()
    }
    /** Sets focus on the switch. */
    focus(t) {
      this.input.focus(t)
    }
    /** Removes focus from the switch. */
    blur() {
      this.input.blur()
    }
    /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
    checkValidity() {
      return this.input.checkValidity()
    }
    /** Gets the associated form, if one exists. */
    getForm() {
      return this.formControlController.getForm()
    }
    /** Checks for validity and shows the browser's validation message if the control is invalid. */
    reportValidity() {
      return this.input.reportValidity()
    }
    /** Sets a custom validation message. Pass an empty string to restore validity. */
    setCustomValidity(t) {
      this.input.setCustomValidity(t),
        this.formControlController.updateValidity()
    }
    render() {
      const t = this.hasSlotController.test('help-text'),
        r = this.helpText ? !0 : !!t
      return C`
      <div
        class=${ct({
          'form-control': !0,
          'form-control--small': this.size === 'small',
          'form-control--medium': this.size === 'medium',
          'form-control--large': this.size === 'large',
          'form-control--has-help-text': r,
        })}
      >
        <label
          part="base"
          class=${ct({
            switch: !0,
            'switch--checked': this.checked,
            'switch--disabled': this.disabled,
            'switch--focused': this.hasFocus,
            'switch--small': this.size === 'small',
            'switch--medium': this.size === 'medium',
            'switch--large': this.size === 'large',
          })}
        >
          <input
            class="switch__input"
            type="checkbox"
            title=${this.title}
            name=${this.name}
            value=${it(this.value)}
            .checked=${rn(this.checked)}
            .disabled=${this.disabled}
            .required=${this.required}
            role="switch"
            aria-checked=${this.checked ? 'true' : 'false'}
            aria-describedby="help-text"
            @click=${this.handleClick}
            @input=${this.handleInput}
            @invalid=${this.handleInvalid}
            @blur=${this.handleBlur}
            @focus=${this.handleFocus}
            @keydown=${this.handleKeyDown}
          />

          <span part="control" class="switch__control">
            <span part="thumb" class="switch__thumb"></span>
          </span>

          <div part="label" class="switch__label">
            <slot></slot>
          </div>
        </label>

        <div
          aria-hidden=${r ? 'false' : 'true'}
          class="form-control__help-text"
          id="help-text"
          part="form-control-help-text"
        >
          <slot name="help-text">${this.helpText}</slot>
        </div>
      </div>
    `
    }
  }
Le.styles = [dt, ln, gC]
c([J('input[type="checkbox"]')], Le.prototype, 'input', 2)
c([at()], Le.prototype, 'hasFocus', 2)
c([p()], Le.prototype, 'title', 2)
c([p()], Le.prototype, 'name', 2)
c([p()], Le.prototype, 'value', 2)
c([p({ reflect: !0 })], Le.prototype, 'size', 2)
c([p({ type: Boolean, reflect: !0 })], Le.prototype, 'disabled', 2)
c([p({ type: Boolean, reflect: !0 })], Le.prototype, 'checked', 2)
c([an('checked')], Le.prototype, 'defaultChecked', 2)
c([p({ reflect: !0 })], Le.prototype, 'form', 2)
c([p({ type: Boolean, reflect: !0 })], Le.prototype, 'required', 2)
c([p({ attribute: 'help-text' })], Le.prototype, 'helpText', 2)
c(
  [X('checked', { waitUntilFirstUpdate: !0 })],
  Le.prototype,
  'handleCheckedChange',
  1,
)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  Le.prototype,
  'handleDisabledChange',
  1,
)
Le.define('sl-switch')
en.define('sl-resize-observer')
Ct.define('sl-select')
var mC = st`
  :host {
    --border-radius: var(--sl-border-radius-pill);
    --color: var(--sl-color-neutral-200);
    --sheen-color: var(--sl-color-neutral-300);

    display: block;
    position: relative;
  }

  .skeleton {
    display: flex;
    width: 100%;
    height: 100%;
    min-height: 1rem;
  }

  .skeleton__indicator {
    flex: 1 1 auto;
    background: var(--color);
    border-radius: var(--border-radius);
  }

  .skeleton--sheen .skeleton__indicator {
    background: linear-gradient(270deg, var(--sheen-color), var(--color), var(--color), var(--sheen-color));
    background-size: 400% 100%;
    animation: sheen 8s ease-in-out infinite;
  }

  .skeleton--pulse .skeleton__indicator {
    animation: pulse 2s ease-in-out 0.5s infinite;
  }

  /* Forced colors mode */
  @media (forced-colors: active) {
    :host {
      --color: GrayText;
    }
  }

  @keyframes sheen {
    0% {
      background-position: 200% 0;
    }
    to {
      background-position: -200% 0;
    }
  }

  @keyframes pulse {
    0% {
      opacity: 1;
    }
    50% {
      opacity: 0.4;
    }
    100% {
      opacity: 1;
    }
  }
`,
  Tc = class extends nt {
    constructor() {
      super(...arguments), (this.effect = 'none')
    }
    render() {
      return C`
      <div
        part="base"
        class=${ct({
          skeleton: !0,
          'skeleton--pulse': this.effect === 'pulse',
          'skeleton--sheen': this.effect === 'sheen',
        })}
      >
        <div part="indicator" class="skeleton__indicator"></div>
      </div>
    `
    }
  }
Tc.styles = [dt, mC]
c([p()], Tc.prototype, 'effect', 2)
Tc.define('sl-skeleton')
var vC = st`
  :host {
    --symbol-color: var(--sl-color-neutral-300);
    --symbol-color-active: var(--sl-color-amber-500);
    --symbol-size: 1.2rem;
    --symbol-spacing: var(--sl-spacing-3x-small);

    display: inline-flex;
  }

  .rating {
    position: relative;
    display: inline-flex;
    border-radius: var(--sl-border-radius-medium);
    vertical-align: middle;
  }

  .rating:focus {
    outline: none;
  }

  .rating:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .rating__symbols {
    display: inline-flex;
    position: relative;
    font-size: var(--symbol-size);
    line-height: 0;
    color: var(--symbol-color);
    white-space: nowrap;
    cursor: pointer;
  }

  .rating__symbols > * {
    padding: var(--symbol-spacing);
  }

  .rating__symbol--active,
  .rating__partial--filled {
    color: var(--symbol-color-active);
  }

  .rating__partial-symbol-container {
    position: relative;
  }

  .rating__partial--filled {
    position: absolute;
    top: var(--symbol-spacing);
    left: var(--symbol-spacing);
  }

  .rating__symbol {
    transition: var(--sl-transition-fast) scale;
    pointer-events: none;
  }

  .rating__symbol--hover {
    scale: 1.2;
  }

  .rating--disabled .rating__symbols,
  .rating--readonly .rating__symbols {
    cursor: default;
  }

  .rating--disabled .rating__symbol--hover,
  .rating--readonly .rating__symbol--hover {
    scale: none;
  }

  .rating--disabled {
    opacity: 0.5;
  }

  .rating--disabled .rating__symbols {
    cursor: not-allowed;
  }

  /* Forced colors mode */
  @media (forced-colors: active) {
    .rating__symbol--active {
      color: SelectedItem;
    }
  }
`
/**
 * @license
 * Copyright 2018 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const yf = 'important',
  bC = ' !' + yf,
  Ke = Bo(
    class extends Fo {
      constructor(t) {
        var r
        if (
          (super(t),
          t.type !== Yr.ATTRIBUTE ||
            t.name !== 'style' ||
            ((r = t.strings) == null ? void 0 : r.length) > 2)
        )
          throw Error(
            'The `styleMap` directive must be used in the `style` attribute and must be the only part in the attribute.',
          )
      }
      render(t) {
        return Object.keys(t).reduce((r, i) => {
          const o = t[i]
          return o == null
            ? r
            : r +
                `${(i = i.includes('-')
                  ? i
                  : i
                      .replace(/(?:^(webkit|moz|ms|o)|)(?=[A-Z])/g, '-$&')
                      .toLowerCase())}:${o};`
        }, '')
      }
      update(t, [r]) {
        const { style: i } = t.element
        if (this.ft === void 0)
          return (this.ft = new Set(Object.keys(r))), this.render(r)
        for (const o of this.ft)
          r[o] == null &&
            (this.ft.delete(o),
            o.includes('-') ? i.removeProperty(o) : (i[o] = null))
        for (const o in r) {
          const a = r[o]
          if (a != null) {
            this.ft.add(o)
            const d = typeof a == 'string' && a.endsWith(bC)
            o.includes('-') || d
              ? i.setProperty(o, d ? a.slice(0, -11) : a, d ? yf : '')
              : (i[o] = a)
          }
        }
        return lr
      }
    },
  )
var De = class extends nt {
  constructor() {
    super(...arguments),
      (this.localize = new Dt(this)),
      (this.hoverValue = 0),
      (this.isHovering = !1),
      (this.label = ''),
      (this.value = 0),
      (this.max = 5),
      (this.precision = 1),
      (this.readonly = !1),
      (this.disabled = !1),
      (this.getSymbol = () =>
        '<sl-icon name="star-fill" library="system"></sl-icon>')
  }
  getValueFromMousePosition(t) {
    return this.getValueFromXCoordinate(t.clientX)
  }
  getValueFromTouchPosition(t) {
    return this.getValueFromXCoordinate(t.touches[0].clientX)
  }
  getValueFromXCoordinate(t) {
    const r = this.localize.dir() === 'rtl',
      { left: i, right: o, width: a } = this.rating.getBoundingClientRect(),
      d = r
        ? this.roundToPrecision(((o - t) / a) * this.max, this.precision)
        : this.roundToPrecision(((t - i) / a) * this.max, this.precision)
    return ue(d, 0, this.max)
  }
  handleClick(t) {
    this.disabled ||
      (this.setValue(this.getValueFromMousePosition(t)), this.emit('sl-change'))
  }
  setValue(t) {
    this.disabled ||
      this.readonly ||
      ((this.value = t === this.value ? 0 : t), (this.isHovering = !1))
  }
  handleKeyDown(t) {
    const r = this.localize.dir() === 'ltr',
      i = this.localize.dir() === 'rtl',
      o = this.value
    if (!(this.disabled || this.readonly)) {
      if (
        t.key === 'ArrowDown' ||
        (r && t.key === 'ArrowLeft') ||
        (i && t.key === 'ArrowRight')
      ) {
        const a = t.shiftKey ? 1 : this.precision
        ;(this.value = Math.max(0, this.value - a)), t.preventDefault()
      }
      if (
        t.key === 'ArrowUp' ||
        (r && t.key === 'ArrowRight') ||
        (i && t.key === 'ArrowLeft')
      ) {
        const a = t.shiftKey ? 1 : this.precision
        ;(this.value = Math.min(this.max, this.value + a)), t.preventDefault()
      }
      t.key === 'Home' && ((this.value = 0), t.preventDefault()),
        t.key === 'End' && ((this.value = this.max), t.preventDefault()),
        this.value !== o && this.emit('sl-change')
    }
  }
  handleMouseEnter(t) {
    ;(this.isHovering = !0),
      (this.hoverValue = this.getValueFromMousePosition(t))
  }
  handleMouseMove(t) {
    this.hoverValue = this.getValueFromMousePosition(t)
  }
  handleMouseLeave() {
    this.isHovering = !1
  }
  handleTouchStart(t) {
    ;(this.isHovering = !0),
      (this.hoverValue = this.getValueFromTouchPosition(t)),
      t.preventDefault()
  }
  handleTouchMove(t) {
    this.hoverValue = this.getValueFromTouchPosition(t)
  }
  handleTouchEnd(t) {
    ;(this.isHovering = !1),
      this.setValue(this.hoverValue),
      this.emit('sl-change'),
      t.preventDefault()
  }
  roundToPrecision(t, r = 0.5) {
    const i = 1 / r
    return Math.ceil(t * i) / i
  }
  handleHoverValueChange() {
    this.emit('sl-hover', {
      detail: {
        phase: 'move',
        value: this.hoverValue,
      },
    })
  }
  handleIsHoveringChange() {
    this.emit('sl-hover', {
      detail: {
        phase: this.isHovering ? 'start' : 'end',
        value: this.hoverValue,
      },
    })
  }
  /** Sets focus on the rating. */
  focus(t) {
    this.rating.focus(t)
  }
  /** Removes focus from the rating. */
  blur() {
    this.rating.blur()
  }
  render() {
    const t = this.localize.dir() === 'rtl',
      r = Array.from(Array(this.max).keys())
    let i = 0
    return (
      this.disabled || this.readonly
        ? (i = this.value)
        : (i = this.isHovering ? this.hoverValue : this.value),
      C`
      <div
        part="base"
        class=${ct({
          rating: !0,
          'rating--readonly': this.readonly,
          'rating--disabled': this.disabled,
          'rating--rtl': t,
        })}
        role="slider"
        aria-label=${this.label}
        aria-disabled=${this.disabled ? 'true' : 'false'}
        aria-readonly=${this.readonly ? 'true' : 'false'}
        aria-valuenow=${this.value}
        aria-valuemin=${0}
        aria-valuemax=${this.max}
        tabindex=${this.disabled ? '-1' : '0'}
        @click=${this.handleClick}
        @keydown=${this.handleKeyDown}
        @mouseenter=${this.handleMouseEnter}
        @touchstart=${this.handleTouchStart}
        @mouseleave=${this.handleMouseLeave}
        @touchend=${this.handleTouchEnd}
        @mousemove=${this.handleMouseMove}
        @touchmove=${this.handleTouchMove}
      >
        <span class="rating__symbols">
          ${r.map(o =>
            i > o && i < o + 1
              ? C`
                <span
                  class=${ct({
                    rating__symbol: !0,
                    'rating__partial-symbol-container': !0,
                    'rating__symbol--hover':
                      this.isHovering && Math.ceil(i) === o + 1,
                  })}
                  role="presentation"
                >
                  <div
                    style=${Ke({
                      clipPath: t
                        ? `inset(0 ${(i - o) * 100}% 0 0)`
                        : `inset(0 0 0 ${(i - o) * 100}%)`,
                    })}
                  >
                    ${Co(this.getSymbol(o + 1))}
                  </div>
                  <div
                    class="rating__partial--filled"
                    style=${Ke({
                      clipPath: t
                        ? `inset(0 0 0 ${100 - (i - o) * 100}%)`
                        : `inset(0 ${100 - (i - o) * 100}% 0 0)`,
                    })}
                  >
                    ${Co(this.getSymbol(o + 1))}
                  </div>
                </span>
              `
              : C`
              <span
                class=${ct({
                  rating__symbol: !0,
                  'rating__symbol--hover':
                    this.isHovering && Math.ceil(i) === o + 1,
                  'rating__symbol--active': i >= o + 1,
                })}
                role="presentation"
              >
                ${Co(this.getSymbol(o + 1))}
              </span>
            `,
          )}
        </span>
      </div>
    `
    )
  }
}
De.styles = [dt, vC]
De.dependencies = { 'sl-icon': Nt }
c([J('.rating')], De.prototype, 'rating', 2)
c([at()], De.prototype, 'hoverValue', 2)
c([at()], De.prototype, 'isHovering', 2)
c([p()], De.prototype, 'label', 2)
c([p({ type: Number })], De.prototype, 'value', 2)
c([p({ type: Number })], De.prototype, 'max', 2)
c([p({ type: Number })], De.prototype, 'precision', 2)
c([p({ type: Boolean, reflect: !0 })], De.prototype, 'readonly', 2)
c([p({ type: Boolean, reflect: !0 })], De.prototype, 'disabled', 2)
c([p()], De.prototype, 'getSymbol', 2)
c([Do({ passive: !0 })], De.prototype, 'handleTouchMove', 1)
c([X('hoverValue')], De.prototype, 'handleHoverValueChange', 1)
c([X('isHovering')], De.prototype, 'handleIsHoveringChange', 1)
De.define('sl-rating')
var yC = [
    { max: 276e4, value: 6e4, unit: 'minute' },
    // max 46 minutes
    { max: 72e6, value: 36e5, unit: 'hour' },
    // max 20 hours
    { max: 5184e5, value: 864e5, unit: 'day' },
    // max 6 days
    { max: 24192e5, value: 6048e5, unit: 'week' },
    // max 28 days
    { max: 28512e6, value: 2592e6, unit: 'month' },
    // max 11 months
    { max: 1 / 0, value: 31536e6, unit: 'year' },
  ],
  dn = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.isoTime = ''),
        (this.relativeTime = ''),
        (this.date = /* @__PURE__ */ new Date()),
        (this.format = 'long'),
        (this.numeric = 'auto'),
        (this.sync = !1)
    }
    disconnectedCallback() {
      super.disconnectedCallback(), clearTimeout(this.updateTimeout)
    }
    render() {
      const t = /* @__PURE__ */ new Date(),
        r = new Date(this.date)
      if (isNaN(r.getMilliseconds()))
        return (this.relativeTime = ''), (this.isoTime = ''), ''
      const i = r.getTime() - t.getTime(),
        { unit: o, value: a } = yC.find(d => Math.abs(i) < d.max)
      if (
        ((this.isoTime = r.toISOString()),
        (this.relativeTime = this.localize.relativeTime(Math.round(i / a), o, {
          numeric: this.numeric,
          style: this.format,
        })),
        clearTimeout(this.updateTimeout),
        this.sync)
      ) {
        let d
        o === 'minute'
          ? (d = Rs('second'))
          : o === 'hour'
          ? (d = Rs('minute'))
          : o === 'day'
          ? (d = Rs('hour'))
          : (d = Rs('day')),
          (this.updateTimeout = window.setTimeout(
            () => this.requestUpdate(),
            d,
          ))
      }
      return C` <time datetime=${this.isoTime}>${this.relativeTime}</time> `
    }
  }
c([at()], dn.prototype, 'isoTime', 2)
c([at()], dn.prototype, 'relativeTime', 2)
c([p()], dn.prototype, 'date', 2)
c([p()], dn.prototype, 'format', 2)
c([p()], dn.prototype, 'numeric', 2)
c([p({ type: Boolean })], dn.prototype, 'sync', 2)
function Rs(t) {
  const i = { second: 1e3, minute: 6e4, hour: 36e5, day: 864e5 }[t]
  return i - (Date.now() % i)
}
dn.define('sl-relative-time')
var _C = st`
  :host {
    --thumb-size: 20px;
    --tooltip-offset: 10px;
    --track-color-active: var(--sl-color-neutral-200);
    --track-color-inactive: var(--sl-color-neutral-200);
    --track-active-offset: 0%;
    --track-height: 6px;

    display: block;
  }

  .range {
    position: relative;
  }

  .range__control {
    --percent: 0%;
    -webkit-appearance: none;
    border-radius: 3px;
    width: 100%;
    height: var(--track-height);
    background: transparent;
    line-height: var(--sl-input-height-medium);
    vertical-align: middle;
    margin: 0;

    background-image: linear-gradient(
      to right,
      var(--track-color-inactive) 0%,
      var(--track-color-inactive) min(var(--percent), var(--track-active-offset)),
      var(--track-color-active) min(var(--percent), var(--track-active-offset)),
      var(--track-color-active) max(var(--percent), var(--track-active-offset)),
      var(--track-color-inactive) max(var(--percent), var(--track-active-offset)),
      var(--track-color-inactive) 100%
    );
  }

  .range--rtl .range__control {
    background-image: linear-gradient(
      to left,
      var(--track-color-inactive) 0%,
      var(--track-color-inactive) min(var(--percent), var(--track-active-offset)),
      var(--track-color-active) min(var(--percent), var(--track-active-offset)),
      var(--track-color-active) max(var(--percent), var(--track-active-offset)),
      var(--track-color-inactive) max(var(--percent), var(--track-active-offset)),
      var(--track-color-inactive) 100%
    );
  }

  /* Webkit */
  .range__control::-webkit-slider-runnable-track {
    width: 100%;
    height: var(--track-height);
    border-radius: 3px;
    border: none;
  }

  .range__control::-webkit-slider-thumb {
    border: none;
    width: var(--thumb-size);
    height: var(--thumb-size);
    border-radius: 50%;
    background-color: var(--sl-color-primary-600);
    border: solid var(--sl-input-border-width) var(--sl-color-primary-600);
    -webkit-appearance: none;
    margin-top: calc(var(--thumb-size) / -2 + var(--track-height) / 2);
    cursor: pointer;
  }

  .range__control:enabled::-webkit-slider-thumb:hover {
    background-color: var(--sl-color-primary-500);
    border-color: var(--sl-color-primary-500);
  }

  .range__control:enabled:focus-visible::-webkit-slider-thumb {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .range__control:enabled::-webkit-slider-thumb:active {
    background-color: var(--sl-color-primary-500);
    border-color: var(--sl-color-primary-500);
    cursor: grabbing;
  }

  /* Firefox */
  .range__control::-moz-focus-outer {
    border: 0;
  }

  .range__control::-moz-range-progress {
    background-color: var(--track-color-active);
    border-radius: 3px;
    height: var(--track-height);
  }

  .range__control::-moz-range-track {
    width: 100%;
    height: var(--track-height);
    background-color: var(--track-color-inactive);
    border-radius: 3px;
    border: none;
  }

  .range__control::-moz-range-thumb {
    border: none;
    height: var(--thumb-size);
    width: var(--thumb-size);
    border-radius: 50%;
    background-color: var(--sl-color-primary-600);
    border-color: var(--sl-color-primary-600);
    transition:
      var(--sl-transition-fast) border-color,
      var(--sl-transition-fast) background-color,
      var(--sl-transition-fast) color,
      var(--sl-transition-fast) box-shadow;
    cursor: pointer;
  }

  .range__control:enabled::-moz-range-thumb:hover {
    background-color: var(--sl-color-primary-500);
    border-color: var(--sl-color-primary-500);
  }

  .range__control:enabled:focus-visible::-moz-range-thumb {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .range__control:enabled::-moz-range-thumb:active {
    background-color: var(--sl-color-primary-500);
    border-color: var(--sl-color-primary-500);
    cursor: grabbing;
  }

  /* States */
  .range__control:focus-visible {
    outline: none;
  }

  .range__control:disabled {
    opacity: 0.5;
  }

  .range__control:disabled::-webkit-slider-thumb {
    cursor: not-allowed;
  }

  .range__control:disabled::-moz-range-thumb {
    cursor: not-allowed;
  }

  /* Tooltip output */
  .range__tooltip {
    position: absolute;
    z-index: var(--sl-z-index-tooltip);
    left: 0;
    border-radius: var(--sl-tooltip-border-radius);
    background-color: var(--sl-tooltip-background-color);
    font-family: var(--sl-tooltip-font-family);
    font-size: var(--sl-tooltip-font-size);
    font-weight: var(--sl-tooltip-font-weight);
    line-height: var(--sl-tooltip-line-height);
    color: var(--sl-tooltip-color);
    opacity: 0;
    padding: var(--sl-tooltip-padding);
    transition: var(--sl-transition-fast) opacity;
    pointer-events: none;
  }

  .range__tooltip:after {
    content: '';
    position: absolute;
    width: 0;
    height: 0;
    left: 50%;
    translate: calc(-1 * var(--sl-tooltip-arrow-size));
  }

  .range--tooltip-visible .range__tooltip {
    opacity: 1;
  }

  /* Tooltip on top */
  .range--tooltip-top .range__tooltip {
    top: calc(-1 * var(--thumb-size) - var(--tooltip-offset));
  }

  .range--tooltip-top .range__tooltip:after {
    border-top: var(--sl-tooltip-arrow-size) solid var(--sl-tooltip-background-color);
    border-left: var(--sl-tooltip-arrow-size) solid transparent;
    border-right: var(--sl-tooltip-arrow-size) solid transparent;
    top: 100%;
  }

  /* Tooltip on bottom */
  .range--tooltip-bottom .range__tooltip {
    bottom: calc(-1 * var(--thumb-size) - var(--tooltip-offset));
  }

  .range--tooltip-bottom .range__tooltip:after {
    border-bottom: var(--sl-tooltip-arrow-size) solid var(--sl-tooltip-background-color);
    border-left: var(--sl-tooltip-arrow-size) solid transparent;
    border-right: var(--sl-tooltip-arrow-size) solid transparent;
    bottom: 100%;
  }

  @media (forced-colors: active) {
    .range__control,
    .range__tooltip {
      border: solid 1px transparent;
    }

    .range__control::-webkit-slider-thumb {
      border: solid 1px transparent;
    }

    .range__control::-moz-range-thumb {
      border: solid 1px transparent;
    }

    .range__tooltip:after {
      display: none;
    }
  }
`,
  te = class extends nt {
    constructor() {
      super(...arguments),
        (this.formControlController = new mi(this)),
        (this.hasSlotController = new He(this, 'help-text', 'label')),
        (this.localize = new Dt(this)),
        (this.hasFocus = !1),
        (this.hasTooltip = !1),
        (this.title = ''),
        (this.name = ''),
        (this.value = 0),
        (this.label = ''),
        (this.helpText = ''),
        (this.disabled = !1),
        (this.min = 0),
        (this.max = 100),
        (this.step = 1),
        (this.tooltip = 'top'),
        (this.tooltipFormatter = t => t.toString()),
        (this.form = ''),
        (this.defaultValue = 0)
    }
    /** Gets the validity state object */
    get validity() {
      return this.input.validity
    }
    /** Gets the validation message */
    get validationMessage() {
      return this.input.validationMessage
    }
    connectedCallback() {
      super.connectedCallback(),
        (this.resizeObserver = new ResizeObserver(() => this.syncRange())),
        this.value < this.min && (this.value = this.min),
        this.value > this.max && (this.value = this.max),
        this.updateComplete.then(() => {
          this.syncRange(), this.resizeObserver.observe(this.input)
        })
    }
    disconnectedCallback() {
      var t
      super.disconnectedCallback(),
        (t = this.resizeObserver) == null || t.unobserve(this.input)
    }
    handleChange() {
      this.emit('sl-change')
    }
    handleInput() {
      ;(this.value = parseFloat(this.input.value)),
        this.emit('sl-input'),
        this.syncRange()
    }
    handleBlur() {
      ;(this.hasFocus = !1), (this.hasTooltip = !1), this.emit('sl-blur')
    }
    handleFocus() {
      ;(this.hasFocus = !0), (this.hasTooltip = !0), this.emit('sl-focus')
    }
    handleThumbDragStart() {
      this.hasTooltip = !0
    }
    handleThumbDragEnd() {
      this.hasTooltip = !1
    }
    syncProgress(t) {
      this.input.style.setProperty('--percent', `${t * 100}%`)
    }
    syncTooltip(t) {
      if (this.output !== null) {
        const r = this.input.offsetWidth,
          i = this.output.offsetWidth,
          o = getComputedStyle(this.input).getPropertyValue('--thumb-size'),
          a = this.localize.dir() === 'rtl',
          d = r * t
        if (a) {
          const h = `${r - d}px + ${t} * ${o}`
          this.output.style.translate = `calc((${h} - ${i / 2}px - ${o} / 2))`
        } else {
          const h = `${d}px - ${t} * ${o}`
          this.output.style.translate = `calc(${h} - ${i / 2}px + ${o} / 2)`
        }
      }
    }
    handleValueChange() {
      this.formControlController.updateValidity(),
        (this.input.value = this.value.toString()),
        (this.value = parseFloat(this.input.value)),
        this.syncRange()
    }
    handleDisabledChange() {
      this.formControlController.setValidity(this.disabled)
    }
    syncRange() {
      const t = Math.max(0, (this.value - this.min) / (this.max - this.min))
      this.syncProgress(t),
        this.tooltip !== 'none' &&
          this.updateComplete.then(() => this.syncTooltip(t))
    }
    handleInvalid(t) {
      this.formControlController.setValidity(!1),
        this.formControlController.emitInvalidEvent(t)
    }
    /** Sets focus on the range. */
    focus(t) {
      this.input.focus(t)
    }
    /** Removes focus from the range. */
    blur() {
      this.input.blur()
    }
    /** Increments the value of the range by the value of the step attribute. */
    stepUp() {
      this.input.stepUp(),
        this.value !== Number(this.input.value) &&
          (this.value = Number(this.input.value))
    }
    /** Decrements the value of the range by the value of the step attribute. */
    stepDown() {
      this.input.stepDown(),
        this.value !== Number(this.input.value) &&
          (this.value = Number(this.input.value))
    }
    /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
    checkValidity() {
      return this.input.checkValidity()
    }
    /** Gets the associated form, if one exists. */
    getForm() {
      return this.formControlController.getForm()
    }
    /** Checks for validity and shows the browser's validation message if the control is invalid. */
    reportValidity() {
      return this.input.reportValidity()
    }
    /** Sets a custom validation message. Pass an empty string to restore validity. */
    setCustomValidity(t) {
      this.input.setCustomValidity(t),
        this.formControlController.updateValidity()
    }
    render() {
      const t = this.hasSlotController.test('label'),
        r = this.hasSlotController.test('help-text'),
        i = this.label ? !0 : !!t,
        o = this.helpText ? !0 : !!r
      return C`
      <div
        part="form-control"
        class=${ct({
          'form-control': !0,
          'form-control--medium': !0,
          // range only has one size
          'form-control--has-label': i,
          'form-control--has-help-text': o,
        })}
      >
        <label
          part="form-control-label"
          class="form-control__label"
          for="input"
          aria-hidden=${i ? 'false' : 'true'}
        >
          <slot name="label">${this.label}</slot>
        </label>

        <div part="form-control-input" class="form-control-input">
          <div
            part="base"
            class=${ct({
              range: !0,
              'range--disabled': this.disabled,
              'range--focused': this.hasFocus,
              'range--rtl': this.localize.dir() === 'rtl',
              'range--tooltip-visible': this.hasTooltip,
              'range--tooltip-top': this.tooltip === 'top',
              'range--tooltip-bottom': this.tooltip === 'bottom',
            })}
            @mousedown=${this.handleThumbDragStart}
            @mouseup=${this.handleThumbDragEnd}
            @touchstart=${this.handleThumbDragStart}
            @touchend=${this.handleThumbDragEnd}
          >
            <input
              part="input"
              id="input"
              class="range__control"
              title=${this.title}
              type="range"
              name=${it(this.name)}
              ?disabled=${this.disabled}
              min=${it(this.min)}
              max=${it(this.max)}
              step=${it(this.step)}
              .value=${rn(this.value.toString())}
              aria-describedby="help-text"
              @change=${this.handleChange}
              @focus=${this.handleFocus}
              @input=${this.handleInput}
              @invalid=${this.handleInvalid}
              @blur=${this.handleBlur}
            />
            ${
              this.tooltip !== 'none' && !this.disabled
                ? C`
                  <output part="tooltip" class="range__tooltip">
                    ${
                      typeof this.tooltipFormatter == 'function'
                        ? this.tooltipFormatter(this.value)
                        : this.value
                    }
                  </output>
                `
                : ''
            }
          </div>
        </div>

        <div
          part="form-control-help-text"
          id="help-text"
          class="form-control__help-text"
          aria-hidden=${o ? 'false' : 'true'}
        >
          <slot name="help-text">${this.helpText}</slot>
        </div>
      </div>
    `
    }
  }
te.styles = [dt, ln, _C]
c([J('.range__control')], te.prototype, 'input', 2)
c([J('.range__tooltip')], te.prototype, 'output', 2)
c([at()], te.prototype, 'hasFocus', 2)
c([at()], te.prototype, 'hasTooltip', 2)
c([p()], te.prototype, 'title', 2)
c([p()], te.prototype, 'name', 2)
c([p({ type: Number })], te.prototype, 'value', 2)
c([p()], te.prototype, 'label', 2)
c([p({ attribute: 'help-text' })], te.prototype, 'helpText', 2)
c([p({ type: Boolean, reflect: !0 })], te.prototype, 'disabled', 2)
c([p({ type: Number })], te.prototype, 'min', 2)
c([p({ type: Number })], te.prototype, 'max', 2)
c([p({ type: Number })], te.prototype, 'step', 2)
c([p()], te.prototype, 'tooltip', 2)
c([p({ attribute: !1 })], te.prototype, 'tooltipFormatter', 2)
c([p({ reflect: !0 })], te.prototype, 'form', 2)
c([an()], te.prototype, 'defaultValue', 2)
c([Do({ passive: !0 })], te.prototype, 'handleThumbDragStart', 1)
c(
  [X('value', { waitUntilFirstUpdate: !0 })],
  te.prototype,
  'handleValueChange',
  1,
)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  te.prototype,
  'handleDisabledChange',
  1,
)
c([X('hasTooltip', { waitUntilFirstUpdate: !0 })], te.prototype, 'syncRange', 1)
te.define('sl-range')
var _f = st`
  :host {
    display: inline-block;
    position: relative;
    width: auto;
    cursor: pointer;
  }

  .button {
    display: inline-flex;
    align-items: stretch;
    justify-content: center;
    width: 100%;
    border-style: solid;
    border-width: var(--sl-input-border-width);
    font-family: var(--sl-input-font-family);
    font-weight: var(--sl-font-weight-semibold);
    text-decoration: none;
    user-select: none;
    -webkit-user-select: none;
    white-space: nowrap;
    vertical-align: middle;
    padding: 0;
    transition:
      var(--sl-transition-x-fast) background-color,
      var(--sl-transition-x-fast) color,
      var(--sl-transition-x-fast) border,
      var(--sl-transition-x-fast) box-shadow;
    cursor: inherit;
  }

  .button::-moz-focus-inner {
    border: 0;
  }

  .button:focus {
    outline: none;
  }

  .button:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .button--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  /* When disabled, prevent mouse events from bubbling up from children */
  .button--disabled * {
    pointer-events: none;
  }

  .button__prefix,
  .button__suffix {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    pointer-events: none;
  }

  .button__label {
    display: inline-block;
  }

  .button__label::slotted(sl-icon) {
    vertical-align: -2px;
  }

  /*
   * Standard buttons
   */

  /* Default */
  .button--standard.button--default {
    background-color: var(--sl-color-neutral-0);
    border-color: var(--sl-input-border-color);
    color: var(--sl-color-neutral-700);
  }

  .button--standard.button--default:hover:not(.button--disabled) {
    background-color: var(--sl-color-primary-50);
    border-color: var(--sl-color-primary-300);
    color: var(--sl-color-primary-700);
  }

  .button--standard.button--default:active:not(.button--disabled) {
    background-color: var(--sl-color-primary-100);
    border-color: var(--sl-color-primary-400);
    color: var(--sl-color-primary-700);
  }

  /* Primary */
  .button--standard.button--primary {
    background-color: var(--sl-color-primary-600);
    border-color: var(--sl-color-primary-600);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--primary:hover:not(.button--disabled) {
    background-color: var(--sl-color-primary-500);
    border-color: var(--sl-color-primary-500);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--primary:active:not(.button--disabled) {
    background-color: var(--sl-color-primary-600);
    border-color: var(--sl-color-primary-600);
    color: var(--sl-color-neutral-0);
  }

  /* Success */
  .button--standard.button--success {
    background-color: var(--sl-color-success-600);
    border-color: var(--sl-color-success-600);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--success:hover:not(.button--disabled) {
    background-color: var(--sl-color-success-500);
    border-color: var(--sl-color-success-500);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--success:active:not(.button--disabled) {
    background-color: var(--sl-color-success-600);
    border-color: var(--sl-color-success-600);
    color: var(--sl-color-neutral-0);
  }

  /* Neutral */
  .button--standard.button--neutral {
    background-color: var(--sl-color-neutral-600);
    border-color: var(--sl-color-neutral-600);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--neutral:hover:not(.button--disabled) {
    background-color: var(--sl-color-neutral-500);
    border-color: var(--sl-color-neutral-500);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--neutral:active:not(.button--disabled) {
    background-color: var(--sl-color-neutral-600);
    border-color: var(--sl-color-neutral-600);
    color: var(--sl-color-neutral-0);
  }

  /* Warning */
  .button--standard.button--warning {
    background-color: var(--sl-color-warning-600);
    border-color: var(--sl-color-warning-600);
    color: var(--sl-color-neutral-0);
  }
  .button--standard.button--warning:hover:not(.button--disabled) {
    background-color: var(--sl-color-warning-500);
    border-color: var(--sl-color-warning-500);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--warning:active:not(.button--disabled) {
    background-color: var(--sl-color-warning-600);
    border-color: var(--sl-color-warning-600);
    color: var(--sl-color-neutral-0);
  }

  /* Danger */
  .button--standard.button--danger {
    background-color: var(--sl-color-danger-600);
    border-color: var(--sl-color-danger-600);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--danger:hover:not(.button--disabled) {
    background-color: var(--sl-color-danger-500);
    border-color: var(--sl-color-danger-500);
    color: var(--sl-color-neutral-0);
  }

  .button--standard.button--danger:active:not(.button--disabled) {
    background-color: var(--sl-color-danger-600);
    border-color: var(--sl-color-danger-600);
    color: var(--sl-color-neutral-0);
  }

  /*
   * Outline buttons
   */

  .button--outline {
    background: none;
    border: solid 1px;
  }

  /* Default */
  .button--outline.button--default {
    border-color: var(--sl-input-border-color);
    color: var(--sl-color-neutral-700);
  }

  .button--outline.button--default:hover:not(.button--disabled),
  .button--outline.button--default.button--checked:not(.button--disabled) {
    border-color: var(--sl-color-primary-600);
    background-color: var(--sl-color-primary-600);
    color: var(--sl-color-neutral-0);
  }

  .button--outline.button--default:active:not(.button--disabled) {
    border-color: var(--sl-color-primary-700);
    background-color: var(--sl-color-primary-700);
    color: var(--sl-color-neutral-0);
  }

  /* Primary */
  .button--outline.button--primary {
    border-color: var(--sl-color-primary-600);
    color: var(--sl-color-primary-600);
  }

  .button--outline.button--primary:hover:not(.button--disabled),
  .button--outline.button--primary.button--checked:not(.button--disabled) {
    background-color: var(--sl-color-primary-600);
    color: var(--sl-color-neutral-0);
  }

  .button--outline.button--primary:active:not(.button--disabled) {
    border-color: var(--sl-color-primary-700);
    background-color: var(--sl-color-primary-700);
    color: var(--sl-color-neutral-0);
  }

  /* Success */
  .button--outline.button--success {
    border-color: var(--sl-color-success-600);
    color: var(--sl-color-success-600);
  }

  .button--outline.button--success:hover:not(.button--disabled),
  .button--outline.button--success.button--checked:not(.button--disabled) {
    background-color: var(--sl-color-success-600);
    color: var(--sl-color-neutral-0);
  }

  .button--outline.button--success:active:not(.button--disabled) {
    border-color: var(--sl-color-success-700);
    background-color: var(--sl-color-success-700);
    color: var(--sl-color-neutral-0);
  }

  /* Neutral */
  .button--outline.button--neutral {
    border-color: var(--sl-color-neutral-600);
    color: var(--sl-color-neutral-600);
  }

  .button--outline.button--neutral:hover:not(.button--disabled),
  .button--outline.button--neutral.button--checked:not(.button--disabled) {
    background-color: var(--sl-color-neutral-600);
    color: var(--sl-color-neutral-0);
  }

  .button--outline.button--neutral:active:not(.button--disabled) {
    border-color: var(--sl-color-neutral-700);
    background-color: var(--sl-color-neutral-700);
    color: var(--sl-color-neutral-0);
  }

  /* Warning */
  .button--outline.button--warning {
    border-color: var(--sl-color-warning-600);
    color: var(--sl-color-warning-600);
  }

  .button--outline.button--warning:hover:not(.button--disabled),
  .button--outline.button--warning.button--checked:not(.button--disabled) {
    background-color: var(--sl-color-warning-600);
    color: var(--sl-color-neutral-0);
  }

  .button--outline.button--warning:active:not(.button--disabled) {
    border-color: var(--sl-color-warning-700);
    background-color: var(--sl-color-warning-700);
    color: var(--sl-color-neutral-0);
  }

  /* Danger */
  .button--outline.button--danger {
    border-color: var(--sl-color-danger-600);
    color: var(--sl-color-danger-600);
  }

  .button--outline.button--danger:hover:not(.button--disabled),
  .button--outline.button--danger.button--checked:not(.button--disabled) {
    background-color: var(--sl-color-danger-600);
    color: var(--sl-color-neutral-0);
  }

  .button--outline.button--danger:active:not(.button--disabled) {
    border-color: var(--sl-color-danger-700);
    background-color: var(--sl-color-danger-700);
    color: var(--sl-color-neutral-0);
  }

  @media (forced-colors: active) {
    .button.button--outline.button--checked:not(.button--disabled) {
      outline: solid 2px transparent;
    }
  }

  /*
   * Text buttons
   */

  .button--text {
    background-color: transparent;
    border-color: transparent;
    color: var(--sl-color-primary-600);
  }

  .button--text:hover:not(.button--disabled) {
    background-color: transparent;
    border-color: transparent;
    color: var(--sl-color-primary-500);
  }

  .button--text:focus-visible:not(.button--disabled) {
    background-color: transparent;
    border-color: transparent;
    color: var(--sl-color-primary-500);
  }

  .button--text:active:not(.button--disabled) {
    background-color: transparent;
    border-color: transparent;
    color: var(--sl-color-primary-700);
  }

  /*
   * Size modifiers
   */

  .button--small {
    height: auto;
    min-height: var(--sl-input-height-small);
    font-size: var(--sl-button-font-size-small);
    line-height: calc(var(--sl-input-height-small) - var(--sl-input-border-width) * 2);
    border-radius: var(--sl-input-border-radius-small);
  }

  .button--medium {
    height: auto;
    min-height: var(--sl-input-height-medium);
    font-size: var(--sl-button-font-size-medium);
    line-height: calc(var(--sl-input-height-medium) - var(--sl-input-border-width) * 2);
    border-radius: var(--sl-input-border-radius-medium);
  }

  .button--large {
    height: auto;
    min-height: var(--sl-input-height-large);
    font-size: var(--sl-button-font-size-large);
    line-height: calc(var(--sl-input-height-large) - var(--sl-input-border-width) * 2);
    border-radius: var(--sl-input-border-radius-large);
  }

  /*
   * Pill modifier
   */

  .button--pill.button--small {
    border-radius: var(--sl-input-height-small);
  }

  .button--pill.button--medium {
    border-radius: var(--sl-input-height-medium);
  }

  .button--pill.button--large {
    border-radius: var(--sl-input-height-large);
  }

  /*
   * Circle modifier
   */

  .button--circle {
    padding-left: 0;
    padding-right: 0;
  }

  .button--circle.button--small {
    width: var(--sl-input-height-small);
    border-radius: 50%;
  }

  .button--circle.button--medium {
    width: var(--sl-input-height-medium);
    border-radius: 50%;
  }

  .button--circle.button--large {
    width: var(--sl-input-height-large);
    border-radius: 50%;
  }

  .button--circle .button__prefix,
  .button--circle .button__suffix,
  .button--circle .button__caret {
    display: none;
  }

  /*
   * Caret modifier
   */

  .button--caret .button__suffix {
    display: none;
  }

  .button--caret .button__caret {
    height: auto;
  }

  /*
   * Loading modifier
   */

  .button--loading {
    position: relative;
    cursor: wait;
  }

  .button--loading .button__prefix,
  .button--loading .button__label,
  .button--loading .button__suffix,
  .button--loading .button__caret {
    visibility: hidden;
  }

  .button--loading sl-spinner {
    --indicator-color: currentColor;
    position: absolute;
    font-size: 1em;
    height: 1em;
    width: 1em;
    top: calc(50% - 0.5em);
    left: calc(50% - 0.5em);
  }

  /*
   * Badges
   */

  .button ::slotted(sl-badge) {
    position: absolute;
    top: 0;
    right: 0;
    translate: 50% -50%;
    pointer-events: none;
  }

  .button--rtl ::slotted(sl-badge) {
    right: auto;
    left: 0;
    translate: -50% -50%;
  }

  /*
   * Button spacing
   */

  .button--has-label.button--small .button__label {
    padding: 0 var(--sl-spacing-small);
  }

  .button--has-label.button--medium .button__label {
    padding: 0 var(--sl-spacing-medium);
  }

  .button--has-label.button--large .button__label {
    padding: 0 var(--sl-spacing-large);
  }

  .button--has-prefix.button--small {
    padding-inline-start: var(--sl-spacing-x-small);
  }

  .button--has-prefix.button--small .button__label {
    padding-inline-start: var(--sl-spacing-x-small);
  }

  .button--has-prefix.button--medium {
    padding-inline-start: var(--sl-spacing-small);
  }

  .button--has-prefix.button--medium .button__label {
    padding-inline-start: var(--sl-spacing-small);
  }

  .button--has-prefix.button--large {
    padding-inline-start: var(--sl-spacing-small);
  }

  .button--has-prefix.button--large .button__label {
    padding-inline-start: var(--sl-spacing-small);
  }

  .button--has-suffix.button--small,
  .button--caret.button--small {
    padding-inline-end: var(--sl-spacing-x-small);
  }

  .button--has-suffix.button--small .button__label,
  .button--caret.button--small .button__label {
    padding-inline-end: var(--sl-spacing-x-small);
  }

  .button--has-suffix.button--medium,
  .button--caret.button--medium {
    padding-inline-end: var(--sl-spacing-small);
  }

  .button--has-suffix.button--medium .button__label,
  .button--caret.button--medium .button__label {
    padding-inline-end: var(--sl-spacing-small);
  }

  .button--has-suffix.button--large,
  .button--caret.button--large {
    padding-inline-end: var(--sl-spacing-small);
  }

  .button--has-suffix.button--large .button__label,
  .button--caret.button--large .button__label {
    padding-inline-end: var(--sl-spacing-small);
  }

  /*
   * Button groups support a variety of button types (e.g. buttons with tooltips, buttons as dropdown triggers, etc.).
   * This means buttons aren't always direct descendants of the button group, thus we can't target them with the
   * ::slotted selector. To work around this, the button group component does some magic to add these special classes to
   * buttons and we style them here instead.
   */

  :host([data-sl-button-group__button--first]:not([data-sl-button-group__button--last])) .button {
    border-start-end-radius: 0;
    border-end-end-radius: 0;
  }

  :host([data-sl-button-group__button--inner]) .button {
    border-radius: 0;
  }

  :host([data-sl-button-group__button--last]:not([data-sl-button-group__button--first])) .button {
    border-start-start-radius: 0;
    border-end-start-radius: 0;
  }

  /* All except the first */
  :host([data-sl-button-group__button]:not([data-sl-button-group__button--first])) {
    margin-inline-start: calc(-1 * var(--sl-input-border-width));
  }

  /* Add a visual separator between solid buttons */
  :host(
      [data-sl-button-group__button]:not(
          [data-sl-button-group__button--first],
          [data-sl-button-group__button--radio],
          [variant='default']
        ):not(:hover)
    )
    .button:after {
    content: '';
    position: absolute;
    top: 0;
    inset-inline-start: 0;
    bottom: 0;
    border-left: solid 1px rgb(128 128 128 / 33%);
    mix-blend-mode: multiply;
  }

  /* Bump hovered, focused, and checked buttons up so their focus ring isn't clipped */
  :host([data-sl-button-group__button--hover]) {
    z-index: 1;
  }

  /* Focus and checked are always on top */
  :host([data-sl-button-group__button--focus]),
  :host([data-sl-button-group__button][checked]) {
    z-index: 2;
  }
`,
  wC = st`
  ${_f}

  .button__prefix,
  .button__suffix,
  .button__label {
    display: inline-flex;
    position: relative;
    align-items: center;
  }

  /* We use a hidden input so constraint validation errors work, since they don't appear to show when used with buttons.
    We can't actually hide it, though, otherwise the messages will be suppressed by the browser. */
  .hidden-input {
    all: unset;
    position: absolute;
    top: 0;
    left: 0;
    bottom: 0;
    right: 0;
    outline: dotted 1px red;
    opacity: 0;
    z-index: -1;
  }
`,
  Mr = class extends nt {
    constructor() {
      super(...arguments),
        (this.hasSlotController = new He(
          this,
          '[default]',
          'prefix',
          'suffix',
        )),
        (this.hasFocus = !1),
        (this.checked = !1),
        (this.disabled = !1),
        (this.size = 'medium'),
        (this.pill = !1)
    }
    connectedCallback() {
      super.connectedCallback(), this.setAttribute('role', 'presentation')
    }
    handleBlur() {
      ;(this.hasFocus = !1), this.emit('sl-blur')
    }
    handleClick(t) {
      if (this.disabled) {
        t.preventDefault(), t.stopPropagation()
        return
      }
      this.checked = !0
    }
    handleFocus() {
      ;(this.hasFocus = !0), this.emit('sl-focus')
    }
    handleDisabledChange() {
      this.setAttribute('aria-disabled', this.disabled ? 'true' : 'false')
    }
    /** Sets focus on the radio button. */
    focus(t) {
      this.input.focus(t)
    }
    /** Removes focus from the radio button. */
    blur() {
      this.input.blur()
    }
    render() {
      return xo`
      <div part="base" role="presentation">
        <button
          part="${`button${this.checked ? ' button--checked' : ''}`}"
          role="radio"
          aria-checked="${this.checked}"
          class=${ct({
            button: !0,
            'button--default': !0,
            'button--small': this.size === 'small',
            'button--medium': this.size === 'medium',
            'button--large': this.size === 'large',
            'button--checked': this.checked,
            'button--disabled': this.disabled,
            'button--focused': this.hasFocus,
            'button--outline': !0,
            'button--pill': this.pill,
            'button--has-label': this.hasSlotController.test('[default]'),
            'button--has-prefix': this.hasSlotController.test('prefix'),
            'button--has-suffix': this.hasSlotController.test('suffix'),
          })}
          aria-disabled=${this.disabled}
          type="button"
          value=${it(this.value)}
          @blur=${this.handleBlur}
          @focus=${this.handleFocus}
          @click=${this.handleClick}
        >
          <slot name="prefix" part="prefix" class="button__prefix"></slot>
          <slot part="label" class="button__label"></slot>
          <slot name="suffix" part="suffix" class="button__suffix"></slot>
        </button>
      </div>
    `
    }
  }
Mr.styles = [dt, wC]
c([J('.button')], Mr.prototype, 'input', 2)
c([J('.hidden-input')], Mr.prototype, 'hiddenInput', 2)
c([at()], Mr.prototype, 'hasFocus', 2)
c([p({ type: Boolean, reflect: !0 })], Mr.prototype, 'checked', 2)
c([p()], Mr.prototype, 'value', 2)
c([p({ type: Boolean, reflect: !0 })], Mr.prototype, 'disabled', 2)
c([p({ reflect: !0 })], Mr.prototype, 'size', 2)
c([p({ type: Boolean, reflect: !0 })], Mr.prototype, 'pill', 2)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  Mr.prototype,
  'handleDisabledChange',
  1,
)
Mr.define('sl-radio-button')
var xC = st`
  :host {
    display: block;
  }

  .form-control {
    position: relative;
    border: none;
    padding: 0;
    margin: 0;
  }

  .form-control__label {
    padding: 0;
  }

  .radio-group--required .radio-group__label::after {
    content: var(--sl-input-required-content);
    margin-inline-start: var(--sl-input-required-content-offset);
  }

  .visually-hidden {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }
`,
  kC = st`
  :host {
    display: inline-block;
  }

  .button-group {
    display: flex;
    flex-wrap: nowrap;
  }
`,
  hn = class extends nt {
    constructor() {
      super(...arguments), (this.disableRole = !1), (this.label = '')
    }
    handleFocus(t) {
      const r = vo(t.target)
      r == null || r.toggleAttribute('data-sl-button-group__button--focus', !0)
    }
    handleBlur(t) {
      const r = vo(t.target)
      r == null || r.toggleAttribute('data-sl-button-group__button--focus', !1)
    }
    handleMouseOver(t) {
      const r = vo(t.target)
      r == null || r.toggleAttribute('data-sl-button-group__button--hover', !0)
    }
    handleMouseOut(t) {
      const r = vo(t.target)
      r == null || r.toggleAttribute('data-sl-button-group__button--hover', !1)
    }
    handleSlotChange() {
      const t = [...this.defaultSlot.assignedElements({ flatten: !0 })]
      t.forEach(r => {
        const i = t.indexOf(r),
          o = vo(r)
        o &&
          (o.toggleAttribute('data-sl-button-group__button', !0),
          o.toggleAttribute('data-sl-button-group__button--first', i === 0),
          o.toggleAttribute(
            'data-sl-button-group__button--inner',
            i > 0 && i < t.length - 1,
          ),
          o.toggleAttribute(
            'data-sl-button-group__button--last',
            i === t.length - 1,
          ),
          o.toggleAttribute(
            'data-sl-button-group__button--radio',
            o.tagName.toLowerCase() === 'sl-radio-button',
          ))
      })
    }
    render() {
      return C`
      <div
        part="base"
        class="button-group"
        role="${this.disableRole ? 'presentation' : 'group'}"
        aria-label=${this.label}
        @focusout=${this.handleBlur}
        @focusin=${this.handleFocus}
        @mouseover=${this.handleMouseOver}
        @mouseout=${this.handleMouseOut}
      >
        <slot @slotchange=${this.handleSlotChange}></slot>
      </div>
    `
    }
  }
hn.styles = [dt, kC]
c([J('slot')], hn.prototype, 'defaultSlot', 2)
c([at()], hn.prototype, 'disableRole', 2)
c([p()], hn.prototype, 'label', 2)
function vo(t) {
  var r
  const i = 'sl-button, sl-radio-button'
  return (r = t.closest(i)) != null ? r : t.querySelector(i)
}
var Ae = class extends nt {
  constructor() {
    super(...arguments),
      (this.formControlController = new mi(this)),
      (this.hasSlotController = new He(this, 'help-text', 'label')),
      (this.customValidityMessage = ''),
      (this.hasButtonGroup = !1),
      (this.errorMessage = ''),
      (this.defaultValue = ''),
      (this.label = ''),
      (this.helpText = ''),
      (this.name = 'option'),
      (this.value = ''),
      (this.size = 'medium'),
      (this.form = ''),
      (this.required = !1)
  }
  /** Gets the validity state object */
  get validity() {
    const t = this.required && !this.value
    return this.customValidityMessage !== '' ? nC : t ? iC : da
  }
  /** Gets the validation message */
  get validationMessage() {
    const t = this.required && !this.value
    return this.customValidityMessage !== ''
      ? this.customValidityMessage
      : t
      ? this.validationInput.validationMessage
      : ''
  }
  connectedCallback() {
    super.connectedCallback(), (this.defaultValue = this.value)
  }
  firstUpdated() {
    this.formControlController.updateValidity()
  }
  getAllRadios() {
    return [...this.querySelectorAll('sl-radio, sl-radio-button')]
  }
  handleRadioClick(t) {
    const r = t.target.closest('sl-radio, sl-radio-button'),
      i = this.getAllRadios(),
      o = this.value
    !r ||
      r.disabled ||
      ((this.value = r.value),
      i.forEach(a => (a.checked = a === r)),
      this.value !== o && (this.emit('sl-change'), this.emit('sl-input')))
  }
  handleKeyDown(t) {
    var r
    if (
      !['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight', ' '].includes(t.key)
    )
      return
    const i = this.getAllRadios().filter(m => !m.disabled),
      o = (r = i.find(m => m.checked)) != null ? r : i[0],
      a = t.key === ' ' ? 0 : ['ArrowUp', 'ArrowLeft'].includes(t.key) ? -1 : 1,
      d = this.value
    let h = i.indexOf(o) + a
    h < 0 && (h = i.length - 1),
      h > i.length - 1 && (h = 0),
      this.getAllRadios().forEach(m => {
        ;(m.checked = !1),
          this.hasButtonGroup || m.setAttribute('tabindex', '-1')
      }),
      (this.value = i[h].value),
      (i[h].checked = !0),
      this.hasButtonGroup
        ? i[h].shadowRoot.querySelector('button').focus()
        : (i[h].setAttribute('tabindex', '0'), i[h].focus()),
      this.value !== d && (this.emit('sl-change'), this.emit('sl-input')),
      t.preventDefault()
  }
  handleLabelClick() {
    this.focus()
  }
  handleInvalid(t) {
    this.formControlController.setValidity(!1),
      this.formControlController.emitInvalidEvent(t)
  }
  async syncRadioElements() {
    var t, r
    const i = this.getAllRadios()
    if (
      (await Promise.all(
        // Sync the checked state and size
        i.map(async o => {
          await o.updateComplete,
            (o.checked = o.value === this.value),
            (o.size = this.size)
        }),
      ),
      (this.hasButtonGroup = i.some(
        o => o.tagName.toLowerCase() === 'sl-radio-button',
      )),
      i.length > 0 && !i.some(o => o.checked))
    )
      if (this.hasButtonGroup) {
        const o =
          (t = i[0].shadowRoot) == null ? void 0 : t.querySelector('button')
        o && o.setAttribute('tabindex', '0')
      } else i[0].setAttribute('tabindex', '0')
    if (this.hasButtonGroup) {
      const o =
        (r = this.shadowRoot) == null
          ? void 0
          : r.querySelector('sl-button-group')
      o && (o.disableRole = !0)
    }
  }
  syncRadios() {
    if (
      customElements.get('sl-radio') &&
      customElements.get('sl-radio-button')
    ) {
      this.syncRadioElements()
      return
    }
    customElements.get('sl-radio')
      ? this.syncRadioElements()
      : customElements.whenDefined('sl-radio').then(() => this.syncRadios()),
      customElements.get('sl-radio-button')
        ? this.syncRadioElements()
        : customElements
            .whenDefined('sl-radio-button')
            .then(() => this.syncRadios())
  }
  updateCheckedRadio() {
    this.getAllRadios().forEach(r => (r.checked = r.value === this.value)),
      this.formControlController.setValidity(this.validity.valid)
  }
  handleSizeChange() {
    this.syncRadios()
  }
  handleValueChange() {
    this.hasUpdated && this.updateCheckedRadio()
  }
  /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
  checkValidity() {
    const t = this.required && !this.value,
      r = this.customValidityMessage !== ''
    return t || r ? (this.formControlController.emitInvalidEvent(), !1) : !0
  }
  /** Gets the associated form, if one exists. */
  getForm() {
    return this.formControlController.getForm()
  }
  /** Checks for validity and shows the browser's validation message if the control is invalid. */
  reportValidity() {
    const t = this.validity.valid
    return (
      (this.errorMessage =
        this.customValidityMessage || t
          ? ''
          : this.validationInput.validationMessage),
      this.formControlController.setValidity(t),
      (this.validationInput.hidden = !0),
      clearTimeout(this.validationTimeout),
      t ||
        ((this.validationInput.hidden = !1),
        this.validationInput.reportValidity(),
        (this.validationTimeout = setTimeout(
          () => (this.validationInput.hidden = !0),
          1e4,
        ))),
      t
    )
  }
  /** Sets a custom validation message. Pass an empty string to restore validity. */
  setCustomValidity(t = '') {
    ;(this.customValidityMessage = t),
      (this.errorMessage = t),
      this.validationInput.setCustomValidity(t),
      this.formControlController.updateValidity()
  }
  /** Sets focus on the radio-group. */
  focus(t) {
    const r = this.getAllRadios(),
      i = r.find(d => d.checked),
      o = r.find(d => !d.disabled),
      a = i || o
    a && a.focus(t)
  }
  render() {
    const t = this.hasSlotController.test('label'),
      r = this.hasSlotController.test('help-text'),
      i = this.label ? !0 : !!t,
      o = this.helpText ? !0 : !!r,
      a = C`
      <slot @slotchange=${this.syncRadios} @click=${this.handleRadioClick} @keydown=${this.handleKeyDown}></slot>
    `
    return C`
      <fieldset
        part="form-control"
        class=${ct({
          'form-control': !0,
          'form-control--small': this.size === 'small',
          'form-control--medium': this.size === 'medium',
          'form-control--large': this.size === 'large',
          'form-control--radio-group': !0,
          'form-control--has-label': i,
          'form-control--has-help-text': o,
        })}
        role="radiogroup"
        aria-labelledby="label"
        aria-describedby="help-text"
        aria-errormessage="error-message"
      >
        <label
          part="form-control-label"
          id="label"
          class="form-control__label"
          aria-hidden=${i ? 'false' : 'true'}
          @click=${this.handleLabelClick}
        >
          <slot name="label">${this.label}</slot>
        </label>

        <div part="form-control-input" class="form-control-input">
          <div class="visually-hidden">
            <div id="error-message" aria-live="assertive">${
              this.errorMessage
            }</div>
            <label class="radio-group__validation">
              <input
                type="text"
                class="radio-group__validation-input"
                ?required=${this.required}
                tabindex="-1"
                hidden
                @invalid=${this.handleInvalid}
              />
            </label>
          </div>

          ${
            this.hasButtonGroup
              ? C`
                <sl-button-group part="button-group" exportparts="base:button-group__base" role="presentation">
                  ${a}
                </sl-button-group>
              `
              : a
          }
        </div>

        <div
          part="form-control-help-text"
          id="help-text"
          class="form-control__help-text"
          aria-hidden=${o ? 'false' : 'true'}
        >
          <slot name="help-text">${this.helpText}</slot>
        </div>
      </fieldset>
    `
  }
}
Ae.styles = [dt, ln, xC]
Ae.dependencies = { 'sl-button-group': hn }
c([J('slot:not([name])')], Ae.prototype, 'defaultSlot', 2)
c([J('.radio-group__validation-input')], Ae.prototype, 'validationInput', 2)
c([at()], Ae.prototype, 'hasButtonGroup', 2)
c([at()], Ae.prototype, 'errorMessage', 2)
c([at()], Ae.prototype, 'defaultValue', 2)
c([p()], Ae.prototype, 'label', 2)
c([p({ attribute: 'help-text' })], Ae.prototype, 'helpText', 2)
c([p()], Ae.prototype, 'name', 2)
c([p({ reflect: !0 })], Ae.prototype, 'value', 2)
c([p({ reflect: !0 })], Ae.prototype, 'size', 2)
c([p({ reflect: !0 })], Ae.prototype, 'form', 2)
c([p({ type: Boolean, reflect: !0 })], Ae.prototype, 'required', 2)
c(
  [X('size', { waitUntilFirstUpdate: !0 })],
  Ae.prototype,
  'handleSizeChange',
  1,
)
c([X('value')], Ae.prototype, 'handleValueChange', 1)
Ae.define('sl-radio-group')
var $C = st`
  :host {
    --size: 128px;
    --track-width: 4px;
    --track-color: var(--sl-color-neutral-200);
    --indicator-width: var(--track-width);
    --indicator-color: var(--sl-color-primary-600);
    --indicator-transition-duration: 0.35s;

    display: inline-flex;
  }

  .progress-ring {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    position: relative;
  }

  .progress-ring__image {
    width: var(--size);
    height: var(--size);
    rotate: -90deg;
    transform-origin: 50% 50%;
  }

  .progress-ring__track,
  .progress-ring__indicator {
    --radius: calc(var(--size) / 2 - max(var(--track-width), var(--indicator-width)) * 0.5);
    --circumference: calc(var(--radius) * 2 * 3.141592654);

    fill: none;
    r: var(--radius);
    cx: calc(var(--size) / 2);
    cy: calc(var(--size) / 2);
  }

  .progress-ring__track {
    stroke: var(--track-color);
    stroke-width: var(--track-width);
  }

  .progress-ring__indicator {
    stroke: var(--indicator-color);
    stroke-width: var(--indicator-width);
    stroke-linecap: round;
    transition-property: stroke-dashoffset;
    transition-duration: var(--indicator-transition-duration);
    stroke-dasharray: var(--circumference) var(--circumference);
    stroke-dashoffset: calc(var(--circumference) - var(--percentage) * var(--circumference));
  }

  .progress-ring__label {
    display: flex;
    align-items: center;
    justify-content: center;
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    text-align: center;
    user-select: none;
    -webkit-user-select: none;
  }
`,
  Un = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.value = 0),
        (this.label = '')
    }
    updated(t) {
      if ((super.updated(t), t.has('value'))) {
        const r = parseFloat(
            getComputedStyle(this.indicator).getPropertyValue('r'),
          ),
          i = 2 * Math.PI * r,
          o = i - (this.value / 100) * i
        this.indicatorOffset = `${o}px`
      }
    }
    render() {
      return C`
      <div
        part="base"
        class="progress-ring"
        role="progressbar"
        aria-label=${
          this.label.length > 0 ? this.label : this.localize.term('progress')
        }
        aria-describedby="label"
        aria-valuemin="0"
        aria-valuemax="100"
        aria-valuenow="${this.value}"
        style="--percentage: ${this.value / 100}"
      >
        <svg class="progress-ring__image">
          <circle class="progress-ring__track"></circle>
          <circle class="progress-ring__indicator" style="stroke-dashoffset: ${
            this.indicatorOffset
          }"></circle>
        </svg>

        <slot id="label" part="label" class="progress-ring__label"></slot>
      </div>
    `
    }
  }
Un.styles = [dt, $C]
c([J('.progress-ring__indicator')], Un.prototype, 'indicator', 2)
c([at()], Un.prototype, 'indicatorOffset', 2)
c([p({ type: Number, reflect: !0 })], Un.prototype, 'value', 2)
c([p()], Un.prototype, 'label', 2)
Un.define('sl-progress-ring')
var CC = st`
  :host {
    display: inline-block;
  }
`
let wf = null
class xf {}
xf.render = function (t, r) {
  wf(t, r)
}
self.QrCreator = xf
;(function (t) {
  function r(m, f, b, A) {
    var y = {},
      k = t(b, f)
    k.u(m), k.J(), (A = A || 0)
    var S = k.h(),
      $ = k.h() + 2 * A
    return (
      (y.text = m),
      (y.level = f),
      (y.version = b),
      (y.O = $),
      (y.a = function (w, T) {
        return (
          (w -= A),
          (T -= A),
          0 > w || w >= S || 0 > T || T >= S ? !1 : k.a(w, T)
        )
      }),
      y
    )
  }
  function i(m, f, b, A, y, k, S, $, w, T) {
    function P(D, R, O, L, U, H, F) {
      D ? (m.lineTo(R + H, O + F), m.arcTo(R, O, L, U, k)) : m.lineTo(R, O)
    }
    S ? m.moveTo(f + k, b) : m.moveTo(f, b),
      P($, A, b, A, y, -k, 0),
      P(w, A, y, f, y, 0, -k),
      P(T, f, y, f, b, k, 0),
      P(S, f, b, A, b, 0, k)
  }
  function o(m, f, b, A, y, k, S, $, w, T) {
    function P(D, R, O, L) {
      m.moveTo(D + O, R),
        m.lineTo(D, R),
        m.lineTo(D, R + L),
        m.arcTo(D, R, D + O, R, k)
    }
    S && P(f, b, k, k),
      $ && P(A, b, -k, k),
      w && P(A, y, -k, -k),
      T && P(f, y, k, -k)
  }
  function a(m, f) {
    var b = f.fill
    if (typeof b == 'string') m.fillStyle = b
    else {
      var A = b.type,
        y = b.colorStops
      if (
        ((b = b.position.map(S => Math.round(S * f.size))),
        A === 'linear-gradient')
      )
        var k = m.createLinearGradient.apply(m, b)
      else if (A === 'radial-gradient') k = m.createRadialGradient.apply(m, b)
      else throw Error('Unsupported fill')
      y.forEach(([S, $]) => {
        k.addColorStop(S, $)
      }),
        (m.fillStyle = k)
    }
  }
  function d(m, f) {
    t: {
      var b = f.text,
        A = f.v,
        y = f.N,
        k = f.K,
        S = f.P
      for (y = Math.max(1, y || 1), k = Math.min(40, k || 40); y <= k; y += 1)
        try {
          var $ = r(b, A, y, S)
          break t
        } catch {}
      $ = void 0
    }
    if (!$) return null
    for (
      b = m.getContext('2d'),
        f.background &&
          ((b.fillStyle = f.background),
          b.fillRect(f.left, f.top, f.size, f.size)),
        A = $.O,
        k = f.size / A,
        b.beginPath(),
        S = 0;
      S < A;
      S += 1
    )
      for (y = 0; y < A; y += 1) {
        var w = b,
          T = f.left + y * k,
          P = f.top + S * k,
          D = S,
          R = y,
          O = $.a,
          L = T + k,
          U = P + k,
          H = D - 1,
          F = D + 1,
          K = R - 1,
          q = R + 1,
          et = Math.floor(Math.min(0.5, Math.max(0, f.R)) * k),
          mt = O(D, R),
          Ot = O(H, K),
          Z = O(H, R)
        H = O(H, q)
        var G = O(D, q)
        ;(q = O(F, q)),
          (R = O(F, R)),
          (F = O(F, K)),
          (D = O(D, K)),
          (T = Math.round(T)),
          (P = Math.round(P)),
          (L = Math.round(L)),
          (U = Math.round(U)),
          mt
            ? i(w, T, P, L, U, et, !Z && !D, !Z && !G, !R && !G, !R && !D)
            : o(
                w,
                T,
                P,
                L,
                U,
                et,
                Z && D && Ot,
                Z && G && H,
                R && G && q,
                R && D && F,
              )
      }
    return a(b, f), b.fill(), m
  }
  var h = {
    minVersion: 1,
    maxVersion: 40,
    ecLevel: 'L',
    left: 0,
    top: 0,
    size: 200,
    fill: '#000',
    background: null,
    text: 'no text',
    radius: 0.5,
    quiet: 0,
  }
  wf = function (m, f) {
    var b = {}
    Object.assign(b, h, m),
      (b.N = b.minVersion),
      (b.K = b.maxVersion),
      (b.v = b.ecLevel),
      (b.left = b.left),
      (b.top = b.top),
      (b.size = b.size),
      (b.fill = b.fill),
      (b.background = b.background),
      (b.text = b.text),
      (b.R = b.radius),
      (b.P = b.quiet),
      f instanceof HTMLCanvasElement
        ? ((f.width !== b.size || f.height !== b.size) &&
            ((f.width = b.size), (f.height = b.size)),
          f.getContext('2d').clearRect(0, 0, f.width, f.height),
          d(f, b))
        : ((m = document.createElement('canvas')),
          (m.width = b.size),
          (m.height = b.size),
          (b = d(m, b)),
          f.appendChild(b))
  }
})(
  (function () {
    function t(f) {
      var b = i.s(f)
      return {
        S: function () {
          return 4
        },
        b: function () {
          return b.length
        },
        write: function (A) {
          for (var y = 0; y < b.length; y += 1) A.put(b[y], 8)
        },
      }
    }
    function r() {
      var f = [],
        b = 0,
        A = {
          B: function () {
            return f
          },
          c: function (y) {
            return ((f[Math.floor(y / 8)] >>> (7 - (y % 8))) & 1) == 1
          },
          put: function (y, k) {
            for (var S = 0; S < k; S += 1) A.m(((y >>> (k - S - 1)) & 1) == 1)
          },
          f: function () {
            return b
          },
          m: function (y) {
            var k = Math.floor(b / 8)
            f.length <= k && f.push(0), y && (f[k] |= 128 >>> b % 8), (b += 1)
          },
        }
      return A
    }
    function i(f, b) {
      function A(D, R) {
        for (var O = -1; 7 >= O; O += 1)
          if (!(-1 >= D + O || $ <= D + O))
            for (var L = -1; 7 >= L; L += 1)
              -1 >= R + L ||
                $ <= R + L ||
                (S[D + O][R + L] =
                  (0 <= O && 6 >= O && (L == 0 || L == 6)) ||
                  (0 <= L && 6 >= L && (O == 0 || O == 6)) ||
                  (2 <= O && 4 >= O && 2 <= L && 4 >= L))
      }
      function y(D, R) {
        for (var O = ($ = 4 * f + 17), L = Array(O), U = 0; U < O; U += 1) {
          L[U] = Array(O)
          for (var H = 0; H < O; H += 1) L[U][H] = null
        }
        for (
          S = L, A(0, 0), A($ - 7, 0), A(0, $ - 7), O = d.G(f), L = 0;
          L < O.length;
          L += 1
        )
          for (U = 0; U < O.length; U += 1) {
            H = O[L]
            var F = O[U]
            if (S[H][F] == null)
              for (var K = -2; 2 >= K; K += 1)
                for (var q = -2; 2 >= q; q += 1)
                  S[H + K][F + q] =
                    K == -2 || K == 2 || q == -2 || q == 2 || (K == 0 && q == 0)
          }
        for (O = 8; O < $ - 8; O += 1) S[O][6] == null && (S[O][6] = O % 2 == 0)
        for (O = 8; O < $ - 8; O += 1) S[6][O] == null && (S[6][O] = O % 2 == 0)
        for (O = d.w((k << 3) | R), L = 0; 15 > L; L += 1)
          (U = !D && ((O >> L) & 1) == 1),
            (S[6 > L ? L : 8 > L ? L + 1 : $ - 15 + L][8] = U),
            (S[8][8 > L ? $ - L - 1 : 9 > L ? 15 - L : 14 - L] = U)
        if (((S[$ - 8][8] = !D), 7 <= f)) {
          for (O = d.A(f), L = 0; 18 > L; L += 1)
            (U = !D && ((O >> L) & 1) == 1),
              (S[Math.floor(L / 3)][(L % 3) + $ - 8 - 3] = U)
          for (L = 0; 18 > L; L += 1)
            (U = !D && ((O >> L) & 1) == 1),
              (S[(L % 3) + $ - 8 - 3][Math.floor(L / 3)] = U)
        }
        if (w == null) {
          for (D = m.I(f, k), O = r(), L = 0; L < T.length; L += 1)
            (U = T[L]), O.put(4, 4), O.put(U.b(), d.f(4, f)), U.write(O)
          for (L = U = 0; L < D.length; L += 1) U += D[L].j
          if (O.f() > 8 * U)
            throw Error('code length overflow. (' + O.f() + '>' + 8 * U + ')')
          for (O.f() + 4 <= 8 * U && O.put(0, 4); O.f() % 8 != 0; ) O.m(!1)
          for (; !(O.f() >= 8 * U) && (O.put(236, 8), !(O.f() >= 8 * U)); )
            O.put(17, 8)
          var et = 0
          for (
            U = L = 0, H = Array(D.length), F = Array(D.length), K = 0;
            K < D.length;
            K += 1
          ) {
            var mt = D[K].j,
              Ot = D[K].o - mt
            for (
              L = Math.max(L, mt), U = Math.max(U, Ot), H[K] = Array(mt), q = 0;
              q < H[K].length;
              q += 1
            )
              H[K][q] = 255 & O.B()[q + et]
            for (
              et += mt,
                q = d.C(Ot),
                mt = o(H[K], q.b() - 1).l(q),
                F[K] = Array(q.b() - 1),
                q = 0;
              q < F[K].length;
              q += 1
            )
              (Ot = q + mt.b() - F[K].length),
                (F[K][q] = 0 <= Ot ? mt.c(Ot) : 0)
          }
          for (q = O = 0; q < D.length; q += 1) O += D[q].o
          for (O = Array(O), q = et = 0; q < L; q += 1)
            for (K = 0; K < D.length; K += 1)
              q < H[K].length && ((O[et] = H[K][q]), (et += 1))
          for (q = 0; q < U; q += 1)
            for (K = 0; K < D.length; K += 1)
              q < F[K].length && ((O[et] = F[K][q]), (et += 1))
          w = O
        }
        for (
          D = w, O = -1, L = $ - 1, U = 7, H = 0, R = d.F(R), F = $ - 1;
          0 < F;
          F -= 2
        )
          for (F == 6 && --F; ; ) {
            for (K = 0; 2 > K; K += 1)
              S[L][F - K] == null &&
                ((q = !1),
                H < D.length && (q = ((D[H] >>> U) & 1) == 1),
                R(L, F - K) && (q = !q),
                (S[L][F - K] = q),
                --U,
                U == -1 && ((H += 1), (U = 7)))
            if (((L += O), 0 > L || $ <= L)) {
              ;(L -= O), (O = -O)
              break
            }
          }
      }
      var k = a[b],
        S = null,
        $ = 0,
        w = null,
        T = [],
        P = {
          u: function (D) {
            ;(D = t(D)), T.push(D), (w = null)
          },
          a: function (D, R) {
            if (0 > D || $ <= D || 0 > R || $ <= R) throw Error(D + ',' + R)
            return S[D][R]
          },
          h: function () {
            return $
          },
          J: function () {
            for (var D = 0, R = 0, O = 0; 8 > O; O += 1) {
              y(!0, O)
              var L = d.D(P)
              ;(O == 0 || D > L) && ((D = L), (R = O))
            }
            y(!1, R)
          },
        }
      return P
    }
    function o(f, b) {
      if (typeof f.length > 'u') throw Error(f.length + '/' + b)
      var A = (function () {
          for (var k = 0; k < f.length && f[k] == 0; ) k += 1
          for (var S = Array(f.length - k + b), $ = 0; $ < f.length - k; $ += 1)
            S[$] = f[$ + k]
          return S
        })(),
        y = {
          c: function (k) {
            return A[k]
          },
          b: function () {
            return A.length
          },
          multiply: function (k) {
            for (var S = Array(y.b() + k.b() - 1), $ = 0; $ < y.b(); $ += 1)
              for (var w = 0; w < k.b(); w += 1)
                S[$ + w] ^= h.i(h.g(y.c($)) + h.g(k.c(w)))
            return o(S, 0)
          },
          l: function (k) {
            if (0 > y.b() - k.b()) return y
            for (
              var S = h.g(y.c(0)) - h.g(k.c(0)), $ = Array(y.b()), w = 0;
              w < y.b();
              w += 1
            )
              $[w] = y.c(w)
            for (w = 0; w < k.b(); w += 1) $[w] ^= h.i(h.g(k.c(w)) + S)
            return o($, 0).l(k)
          },
        }
      return y
    }
    i.s = function (f) {
      for (var b = [], A = 0; A < f.length; A++) {
        var y = f.charCodeAt(A)
        128 > y
          ? b.push(y)
          : 2048 > y
          ? b.push(192 | (y >> 6), 128 | (y & 63))
          : 55296 > y || 57344 <= y
          ? b.push(224 | (y >> 12), 128 | ((y >> 6) & 63), 128 | (y & 63))
          : (A++,
            (y = 65536 + (((y & 1023) << 10) | (f.charCodeAt(A) & 1023))),
            b.push(
              240 | (y >> 18),
              128 | ((y >> 12) & 63),
              128 | ((y >> 6) & 63),
              128 | (y & 63),
            ))
      }
      return b
    }
    var a = { L: 1, M: 0, Q: 3, H: 2 },
      d = /* @__PURE__ */ (function () {
        function f(y) {
          for (var k = 0; y != 0; ) (k += 1), (y >>>= 1)
          return k
        }
        var b = [
            [],
            [6, 18],
            [6, 22],
            [6, 26],
            [6, 30],
            [6, 34],
            [6, 22, 38],
            [6, 24, 42],
            [6, 26, 46],
            [6, 28, 50],
            [6, 30, 54],
            [6, 32, 58],
            [6, 34, 62],
            [6, 26, 46, 66],
            [6, 26, 48, 70],
            [6, 26, 50, 74],
            [6, 30, 54, 78],
            [6, 30, 56, 82],
            [6, 30, 58, 86],
            [6, 34, 62, 90],
            [6, 28, 50, 72, 94],
            [6, 26, 50, 74, 98],
            [6, 30, 54, 78, 102],
            [6, 28, 54, 80, 106],
            [6, 32, 58, 84, 110],
            [6, 30, 58, 86, 114],
            [6, 34, 62, 90, 118],
            [6, 26, 50, 74, 98, 122],
            [6, 30, 54, 78, 102, 126],
            [6, 26, 52, 78, 104, 130],
            [6, 30, 56, 82, 108, 134],
            [6, 34, 60, 86, 112, 138],
            [6, 30, 58, 86, 114, 142],
            [6, 34, 62, 90, 118, 146],
            [6, 30, 54, 78, 102, 126, 150],
            [6, 24, 50, 76, 102, 128, 154],
            [6, 28, 54, 80, 106, 132, 158],
            [6, 32, 58, 84, 110, 136, 162],
            [6, 26, 54, 82, 110, 138, 166],
            [6, 30, 58, 86, 114, 142, 170],
          ],
          A = {
            w: function (y) {
              for (var k = y << 10; 0 <= f(k) - f(1335); )
                k ^= 1335 << (f(k) - f(1335))
              return ((y << 10) | k) ^ 21522
            },
            A: function (y) {
              for (var k = y << 12; 0 <= f(k) - f(7973); )
                k ^= 7973 << (f(k) - f(7973))
              return (y << 12) | k
            },
            G: function (y) {
              return b[y - 1]
            },
            F: function (y) {
              switch (y) {
                case 0:
                  return function (k, S) {
                    return (k + S) % 2 == 0
                  }
                case 1:
                  return function (k) {
                    return k % 2 == 0
                  }
                case 2:
                  return function (k, S) {
                    return S % 3 == 0
                  }
                case 3:
                  return function (k, S) {
                    return (k + S) % 3 == 0
                  }
                case 4:
                  return function (k, S) {
                    return (Math.floor(k / 2) + Math.floor(S / 3)) % 2 == 0
                  }
                case 5:
                  return function (k, S) {
                    return ((k * S) % 2) + ((k * S) % 3) == 0
                  }
                case 6:
                  return function (k, S) {
                    return (((k * S) % 2) + ((k * S) % 3)) % 2 == 0
                  }
                case 7:
                  return function (k, S) {
                    return (((k * S) % 3) + ((k + S) % 2)) % 2 == 0
                  }
                default:
                  throw Error('bad maskPattern:' + y)
              }
            },
            C: function (y) {
              for (var k = o([1], 0), S = 0; S < y; S += 1)
                k = k.multiply(o([1, h.i(S)], 0))
              return k
            },
            f: function (y, k) {
              if (y != 4 || 1 > k || 40 < k)
                throw Error('mode: ' + y + '; type: ' + k)
              return 10 > k ? 8 : 16
            },
            D: function (y) {
              for (var k = y.h(), S = 0, $ = 0; $ < k; $ += 1)
                for (var w = 0; w < k; w += 1) {
                  for (var T = 0, P = y.a($, w), D = -1; 1 >= D; D += 1)
                    if (!(0 > $ + D || k <= $ + D))
                      for (var R = -1; 1 >= R; R += 1)
                        0 > w + R ||
                          k <= w + R ||
                          ((D != 0 || R != 0) &&
                            P == y.a($ + D, w + R) &&
                            (T += 1))
                  5 < T && (S += 3 + T - 5)
                }
              for ($ = 0; $ < k - 1; $ += 1)
                for (w = 0; w < k - 1; w += 1)
                  (T = 0),
                    y.a($, w) && (T += 1),
                    y.a($ + 1, w) && (T += 1),
                    y.a($, w + 1) && (T += 1),
                    y.a($ + 1, w + 1) && (T += 1),
                    (T == 0 || T == 4) && (S += 3)
              for ($ = 0; $ < k; $ += 1)
                for (w = 0; w < k - 6; w += 1)
                  y.a($, w) &&
                    !y.a($, w + 1) &&
                    y.a($, w + 2) &&
                    y.a($, w + 3) &&
                    y.a($, w + 4) &&
                    !y.a($, w + 5) &&
                    y.a($, w + 6) &&
                    (S += 40)
              for (w = 0; w < k; w += 1)
                for ($ = 0; $ < k - 6; $ += 1)
                  y.a($, w) &&
                    !y.a($ + 1, w) &&
                    y.a($ + 2, w) &&
                    y.a($ + 3, w) &&
                    y.a($ + 4, w) &&
                    !y.a($ + 5, w) &&
                    y.a($ + 6, w) &&
                    (S += 40)
              for (w = T = 0; w < k; w += 1)
                for ($ = 0; $ < k; $ += 1) y.a($, w) && (T += 1)
              return (S += (Math.abs((100 * T) / k / k - 50) / 5) * 10)
            },
          }
        return A
      })(),
      h = (function () {
        for (var f = Array(256), b = Array(256), A = 0; 8 > A; A += 1)
          f[A] = 1 << A
        for (A = 8; 256 > A; A += 1)
          f[A] = f[A - 4] ^ f[A - 5] ^ f[A - 6] ^ f[A - 8]
        for (A = 0; 255 > A; A += 1) b[f[A]] = A
        return {
          g: function (y) {
            if (1 > y) throw Error('glog(' + y + ')')
            return b[y]
          },
          i: function (y) {
            for (; 0 > y; ) y += 255
            for (; 256 <= y; ) y -= 255
            return f[y]
          },
        }
      })(),
      m = /* @__PURE__ */ (function () {
        function f(y, k) {
          switch (k) {
            case a.L:
              return b[4 * (y - 1)]
            case a.M:
              return b[4 * (y - 1) + 1]
            case a.Q:
              return b[4 * (y - 1) + 2]
            case a.H:
              return b[4 * (y - 1) + 3]
          }
        }
        var b = [
            [1, 26, 19],
            [1, 26, 16],
            [1, 26, 13],
            [1, 26, 9],
            [1, 44, 34],
            [1, 44, 28],
            [1, 44, 22],
            [1, 44, 16],
            [1, 70, 55],
            [1, 70, 44],
            [2, 35, 17],
            [2, 35, 13],
            [1, 100, 80],
            [2, 50, 32],
            [2, 50, 24],
            [4, 25, 9],
            [1, 134, 108],
            [2, 67, 43],
            [2, 33, 15, 2, 34, 16],
            [2, 33, 11, 2, 34, 12],
            [2, 86, 68],
            [4, 43, 27],
            [4, 43, 19],
            [4, 43, 15],
            [2, 98, 78],
            [4, 49, 31],
            [2, 32, 14, 4, 33, 15],
            [4, 39, 13, 1, 40, 14],
            [2, 121, 97],
            [2, 60, 38, 2, 61, 39],
            [4, 40, 18, 2, 41, 19],
            [4, 40, 14, 2, 41, 15],
            [2, 146, 116],
            [3, 58, 36, 2, 59, 37],
            [4, 36, 16, 4, 37, 17],
            [4, 36, 12, 4, 37, 13],
            [2, 86, 68, 2, 87, 69],
            [4, 69, 43, 1, 70, 44],
            [6, 43, 19, 2, 44, 20],
            [6, 43, 15, 2, 44, 16],
            [4, 101, 81],
            [1, 80, 50, 4, 81, 51],
            [4, 50, 22, 4, 51, 23],
            [3, 36, 12, 8, 37, 13],
            [2, 116, 92, 2, 117, 93],
            [6, 58, 36, 2, 59, 37],
            [4, 46, 20, 6, 47, 21],
            [7, 42, 14, 4, 43, 15],
            [4, 133, 107],
            [8, 59, 37, 1, 60, 38],
            [8, 44, 20, 4, 45, 21],
            [12, 33, 11, 4, 34, 12],
            [3, 145, 115, 1, 146, 116],
            [4, 64, 40, 5, 65, 41],
            [11, 36, 16, 5, 37, 17],
            [11, 36, 12, 5, 37, 13],
            [5, 109, 87, 1, 110, 88],
            [5, 65, 41, 5, 66, 42],
            [5, 54, 24, 7, 55, 25],
            [11, 36, 12, 7, 37, 13],
            [5, 122, 98, 1, 123, 99],
            [7, 73, 45, 3, 74, 46],
            [15, 43, 19, 2, 44, 20],
            [3, 45, 15, 13, 46, 16],
            [1, 135, 107, 5, 136, 108],
            [10, 74, 46, 1, 75, 47],
            [1, 50, 22, 15, 51, 23],
            [2, 42, 14, 17, 43, 15],
            [5, 150, 120, 1, 151, 121],
            [9, 69, 43, 4, 70, 44],
            [17, 50, 22, 1, 51, 23],
            [2, 42, 14, 19, 43, 15],
            [3, 141, 113, 4, 142, 114],
            [3, 70, 44, 11, 71, 45],
            [17, 47, 21, 4, 48, 22],
            [9, 39, 13, 16, 40, 14],
            [3, 135, 107, 5, 136, 108],
            [3, 67, 41, 13, 68, 42],
            [15, 54, 24, 5, 55, 25],
            [15, 43, 15, 10, 44, 16],
            [4, 144, 116, 4, 145, 117],
            [17, 68, 42],
            [17, 50, 22, 6, 51, 23],
            [19, 46, 16, 6, 47, 17],
            [2, 139, 111, 7, 140, 112],
            [17, 74, 46],
            [7, 54, 24, 16, 55, 25],
            [34, 37, 13],
            [4, 151, 121, 5, 152, 122],
            [4, 75, 47, 14, 76, 48],
            [11, 54, 24, 14, 55, 25],
            [16, 45, 15, 14, 46, 16],
            [6, 147, 117, 4, 148, 118],
            [6, 73, 45, 14, 74, 46],
            [11, 54, 24, 16, 55, 25],
            [30, 46, 16, 2, 47, 17],
            [8, 132, 106, 4, 133, 107],
            [8, 75, 47, 13, 76, 48],
            [7, 54, 24, 22, 55, 25],
            [22, 45, 15, 13, 46, 16],
            [10, 142, 114, 2, 143, 115],
            [19, 74, 46, 4, 75, 47],
            [28, 50, 22, 6, 51, 23],
            [33, 46, 16, 4, 47, 17],
            [8, 152, 122, 4, 153, 123],
            [22, 73, 45, 3, 74, 46],
            [8, 53, 23, 26, 54, 24],
            [12, 45, 15, 28, 46, 16],
            [3, 147, 117, 10, 148, 118],
            [3, 73, 45, 23, 74, 46],
            [4, 54, 24, 31, 55, 25],
            [11, 45, 15, 31, 46, 16],
            [7, 146, 116, 7, 147, 117],
            [21, 73, 45, 7, 74, 46],
            [1, 53, 23, 37, 54, 24],
            [19, 45, 15, 26, 46, 16],
            [5, 145, 115, 10, 146, 116],
            [19, 75, 47, 10, 76, 48],
            [15, 54, 24, 25, 55, 25],
            [23, 45, 15, 25, 46, 16],
            [13, 145, 115, 3, 146, 116],
            [2, 74, 46, 29, 75, 47],
            [42, 54, 24, 1, 55, 25],
            [23, 45, 15, 28, 46, 16],
            [17, 145, 115],
            [10, 74, 46, 23, 75, 47],
            [10, 54, 24, 35, 55, 25],
            [19, 45, 15, 35, 46, 16],
            [17, 145, 115, 1, 146, 116],
            [14, 74, 46, 21, 75, 47],
            [29, 54, 24, 19, 55, 25],
            [11, 45, 15, 46, 46, 16],
            [13, 145, 115, 6, 146, 116],
            [14, 74, 46, 23, 75, 47],
            [44, 54, 24, 7, 55, 25],
            [59, 46, 16, 1, 47, 17],
            [12, 151, 121, 7, 152, 122],
            [12, 75, 47, 26, 76, 48],
            [39, 54, 24, 14, 55, 25],
            [22, 45, 15, 41, 46, 16],
            [6, 151, 121, 14, 152, 122],
            [6, 75, 47, 34, 76, 48],
            [46, 54, 24, 10, 55, 25],
            [2, 45, 15, 64, 46, 16],
            [17, 152, 122, 4, 153, 123],
            [29, 74, 46, 14, 75, 47],
            [49, 54, 24, 10, 55, 25],
            [24, 45, 15, 46, 46, 16],
            [4, 152, 122, 18, 153, 123],
            [13, 74, 46, 32, 75, 47],
            [48, 54, 24, 14, 55, 25],
            [42, 45, 15, 32, 46, 16],
            [20, 147, 117, 4, 148, 118],
            [40, 75, 47, 7, 76, 48],
            [43, 54, 24, 22, 55, 25],
            [10, 45, 15, 67, 46, 16],
            [19, 148, 118, 6, 149, 119],
            [18, 75, 47, 31, 76, 48],
            [34, 54, 24, 34, 55, 25],
            [20, 45, 15, 61, 46, 16],
          ],
          A = {
            I: function (y, k) {
              var S = f(y, k)
              if (typeof S > 'u')
                throw Error(
                  'bad rs block @ typeNumber:' + y + '/errorCorrectLevel:' + k,
                )
              ;(y = S.length / 3), (k = [])
              for (var $ = 0; $ < y; $ += 1)
                for (
                  var w = S[3 * $], T = S[3 * $ + 1], P = S[3 * $ + 2], D = 0;
                  D < w;
                  D += 1
                ) {
                  var R = P,
                    O = {}
                  ;(O.o = T), (O.j = R), k.push(O)
                }
              return k
            },
          }
        return A
      })()
    return i
  })(),
)
const SC = QrCreator
var Rr = class extends nt {
  constructor() {
    super(...arguments),
      (this.value = ''),
      (this.label = ''),
      (this.size = 128),
      (this.fill = 'black'),
      (this.background = 'white'),
      (this.radius = 0),
      (this.errorCorrection = 'H')
  }
  firstUpdated() {
    this.generate()
  }
  generate() {
    this.hasUpdated &&
      SC.render(
        {
          text: this.value,
          radius: this.radius,
          ecLevel: this.errorCorrection,
          fill: this.fill,
          background: this.background,
          // We draw the canvas larger and scale its container down to avoid blurring on high-density displays
          size: this.size * 2,
        },
        this.canvas,
      )
  }
  render() {
    var t
    return C`
      <canvas
        part="base"
        class="qr-code"
        role="img"
        aria-label=${
          ((t = this.label) == null ? void 0 : t.length) > 0
            ? this.label
            : this.value
        }
        style=${Ke({
          width: `${this.size}px`,
          height: `${this.size}px`,
        })}
      ></canvas>
    `
  }
}
Rr.styles = [dt, CC]
c([J('canvas')], Rr.prototype, 'canvas', 2)
c([p()], Rr.prototype, 'value', 2)
c([p()], Rr.prototype, 'label', 2)
c([p({ type: Number })], Rr.prototype, 'size', 2)
c([p()], Rr.prototype, 'fill', 2)
c([p()], Rr.prototype, 'background', 2)
c([p({ type: Number })], Rr.prototype, 'radius', 2)
c([p({ attribute: 'error-correction' })], Rr.prototype, 'errorCorrection', 2)
c(
  [X(['background', 'errorCorrection', 'fill', 'radius', 'size', 'value'])],
  Rr.prototype,
  'generate',
  1,
)
Rr.define('sl-qr-code')
var zC = st`
  :host {
    display: block;
  }

  :host(:focus-visible) {
    outline: 0px;
  }

  .radio {
    display: inline-flex;
    align-items: top;
    font-family: var(--sl-input-font-family);
    font-size: var(--sl-input-font-size-medium);
    font-weight: var(--sl-input-font-weight);
    color: var(--sl-input-label-color);
    vertical-align: middle;
    cursor: pointer;
  }

  .radio--small {
    --toggle-size: var(--sl-toggle-size-small);
    font-size: var(--sl-input-font-size-small);
  }

  .radio--medium {
    --toggle-size: var(--sl-toggle-size-medium);
    font-size: var(--sl-input-font-size-medium);
  }

  .radio--large {
    --toggle-size: var(--sl-toggle-size-large);
    font-size: var(--sl-input-font-size-large);
  }

  .radio__checked-icon {
    display: inline-flex;
    width: var(--toggle-size);
    height: var(--toggle-size);
  }

  .radio__control {
    flex: 0 0 auto;
    position: relative;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: var(--toggle-size);
    height: var(--toggle-size);
    border: solid var(--sl-input-border-width) var(--sl-input-border-color);
    border-radius: 50%;
    background-color: var(--sl-input-background-color);
    color: transparent;
    transition:
      var(--sl-transition-fast) border-color,
      var(--sl-transition-fast) background-color,
      var(--sl-transition-fast) color,
      var(--sl-transition-fast) box-shadow;
  }

  .radio__input {
    position: absolute;
    opacity: 0;
    padding: 0;
    margin: 0;
    pointer-events: none;
  }

  /* Hover */
  .radio:not(.radio--checked):not(.radio--disabled) .radio__control:hover {
    border-color: var(--sl-input-border-color-hover);
    background-color: var(--sl-input-background-color-hover);
  }

  /* Checked */
  .radio--checked .radio__control {
    color: var(--sl-color-neutral-0);
    border-color: var(--sl-color-primary-600);
    background-color: var(--sl-color-primary-600);
  }

  /* Checked + hover */
  .radio.radio--checked:not(.radio--disabled) .radio__control:hover {
    border-color: var(--sl-color-primary-500);
    background-color: var(--sl-color-primary-500);
  }

  /* Checked + focus */
  :host(:focus-visible) .radio__control {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  /* Disabled */
  .radio--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  /* When the control isn't checked, hide the circle for Windows High Contrast mode a11y */
  .radio:not(.radio--checked) svg circle {
    opacity: 0;
  }

  .radio__label {
    display: inline-block;
    color: var(--sl-input-label-color);
    line-height: var(--toggle-size);
    margin-inline-start: 0.5em;
    user-select: none;
    -webkit-user-select: none;
  }
`,
  jr = class extends nt {
    constructor() {
      super(),
        (this.checked = !1),
        (this.hasFocus = !1),
        (this.size = 'medium'),
        (this.disabled = !1),
        (this.handleBlur = () => {
          ;(this.hasFocus = !1), this.emit('sl-blur')
        }),
        (this.handleClick = () => {
          this.disabled || (this.checked = !0)
        }),
        (this.handleFocus = () => {
          ;(this.hasFocus = !0), this.emit('sl-focus')
        }),
        this.addEventListener('blur', this.handleBlur),
        this.addEventListener('click', this.handleClick),
        this.addEventListener('focus', this.handleFocus)
    }
    connectedCallback() {
      super.connectedCallback(), this.setInitialAttributes()
    }
    setInitialAttributes() {
      this.setAttribute('role', 'radio'),
        this.setAttribute('tabindex', '-1'),
        this.setAttribute('aria-disabled', this.disabled ? 'true' : 'false')
    }
    handleCheckedChange() {
      this.setAttribute('aria-checked', this.checked ? 'true' : 'false'),
        this.setAttribute('tabindex', this.checked ? '0' : '-1')
    }
    handleDisabledChange() {
      this.setAttribute('aria-disabled', this.disabled ? 'true' : 'false')
    }
    render() {
      return C`
      <span
        part="base"
        class=${ct({
          radio: !0,
          'radio--checked': this.checked,
          'radio--disabled': this.disabled,
          'radio--focused': this.hasFocus,
          'radio--small': this.size === 'small',
          'radio--medium': this.size === 'medium',
          'radio--large': this.size === 'large',
        })}
      >
        <span part="${`control${
          this.checked ? ' control--checked' : ''
        }`}" class="radio__control">
          ${
            this.checked
              ? C` <sl-icon part="checked-icon" class="radio__checked-icon" library="system" name="radio"></sl-icon> `
              : ''
          }
        </span>

        <slot part="label" class="radio__label"></slot>
      </span>
    `
    }
  }
jr.styles = [dt, zC]
jr.dependencies = { 'sl-icon': Nt }
c([at()], jr.prototype, 'checked', 2)
c([at()], jr.prototype, 'hasFocus', 2)
c([p()], jr.prototype, 'value', 2)
c([p({ reflect: !0 })], jr.prototype, 'size', 2)
c([p({ type: Boolean, reflect: !0 })], jr.prototype, 'disabled', 2)
c([X('checked')], jr.prototype, 'handleCheckedChange', 1)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  jr.prototype,
  'handleDisabledChange',
  1,
)
jr.define('sl-radio')
var AC = st`
  :host {
    display: block;
    user-select: none;
    -webkit-user-select: none;
  }

  :host(:focus) {
    outline: none;
  }

  .option {
    position: relative;
    display: flex;
    align-items: center;
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-medium);
    font-weight: var(--sl-font-weight-normal);
    line-height: var(--sl-line-height-normal);
    letter-spacing: var(--sl-letter-spacing-normal);
    color: var(--sl-color-neutral-700);
    padding: var(--sl-spacing-x-small) var(--sl-spacing-medium) var(--sl-spacing-x-small) var(--sl-spacing-x-small);
    transition: var(--sl-transition-fast) fill;
    cursor: pointer;
  }

  .option--hover:not(.option--current):not(.option--disabled) {
    background-color: var(--sl-color-neutral-100);
    color: var(--sl-color-neutral-1000);
  }

  .option--current,
  .option--current.option--disabled {
    background-color: var(--sl-color-primary-600);
    color: var(--sl-color-neutral-0);
    opacity: 1;
  }

  .option--disabled {
    outline: none;
    opacity: 0.5;
    cursor: not-allowed;
  }

  .option__label {
    flex: 1 1 auto;
    display: inline-block;
    line-height: var(--sl-line-height-dense);
  }

  .option .option__check {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    justify-content: center;
    visibility: hidden;
    padding-inline-end: var(--sl-spacing-2x-small);
  }

  .option--selected .option__check {
    visibility: visible;
  }

  .option__prefix,
  .option__suffix {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
  }

  .option__prefix::slotted(*) {
    margin-inline-end: var(--sl-spacing-x-small);
  }

  .option__suffix::slotted(*) {
    margin-inline-start: var(--sl-spacing-x-small);
  }

  @media (forced-colors: active) {
    :host(:hover:not([aria-disabled='true'])) .option {
      outline: dashed 1px SelectedItem;
      outline-offset: -1px;
    }
  }
`,
  Ne = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.current = !1),
        (this.selected = !1),
        (this.hasHover = !1),
        (this.value = ''),
        (this.disabled = !1)
    }
    connectedCallback() {
      super.connectedCallback(),
        this.setAttribute('role', 'option'),
        this.setAttribute('aria-selected', 'false')
    }
    handleDefaultSlotChange() {
      const t = this.getTextLabel()
      if (typeof this.cachedTextLabel > 'u') {
        this.cachedTextLabel = t
        return
      }
      t !== this.cachedTextLabel &&
        ((this.cachedTextLabel = t),
        this.emit('slotchange', { bubbles: !0, composed: !1, cancelable: !1 }))
    }
    handleMouseEnter() {
      this.hasHover = !0
    }
    handleMouseLeave() {
      this.hasHover = !1
    }
    handleDisabledChange() {
      this.setAttribute('aria-disabled', this.disabled ? 'true' : 'false')
    }
    handleSelectedChange() {
      this.setAttribute('aria-selected', this.selected ? 'true' : 'false')
    }
    handleValueChange() {
      typeof this.value != 'string' && (this.value = String(this.value)),
        this.value.includes(' ') &&
          (console.error(
            'Option values cannot include a space. All spaces have been replaced with underscores.',
            this,
          ),
          (this.value = this.value.replace(/ /g, '_')))
    }
    /** Returns a plain text label based on the option's content. */
    getTextLabel() {
      const t = this.childNodes
      let r = ''
      return (
        [...t].forEach(i => {
          i.nodeType === Node.ELEMENT_NODE &&
            (i.hasAttribute('slot') || (r += i.textContent)),
            i.nodeType === Node.TEXT_NODE && (r += i.textContent)
        }),
        r.trim()
      )
    }
    render() {
      return C`
      <div
        part="base"
        class=${ct({
          option: !0,
          'option--current': this.current,
          'option--disabled': this.disabled,
          'option--selected': this.selected,
          'option--hover': this.hasHover,
        })}
        @mouseenter=${this.handleMouseEnter}
        @mouseleave=${this.handleMouseLeave}
      >
        <sl-icon part="checked-icon" class="option__check" name="check" library="system" aria-hidden="true"></sl-icon>
        <slot part="prefix" name="prefix" class="option__prefix"></slot>
        <slot part="label" class="option__label" @slotchange=${
          this.handleDefaultSlotChange
        }></slot>
        <slot part="suffix" name="suffix" class="option__suffix"></slot>
      </div>
    `
    }
  }
Ne.styles = [dt, AC]
Ne.dependencies = { 'sl-icon': Nt }
c([J('.option__label')], Ne.prototype, 'defaultSlot', 2)
c([at()], Ne.prototype, 'current', 2)
c([at()], Ne.prototype, 'selected', 2)
c([at()], Ne.prototype, 'hasHover', 2)
c([p({ reflect: !0 })], Ne.prototype, 'value', 2)
c([p({ type: Boolean, reflect: !0 })], Ne.prototype, 'disabled', 2)
c([X('disabled')], Ne.prototype, 'handleDisabledChange', 1)
c([X('selected')], Ne.prototype, 'handleSelectedChange', 1)
c([X('value')], Ne.prototype, 'handleValueChange', 1)
Ne.define('sl-option')
Ut.define('sl-popup')
var EC = st`
  :host {
    --height: 1rem;
    --track-color: var(--sl-color-neutral-200);
    --indicator-color: var(--sl-color-primary-600);
    --label-color: var(--sl-color-neutral-0);

    display: block;
  }

  .progress-bar {
    position: relative;
    background-color: var(--track-color);
    height: var(--height);
    border-radius: var(--sl-border-radius-pill);
    box-shadow: inset var(--sl-shadow-small);
    overflow: hidden;
  }

  .progress-bar__indicator {
    height: 100%;
    font-family: var(--sl-font-sans);
    font-size: 12px;
    font-weight: var(--sl-font-weight-normal);
    background-color: var(--indicator-color);
    color: var(--label-color);
    text-align: center;
    line-height: var(--height);
    white-space: nowrap;
    overflow: hidden;
    transition:
      400ms width,
      400ms background-color;
    user-select: none;
    -webkit-user-select: none;
  }

  /* Indeterminate */
  .progress-bar--indeterminate .progress-bar__indicator {
    position: absolute;
    animation: indeterminate 2.5s infinite cubic-bezier(0.37, 0, 0.63, 1);
  }

  .progress-bar--indeterminate.progress-bar--rtl .progress-bar__indicator {
    animation-name: indeterminate-rtl;
  }

  @media (forced-colors: active) {
    .progress-bar {
      outline: solid 1px SelectedItem;
      background-color: var(--sl-color-neutral-0);
    }

    .progress-bar__indicator {
      outline: solid 1px SelectedItem;
      background-color: SelectedItem;
    }
  }

  @keyframes indeterminate {
    0% {
      left: -50%;
      width: 50%;
    }
    75%,
    100% {
      left: 100%;
      width: 50%;
    }
  }

  @keyframes indeterminate-rtl {
    0% {
      right: -50%;
      width: 50%;
    }
    75%,
    100% {
      right: 100%;
      width: 50%;
    }
  }
`,
  Uo = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.value = 0),
        (this.indeterminate = !1),
        (this.label = '')
    }
    render() {
      return C`
      <div
        part="base"
        class=${ct({
          'progress-bar': !0,
          'progress-bar--indeterminate': this.indeterminate,
          'progress-bar--rtl': this.localize.dir() === 'rtl',
        })}
        role="progressbar"
        title=${it(this.title)}
        aria-label=${
          this.label.length > 0 ? this.label : this.localize.term('progress')
        }
        aria-valuemin="0"
        aria-valuemax="100"
        aria-valuenow=${this.indeterminate ? 0 : this.value}
      >
        <div part="indicator" class="progress-bar__indicator" style=${Ke({
          width: `${this.value}%`,
        })}>
          ${
            this.indeterminate
              ? ''
              : C` <slot part="label" class="progress-bar__label"></slot> `
          }
        </div>
      </div>
    `
    }
  }
Uo.styles = [dt, EC]
c([p({ type: Number, reflect: !0 })], Uo.prototype, 'value', 2)
c([p({ type: Boolean, reflect: !0 })], Uo.prototype, 'indeterminate', 2)
c([p()], Uo.prototype, 'label', 2)
Uo.define('sl-progress-bar')
var TC = st`
  :host {
    display: block;
  }

  .menu-label {
    display: inline-block;
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-small);
    font-weight: var(--sl-font-weight-semibold);
    line-height: var(--sl-line-height-normal);
    letter-spacing: var(--sl-letter-spacing-normal);
    color: var(--sl-color-neutral-500);
    padding: var(--sl-spacing-2x-small) var(--sl-spacing-x-large);
    user-select: none;
    -webkit-user-select: none;
  }
`,
  kf = class extends nt {
    render() {
      return C` <slot part="base" class="menu-label"></slot> `
    }
  }
kf.styles = [dt, TC]
kf.define('sl-menu-label')
var IC = st`
  :host {
    display: contents;
  }
`,
  Zr = class extends nt {
    constructor() {
      super(...arguments),
        (this.attrOldValue = !1),
        (this.charData = !1),
        (this.charDataOldValue = !1),
        (this.childList = !1),
        (this.disabled = !1),
        (this.handleMutation = t => {
          this.emit('sl-mutation', {
            detail: { mutationList: t },
          })
        })
    }
    connectedCallback() {
      super.connectedCallback(),
        (this.mutationObserver = new MutationObserver(this.handleMutation)),
        this.disabled || this.startObserver()
    }
    disconnectedCallback() {
      super.disconnectedCallback(), this.stopObserver()
    }
    startObserver() {
      const t = typeof this.attr == 'string' && this.attr.length > 0,
        r = t && this.attr !== '*' ? this.attr.split(' ') : void 0
      try {
        this.mutationObserver.observe(this, {
          subtree: !0,
          childList: this.childList,
          attributes: t,
          attributeFilter: r,
          attributeOldValue: this.attrOldValue,
          characterData: this.charData,
          characterDataOldValue: this.charDataOldValue,
        })
      } catch {}
    }
    stopObserver() {
      this.mutationObserver.disconnect()
    }
    handleDisabledChange() {
      this.disabled ? this.stopObserver() : this.startObserver()
    }
    handleChange() {
      this.stopObserver(), this.startObserver()
    }
    render() {
      return C` <slot></slot> `
    }
  }
Zr.styles = [dt, IC]
c([p({ reflect: !0 })], Zr.prototype, 'attr', 2)
c(
  [p({ attribute: 'attr-old-value', type: Boolean, reflect: !0 })],
  Zr.prototype,
  'attrOldValue',
  2,
)
c(
  [p({ attribute: 'char-data', type: Boolean, reflect: !0 })],
  Zr.prototype,
  'charData',
  2,
)
c(
  [p({ attribute: 'char-data-old-value', type: Boolean, reflect: !0 })],
  Zr.prototype,
  'charDataOldValue',
  2,
)
c(
  [p({ attribute: 'child-list', type: Boolean, reflect: !0 })],
  Zr.prototype,
  'childList',
  2,
)
c([p({ type: Boolean, reflect: !0 })], Zr.prototype, 'disabled', 2)
c([X('disabled')], Zr.prototype, 'handleDisabledChange', 1)
c(
  [
    X('attr', { waitUntilFirstUpdate: !0 }),
    X('attr-old-value', { waitUntilFirstUpdate: !0 }),
    X('char-data', { waitUntilFirstUpdate: !0 }),
    X('char-data-old-value', { waitUntilFirstUpdate: !0 }),
    X('childList', { waitUntilFirstUpdate: !0 }),
  ],
  Zr.prototype,
  'handleChange',
  1,
)
Zr.define('sl-mutation-observer')
bt.define('sl-input')
var OC = st`
  :host {
    display: block;
    position: relative;
    background: var(--sl-panel-background-color);
    border: solid var(--sl-panel-border-width) var(--sl-panel-border-color);
    border-radius: var(--sl-border-radius-medium);
    padding: var(--sl-spacing-x-small) 0;
    overflow: auto;
    overscroll-behavior: none;
  }

  ::slotted(sl-divider) {
    --spacing: var(--sl-spacing-x-small);
  }
`,
  Ic = class extends nt {
    connectedCallback() {
      super.connectedCallback(), this.setAttribute('role', 'menu')
    }
    handleClick(t) {
      const r = ['menuitem', 'menuitemcheckbox'],
        i = t.composedPath(),
        o = i.find(m => {
          var f
          return r.includes(
            ((f = m == null ? void 0 : m.getAttribute) == null
              ? void 0
              : f.call(m, 'role')) || '',
          )
        })
      if (
        !o ||
        i.find(m => {
          var f
          return (
            ((f = m == null ? void 0 : m.getAttribute) == null
              ? void 0
              : f.call(m, 'role')) === 'menu'
          )
        }) !== this
      )
        return
      const h = o
      h.type === 'checkbox' && (h.checked = !h.checked),
        this.emit('sl-select', { detail: { item: h } })
    }
    handleKeyDown(t) {
      if (t.key === 'Enter' || t.key === ' ') {
        const r = this.getCurrentItem()
        t.preventDefault(), t.stopPropagation(), r == null || r.click()
      } else if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(t.key)) {
        const r = this.getAllItems(),
          i = this.getCurrentItem()
        let o = i ? r.indexOf(i) : 0
        r.length > 0 &&
          (t.preventDefault(),
          t.stopPropagation(),
          t.key === 'ArrowDown'
            ? o++
            : t.key === 'ArrowUp'
            ? o--
            : t.key === 'Home'
            ? (o = 0)
            : t.key === 'End' && (o = r.length - 1),
          o < 0 && (o = r.length - 1),
          o > r.length - 1 && (o = 0),
          this.setCurrentItem(r[o]),
          r[o].focus())
      }
    }
    handleMouseDown(t) {
      const r = t.target
      this.isMenuItem(r) && this.setCurrentItem(r)
    }
    handleSlotChange() {
      const t = this.getAllItems()
      t.length > 0 && this.setCurrentItem(t[0])
    }
    isMenuItem(t) {
      var r
      return (
        t.tagName.toLowerCase() === 'sl-menu-item' ||
        ['menuitem', 'menuitemcheckbox', 'menuitemradio'].includes(
          (r = t.getAttribute('role')) != null ? r : '',
        )
      )
    }
    /** @internal Gets all slotted menu items, ignoring dividers, headers, and other elements. */
    getAllItems() {
      return [...this.defaultSlot.assignedElements({ flatten: !0 })].filter(
        t => !(t.inert || !this.isMenuItem(t)),
      )
    }
    /**
     * @internal Gets the current menu item, which is the menu item that has `tabindex="0"` within the roving tab index.
     * The menu item may or may not have focus, but for keyboard interaction purposes it's considered the "active" item.
     */
    getCurrentItem() {
      return this.getAllItems().find(t => t.getAttribute('tabindex') === '0')
    }
    /**
     * @internal Sets the current menu item to the specified element. This sets `tabindex="0"` on the target element and
     * `tabindex="-1"` to all other items. This method must be called prior to setting focus on a menu item.
     */
    setCurrentItem(t) {
      this.getAllItems().forEach(i => {
        i.setAttribute('tabindex', i === t ? '0' : '-1')
      })
    }
    render() {
      return C`
      <slot
        @slotchange=${this.handleSlotChange}
        @click=${this.handleClick}
        @keydown=${this.handleKeyDown}
        @mousedown=${this.handleMouseDown}
      ></slot>
    `
    }
  }
Ic.styles = [dt, OC]
c([J('slot')], Ic.prototype, 'defaultSlot', 2)
Ic.define('sl-menu')
var LC = st`
  :host {
    --submenu-offset: -2px;

    display: block;
  }

  :host([inert]) {
    display: none;
  }

  .menu-item {
    position: relative;
    display: flex;
    align-items: stretch;
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-medium);
    font-weight: var(--sl-font-weight-normal);
    line-height: var(--sl-line-height-normal);
    letter-spacing: var(--sl-letter-spacing-normal);
    color: var(--sl-color-neutral-700);
    padding: var(--sl-spacing-2x-small) var(--sl-spacing-2x-small);
    transition: var(--sl-transition-fast) fill;
    user-select: none;
    -webkit-user-select: none;
    white-space: nowrap;
    cursor: pointer;
  }

  .menu-item.menu-item--disabled {
    outline: none;
    opacity: 0.5;
    cursor: not-allowed;
  }

  .menu-item.menu-item--loading {
    outline: none;
    cursor: wait;
  }

  .menu-item.menu-item--loading *:not(sl-spinner) {
    opacity: 0.5;
  }

  .menu-item--loading sl-spinner {
    --indicator-color: currentColor;
    --track-width: 1px;
    position: absolute;
    font-size: 0.75em;
    top: calc(50% - 0.5em);
    left: 0.65rem;
    opacity: 1;
  }

  .menu-item .menu-item__label {
    flex: 1 1 auto;
    display: inline-block;
    text-overflow: ellipsis;
    overflow: hidden;
  }

  .menu-item .menu-item__prefix {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
  }

  .menu-item .menu-item__prefix::slotted(*) {
    margin-inline-end: var(--sl-spacing-x-small);
  }

  .menu-item .menu-item__suffix {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
  }

  .menu-item .menu-item__suffix::slotted(*) {
    margin-inline-start: var(--sl-spacing-x-small);
  }

  /* Safe triangle */
  .menu-item--submenu-expanded::after {
    content: '';
    position: fixed;
    z-index: calc(var(--sl-z-index-dropdown) - 1);
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    clip-path: polygon(
      var(--safe-triangle-cursor-x, 0) var(--safe-triangle-cursor-y, 0),
      var(--safe-triangle-submenu-start-x, 0) var(--safe-triangle-submenu-start-y, 0),
      var(--safe-triangle-submenu-end-x, 0) var(--safe-triangle-submenu-end-y, 0)
    );
  }

  :host(:focus-visible) {
    outline: none;
  }

  :host(:hover:not([aria-disabled='true'], :focus-visible)) .menu-item,
  .menu-item--submenu-expanded {
    background-color: var(--sl-color-neutral-100);
    color: var(--sl-color-neutral-1000);
  }

  :host(:focus-visible) .menu-item {
    outline: none;
    background-color: var(--sl-color-primary-600);
    color: var(--sl-color-neutral-0);
    opacity: 1;
  }

  .menu-item .menu-item__check,
  .menu-item .menu-item__chevron {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 1.5em;
    visibility: hidden;
  }

  .menu-item--checked .menu-item__check,
  .menu-item--has-submenu .menu-item__chevron {
    visibility: visible;
  }

  /* Add elevation and z-index to submenus */
  sl-popup::part(popup) {
    box-shadow: var(--sl-shadow-large);
    z-index: var(--sl-z-index-dropdown);
    margin-left: var(--submenu-offset);
  }

  .menu-item--rtl sl-popup::part(popup) {
    margin-left: calc(-1 * var(--submenu-offset));
  }

  @media (forced-colors: active) {
    :host(:hover:not([aria-disabled='true'])) .menu-item,
    :host(:focus-visible) .menu-item {
      outline: dashed 1px SelectedItem;
      outline-offset: -1px;
    }
  }

  ::slotted(sl-menu) {
    max-width: var(--auto-size-available-width) !important;
    max-height: var(--auto-size-available-height) !important;
  }
`
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const zo = (t, r) => {
    var o
    const i = t._$AN
    if (i === void 0) return !1
    for (const a of i) (o = a._$AO) == null || o.call(a, r, !1), zo(a, r)
    return !0
  },
  ta = t => {
    let r, i
    do {
      if ((r = t._$AM) === void 0) break
      ;(i = r._$AN), i.delete(t), (t = r)
    } while ((i == null ? void 0 : i.size) === 0)
  },
  $f = t => {
    for (let r; (r = t._$AM); t = r) {
      let i = r._$AN
      if (i === void 0) r._$AN = i = /* @__PURE__ */ new Set()
      else if (i.has(t)) break
      i.add(t), RC(r)
    }
  }
function DC(t) {
  this._$AN !== void 0 ? (ta(this), (this._$AM = t), $f(this)) : (this._$AM = t)
}
function MC(t, r = !1, i = 0) {
  const o = this._$AH,
    a = this._$AN
  if (a !== void 0 && a.size !== 0)
    if (r)
      if (Array.isArray(o))
        for (let d = i; d < o.length; d++) zo(o[d], !1), ta(o[d])
      else o != null && (zo(o, !1), ta(o))
    else zo(this, t)
}
const RC = t => {
  t.type == Yr.CHILD && (t._$AP ?? (t._$AP = MC), t._$AQ ?? (t._$AQ = DC))
}
class PC extends Fo {
  constructor() {
    super(...arguments), (this._$AN = void 0)
  }
  _$AT(r, i, o) {
    super._$AT(r, i, o), $f(this), (this.isConnected = r._$AU)
  }
  _$AO(r, i = !0) {
    var o, a
    r !== this.isConnected &&
      ((this.isConnected = r),
      r
        ? (o = this.reconnected) == null || o.call(this)
        : (a = this.disconnected) == null || a.call(this)),
      i && (zo(this, r), ta(this))
  }
  setValue(r) {
    if (Hp(this._$Ct)) this._$Ct._$AI(r, this)
    else {
      const i = [...this._$Ct._$AH]
      ;(i[this._$Ci] = r), this._$Ct._$AI(i, this, 0)
    }
  }
  disconnected() {}
  reconnected() {}
}
/**
 * @license
 * Copyright 2020 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const BC = () => new FC()
class FC {}
const Gl = /* @__PURE__ */ new WeakMap(),
  NC = Bo(
    class extends PC {
      render(t) {
        return Gt
      }
      update(t, [r]) {
        var o
        const i = r !== this.Y
        return (
          i && this.Y !== void 0 && this.rt(void 0),
          (i || this.lt !== this.ct) &&
            ((this.Y = r),
            (this.ht = (o = t.options) == null ? void 0 : o.host),
            this.rt((this.ct = t.element))),
          Gt
        )
      }
      rt(t) {
        if ((this.isConnected || (t = void 0), typeof this.Y == 'function')) {
          const r = this.ht ?? globalThis
          let i = Gl.get(r)
          i === void 0 && ((i = /* @__PURE__ */ new WeakMap()), Gl.set(r, i)),
            i.get(this.Y) !== void 0 && this.Y.call(this.ht, void 0),
            i.set(this.Y, t),
            t !== void 0 && this.Y.call(this.ht, t)
        } else this.Y.value = t
      }
      get lt() {
        var t, r
        return typeof this.Y == 'function'
          ? (t = Gl.get(this.ht ?? globalThis)) == null
            ? void 0
            : t.get(this.Y)
          : (r = this.Y) == null
          ? void 0
          : r.value
      }
      disconnected() {
        this.lt === this.ct && this.rt(void 0)
      }
      reconnected() {
        this.rt(this.ct)
      }
    },
  )
var UC = class {
    constructor(t, r) {
      ;(this.popupRef = BC()),
        (this.enableSubmenuTimer = -1),
        (this.isConnected = !1),
        (this.isPopupConnected = !1),
        (this.skidding = 0),
        (this.submenuOpenDelay = 100),
        (this.handleMouseMove = i => {
          this.host.style.setProperty(
            '--safe-triangle-cursor-x',
            `${i.clientX}px`,
          ),
            this.host.style.setProperty(
              '--safe-triangle-cursor-y',
              `${i.clientY}px`,
            )
        }),
        (this.handleMouseOver = () => {
          this.hasSlotController.test('submenu') && this.enableSubmenu()
        }),
        (this.handleKeyDown = i => {
          switch (i.key) {
            case 'Escape':
            case 'Tab':
              this.disableSubmenu()
              break
            case 'ArrowLeft':
              i.target !== this.host &&
                (i.preventDefault(),
                i.stopPropagation(),
                this.host.focus(),
                this.disableSubmenu())
              break
            case 'ArrowRight':
            case 'Enter':
            case ' ':
              this.handleSubmenuEntry(i)
              break
          }
        }),
        (this.handleClick = i => {
          var o
          i.target === this.host
            ? (i.preventDefault(), i.stopPropagation())
            : i.target instanceof Element &&
              (i.target.tagName === 'sl-menu-item' ||
                ((o = i.target.role) != null && o.startsWith('menuitem'))) &&
              this.disableSubmenu()
        }),
        (this.handleFocusOut = i => {
          ;(i.relatedTarget &&
            i.relatedTarget instanceof Element &&
            this.host.contains(i.relatedTarget)) ||
            this.disableSubmenu()
        }),
        (this.handlePopupMouseover = i => {
          i.stopPropagation()
        }),
        (this.handlePopupReposition = () => {
          const i = this.host.renderRoot.querySelector("slot[name='submenu']"),
            o =
              i == null
                ? void 0
                : i
                    .assignedElements({ flatten: !0 })
                    .filter(b => b.localName === 'sl-menu')[0],
            a = getComputedStyle(this.host).direction === 'rtl'
          if (!o) return
          const {
            left: d,
            top: h,
            width: m,
            height: f,
          } = o.getBoundingClientRect()
          this.host.style.setProperty(
            '--safe-triangle-submenu-start-x',
            `${a ? d + m : d}px`,
          ),
            this.host.style.setProperty(
              '--safe-triangle-submenu-start-y',
              `${h}px`,
            ),
            this.host.style.setProperty(
              '--safe-triangle-submenu-end-x',
              `${a ? d + m : d}px`,
            ),
            this.host.style.setProperty(
              '--safe-triangle-submenu-end-y',
              `${h + f}px`,
            )
        }),
        (this.host = t).addController(this),
        (this.hasSlotController = r)
    }
    hostConnected() {
      this.hasSlotController.test('submenu') &&
        !this.host.disabled &&
        this.addListeners()
    }
    hostDisconnected() {
      this.removeListeners()
    }
    hostUpdated() {
      this.hasSlotController.test('submenu') && !this.host.disabled
        ? (this.addListeners(), this.updateSkidding())
        : this.removeListeners()
    }
    addListeners() {
      this.isConnected ||
        (this.host.addEventListener('mousemove', this.handleMouseMove),
        this.host.addEventListener('mouseover', this.handleMouseOver),
        this.host.addEventListener('keydown', this.handleKeyDown),
        this.host.addEventListener('click', this.handleClick),
        this.host.addEventListener('focusout', this.handleFocusOut),
        (this.isConnected = !0)),
        this.isPopupConnected ||
          (this.popupRef.value &&
            (this.popupRef.value.addEventListener(
              'mouseover',
              this.handlePopupMouseover,
            ),
            this.popupRef.value.addEventListener(
              'sl-reposition',
              this.handlePopupReposition,
            ),
            (this.isPopupConnected = !0)))
    }
    removeListeners() {
      this.isConnected &&
        (this.host.removeEventListener('mousemove', this.handleMouseMove),
        this.host.removeEventListener('mouseover', this.handleMouseOver),
        this.host.removeEventListener('keydown', this.handleKeyDown),
        this.host.removeEventListener('click', this.handleClick),
        this.host.removeEventListener('focusout', this.handleFocusOut),
        (this.isConnected = !1)),
        this.isPopupConnected &&
          this.popupRef.value &&
          (this.popupRef.value.removeEventListener(
            'mouseover',
            this.handlePopupMouseover,
          ),
          this.popupRef.value.removeEventListener(
            'sl-reposition',
            this.handlePopupReposition,
          ),
          (this.isPopupConnected = !1))
    }
    handleSubmenuEntry(t) {
      const r = this.host.renderRoot.querySelector("slot[name='submenu']")
      if (!r) {
        console.error(
          'Cannot activate a submenu if no corresponding menuitem can be found.',
          this,
        )
        return
      }
      let i = null
      for (const o of r.assignedElements())
        if (
          ((i = o.querySelectorAll("sl-menu-item, [role^='menuitem']")),
          i.length !== 0)
        )
          break
      if (!(!i || i.length === 0)) {
        i[0].setAttribute('tabindex', '0')
        for (let o = 1; o !== i.length; ++o) i[o].setAttribute('tabindex', '-1')
        this.popupRef.value &&
          (t.preventDefault(),
          t.stopPropagation(),
          this.popupRef.value.active
            ? i[0] instanceof HTMLElement && i[0].focus()
            : (this.enableSubmenu(!1),
              this.host.updateComplete.then(() => {
                i[0] instanceof HTMLElement && i[0].focus()
              }),
              this.host.requestUpdate()))
      }
    }
    setSubmenuState(t) {
      this.popupRef.value &&
        this.popupRef.value.active !== t &&
        ((this.popupRef.value.active = t), this.host.requestUpdate())
    }
    // Shows the submenu. Supports disabling the opening delay, e.g. for keyboard events that want to set the focus to the
    // newly opened menu.
    enableSubmenu(t = !0) {
      t
        ? (window.clearTimeout(this.enableSubmenuTimer),
          (this.enableSubmenuTimer = window.setTimeout(() => {
            this.setSubmenuState(!0)
          }, this.submenuOpenDelay)))
        : this.setSubmenuState(!0)
    }
    disableSubmenu() {
      window.clearTimeout(this.enableSubmenuTimer), this.setSubmenuState(!1)
    }
    // Calculate the space the top of a menu takes-up, for aligning the popup menu-item with the activating element.
    updateSkidding() {
      var t
      if (!((t = this.host.parentElement) != null && t.computedStyleMap)) return
      const r = this.host.parentElement.computedStyleMap(),
        o = ['padding-top', 'border-top-width', 'margin-top'].reduce((a, d) => {
          var h
          const m = (h = r.get(d)) != null ? h : new CSSUnitValue(0, 'px'),
            b = (m instanceof CSSUnitValue ? m : new CSSUnitValue(0, 'px')).to(
              'px',
            )
          return a - b.value
        }, 0)
      this.skidding = o
    }
    isExpanded() {
      return this.popupRef.value ? this.popupRef.value.active : !1
    }
    renderSubmenu() {
      const t = getComputedStyle(this.host).direction === 'rtl'
      return this.isConnected
        ? C`
      <sl-popup
        ${NC(this.popupRef)}
        placement=${t ? 'left-start' : 'right-start'}
        anchor="anchor"
        flip
        flip-fallback-strategy="best-fit"
        skidding="${this.skidding}"
        strategy="fixed"
        auto-size="vertical"
        auto-size-padding="10"
      >
        <slot name="submenu"></slot>
      </sl-popup>
    `
        : C` <slot name="submenu" hidden></slot> `
    }
  },
  hr = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.type = 'normal'),
        (this.checked = !1),
        (this.value = ''),
        (this.loading = !1),
        (this.disabled = !1),
        (this.hasSlotController = new He(this, 'submenu')),
        (this.submenuController = new UC(this, this.hasSlotController)),
        (this.handleHostClick = t => {
          this.disabled && (t.preventDefault(), t.stopImmediatePropagation())
        }),
        (this.handleMouseOver = t => {
          this.focus(), t.stopPropagation()
        })
    }
    connectedCallback() {
      super.connectedCallback(),
        this.addEventListener('click', this.handleHostClick),
        this.addEventListener('mouseover', this.handleMouseOver)
    }
    disconnectedCallback() {
      super.disconnectedCallback(),
        this.removeEventListener('click', this.handleHostClick),
        this.removeEventListener('mouseover', this.handleMouseOver)
    }
    handleDefaultSlotChange() {
      const t = this.getTextLabel()
      if (typeof this.cachedTextLabel > 'u') {
        this.cachedTextLabel = t
        return
      }
      t !== this.cachedTextLabel &&
        ((this.cachedTextLabel = t),
        this.emit('slotchange', { bubbles: !0, composed: !1, cancelable: !1 }))
    }
    handleCheckedChange() {
      if (this.checked && this.type !== 'checkbox') {
        ;(this.checked = !1),
          console.error(
            'The checked attribute can only be used on menu items with type="checkbox"',
            this,
          )
        return
      }
      this.type === 'checkbox'
        ? this.setAttribute('aria-checked', this.checked ? 'true' : 'false')
        : this.removeAttribute('aria-checked')
    }
    handleDisabledChange() {
      this.setAttribute('aria-disabled', this.disabled ? 'true' : 'false')
    }
    handleTypeChange() {
      this.type === 'checkbox'
        ? (this.setAttribute('role', 'menuitemcheckbox'),
          this.setAttribute('aria-checked', this.checked ? 'true' : 'false'))
        : (this.setAttribute('role', 'menuitem'),
          this.removeAttribute('aria-checked'))
    }
    /** Returns a text label based on the contents of the menu item's default slot. */
    getTextLabel() {
      return oC(this.defaultSlot)
    }
    isSubmenu() {
      return this.hasSlotController.test('submenu')
    }
    render() {
      const t = this.localize.dir() === 'rtl',
        r = this.submenuController.isExpanded()
      return C`
      <div
        id="anchor"
        part="base"
        class=${ct({
          'menu-item': !0,
          'menu-item--rtl': t,
          'menu-item--checked': this.checked,
          'menu-item--disabled': this.disabled,
          'menu-item--loading': this.loading,
          'menu-item--has-submenu': this.isSubmenu(),
          'menu-item--submenu-expanded': r,
        })}
        ?aria-haspopup="${this.isSubmenu()}"
        ?aria-expanded="${!!r}"
      >
        <span part="checked-icon" class="menu-item__check">
          <sl-icon name="check" library="system" aria-hidden="true"></sl-icon>
        </span>

        <slot name="prefix" part="prefix" class="menu-item__prefix"></slot>

        <slot part="label" class="menu-item__label" @slotchange=${
          this.handleDefaultSlotChange
        }></slot>

        <slot name="suffix" part="suffix" class="menu-item__suffix"></slot>

        <span part="submenu-icon" class="menu-item__chevron">
          <sl-icon name=${
            t ? 'chevron-left' : 'chevron-right'
          } library="system" aria-hidden="true"></sl-icon>
        </span>

        ${this.submenuController.renderSubmenu()}
        ${
          this.loading
            ? C` <sl-spinner part="spinner" exportparts="base:spinner__base"></sl-spinner> `
            : ''
        }
      </div>
    `
    }
  }
hr.styles = [dt, LC]
hr.dependencies = {
  'sl-icon': Nt,
  'sl-popup': Ut,
  'sl-spinner': No,
}
c([J('slot:not([name])')], hr.prototype, 'defaultSlot', 2)
c([J('.menu-item')], hr.prototype, 'menuItem', 2)
c([p()], hr.prototype, 'type', 2)
c([p({ type: Boolean, reflect: !0 })], hr.prototype, 'checked', 2)
c([p()], hr.prototype, 'value', 2)
c([p({ type: Boolean, reflect: !0 })], hr.prototype, 'loading', 2)
c([p({ type: Boolean, reflect: !0 })], hr.prototype, 'disabled', 2)
c([X('checked')], hr.prototype, 'handleCheckedChange', 1)
c([X('disabled')], hr.prototype, 'handleDisabledChange', 1)
c([X('type')], hr.prototype, 'handleTypeChange', 1)
hr.define('sl-menu-item')
var HC = st`
  :host {
    --divider-width: 2px;
    --handle-size: 2.5rem;

    display: inline-block;
    position: relative;
  }

  .image-comparer {
    max-width: 100%;
    max-height: 100%;
    overflow: hidden;
  }

  .image-comparer__before,
  .image-comparer__after {
    display: block;
    pointer-events: none;
  }

  .image-comparer__before::slotted(img),
  .image-comparer__after::slotted(img),
  .image-comparer__before::slotted(svg),
  .image-comparer__after::slotted(svg) {
    display: block;
    max-width: 100% !important;
    height: auto;
  }

  .image-comparer__after {
    position: absolute;
    top: 0;
    left: 0;
    height: 100%;
    width: 100%;
  }

  .image-comparer__divider {
    display: flex;
    align-items: center;
    justify-content: center;
    position: absolute;
    top: 0;
    width: var(--divider-width);
    height: 100%;
    background-color: var(--sl-color-neutral-0);
    translate: calc(var(--divider-width) / -2);
    cursor: ew-resize;
  }

  .image-comparer__handle {
    display: flex;
    align-items: center;
    justify-content: center;
    position: absolute;
    top: calc(50% - (var(--handle-size) / 2));
    width: var(--handle-size);
    height: var(--handle-size);
    background-color: var(--sl-color-neutral-0);
    border-radius: var(--sl-border-radius-circle);
    font-size: calc(var(--handle-size) * 0.5);
    color: var(--sl-color-neutral-700);
    cursor: inherit;
    z-index: 10;
  }

  .image-comparer__handle:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }
`,
  un = class extends nt {
    constructor() {
      super(...arguments), (this.localize = new Dt(this)), (this.position = 50)
    }
    handleDrag(t) {
      const { width: r } = this.base.getBoundingClientRect(),
        i = this.localize.dir() === 'rtl'
      t.preventDefault(),
        wo(this.base, {
          onMove: o => {
            ;(this.position = parseFloat(ue((o / r) * 100, 0, 100).toFixed(2))),
              i && (this.position = 100 - this.position)
          },
          initialEvent: t,
        })
    }
    handleKeyDown(t) {
      const r = this.localize.dir() === 'ltr',
        i = this.localize.dir() === 'rtl'
      if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(t.key)) {
        const o = t.shiftKey ? 10 : 1
        let a = this.position
        t.preventDefault(),
          ((r && t.key === 'ArrowLeft') || (i && t.key === 'ArrowRight')) &&
            (a -= o),
          ((r && t.key === 'ArrowRight') || (i && t.key === 'ArrowLeft')) &&
            (a += o),
          t.key === 'Home' && (a = 0),
          t.key === 'End' && (a = 100),
          (a = ue(a, 0, 100)),
          (this.position = a)
      }
    }
    handlePositionChange() {
      this.emit('sl-change')
    }
    render() {
      const t = this.localize.dir() === 'rtl'
      return C`
      <div
        part="base"
        id="image-comparer"
        class=${ct({
          'image-comparer': !0,
          'image-comparer--rtl': t,
        })}
        @keydown=${this.handleKeyDown}
      >
        <div class="image-comparer__image">
          <div part="before" class="image-comparer__before">
            <slot name="before"></slot>
          </div>

          <div
            part="after"
            class="image-comparer__after"
            style=${Ke({
              clipPath: t
                ? `inset(0 0 0 ${100 - this.position}%)`
                : `inset(0 ${100 - this.position}% 0 0)`,
            })}
          >
            <slot name="after"></slot>
          </div>
        </div>

        <div
          part="divider"
          class="image-comparer__divider"
          style=${Ke({
            left: t ? `${100 - this.position}%` : `${this.position}%`,
          })}
          @mousedown=${this.handleDrag}
          @touchstart=${this.handleDrag}
        >
          <div
            part="handle"
            class="image-comparer__handle"
            role="scrollbar"
            aria-valuenow=${this.position}
            aria-valuemin="0"
            aria-valuemax="100"
            aria-controls="image-comparer"
            tabindex="0"
          >
            <slot name="handle">
              <sl-icon library="system" name="grip-vertical"></sl-icon>
            </slot>
          </div>
        </div>
      </div>
    `
    }
  }
un.styles = [dt, HC]
un.scopedElement = { 'sl-icon': Nt }
c([J('.image-comparer')], un.prototype, 'base', 2)
c([J('.image-comparer__handle')], un.prototype, 'handle', 2)
c([p({ type: Number, reflect: !0 })], un.prototype, 'position', 2)
c(
  [X('position', { waitUntilFirstUpdate: !0 })],
  un.prototype,
  'handlePositionChange',
  1,
)
un.define('sl-image-comparer')
var VC = st`
  :host {
    display: block;
  }
`,
  Xl = /* @__PURE__ */ new Map()
function WC(t, r = 'cors') {
  const i = Xl.get(t)
  if (i !== void 0) return Promise.resolve(i)
  const o = fetch(t, { mode: r }).then(async a => {
    const d = {
      ok: a.ok,
      status: a.status,
      html: await a.text(),
    }
    return Xl.set(t, d), d
  })
  return Xl.set(t, o), o
}
var Hn = class extends nt {
  constructor() {
    super(...arguments), (this.mode = 'cors'), (this.allowScripts = !1)
  }
  executeScript(t) {
    const r = document.createElement('script')
    ;[...t.attributes].forEach(i => r.setAttribute(i.name, i.value)),
      (r.textContent = t.textContent),
      t.parentNode.replaceChild(r, t)
  }
  async handleSrcChange() {
    try {
      const t = this.src,
        r = await WC(t, this.mode)
      if (t !== this.src) return
      if (!r.ok) {
        this.emit('sl-error', { detail: { status: r.status } })
        return
      }
      ;(this.innerHTML = r.html),
        this.allowScripts &&
          [...this.querySelectorAll('script')].forEach(i =>
            this.executeScript(i),
          ),
        this.emit('sl-load')
    } catch {
      this.emit('sl-error', { detail: { status: -1 } })
    }
  }
  render() {
    return C`<slot></slot>`
  }
}
Hn.styles = [dt, VC]
c([p()], Hn.prototype, 'src', 2)
c([p()], Hn.prototype, 'mode', 2)
c(
  [p({ attribute: 'allow-scripts', type: Boolean })],
  Hn.prototype,
  'allowScripts',
  2,
)
c([X('src')], Hn.prototype, 'handleSrcChange', 1)
Hn.define('sl-include')
Nt.define('sl-icon')
ye.define('sl-icon-button')
var ha = class extends nt {
  constructor() {
    super(...arguments),
      (this.localize = new Dt(this)),
      (this.value = 0),
      (this.unit = 'byte'),
      (this.display = 'short')
  }
  render() {
    if (isNaN(this.value)) return ''
    const t = ['', 'kilo', 'mega', 'giga', 'tera'],
      r = ['', 'kilo', 'mega', 'giga', 'tera', 'peta'],
      i = this.unit === 'bit' ? t : r,
      o = Math.max(
        0,
        Math.min(Math.floor(Math.log10(this.value) / 3), i.length - 1),
      ),
      a = i[o] + this.unit,
      d = parseFloat((this.value / Math.pow(1e3, o)).toPrecision(3))
    return this.localize.number(d, {
      style: 'unit',
      unit: a,
      unitDisplay: this.display,
    })
  }
}
c([p({ type: Number })], ha.prototype, 'value', 2)
c([p()], ha.prototype, 'unit', 2)
c([p()], ha.prototype, 'display', 2)
ha.define('sl-format-bytes')
var ur = class extends nt {
  constructor() {
    super(...arguments),
      (this.localize = new Dt(this)),
      (this.date = /* @__PURE__ */ new Date()),
      (this.hourFormat = 'auto')
  }
  render() {
    const t = new Date(this.date),
      r = this.hourFormat === 'auto' ? void 0 : this.hourFormat === '12'
    if (!isNaN(t.getMilliseconds()))
      return C`
      <time datetime=${t.toISOString()}>
        ${this.localize.date(t, {
          weekday: this.weekday,
          era: this.era,
          year: this.year,
          month: this.month,
          day: this.day,
          hour: this.hour,
          minute: this.minute,
          second: this.second,
          timeZoneName: this.timeZoneName,
          timeZone: this.timeZone,
          hour12: r,
        })}
      </time>
    `
  }
}
c([p()], ur.prototype, 'date', 2)
c([p()], ur.prototype, 'weekday', 2)
c([p()], ur.prototype, 'era', 2)
c([p()], ur.prototype, 'year', 2)
c([p()], ur.prototype, 'month', 2)
c([p()], ur.prototype, 'day', 2)
c([p()], ur.prototype, 'hour', 2)
c([p()], ur.prototype, 'minute', 2)
c([p()], ur.prototype, 'second', 2)
c([p({ attribute: 'time-zone-name' })], ur.prototype, 'timeZoneName', 2)
c([p({ attribute: 'time-zone' })], ur.prototype, 'timeZone', 2)
c([p({ attribute: 'hour-format' })], ur.prototype, 'hourFormat', 2)
ur.define('sl-format-date')
var Pr = class extends nt {
  constructor() {
    super(...arguments),
      (this.localize = new Dt(this)),
      (this.value = 0),
      (this.type = 'decimal'),
      (this.noGrouping = !1),
      (this.currency = 'USD'),
      (this.currencyDisplay = 'symbol')
  }
  render() {
    return isNaN(this.value)
      ? ''
      : this.localize.number(this.value, {
          style: this.type,
          currency: this.currency,
          currencyDisplay: this.currencyDisplay,
          useGrouping: !this.noGrouping,
          minimumIntegerDigits: this.minimumIntegerDigits,
          minimumFractionDigits: this.minimumFractionDigits,
          maximumFractionDigits: this.maximumFractionDigits,
          minimumSignificantDigits: this.minimumSignificantDigits,
          maximumSignificantDigits: this.maximumSignificantDigits,
        })
  }
}
c([p({ type: Number })], Pr.prototype, 'value', 2)
c([p()], Pr.prototype, 'type', 2)
c(
  [p({ attribute: 'no-grouping', type: Boolean })],
  Pr.prototype,
  'noGrouping',
  2,
)
c([p()], Pr.prototype, 'currency', 2)
c([p({ attribute: 'currency-display' })], Pr.prototype, 'currencyDisplay', 2)
c(
  [p({ attribute: 'minimum-integer-digits', type: Number })],
  Pr.prototype,
  'minimumIntegerDigits',
  2,
)
c(
  [p({ attribute: 'minimum-fraction-digits', type: Number })],
  Pr.prototype,
  'minimumFractionDigits',
  2,
)
c(
  [p({ attribute: 'maximum-fraction-digits', type: Number })],
  Pr.prototype,
  'maximumFractionDigits',
  2,
)
c(
  [p({ attribute: 'minimum-significant-digits', type: Number })],
  Pr.prototype,
  'minimumSignificantDigits',
  2,
)
c(
  [p({ attribute: 'maximum-significant-digits', type: Number })],
  Pr.prototype,
  'maximumSignificantDigits',
  2,
)
Pr.define('sl-format-number')
var qC = st`
  :host {
    --color: var(--sl-panel-border-color);
    --width: var(--sl-panel-border-width);
    --spacing: var(--sl-spacing-medium);
  }

  :host(:not([vertical])) {
    display: block;
    border-top: solid var(--width) var(--color);
    margin: var(--spacing) 0;
  }

  :host([vertical]) {
    display: inline-block;
    height: 100%;
    border-left: solid var(--width) var(--color);
    margin: 0 var(--spacing);
  }
`,
  ua = class extends nt {
    constructor() {
      super(...arguments), (this.vertical = !1)
    }
    connectedCallback() {
      super.connectedCallback(), this.setAttribute('role', 'separator')
    }
    handleVerticalChange() {
      this.setAttribute(
        'aria-orientation',
        this.vertical ? 'vertical' : 'horizontal',
      )
    }
  }
ua.styles = [dt, qC]
c([p({ type: Boolean, reflect: !0 })], ua.prototype, 'vertical', 2)
c([X('vertical')], ua.prototype, 'handleVerticalChange', 1)
ua.define('sl-divider')
var YC = st`
  :host {
    --size: 25rem;
    --header-spacing: var(--sl-spacing-large);
    --body-spacing: var(--sl-spacing-large);
    --footer-spacing: var(--sl-spacing-large);

    display: contents;
  }

  .drawer {
    top: 0;
    inset-inline-start: 0;
    width: 100%;
    height: 100%;
    pointer-events: none;
    overflow: hidden;
  }

  .drawer--contained {
    position: absolute;
    z-index: initial;
  }

  .drawer--fixed {
    position: fixed;
    z-index: var(--sl-z-index-drawer);
  }

  .drawer__panel {
    position: absolute;
    display: flex;
    flex-direction: column;
    z-index: 2;
    max-width: 100%;
    max-height: 100%;
    background-color: var(--sl-panel-background-color);
    box-shadow: var(--sl-shadow-x-large);
    overflow: auto;
    pointer-events: all;
  }

  .drawer__panel:focus {
    outline: none;
  }

  .drawer--top .drawer__panel {
    top: 0;
    inset-inline-end: auto;
    bottom: auto;
    inset-inline-start: 0;
    width: 100%;
    height: var(--size);
  }

  .drawer--end .drawer__panel {
    top: 0;
    inset-inline-end: 0;
    bottom: auto;
    inset-inline-start: auto;
    width: var(--size);
    height: 100%;
  }

  .drawer--bottom .drawer__panel {
    top: auto;
    inset-inline-end: auto;
    bottom: 0;
    inset-inline-start: 0;
    width: 100%;
    height: var(--size);
  }

  .drawer--start .drawer__panel {
    top: 0;
    inset-inline-end: auto;
    bottom: auto;
    inset-inline-start: 0;
    width: var(--size);
    height: 100%;
  }

  .drawer__header {
    display: flex;
  }

  .drawer__title {
    flex: 1 1 auto;
    font: inherit;
    font-size: var(--sl-font-size-large);
    line-height: var(--sl-line-height-dense);
    padding: var(--header-spacing);
    margin: 0;
  }

  .drawer__header-actions {
    flex-shrink: 0;
    display: flex;
    flex-wrap: wrap;
    justify-content: end;
    gap: var(--sl-spacing-2x-small);
    padding: 0 var(--header-spacing);
  }

  .drawer__header-actions sl-icon-button,
  .drawer__header-actions ::slotted(sl-icon-button) {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    font-size: var(--sl-font-size-medium);
  }

  .drawer__body {
    flex: 1 1 auto;
    display: block;
    padding: var(--body-spacing);
    overflow: auto;
    -webkit-overflow-scrolling: touch;
  }

  .drawer__footer {
    text-align: right;
    padding: var(--footer-spacing);
  }

  .drawer__footer ::slotted(sl-button:not(:last-of-type)) {
    margin-inline-end: var(--sl-spacing-x-small);
  }

  .drawer:not(.drawer--has-footer) .drawer__footer {
    display: none;
  }

  .drawer__overlay {
    display: block;
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    background-color: var(--sl-overlay-background-color);
    pointer-events: all;
  }

  .drawer--contained .drawer__overlay {
    display: none;
  }

  @media (forced-colors: active) {
    .drawer__panel {
      border: solid 1px var(--sl-color-neutral-0);
    }
  }
`,
  wp = /* @__PURE__ */ new WeakMap()
function Cf(t) {
  let r = wp.get(t)
  return r || ((r = window.getComputedStyle(t, null)), wp.set(t, r)), r
}
function KC(t) {
  if (typeof t.checkVisibility == 'function')
    return t.checkVisibility({ checkOpacity: !1, checkVisibilityCSS: !0 })
  const r = Cf(t)
  return r.visibility !== 'hidden' && r.display !== 'none'
}
function GC(t) {
  const r = Cf(t),
    { overflowY: i, overflowX: o } = r
  return i === 'scroll' || o === 'scroll'
    ? !0
    : i !== 'auto' || o !== 'auto'
    ? !1
    : (t.scrollHeight > t.clientHeight && i === 'auto') ||
      (t.scrollWidth > t.clientWidth && o === 'auto')
}
function XC(t) {
  const r = t.tagName.toLowerCase(),
    i = Number(t.getAttribute('tabindex'))
  return (t.hasAttribute('tabindex') && (isNaN(i) || i <= -1)) ||
    t.hasAttribute('disabled') ||
    t.closest('[inert]') ||
    (r === 'input' &&
      t.getAttribute('type') === 'radio' &&
      !t.hasAttribute('checked')) ||
    !KC(t)
    ? !1
    : ((r === 'audio' || r === 'video') && t.hasAttribute('controls')) ||
      t.hasAttribute('tabindex') ||
      (t.hasAttribute('contenteditable') &&
        t.getAttribute('contenteditable') !== 'false') ||
      [
        'button',
        'input',
        'select',
        'textarea',
        'a',
        'audio',
        'video',
        'summary',
        'iframe',
      ].includes(r)
    ? !0
    : GC(t)
}
function jC(t) {
  var r, i
  const o = cc(t),
    a = (r = o[0]) != null ? r : null,
    d = (i = o[o.length - 1]) != null ? i : null
  return { start: a, end: d }
}
function ZC(t, r) {
  var i
  return ((i = t.getRootNode({ composed: !0 })) == null ? void 0 : i.host) !== r
}
function cc(t) {
  const r = /* @__PURE__ */ new WeakMap(),
    i = []
  function o(a) {
    if (a instanceof Element) {
      if (a.hasAttribute('inert') || a.closest('[inert]') || r.has(a)) return
      r.set(a, !0),
        !i.includes(a) && XC(a) && i.push(a),
        a instanceof HTMLSlotElement &&
          ZC(a, t) &&
          a.assignedElements({ flatten: !0 }).forEach(d => {
            o(d)
          }),
        a.shadowRoot !== null && a.shadowRoot.mode === 'open' && o(a.shadowRoot)
    }
    for (const d of a.children) o(d)
  }
  return (
    o(t),
    i.sort((a, d) => {
      const h = Number(a.getAttribute('tabindex')) || 0
      return (Number(d.getAttribute('tabindex')) || 0) - h
    })
  )
}
function* Oc(t = document.activeElement) {
  t != null &&
    (yield t,
    'shadowRoot' in t &&
      t.shadowRoot &&
      t.shadowRoot.mode !== 'closed' &&
      (yield* J_(Oc(t.shadowRoot.activeElement))))
}
function JC() {
  return [...Oc()].pop()
}
var bo = [],
  Sf = class {
    constructor(t) {
      ;(this.tabDirection = 'forward'),
        (this.handleFocusIn = () => {
          this.isActive() && this.checkFocus()
        }),
        (this.handleKeyDown = r => {
          var i
          if (r.key !== 'Tab' || this.isExternalActivated || !this.isActive())
            return
          const o = JC()
          if (
            ((this.previousFocus = o),
            this.previousFocus &&
              this.possiblyHasTabbableChildren(this.previousFocus))
          )
            return
          r.shiftKey
            ? (this.tabDirection = 'backward')
            : (this.tabDirection = 'forward')
          const a = cc(this.element)
          let d = a.findIndex(m => m === o)
          this.previousFocus = this.currentFocus
          const h = this.tabDirection === 'forward' ? 1 : -1
          for (;;) {
            d + h >= a.length
              ? (d = 0)
              : d + h < 0
              ? (d = a.length - 1)
              : (d += h),
              (this.previousFocus = this.currentFocus)
            const m =
              /** @type {HTMLElement} */
              a[d]
            if (
              (this.tabDirection === 'backward' &&
                this.previousFocus &&
                this.possiblyHasTabbableChildren(this.previousFocus)) ||
              (m && this.possiblyHasTabbableChildren(m))
            )
              return
            r.preventDefault(),
              (this.currentFocus = m),
              (i = this.currentFocus) == null || i.focus({ preventScroll: !1 })
            const f = [...Oc()]
            if (
              f.includes(this.currentFocus) ||
              !f.includes(this.previousFocus)
            )
              break
          }
          setTimeout(() => this.checkFocus())
        }),
        (this.handleKeyUp = () => {
          this.tabDirection = 'forward'
        }),
        (this.element = t),
        (this.elementsWithTabbableControls = ['iframe'])
    }
    /** Activates focus trapping. */
    activate() {
      bo.push(this.element),
        document.addEventListener('focusin', this.handleFocusIn),
        document.addEventListener('keydown', this.handleKeyDown),
        document.addEventListener('keyup', this.handleKeyUp)
    }
    /** Deactivates focus trapping. */
    deactivate() {
      ;(bo = bo.filter(t => t !== this.element)),
        (this.currentFocus = null),
        document.removeEventListener('focusin', this.handleFocusIn),
        document.removeEventListener('keydown', this.handleKeyDown),
        document.removeEventListener('keyup', this.handleKeyUp)
    }
    /** Determines if this modal element is currently active or not. */
    isActive() {
      return bo[bo.length - 1] === this.element
    }
    /** Activates external modal behavior and temporarily disables focus trapping. */
    activateExternal() {
      this.isExternalActivated = !0
    }
    /** Deactivates external modal behavior and re-enables focus trapping. */
    deactivateExternal() {
      this.isExternalActivated = !1
    }
    checkFocus() {
      if (this.isActive() && !this.isExternalActivated) {
        const t = cc(this.element)
        if (!this.element.matches(':focus-within')) {
          const r = t[0],
            i = t[t.length - 1],
            o = this.tabDirection === 'forward' ? r : i
          typeof (o == null ? void 0 : o.focus) == 'function' &&
            ((this.currentFocus = o), o.focus({ preventScroll: !1 }))
        }
      }
    }
    possiblyHasTabbableChildren(t) {
      return (
        this.elementsWithTabbableControls.includes(t.tagName.toLowerCase()) ||
        t.hasAttribute('controls')
      )
    }
  }
function xp(t) {
  return t.charAt(0).toUpperCase() + t.slice(1)
}
var pr = class extends nt {
  constructor() {
    super(...arguments),
      (this.hasSlotController = new He(this, 'footer')),
      (this.localize = new Dt(this)),
      (this.modal = new Sf(this)),
      (this.open = !1),
      (this.label = ''),
      (this.placement = 'end'),
      (this.contained = !1),
      (this.noHeader = !1),
      (this.handleDocumentKeyDown = t => {
        this.contained ||
          (t.key === 'Escape' &&
            this.modal.isActive() &&
            this.open &&
            (t.stopImmediatePropagation(), this.requestClose('keyboard')))
      })
  }
  firstUpdated() {
    ;(this.drawer.hidden = !this.open),
      this.open &&
        (this.addOpenListeners(),
        this.contained || (this.modal.activate(), ko(this)))
  }
  disconnectedCallback() {
    var t
    super.disconnectedCallback(),
      $o(this),
      (t = this.closeWatcher) == null || t.destroy()
  }
  requestClose(t) {
    if (
      this.emit('sl-request-close', {
        cancelable: !0,
        detail: { source: t },
      }).defaultPrevented
    ) {
      const i = Xt(this, 'drawer.denyClose', { dir: this.localize.dir() })
      ie(this.panel, i.keyframes, i.options)
      return
    }
    this.hide()
  }
  addOpenListeners() {
    var t
    'CloseWatcher' in window
      ? ((t = this.closeWatcher) == null || t.destroy(),
        this.contained ||
          ((this.closeWatcher = new CloseWatcher()),
          (this.closeWatcher.onclose = () => this.requestClose('keyboard'))))
      : document.addEventListener('keydown', this.handleDocumentKeyDown)
  }
  removeOpenListeners() {
    var t
    document.removeEventListener('keydown', this.handleDocumentKeyDown),
      (t = this.closeWatcher) == null || t.destroy()
  }
  async handleOpenChange() {
    if (this.open) {
      this.emit('sl-show'),
        this.addOpenListeners(),
        (this.originalTrigger = document.activeElement),
        this.contained || (this.modal.activate(), ko(this))
      const t = this.querySelector('[autofocus]')
      t && t.removeAttribute('autofocus'),
        await Promise.all([pe(this.drawer), pe(this.overlay)]),
        (this.drawer.hidden = !1),
        requestAnimationFrame(() => {
          this.emit('sl-initial-focus', { cancelable: !0 }).defaultPrevented ||
            (t
              ? t.focus({ preventScroll: !0 })
              : this.panel.focus({ preventScroll: !0 })),
            t && t.setAttribute('autofocus', '')
        })
      const r = Xt(this, `drawer.show${xp(this.placement)}`, {
          dir: this.localize.dir(),
        }),
        i = Xt(this, 'drawer.overlay.show', { dir: this.localize.dir() })
      await Promise.all([
        ie(this.panel, r.keyframes, r.options),
        ie(this.overlay, i.keyframes, i.options),
      ]),
        this.emit('sl-after-show')
    } else {
      this.emit('sl-hide'),
        this.removeOpenListeners(),
        this.contained || (this.modal.deactivate(), $o(this)),
        await Promise.all([pe(this.drawer), pe(this.overlay)])
      const t = Xt(this, `drawer.hide${xp(this.placement)}`, {
          dir: this.localize.dir(),
        }),
        r = Xt(this, 'drawer.overlay.hide', { dir: this.localize.dir() })
      await Promise.all([
        ie(this.overlay, r.keyframes, r.options).then(() => {
          this.overlay.hidden = !0
        }),
        ie(this.panel, t.keyframes, t.options).then(() => {
          this.panel.hidden = !0
        }),
      ]),
        (this.drawer.hidden = !0),
        (this.overlay.hidden = !1),
        (this.panel.hidden = !1)
      const i = this.originalTrigger
      typeof (i == null ? void 0 : i.focus) == 'function' &&
        setTimeout(() => i.focus()),
        this.emit('sl-after-hide')
    }
  }
  handleNoModalChange() {
    this.open && !this.contained && (this.modal.activate(), ko(this)),
      this.open && this.contained && (this.modal.deactivate(), $o(this))
  }
  /** Shows the drawer. */
  async show() {
    if (!this.open) return (this.open = !0), Ue(this, 'sl-after-show')
  }
  /** Hides the drawer */
  async hide() {
    if (this.open) return (this.open = !1), Ue(this, 'sl-after-hide')
  }
  render() {
    return C`
      <div
        part="base"
        class=${ct({
          drawer: !0,
          'drawer--open': this.open,
          'drawer--top': this.placement === 'top',
          'drawer--end': this.placement === 'end',
          'drawer--bottom': this.placement === 'bottom',
          'drawer--start': this.placement === 'start',
          'drawer--contained': this.contained,
          'drawer--fixed': !this.contained,
          'drawer--rtl': this.localize.dir() === 'rtl',
          'drawer--has-footer': this.hasSlotController.test('footer'),
        })}
      >
        <div part="overlay" class="drawer__overlay" @click=${() =>
          this.requestClose('overlay')} tabindex="-1"></div>

        <div
          part="panel"
          class="drawer__panel"
          role="dialog"
          aria-modal="true"
          aria-hidden=${this.open ? 'false' : 'true'}
          aria-label=${it(this.noHeader ? this.label : void 0)}
          aria-labelledby=${it(this.noHeader ? void 0 : 'title')}
          tabindex="0"
        >
          ${
            this.noHeader
              ? ''
              : C`
                <header part="header" class="drawer__header">
                  <h2 part="title" class="drawer__title" id="title">
                    <!-- If there's no label, use an invisible character to prevent the header from collapsing -->
                    <slot name="label"> ${
                      this.label.length > 0 ? this.label : '\uFEFF'
                    } </slot>
                  </h2>
                  <div part="header-actions" class="drawer__header-actions">
                    <slot name="header-actions"></slot>
                    <sl-icon-button
                      part="close-button"
                      exportparts="base:close-button__base"
                      class="drawer__close"
                      name="x-lg"
                      label=${this.localize.term('close')}
                      library="system"
                      @click=${() => this.requestClose('close-button')}
                    ></sl-icon-button>
                  </div>
                </header>
              `
          }

          <slot part="body" class="drawer__body"></slot>

          <footer part="footer" class="drawer__footer">
            <slot name="footer"></slot>
          </footer>
        </div>
      </div>
    `
  }
}
pr.styles = [dt, YC]
pr.dependencies = { 'sl-icon-button': ye }
c([J('.drawer')], pr.prototype, 'drawer', 2)
c([J('.drawer__panel')], pr.prototype, 'panel', 2)
c([J('.drawer__overlay')], pr.prototype, 'overlay', 2)
c([p({ type: Boolean, reflect: !0 })], pr.prototype, 'open', 2)
c([p({ reflect: !0 })], pr.prototype, 'label', 2)
c([p({ reflect: !0 })], pr.prototype, 'placement', 2)
c([p({ type: Boolean, reflect: !0 })], pr.prototype, 'contained', 2)
c(
  [p({ attribute: 'no-header', type: Boolean, reflect: !0 })],
  pr.prototype,
  'noHeader',
  2,
)
c(
  [X('open', { waitUntilFirstUpdate: !0 })],
  pr.prototype,
  'handleOpenChange',
  1,
)
c(
  [X('contained', { waitUntilFirstUpdate: !0 })],
  pr.prototype,
  'handleNoModalChange',
  1,
)
Mt('drawer.showTop', {
  keyframes: [
    { opacity: 0, translate: '0 -100%' },
    { opacity: 1, translate: '0 0' },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('drawer.hideTop', {
  keyframes: [
    { opacity: 1, translate: '0 0' },
    { opacity: 0, translate: '0 -100%' },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('drawer.showEnd', {
  keyframes: [
    { opacity: 0, translate: '100%' },
    { opacity: 1, translate: '0' },
  ],
  rtlKeyframes: [
    { opacity: 0, translate: '-100%' },
    { opacity: 1, translate: '0' },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('drawer.hideEnd', {
  keyframes: [
    { opacity: 1, translate: '0' },
    { opacity: 0, translate: '100%' },
  ],
  rtlKeyframes: [
    { opacity: 1, translate: '0' },
    { opacity: 0, translate: '-100%' },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('drawer.showBottom', {
  keyframes: [
    { opacity: 0, translate: '0 100%' },
    { opacity: 1, translate: '0 0' },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('drawer.hideBottom', {
  keyframes: [
    { opacity: 1, translate: '0 0' },
    { opacity: 0, translate: '0 100%' },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('drawer.showStart', {
  keyframes: [
    { opacity: 0, translate: '-100%' },
    { opacity: 1, translate: '0' },
  ],
  rtlKeyframes: [
    { opacity: 0, translate: '100%' },
    { opacity: 1, translate: '0' },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('drawer.hideStart', {
  keyframes: [
    { opacity: 1, translate: '0' },
    { opacity: 0, translate: '-100%' },
  ],
  rtlKeyframes: [
    { opacity: 1, translate: '0' },
    { opacity: 0, translate: '100%' },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('drawer.denyClose', {
  keyframes: [{ scale: 1 }, { scale: 1.01 }, { scale: 1 }],
  options: { duration: 250 },
})
Mt('drawer.overlay.show', {
  keyframes: [{ opacity: 0 }, { opacity: 1 }],
  options: { duration: 250 },
})
Mt('drawer.overlay.hide', {
  keyframes: [{ opacity: 1 }, { opacity: 0 }],
  options: { duration: 250 },
})
pr.define('sl-drawer')
var QC = st`
  :host {
    display: inline-block;
  }

  .dropdown::part(popup) {
    z-index: var(--sl-z-index-dropdown);
  }

  .dropdown[data-current-placement^='top']::part(popup) {
    transform-origin: bottom;
  }

  .dropdown[data-current-placement^='bottom']::part(popup) {
    transform-origin: top;
  }

  .dropdown[data-current-placement^='left']::part(popup) {
    transform-origin: right;
  }

  .dropdown[data-current-placement^='right']::part(popup) {
    transform-origin: left;
  }

  .dropdown__trigger {
    display: block;
  }

  .dropdown__panel {
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-medium);
    font-weight: var(--sl-font-weight-normal);
    box-shadow: var(--sl-shadow-large);
    border-radius: var(--sl-border-radius-medium);
    pointer-events: none;
  }

  .dropdown--open .dropdown__panel {
    display: block;
    pointer-events: all;
  }

  /* When users slot a menu, make sure it conforms to the popup's auto-size */
  ::slotted(sl-menu) {
    max-width: var(--auto-size-available-width) !important;
    max-height: var(--auto-size-available-height) !important;
  }
`,
  Ee = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.open = !1),
        (this.placement = 'bottom-start'),
        (this.disabled = !1),
        (this.stayOpenOnSelect = !1),
        (this.distance = 0),
        (this.skidding = 0),
        (this.hoist = !1),
        (this.sync = void 0),
        (this.handleKeyDown = t => {
          this.open &&
            t.key === 'Escape' &&
            (t.stopPropagation(), this.hide(), this.focusOnTrigger())
        }),
        (this.handleDocumentKeyDown = t => {
          var r
          if (t.key === 'Escape' && this.open && !this.closeWatcher) {
            t.stopPropagation(), this.focusOnTrigger(), this.hide()
            return
          }
          if (t.key === 'Tab') {
            if (
              this.open &&
              ((r = document.activeElement) == null
                ? void 0
                : r.tagName.toLowerCase()) === 'sl-menu-item'
            ) {
              t.preventDefault(), this.hide(), this.focusOnTrigger()
              return
            }
            setTimeout(() => {
              var i, o, a
              const d =
                ((i = this.containingElement) == null
                  ? void 0
                  : i.getRootNode()) instanceof ShadowRoot
                  ? (a =
                      (o = document.activeElement) == null
                        ? void 0
                        : o.shadowRoot) == null
                    ? void 0
                    : a.activeElement
                  : document.activeElement
              ;(!this.containingElement ||
                (d == null
                  ? void 0
                  : d.closest(this.containingElement.tagName.toLowerCase())) !==
                  this.containingElement) &&
                this.hide()
            })
          }
        }),
        (this.handleDocumentMouseDown = t => {
          const r = t.composedPath()
          this.containingElement &&
            !r.includes(this.containingElement) &&
            this.hide()
        }),
        (this.handlePanelSelect = t => {
          const r = t.target
          !this.stayOpenOnSelect &&
            r.tagName.toLowerCase() === 'sl-menu' &&
            (this.hide(), this.focusOnTrigger())
        })
    }
    connectedCallback() {
      super.connectedCallback(),
        this.containingElement || (this.containingElement = this)
    }
    firstUpdated() {
      ;(this.panel.hidden = !this.open),
        this.open && (this.addOpenListeners(), (this.popup.active = !0))
    }
    disconnectedCallback() {
      super.disconnectedCallback(), this.removeOpenListeners(), this.hide()
    }
    focusOnTrigger() {
      const t = this.trigger.assignedElements({ flatten: !0 })[0]
      typeof (t == null ? void 0 : t.focus) == 'function' && t.focus()
    }
    getMenu() {
      return this.panel
        .assignedElements({ flatten: !0 })
        .find(t => t.tagName.toLowerCase() === 'sl-menu')
    }
    handleTriggerClick() {
      this.open ? this.hide() : (this.show(), this.focusOnTrigger())
    }
    async handleTriggerKeyDown(t) {
      if ([' ', 'Enter'].includes(t.key)) {
        t.preventDefault(), this.handleTriggerClick()
        return
      }
      const r = this.getMenu()
      if (r) {
        const i = r.getAllItems(),
          o = i[0],
          a = i[i.length - 1]
        ;['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(t.key) &&
          (t.preventDefault(),
          this.open || (this.show(), await this.updateComplete),
          i.length > 0 &&
            this.updateComplete.then(() => {
              ;(t.key === 'ArrowDown' || t.key === 'Home') &&
                (r.setCurrentItem(o), o.focus()),
                (t.key === 'ArrowUp' || t.key === 'End') &&
                  (r.setCurrentItem(a), a.focus())
            }))
      }
    }
    handleTriggerKeyUp(t) {
      t.key === ' ' && t.preventDefault()
    }
    handleTriggerSlotChange() {
      this.updateAccessibleTrigger()
    }
    //
    // Slotted triggers can be arbitrary content, but we need to link them to the dropdown panel with `aria-haspopup` and
    // `aria-expanded`. These must be applied to the "accessible trigger" (the tabbable portion of the trigger element
    // that gets slotted in) so screen readers will understand them. The accessible trigger could be the slotted element,
    // a child of the slotted element, or an element in the slotted element's shadow root.
    //
    // For example, the accessible trigger of an <sl-button> is a <button> located inside its shadow root.
    //
    // To determine this, we assume the first tabbable element in the trigger slot is the "accessible trigger."
    //
    updateAccessibleTrigger() {
      const r = this.trigger
        .assignedElements({ flatten: !0 })
        .find(o => jC(o).start)
      let i
      if (r) {
        switch (r.tagName.toLowerCase()) {
          case 'sl-button':
          case 'sl-icon-button':
            i = r.button
            break
          default:
            i = r
        }
        i.setAttribute('aria-haspopup', 'true'),
          i.setAttribute('aria-expanded', this.open ? 'true' : 'false')
      }
    }
    /** Shows the dropdown panel. */
    async show() {
      if (!this.open) return (this.open = !0), Ue(this, 'sl-after-show')
    }
    /** Hides the dropdown panel */
    async hide() {
      if (this.open) return (this.open = !1), Ue(this, 'sl-after-hide')
    }
    /**
     * Instructs the dropdown menu to reposition. Useful when the position or size of the trigger changes when the menu
     * is activated.
     */
    reposition() {
      this.popup.reposition()
    }
    addOpenListeners() {
      var t
      this.panel.addEventListener('sl-select', this.handlePanelSelect),
        'CloseWatcher' in window
          ? ((t = this.closeWatcher) == null || t.destroy(),
            (this.closeWatcher = new CloseWatcher()),
            (this.closeWatcher.onclose = () => {
              this.hide(), this.focusOnTrigger()
            }))
          : this.panel.addEventListener('keydown', this.handleKeyDown),
        document.addEventListener('keydown', this.handleDocumentKeyDown),
        document.addEventListener('mousedown', this.handleDocumentMouseDown)
    }
    removeOpenListeners() {
      var t
      this.panel &&
        (this.panel.removeEventListener('sl-select', this.handlePanelSelect),
        this.panel.removeEventListener('keydown', this.handleKeyDown)),
        document.removeEventListener('keydown', this.handleDocumentKeyDown),
        document.removeEventListener('mousedown', this.handleDocumentMouseDown),
        (t = this.closeWatcher) == null || t.destroy()
    }
    async handleOpenChange() {
      if (this.disabled) {
        this.open = !1
        return
      }
      if ((this.updateAccessibleTrigger(), this.open)) {
        this.emit('sl-show'),
          this.addOpenListeners(),
          await pe(this),
          (this.panel.hidden = !1),
          (this.popup.active = !0)
        const { keyframes: t, options: r } = Xt(this, 'dropdown.show', {
          dir: this.localize.dir(),
        })
        await ie(this.popup.popup, t, r), this.emit('sl-after-show')
      } else {
        this.emit('sl-hide'), this.removeOpenListeners(), await pe(this)
        const { keyframes: t, options: r } = Xt(this, 'dropdown.hide', {
          dir: this.localize.dir(),
        })
        await ie(this.popup.popup, t, r),
          (this.panel.hidden = !0),
          (this.popup.active = !1),
          this.emit('sl-after-hide')
      }
    }
    render() {
      return C`
      <sl-popup
        part="base"
        exportparts="popup:base__popup"
        id="dropdown"
        placement=${this.placement}
        distance=${this.distance}
        skidding=${this.skidding}
        strategy=${this.hoist ? 'fixed' : 'absolute'}
        flip
        shift
        auto-size="vertical"
        auto-size-padding="10"
        sync=${it(this.sync ? this.sync : void 0)}
        class=${ct({
          dropdown: !0,
          'dropdown--open': this.open,
        })}
      >
        <slot
          name="trigger"
          slot="anchor"
          part="trigger"
          class="dropdown__trigger"
          @click=${this.handleTriggerClick}
          @keydown=${this.handleTriggerKeyDown}
          @keyup=${this.handleTriggerKeyUp}
          @slotchange=${this.handleTriggerSlotChange}
        ></slot>

        <div aria-hidden=${
          this.open ? 'false' : 'true'
        } aria-labelledby="dropdown">
          <slot part="panel" class="dropdown__panel"></slot>
        </div>
      </sl-popup>
    `
    }
  }
Ee.styles = [dt, QC]
Ee.dependencies = { 'sl-popup': Ut }
c([J('.dropdown')], Ee.prototype, 'popup', 2)
c([J('.dropdown__trigger')], Ee.prototype, 'trigger', 2)
c([J('.dropdown__panel')], Ee.prototype, 'panel', 2)
c([p({ type: Boolean, reflect: !0 })], Ee.prototype, 'open', 2)
c([p({ reflect: !0 })], Ee.prototype, 'placement', 2)
c([p({ type: Boolean, reflect: !0 })], Ee.prototype, 'disabled', 2)
c(
  [p({ attribute: 'stay-open-on-select', type: Boolean, reflect: !0 })],
  Ee.prototype,
  'stayOpenOnSelect',
  2,
)
c([p({ attribute: !1 })], Ee.prototype, 'containingElement', 2)
c([p({ type: Number })], Ee.prototype, 'distance', 2)
c([p({ type: Number })], Ee.prototype, 'skidding', 2)
c([p({ type: Boolean })], Ee.prototype, 'hoist', 2)
c([p({ reflect: !0 })], Ee.prototype, 'sync', 2)
c(
  [X('open', { waitUntilFirstUpdate: !0 })],
  Ee.prototype,
  'handleOpenChange',
  1,
)
Mt('dropdown.show', {
  keyframes: [
    { opacity: 0, scale: 0.9 },
    { opacity: 1, scale: 1 },
  ],
  options: { duration: 100, easing: 'ease' },
})
Mt('dropdown.hide', {
  keyframes: [
    { opacity: 1, scale: 1 },
    { opacity: 0, scale: 0.9 },
  ],
  options: { duration: 100, easing: 'ease' },
})
Ee.define('sl-dropdown')
var tS = st`
  :host {
    --error-color: var(--sl-color-danger-600);
    --success-color: var(--sl-color-success-600);

    display: inline-block;
  }

  .copy-button__button {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    background: none;
    border: none;
    border-radius: var(--sl-border-radius-medium);
    font-size: inherit;
    color: inherit;
    padding: var(--sl-spacing-x-small);
    cursor: pointer;
    transition: var(--sl-transition-x-fast) color;
  }

  .copy-button--success .copy-button__button {
    color: var(--success-color);
  }

  .copy-button--error .copy-button__button {
    color: var(--error-color);
  }

  .copy-button__button:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .copy-button__button[disabled] {
    opacity: 0.5;
    cursor: not-allowed !important;
  }

  slot {
    display: inline-flex;
  }
`,
  ke = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.isCopying = !1),
        (this.status = 'rest'),
        (this.value = ''),
        (this.from = ''),
        (this.disabled = !1),
        (this.copyLabel = ''),
        (this.successLabel = ''),
        (this.errorLabel = ''),
        (this.feedbackDuration = 1e3),
        (this.tooltipPlacement = 'top'),
        (this.hoist = !1)
    }
    async handleCopy() {
      if (this.disabled || this.isCopying) return
      this.isCopying = !0
      let t = this.value
      if (this.from) {
        const r = this.getRootNode(),
          i = this.from.includes('.'),
          o = this.from.includes('[') && this.from.includes(']')
        let a = this.from,
          d = ''
        i
          ? ([a, d] = this.from.trim().split('.'))
          : o && ([a, d] = this.from.trim().replace(/\]$/, '').split('['))
        const h = 'getElementById' in r ? r.getElementById(a) : null
        h
          ? o
            ? (t = h.getAttribute(d) || '')
            : i
            ? (t = h[d] || '')
            : (t = h.textContent || '')
          : (this.showStatus('error'), this.emit('sl-error'))
      }
      if (!t) this.showStatus('error'), this.emit('sl-error')
      else
        try {
          await navigator.clipboard.writeText(t),
            this.showStatus('success'),
            this.emit('sl-copy', {
              detail: {
                value: t,
              },
            })
        } catch {
          this.showStatus('error'), this.emit('sl-error')
        }
    }
    async showStatus(t) {
      const r = this.copyLabel || this.localize.term('copy'),
        i = this.successLabel || this.localize.term('copied'),
        o = this.errorLabel || this.localize.term('error'),
        a = t === 'success' ? this.successIcon : this.errorIcon,
        d = Xt(this, 'copy.in', { dir: 'ltr' }),
        h = Xt(this, 'copy.out', { dir: 'ltr' })
      ;(this.tooltip.content = t === 'success' ? i : o),
        await this.copyIcon.animate(h.keyframes, h.options).finished,
        (this.copyIcon.hidden = !0),
        (this.status = t),
        (a.hidden = !1),
        await a.animate(d.keyframes, d.options).finished,
        setTimeout(async () => {
          await a.animate(h.keyframes, h.options).finished,
            (a.hidden = !0),
            (this.status = 'rest'),
            (this.copyIcon.hidden = !1),
            await this.copyIcon.animate(d.keyframes, d.options).finished,
            (this.tooltip.content = r),
            (this.isCopying = !1)
        }, this.feedbackDuration)
    }
    render() {
      const t = this.copyLabel || this.localize.term('copy')
      return C`
      <sl-tooltip
        class=${ct({
          'copy-button': !0,
          'copy-button--success': this.status === 'success',
          'copy-button--error': this.status === 'error',
        })}
        content=${t}
        placement=${this.tooltipPlacement}
        ?disabled=${this.disabled}
        ?hoist=${this.hoist}
        exportparts="
          base:tooltip__base,
          base__popup:tooltip__base__popup,
          base__arrow:tooltip__base__arrow,
          body:tooltip__body
        "
      >
        <button
          class="copy-button__button"
          part="button"
          type="button"
          ?disabled=${this.disabled}
          @click=${this.handleCopy}
        >
          <slot part="copy-icon" name="copy-icon">
            <sl-icon library="system" name="copy"></sl-icon>
          </slot>
          <slot part="success-icon" name="success-icon" hidden>
            <sl-icon library="system" name="check"></sl-icon>
          </slot>
          <slot part="error-icon" name="error-icon" hidden>
            <sl-icon library="system" name="x-lg"></sl-icon>
          </slot>
        </button>
      </sl-tooltip>
    `
    }
  }
ke.styles = [dt, tS]
ke.dependencies = {
  'sl-icon': Nt,
  'sl-tooltip': le,
}
c([J('slot[name="copy-icon"]')], ke.prototype, 'copyIcon', 2)
c([J('slot[name="success-icon"]')], ke.prototype, 'successIcon', 2)
c([J('slot[name="error-icon"]')], ke.prototype, 'errorIcon', 2)
c([J('sl-tooltip')], ke.prototype, 'tooltip', 2)
c([at()], ke.prototype, 'isCopying', 2)
c([at()], ke.prototype, 'status', 2)
c([p()], ke.prototype, 'value', 2)
c([p()], ke.prototype, 'from', 2)
c([p({ type: Boolean, reflect: !0 })], ke.prototype, 'disabled', 2)
c([p({ attribute: 'copy-label' })], ke.prototype, 'copyLabel', 2)
c([p({ attribute: 'success-label' })], ke.prototype, 'successLabel', 2)
c([p({ attribute: 'error-label' })], ke.prototype, 'errorLabel', 2)
c(
  [p({ attribute: 'feedback-duration', type: Number })],
  ke.prototype,
  'feedbackDuration',
  2,
)
c([p({ attribute: 'tooltip-placement' })], ke.prototype, 'tooltipPlacement', 2)
c([p({ type: Boolean })], ke.prototype, 'hoist', 2)
Mt('copy.in', {
  keyframes: [
    { scale: '.25', opacity: '.25' },
    { scale: '1', opacity: '1' },
  ],
  options: { duration: 100 },
})
Mt('copy.out', {
  keyframes: [
    { scale: '1', opacity: '1' },
    { scale: '.25', opacity: '0' },
  ],
  options: { duration: 100 },
})
ke.define('sl-copy-button')
var eS = st`
  :host {
    display: block;
  }

  .details {
    border: solid 1px var(--sl-color-neutral-200);
    border-radius: var(--sl-border-radius-medium);
    background-color: var(--sl-color-neutral-0);
    overflow-anchor: none;
  }

  .details--disabled {
    opacity: 0.5;
  }

  .details__header {
    display: flex;
    align-items: center;
    border-radius: inherit;
    padding: var(--sl-spacing-medium);
    user-select: none;
    -webkit-user-select: none;
    cursor: pointer;
  }

  .details__header::-webkit-details-marker {
    display: none;
  }

  .details__header:focus {
    outline: none;
  }

  .details__header:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: calc(1px + var(--sl-focus-ring-offset));
  }

  .details--disabled .details__header {
    cursor: not-allowed;
  }

  .details--disabled .details__header:focus-visible {
    outline: none;
    box-shadow: none;
  }

  .details__summary {
    flex: 1 1 auto;
    display: flex;
    align-items: center;
  }

  .details__summary-icon {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    transition: var(--sl-transition-medium) rotate ease;
  }

  .details--open .details__summary-icon {
    rotate: 90deg;
  }

  .details--open.details--rtl .details__summary-icon {
    rotate: -90deg;
  }

  .details--open slot[name='expand-icon'],
  .details:not(.details--open) slot[name='collapse-icon'] {
    display: none;
  }

  .details__body {
    overflow: hidden;
  }

  .details__content {
    display: block;
    padding: var(--sl-spacing-medium);
  }
`,
  Br = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.open = !1),
        (this.disabled = !1)
    }
    firstUpdated() {
      ;(this.body.style.height = this.open ? 'auto' : '0'),
        this.open && (this.details.open = !0),
        (this.detailsObserver = new MutationObserver(t => {
          for (const r of t)
            r.type === 'attributes' &&
              r.attributeName === 'open' &&
              (this.details.open ? this.show() : this.hide())
        })),
        this.detailsObserver.observe(this.details, { attributes: !0 })
    }
    disconnectedCallback() {
      var t
      super.disconnectedCallback(),
        (t = this.detailsObserver) == null || t.disconnect()
    }
    handleSummaryClick(t) {
      t.preventDefault(),
        this.disabled ||
          (this.open ? this.hide() : this.show(), this.header.focus())
    }
    handleSummaryKeyDown(t) {
      ;(t.key === 'Enter' || t.key === ' ') &&
        (t.preventDefault(), this.open ? this.hide() : this.show()),
        (t.key === 'ArrowUp' || t.key === 'ArrowLeft') &&
          (t.preventDefault(), this.hide()),
        (t.key === 'ArrowDown' || t.key === 'ArrowRight') &&
          (t.preventDefault(), this.show())
    }
    async handleOpenChange() {
      if (this.open) {
        if (
          ((this.details.open = !0),
          this.emit('sl-show', { cancelable: !0 }).defaultPrevented)
        ) {
          ;(this.open = !1), (this.details.open = !1)
          return
        }
        await pe(this.body)
        const { keyframes: r, options: i } = Xt(this, 'details.show', {
          dir: this.localize.dir(),
        })
        await ie(this.body, Zs(r, this.body.scrollHeight), i),
          (this.body.style.height = 'auto'),
          this.emit('sl-after-show')
      } else {
        if (this.emit('sl-hide', { cancelable: !0 }).defaultPrevented) {
          ;(this.details.open = !0), (this.open = !0)
          return
        }
        await pe(this.body)
        const { keyframes: r, options: i } = Xt(this, 'details.hide', {
          dir: this.localize.dir(),
        })
        await ie(this.body, Zs(r, this.body.scrollHeight), i),
          (this.body.style.height = 'auto'),
          (this.details.open = !1),
          this.emit('sl-after-hide')
      }
    }
    /** Shows the details. */
    async show() {
      if (!(this.open || this.disabled))
        return (this.open = !0), Ue(this, 'sl-after-show')
    }
    /** Hides the details */
    async hide() {
      if (!(!this.open || this.disabled))
        return (this.open = !1), Ue(this, 'sl-after-hide')
    }
    render() {
      const t = this.localize.dir() === 'rtl'
      return C`
      <details
        part="base"
        class=${ct({
          details: !0,
          'details--open': this.open,
          'details--disabled': this.disabled,
          'details--rtl': t,
        })}
      >
        <summary
          part="header"
          id="header"
          class="details__header"
          role="button"
          aria-expanded=${this.open ? 'true' : 'false'}
          aria-controls="content"
          aria-disabled=${this.disabled ? 'true' : 'false'}
          tabindex=${this.disabled ? '-1' : '0'}
          @click=${this.handleSummaryClick}
          @keydown=${this.handleSummaryKeyDown}
        >
          <slot name="summary" part="summary" class="details__summary">${
            this.summary
          }</slot>

          <span part="summary-icon" class="details__summary-icon">
            <slot name="expand-icon">
              <sl-icon library="system" name=${
                t ? 'chevron-left' : 'chevron-right'
              }></sl-icon>
            </slot>
            <slot name="collapse-icon">
              <sl-icon library="system" name=${
                t ? 'chevron-left' : 'chevron-right'
              }></sl-icon>
            </slot>
          </span>
        </summary>

        <div class="details__body" role="region" aria-labelledby="header">
          <slot part="content" id="content" class="details__content"></slot>
        </div>
      </details>
    `
    }
  }
Br.styles = [dt, eS]
Br.dependencies = {
  'sl-icon': Nt,
}
c([J('.details')], Br.prototype, 'details', 2)
c([J('.details__header')], Br.prototype, 'header', 2)
c([J('.details__body')], Br.prototype, 'body', 2)
c([J('.details__expand-icon-slot')], Br.prototype, 'expandIconSlot', 2)
c([p({ type: Boolean, reflect: !0 })], Br.prototype, 'open', 2)
c([p()], Br.prototype, 'summary', 2)
c([p({ type: Boolean, reflect: !0 })], Br.prototype, 'disabled', 2)
c(
  [X('open', { waitUntilFirstUpdate: !0 })],
  Br.prototype,
  'handleOpenChange',
  1,
)
Mt('details.show', {
  keyframes: [
    { height: '0', opacity: '0' },
    { height: 'auto', opacity: '1' },
  ],
  options: { duration: 250, easing: 'linear' },
})
Mt('details.hide', {
  keyframes: [
    { height: 'auto', opacity: '1' },
    { height: '0', opacity: '0' },
  ],
  options: { duration: 250, easing: 'linear' },
})
Br.define('sl-details')
var rS = st`
  :host {
    --width: 31rem;
    --header-spacing: var(--sl-spacing-large);
    --body-spacing: var(--sl-spacing-large);
    --footer-spacing: var(--sl-spacing-large);

    display: contents;
  }

  .dialog {
    display: flex;
    align-items: center;
    justify-content: center;
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    z-index: var(--sl-z-index-dialog);
  }

  .dialog__panel {
    display: flex;
    flex-direction: column;
    z-index: 2;
    width: var(--width);
    max-width: calc(100% - var(--sl-spacing-2x-large));
    max-height: calc(100% - var(--sl-spacing-2x-large));
    background-color: var(--sl-panel-background-color);
    border-radius: var(--sl-border-radius-medium);
    box-shadow: var(--sl-shadow-x-large);
  }

  .dialog__panel:focus {
    outline: none;
  }

  /* Ensure there's enough vertical padding for phones that don't update vh when chrome appears (e.g. iPhone) */
  @media screen and (max-width: 420px) {
    .dialog__panel {
      max-height: 80vh;
    }
  }

  .dialog--open .dialog__panel {
    display: flex;
    opacity: 1;
  }

  .dialog__header {
    flex: 0 0 auto;
    display: flex;
  }

  .dialog__title {
    flex: 1 1 auto;
    font: inherit;
    font-size: var(--sl-font-size-large);
    line-height: var(--sl-line-height-dense);
    padding: var(--header-spacing);
    margin: 0;
  }

  .dialog__header-actions {
    flex-shrink: 0;
    display: flex;
    flex-wrap: wrap;
    justify-content: end;
    gap: var(--sl-spacing-2x-small);
    padding: 0 var(--header-spacing);
  }

  .dialog__header-actions sl-icon-button,
  .dialog__header-actions ::slotted(sl-icon-button) {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    font-size: var(--sl-font-size-medium);
  }

  .dialog__body {
    flex: 1 1 auto;
    display: block;
    padding: var(--body-spacing);
    overflow: auto;
    -webkit-overflow-scrolling: touch;
  }

  .dialog__footer {
    flex: 0 0 auto;
    text-align: right;
    padding: var(--footer-spacing);
  }

  .dialog__footer ::slotted(sl-button:not(:first-of-type)) {
    margin-inline-start: var(--sl-spacing-x-small);
  }

  .dialog:not(.dialog--has-footer) .dialog__footer {
    display: none;
  }

  .dialog__overlay {
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    background-color: var(--sl-overlay-background-color);
  }

  @media (forced-colors: active) {
    .dialog__panel {
      border: solid 1px var(--sl-color-neutral-0);
    }
  }
`,
  Jr = class extends nt {
    constructor() {
      super(...arguments),
        (this.hasSlotController = new He(this, 'footer')),
        (this.localize = new Dt(this)),
        (this.modal = new Sf(this)),
        (this.open = !1),
        (this.label = ''),
        (this.noHeader = !1),
        (this.handleDocumentKeyDown = t => {
          t.key === 'Escape' &&
            this.modal.isActive() &&
            this.open &&
            (t.stopPropagation(), this.requestClose('keyboard'))
        })
    }
    firstUpdated() {
      ;(this.dialog.hidden = !this.open),
        this.open && (this.addOpenListeners(), this.modal.activate(), ko(this))
    }
    disconnectedCallback() {
      var t
      super.disconnectedCallback(),
        this.modal.deactivate(),
        $o(this),
        (t = this.closeWatcher) == null || t.destroy()
    }
    requestClose(t) {
      if (
        this.emit('sl-request-close', {
          cancelable: !0,
          detail: { source: t },
        }).defaultPrevented
      ) {
        const i = Xt(this, 'dialog.denyClose', { dir: this.localize.dir() })
        ie(this.panel, i.keyframes, i.options)
        return
      }
      this.hide()
    }
    addOpenListeners() {
      var t
      'CloseWatcher' in window
        ? ((t = this.closeWatcher) == null || t.destroy(),
          (this.closeWatcher = new CloseWatcher()),
          (this.closeWatcher.onclose = () => this.requestClose('keyboard')))
        : document.addEventListener('keydown', this.handleDocumentKeyDown)
    }
    removeOpenListeners() {
      var t
      ;(t = this.closeWatcher) == null || t.destroy(),
        document.removeEventListener('keydown', this.handleDocumentKeyDown)
    }
    async handleOpenChange() {
      if (this.open) {
        this.emit('sl-show'),
          this.addOpenListeners(),
          (this.originalTrigger = document.activeElement),
          this.modal.activate(),
          ko(this)
        const t = this.querySelector('[autofocus]')
        t && t.removeAttribute('autofocus'),
          await Promise.all([pe(this.dialog), pe(this.overlay)]),
          (this.dialog.hidden = !1),
          requestAnimationFrame(() => {
            this.emit('sl-initial-focus', { cancelable: !0 })
              .defaultPrevented ||
              (t
                ? t.focus({ preventScroll: !0 })
                : this.panel.focus({ preventScroll: !0 })),
              t && t.setAttribute('autofocus', '')
          })
        const r = Xt(this, 'dialog.show', { dir: this.localize.dir() }),
          i = Xt(this, 'dialog.overlay.show', { dir: this.localize.dir() })
        await Promise.all([
          ie(this.panel, r.keyframes, r.options),
          ie(this.overlay, i.keyframes, i.options),
        ]),
          this.emit('sl-after-show')
      } else {
        this.emit('sl-hide'),
          this.removeOpenListeners(),
          this.modal.deactivate(),
          await Promise.all([pe(this.dialog), pe(this.overlay)])
        const t = Xt(this, 'dialog.hide', { dir: this.localize.dir() }),
          r = Xt(this, 'dialog.overlay.hide', { dir: this.localize.dir() })
        await Promise.all([
          ie(this.overlay, r.keyframes, r.options).then(() => {
            this.overlay.hidden = !0
          }),
          ie(this.panel, t.keyframes, t.options).then(() => {
            this.panel.hidden = !0
          }),
        ]),
          (this.dialog.hidden = !0),
          (this.overlay.hidden = !1),
          (this.panel.hidden = !1),
          $o(this)
        const i = this.originalTrigger
        typeof (i == null ? void 0 : i.focus) == 'function' &&
          setTimeout(() => i.focus()),
          this.emit('sl-after-hide')
      }
    }
    /** Shows the dialog. */
    async show() {
      if (!this.open) return (this.open = !0), Ue(this, 'sl-after-show')
    }
    /** Hides the dialog */
    async hide() {
      if (this.open) return (this.open = !1), Ue(this, 'sl-after-hide')
    }
    render() {
      return C`
      <div
        part="base"
        class=${ct({
          dialog: !0,
          'dialog--open': this.open,
          'dialog--has-footer': this.hasSlotController.test('footer'),
        })}
      >
        <div part="overlay" class="dialog__overlay" @click=${() =>
          this.requestClose('overlay')} tabindex="-1"></div>

        <div
          part="panel"
          class="dialog__panel"
          role="dialog"
          aria-modal="true"
          aria-hidden=${this.open ? 'false' : 'true'}
          aria-label=${it(this.noHeader ? this.label : void 0)}
          aria-labelledby=${it(this.noHeader ? void 0 : 'title')}
          tabindex="-1"
        >
          ${
            this.noHeader
              ? ''
              : C`
                <header part="header" class="dialog__header">
                  <h2 part="title" class="dialog__title" id="title">
                    <slot name="label"> ${
                      this.label.length > 0 ? this.label : '\uFEFF'
                    } </slot>
                  </h2>
                  <div part="header-actions" class="dialog__header-actions">
                    <slot name="header-actions"></slot>
                    <sl-icon-button
                      part="close-button"
                      exportparts="base:close-button__base"
                      class="dialog__close"
                      name="x-lg"
                      label=${this.localize.term('close')}
                      library="system"
                      @click="${() => this.requestClose('close-button')}"
                    ></sl-icon-button>
                  </div>
                </header>
              `
          }
          ${''}
          <div part="body" class="dialog__body" tabindex="-1"><slot></slot></div>

          <footer part="footer" class="dialog__footer">
            <slot name="footer"></slot>
          </footer>
        </div>
      </div>
    `
    }
  }
Jr.styles = [dt, rS]
Jr.dependencies = {
  'sl-icon-button': ye,
}
c([J('.dialog')], Jr.prototype, 'dialog', 2)
c([J('.dialog__panel')], Jr.prototype, 'panel', 2)
c([J('.dialog__overlay')], Jr.prototype, 'overlay', 2)
c([p({ type: Boolean, reflect: !0 })], Jr.prototype, 'open', 2)
c([p({ reflect: !0 })], Jr.prototype, 'label', 2)
c(
  [p({ attribute: 'no-header', type: Boolean, reflect: !0 })],
  Jr.prototype,
  'noHeader',
  2,
)
c(
  [X('open', { waitUntilFirstUpdate: !0 })],
  Jr.prototype,
  'handleOpenChange',
  1,
)
Mt('dialog.show', {
  keyframes: [
    { opacity: 0, scale: 0.8 },
    { opacity: 1, scale: 1 },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('dialog.hide', {
  keyframes: [
    { opacity: 1, scale: 1 },
    { opacity: 0, scale: 0.8 },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('dialog.denyClose', {
  keyframes: [{ scale: 1 }, { scale: 1.02 }, { scale: 1 }],
  options: { duration: 250 },
})
Mt('dialog.overlay.show', {
  keyframes: [{ opacity: 0 }, { opacity: 1 }],
  options: { duration: 250 },
})
Mt('dialog.overlay.hide', {
  keyframes: [{ opacity: 1 }, { opacity: 0 }],
  options: { duration: 250 },
})
Jr.define('sl-dialog')
_e.define('sl-checkbox')
var iS = st`
  :host {
    --grid-width: 280px;
    --grid-height: 200px;
    --grid-handle-size: 16px;
    --slider-height: 15px;
    --slider-handle-size: 17px;
    --swatch-size: 25px;

    display: inline-block;
  }

  .color-picker {
    width: var(--grid-width);
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-medium);
    font-weight: var(--sl-font-weight-normal);
    color: var(--color);
    background-color: var(--sl-panel-background-color);
    border-radius: var(--sl-border-radius-medium);
    user-select: none;
    -webkit-user-select: none;
  }

  .color-picker--inline {
    border: solid var(--sl-panel-border-width) var(--sl-panel-border-color);
  }

  .color-picker--inline:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .color-picker__grid {
    position: relative;
    height: var(--grid-height);
    background-image: linear-gradient(to bottom, rgba(0, 0, 0, 0) 0%, rgba(0, 0, 0, 1) 100%),
      linear-gradient(to right, #fff 0%, rgba(255, 255, 255, 0) 100%);
    border-top-left-radius: var(--sl-border-radius-medium);
    border-top-right-radius: var(--sl-border-radius-medium);
    cursor: crosshair;
    forced-color-adjust: none;
  }

  .color-picker__grid-handle {
    position: absolute;
    width: var(--grid-handle-size);
    height: var(--grid-handle-size);
    border-radius: 50%;
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.25);
    border: solid 2px white;
    margin-top: calc(var(--grid-handle-size) / -2);
    margin-left: calc(var(--grid-handle-size) / -2);
    transition: var(--sl-transition-fast) scale;
  }

  .color-picker__grid-handle--dragging {
    cursor: none;
    scale: 1.5;
  }

  .color-picker__grid-handle:focus-visible {
    outline: var(--sl-focus-ring);
  }

  .color-picker__controls {
    padding: var(--sl-spacing-small);
    display: flex;
    align-items: center;
  }

  .color-picker__sliders {
    flex: 1 1 auto;
  }

  .color-picker__slider {
    position: relative;
    height: var(--slider-height);
    border-radius: var(--sl-border-radius-pill);
    box-shadow: inset 0 0 0 1px rgba(0, 0, 0, 0.2);
    forced-color-adjust: none;
  }

  .color-picker__slider:not(:last-of-type) {
    margin-bottom: var(--sl-spacing-small);
  }

  .color-picker__slider-handle {
    position: absolute;
    top: calc(50% - var(--slider-handle-size) / 2);
    width: var(--slider-handle-size);
    height: var(--slider-handle-size);
    background-color: white;
    border-radius: 50%;
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.25);
    margin-left: calc(var(--slider-handle-size) / -2);
  }

  .color-picker__slider-handle:focus-visible {
    outline: var(--sl-focus-ring);
  }

  .color-picker__hue {
    background-image: linear-gradient(
      to right,
      rgb(255, 0, 0) 0%,
      rgb(255, 255, 0) 17%,
      rgb(0, 255, 0) 33%,
      rgb(0, 255, 255) 50%,
      rgb(0, 0, 255) 67%,
      rgb(255, 0, 255) 83%,
      rgb(255, 0, 0) 100%
    );
  }

  .color-picker__alpha .color-picker__alpha-gradient {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    border-radius: inherit;
  }

  .color-picker__preview {
    flex: 0 0 auto;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    position: relative;
    width: 2.25rem;
    height: 2.25rem;
    border: none;
    border-radius: var(--sl-border-radius-circle);
    background: none;
    margin-left: var(--sl-spacing-small);
    cursor: copy;
    forced-color-adjust: none;
  }

  .color-picker__preview:before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    border-radius: inherit;
    box-shadow: inset 0 0 0 1px rgba(0, 0, 0, 0.2);

    /* We use a custom property in lieu of currentColor because of https://bugs.webkit.org/show_bug.cgi?id=216780 */
    background-color: var(--preview-color);
  }

  .color-picker__preview:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .color-picker__preview-color {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    border: solid 1px rgba(0, 0, 0, 0.125);
  }

  .color-picker__preview-color--copied {
    animation: pulse 0.75s;
  }

  @keyframes pulse {
    0% {
      box-shadow: 0 0 0 0 var(--sl-color-primary-500);
    }
    70% {
      box-shadow: 0 0 0 0.5rem transparent;
    }
    100% {
      box-shadow: 0 0 0 0 transparent;
    }
  }

  .color-picker__user-input {
    display: flex;
    padding: 0 var(--sl-spacing-small) var(--sl-spacing-small) var(--sl-spacing-small);
  }

  .color-picker__user-input sl-input {
    min-width: 0; /* fix input width in Safari */
    flex: 1 1 auto;
  }

  .color-picker__user-input sl-button-group {
    margin-left: var(--sl-spacing-small);
  }

  .color-picker__user-input sl-button {
    min-width: 3.25rem;
    max-width: 3.25rem;
    font-size: 1rem;
  }

  .color-picker__swatches {
    display: grid;
    grid-template-columns: repeat(8, 1fr);
    grid-gap: 0.5rem;
    justify-items: center;
    border-top: solid 1px var(--sl-color-neutral-200);
    padding: var(--sl-spacing-small);
    forced-color-adjust: none;
  }

  .color-picker__swatch {
    position: relative;
    width: var(--swatch-size);
    height: var(--swatch-size);
    border-radius: var(--sl-border-radius-small);
  }

  .color-picker__swatch .color-picker__swatch-color {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    border: solid 1px rgba(0, 0, 0, 0.125);
    border-radius: inherit;
    cursor: pointer;
  }

  .color-picker__swatch:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .color-picker__transparent-bg {
    background-image: linear-gradient(45deg, var(--sl-color-neutral-300) 25%, transparent 25%),
      linear-gradient(45deg, transparent 75%, var(--sl-color-neutral-300) 75%),
      linear-gradient(45deg, transparent 75%, var(--sl-color-neutral-300) 75%),
      linear-gradient(45deg, var(--sl-color-neutral-300) 25%, transparent 25%);
    background-size: 10px 10px;
    background-position:
      0 0,
      0 0,
      -5px -5px,
      5px 5px;
  }

  .color-picker--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .color-picker--disabled .color-picker__grid,
  .color-picker--disabled .color-picker__grid-handle,
  .color-picker--disabled .color-picker__slider,
  .color-picker--disabled .color-picker__slider-handle,
  .color-picker--disabled .color-picker__preview,
  .color-picker--disabled .color-picker__swatch,
  .color-picker--disabled .color-picker__swatch-color {
    pointer-events: none;
  }

  /*
   * Color dropdown
   */

  .color-dropdown::part(panel) {
    max-height: none;
    background-color: var(--sl-panel-background-color);
    border: solid var(--sl-panel-border-width) var(--sl-panel-border-color);
    border-radius: var(--sl-border-radius-medium);
    overflow: visible;
  }

  .color-dropdown__trigger {
    display: inline-block;
    position: relative;
    background-color: transparent;
    border: none;
    cursor: pointer;
    forced-color-adjust: none;
  }

  .color-dropdown__trigger.color-dropdown__trigger--small {
    width: var(--sl-input-height-small);
    height: var(--sl-input-height-small);
    border-radius: var(--sl-border-radius-circle);
  }

  .color-dropdown__trigger.color-dropdown__trigger--medium {
    width: var(--sl-input-height-medium);
    height: var(--sl-input-height-medium);
    border-radius: var(--sl-border-radius-circle);
  }

  .color-dropdown__trigger.color-dropdown__trigger--large {
    width: var(--sl-input-height-large);
    height: var(--sl-input-height-large);
    border-radius: var(--sl-border-radius-circle);
  }

  .color-dropdown__trigger:before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    border-radius: inherit;
    background-color: currentColor;
    box-shadow:
      inset 0 0 0 2px var(--sl-input-border-color),
      inset 0 0 0 4px var(--sl-color-neutral-0);
  }

  .color-dropdown__trigger--empty:before {
    background-color: transparent;
  }

  .color-dropdown__trigger:focus-visible {
    outline: none;
  }

  .color-dropdown__trigger:focus-visible:not(.color-dropdown__trigger--disabled) {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .color-dropdown__trigger.color-dropdown__trigger--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`,
  Ht = class extends nt {
    constructor() {
      super(...arguments),
        (this.formControlController = new mi(this, {
          assumeInteractionOn: ['click'],
        })),
        (this.hasSlotController = new He(
          this,
          '[default]',
          'prefix',
          'suffix',
        )),
        (this.localize = new Dt(this)),
        (this.hasFocus = !1),
        (this.invalid = !1),
        (this.title = ''),
        (this.variant = 'default'),
        (this.size = 'medium'),
        (this.caret = !1),
        (this.disabled = !1),
        (this.loading = !1),
        (this.outline = !1),
        (this.pill = !1),
        (this.circle = !1),
        (this.type = 'button'),
        (this.name = ''),
        (this.value = ''),
        (this.href = ''),
        (this.rel = 'noreferrer noopener')
    }
    /** Gets the validity state object */
    get validity() {
      return this.isButton() ? this.button.validity : da
    }
    /** Gets the validation message */
    get validationMessage() {
      return this.isButton() ? this.button.validationMessage : ''
    }
    firstUpdated() {
      this.isButton() && this.formControlController.updateValidity()
    }
    handleBlur() {
      ;(this.hasFocus = !1), this.emit('sl-blur')
    }
    handleFocus() {
      ;(this.hasFocus = !0), this.emit('sl-focus')
    }
    handleClick() {
      this.type === 'submit' && this.formControlController.submit(this),
        this.type === 'reset' && this.formControlController.reset(this)
    }
    handleInvalid(t) {
      this.formControlController.setValidity(!1),
        this.formControlController.emitInvalidEvent(t)
    }
    isButton() {
      return !this.href
    }
    isLink() {
      return !!this.href
    }
    handleDisabledChange() {
      this.isButton() && this.formControlController.setValidity(this.disabled)
    }
    /** Simulates a click on the button. */
    click() {
      this.button.click()
    }
    /** Sets focus on the button. */
    focus(t) {
      this.button.focus(t)
    }
    /** Removes focus from the button. */
    blur() {
      this.button.blur()
    }
    /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
    checkValidity() {
      return this.isButton() ? this.button.checkValidity() : !0
    }
    /** Gets the associated form, if one exists. */
    getForm() {
      return this.formControlController.getForm()
    }
    /** Checks for validity and shows the browser's validation message if the control is invalid. */
    reportValidity() {
      return this.isButton() ? this.button.reportValidity() : !0
    }
    /** Sets a custom validation message. Pass an empty string to restore validity. */
    setCustomValidity(t) {
      this.isButton() &&
        (this.button.setCustomValidity(t),
        this.formControlController.updateValidity())
    }
    render() {
      const t = this.isLink(),
        r = t ? Qs`a` : Qs`button`
      return xo`
      <${r}
        part="base"
        class=${ct({
          button: !0,
          'button--default': this.variant === 'default',
          'button--primary': this.variant === 'primary',
          'button--success': this.variant === 'success',
          'button--neutral': this.variant === 'neutral',
          'button--warning': this.variant === 'warning',
          'button--danger': this.variant === 'danger',
          'button--text': this.variant === 'text',
          'button--small': this.size === 'small',
          'button--medium': this.size === 'medium',
          'button--large': this.size === 'large',
          'button--caret': this.caret,
          'button--circle': this.circle,
          'button--disabled': this.disabled,
          'button--focused': this.hasFocus,
          'button--loading': this.loading,
          'button--standard': !this.outline,
          'button--outline': this.outline,
          'button--pill': this.pill,
          'button--rtl': this.localize.dir() === 'rtl',
          'button--has-label': this.hasSlotController.test('[default]'),
          'button--has-prefix': this.hasSlotController.test('prefix'),
          'button--has-suffix': this.hasSlotController.test('suffix'),
        })}
        ?disabled=${it(t ? void 0 : this.disabled)}
        type=${it(t ? void 0 : this.type)}
        title=${this.title}
        name=${it(t ? void 0 : this.name)}
        value=${it(t ? void 0 : this.value)}
        href=${it(t && !this.disabled ? this.href : void 0)}
        target=${it(t ? this.target : void 0)}
        download=${it(t ? this.download : void 0)}
        rel=${it(t ? this.rel : void 0)}
        role=${it(t ? void 0 : 'button')}
        aria-disabled=${this.disabled ? 'true' : 'false'}
        tabindex=${this.disabled ? '-1' : '0'}
        @blur=${this.handleBlur}
        @focus=${this.handleFocus}
        @invalid=${this.isButton() ? this.handleInvalid : null}
        @click=${this.handleClick}
      >
        <slot name="prefix" part="prefix" class="button__prefix"></slot>
        <slot part="label" class="button__label"></slot>
        <slot name="suffix" part="suffix" class="button__suffix"></slot>
        ${
          this.caret
            ? xo` <sl-icon part="caret" class="button__caret" library="system" name="caret"></sl-icon> `
            : ''
        }
        ${this.loading ? xo`<sl-spinner part="spinner"></sl-spinner>` : ''}
      </${r}>
    `
    }
  }
Ht.styles = [dt, _f]
Ht.dependencies = {
  'sl-icon': Nt,
  'sl-spinner': No,
}
c([J('.button')], Ht.prototype, 'button', 2)
c([at()], Ht.prototype, 'hasFocus', 2)
c([at()], Ht.prototype, 'invalid', 2)
c([p()], Ht.prototype, 'title', 2)
c([p({ reflect: !0 })], Ht.prototype, 'variant', 2)
c([p({ reflect: !0 })], Ht.prototype, 'size', 2)
c([p({ type: Boolean, reflect: !0 })], Ht.prototype, 'caret', 2)
c([p({ type: Boolean, reflect: !0 })], Ht.prototype, 'disabled', 2)
c([p({ type: Boolean, reflect: !0 })], Ht.prototype, 'loading', 2)
c([p({ type: Boolean, reflect: !0 })], Ht.prototype, 'outline', 2)
c([p({ type: Boolean, reflect: !0 })], Ht.prototype, 'pill', 2)
c([p({ type: Boolean, reflect: !0 })], Ht.prototype, 'circle', 2)
c([p()], Ht.prototype, 'type', 2)
c([p()], Ht.prototype, 'name', 2)
c([p()], Ht.prototype, 'value', 2)
c([p()], Ht.prototype, 'href', 2)
c([p()], Ht.prototype, 'target', 2)
c([p()], Ht.prototype, 'rel', 2)
c([p()], Ht.prototype, 'download', 2)
c([p()], Ht.prototype, 'form', 2)
c([p({ attribute: 'formaction' })], Ht.prototype, 'formAction', 2)
c([p({ attribute: 'formenctype' })], Ht.prototype, 'formEnctype', 2)
c([p({ attribute: 'formmethod' })], Ht.prototype, 'formMethod', 2)
c(
  [p({ attribute: 'formnovalidate', type: Boolean })],
  Ht.prototype,
  'formNoValidate',
  2,
)
c([p({ attribute: 'formtarget' })], Ht.prototype, 'formTarget', 2)
c(
  [X('disabled', { waitUntilFirstUpdate: !0 })],
  Ht.prototype,
  'handleDisabledChange',
  1,
)
function ze(t, r) {
  nS(t) && (t = '100%')
  const i = oS(t)
  return (
    (t = r === 360 ? t : Math.min(r, Math.max(0, parseFloat(t)))),
    i && (t = parseInt(String(t * r), 10) / 100),
    Math.abs(t - r) < 1e-6
      ? 1
      : (r === 360
          ? (t = (t < 0 ? (t % r) + r : t % r) / parseFloat(String(r)))
          : (t = (t % r) / parseFloat(String(r))),
        t)
  )
}
function Ps(t) {
  return Math.min(1, Math.max(0, t))
}
function nS(t) {
  return typeof t == 'string' && t.indexOf('.') !== -1 && parseFloat(t) === 1
}
function oS(t) {
  return typeof t == 'string' && t.indexOf('%') !== -1
}
function zf(t) {
  return (t = parseFloat(t)), (isNaN(t) || t < 0 || t > 1) && (t = 1), t
}
function Bs(t) {
  return Number(t) <= 1 ? `${Number(t) * 100}%` : t
}
function Zi(t) {
  return t.length === 1 ? '0' + t : String(t)
}
function sS(t, r, i) {
  return {
    r: ze(t, 255) * 255,
    g: ze(r, 255) * 255,
    b: ze(i, 255) * 255,
  }
}
function kp(t, r, i) {
  ;(t = ze(t, 255)), (r = ze(r, 255)), (i = ze(i, 255))
  const o = Math.max(t, r, i),
    a = Math.min(t, r, i)
  let d = 0,
    h = 0
  const m = (o + a) / 2
  if (o === a) (h = 0), (d = 0)
  else {
    const f = o - a
    switch (((h = m > 0.5 ? f / (2 - o - a) : f / (o + a)), o)) {
      case t:
        d = (r - i) / f + (r < i ? 6 : 0)
        break
      case r:
        d = (i - t) / f + 2
        break
      case i:
        d = (t - r) / f + 4
        break
    }
    d /= 6
  }
  return { h: d, s: h, l: m }
}
function jl(t, r, i) {
  return (
    i < 0 && (i += 1),
    i > 1 && (i -= 1),
    i < 1 / 6
      ? t + (r - t) * (6 * i)
      : i < 1 / 2
      ? r
      : i < 2 / 3
      ? t + (r - t) * (2 / 3 - i) * 6
      : t
  )
}
function aS(t, r, i) {
  let o, a, d
  if (((t = ze(t, 360)), (r = ze(r, 100)), (i = ze(i, 100)), r === 0))
    (a = i), (d = i), (o = i)
  else {
    const h = i < 0.5 ? i * (1 + r) : i + r - i * r,
      m = 2 * i - h
    ;(o = jl(m, h, t + 1 / 3)), (a = jl(m, h, t)), (d = jl(m, h, t - 1 / 3))
  }
  return { r: o * 255, g: a * 255, b: d * 255 }
}
function $p(t, r, i) {
  ;(t = ze(t, 255)), (r = ze(r, 255)), (i = ze(i, 255))
  const o = Math.max(t, r, i),
    a = Math.min(t, r, i)
  let d = 0
  const h = o,
    m = o - a,
    f = o === 0 ? 0 : m / o
  if (o === a) d = 0
  else {
    switch (o) {
      case t:
        d = (r - i) / m + (r < i ? 6 : 0)
        break
      case r:
        d = (i - t) / m + 2
        break
      case i:
        d = (t - r) / m + 4
        break
    }
    d /= 6
  }
  return { h: d, s: f, v: h }
}
function lS(t, r, i) {
  ;(t = ze(t, 360) * 6), (r = ze(r, 100)), (i = ze(i, 100))
  const o = Math.floor(t),
    a = t - o,
    d = i * (1 - r),
    h = i * (1 - a * r),
    m = i * (1 - (1 - a) * r),
    f = o % 6,
    b = [i, h, d, d, m, i][f],
    A = [m, i, i, h, d, d][f],
    y = [d, d, m, i, i, h][f]
  return { r: b * 255, g: A * 255, b: y * 255 }
}
function Cp(t, r, i, o) {
  const a = [
    Zi(Math.round(t).toString(16)),
    Zi(Math.round(r).toString(16)),
    Zi(Math.round(i).toString(16)),
  ]
  return o &&
    a[0].startsWith(a[0].charAt(1)) &&
    a[1].startsWith(a[1].charAt(1)) &&
    a[2].startsWith(a[2].charAt(1))
    ? a[0].charAt(0) + a[1].charAt(0) + a[2].charAt(0)
    : a.join('')
}
function cS(t, r, i, o, a) {
  const d = [
    Zi(Math.round(t).toString(16)),
    Zi(Math.round(r).toString(16)),
    Zi(Math.round(i).toString(16)),
    Zi(dS(o)),
  ]
  return a &&
    d[0].startsWith(d[0].charAt(1)) &&
    d[1].startsWith(d[1].charAt(1)) &&
    d[2].startsWith(d[2].charAt(1)) &&
    d[3].startsWith(d[3].charAt(1))
    ? d[0].charAt(0) + d[1].charAt(0) + d[2].charAt(0) + d[3].charAt(0)
    : d.join('')
}
function dS(t) {
  return Math.round(parseFloat(t) * 255).toString(16)
}
function Sp(t) {
  return sr(t) / 255
}
function sr(t) {
  return parseInt(t, 16)
}
function hS(t) {
  return {
    r: t >> 16,
    g: (t & 65280) >> 8,
    b: t & 255,
  }
}
const dc = {
  aliceblue: '#f0f8ff',
  antiquewhite: '#faebd7',
  aqua: '#00ffff',
  aquamarine: '#7fffd4',
  azure: '#f0ffff',
  beige: '#f5f5dc',
  bisque: '#ffe4c4',
  black: '#000000',
  blanchedalmond: '#ffebcd',
  blue: '#0000ff',
  blueviolet: '#8a2be2',
  brown: '#a52a2a',
  burlywood: '#deb887',
  cadetblue: '#5f9ea0',
  chartreuse: '#7fff00',
  chocolate: '#d2691e',
  coral: '#ff7f50',
  cornflowerblue: '#6495ed',
  cornsilk: '#fff8dc',
  crimson: '#dc143c',
  cyan: '#00ffff',
  darkblue: '#00008b',
  darkcyan: '#008b8b',
  darkgoldenrod: '#b8860b',
  darkgray: '#a9a9a9',
  darkgreen: '#006400',
  darkgrey: '#a9a9a9',
  darkkhaki: '#bdb76b',
  darkmagenta: '#8b008b',
  darkolivegreen: '#556b2f',
  darkorange: '#ff8c00',
  darkorchid: '#9932cc',
  darkred: '#8b0000',
  darksalmon: '#e9967a',
  darkseagreen: '#8fbc8f',
  darkslateblue: '#483d8b',
  darkslategray: '#2f4f4f',
  darkslategrey: '#2f4f4f',
  darkturquoise: '#00ced1',
  darkviolet: '#9400d3',
  deeppink: '#ff1493',
  deepskyblue: '#00bfff',
  dimgray: '#696969',
  dimgrey: '#696969',
  dodgerblue: '#1e90ff',
  firebrick: '#b22222',
  floralwhite: '#fffaf0',
  forestgreen: '#228b22',
  fuchsia: '#ff00ff',
  gainsboro: '#dcdcdc',
  ghostwhite: '#f8f8ff',
  goldenrod: '#daa520',
  gold: '#ffd700',
  gray: '#808080',
  green: '#008000',
  greenyellow: '#adff2f',
  grey: '#808080',
  honeydew: '#f0fff0',
  hotpink: '#ff69b4',
  indianred: '#cd5c5c',
  indigo: '#4b0082',
  ivory: '#fffff0',
  khaki: '#f0e68c',
  lavenderblush: '#fff0f5',
  lavender: '#e6e6fa',
  lawngreen: '#7cfc00',
  lemonchiffon: '#fffacd',
  lightblue: '#add8e6',
  lightcoral: '#f08080',
  lightcyan: '#e0ffff',
  lightgoldenrodyellow: '#fafad2',
  lightgray: '#d3d3d3',
  lightgreen: '#90ee90',
  lightgrey: '#d3d3d3',
  lightpink: '#ffb6c1',
  lightsalmon: '#ffa07a',
  lightseagreen: '#20b2aa',
  lightskyblue: '#87cefa',
  lightslategray: '#778899',
  lightslategrey: '#778899',
  lightsteelblue: '#b0c4de',
  lightyellow: '#ffffe0',
  lime: '#00ff00',
  limegreen: '#32cd32',
  linen: '#faf0e6',
  magenta: '#ff00ff',
  maroon: '#800000',
  mediumaquamarine: '#66cdaa',
  mediumblue: '#0000cd',
  mediumorchid: '#ba55d3',
  mediumpurple: '#9370db',
  mediumseagreen: '#3cb371',
  mediumslateblue: '#7b68ee',
  mediumspringgreen: '#00fa9a',
  mediumturquoise: '#48d1cc',
  mediumvioletred: '#c71585',
  midnightblue: '#191970',
  mintcream: '#f5fffa',
  mistyrose: '#ffe4e1',
  moccasin: '#ffe4b5',
  navajowhite: '#ffdead',
  navy: '#000080',
  oldlace: '#fdf5e6',
  olive: '#808000',
  olivedrab: '#6b8e23',
  orange: '#ffa500',
  orangered: '#ff4500',
  orchid: '#da70d6',
  palegoldenrod: '#eee8aa',
  palegreen: '#98fb98',
  paleturquoise: '#afeeee',
  palevioletred: '#db7093',
  papayawhip: '#ffefd5',
  peachpuff: '#ffdab9',
  peru: '#cd853f',
  pink: '#ffc0cb',
  plum: '#dda0dd',
  powderblue: '#b0e0e6',
  purple: '#800080',
  rebeccapurple: '#663399',
  red: '#ff0000',
  rosybrown: '#bc8f8f',
  royalblue: '#4169e1',
  saddlebrown: '#8b4513',
  salmon: '#fa8072',
  sandybrown: '#f4a460',
  seagreen: '#2e8b57',
  seashell: '#fff5ee',
  sienna: '#a0522d',
  silver: '#c0c0c0',
  skyblue: '#87ceeb',
  slateblue: '#6a5acd',
  slategray: '#708090',
  slategrey: '#708090',
  snow: '#fffafa',
  springgreen: '#00ff7f',
  steelblue: '#4682b4',
  tan: '#d2b48c',
  teal: '#008080',
  thistle: '#d8bfd8',
  tomato: '#ff6347',
  turquoise: '#40e0d0',
  violet: '#ee82ee',
  wheat: '#f5deb3',
  white: '#ffffff',
  whitesmoke: '#f5f5f5',
  yellow: '#ffff00',
  yellowgreen: '#9acd32',
}
function uS(t) {
  let r = { r: 0, g: 0, b: 0 },
    i = 1,
    o = null,
    a = null,
    d = null,
    h = !1,
    m = !1
  return (
    typeof t == 'string' && (t = gS(t)),
    typeof t == 'object' &&
      (hi(t.r) && hi(t.g) && hi(t.b)
        ? ((r = sS(t.r, t.g, t.b)),
          (h = !0),
          (m = String(t.r).substr(-1) === '%' ? 'prgb' : 'rgb'))
        : hi(t.h) && hi(t.s) && hi(t.v)
        ? ((o = Bs(t.s)),
          (a = Bs(t.v)),
          (r = lS(t.h, o, a)),
          (h = !0),
          (m = 'hsv'))
        : hi(t.h) &&
          hi(t.s) &&
          hi(t.l) &&
          ((o = Bs(t.s)),
          (d = Bs(t.l)),
          (r = aS(t.h, o, d)),
          (h = !0),
          (m = 'hsl')),
      Object.prototype.hasOwnProperty.call(t, 'a') && (i = t.a)),
    (i = zf(i)),
    {
      ok: h,
      format: t.format || m,
      r: Math.min(255, Math.max(r.r, 0)),
      g: Math.min(255, Math.max(r.g, 0)),
      b: Math.min(255, Math.max(r.b, 0)),
      a: i,
    }
  )
}
const pS = '[-\\+]?\\d+%?',
  fS = '[-\\+]?\\d*\\.\\d+%?',
  Ei = `(?:${fS})|(?:${pS})`,
  Zl = `[\\s|\\(]+(${Ei})[,|\\s]+(${Ei})[,|\\s]+(${Ei})\\s*\\)?`,
  Jl = `[\\s|\\(]+(${Ei})[,|\\s]+(${Ei})[,|\\s]+(${Ei})[,|\\s]+(${Ei})\\s*\\)?`,
  Lr = {
    CSS_UNIT: new RegExp(Ei),
    rgb: new RegExp('rgb' + Zl),
    rgba: new RegExp('rgba' + Jl),
    hsl: new RegExp('hsl' + Zl),
    hsla: new RegExp('hsla' + Jl),
    hsv: new RegExp('hsv' + Zl),
    hsva: new RegExp('hsva' + Jl),
    hex3: /^#?([0-9a-fA-F]{1})([0-9a-fA-F]{1})([0-9a-fA-F]{1})$/,
    hex6: /^#?([0-9a-fA-F]{2})([0-9a-fA-F]{2})([0-9a-fA-F]{2})$/,
    hex4: /^#?([0-9a-fA-F]{1})([0-9a-fA-F]{1})([0-9a-fA-F]{1})([0-9a-fA-F]{1})$/,
    hex8: /^#?([0-9a-fA-F]{2})([0-9a-fA-F]{2})([0-9a-fA-F]{2})([0-9a-fA-F]{2})$/,
  }
function gS(t) {
  if (((t = t.trim().toLowerCase()), t.length === 0)) return !1
  let r = !1
  if (dc[t]) (t = dc[t]), (r = !0)
  else if (t === 'transparent')
    return { r: 0, g: 0, b: 0, a: 0, format: 'name' }
  let i = Lr.rgb.exec(t)
  return i
    ? { r: i[1], g: i[2], b: i[3] }
    : ((i = Lr.rgba.exec(t)),
      i
        ? { r: i[1], g: i[2], b: i[3], a: i[4] }
        : ((i = Lr.hsl.exec(t)),
          i
            ? { h: i[1], s: i[2], l: i[3] }
            : ((i = Lr.hsla.exec(t)),
              i
                ? { h: i[1], s: i[2], l: i[3], a: i[4] }
                : ((i = Lr.hsv.exec(t)),
                  i
                    ? { h: i[1], s: i[2], v: i[3] }
                    : ((i = Lr.hsva.exec(t)),
                      i
                        ? { h: i[1], s: i[2], v: i[3], a: i[4] }
                        : ((i = Lr.hex8.exec(t)),
                          i
                            ? {
                                r: sr(i[1]),
                                g: sr(i[2]),
                                b: sr(i[3]),
                                a: Sp(i[4]),
                                format: r ? 'name' : 'hex8',
                              }
                            : ((i = Lr.hex6.exec(t)),
                              i
                                ? {
                                    r: sr(i[1]),
                                    g: sr(i[2]),
                                    b: sr(i[3]),
                                    format: r ? 'name' : 'hex',
                                  }
                                : ((i = Lr.hex4.exec(t)),
                                  i
                                    ? {
                                        r: sr(i[1] + i[1]),
                                        g: sr(i[2] + i[2]),
                                        b: sr(i[3] + i[3]),
                                        a: Sp(i[4] + i[4]),
                                        format: r ? 'name' : 'hex8',
                                      }
                                    : ((i = Lr.hex3.exec(t)),
                                      i
                                        ? {
                                            r: sr(i[1] + i[1]),
                                            g: sr(i[2] + i[2]),
                                            b: sr(i[3] + i[3]),
                                            format: r ? 'name' : 'hex',
                                          }
                                        : !1)))))))))
}
function hi(t) {
  return !!Lr.CSS_UNIT.exec(String(t))
}
class re {
  constructor(r = '', i = {}) {
    if (r instanceof re) return r
    typeof r == 'number' && (r = hS(r)), (this.originalInput = r)
    const o = uS(r)
    ;(this.originalInput = r),
      (this.r = o.r),
      (this.g = o.g),
      (this.b = o.b),
      (this.a = o.a),
      (this.roundA = Math.round(100 * this.a) / 100),
      (this.format = i.format ?? o.format),
      (this.gradientType = i.gradientType),
      this.r < 1 && (this.r = Math.round(this.r)),
      this.g < 1 && (this.g = Math.round(this.g)),
      this.b < 1 && (this.b = Math.round(this.b)),
      (this.isValid = o.ok)
  }
  isDark() {
    return this.getBrightness() < 128
  }
  isLight() {
    return !this.isDark()
  }
  /**
   * Returns the perceived brightness of the color, from 0-255.
   */
  getBrightness() {
    const r = this.toRgb()
    return (r.r * 299 + r.g * 587 + r.b * 114) / 1e3
  }
  /**
   * Returns the perceived luminance of a color, from 0-1.
   */
  getLuminance() {
    const r = this.toRgb()
    let i, o, a
    const d = r.r / 255,
      h = r.g / 255,
      m = r.b / 255
    return (
      d <= 0.03928 ? (i = d / 12.92) : (i = Math.pow((d + 0.055) / 1.055, 2.4)),
      h <= 0.03928 ? (o = h / 12.92) : (o = Math.pow((h + 0.055) / 1.055, 2.4)),
      m <= 0.03928 ? (a = m / 12.92) : (a = Math.pow((m + 0.055) / 1.055, 2.4)),
      0.2126 * i + 0.7152 * o + 0.0722 * a
    )
  }
  /**
   * Returns the alpha value of a color, from 0-1.
   */
  getAlpha() {
    return this.a
  }
  /**
   * Sets the alpha value on the current color.
   *
   * @param alpha - The new alpha value. The accepted range is 0-1.
   */
  setAlpha(r) {
    return (
      (this.a = zf(r)), (this.roundA = Math.round(100 * this.a) / 100), this
    )
  }
  /**
   * Returns whether the color is monochrome.
   */
  isMonochrome() {
    const { s: r } = this.toHsl()
    return r === 0
  }
  /**
   * Returns the object as a HSVA object.
   */
  toHsv() {
    const r = $p(this.r, this.g, this.b)
    return { h: r.h * 360, s: r.s, v: r.v, a: this.a }
  }
  /**
   * Returns the hsva values interpolated into a string with the following format:
   * "hsva(xxx, xxx, xxx, xx)".
   */
  toHsvString() {
    const r = $p(this.r, this.g, this.b),
      i = Math.round(r.h * 360),
      o = Math.round(r.s * 100),
      a = Math.round(r.v * 100)
    return this.a === 1
      ? `hsv(${i}, ${o}%, ${a}%)`
      : `hsva(${i}, ${o}%, ${a}%, ${this.roundA})`
  }
  /**
   * Returns the object as a HSLA object.
   */
  toHsl() {
    const r = kp(this.r, this.g, this.b)
    return { h: r.h * 360, s: r.s, l: r.l, a: this.a }
  }
  /**
   * Returns the hsla values interpolated into a string with the following format:
   * "hsla(xxx, xxx, xxx, xx)".
   */
  toHslString() {
    const r = kp(this.r, this.g, this.b),
      i = Math.round(r.h * 360),
      o = Math.round(r.s * 100),
      a = Math.round(r.l * 100)
    return this.a === 1
      ? `hsl(${i}, ${o}%, ${a}%)`
      : `hsla(${i}, ${o}%, ${a}%, ${this.roundA})`
  }
  /**
   * Returns the hex value of the color.
   * @param allow3Char will shorten hex value to 3 char if possible
   */
  toHex(r = !1) {
    return Cp(this.r, this.g, this.b, r)
  }
  /**
   * Returns the hex value of the color -with a # prefixed.
   * @param allow3Char will shorten hex value to 3 char if possible
   */
  toHexString(r = !1) {
    return '#' + this.toHex(r)
  }
  /**
   * Returns the hex 8 value of the color.
   * @param allow4Char will shorten hex value to 4 char if possible
   */
  toHex8(r = !1) {
    return cS(this.r, this.g, this.b, this.a, r)
  }
  /**
   * Returns the hex 8 value of the color -with a # prefixed.
   * @param allow4Char will shorten hex value to 4 char if possible
   */
  toHex8String(r = !1) {
    return '#' + this.toHex8(r)
  }
  /**
   * Returns the shorter hex value of the color depends on its alpha -with a # prefixed.
   * @param allowShortChar will shorten hex value to 3 or 4 char if possible
   */
  toHexShortString(r = !1) {
    return this.a === 1 ? this.toHexString(r) : this.toHex8String(r)
  }
  /**
   * Returns the object as a RGBA object.
   */
  toRgb() {
    return {
      r: Math.round(this.r),
      g: Math.round(this.g),
      b: Math.round(this.b),
      a: this.a,
    }
  }
  /**
   * Returns the RGBA values interpolated into a string with the following format:
   * "RGBA(xxx, xxx, xxx, xx)".
   */
  toRgbString() {
    const r = Math.round(this.r),
      i = Math.round(this.g),
      o = Math.round(this.b)
    return this.a === 1
      ? `rgb(${r}, ${i}, ${o})`
      : `rgba(${r}, ${i}, ${o}, ${this.roundA})`
  }
  /**
   * Returns the object as a RGBA object.
   */
  toPercentageRgb() {
    const r = i => `${Math.round(ze(i, 255) * 100)}%`
    return {
      r: r(this.r),
      g: r(this.g),
      b: r(this.b),
      a: this.a,
    }
  }
  /**
   * Returns the RGBA relative values interpolated into a string
   */
  toPercentageRgbString() {
    const r = i => Math.round(ze(i, 255) * 100)
    return this.a === 1
      ? `rgb(${r(this.r)}%, ${r(this.g)}%, ${r(this.b)}%)`
      : `rgba(${r(this.r)}%, ${r(this.g)}%, ${r(this.b)}%, ${this.roundA})`
  }
  /**
   * The 'real' name of the color -if there is one.
   */
  toName() {
    if (this.a === 0) return 'transparent'
    if (this.a < 1) return !1
    const r = '#' + Cp(this.r, this.g, this.b, !1)
    for (const [i, o] of Object.entries(dc)) if (r === o) return i
    return !1
  }
  toString(r) {
    const i = !!r
    r = r ?? this.format
    let o = !1
    const a = this.a < 1 && this.a >= 0
    return !i && a && (r.startsWith('hex') || r === 'name')
      ? r === 'name' && this.a === 0
        ? this.toName()
        : this.toRgbString()
      : (r === 'rgb' && (o = this.toRgbString()),
        r === 'prgb' && (o = this.toPercentageRgbString()),
        (r === 'hex' || r === 'hex6') && (o = this.toHexString()),
        r === 'hex3' && (o = this.toHexString(!0)),
        r === 'hex4' && (o = this.toHex8String(!0)),
        r === 'hex8' && (o = this.toHex8String()),
        r === 'name' && (o = this.toName()),
        r === 'hsl' && (o = this.toHslString()),
        r === 'hsv' && (o = this.toHsvString()),
        o || this.toHexString())
  }
  toNumber() {
    return (
      (Math.round(this.r) << 16) +
      (Math.round(this.g) << 8) +
      Math.round(this.b)
    )
  }
  clone() {
    return new re(this.toString())
  }
  /**
   * Lighten the color a given amount. Providing 100 will always return white.
   * @param amount - valid between 1-100
   */
  lighten(r = 10) {
    const i = this.toHsl()
    return (i.l += r / 100), (i.l = Ps(i.l)), new re(i)
  }
  /**
   * Brighten the color a given amount, from 0 to 100.
   * @param amount - valid between 1-100
   */
  brighten(r = 10) {
    const i = this.toRgb()
    return (
      (i.r = Math.max(0, Math.min(255, i.r - Math.round(255 * -(r / 100))))),
      (i.g = Math.max(0, Math.min(255, i.g - Math.round(255 * -(r / 100))))),
      (i.b = Math.max(0, Math.min(255, i.b - Math.round(255 * -(r / 100))))),
      new re(i)
    )
  }
  /**
   * Darken the color a given amount, from 0 to 100.
   * Providing 100 will always return black.
   * @param amount - valid between 1-100
   */
  darken(r = 10) {
    const i = this.toHsl()
    return (i.l -= r / 100), (i.l = Ps(i.l)), new re(i)
  }
  /**
   * Mix the color with pure white, from 0 to 100.
   * Providing 0 will do nothing, providing 100 will always return white.
   * @param amount - valid between 1-100
   */
  tint(r = 10) {
    return this.mix('white', r)
  }
  /**
   * Mix the color with pure black, from 0 to 100.
   * Providing 0 will do nothing, providing 100 will always return black.
   * @param amount - valid between 1-100
   */
  shade(r = 10) {
    return this.mix('black', r)
  }
  /**
   * Desaturate the color a given amount, from 0 to 100.
   * Providing 100 will is the same as calling greyscale
   * @param amount - valid between 1-100
   */
  desaturate(r = 10) {
    const i = this.toHsl()
    return (i.s -= r / 100), (i.s = Ps(i.s)), new re(i)
  }
  /**
   * Saturate the color a given amount, from 0 to 100.
   * @param amount - valid between 1-100
   */
  saturate(r = 10) {
    const i = this.toHsl()
    return (i.s += r / 100), (i.s = Ps(i.s)), new re(i)
  }
  /**
   * Completely desaturates a color into greyscale.
   * Same as calling `desaturate(100)`
   */
  greyscale() {
    return this.desaturate(100)
  }
  /**
   * Spin takes a positive or negative amount within [-360, 360] indicating the change of hue.
   * Values outside of this range will be wrapped into this range.
   */
  spin(r) {
    const i = this.toHsl(),
      o = (i.h + r) % 360
    return (i.h = o < 0 ? 360 + o : o), new re(i)
  }
  /**
   * Mix the current color a given amount with another color, from 0 to 100.
   * 0 means no mixing (return current color).
   */
  mix(r, i = 50) {
    const o = this.toRgb(),
      a = new re(r).toRgb(),
      d = i / 100,
      h = {
        r: (a.r - o.r) * d + o.r,
        g: (a.g - o.g) * d + o.g,
        b: (a.b - o.b) * d + o.b,
        a: (a.a - o.a) * d + o.a,
      }
    return new re(h)
  }
  analogous(r = 6, i = 30) {
    const o = this.toHsl(),
      a = 360 / i,
      d = [this]
    for (o.h = (o.h - ((a * r) >> 1) + 720) % 360; --r; )
      (o.h = (o.h + a) % 360), d.push(new re(o))
    return d
  }
  /**
   * taken from https://github.com/infusion/jQuery-xcolor/blob/master/jquery.xcolor.js
   */
  complement() {
    const r = this.toHsl()
    return (r.h = (r.h + 180) % 360), new re(r)
  }
  monochromatic(r = 6) {
    const i = this.toHsv(),
      { h: o } = i,
      { s: a } = i
    let { v: d } = i
    const h = [],
      m = 1 / r
    for (; r--; ) h.push(new re({ h: o, s: a, v: d })), (d = (d + m) % 1)
    return h
  }
  splitcomplement() {
    const r = this.toHsl(),
      { h: i } = r
    return [
      this,
      new re({ h: (i + 72) % 360, s: r.s, l: r.l }),
      new re({ h: (i + 216) % 360, s: r.s, l: r.l }),
    ]
  }
  /**
   * Compute how the color would appear on a background
   */
  onBackground(r) {
    const i = this.toRgb(),
      o = new re(r).toRgb(),
      a = i.a + o.a * (1 - i.a)
    return new re({
      r: (i.r * i.a + o.r * o.a * (1 - i.a)) / a,
      g: (i.g * i.a + o.g * o.a * (1 - i.a)) / a,
      b: (i.b * i.a + o.b * o.a * (1 - i.a)) / a,
      a,
    })
  }
  /**
   * Alias for `polyad(3)`
   */
  triad() {
    return this.polyad(3)
  }
  /**
   * Alias for `polyad(4)`
   */
  tetrad() {
    return this.polyad(4)
  }
  /**
   * Get polyad colors, like (for 1, 2, 3, 4, 5, 6, 7, 8, etc...)
   * monad, dyad, triad, tetrad, pentad, hexad, heptad, octad, etc...
   */
  polyad(r) {
    const i = this.toHsl(),
      { h: o } = i,
      a = [this],
      d = 360 / r
    for (let h = 1; h < r; h++)
      a.push(new re({ h: (o + h * d) % 360, s: i.s, l: i.l }))
    return a
  }
  /**
   * compare color vs current color
   */
  equals(r) {
    return this.toRgbString() === new re(r).toRgbString()
  }
}
var zp = 'EyeDropper' in window,
  Tt = class extends nt {
    constructor() {
      super(),
        (this.formControlController = new mi(this)),
        (this.isSafeValue = !1),
        (this.localize = new Dt(this)),
        (this.hasFocus = !1),
        (this.isDraggingGridHandle = !1),
        (this.isEmpty = !1),
        (this.inputValue = ''),
        (this.hue = 0),
        (this.saturation = 100),
        (this.brightness = 100),
        (this.alpha = 100),
        (this.value = ''),
        (this.defaultValue = ''),
        (this.label = ''),
        (this.format = 'hex'),
        (this.inline = !1),
        (this.size = 'medium'),
        (this.noFormatToggle = !1),
        (this.name = ''),
        (this.disabled = !1),
        (this.hoist = !1),
        (this.opacity = !1),
        (this.uppercase = !1),
        (this.swatches = ''),
        (this.form = ''),
        (this.required = !1),
        (this.handleFocusIn = () => {
          ;(this.hasFocus = !0), this.emit('sl-focus')
        }),
        (this.handleFocusOut = () => {
          ;(this.hasFocus = !1), this.emit('sl-blur')
        }),
        this.addEventListener('focusin', this.handleFocusIn),
        this.addEventListener('focusout', this.handleFocusOut)
    }
    /** Gets the validity state object */
    get validity() {
      return this.input.validity
    }
    /** Gets the validation message */
    get validationMessage() {
      return this.input.validationMessage
    }
    firstUpdated() {
      this.input.updateComplete.then(() => {
        this.formControlController.updateValidity()
      })
    }
    handleCopy() {
      this.input.select(),
        document.execCommand('copy'),
        this.previewButton.focus(),
        this.previewButton.classList.add('color-picker__preview-color--copied'),
        this.previewButton.addEventListener('animationend', () => {
          this.previewButton.classList.remove(
            'color-picker__preview-color--copied',
          )
        })
    }
    handleFormatToggle() {
      const t = ['hex', 'rgb', 'hsl', 'hsv'],
        r = (t.indexOf(this.format) + 1) % t.length
      ;(this.format = t[r]),
        this.setColor(this.value),
        this.emit('sl-change'),
        this.emit('sl-input')
    }
    handleAlphaDrag(t) {
      const r = this.shadowRoot.querySelector(
          '.color-picker__slider.color-picker__alpha',
        ),
        i = r.querySelector('.color-picker__slider-handle'),
        { width: o } = r.getBoundingClientRect()
      let a = this.value,
        d = this.value
      i.focus(),
        t.preventDefault(),
        wo(r, {
          onMove: h => {
            ;(this.alpha = ue((h / o) * 100, 0, 100)),
              this.syncValues(),
              this.value !== d && ((d = this.value), this.emit('sl-input'))
          },
          onStop: () => {
            this.value !== a && ((a = this.value), this.emit('sl-change'))
          },
          initialEvent: t,
        })
    }
    handleHueDrag(t) {
      const r = this.shadowRoot.querySelector(
          '.color-picker__slider.color-picker__hue',
        ),
        i = r.querySelector('.color-picker__slider-handle'),
        { width: o } = r.getBoundingClientRect()
      let a = this.value,
        d = this.value
      i.focus(),
        t.preventDefault(),
        wo(r, {
          onMove: h => {
            ;(this.hue = ue((h / o) * 360, 0, 360)),
              this.syncValues(),
              this.value !== d && ((d = this.value), this.emit('sl-input'))
          },
          onStop: () => {
            this.value !== a && ((a = this.value), this.emit('sl-change'))
          },
          initialEvent: t,
        })
    }
    handleGridDrag(t) {
      const r = this.shadowRoot.querySelector('.color-picker__grid'),
        i = r.querySelector('.color-picker__grid-handle'),
        { width: o, height: a } = r.getBoundingClientRect()
      let d = this.value,
        h = this.value
      i.focus(),
        t.preventDefault(),
        (this.isDraggingGridHandle = !0),
        wo(r, {
          onMove: (m, f) => {
            ;(this.saturation = ue((m / o) * 100, 0, 100)),
              (this.brightness = ue(100 - (f / a) * 100, 0, 100)),
              this.syncValues(),
              this.value !== h && ((h = this.value), this.emit('sl-input'))
          },
          onStop: () => {
            ;(this.isDraggingGridHandle = !1),
              this.value !== d && ((d = this.value), this.emit('sl-change'))
          },
          initialEvent: t,
        })
    }
    handleAlphaKeyDown(t) {
      const r = t.shiftKey ? 10 : 1,
        i = this.value
      t.key === 'ArrowLeft' &&
        (t.preventDefault(),
        (this.alpha = ue(this.alpha - r, 0, 100)),
        this.syncValues()),
        t.key === 'ArrowRight' &&
          (t.preventDefault(),
          (this.alpha = ue(this.alpha + r, 0, 100)),
          this.syncValues()),
        t.key === 'Home' &&
          (t.preventDefault(), (this.alpha = 0), this.syncValues()),
        t.key === 'End' &&
          (t.preventDefault(), (this.alpha = 100), this.syncValues()),
        this.value !== i && (this.emit('sl-change'), this.emit('sl-input'))
    }
    handleHueKeyDown(t) {
      const r = t.shiftKey ? 10 : 1,
        i = this.value
      t.key === 'ArrowLeft' &&
        (t.preventDefault(),
        (this.hue = ue(this.hue - r, 0, 360)),
        this.syncValues()),
        t.key === 'ArrowRight' &&
          (t.preventDefault(),
          (this.hue = ue(this.hue + r, 0, 360)),
          this.syncValues()),
        t.key === 'Home' &&
          (t.preventDefault(), (this.hue = 0), this.syncValues()),
        t.key === 'End' &&
          (t.preventDefault(), (this.hue = 360), this.syncValues()),
        this.value !== i && (this.emit('sl-change'), this.emit('sl-input'))
    }
    handleGridKeyDown(t) {
      const r = t.shiftKey ? 10 : 1,
        i = this.value
      t.key === 'ArrowLeft' &&
        (t.preventDefault(),
        (this.saturation = ue(this.saturation - r, 0, 100)),
        this.syncValues()),
        t.key === 'ArrowRight' &&
          (t.preventDefault(),
          (this.saturation = ue(this.saturation + r, 0, 100)),
          this.syncValues()),
        t.key === 'ArrowUp' &&
          (t.preventDefault(),
          (this.brightness = ue(this.brightness + r, 0, 100)),
          this.syncValues()),
        t.key === 'ArrowDown' &&
          (t.preventDefault(),
          (this.brightness = ue(this.brightness - r, 0, 100)),
          this.syncValues()),
        this.value !== i && (this.emit('sl-change'), this.emit('sl-input'))
    }
    handleInputChange(t) {
      const r = t.target,
        i = this.value
      t.stopPropagation(),
        this.input.value
          ? (this.setColor(r.value), (r.value = this.value))
          : (this.value = ''),
        this.value !== i && (this.emit('sl-change'), this.emit('sl-input'))
    }
    handleInputInput(t) {
      this.formControlController.updateValidity(), t.stopPropagation()
    }
    handleInputKeyDown(t) {
      if (t.key === 'Enter') {
        const r = this.value
        this.input.value
          ? (this.setColor(this.input.value),
            (this.input.value = this.value),
            this.value !== r && (this.emit('sl-change'), this.emit('sl-input')),
            setTimeout(() => this.input.select()))
          : (this.hue = 0)
      }
    }
    handleInputInvalid(t) {
      this.formControlController.setValidity(!1),
        this.formControlController.emitInvalidEvent(t)
    }
    handleTouchMove(t) {
      t.preventDefault()
    }
    parseColor(t) {
      const r = new re(t)
      if (!r.isValid) return null
      const i = r.toHsl(),
        o = {
          h: i.h,
          s: i.s * 100,
          l: i.l * 100,
          a: i.a,
        },
        a = r.toRgb(),
        d = r.toHexString(),
        h = r.toHex8String(),
        m = r.toHsv(),
        f = {
          h: m.h,
          s: m.s * 100,
          v: m.v * 100,
          a: m.a,
        }
      return {
        hsl: {
          h: o.h,
          s: o.s,
          l: o.l,
          string: this.setLetterCase(
            `hsl(${Math.round(o.h)}, ${Math.round(o.s)}%, ${Math.round(o.l)}%)`,
          ),
        },
        hsla: {
          h: o.h,
          s: o.s,
          l: o.l,
          a: o.a,
          string: this.setLetterCase(
            `hsla(${Math.round(o.h)}, ${Math.round(o.s)}%, ${Math.round(
              o.l,
            )}%, ${o.a.toFixed(2).toString()})`,
          ),
        },
        hsv: {
          h: f.h,
          s: f.s,
          v: f.v,
          string: this.setLetterCase(
            `hsv(${Math.round(f.h)}, ${Math.round(f.s)}%, ${Math.round(f.v)}%)`,
          ),
        },
        hsva: {
          h: f.h,
          s: f.s,
          v: f.v,
          a: f.a,
          string: this.setLetterCase(
            `hsva(${Math.round(f.h)}, ${Math.round(f.s)}%, ${Math.round(
              f.v,
            )}%, ${f.a.toFixed(2).toString()})`,
          ),
        },
        rgb: {
          r: a.r,
          g: a.g,
          b: a.b,
          string: this.setLetterCase(
            `rgb(${Math.round(a.r)}, ${Math.round(a.g)}, ${Math.round(a.b)})`,
          ),
        },
        rgba: {
          r: a.r,
          g: a.g,
          b: a.b,
          a: a.a,
          string: this.setLetterCase(
            `rgba(${Math.round(a.r)}, ${Math.round(a.g)}, ${Math.round(
              a.b,
            )}, ${a.a.toFixed(2).toString()})`,
          ),
        },
        hex: this.setLetterCase(d),
        hexa: this.setLetterCase(h),
      }
    }
    setColor(t) {
      const r = this.parseColor(t)
      return r === null
        ? !1
        : ((this.hue = r.hsva.h),
          (this.saturation = r.hsva.s),
          (this.brightness = r.hsva.v),
          (this.alpha = this.opacity ? r.hsva.a * 100 : 100),
          this.syncValues(),
          !0)
    }
    setLetterCase(t) {
      return typeof t != 'string'
        ? ''
        : this.uppercase
        ? t.toUpperCase()
        : t.toLowerCase()
    }
    async syncValues() {
      const t = this.parseColor(
        `hsva(${this.hue}, ${this.saturation}%, ${this.brightness}%, ${
          this.alpha / 100
        })`,
      )
      t !== null &&
        (this.format === 'hsl'
          ? (this.inputValue = this.opacity ? t.hsla.string : t.hsl.string)
          : this.format === 'rgb'
          ? (this.inputValue = this.opacity ? t.rgba.string : t.rgb.string)
          : this.format === 'hsv'
          ? (this.inputValue = this.opacity ? t.hsva.string : t.hsv.string)
          : (this.inputValue = this.opacity ? t.hexa : t.hex),
        (this.isSafeValue = !0),
        (this.value = this.inputValue),
        await this.updateComplete,
        (this.isSafeValue = !1))
    }
    handleAfterHide() {
      this.previewButton.classList.remove('color-picker__preview-color--copied')
    }
    handleEyeDropper() {
      if (!zp) return
      new EyeDropper()
        .open()
        .then(r => {
          const i = this.value
          this.setColor(r.sRGBHex),
            this.value !== i && (this.emit('sl-change'), this.emit('sl-input'))
        })
        .catch(() => {})
    }
    selectSwatch(t) {
      const r = this.value
      this.disabled ||
        (this.setColor(t),
        this.value !== r && (this.emit('sl-change'), this.emit('sl-input')))
    }
    /** Generates a hex string from HSV values. Hue must be 0-360. All other arguments must be 0-100. */
    getHexString(t, r, i, o = 100) {
      const a = new re(`hsva(${t}, ${r}%, ${i}%, ${o / 100})`)
      return a.isValid ? a.toHex8String() : ''
    }
    // Prevents nested components from leaking events
    stopNestedEventPropagation(t) {
      t.stopImmediatePropagation()
    }
    handleFormatChange() {
      this.syncValues()
    }
    handleOpacityChange() {
      this.alpha = 100
    }
    handleValueChange(t, r) {
      if (
        ((this.isEmpty = !r),
        r ||
          ((this.hue = 0),
          (this.saturation = 0),
          (this.brightness = 100),
          (this.alpha = 100)),
        !this.isSafeValue)
      ) {
        const i = this.parseColor(r)
        i !== null
          ? ((this.inputValue = this.value),
            (this.hue = i.hsva.h),
            (this.saturation = i.hsva.s),
            (this.brightness = i.hsva.v),
            (this.alpha = i.hsva.a * 100),
            this.syncValues())
          : (this.inputValue = t ?? '')
      }
    }
    /** Sets focus on the color picker. */
    focus(t) {
      this.inline ? this.base.focus(t) : this.trigger.focus(t)
    }
    /** Removes focus from the color picker. */
    blur() {
      var t
      const r = this.inline ? this.base : this.trigger
      this.hasFocus && (r.focus({ preventScroll: !0 }), r.blur()),
        (t = this.dropdown) != null && t.open && this.dropdown.hide()
    }
    /** Returns the current value as a string in the specified format. */
    getFormattedValue(t = 'hex') {
      const r = this.parseColor(
        `hsva(${this.hue}, ${this.saturation}%, ${this.brightness}%, ${
          this.alpha / 100
        })`,
      )
      if (r === null) return ''
      switch (t) {
        case 'hex':
          return r.hex
        case 'hexa':
          return r.hexa
        case 'rgb':
          return r.rgb.string
        case 'rgba':
          return r.rgba.string
        case 'hsl':
          return r.hsl.string
        case 'hsla':
          return r.hsla.string
        case 'hsv':
          return r.hsv.string
        case 'hsva':
          return r.hsva.string
        default:
          return ''
      }
    }
    /** Checks for validity but does not show a validation message. Returns `true` when valid and `false` when invalid. */
    checkValidity() {
      return this.input.checkValidity()
    }
    /** Gets the associated form, if one exists. */
    getForm() {
      return this.formControlController.getForm()
    }
    /** Checks for validity and shows the browser's validation message if the control is invalid. */
    reportValidity() {
      return !this.inline && !this.validity.valid
        ? (this.dropdown.show(),
          this.addEventListener(
            'sl-after-show',
            () => this.input.reportValidity(),
            { once: !0 },
          ),
          this.disabled || this.formControlController.emitInvalidEvent(),
          !1)
        : this.input.reportValidity()
    }
    /** Sets a custom validation message. Pass an empty string to restore validity. */
    setCustomValidity(t) {
      this.input.setCustomValidity(t),
        this.formControlController.updateValidity()
    }
    render() {
      const t = this.saturation,
        r = 100 - this.brightness,
        i = Array.isArray(this.swatches)
          ? this.swatches
          : this.swatches.split(';').filter(a => a.trim() !== ''),
        o = C`
      <div
        part="base"
        class=${ct({
          'color-picker': !0,
          'color-picker--inline': this.inline,
          'color-picker--disabled': this.disabled,
          'color-picker--focused': this.hasFocus,
        })}
        aria-disabled=${this.disabled ? 'true' : 'false'}
        aria-labelledby="label"
        tabindex=${this.inline ? '0' : '-1'}
      >
        ${
          this.inline
            ? C`
              <sl-visually-hidden id="label">
                <slot name="label">${this.label}</slot>
              </sl-visually-hidden>
            `
            : null
        }

        <div
          part="grid"
          class="color-picker__grid"
          style=${Ke({
            backgroundColor: this.getHexString(this.hue, 100, 100),
          })}
          @pointerdown=${this.handleGridDrag}
          @touchmove=${this.handleTouchMove}
        >
          <span
            part="grid-handle"
            class=${ct({
              'color-picker__grid-handle': !0,
              'color-picker__grid-handle--dragging': this.isDraggingGridHandle,
            })}
            style=${Ke({
              top: `${r}%`,
              left: `${t}%`,
              backgroundColor: this.getHexString(
                this.hue,
                this.saturation,
                this.brightness,
                this.alpha,
              ),
            })}
            role="application"
            aria-label="HSV"
            tabindex=${it(this.disabled ? void 0 : '0')}
            @keydown=${this.handleGridKeyDown}
          ></span>
        </div>

        <div class="color-picker__controls">
          <div class="color-picker__sliders">
            <div
              part="slider hue-slider"
              class="color-picker__hue color-picker__slider"
              @pointerdown=${this.handleHueDrag}
              @touchmove=${this.handleTouchMove}
            >
              <span
                part="slider-handle hue-slider-handle"
                class="color-picker__slider-handle"
                style=${Ke({
                  left: `${this.hue === 0 ? 0 : 100 / (360 / this.hue)}%`,
                })}
                role="slider"
                aria-label="hue"
                aria-orientation="horizontal"
                aria-valuemin="0"
                aria-valuemax="360"
                aria-valuenow=${`${Math.round(this.hue)}`}
                tabindex=${it(this.disabled ? void 0 : '0')}
                @keydown=${this.handleHueKeyDown}
              ></span>
            </div>

            ${
              this.opacity
                ? C`
                  <div
                    part="slider opacity-slider"
                    class="color-picker__alpha color-picker__slider color-picker__transparent-bg"
                    @pointerdown="${this.handleAlphaDrag}"
                    @touchmove=${this.handleTouchMove}
                  >
                    <div
                      class="color-picker__alpha-gradient"
                      style=${Ke({
                        backgroundImage: `linear-gradient(
                          to right,
                          ${this.getHexString(
                            this.hue,
                            this.saturation,
                            this.brightness,
                            0,
                          )} 0%,
                          ${this.getHexString(
                            this.hue,
                            this.saturation,
                            this.brightness,
                            100,
                          )} 100%
                        )`,
                      })}
                    ></div>
                    <span
                      part="slider-handle opacity-slider-handle"
                      class="color-picker__slider-handle"
                      style=${Ke({
                        left: `${this.alpha}%`,
                      })}
                      role="slider"
                      aria-label="alpha"
                      aria-orientation="horizontal"
                      aria-valuemin="0"
                      aria-valuemax="100"
                      aria-valuenow=${Math.round(this.alpha)}
                      tabindex=${it(this.disabled ? void 0 : '0')}
                      @keydown=${this.handleAlphaKeyDown}
                    ></span>
                  </div>
                `
                : ''
            }
          </div>

          <button
            type="button"
            part="preview"
            class="color-picker__preview color-picker__transparent-bg"
            aria-label=${this.localize.term('copy')}
            style=${Ke({
              '--preview-color': this.getHexString(
                this.hue,
                this.saturation,
                this.brightness,
                this.alpha,
              ),
            })}
            @click=${this.handleCopy}
          ></button>
        </div>

        <div class="color-picker__user-input" aria-live="polite">
          <sl-input
            part="input"
            type="text"
            name=${this.name}
            autocomplete="off"
            autocorrect="off"
            autocapitalize="off"
            spellcheck="false"
            value=${this.isEmpty ? '' : this.inputValue}
            ?required=${this.required}
            ?disabled=${this.disabled}
            aria-label=${this.localize.term('currentValue')}
            @keydown=${this.handleInputKeyDown}
            @sl-change=${this.handleInputChange}
            @sl-input=${this.handleInputInput}
            @sl-invalid=${this.handleInputInvalid}
            @sl-blur=${this.stopNestedEventPropagation}
            @sl-focus=${this.stopNestedEventPropagation}
          ></sl-input>

          <sl-button-group>
            ${
              this.noFormatToggle
                ? ''
                : C`
                  <sl-button
                    part="format-button"
                    aria-label=${this.localize.term('toggleColorFormat')}
                    exportparts="
                      base:format-button__base,
                      prefix:format-button__prefix,
                      label:format-button__label,
                      suffix:format-button__suffix,
                      caret:format-button__caret
                    "
                    @click=${this.handleFormatToggle}
                    @sl-blur=${this.stopNestedEventPropagation}
                    @sl-focus=${this.stopNestedEventPropagation}
                  >
                    ${this.setLetterCase(this.format)}
                  </sl-button>
                `
            }
            ${
              zp
                ? C`
                  <sl-button
                    part="eye-dropper-button"
                    exportparts="
                      base:eye-dropper-button__base,
                      prefix:eye-dropper-button__prefix,
                      label:eye-dropper-button__label,
                      suffix:eye-dropper-button__suffix,
                      caret:eye-dropper-button__caret
                    "
                    @click=${this.handleEyeDropper}
                    @sl-blur=${this.stopNestedEventPropagation}
                    @sl-focus=${this.stopNestedEventPropagation}
                  >
                    <sl-icon
                      library="system"
                      name="eyedropper"
                      label=${this.localize.term('selectAColorFromTheScreen')}
                    ></sl-icon>
                  </sl-button>
                `
                : ''
            }
          </sl-button-group>
        </div>

        ${
          i.length > 0
            ? C`
              <div part="swatches" class="color-picker__swatches">
                ${i.map(a => {
                  const d = this.parseColor(a)
                  return d
                    ? C`
                    <div
                      part="swatch"
                      class="color-picker__swatch color-picker__transparent-bg"
                      tabindex=${it(this.disabled ? void 0 : '0')}
                      role="button"
                      aria-label=${a}
                      @click=${() => this.selectSwatch(a)}
                      @keydown=${h =>
                        !this.disabled &&
                        h.key === 'Enter' &&
                        this.setColor(d.hexa)}
                    >
                      <div
                        class="color-picker__swatch-color"
                        style=${Ke({ backgroundColor: d.hexa })}
                      ></div>
                    </div>
                  `
                    : (console.error(
                        `Unable to parse swatch color: "${a}"`,
                        this,
                      ),
                      '')
                })}
              </div>
            `
            : ''
        }
      </div>
    `
      return this.inline
        ? o
        : C`
      <sl-dropdown
        class="color-dropdown"
        aria-disabled=${this.disabled ? 'true' : 'false'}
        .containing-element=${this}
        ?disabled=${this.disabled}
        ?hoist=${this.hoist}
        @sl-after-hide=${this.handleAfterHide}
      >
        <button
          part="trigger"
          slot="trigger"
          class=${ct({
            'color-dropdown__trigger': !0,
            'color-dropdown__trigger--disabled': this.disabled,
            'color-dropdown__trigger--small': this.size === 'small',
            'color-dropdown__trigger--medium': this.size === 'medium',
            'color-dropdown__trigger--large': this.size === 'large',
            'color-dropdown__trigger--empty': this.isEmpty,
            'color-dropdown__trigger--focused': this.hasFocus,
            'color-picker__transparent-bg': !0,
          })}
          style=${Ke({
            color: this.getHexString(
              this.hue,
              this.saturation,
              this.brightness,
              this.alpha,
            ),
          })}
          type="button"
        >
          <sl-visually-hidden>
            <slot name="label">${this.label}</slot>
          </sl-visually-hidden>
        </button>
        ${o}
      </sl-dropdown>
    `
    }
  }
Tt.styles = [dt, iS]
Tt.dependencies = {
  'sl-button-group': hn,
  'sl-button': Ht,
  'sl-dropdown': Ee,
  'sl-icon': Nt,
  'sl-input': bt,
  'sl-visually-hidden': Ec,
}
c([J('[part~="base"]')], Tt.prototype, 'base', 2)
c([J('[part~="input"]')], Tt.prototype, 'input', 2)
c([J('.color-dropdown')], Tt.prototype, 'dropdown', 2)
c([J('[part~="preview"]')], Tt.prototype, 'previewButton', 2)
c([J('[part~="trigger"]')], Tt.prototype, 'trigger', 2)
c([at()], Tt.prototype, 'hasFocus', 2)
c([at()], Tt.prototype, 'isDraggingGridHandle', 2)
c([at()], Tt.prototype, 'isEmpty', 2)
c([at()], Tt.prototype, 'inputValue', 2)
c([at()], Tt.prototype, 'hue', 2)
c([at()], Tt.prototype, 'saturation', 2)
c([at()], Tt.prototype, 'brightness', 2)
c([at()], Tt.prototype, 'alpha', 2)
c([p()], Tt.prototype, 'value', 2)
c([an()], Tt.prototype, 'defaultValue', 2)
c([p()], Tt.prototype, 'label', 2)
c([p()], Tt.prototype, 'format', 2)
c([p({ type: Boolean, reflect: !0 })], Tt.prototype, 'inline', 2)
c([p({ reflect: !0 })], Tt.prototype, 'size', 2)
c(
  [p({ attribute: 'no-format-toggle', type: Boolean })],
  Tt.prototype,
  'noFormatToggle',
  2,
)
c([p()], Tt.prototype, 'name', 2)
c([p({ type: Boolean, reflect: !0 })], Tt.prototype, 'disabled', 2)
c([p({ type: Boolean })], Tt.prototype, 'hoist', 2)
c([p({ type: Boolean })], Tt.prototype, 'opacity', 2)
c([p({ type: Boolean })], Tt.prototype, 'uppercase', 2)
c([p()], Tt.prototype, 'swatches', 2)
c([p({ reflect: !0 })], Tt.prototype, 'form', 2)
c([p({ type: Boolean, reflect: !0 })], Tt.prototype, 'required', 2)
c([Do({ passive: !1 })], Tt.prototype, 'handleTouchMove', 1)
c(
  [X('format', { waitUntilFirstUpdate: !0 })],
  Tt.prototype,
  'handleFormatChange',
  1,
)
c(
  [X('opacity', { waitUntilFirstUpdate: !0 })],
  Tt.prototype,
  'handleOpacityChange',
  1,
)
c([X('value')], Tt.prototype, 'handleValueChange', 1)
Tt.define('sl-color-picker')
var mS = st`
  :host {
    --border-color: var(--sl-color-neutral-200);
    --border-radius: var(--sl-border-radius-medium);
    --border-width: 1px;
    --padding: var(--sl-spacing-large);

    display: inline-block;
  }

  .card {
    display: flex;
    flex-direction: column;
    background-color: var(--sl-panel-background-color);
    box-shadow: var(--sl-shadow-x-small);
    border: solid var(--border-width) var(--border-color);
    border-radius: var(--border-radius);
  }

  .card__image {
    display: flex;
    border-top-left-radius: var(--border-radius);
    border-top-right-radius: var(--border-radius);
    margin: calc(-1 * var(--border-width));
    overflow: hidden;
  }

  .card__image::slotted(img) {
    display: block;
    width: 100%;
  }

  .card:not(.card--has-image) .card__image {
    display: none;
  }

  .card__header {
    display: block;
    border-bottom: solid var(--border-width) var(--border-color);
    padding: calc(var(--padding) / 2) var(--padding);
  }

  .card:not(.card--has-header) .card__header {
    display: none;
  }

  .card:not(.card--has-image) .card__header {
    border-top-left-radius: var(--border-radius);
    border-top-right-radius: var(--border-radius);
  }

  .card__body {
    display: block;
    padding: var(--padding);
  }

  .card--has-footer .card__footer {
    display: block;
    border-top: solid var(--border-width) var(--border-color);
    padding: var(--padding);
  }

  .card:not(.card--has-footer) .card__footer {
    display: none;
  }
`,
  Af = class extends nt {
    constructor() {
      super(...arguments),
        (this.hasSlotController = new He(this, 'footer', 'header', 'image'))
    }
    render() {
      return C`
      <div
        part="base"
        class=${ct({
          card: !0,
          'card--has-footer': this.hasSlotController.test('footer'),
          'card--has-image': this.hasSlotController.test('image'),
          'card--has-header': this.hasSlotController.test('header'),
        })}
      >
        <slot name="image" part="image" class="card__image"></slot>
        <slot name="header" part="header" class="card__header"></slot>
        <slot part="body" class="card__body"></slot>
        <slot name="footer" part="footer" class="card__footer"></slot>
      </div>
    `
    }
  }
Af.styles = [dt, mS]
Af.define('sl-card')
var vS = class {
    constructor(t, r) {
      ;(this.timerId = 0),
        (this.activeInteractions = 0),
        (this.paused = !1),
        (this.stopped = !0),
        (this.pause = () => {
          this.activeInteractions++ ||
            ((this.paused = !0), this.host.requestUpdate())
        }),
        (this.resume = () => {
          --this.activeInteractions ||
            ((this.paused = !1), this.host.requestUpdate())
        }),
        t.addController(this),
        (this.host = t),
        (this.tickCallback = r)
    }
    hostConnected() {
      this.host.addEventListener('mouseenter', this.pause),
        this.host.addEventListener('mouseleave', this.resume),
        this.host.addEventListener('focusin', this.pause),
        this.host.addEventListener('focusout', this.resume),
        this.host.addEventListener('touchstart', this.pause, { passive: !0 }),
        this.host.addEventListener('touchend', this.resume)
    }
    hostDisconnected() {
      this.stop(),
        this.host.removeEventListener('mouseenter', this.pause),
        this.host.removeEventListener('mouseleave', this.resume),
        this.host.removeEventListener('focusin', this.pause),
        this.host.removeEventListener('focusout', this.resume),
        this.host.removeEventListener('touchstart', this.pause),
        this.host.removeEventListener('touchend', this.resume)
    }
    start(t) {
      this.stop(),
        (this.stopped = !1),
        (this.timerId = window.setInterval(() => {
          this.paused || this.tickCallback()
        }, t))
    }
    stop() {
      clearInterval(this.timerId),
        (this.stopped = !0),
        this.host.requestUpdate()
    }
  },
  bS = st`
  :host {
    --slide-gap: var(--sl-spacing-medium, 1rem);
    --aspect-ratio: 16 / 9;
    --scroll-hint: 0px;

    display: flex;
  }

  .carousel {
    display: grid;
    grid-template-columns: min-content 1fr min-content;
    grid-template-rows: 1fr min-content;
    grid-template-areas:
      '. slides .'
      '. pagination .';
    gap: var(--sl-spacing-medium);
    align-items: center;
    min-height: 100%;
    min-width: 100%;
    position: relative;
  }

  .carousel__pagination {
    grid-area: pagination;
    display: flex;
    flex-wrap: wrap;
    justify-content: center;
    gap: var(--sl-spacing-small);
  }

  .carousel__slides {
    grid-area: slides;

    display: grid;
    height: 100%;
    width: 100%;
    align-items: center;
    justify-items: center;
    overflow: auto;
    overscroll-behavior-x: contain;
    scrollbar-width: none;
    aspect-ratio: calc(var(--aspect-ratio) * var(--slides-per-page));
    border-radius: var(--sl-border-radius-small);

    --slide-size: calc((100% - (var(--slides-per-page) - 1) * var(--slide-gap)) / var(--slides-per-page));
  }

  @media (prefers-reduced-motion) {
    :where(.carousel__slides) {
      scroll-behavior: auto;
    }
  }

  .carousel__slides--horizontal {
    grid-auto-flow: column;
    grid-auto-columns: var(--slide-size);
    grid-auto-rows: 100%;
    column-gap: var(--slide-gap);
    scroll-snap-type: x mandatory;
    scroll-padding-inline: var(--scroll-hint);
    padding-inline: var(--scroll-hint);
    overflow-y: hidden;
  }

  .carousel__slides--vertical {
    grid-auto-flow: row;
    grid-auto-columns: 100%;
    grid-auto-rows: var(--slide-size);
    row-gap: var(--slide-gap);
    scroll-snap-type: y mandatory;
    scroll-padding-block: var(--scroll-hint);
    padding-block: var(--scroll-hint);
    overflow-x: hidden;
  }

  .carousel__slides--dragging {
  }

  :host([vertical]) ::slotted(sl-carousel-item) {
    height: 100%;
  }

  .carousel__slides::-webkit-scrollbar {
    display: none;
  }

  .carousel__navigation {
    grid-area: navigation;
    display: contents;
    font-size: var(--sl-font-size-x-large);
  }

  .carousel__navigation-button {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    background: none;
    border: none;
    border-radius: var(--sl-border-radius-small);
    font-size: inherit;
    color: var(--sl-color-neutral-600);
    padding: var(--sl-spacing-x-small);
    cursor: pointer;
    transition: var(--sl-transition-medium) color;
    appearance: none;
  }

  .carousel__navigation-button--disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .carousel__navigation-button--disabled::part(base) {
    pointer-events: none;
  }

  .carousel__navigation-button--previous {
    grid-column: 1;
    grid-row: 1;
  }

  .carousel__navigation-button--next {
    grid-column: 3;
    grid-row: 1;
  }

  .carousel__pagination-item {
    display: block;
    cursor: pointer;
    background: none;
    border: 0;
    border-radius: var(--sl-border-radius-circle);
    width: var(--sl-spacing-small);
    height: var(--sl-spacing-small);
    background-color: var(--sl-color-neutral-300);
    padding: 0;
    margin: 0;
  }

  .carousel__pagination-item--active {
    background-color: var(--sl-color-neutral-700);
    transform: scale(1.2);
  }

  /* Focus styles */
  .carousel__slides:focus-visible,
  .carousel__navigation-button:focus-visible,
  .carousel__pagination-item:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }
`
/**
 * @license
 * Copyright 2021 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function* yS(t, r) {
  if (t !== void 0) {
    let i = 0
    for (const o of t) yield r(o, i++)
  }
}
/**
 * @license
 * Copyright 2021 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function* _S(t, r, i = 1) {
  const o = r === void 0 ? 0 : t
  r ?? (r = t)
  for (let a = o; i > 0 ? a < r : r < a; a += i) yield a
}
var oe = class extends nt {
  constructor() {
    super(...arguments),
      (this.loop = !1),
      (this.navigation = !1),
      (this.pagination = !1),
      (this.autoplay = !1),
      (this.autoplayInterval = 3e3),
      (this.slidesPerPage = 1),
      (this.slidesPerMove = 1),
      (this.orientation = 'horizontal'),
      (this.mouseDragging = !1),
      (this.activeSlide = 0),
      (this.scrolling = !1),
      (this.dragging = !1),
      (this.autoplayController = new vS(this, () => this.next())),
      (this.localize = new Dt(this)),
      (this.pendingSlideChange = !1),
      (this.handleMouseDrag = t => {
        this.dragging ||
          (this.scrollContainer.style.setProperty('scroll-snap-type', 'none'),
          (this.dragging = !0)),
          this.scrollContainer.scrollBy({
            left: -t.movementX,
            top: -t.movementY,
            behavior: 'instant',
          })
      }),
      (this.handleMouseDragEnd = () => {
        const t = this.scrollContainer
        document.removeEventListener('pointermove', this.handleMouseDrag, {
          capture: !0,
        })
        const r = t.scrollLeft,
          i = t.scrollTop
        t.style.removeProperty('scroll-snap-type'),
          t.style.setProperty('overflow', 'hidden')
        const o = t.scrollLeft,
          a = t.scrollTop
        t.style.removeProperty('overflow'),
          t.style.setProperty('scroll-snap-type', 'none'),
          t.scrollTo({ left: r, top: i, behavior: 'instant' }),
          requestAnimationFrame(async () => {
            ;(r !== o || i !== a) &&
              (t.scrollTo({
                left: o,
                top: a,
                behavior: nc() ? 'auto' : 'smooth',
              }),
              await Ue(t, 'scrollend')),
              t.style.removeProperty('scroll-snap-type'),
              (this.dragging = !1),
              this.handleScrollEnd()
          })
      }),
      (this.handleSlotChange = t => {
        t.some(i =>
          [...i.addedNodes, ...i.removedNodes].some(
            o => this.isCarouselItem(o) && !o.hasAttribute('data-clone'),
          ),
        ) && this.initializeSlides(),
          this.requestUpdate()
      })
  }
  connectedCallback() {
    super.connectedCallback(),
      this.setAttribute('role', 'region'),
      this.setAttribute('aria-label', this.localize.term('carousel'))
  }
  disconnectedCallback() {
    var t
    super.disconnectedCallback(),
      (t = this.mutationObserver) == null || t.disconnect()
  }
  firstUpdated() {
    this.initializeSlides(),
      (this.mutationObserver = new MutationObserver(this.handleSlotChange)),
      this.mutationObserver.observe(this, {
        childList: !0,
        subtree: !0,
      })
  }
  willUpdate(t) {
    ;(t.has('slidesPerMove') || t.has('slidesPerPage')) &&
      (this.slidesPerMove = Math.min(this.slidesPerMove, this.slidesPerPage))
  }
  getPageCount() {
    const t = this.getSlides().length,
      { slidesPerPage: r, slidesPerMove: i, loop: o } = this,
      a = o ? t / i : (t - r) / i + 1
    return Math.ceil(a)
  }
  getCurrentPage() {
    return Math.ceil(this.activeSlide / this.slidesPerMove)
  }
  canScrollNext() {
    return this.loop || this.getCurrentPage() < this.getPageCount() - 1
  }
  canScrollPrev() {
    return this.loop || this.getCurrentPage() > 0
  }
  /** @internal Gets all carousel items. */
  getSlides({ excludeClones: t = !0 } = {}) {
    return [...this.children].filter(
      r => this.isCarouselItem(r) && (!t || !r.hasAttribute('data-clone')),
    )
  }
  handleKeyDown(t) {
    if (
      [
        'ArrowLeft',
        'ArrowRight',
        'ArrowUp',
        'ArrowDown',
        'Home',
        'End',
      ].includes(t.key)
    ) {
      const r = t.target,
        i = this.localize.dir() === 'rtl',
        o = r.closest('[part~="pagination-item"]') !== null,
        a =
          t.key === 'ArrowDown' ||
          (!i && t.key === 'ArrowRight') ||
          (i && t.key === 'ArrowLeft'),
        d =
          t.key === 'ArrowUp' ||
          (!i && t.key === 'ArrowLeft') ||
          (i && t.key === 'ArrowRight')
      t.preventDefault(),
        d && this.previous(),
        a && this.next(),
        t.key === 'Home' && this.goToSlide(0),
        t.key === 'End' && this.goToSlide(this.getSlides().length - 1),
        o &&
          this.updateComplete.then(() => {
            var h
            const m =
              (h = this.shadowRoot) == null
                ? void 0
                : h.querySelector('[part~="pagination-item--active"]')
            m && m.focus()
          })
    }
  }
  handleMouseDragStart(t) {
    this.mouseDragging &&
      t.button === 0 &&
      (t.preventDefault(),
      document.addEventListener('pointermove', this.handleMouseDrag, {
        capture: !0,
        passive: !0,
      }),
      document.addEventListener('pointerup', this.handleMouseDragEnd, {
        capture: !0,
        once: !0,
      }))
  }
  handleScroll() {
    ;(this.scrolling = !0), this.pendingSlideChange || this.synchronizeSlides()
  }
  /** @internal Synchronizes the slides with the IntersectionObserver API. */
  synchronizeSlides() {
    const t = new IntersectionObserver(
      r => {
        t.disconnect()
        for (const m of r) {
          const f = m.target
          f.toggleAttribute('inert', !m.isIntersecting),
            f.classList.toggle('--in-view', m.isIntersecting),
            f.setAttribute('aria-hidden', m.isIntersecting ? 'false' : 'true')
        }
        const i = r.find(m => m.isIntersecting)
        if (!i) return
        const o = this.getSlides({ excludeClones: !1 }),
          a = this.getSlides().length,
          d = o.indexOf(i.target),
          h = this.loop ? d - this.slidesPerPage : d
        if (
          ((this.activeSlide =
            (Math.ceil(h / this.slidesPerMove) * this.slidesPerMove + a) % a),
          !this.scrolling && this.loop && i.target.hasAttribute('data-clone'))
        ) {
          const m = Number(i.target.getAttribute('data-clone'))
          this.goToSlide(m, 'instant')
        }
      },
      {
        root: this.scrollContainer,
        threshold: 0.6,
      },
    )
    this.getSlides({ excludeClones: !1 }).forEach(r => {
      t.observe(r)
    })
  }
  handleScrollEnd() {
    !this.scrolling ||
      this.dragging ||
      ((this.scrolling = !1),
      (this.pendingSlideChange = !1),
      this.synchronizeSlides())
  }
  isCarouselItem(t) {
    return (
      t instanceof Element && t.tagName.toLowerCase() === 'sl-carousel-item'
    )
  }
  initializeSlides() {
    this.getSlides({ excludeClones: !1 }).forEach((t, r) => {
      t.classList.remove('--in-view'),
        t.classList.remove('--is-active'),
        t.setAttribute('aria-label', this.localize.term('slideNum', r + 1)),
        t.hasAttribute('data-clone') && t.remove()
    }),
      this.updateSlidesSnap(),
      this.loop && this.createClones(),
      this.synchronizeSlides(),
      this.goToSlide(this.activeSlide, 'auto')
  }
  createClones() {
    const t = this.getSlides(),
      r = this.slidesPerPage,
      i = t.slice(-r),
      o = t.slice(0, r)
    i.reverse().forEach((a, d) => {
      const h = a.cloneNode(!0)
      h.setAttribute('data-clone', String(t.length - d - 1)), this.prepend(h)
    }),
      o.forEach((a, d) => {
        const h = a.cloneNode(!0)
        h.setAttribute('data-clone', String(d)), this.append(h)
      })
  }
  handleSlideChange() {
    const t = this.getSlides()
    t.forEach((r, i) => {
      r.classList.toggle('--is-active', i === this.activeSlide)
    }),
      this.hasUpdated &&
        this.emit('sl-slide-change', {
          detail: {
            index: this.activeSlide,
            slide: t[this.activeSlide],
          },
        })
  }
  updateSlidesSnap() {
    const t = this.getSlides(),
      r = this.slidesPerMove
    t.forEach((i, o) => {
      ;(o + r) % r === 0
        ? i.style.removeProperty('scroll-snap-align')
        : i.style.setProperty('scroll-snap-align', 'none')
    })
  }
  handleAutoplayChange() {
    this.autoplayController.stop(),
      this.autoplay && this.autoplayController.start(this.autoplayInterval)
  }
  /**
   * Move the carousel backward by `slides-per-move` slides.
   *
   * @param behavior - The behavior used for scrolling.
   */
  previous(t = 'smooth') {
    this.goToSlide(this.activeSlide - this.slidesPerMove, t)
  }
  /**
   * Move the carousel forward by `slides-per-move` slides.
   *
   * @param behavior - The behavior used for scrolling.
   */
  next(t = 'smooth') {
    this.goToSlide(this.activeSlide + this.slidesPerMove, t)
  }
  /**
   * Scrolls the carousel to the slide specified by `index`.
   *
   * @param index - The slide index.
   * @param behavior - The behavior used for scrolling.
   */
  goToSlide(t, r = 'smooth') {
    const { slidesPerPage: i, loop: o } = this,
      a = this.getSlides(),
      d = this.getSlides({ excludeClones: !1 })
    if (!a.length) return
    const h = o ? (t + a.length) % a.length : ue(t, 0, a.length - i)
    this.activeSlide = h
    const m = this.localize.dir() === 'rtl',
      f = ue(t + (o ? i : 0) + (m ? i - 1 : 0), 0, d.length - 1),
      b = d[f]
    this.scrollToSlide(b, nc() ? 'auto' : r)
  }
  scrollToSlide(t, r = 'smooth') {
    const i = this.scrollContainer,
      o = i.getBoundingClientRect(),
      a = t.getBoundingClientRect(),
      d = a.left - o.left,
      h = a.top - o.top
    ;(d || h) &&
      ((this.pendingSlideChange = !0),
      i.scrollTo({
        left: d + i.scrollLeft,
        top: h + i.scrollTop,
        behavior: r,
      }))
  }
  render() {
    const { slidesPerMove: t, scrolling: r } = this,
      i = this.getPageCount(),
      o = this.getCurrentPage(),
      a = this.canScrollPrev(),
      d = this.canScrollNext(),
      h = this.localize.dir() === 'rtl'
    return C`
      <div part="base" class="carousel">
        <div
          id="scroll-container"
          part="scroll-container"
          class="${ct({
            carousel__slides: !0,
            'carousel__slides--horizontal': this.orientation === 'horizontal',
            'carousel__slides--vertical': this.orientation === 'vertical',
            'carousel__slides--dragging': this.dragging,
          })}"
          style="--slides-per-page: ${this.slidesPerPage};"
          aria-busy="${r ? 'true' : 'false'}"
          aria-atomic="true"
          tabindex="0"
          @keydown=${this.handleKeyDown}
          @mousedown="${this.handleMouseDragStart}"
          @scroll="${this.handleScroll}"
          @scrollend=${this.handleScrollEnd}
        >
          <slot></slot>
        </div>

        ${
          this.navigation
            ? C`
              <div part="navigation" class="carousel__navigation">
                <button
                  part="navigation-button navigation-button--previous"
                  class="${ct({
                    'carousel__navigation-button': !0,
                    'carousel__navigation-button--previous': !0,
                    'carousel__navigation-button--disabled': !a,
                  })}"
                  aria-label="${this.localize.term('previousSlide')}"
                  aria-controls="scroll-container"
                  aria-disabled="${a ? 'false' : 'true'}"
                  @click=${a ? () => this.previous() : null}
                >
                  <slot name="previous-icon">
                    <sl-icon library="system" name="${
                      h ? 'chevron-left' : 'chevron-right'
                    }"></sl-icon>
                  </slot>
                </button>

                <button
                  part="navigation-button navigation-button--next"
                  class=${ct({
                    'carousel__navigation-button': !0,
                    'carousel__navigation-button--next': !0,
                    'carousel__navigation-button--disabled': !d,
                  })}
                  aria-label="${this.localize.term('nextSlide')}"
                  aria-controls="scroll-container"
                  aria-disabled="${d ? 'false' : 'true'}"
                  @click=${d ? () => this.next() : null}
                >
                  <slot name="next-icon">
                    <sl-icon library="system" name="${
                      h ? 'chevron-right' : 'chevron-left'
                    }"></sl-icon>
                  </slot>
                </button>
              </div>
            `
            : ''
        }
        ${
          this.pagination
            ? C`
              <div part="pagination" role="tablist" class="carousel__pagination" aria-controls="scroll-container">
                ${yS(_S(i), m => {
                  const f = m === o
                  return C`
                    <button
                      part="pagination-item ${
                        f ? 'pagination-item--active' : ''
                      }"
                      class="${ct({
                        'carousel__pagination-item': !0,
                        'carousel__pagination-item--active': f,
                      })}"
                      role="tab"
                      aria-selected="${f ? 'true' : 'false'}"
                      aria-label="${this.localize.term('goToSlide', m + 1, i)}"
                      tabindex=${f ? '0' : '-1'}
                      @click=${() => this.goToSlide(m * t)}
                      @keydown=${this.handleKeyDown}
                    ></button>
                  `
                })}
              </div>
            `
            : ''
        }
      </div>
    `
  }
}
oe.styles = [dt, bS]
oe.dependencies = { 'sl-icon': Nt }
c([p({ type: Boolean, reflect: !0 })], oe.prototype, 'loop', 2)
c([p({ type: Boolean, reflect: !0 })], oe.prototype, 'navigation', 2)
c([p({ type: Boolean, reflect: !0 })], oe.prototype, 'pagination', 2)
c([p({ type: Boolean, reflect: !0 })], oe.prototype, 'autoplay', 2)
c(
  [p({ type: Number, attribute: 'autoplay-interval' })],
  oe.prototype,
  'autoplayInterval',
  2,
)
c(
  [p({ type: Number, attribute: 'slides-per-page' })],
  oe.prototype,
  'slidesPerPage',
  2,
)
c(
  [p({ type: Number, attribute: 'slides-per-move' })],
  oe.prototype,
  'slidesPerMove',
  2,
)
c([p()], oe.prototype, 'orientation', 2)
c(
  [p({ type: Boolean, reflect: !0, attribute: 'mouse-dragging' })],
  oe.prototype,
  'mouseDragging',
  2,
)
c([J('.carousel__slides')], oe.prototype, 'scrollContainer', 2)
c([J('.carousel__pagination')], oe.prototype, 'paginationContainer', 2)
c([at()], oe.prototype, 'activeSlide', 2)
c([at()], oe.prototype, 'scrolling', 2)
c([at()], oe.prototype, 'dragging', 2)
c([Do({ passive: !0 })], oe.prototype, 'handleScroll', 1)
c(
  [
    X('loop', { waitUntilFirstUpdate: !0 }),
    X('slidesPerPage', { waitUntilFirstUpdate: !0 }),
  ],
  oe.prototype,
  'initializeSlides',
  1,
)
c([X('activeSlide')], oe.prototype, 'handleSlideChange', 1)
c([X('slidesPerMove')], oe.prototype, 'updateSlidesSnap', 1)
c([X('autoplay')], oe.prototype, 'handleAutoplayChange', 1)
oe.define('sl-carousel')
var wS = st`
  :host {
    --aspect-ratio: inherit;

    display: flex;
    align-items: center;
    justify-content: center;
    flex-direction: column;
    width: 100%;
    max-height: 100%;
    aspect-ratio: var(--aspect-ratio);
    scroll-snap-align: start;
    scroll-snap-stop: always;
  }

  ::slotted(img) {
    width: 100% !important;
    height: 100% !important;
    object-fit: cover;
  }
`,
  Ef = class extends nt {
    connectedCallback() {
      super.connectedCallback(), this.setAttribute('role', 'group')
    }
    render() {
      return C` <slot></slot> `
    }
  }
Ef.styles = [dt, wS]
Ef.define('sl-carousel-item')
Ht.define('sl-button')
hn.define('sl-button-group')
var xS = st`
  :host {
    display: inline-block;

    --size: 3rem;
  }

  .avatar {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    position: relative;
    width: var(--size);
    height: var(--size);
    background-color: var(--sl-color-neutral-400);
    font-family: var(--sl-font-sans);
    font-size: calc(var(--size) * 0.5);
    font-weight: var(--sl-font-weight-normal);
    color: var(--sl-color-neutral-0);
    user-select: none;
    -webkit-user-select: none;
    vertical-align: middle;
  }

  .avatar--circle,
  .avatar--circle .avatar__image {
    border-radius: var(--sl-border-radius-circle);
  }

  .avatar--rounded,
  .avatar--rounded .avatar__image {
    border-radius: var(--sl-border-radius-medium);
  }

  .avatar--square {
    border-radius: 0;
  }

  .avatar__icon {
    display: flex;
    align-items: center;
    justify-content: center;
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
  }

  .avatar__initials {
    line-height: 1;
    text-transform: uppercase;
  }

  .avatar__image {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    object-fit: cover;
    overflow: hidden;
  }
`,
  Qr = class extends nt {
    constructor() {
      super(...arguments),
        (this.hasError = !1),
        (this.image = ''),
        (this.label = ''),
        (this.initials = ''),
        (this.loading = 'eager'),
        (this.shape = 'circle')
    }
    handleImageChange() {
      this.hasError = !1
    }
    handleImageLoadError() {
      ;(this.hasError = !0), this.emit('sl-error')
    }
    render() {
      const t = C`
      <img
        part="image"
        class="avatar__image"
        src="${this.image}"
        loading="${this.loading}"
        alt=""
        @error="${this.handleImageLoadError}"
      />
    `
      let r = C``
      return (
        this.initials
          ? (r = C`<div part="initials" class="avatar__initials">${this.initials}</div>`)
          : (r = C`
        <div part="icon" class="avatar__icon" aria-hidden="true">
          <slot name="icon">
            <sl-icon name="person-fill" library="system"></sl-icon>
          </slot>
        </div>
      `),
        C`
      <div
        part="base"
        class=${ct({
          avatar: !0,
          'avatar--circle': this.shape === 'circle',
          'avatar--rounded': this.shape === 'rounded',
          'avatar--square': this.shape === 'square',
        })}
        role="img"
        aria-label=${this.label}
      >
        ${this.image && !this.hasError ? t : r}
      </div>
    `
      )
    }
  }
Qr.styles = [dt, xS]
Qr.dependencies = {
  'sl-icon': Nt,
}
c([at()], Qr.prototype, 'hasError', 2)
c([p()], Qr.prototype, 'image', 2)
c([p()], Qr.prototype, 'label', 2)
c([p()], Qr.prototype, 'initials', 2)
c([p()], Qr.prototype, 'loading', 2)
c([p({ reflect: !0 })], Qr.prototype, 'shape', 2)
c([X('image')], Qr.prototype, 'handleImageChange', 1)
Qr.define('sl-avatar')
var kS = st`
  .breadcrumb {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
  }
`,
  Vn = class extends nt {
    constructor() {
      super(...arguments),
        (this.localize = new Dt(this)),
        (this.separatorDir = this.localize.dir()),
        (this.label = '')
    }
    // Generates a clone of the separator element to use for each breadcrumb item
    getSeparator() {
      const r = this.separatorSlot
        .assignedElements({ flatten: !0 })[0]
        .cloneNode(!0)
      return (
        [r, ...r.querySelectorAll('[id]')].forEach(i =>
          i.removeAttribute('id'),
        ),
        r.setAttribute('data-default', ''),
        (r.slot = 'separator'),
        r
      )
    }
    handleSlotChange() {
      const t = [...this.defaultSlot.assignedElements({ flatten: !0 })].filter(
        r => r.tagName.toLowerCase() === 'sl-breadcrumb-item',
      )
      t.forEach((r, i) => {
        const o = r.querySelector('[slot="separator"]')
        o === null
          ? r.append(this.getSeparator())
          : o.hasAttribute('data-default') &&
            o.replaceWith(this.getSeparator()),
          i === t.length - 1
            ? r.setAttribute('aria-current', 'page')
            : r.removeAttribute('aria-current')
      })
    }
    render() {
      return (
        this.separatorDir !== this.localize.dir() &&
          ((this.separatorDir = this.localize.dir()),
          this.updateComplete.then(() => this.handleSlotChange())),
        C`
      <nav part="base" class="breadcrumb" aria-label=${this.label}>
        <slot @slotchange=${this.handleSlotChange}></slot>
      </nav>

      <span hidden aria-hidden="true">
        <slot name="separator">
          <sl-icon name=${
            this.localize.dir() === 'rtl' ? 'chevron-left' : 'chevron-right'
          } library="system"></sl-icon>
        </slot>
      </span>
    `
      )
    }
  }
Vn.styles = [dt, kS]
Vn.dependencies = { 'sl-icon': Nt }
c([J('slot')], Vn.prototype, 'defaultSlot', 2)
c([J('slot[name="separator"]')], Vn.prototype, 'separatorSlot', 2)
c([p()], Vn.prototype, 'label', 2)
Vn.define('sl-breadcrumb')
var $S = st`
  :host {
    display: inline-flex;
  }

  .breadcrumb-item {
    display: inline-flex;
    align-items: center;
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-small);
    font-weight: var(--sl-font-weight-semibold);
    color: var(--sl-color-neutral-600);
    line-height: var(--sl-line-height-normal);
    white-space: nowrap;
  }

  .breadcrumb-item__label {
    display: inline-block;
    font-family: inherit;
    font-size: inherit;
    font-weight: inherit;
    line-height: inherit;
    text-decoration: none;
    color: inherit;
    background: none;
    border: none;
    border-radius: var(--sl-border-radius-medium);
    padding: 0;
    margin: 0;
    cursor: pointer;
    transition: var(--sl-transition-fast) --color;
  }

  :host(:not(:last-of-type)) .breadcrumb-item__label {
    color: var(--sl-color-primary-600);
  }

  :host(:not(:last-of-type)) .breadcrumb-item__label:hover {
    color: var(--sl-color-primary-500);
  }

  :host(:not(:last-of-type)) .breadcrumb-item__label:active {
    color: var(--sl-color-primary-600);
  }

  .breadcrumb-item__label:focus {
    outline: none;
  }

  .breadcrumb-item__label:focus-visible {
    outline: var(--sl-focus-ring);
    outline-offset: var(--sl-focus-ring-offset);
  }

  .breadcrumb-item__prefix,
  .breadcrumb-item__suffix {
    display: none;
    flex: 0 0 auto;
    display: flex;
    align-items: center;
  }

  .breadcrumb-item--has-prefix .breadcrumb-item__prefix {
    display: inline-flex;
    margin-inline-end: var(--sl-spacing-x-small);
  }

  .breadcrumb-item--has-suffix .breadcrumb-item__suffix {
    display: inline-flex;
    margin-inline-start: var(--sl-spacing-x-small);
  }

  :host(:last-of-type) .breadcrumb-item__separator {
    display: none;
  }

  .breadcrumb-item__separator {
    display: inline-flex;
    align-items: center;
    margin: 0 var(--sl-spacing-x-small);
    user-select: none;
    -webkit-user-select: none;
  }
`,
  Ri = class extends nt {
    constructor() {
      super(...arguments),
        (this.hasSlotController = new He(this, 'prefix', 'suffix')),
        (this.renderType = 'button'),
        (this.rel = 'noreferrer noopener')
    }
    setRenderType() {
      const t =
        this.defaultSlot
          .assignedElements({ flatten: !0 })
          .filter(r => r.tagName.toLowerCase() === 'sl-dropdown').length > 0
      if (this.href) {
        this.renderType = 'link'
        return
      }
      if (t) {
        this.renderType = 'dropdown'
        return
      }
      this.renderType = 'button'
    }
    hrefChanged() {
      this.setRenderType()
    }
    handleSlotChange() {
      this.setRenderType()
    }
    render() {
      return C`
      <div
        part="base"
        class=${ct({
          'breadcrumb-item': !0,
          'breadcrumb-item--has-prefix': this.hasSlotController.test('prefix'),
          'breadcrumb-item--has-suffix': this.hasSlotController.test('suffix'),
        })}
      >
        <span part="prefix" class="breadcrumb-item__prefix">
          <slot name="prefix"></slot>
        </span>

        ${
          this.renderType === 'link'
            ? C`
              <a
                part="label"
                class="breadcrumb-item__label breadcrumb-item__label--link"
                href="${this.href}"
                target="${it(this.target ? this.target : void 0)}"
                rel=${it(this.target ? this.rel : void 0)}
              >
                <slot @slotchange=${this.handleSlotChange}></slot>
              </a>
            `
            : ''
        }
        ${
          this.renderType === 'button'
            ? C`
              <button part="label" type="button" class="breadcrumb-item__label breadcrumb-item__label--button">
                <slot @slotchange=${this.handleSlotChange}></slot>
              </button>
            `
            : ''
        }
        ${
          this.renderType === 'dropdown'
            ? C`
              <div part="label" class="breadcrumb-item__label breadcrumb-item__label--drop-down">
                <slot @slotchange=${this.handleSlotChange}></slot>
              </div>
            `
            : ''
        }

        <span part="suffix" class="breadcrumb-item__suffix">
          <slot name="suffix"></slot>
        </span>

        <span part="separator" class="breadcrumb-item__separator" aria-hidden="true">
          <slot name="separator"></slot>
        </span>
      </div>
    `
    }
  }
Ri.styles = [dt, $S]
c([J('slot:not([name])')], Ri.prototype, 'defaultSlot', 2)
c([at()], Ri.prototype, 'renderType', 2)
c([p()], Ri.prototype, 'href', 2)
c([p()], Ri.prototype, 'target', 2)
c([p()], Ri.prototype, 'rel', 2)
c([X('href', { waitUntilFirstUpdate: !0 })], Ri.prototype, 'hrefChanged', 1)
Ri.define('sl-breadcrumb-item')
var CS = st`
  :host {
    --control-box-size: 3rem;
    --icon-size: calc(var(--control-box-size) * 0.625);

    display: inline-flex;
    position: relative;
    cursor: pointer;
  }

  img {
    display: block;
    width: 100%;
    height: 100%;
  }

  img[aria-hidden='true'] {
    display: none;
  }

  .animated-image__control-box {
    display: flex;
    position: absolute;
    align-items: center;
    justify-content: center;
    top: calc(50% - var(--control-box-size) / 2);
    right: calc(50% - var(--control-box-size) / 2);
    width: var(--control-box-size);
    height: var(--control-box-size);
    font-size: var(--icon-size);
    background: none;
    border: solid 2px currentColor;
    background-color: rgb(0 0 0 /50%);
    border-radius: var(--sl-border-radius-circle);
    color: white;
    pointer-events: none;
    transition: var(--sl-transition-fast) opacity;
  }

  :host([play]:hover) .animated-image__control-box {
    opacity: 1;
  }

  :host([play]:not(:hover)) .animated-image__control-box {
    opacity: 0;
  }

  :host([play]) slot[name='play-icon'],
  :host(:not([play])) slot[name='pause-icon'] {
    display: none;
  }
`,
  Fr = class extends nt {
    constructor() {
      super(...arguments), (this.isLoaded = !1)
    }
    handleClick() {
      this.play = !this.play
    }
    handleLoad() {
      const t = document.createElement('canvas'),
        { width: r, height: i } = this.animatedImage
      ;(t.width = r),
        (t.height = i),
        t.getContext('2d').drawImage(this.animatedImage, 0, 0, r, i),
        (this.frozenFrame = t.toDataURL('image/gif')),
        this.isLoaded || (this.emit('sl-load'), (this.isLoaded = !0))
    }
    handleError() {
      this.emit('sl-error')
    }
    handlePlayChange() {
      this.play &&
        ((this.animatedImage.src = ''), (this.animatedImage.src = this.src))
    }
    handleSrcChange() {
      this.isLoaded = !1
    }
    render() {
      return C`
      <div class="animated-image">
        <img
          class="animated-image__animated"
          src=${this.src}
          alt=${this.alt}
          crossorigin="anonymous"
          aria-hidden=${this.play ? 'false' : 'true'}
          @click=${this.handleClick}
          @load=${this.handleLoad}
          @error=${this.handleError}
        />

        ${
          this.isLoaded
            ? C`
              <img
                class="animated-image__frozen"
                src=${this.frozenFrame}
                alt=${this.alt}
                aria-hidden=${this.play ? 'true' : 'false'}
                @click=${this.handleClick}
              />

              <div part="control-box" class="animated-image__control-box">
                <slot name="play-icon"><sl-icon name="play-fill" library="system"></sl-icon></slot>
                <slot name="pause-icon"><sl-icon name="pause-fill" library="system"></sl-icon></slot>
              </div>
            `
            : ''
        }
      </div>
    `
    }
  }
Fr.styles = [dt, CS]
Fr.dependencies = { 'sl-icon': Nt }
c([J('.animated-image__animated')], Fr.prototype, 'animatedImage', 2)
c([at()], Fr.prototype, 'frozenFrame', 2)
c([at()], Fr.prototype, 'isLoaded', 2)
c([p()], Fr.prototype, 'src', 2)
c([p()], Fr.prototype, 'alt', 2)
c([p({ type: Boolean, reflect: !0 })], Fr.prototype, 'play', 2)
c(
  [X('play', { waitUntilFirstUpdate: !0 })],
  Fr.prototype,
  'handlePlayChange',
  1,
)
c([X('src')], Fr.prototype, 'handleSrcChange', 1)
Fr.define('sl-animated-image')
var SS = st`
  :host {
    display: inline-flex;
  }

  .badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: max(12px, 0.75em);
    font-weight: var(--sl-font-weight-semibold);
    letter-spacing: var(--sl-letter-spacing-normal);
    line-height: 1;
    border-radius: var(--sl-border-radius-small);
    border: solid 1px var(--sl-color-neutral-0);
    white-space: nowrap;
    padding: 0.35em 0.6em;
    user-select: none;
    -webkit-user-select: none;
    cursor: inherit;
  }

  /* Variant modifiers */
  .badge--primary {
    background-color: var(--sl-color-primary-600);
    color: var(--sl-color-neutral-0);
  }

  .badge--success {
    background-color: var(--sl-color-success-600);
    color: var(--sl-color-neutral-0);
  }

  .badge--neutral {
    background-color: var(--sl-color-neutral-600);
    color: var(--sl-color-neutral-0);
  }

  .badge--warning {
    background-color: var(--sl-color-warning-600);
    color: var(--sl-color-neutral-0);
  }

  .badge--danger {
    background-color: var(--sl-color-danger-600);
    color: var(--sl-color-neutral-0);
  }

  /* Pill modifier */
  .badge--pill {
    border-radius: var(--sl-border-radius-pill);
  }

  /* Pulse modifier */
  .badge--pulse {
    animation: pulse 1.5s infinite;
  }

  .badge--pulse.badge--primary {
    --pulse-color: var(--sl-color-primary-600);
  }

  .badge--pulse.badge--success {
    --pulse-color: var(--sl-color-success-600);
  }

  .badge--pulse.badge--neutral {
    --pulse-color: var(--sl-color-neutral-600);
  }

  .badge--pulse.badge--warning {
    --pulse-color: var(--sl-color-warning-600);
  }

  .badge--pulse.badge--danger {
    --pulse-color: var(--sl-color-danger-600);
  }

  @keyframes pulse {
    0% {
      box-shadow: 0 0 0 0 var(--pulse-color);
    }
    70% {
      box-shadow: 0 0 0 0.5rem transparent;
    }
    100% {
      box-shadow: 0 0 0 0 transparent;
    }
  }
`,
  Ho = class extends nt {
    constructor() {
      super(...arguments),
        (this.variant = 'primary'),
        (this.pill = !1),
        (this.pulse = !1)
    }
    render() {
      return C`
      <span
        part="base"
        class=${ct({
          badge: !0,
          'badge--primary': this.variant === 'primary',
          'badge--success': this.variant === 'success',
          'badge--neutral': this.variant === 'neutral',
          'badge--warning': this.variant === 'warning',
          'badge--danger': this.variant === 'danger',
          'badge--pill': this.pill,
          'badge--pulse': this.pulse,
        })}
        role="status"
      >
        <slot></slot>
      </span>
    `
    }
  }
Ho.styles = [dt, SS]
c([p({ reflect: !0 })], Ho.prototype, 'variant', 2)
c([p({ type: Boolean, reflect: !0 })], Ho.prototype, 'pill', 2)
c([p({ type: Boolean, reflect: !0 })], Ho.prototype, 'pulse', 2)
Ho.define('sl-badge')
var zS = st`
  :host {
    display: contents;

    /* For better DX, we'll reset the margin here so the base part can inherit it */
    margin: 0;
  }

  .alert {
    position: relative;
    display: flex;
    align-items: stretch;
    background-color: var(--sl-panel-background-color);
    border: solid var(--sl-panel-border-width) var(--sl-panel-border-color);
    border-top-width: calc(var(--sl-panel-border-width) * 3);
    border-radius: var(--sl-border-radius-medium);
    font-family: var(--sl-font-sans);
    font-size: var(--sl-font-size-small);
    font-weight: var(--sl-font-weight-normal);
    line-height: 1.6;
    color: var(--sl-color-neutral-700);
    margin: inherit;
    overflow: hidden;
  }

  .alert:not(.alert--has-icon) .alert__icon,
  .alert:not(.alert--closable) .alert__close-button {
    display: none;
  }

  .alert__icon {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    font-size: var(--sl-font-size-large);
    padding-inline-start: var(--sl-spacing-large);
  }

  .alert--has-countdown {
    border-bottom: none;
  }

  .alert--primary {
    border-top-color: var(--sl-color-primary-600);
  }

  .alert--primary .alert__icon {
    color: var(--sl-color-primary-600);
  }

  .alert--success {
    border-top-color: var(--sl-color-success-600);
  }

  .alert--success .alert__icon {
    color: var(--sl-color-success-600);
  }

  .alert--neutral {
    border-top-color: var(--sl-color-neutral-600);
  }

  .alert--neutral .alert__icon {
    color: var(--sl-color-neutral-600);
  }

  .alert--warning {
    border-top-color: var(--sl-color-warning-600);
  }

  .alert--warning .alert__icon {
    color: var(--sl-color-warning-600);
  }

  .alert--danger {
    border-top-color: var(--sl-color-danger-600);
  }

  .alert--danger .alert__icon {
    color: var(--sl-color-danger-600);
  }

  .alert__message {
    flex: 1 1 auto;
    display: block;
    padding: var(--sl-spacing-large);
    overflow: hidden;
  }

  .alert__close-button {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    font-size: var(--sl-font-size-medium);
    padding-inline-end: var(--sl-spacing-medium);
  }

  .alert__countdown {
    position: absolute;
    bottom: 0;
    left: 0;
    width: 100%;
    height: calc(var(--sl-panel-border-width) * 3);
    background-color: var(--sl-panel-border-color);
    display: flex;
  }

  .alert__countdown--ltr {
    justify-content: flex-end;
  }

  .alert__countdown .alert__countdown-elapsed {
    height: 100%;
    width: 0;
  }

  .alert--primary .alert__countdown-elapsed {
    background-color: var(--sl-color-primary-600);
  }

  .alert--success .alert__countdown-elapsed {
    background-color: var(--sl-color-success-600);
  }

  .alert--neutral .alert__countdown-elapsed {
    background-color: var(--sl-color-neutral-600);
  }

  .alert--warning .alert__countdown-elapsed {
    background-color: var(--sl-color-warning-600);
  }

  .alert--danger .alert__countdown-elapsed {
    background-color: var(--sl-color-danger-600);
  }

  .alert__timer {
    display: none;
  }
`,
  En = Object.assign(document.createElement('div'), {
    className: 'sl-toast-stack',
  }),
  fr = class extends nt {
    constructor() {
      super(...arguments),
        (this.hasSlotController = new He(this, 'icon', 'suffix')),
        (this.localize = new Dt(this)),
        (this.open = !1),
        (this.closable = !1),
        (this.variant = 'primary'),
        (this.duration = 1 / 0),
        (this.remainingTime = this.duration)
    }
    firstUpdated() {
      this.base.hidden = !this.open
    }
    restartAutoHide() {
      this.handleCountdownChange(),
        clearTimeout(this.autoHideTimeout),
        clearInterval(this.remainingTimeInterval),
        this.open &&
          this.duration < 1 / 0 &&
          ((this.autoHideTimeout = window.setTimeout(
            () => this.hide(),
            this.duration,
          )),
          (this.remainingTime = this.duration),
          (this.remainingTimeInterval = window.setInterval(() => {
            this.remainingTime -= 100
          }, 100)))
    }
    pauseAutoHide() {
      var t
      ;(t = this.countdownAnimation) == null || t.pause(),
        clearTimeout(this.autoHideTimeout),
        clearInterval(this.remainingTimeInterval)
    }
    resumeAutoHide() {
      var t
      this.duration < 1 / 0 &&
        ((this.autoHideTimeout = window.setTimeout(
          () => this.hide(),
          this.remainingTime,
        )),
        (this.remainingTimeInterval = window.setInterval(() => {
          this.remainingTime -= 100
        }, 100)),
        (t = this.countdownAnimation) == null || t.play())
    }
    handleCountdownChange() {
      if (this.open && this.duration < 1 / 0 && this.countdown) {
        const { countdownElement: t } = this,
          r = '100%',
          i = '0'
        this.countdownAnimation = t.animate([{ width: r }, { width: i }], {
          duration: this.duration,
          easing: 'linear',
        })
      }
    }
    handleCloseClick() {
      this.hide()
    }
    async handleOpenChange() {
      if (this.open) {
        this.emit('sl-show'),
          this.duration < 1 / 0 && this.restartAutoHide(),
          await pe(this.base),
          (this.base.hidden = !1)
        const { keyframes: t, options: r } = Xt(this, 'alert.show', {
          dir: this.localize.dir(),
        })
        await ie(this.base, t, r), this.emit('sl-after-show')
      } else {
        this.emit('sl-hide'),
          clearTimeout(this.autoHideTimeout),
          clearInterval(this.remainingTimeInterval),
          await pe(this.base)
        const { keyframes: t, options: r } = Xt(this, 'alert.hide', {
          dir: this.localize.dir(),
        })
        await ie(this.base, t, r),
          (this.base.hidden = !0),
          this.emit('sl-after-hide')
      }
    }
    handleDurationChange() {
      this.restartAutoHide()
    }
    /** Shows the alert. */
    async show() {
      if (!this.open) return (this.open = !0), Ue(this, 'sl-after-show')
    }
    /** Hides the alert */
    async hide() {
      if (this.open) return (this.open = !1), Ue(this, 'sl-after-hide')
    }
    /**
     * Displays the alert as a toast notification. This will move the alert out of its position in the DOM and, when
     * dismissed, it will be removed from the DOM completely. By storing a reference to the alert, you can reuse it by
     * calling this method again. The returned promise will resolve after the alert is hidden.
     */
    async toast() {
      return new Promise(t => {
        this.handleCountdownChange(),
          En.parentElement === null && document.body.append(En),
          En.appendChild(this),
          requestAnimationFrame(() => {
            this.clientWidth, this.show()
          }),
          this.addEventListener(
            'sl-after-hide',
            () => {
              En.removeChild(this),
                t(),
                En.querySelector('sl-alert') === null && En.remove()
            },
            { once: !0 },
          )
      })
    }
    render() {
      return C`
      <div
        part="base"
        class=${ct({
          alert: !0,
          'alert--open': this.open,
          'alert--closable': this.closable,
          'alert--has-countdown': !!this.countdown,
          'alert--has-icon': this.hasSlotController.test('icon'),
          'alert--primary': this.variant === 'primary',
          'alert--success': this.variant === 'success',
          'alert--neutral': this.variant === 'neutral',
          'alert--warning': this.variant === 'warning',
          'alert--danger': this.variant === 'danger',
        })}
        role="alert"
        aria-hidden=${this.open ? 'false' : 'true'}
        @mouseenter=${this.pauseAutoHide}
        @mouseleave=${this.resumeAutoHide}
      >
        <div part="icon" class="alert__icon">
          <slot name="icon"></slot>
        </div>

        <div part="message" class="alert__message" aria-live="polite">
          <slot></slot>
        </div>

        ${
          this.closable
            ? C`
              <sl-icon-button
                part="close-button"
                exportparts="base:close-button__base"
                class="alert__close-button"
                name="x-lg"
                library="system"
                label=${this.localize.term('close')}
                @click=${this.handleCloseClick}
              ></sl-icon-button>
            `
            : ''
        }

        <div role="timer" class="alert__timer">${this.remainingTime}</div>

        ${
          this.countdown
            ? C`
              <div
                class=${ct({
                  alert__countdown: !0,
                  'alert__countdown--ltr': this.countdown === 'ltr',
                })}
              >
                <div class="alert__countdown-elapsed"></div>
              </div>
            `
            : ''
        }
      </div>
    `
    }
  }
fr.styles = [dt, zS]
fr.dependencies = { 'sl-icon-button': ye }
c([J('[part~="base"]')], fr.prototype, 'base', 2)
c([J('.alert__countdown-elapsed')], fr.prototype, 'countdownElement', 2)
c([p({ type: Boolean, reflect: !0 })], fr.prototype, 'open', 2)
c([p({ type: Boolean, reflect: !0 })], fr.prototype, 'closable', 2)
c([p({ reflect: !0 })], fr.prototype, 'variant', 2)
c([p({ type: Number })], fr.prototype, 'duration', 2)
c([p({ type: String, reflect: !0 })], fr.prototype, 'countdown', 2)
c([at()], fr.prototype, 'remainingTime', 2)
c(
  [X('open', { waitUntilFirstUpdate: !0 })],
  fr.prototype,
  'handleOpenChange',
  1,
)
c([X('duration')], fr.prototype, 'handleDurationChange', 1)
Mt('alert.show', {
  keyframes: [
    { opacity: 0, scale: 0.8 },
    { opacity: 1, scale: 1 },
  ],
  options: { duration: 250, easing: 'ease' },
})
Mt('alert.hide', {
  keyframes: [
    { opacity: 1, scale: 1 },
    { opacity: 0, scale: 0.8 },
  ],
  options: { duration: 250, easing: 'ease' },
})
fr.define('sl-alert')
const AS = [
    {
      offset: 0,
      easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)',
      transform: 'translate3d(0, 0, 0)',
    },
    {
      offset: 0.2,
      easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)',
      transform: 'translate3d(0, 0, 0)',
    },
    {
      offset: 0.4,
      easing: 'cubic-bezier(0.755, 0.05, 0.855, 0.06)',
      transform: 'translate3d(0, -30px, 0) scaleY(1.1)',
    },
    {
      offset: 0.43,
      easing: 'cubic-bezier(0.755, 0.05, 0.855, 0.06)',
      transform: 'translate3d(0, -30px, 0) scaleY(1.1)',
    },
    {
      offset: 0.53,
      easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)',
      transform: 'translate3d(0, 0, 0)',
    },
    {
      offset: 0.7,
      easing: 'cubic-bezier(0.755, 0.05, 0.855, 0.06)',
      transform: 'translate3d(0, -15px, 0) scaleY(1.05)',
    },
    {
      offset: 0.8,
      'transition-timing-function': 'cubic-bezier(0.215, 0.61, 0.355, 1)',
      transform: 'translate3d(0, 0, 0) scaleY(0.95)',
    },
    { offset: 0.9, transform: 'translate3d(0, -4px, 0) scaleY(1.02)' },
    {
      offset: 1,
      easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)',
      transform: 'translate3d(0, 0, 0)',
    },
  ],
  ES = [
    { offset: 0, opacity: '1' },
    { offset: 0.25, opacity: '0' },
    { offset: 0.5, opacity: '1' },
    { offset: 0.75, opacity: '0' },
    { offset: 1, opacity: '1' },
  ],
  TS = [
    { offset: 0, transform: 'translateX(0)' },
    { offset: 0.065, transform: 'translateX(-6px) rotateY(-9deg)' },
    { offset: 0.185, transform: 'translateX(5px) rotateY(7deg)' },
    { offset: 0.315, transform: 'translateX(-3px) rotateY(-5deg)' },
    { offset: 0.435, transform: 'translateX(2px) rotateY(3deg)' },
    { offset: 0.5, transform: 'translateX(0)' },
  ],
  IS = [
    { offset: 0, transform: 'scale(1)' },
    { offset: 0.14, transform: 'scale(1.3)' },
    { offset: 0.28, transform: 'scale(1)' },
    { offset: 0.42, transform: 'scale(1.3)' },
    { offset: 0.7, transform: 'scale(1)' },
  ],
  OS = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    { offset: 0.111, transform: 'translate3d(0, 0, 0)' },
    { offset: 0.222, transform: 'skewX(-12.5deg) skewY(-12.5deg)' },
    { offset: 0.33299999999999996, transform: 'skewX(6.25deg) skewY(6.25deg)' },
    { offset: 0.444, transform: 'skewX(-3.125deg) skewY(-3.125deg)' },
    { offset: 0.555, transform: 'skewX(1.5625deg) skewY(1.5625deg)' },
    {
      offset: 0.6659999999999999,
      transform: 'skewX(-0.78125deg) skewY(-0.78125deg)',
    },
    { offset: 0.777, transform: 'skewX(0.390625deg) skewY(0.390625deg)' },
    { offset: 0.888, transform: 'skewX(-0.1953125deg) skewY(-0.1953125deg)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  LS = [
    { offset: 0, transform: 'scale3d(1, 1, 1)' },
    { offset: 0.5, transform: 'scale3d(1.05, 1.05, 1.05)' },
    { offset: 1, transform: 'scale3d(1, 1, 1)' },
  ],
  DS = [
    { offset: 0, transform: 'scale3d(1, 1, 1)' },
    { offset: 0.3, transform: 'scale3d(1.25, 0.75, 1)' },
    { offset: 0.4, transform: 'scale3d(0.75, 1.25, 1)' },
    { offset: 0.5, transform: 'scale3d(1.15, 0.85, 1)' },
    { offset: 0.65, transform: 'scale3d(0.95, 1.05, 1)' },
    { offset: 0.75, transform: 'scale3d(1.05, 0.95, 1)' },
    { offset: 1, transform: 'scale3d(1, 1, 1)' },
  ],
  MS = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    { offset: 0.1, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 0.2, transform: 'translate3d(10px, 0, 0)' },
    { offset: 0.3, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 0.4, transform: 'translate3d(10px, 0, 0)' },
    { offset: 0.5, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 0.6, transform: 'translate3d(10px, 0, 0)' },
    { offset: 0.7, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 0.8, transform: 'translate3d(10px, 0, 0)' },
    { offset: 0.9, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  RS = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    { offset: 0.1, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 0.2, transform: 'translate3d(10px, 0, 0)' },
    { offset: 0.3, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 0.4, transform: 'translate3d(10px, 0, 0)' },
    { offset: 0.5, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 0.6, transform: 'translate3d(10px, 0, 0)' },
    { offset: 0.7, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 0.8, transform: 'translate3d(10px, 0, 0)' },
    { offset: 0.9, transform: 'translate3d(-10px, 0, 0)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  PS = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    { offset: 0.1, transform: 'translate3d(0, -10px, 0)' },
    { offset: 0.2, transform: 'translate3d(0, 10px, 0)' },
    { offset: 0.3, transform: 'translate3d(0, -10px, 0)' },
    { offset: 0.4, transform: 'translate3d(0, 10px, 0)' },
    { offset: 0.5, transform: 'translate3d(0, -10px, 0)' },
    { offset: 0.6, transform: 'translate3d(0, 10px, 0)' },
    { offset: 0.7, transform: 'translate3d(0, -10px, 0)' },
    { offset: 0.8, transform: 'translate3d(0, 10px, 0)' },
    { offset: 0.9, transform: 'translate3d(0, -10px, 0)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  BS = [
    { offset: 0.2, transform: 'rotate3d(0, 0, 1, 15deg)' },
    { offset: 0.4, transform: 'rotate3d(0, 0, 1, -10deg)' },
    { offset: 0.6, transform: 'rotate3d(0, 0, 1, 5deg)' },
    { offset: 0.8, transform: 'rotate3d(0, 0, 1, -5deg)' },
    { offset: 1, transform: 'rotate3d(0, 0, 1, 0deg)' },
  ],
  FS = [
    { offset: 0, transform: 'scale3d(1, 1, 1)' },
    {
      offset: 0.1,
      transform: 'scale3d(0.9, 0.9, 0.9) rotate3d(0, 0, 1, -3deg)',
    },
    {
      offset: 0.2,
      transform: 'scale3d(0.9, 0.9, 0.9) rotate3d(0, 0, 1, -3deg)',
    },
    {
      offset: 0.3,
      transform: 'scale3d(1.1, 1.1, 1.1) rotate3d(0, 0, 1, 3deg)',
    },
    {
      offset: 0.4,
      transform: 'scale3d(1.1, 1.1, 1.1) rotate3d(0, 0, 1, -3deg)',
    },
    {
      offset: 0.5,
      transform: 'scale3d(1.1, 1.1, 1.1) rotate3d(0, 0, 1, 3deg)',
    },
    {
      offset: 0.6,
      transform: 'scale3d(1.1, 1.1, 1.1) rotate3d(0, 0, 1, -3deg)',
    },
    {
      offset: 0.7,
      transform: 'scale3d(1.1, 1.1, 1.1) rotate3d(0, 0, 1, 3deg)',
    },
    {
      offset: 0.8,
      transform: 'scale3d(1.1, 1.1, 1.1) rotate3d(0, 0, 1, -3deg)',
    },
    {
      offset: 0.9,
      transform: 'scale3d(1.1, 1.1, 1.1) rotate3d(0, 0, 1, 3deg)',
    },
    { offset: 1, transform: 'scale3d(1, 1, 1)' },
  ],
  NS = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    {
      offset: 0.15,
      transform: 'translate3d(-25%, 0, 0) rotate3d(0, 0, 1, -5deg)',
    },
    {
      offset: 0.3,
      transform: 'translate3d(20%, 0, 0) rotate3d(0, 0, 1, 3deg)',
    },
    {
      offset: 0.45,
      transform: 'translate3d(-15%, 0, 0) rotate3d(0, 0, 1, -3deg)',
    },
    {
      offset: 0.6,
      transform: 'translate3d(10%, 0, 0) rotate3d(0, 0, 1, 2deg)',
    },
    {
      offset: 0.75,
      transform: 'translate3d(-5%, 0, 0) rotate3d(0, 0, 1, -1deg)',
    },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  US = [
    { offset: 0, transform: 'translateY(-1200px) scale(0.7)', opacity: '0.7' },
    { offset: 0.8, transform: 'translateY(0px) scale(0.7)', opacity: '0.7' },
    { offset: 1, transform: 'scale(1)', opacity: '1' },
  ],
  HS = [
    { offset: 0, transform: 'translateX(-2000px) scale(0.7)', opacity: '0.7' },
    { offset: 0.8, transform: 'translateX(0px) scale(0.7)', opacity: '0.7' },
    { offset: 1, transform: 'scale(1)', opacity: '1' },
  ],
  VS = [
    { offset: 0, transform: 'translateX(2000px) scale(0.7)', opacity: '0.7' },
    { offset: 0.8, transform: 'translateX(0px) scale(0.7)', opacity: '0.7' },
    { offset: 1, transform: 'scale(1)', opacity: '1' },
  ],
  WS = [
    { offset: 0, transform: 'translateY(1200px) scale(0.7)', opacity: '0.7' },
    { offset: 0.8, transform: 'translateY(0px) scale(0.7)', opacity: '0.7' },
    { offset: 1, transform: 'scale(1)', opacity: '1' },
  ],
  qS = [
    { offset: 0, transform: 'scale(1)', opacity: '1' },
    { offset: 0.2, transform: 'translateY(0px) scale(0.7)', opacity: '0.7' },
    { offset: 1, transform: 'translateY(700px) scale(0.7)', opacity: '0.7' },
  ],
  YS = [
    { offset: 0, transform: 'scale(1)', opacity: '1' },
    { offset: 0.2, transform: 'translateX(0px) scale(0.7)', opacity: '0.7' },
    { offset: 1, transform: 'translateX(-2000px) scale(0.7)', opacity: '0.7' },
  ],
  KS = [
    { offset: 0, transform: 'scale(1)', opacity: '1' },
    { offset: 0.2, transform: 'translateX(0px) scale(0.7)', opacity: '0.7' },
    { offset: 1, transform: 'translateX(2000px) scale(0.7)', opacity: '0.7' },
  ],
  GS = [
    { offset: 0, transform: 'scale(1)', opacity: '1' },
    { offset: 0.2, transform: 'translateY(0px) scale(0.7)', opacity: '0.7' },
    { offset: 1, transform: 'translateY(-700px) scale(0.7)', opacity: '0.7' },
  ],
  XS = [
    { offset: 0, opacity: '0', transform: 'scale3d(0.3, 0.3, 0.3)' },
    { offset: 0, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.2, transform: 'scale3d(1.1, 1.1, 1.1)' },
    { offset: 0.2, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.4, transform: 'scale3d(0.9, 0.9, 0.9)' },
    { offset: 0.4, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.6, opacity: '1', transform: 'scale3d(1.03, 1.03, 1.03)' },
    { offset: 0.6, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.8, transform: 'scale3d(0.97, 0.97, 0.97)' },
    { offset: 0.8, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 1, opacity: '1', transform: 'scale3d(1, 1, 1)' },
    { offset: 1, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
  ],
  jS = [
    {
      offset: 0,
      opacity: '0',
      transform: 'translate3d(0, -3000px, 0) scaleY(3)',
    },
    { offset: 0, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    {
      offset: 0.6,
      opacity: '1',
      transform: 'translate3d(0, 25px, 0) scaleY(0.9)',
    },
    { offset: 0.6, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.75, transform: 'translate3d(0, -10px, 0) scaleY(0.95)' },
    { offset: 0.75, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.9, transform: 'translate3d(0, 5px, 0) scaleY(0.985)' },
    { offset: 0.9, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
    { offset: 1, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
  ],
  ZS = [
    {
      offset: 0,
      opacity: '0',
      transform: 'translate3d(-3000px, 0, 0) scaleX(3)',
    },
    { offset: 0, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    {
      offset: 0.6,
      opacity: '1',
      transform: 'translate3d(25px, 0, 0) scaleX(1)',
    },
    { offset: 0.6, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.75, transform: 'translate3d(-10px, 0, 0) scaleX(0.98)' },
    { offset: 0.75, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.9, transform: 'translate3d(5px, 0, 0) scaleX(0.995)' },
    { offset: 0.9, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
    { offset: 1, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
  ],
  JS = [
    {
      offset: 0,
      opacity: '0',
      transform: 'translate3d(3000px, 0, 0) scaleX(3)',
    },
    { offset: 0, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    {
      offset: 0.6,
      opacity: '1',
      transform: 'translate3d(-25px, 0, 0) scaleX(1)',
    },
    { offset: 0.6, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.75, transform: 'translate3d(10px, 0, 0) scaleX(0.98)' },
    { offset: 0.75, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.9, transform: 'translate3d(-5px, 0, 0) scaleX(0.995)' },
    { offset: 0.9, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
    { offset: 1, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
  ],
  QS = [
    {
      offset: 0,
      opacity: '0',
      transform: 'translate3d(0, 3000px, 0) scaleY(5)',
    },
    { offset: 0, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    {
      offset: 0.6,
      opacity: '1',
      transform: 'translate3d(0, -20px, 0) scaleY(0.9)',
    },
    { offset: 0.6, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.75, transform: 'translate3d(0, 10px, 0) scaleY(0.95)' },
    { offset: 0.75, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 0.9, transform: 'translate3d(0, -5px, 0) scaleY(0.985)' },
    { offset: 0.9, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
    { offset: 1, easing: 'cubic-bezier(0.215, 0.61, 0.355, 1)' },
  ],
  t5 = [
    { offset: 0.2, transform: 'scale3d(0.9, 0.9, 0.9)' },
    { offset: 0.5, opacity: '1', transform: 'scale3d(1.1, 1.1, 1.1)' },
    { offset: 0.55, opacity: '1', transform: 'scale3d(1.1, 1.1, 1.1)' },
    { offset: 1, opacity: '0', transform: 'scale3d(0.3, 0.3, 0.3)' },
  ],
  e5 = [
    { offset: 0.2, transform: 'translate3d(0, 10px, 0) scaleY(0.985)' },
    {
      offset: 0.4,
      opacity: '1',
      transform: 'translate3d(0, -20px, 0) scaleY(0.9)',
    },
    {
      offset: 0.45,
      opacity: '1',
      transform: 'translate3d(0, -20px, 0) scaleY(0.9)',
    },
    {
      offset: 1,
      opacity: '0',
      transform: 'translate3d(0, 2000px, 0) scaleY(3)',
    },
  ],
  r5 = [
    {
      offset: 0.2,
      opacity: '1',
      transform: 'translate3d(20px, 0, 0) scaleX(0.9)',
    },
    {
      offset: 1,
      opacity: '0',
      transform: 'translate3d(-2000px, 0, 0) scaleX(2)',
    },
  ],
  i5 = [
    {
      offset: 0.2,
      opacity: '1',
      transform: 'translate3d(-20px, 0, 0) scaleX(0.9)',
    },
    {
      offset: 1,
      opacity: '0',
      transform: 'translate3d(2000px, 0, 0) scaleX(2)',
    },
  ],
  n5 = [
    { offset: 0.2, transform: 'translate3d(0, -10px, 0) scaleY(0.985)' },
    {
      offset: 0.4,
      opacity: '1',
      transform: 'translate3d(0, 20px, 0) scaleY(0.9)',
    },
    {
      offset: 0.45,
      opacity: '1',
      transform: 'translate3d(0, 20px, 0) scaleY(0.9)',
    },
    {
      offset: 1,
      opacity: '0',
      transform: 'translate3d(0, -2000px, 0) scaleY(3)',
    },
  ],
  o5 = [
    { offset: 0, opacity: '0' },
    { offset: 1, opacity: '1' },
  ],
  s5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(-100%, 100%, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  a5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(100%, 100%, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  l5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(0, -100%, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  c5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(0, -2000px, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  d5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(-100%, 0, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  h5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(-2000px, 0, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  u5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(100%, 0, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  p5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(2000px, 0, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  f5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(-100%, -100%, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  g5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(100%, -100%, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  m5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(0, 100%, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  v5 = [
    { offset: 0, opacity: '0', transform: 'translate3d(0, 2000px, 0)' },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  b5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0' },
  ],
  y5 = [
    { offset: 0, opacity: '1', transform: 'translate3d(0, 0, 0)' },
    { offset: 1, opacity: '0', transform: 'translate3d(-100%, 100%, 0)' },
  ],
  _5 = [
    { offset: 0, opacity: '1', transform: 'translate3d(0, 0, 0)' },
    { offset: 1, opacity: '0', transform: 'translate3d(100%, 100%, 0)' },
  ],
  w5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0', transform: 'translate3d(0, 100%, 0)' },
  ],
  x5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0', transform: 'translate3d(0, 2000px, 0)' },
  ],
  k5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0', transform: 'translate3d(-100%, 0, 0)' },
  ],
  $5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0', transform: 'translate3d(-2000px, 0, 0)' },
  ],
  C5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0', transform: 'translate3d(100%, 0, 0)' },
  ],
  S5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0', transform: 'translate3d(2000px, 0, 0)' },
  ],
  z5 = [
    { offset: 0, opacity: '1', transform: 'translate3d(0, 0, 0)' },
    { offset: 1, opacity: '0', transform: 'translate3d(-100%, -100%, 0)' },
  ],
  A5 = [
    { offset: 0, opacity: '1', transform: 'translate3d(0, 0, 0)' },
    { offset: 1, opacity: '0', transform: 'translate3d(100%, -100%, 0)' },
  ],
  E5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0', transform: 'translate3d(0, -100%, 0)' },
  ],
  T5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, opacity: '0', transform: 'translate3d(0, -2000px, 0)' },
  ],
  I5 = [
    {
      offset: 0,
      transform:
        'perspective(400px) scale3d(1, 1, 1) translate3d(0, 0, 0) rotate3d(0, 1, 0, -360deg)',
      easing: 'ease-out',
    },
    {
      offset: 0.4,
      transform: `perspective(400px) scale3d(1, 1, 1) translate3d(0, 0, 150px)
      rotate3d(0, 1, 0, -190deg)`,
      easing: 'ease-out',
    },
    {
      offset: 0.5,
      transform: `perspective(400px) scale3d(1, 1, 1) translate3d(0, 0, 150px)
      rotate3d(0, 1, 0, -170deg)`,
      easing: 'ease-in',
    },
    {
      offset: 0.8,
      transform: `perspective(400px) scale3d(0.95, 0.95, 0.95) translate3d(0, 0, 0)
      rotate3d(0, 1, 0, 0deg)`,
      easing: 'ease-in',
    },
    {
      offset: 1,
      transform:
        'perspective(400px) scale3d(1, 1, 1) translate3d(0, 0, 0) rotate3d(0, 1, 0, 0deg)',
      easing: 'ease-in',
    },
  ],
  O5 = [
    {
      offset: 0,
      transform: 'perspective(400px) rotate3d(1, 0, 0, 90deg)',
      easing: 'ease-in',
      opacity: '0',
    },
    {
      offset: 0.4,
      transform: 'perspective(400px) rotate3d(1, 0, 0, -20deg)',
      easing: 'ease-in',
    },
    {
      offset: 0.6,
      transform: 'perspective(400px) rotate3d(1, 0, 0, 10deg)',
      opacity: '1',
    },
    { offset: 0.8, transform: 'perspective(400px) rotate3d(1, 0, 0, -5deg)' },
    { offset: 1, transform: 'perspective(400px)' },
  ],
  L5 = [
    {
      offset: 0,
      transform: 'perspective(400px) rotate3d(0, 1, 0, 90deg)',
      easing: 'ease-in',
      opacity: '0',
    },
    {
      offset: 0.4,
      transform: 'perspective(400px) rotate3d(0, 1, 0, -20deg)',
      easing: 'ease-in',
    },
    {
      offset: 0.6,
      transform: 'perspective(400px) rotate3d(0, 1, 0, 10deg)',
      opacity: '1',
    },
    { offset: 0.8, transform: 'perspective(400px) rotate3d(0, 1, 0, -5deg)' },
    { offset: 1, transform: 'perspective(400px)' },
  ],
  D5 = [
    { offset: 0, transform: 'perspective(400px)' },
    {
      offset: 0.3,
      transform: 'perspective(400px) rotate3d(1, 0, 0, -20deg)',
      opacity: '1',
    },
    {
      offset: 1,
      transform: 'perspective(400px) rotate3d(1, 0, 0, 90deg)',
      opacity: '0',
    },
  ],
  M5 = [
    { offset: 0, transform: 'perspective(400px)' },
    {
      offset: 0.3,
      transform: 'perspective(400px) rotate3d(0, 1, 0, -15deg)',
      opacity: '1',
    },
    {
      offset: 1,
      transform: 'perspective(400px) rotate3d(0, 1, 0, 90deg)',
      opacity: '0',
    },
  ],
  R5 = [
    {
      offset: 0,
      transform: 'translate3d(-100%, 0, 0) skewX(30deg)',
      opacity: '0',
    },
    { offset: 0.6, transform: 'skewX(-20deg)', opacity: '1' },
    { offset: 0.8, transform: 'skewX(5deg)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  P5 = [
    {
      offset: 0,
      transform: 'translate3d(100%, 0, 0) skewX(-30deg)',
      opacity: '0',
    },
    { offset: 0.6, transform: 'skewX(20deg)', opacity: '1' },
    { offset: 0.8, transform: 'skewX(-5deg)' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  B5 = [
    { offset: 0, opacity: '1' },
    {
      offset: 1,
      transform: 'translate3d(-100%, 0, 0) skewX(-30deg)',
      opacity: '0',
    },
  ],
  F5 = [
    { offset: 0, opacity: '1' },
    {
      offset: 1,
      transform: 'translate3d(100%, 0, 0) skewX(30deg)',
      opacity: '0',
    },
  ],
  N5 = [
    { offset: 0, transform: 'rotate3d(0, 0, 1, -200deg)', opacity: '0' },
    { offset: 1, transform: 'translate3d(0, 0, 0)', opacity: '1' },
  ],
  U5 = [
    { offset: 0, transform: 'rotate3d(0, 0, 1, -45deg)', opacity: '0' },
    { offset: 1, transform: 'translate3d(0, 0, 0)', opacity: '1' },
  ],
  H5 = [
    { offset: 0, transform: 'rotate3d(0, 0, 1, 45deg)', opacity: '0' },
    { offset: 1, transform: 'translate3d(0, 0, 0)', opacity: '1' },
  ],
  V5 = [
    { offset: 0, transform: 'rotate3d(0, 0, 1, 45deg)', opacity: '0' },
    { offset: 1, transform: 'translate3d(0, 0, 0)', opacity: '1' },
  ],
  W5 = [
    { offset: 0, transform: 'rotate3d(0, 0, 1, -90deg)', opacity: '0' },
    { offset: 1, transform: 'translate3d(0, 0, 0)', opacity: '1' },
  ],
  q5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, transform: 'rotate3d(0, 0, 1, 200deg)', opacity: '0' },
  ],
  Y5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, transform: 'rotate3d(0, 0, 1, 45deg)', opacity: '0' },
  ],
  K5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, transform: 'rotate3d(0, 0, 1, -45deg)', opacity: '0' },
  ],
  G5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, transform: 'rotate3d(0, 0, 1, -45deg)', opacity: '0' },
  ],
  X5 = [
    { offset: 0, opacity: '1' },
    { offset: 1, transform: 'rotate3d(0, 0, 1, 90deg)', opacity: '0' },
  ],
  j5 = [
    { offset: 0, transform: 'translate3d(0, -100%, 0)', visibility: 'visible' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  Z5 = [
    { offset: 0, transform: 'translate3d(-100%, 0, 0)', visibility: 'visible' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  J5 = [
    { offset: 0, transform: 'translate3d(100%, 0, 0)', visibility: 'visible' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  Q5 = [
    { offset: 0, transform: 'translate3d(0, 100%, 0)', visibility: 'visible' },
    { offset: 1, transform: 'translate3d(0, 0, 0)' },
  ],
  tz = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    { offset: 1, visibility: 'hidden', transform: 'translate3d(0, 100%, 0)' },
  ],
  ez = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    { offset: 1, visibility: 'hidden', transform: 'translate3d(-100%, 0, 0)' },
  ],
  rz = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    { offset: 1, visibility: 'hidden', transform: 'translate3d(100%, 0, 0)' },
  ],
  iz = [
    { offset: 0, transform: 'translate3d(0, 0, 0)' },
    { offset: 1, visibility: 'hidden', transform: 'translate3d(0, -100%, 0)' },
  ],
  nz = [
    { offset: 0, easing: 'ease-in-out' },
    {
      offset: 0.2,
      transform: 'rotate3d(0, 0, 1, 80deg)',
      easing: 'ease-in-out',
    },
    {
      offset: 0.4,
      transform: 'rotate3d(0, 0, 1, 60deg)',
      easing: 'ease-in-out',
      opacity: '1',
    },
    {
      offset: 0.6,
      transform: 'rotate3d(0, 0, 1, 80deg)',
      easing: 'ease-in-out',
    },
    {
      offset: 0.8,
      transform: 'rotate3d(0, 0, 1, 60deg)',
      easing: 'ease-in-out',
      opacity: '1',
    },
    { offset: 1, transform: 'translate3d(0, 700px, 0)', opacity: '0' },
  ],
  oz = [
    {
      offset: 0,
      opacity: '0',
      transform: 'scale(0.1) rotate(30deg)',
      'transform-origin': 'center bottom',
    },
    { offset: 0.5, transform: 'rotate(-10deg)' },
    { offset: 0.7, transform: 'rotate(3deg)' },
    { offset: 1, opacity: '1', transform: 'scale(1)' },
  ],
  sz = [
    {
      offset: 0,
      opacity: '0',
      transform: 'translate3d(-100%, 0, 0) rotate3d(0, 0, 1, -120deg)',
    },
    { offset: 1, opacity: '1', transform: 'translate3d(0, 0, 0)' },
  ],
  az = [
    { offset: 0, opacity: '1' },
    {
      offset: 1,
      opacity: '0',
      transform: 'translate3d(100%, 0, 0) rotate3d(0, 0, 1, 120deg)',
    },
  ],
  lz = [
    { offset: 0, opacity: '0', transform: 'scale3d(0.3, 0.3, 0.3)' },
    { offset: 0.5, opacity: '1' },
  ],
  cz = [
    {
      offset: 0,
      opacity: '0',
      transform: 'scale3d(0.1, 0.1, 0.1) translate3d(0, -1000px, 0)',
      easing: 'cubic-bezier(0.55, 0.055, 0.675, 0.19)',
    },
    {
      offset: 0.6,
      opacity: '1',
      transform: 'scale3d(0.475, 0.475, 0.475) translate3d(0, 60px, 0)',
      easing: 'cubic-bezier(0.175, 0.885, 0.32, 1)',
    },
  ],
  dz = [
    {
      offset: 0,
      opacity: '0',
      transform: 'scale3d(0.1, 0.1, 0.1) translate3d(-1000px, 0, 0)',
      easing: 'cubic-bezier(0.55, 0.055, 0.675, 0.19)',
    },
    {
      offset: 0.6,
      opacity: '1',
      transform: 'scale3d(0.475, 0.475, 0.475) translate3d(10px, 0, 0)',
      easing: 'cubic-bezier(0.175, 0.885, 0.32, 1)',
    },
  ],
  hz = [
    {
      offset: 0,
      opacity: '0',
      transform: 'scale3d(0.1, 0.1, 0.1) translate3d(1000px, 0, 0)',
      easing: 'cubic-bezier(0.55, 0.055, 0.675, 0.19)',
    },
    {
      offset: 0.6,
      opacity: '1',
      transform: 'scale3d(0.475, 0.475, 0.475) translate3d(-10px, 0, 0)',
      easing: 'cubic-bezier(0.175, 0.885, 0.32, 1)',
    },
  ],
  uz = [
    {
      offset: 0,
      opacity: '0',
      transform: 'scale3d(0.1, 0.1, 0.1) translate3d(0, 1000px, 0)',
      easing: 'cubic-bezier(0.55, 0.055, 0.675, 0.19)',
    },
    {
      offset: 0.6,
      opacity: '1',
      transform: 'scale3d(0.475, 0.475, 0.475) translate3d(0, -60px, 0)',
      easing: 'cubic-bezier(0.175, 0.885, 0.32, 1)',
    },
  ],
  pz = [
    { offset: 0, opacity: '1' },
    { offset: 0.5, opacity: '0', transform: 'scale3d(0.3, 0.3, 0.3)' },
    { offset: 1, opacity: '0' },
  ],
  fz = [
    {
      offset: 0.4,
      opacity: '1',
      transform: 'scale3d(0.475, 0.475, 0.475) translate3d(0, -60px, 0)',
      easing: 'cubic-bezier(0.55, 0.055, 0.675, 0.19)',
    },
    {
      offset: 1,
      opacity: '0',
      transform: 'scale3d(0.1, 0.1, 0.1) translate3d(0, 2000px, 0)',
      easing: 'cubic-bezier(0.175, 0.885, 0.32, 1)',
    },
  ],
  gz = [
    {
      offset: 0.4,
      opacity: '1',
      transform: 'scale3d(0.475, 0.475, 0.475) translate3d(42px, 0, 0)',
    },
    {
      offset: 1,
      opacity: '0',
      transform: 'scale(0.1) translate3d(-2000px, 0, 0)',
    },
  ],
  mz = [
    {
      offset: 0.4,
      opacity: '1',
      transform: 'scale3d(0.475, 0.475, 0.475) translate3d(-42px, 0, 0)',
    },
    {
      offset: 1,
      opacity: '0',
      transform: 'scale(0.1) translate3d(2000px, 0, 0)',
    },
  ],
  vz = [
    {
      offset: 0.4,
      opacity: '1',
      transform: 'scale3d(0.475, 0.475, 0.475) translate3d(0, 60px, 0)',
      easing: 'cubic-bezier(0.55, 0.055, 0.675, 0.19)',
    },
    {
      offset: 1,
      opacity: '0',
      transform: 'scale3d(0.1, 0.1, 0.1) translate3d(0, -2000px, 0)',
      easing: 'cubic-bezier(0.175, 0.885, 0.32, 1)',
    },
  ],
  Tf = {
    linear: 'linear',
    ease: 'ease',
    easeIn: 'ease-in',
    easeOut: 'ease-out',
    easeInOut: 'ease-in-out',
    easeInSine: 'cubic-bezier(0.47, 0, 0.745, 0.715)',
    easeOutSine: 'cubic-bezier(0.39, 0.575, 0.565, 1)',
    easeInOutSine: 'cubic-bezier(0.445, 0.05, 0.55, 0.95)',
    easeInQuad: 'cubic-bezier(0.55, 0.085, 0.68, 0.53)',
    easeOutQuad: 'cubic-bezier(0.25, 0.46, 0.45, 0.94)',
    easeInOutQuad: 'cubic-bezier(0.455, 0.03, 0.515, 0.955)',
    easeInCubic: 'cubic-bezier(0.55, 0.055, 0.675, 0.19)',
    easeOutCubic: 'cubic-bezier(0.215, 0.61, 0.355, 1)',
    easeInOutCubic: 'cubic-bezier(0.645, 0.045, 0.355, 1)',
    easeInQuart: 'cubic-bezier(0.895, 0.03, 0.685, 0.22)',
    easeOutQuart: 'cubic-bezier(0.165, 0.84, 0.44, 1)',
    easeInOutQuart: 'cubic-bezier(0.77, 0, 0.175, 1)',
    easeInQuint: 'cubic-bezier(0.755, 0.05, 0.855, 0.06)',
    easeOutQuint: 'cubic-bezier(0.23, 1, 0.32, 1)',
    easeInOutQuint: 'cubic-bezier(0.86, 0, 0.07, 1)',
    easeInExpo: 'cubic-bezier(0.95, 0.05, 0.795, 0.035)',
    easeOutExpo: 'cubic-bezier(0.19, 1, 0.22, 1)',
    easeInOutExpo: 'cubic-bezier(1, 0, 0, 1)',
    easeInCirc: 'cubic-bezier(0.6, 0.04, 0.98, 0.335)',
    easeOutCirc: 'cubic-bezier(0.075, 0.82, 0.165, 1)',
    easeInOutCirc: 'cubic-bezier(0.785, 0.135, 0.15, 0.86)',
    easeInBack: 'cubic-bezier(0.6, -0.28, 0.735, 0.045)',
    easeOutBack: 'cubic-bezier(0.175, 0.885, 0.32, 1.275)',
    easeInOutBack: 'cubic-bezier(0.68, -0.55, 0.265, 1.55)',
  },
  bz = /* @__PURE__ */ Object.freeze(
    /* @__PURE__ */ Object.defineProperty(
      {
        __proto__: null,
        backInDown: US,
        backInLeft: HS,
        backInRight: VS,
        backInUp: WS,
        backOutDown: qS,
        backOutLeft: YS,
        backOutRight: KS,
        backOutUp: GS,
        bounce: AS,
        bounceIn: XS,
        bounceInDown: jS,
        bounceInLeft: ZS,
        bounceInRight: JS,
        bounceInUp: QS,
        bounceOut: t5,
        bounceOutDown: e5,
        bounceOutLeft: r5,
        bounceOutRight: i5,
        bounceOutUp: n5,
        easings: Tf,
        fadeIn: o5,
        fadeInBottomLeft: s5,
        fadeInBottomRight: a5,
        fadeInDown: l5,
        fadeInDownBig: c5,
        fadeInLeft: d5,
        fadeInLeftBig: h5,
        fadeInRight: u5,
        fadeInRightBig: p5,
        fadeInTopLeft: f5,
        fadeInTopRight: g5,
        fadeInUp: m5,
        fadeInUpBig: v5,
        fadeOut: b5,
        fadeOutBottomLeft: y5,
        fadeOutBottomRight: _5,
        fadeOutDown: w5,
        fadeOutDownBig: x5,
        fadeOutLeft: k5,
        fadeOutLeftBig: $5,
        fadeOutRight: C5,
        fadeOutRightBig: S5,
        fadeOutTopLeft: z5,
        fadeOutTopRight: A5,
        fadeOutUp: E5,
        fadeOutUpBig: T5,
        flash: ES,
        flip: I5,
        flipInX: O5,
        flipInY: L5,
        flipOutX: D5,
        flipOutY: M5,
        headShake: TS,
        heartBeat: IS,
        hinge: nz,
        jackInTheBox: oz,
        jello: OS,
        lightSpeedInLeft: R5,
        lightSpeedInRight: P5,
        lightSpeedOutLeft: B5,
        lightSpeedOutRight: F5,
        pulse: LS,
        rollIn: sz,
        rollOut: az,
        rotateIn: N5,
        rotateInDownLeft: U5,
        rotateInDownRight: H5,
        rotateInUpLeft: V5,
        rotateInUpRight: W5,
        rotateOut: q5,
        rotateOutDownLeft: Y5,
        rotateOutDownRight: K5,
        rotateOutUpLeft: G5,
        rotateOutUpRight: X5,
        rubberBand: DS,
        shake: MS,
        shakeX: RS,
        shakeY: PS,
        slideInDown: j5,
        slideInLeft: Z5,
        slideInRight: J5,
        slideInUp: Q5,
        slideOutDown: tz,
        slideOutLeft: ez,
        slideOutRight: rz,
        slideOutUp: iz,
        swing: BS,
        tada: FS,
        wobble: NS,
        zoomIn: lz,
        zoomInDown: cz,
        zoomInLeft: dz,
        zoomInRight: hz,
        zoomInUp: uz,
        zoomOut: pz,
        zoomOutDown: fz,
        zoomOutLeft: gz,
        zoomOutRight: mz,
        zoomOutUp: vz,
      },
      Symbol.toStringTag,
      { value: 'Module' },
    ),
  )
var yz = st`
  :host {
    display: contents;
  }
`,
  $e = class extends nt {
    constructor() {
      super(...arguments),
        (this.hasStarted = !1),
        (this.name = 'none'),
        (this.play = !1),
        (this.delay = 0),
        (this.direction = 'normal'),
        (this.duration = 1e3),
        (this.easing = 'linear'),
        (this.endDelay = 0),
        (this.fill = 'auto'),
        (this.iterations = 1 / 0),
        (this.iterationStart = 0),
        (this.playbackRate = 1),
        (this.handleAnimationFinish = () => {
          ;(this.play = !1), (this.hasStarted = !1), this.emit('sl-finish')
        }),
        (this.handleAnimationCancel = () => {
          ;(this.play = !1), (this.hasStarted = !1), this.emit('sl-cancel')
        })
    }
    /** Gets and sets the current animation time. */
    get currentTime() {
      var t, r
      return (r = (t = this.animation) == null ? void 0 : t.currentTime) != null
        ? r
        : 0
    }
    set currentTime(t) {
      this.animation && (this.animation.currentTime = t)
    }
    connectedCallback() {
      super.connectedCallback(), this.createAnimation()
    }
    disconnectedCallback() {
      super.disconnectedCallback(), this.destroyAnimation()
    }
    handleSlotChange() {
      this.destroyAnimation(), this.createAnimation()
    }
    async createAnimation() {
      var t, r
      const i = (t = Tf[this.easing]) != null ? t : this.easing,
        o = (r = this.keyframes) != null ? r : bz[this.name],
        d = (await this.defaultSlot).assignedElements()[0]
      return !d || !o
        ? !1
        : (this.destroyAnimation(),
          (this.animation = d.animate(o, {
            delay: this.delay,
            direction: this.direction,
            duration: this.duration,
            easing: i,
            endDelay: this.endDelay,
            fill: this.fill,
            iterationStart: this.iterationStart,
            iterations: this.iterations,
          })),
          (this.animation.playbackRate = this.playbackRate),
          this.animation.addEventListener('cancel', this.handleAnimationCancel),
          this.animation.addEventListener('finish', this.handleAnimationFinish),
          this.play
            ? ((this.hasStarted = !0), this.emit('sl-start'))
            : this.animation.pause(),
          !0)
    }
    destroyAnimation() {
      this.animation &&
        (this.animation.cancel(),
        this.animation.removeEventListener(
          'cancel',
          this.handleAnimationCancel,
        ),
        this.animation.removeEventListener(
          'finish',
          this.handleAnimationFinish,
        ),
        (this.hasStarted = !1))
    }
    handleAnimationChange() {
      this.hasUpdated && this.createAnimation()
    }
    handlePlayChange() {
      return this.animation
        ? (this.play &&
            !this.hasStarted &&
            ((this.hasStarted = !0), this.emit('sl-start')),
          this.play ? this.animation.play() : this.animation.pause(),
          !0)
        : !1
    }
    handlePlaybackRateChange() {
      this.animation && (this.animation.playbackRate = this.playbackRate)
    }
    /** Clears all keyframe effects caused by this animation and aborts its playback. */
    cancel() {
      var t
      ;(t = this.animation) == null || t.cancel()
    }
    /** Sets the playback time to the end of the animation corresponding to the current playback direction. */
    finish() {
      var t
      ;(t = this.animation) == null || t.finish()
    }
    render() {
      return C` <slot @slotchange=${this.handleSlotChange}></slot> `
    }
  }
$e.styles = [dt, yz]
c([ew('slot')], $e.prototype, 'defaultSlot', 2)
c([p()], $e.prototype, 'name', 2)
c([p({ type: Boolean, reflect: !0 })], $e.prototype, 'play', 2)
c([p({ type: Number })], $e.prototype, 'delay', 2)
c([p()], $e.prototype, 'direction', 2)
c([p({ type: Number })], $e.prototype, 'duration', 2)
c([p()], $e.prototype, 'easing', 2)
c([p({ attribute: 'end-delay', type: Number })], $e.prototype, 'endDelay', 2)
c([p()], $e.prototype, 'fill', 2)
c([p({ type: Number })], $e.prototype, 'iterations', 2)
c(
  [p({ attribute: 'iteration-start', type: Number })],
  $e.prototype,
  'iterationStart',
  2,
)
c([p({ attribute: !1 })], $e.prototype, 'keyframes', 2)
c(
  [p({ attribute: 'playback-rate', type: Number })],
  $e.prototype,
  'playbackRate',
  2,
)
c(
  [
    X([
      'name',
      'delay',
      'direction',
      'duration',
      'easing',
      'endDelay',
      'fill',
      'iterations',
      'iterationsStart',
      'keyframes',
    ]),
  ],
  $e.prototype,
  'handleAnimationChange',
  1,
)
c([X('play')], $e.prototype, 'handlePlayChange', 1)
c([X('playbackRate')], $e.prototype, 'handlePlaybackRateChange', 1)
$e.define('sl-animation')
const _z = `:host {
  --tag-background: var(--color-variant-lucid);
  --tag-color: var(--color-variant);
  --tag-font-family: var(--font-mono);
  --tag-font-size: var(--font-size);

  display: inline-flex;
  border-radius: var(--tag-radius);
}
[part='base'] {
  display: flex;
  background: var(--tag-background, var(--color-gray-5));
  font-size: var(--tag-font-size);
  line-height: 1;
  border-radius: var(--tag-radius);
  padding: var(--tag-padding-y) var(--tag-padding-x);
  text-align: center;
  white-space: nowrap;
}
:host([removable]) [part='base'] {
  padding-right: calc(var(--tag-padding-x) / 2);
}
.tag__remove {
  margin-left: 0.25em;
}
.select__tags > * {
  margin-right: 0.5em;
}
.select__tags > *:last-child {
  margin-right: 0;
}
`
class Ap extends dr(Kl, nn) {
  constructor() {
    super(),
      (this.size = Lt.M),
      (this.variant = $t.Neutral),
      (this.shape = Be.Round)
  }
  connectedCallback() {
    super.connectedCallback(),
      this.addEventListener('sl-remove', () => this.emit('remove'))
  }
}
W(Ap, 'styles', [
  Kl.styles,
  Bt(),
  aa('tag'),
  ge(),
  fi('tag'),
  je('tag', 1.25, 2),
  ft(_z),
]),
  W(Ap, 'properties', {
    ...Kl.properties,
    size: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
  })
const wz = `:host {
  --selector-height: auto;
  --selector-width: 100%;
  --selector-background: var(--color-white);

  display: inline-flex;
  border-radius: var(--selector-radius);
  font-family: var(--selector-font-family);
  text-align: var(--input-text-align, inherit);
}
[part='form-control'] {
  width: 100%;
  text-align: inherit;
  font-family: inherit;
}
[part='form-control-input'] .select {
  appearance: none;
  -webkit-appearance: none;
  text-decoration: none;
  text-align: inherit;
  border: 0;
  white-space: nowrap;
  cursor: pointer;
  width: var(--selector-width);
  height: var(--selector-height);
  font-size: var(--selector-font-size);
  border-radius: var(--selector-radius);
  color: var(--selector-color);
  background: var(--selector-background);
  box-shadow: var(--selector-box-shadow);
}
:host([error]) [part='form-control-input'] .select {
  color: var(--from-input-color-error);
}
[part='combobox'] {
  padding: var(--selector-padding-y) var(--selector-padding-x);
}
[part='listbox'] {
  margin-top: var(--step);
  background: var(--color-light);
  padding: var(--step) var(--step);
  box-shadow: var(--tooltip-shadow);
  border-radius: var(--tooltip-border-radius);
}
:host([listbox-mode='fit-content']) [part='listbox'] {
  display: inline-block;
}
:host([side='left']) [part='listbox'] {
  left: 0;
}
:host([side='right']) [part='listbox'] {
  right: 0;
}
:host([side='center']) [part='listbox'] {
  left: 50%;
  transform: translate3d(-50%, 0, 0);
}
[part='form-control-input'] .select::part(popup) {
  z-index: var(--layer-high);
}
.select__tags > * {
  margin-right: var(--step);
}
.select__tags > *:last-child {
  margin-right: 0;
}
.select__clear {
  margin-right: var(--step-2);
}
`,
  Ql = 'tbk-option',
  xz = ce({
    FitContent: 'fit-content',
    FitSelector: 'fit-selector',
  })
class Ep extends dr(Ct, nn) {
  constructor() {
    super()
    W(this, 'resizeObserver')
    ;(this.size = Lt.M),
      (this.side = Jt.Left),
      (this.listboxMode = xz.FitSelector),
      (this.variant = $t.Neutral),
      (this.shape = Be.Round),
      (this.hoist = !0),
      (this.disabled = !1),
      (this.required = !1),
      (this.error = !1),
      (this.getTag = i => C`
        <tbk-tag
          size="2xs"
          removable
        >
          ${i.getTextLabel(!0)}
        </tbk-tag>
      `)
  }
  connectedCallback() {
    super.connectedCallback(),
      this.addEventListener('sl-change', () => this.emit('change')),
      this.addEventListener('sl-input', () => this.emit('input')),
      this.addEventListener('sl-show', () => this.emit('show')),
      this.addEventListener('sl-hide', () => this.emit('hide'))
  }
  get elCombobox() {
    return this.renderRoot.querySelector('[part="combobox"]')
  }
  get elFocusable() {
    return this.elCombobox
  }
  // I have to override this method in order to render a tbk-tag instead of sl-tag for the +more
  get tags() {
    return this.selectedOptions.map((i, o) => {
      if (o < this.maxOptionsVisible || this.maxOptionsVisible <= 0) {
        const a = this.getTag(i, o)
        return C`<div @remove=${d => this.handleTagRemove(d, i)}>
          ${typeof a == 'string' ? Co(a) : a}
        </div>`
      } else if (o === this.maxOptionsVisible)
        return C`<tbk-tag size="2xs">+${
          this.selectedOptions.length - o
        }</tbk-tag>`
      return C``
    })
  }
  handleOptionClick(i) {
    const a = i.target.closest(Ql),
      d = this.value
    a &&
      !a.disabled &&
      (this.multiple
        ? this.toggleOptionSelection(a)
        : this.setSelectedOptions(a),
      this.updateComplete.then(() =>
        this.displayInput.focus({ preventScroll: !0 }),
      ),
      this.value !== d &&
        this.updateComplete.then(() => {
          this.emit('sl-input'), this.emit('sl-change')
        }),
      this.multiple ||
        (this.hide(), this.displayInput.focus({ preventScroll: !0 })))
  }
  handleDefaultSlotChange() {
    const i = this.getAllOptions(),
      o = Array.isArray(this.value) ? this.value : [this.value]
    customElements.get(Ql)
      ? this.setSelectedOptions(
          i.filter(a => o.includes(a.value)),
          !1,
        )
      : customElements
          .whenDefined(Ql)
          .then(() => this.handleDefaultSlotChange())
  }
  selectionChanged(i = !0) {
    var a
    const o = this.getAllOptions()
    if (((this.selectedOptions = o.filter(d => d.selected)), this.multiple))
      i && (this.value = this.selectedOptions.map(d => d.value)),
        this.placeholder && this.value.length === 0
          ? (this.displayLabel = '')
          : (this.displayLabel = this.localize.term(
              'numOptionsSelected',
              this.selectedOptions.length,
            ))
    else {
      const d = this.selectedOptions[0]
      i && (this.value = (d == null ? void 0 : d.value) ?? ''),
        (this.displayLabel =
          ((a = d == null ? void 0 : d.getTextLabel) == null
            ? void 0
            : a.call(d)) ?? '')
    }
    this.updateComplete.then(() => {
      this.formControlController.updateValidity()
    })
  }
  setSelectedOptions(i, o = !0) {
    const a = this.getAllOptions(),
      d = Array.isArray(i) ? i : [i]
    a.forEach(h => (h.selected = !1)),
      d.length && d.forEach(h => (h.selected = !0)),
      this.selectionChanged(o)
  }
  updateMinWidth() {
    var a
    let i = 0
    const o = document.createElement('div')
    ;(o.style.display = 'inline-block'),
      (a = this.parentElement) == null || a.appendChild(o),
      this.getAllOptions().forEach(d => {
        d.setAttribute('variant', this.variant),
          d.setAttribute('size', this.size),
          d.elSlotLabel.assignedNodes().forEach(m => {
            ;(o.innerHTML = m.textContent), (i = Math.max(i, o.clientWidth))
          })
      }),
      (this.elCombobox.style.minWidth = `${i}px`),
      o.parentElement.removeChild(o)
  }
  getAllOptions() {
    return [...this.querySelectorAll('tbk-option')]
  }
  getFirstOption() {
    return this.querySelector('tbk-option')
  }
  focus(i) {
    var o
    ;(o = this.displayInput) == null || o.focus(i)
  }
  blur() {
    var i
    ;(i = this.displayInput) == null || i.blur()
  }
}
W(Ep, 'styles', [
  Ct.styles,
  Bt(),
  on('.select--focused'),
  aa('selector'),
  ge(),
  fi('selector'),
  je('selector', 1.25, 2),
  ft(wz),
]),
  W(Ep, 'properties', {
    ...Ct.properties,
    side: { type: String, reflect: !0 },
    listboxMode: { type: String, reflect: !0, attribute: 'listbox-mode' },
    size: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    disabled: { type: Boolean, reflect: !0 },
    outline: { type: Boolean, reflect: !0 },
    shadow: { type: Boolean, reflect: !0 },
    required: { type: Boolean, reflect: !0 },
    error: { type: Boolean, reflect: !0 },
  })
const kz = `:host {
  --option-font-size: var(--font-size);
  --option-background: var(--color-variant-lucid);
  --option-text-align: var(--text-align);
  --option-box-shadow: var(--shadow, 0 0 0 0 transparent), var(--shadow-inset, 0 0 0 0 transparent);
}
[part='base'] {
  font-size: var(--option-font-size);
  padding: var(--option-padding-y) var(--option-padding-x);
  text-align: var(--option-text-align);
  border-radius: var(--option-radius);
}
[part='prefix'] {
  margin-right: var(--step);
}
[part='checked-icon'] {
  margin-right: var(--step);
}
:host(:hover) [part='base'] {
  cursor: pointer;
  background: var(--option-background);
}
`
class Tp extends dr(Ne) {
  constructor() {
    super(),
      (this.size = Lt.XS),
      (this.variant = $t.Neutral),
      (this.side = Jt.Left),
      (this.shape = Be.Round)
  }
  getTextLabel(r = !1) {
    const i = this.childNodes
    let o = ''
    return (
      [...i].forEach(a => {
        It(r) && a.nodeType === Node.ELEMENT_NODE && It && (o += a.textContent),
          a.nodeType === Node.TEXT_NODE && (o += a.textContent)
      }),
      o.trim()
    )
  }
}
W(Tp, 'styles', [
  Ne.styles,
  Bt(),
  on('.option--current'),
  ge(),
  Xr(),
  xc(),
  fi('option'),
  kk('[part="base"]'),
  je('option', 1.25, 2),
  ft(kz),
]),
  W(Tp, 'properties', {
    ...Ne.properties,
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    side: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
  })
const $z = `:host {
  --label-font-size: var(--font-size);
}
[part='base'] {
  color: var(--color-header);
  font-size: var(--label-font-size);
  display: flex;
  gap: var(--step);
  white-space: nowrap;
}
:host([orientation='horizontal']) [part='base'] {
  flex-direction: row;
  align-items: center;
  justify-content: flex-start;
}
:host([orientation='vertical']) [part='base'] {
  flex-direction: column;
  justify-content: center;
  align-items: flex-start;
}
:host([orientation='vertical']) {
  ::slotted([slot='label']) {
    display: block;
    margin-left: var(--step);
  }
}
slot:empty {
  position: absolute;
}
slot[name='label'] {
  display: block;
}
slot:not([name='label']) {
  font-weight: var(--text-semibold);
}
`
class Ip extends de {
  constructor() {
    super(),
      (this.text = ''),
      (this.for = ''),
      (this.size = Lt.M),
      (this.orientation = vc.Horizontal)
  }
  connectedCallback() {
    super.connectedCallback(),
      this.addEventListener('click', this._setFocusToRelatedInput.bind(this))
  }
  _setFocusToRelatedInput() {
    var r, i
    ;((r = this.querySelector(`#${this.for}`)) == null ? void 0 : r.focus()) ??
      ((i = this.parentElement.querySelector(`#${this.for}`)) == null ||
        i.focus())
  }
  render() {
    return C`
      <div part="base">
        <slot name="label"><label for="${this.for}">${this.text}</label></slot>
        <slot></slot>
      </div>
    `
  }
}
W(Ip, 'styles', [Bt(), ge(), ft($z)]),
  W(Ip, 'properties', {
    for: { type: String },
    text: { type: String },
    orientation: { type: String, reflect: !0 },
    size: { type: String, reflect: !0 },
  })
export {
  Fu as Badge,
  Wl as Button,
  dp as Datetime,
  np as Details,
  gp as FormField,
  mp as FormFieldset,
  Bu as Icon,
  fp as IconLoading,
  Gu as Information,
  bp as Input,
  Ip as Label,
  tp as Metadata,
  ep as MetadataItem,
  rp as MetadataSection,
  up as Metric,
  ju as ModelName,
  Tp as Option,
  Oz as ResizeObserver,
  _$ as Scroll,
  Ep as Selector,
  hp as SideNavigation,
  Ju as SourceList,
  Zu as SourceListItem,
  Qu as SourceListSection,
  ip as SplitPane,
  pp as StatModelFreshness,
  sp as Tab,
  cp as TabPanel,
  lp as Tabs,
  Xu as TextBlock,
  Ku as Tooltip,
}
