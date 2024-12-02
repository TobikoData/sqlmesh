var X0 = Object.defineProperty
var Z0 = (n, i, r) =>
  i in n
    ? X0(n, i, { enumerable: !0, configurable: !0, writable: !0, value: r })
    : (n[i] = r)
var Y = (n, i, r) => Z0(n, typeof i != 'symbol' ? i + '' : i, r)
/**
 * @license
 * Copyright 2019 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const fo = globalThis,
  Da =
    fo.ShadowRoot &&
    (fo.ShadyCSS === void 0 || fo.ShadyCSS.nativeShadow) &&
    'adoptedStyleSheets' in Document.prototype &&
    'replace' in CSSStyleSheet.prototype,
  Ba = Symbol(),
  zu = /* @__PURE__ */ new WeakMap()
let wh = class {
  constructor(i, r, a) {
    if (((this._$cssResult$ = !0), a !== Ba))
      throw Error(
        'CSSResult is not constructable. Use `unsafeCSS` or `css` instead.',
      )
    ;(this.cssText = i), (this.t = r)
  }
  get styleSheet() {
    let i = this.o
    const r = this.t
    if (Da && i === void 0) {
      const a = r !== void 0 && r.length === 1
      a && (i = zu.get(r)),
        i === void 0 &&
          ((this.o = i = new CSSStyleSheet()).replaceSync(this.cssText),
          a && zu.set(r, i))
    }
    return i
  }
  toString() {
    return this.cssText
  }
}
const pt = n => new wh(typeof n == 'string' ? n : n + '', void 0, Ba),
  en = (n, ...i) => {
    const r =
      n.length === 1
        ? n[0]
        : i.reduce(
            (a, l, h) =>
              a +
              (f => {
                if (f._$cssResult$ === !0) return f.cssText
                if (typeof f == 'number') return f
                throw Error(
                  "Value passed to 'css' function must be a 'css' function result: " +
                    f +
                    ". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.",
                )
              })(l) +
              n[h + 1],
            n[0],
          )
    return new wh(r, n, Ba)
  },
  j0 = (n, i) => {
    if (Da)
      n.adoptedStyleSheets = i.map(r =>
        r instanceof CSSStyleSheet ? r : r.styleSheet,
      )
    else
      for (const r of i) {
        const a = document.createElement('style'),
          l = fo.litNonce
        l !== void 0 && a.setAttribute('nonce', l),
          (a.textContent = r.cssText),
          n.appendChild(a)
      }
  },
  Pu = Da
    ? n => n
    : n =>
        n instanceof CSSStyleSheet
          ? (i => {
              let r = ''
              for (const a of i.cssRules) r += a.cssText
              return pt(r)
            })(n)
          : n
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const {
    is: J0,
    defineProperty: Q0,
    getOwnPropertyDescriptor: ty,
    getOwnPropertyNames: ey,
    getOwnPropertySymbols: ny,
    getPrototypeOf: ry,
  } = Object,
  zn = globalThis,
  Ru = zn.trustedTypes,
  iy = Ru ? Ru.emptyScript : '',
  ia = zn.reactiveElementPolyfillSupport,
  ei = (n, i) => n,
  bo = {
    toAttribute(n, i) {
      switch (i) {
        case Boolean:
          n = n ? iy : null
          break
        case Object:
        case Array:
          n = n == null ? n : JSON.stringify(n)
      }
      return n
    },
    fromAttribute(n, i) {
      let r = n
      switch (i) {
        case Boolean:
          r = n !== null
          break
        case Number:
          r = n === null ? null : Number(n)
          break
        case Object:
        case Array:
          try {
            r = JSON.parse(n)
          } catch {
            r = null
          }
      }
      return r
    },
  },
  Ua = (n, i) => !J0(n, i),
  Mu = {
    attribute: !0,
    type: String,
    converter: bo,
    reflect: !1,
    hasChanged: Ua,
  }
Symbol.metadata ?? (Symbol.metadata = Symbol('metadata')),
  zn.litPropertyMetadata ??
    (zn.litPropertyMetadata = /* @__PURE__ */ new WeakMap())
class br extends HTMLElement {
  static addInitializer(i) {
    this._$Ei(), (this.l ?? (this.l = [])).push(i)
  }
  static get observedAttributes() {
    return this.finalize(), this._$Eh && [...this._$Eh.keys()]
  }
  static createProperty(i, r = Mu) {
    if (
      (r.state && (r.attribute = !1),
      this._$Ei(),
      this.elementProperties.set(i, r),
      !r.noAccessor)
    ) {
      const a = Symbol(),
        l = this.getPropertyDescriptor(i, a, r)
      l !== void 0 && Q0(this.prototype, i, l)
    }
  }
  static getPropertyDescriptor(i, r, a) {
    const { get: l, set: h } = ty(this.prototype, i) ?? {
      get() {
        return this[r]
      },
      set(f) {
        this[r] = f
      },
    }
    return {
      get() {
        return l == null ? void 0 : l.call(this)
      },
      set(f) {
        const v = l == null ? void 0 : l.call(this)
        h.call(this, f), this.requestUpdate(i, v, a)
      },
      configurable: !0,
      enumerable: !0,
    }
  }
  static getPropertyOptions(i) {
    return this.elementProperties.get(i) ?? Mu
  }
  static _$Ei() {
    if (this.hasOwnProperty(ei('elementProperties'))) return
    const i = ry(this)
    i.finalize(),
      i.l !== void 0 && (this.l = [...i.l]),
      (this.elementProperties = new Map(i.elementProperties))
  }
  static finalize() {
    if (this.hasOwnProperty(ei('finalized'))) return
    if (
      ((this.finalized = !0),
      this._$Ei(),
      this.hasOwnProperty(ei('properties')))
    ) {
      const r = this.properties,
        a = [...ey(r), ...ny(r)]
      for (const l of a) this.createProperty(l, r[l])
    }
    const i = this[Symbol.metadata]
    if (i !== null) {
      const r = litPropertyMetadata.get(i)
      if (r !== void 0) for (const [a, l] of r) this.elementProperties.set(a, l)
    }
    this._$Eh = /* @__PURE__ */ new Map()
    for (const [r, a] of this.elementProperties) {
      const l = this._$Eu(r, a)
      l !== void 0 && this._$Eh.set(l, r)
    }
    this.elementStyles = this.finalizeStyles(this.styles)
  }
  static finalizeStyles(i) {
    const r = []
    if (Array.isArray(i)) {
      const a = new Set(i.flat(1 / 0).reverse())
      for (const l of a) r.unshift(Pu(l))
    } else i !== void 0 && r.push(Pu(i))
    return r
  }
  static _$Eu(i, r) {
    const a = r.attribute
    return a === !1
      ? void 0
      : typeof a == 'string'
      ? a
      : typeof i == 'string'
      ? i.toLowerCase()
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
    var i
    ;(this._$ES = new Promise(r => (this.enableUpdating = r))),
      (this._$AL = /* @__PURE__ */ new Map()),
      this._$E_(),
      this.requestUpdate(),
      (i = this.constructor.l) == null || i.forEach(r => r(this))
  }
  addController(i) {
    var r
    ;(this._$EO ?? (this._$EO = /* @__PURE__ */ new Set())).add(i),
      this.renderRoot !== void 0 &&
        this.isConnected &&
        ((r = i.hostConnected) == null || r.call(i))
  }
  removeController(i) {
    var r
    ;(r = this._$EO) == null || r.delete(i)
  }
  _$E_() {
    const i = /* @__PURE__ */ new Map(),
      r = this.constructor.elementProperties
    for (const a of r.keys())
      this.hasOwnProperty(a) && (i.set(a, this[a]), delete this[a])
    i.size > 0 && (this._$Ep = i)
  }
  createRenderRoot() {
    const i =
      this.shadowRoot ?? this.attachShadow(this.constructor.shadowRootOptions)
    return j0(i, this.constructor.elementStyles), i
  }
  connectedCallback() {
    var i
    this.renderRoot ?? (this.renderRoot = this.createRenderRoot()),
      this.enableUpdating(!0),
      (i = this._$EO) == null ||
        i.forEach(r => {
          var a
          return (a = r.hostConnected) == null ? void 0 : a.call(r)
        })
  }
  enableUpdating(i) {}
  disconnectedCallback() {
    var i
    ;(i = this._$EO) == null ||
      i.forEach(r => {
        var a
        return (a = r.hostDisconnected) == null ? void 0 : a.call(r)
      })
  }
  attributeChangedCallback(i, r, a) {
    this._$AK(i, a)
  }
  _$EC(i, r) {
    var h
    const a = this.constructor.elementProperties.get(i),
      l = this.constructor._$Eu(i, a)
    if (l !== void 0 && a.reflect === !0) {
      const f = (
        ((h = a.converter) == null ? void 0 : h.toAttribute) !== void 0
          ? a.converter
          : bo
      ).toAttribute(r, a.type)
      ;(this._$Em = i),
        f == null ? this.removeAttribute(l) : this.setAttribute(l, f),
        (this._$Em = null)
    }
  }
  _$AK(i, r) {
    var h
    const a = this.constructor,
      l = a._$Eh.get(i)
    if (l !== void 0 && this._$Em !== l) {
      const f = a.getPropertyOptions(l),
        v =
          typeof f.converter == 'function'
            ? { fromAttribute: f.converter }
            : ((h = f.converter) == null ? void 0 : h.fromAttribute) !== void 0
            ? f.converter
            : bo
      ;(this._$Em = l),
        (this[l] = v.fromAttribute(r, f.type)),
        (this._$Em = null)
    }
  }
  requestUpdate(i, r, a) {
    if (i !== void 0) {
      if (
        (a ?? (a = this.constructor.getPropertyOptions(i)),
        !(a.hasChanged ?? Ua)(this[i], r))
      )
        return
      this.P(i, r, a)
    }
    this.isUpdatePending === !1 && (this._$ES = this._$ET())
  }
  P(i, r, a) {
    this._$AL.has(i) || this._$AL.set(i, r),
      a.reflect === !0 &&
        this._$Em !== i &&
        (this._$Ej ?? (this._$Ej = /* @__PURE__ */ new Set())).add(i)
  }
  async _$ET() {
    this.isUpdatePending = !0
    try {
      await this._$ES
    } catch (r) {
      Promise.reject(r)
    }
    const i = this.scheduleUpdate()
    return i != null && (await i), !this.isUpdatePending
  }
  scheduleUpdate() {
    return this.performUpdate()
  }
  performUpdate() {
    var a
    if (!this.isUpdatePending) return
    if (!this.hasUpdated) {
      if (
        (this.renderRoot ?? (this.renderRoot = this.createRenderRoot()),
        this._$Ep)
      ) {
        for (const [h, f] of this._$Ep) this[h] = f
        this._$Ep = void 0
      }
      const l = this.constructor.elementProperties
      if (l.size > 0)
        for (const [h, f] of l)
          f.wrapped !== !0 ||
            this._$AL.has(h) ||
            this[h] === void 0 ||
            this.P(h, this[h], f)
    }
    let i = !1
    const r = this._$AL
    try {
      ;(i = this.shouldUpdate(r)),
        i
          ? (this.willUpdate(r),
            (a = this._$EO) == null ||
              a.forEach(l => {
                var h
                return (h = l.hostUpdate) == null ? void 0 : h.call(l)
              }),
            this.update(r))
          : this._$EU()
    } catch (l) {
      throw ((i = !1), this._$EU(), l)
    }
    i && this._$AE(r)
  }
  willUpdate(i) {}
  _$AE(i) {
    var r
    ;(r = this._$EO) == null ||
      r.forEach(a => {
        var l
        return (l = a.hostUpdated) == null ? void 0 : l.call(a)
      }),
      this.hasUpdated || ((this.hasUpdated = !0), this.firstUpdated(i)),
      this.updated(i)
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
  shouldUpdate(i) {
    return !0
  }
  update(i) {
    this._$Ej && (this._$Ej = this._$Ej.forEach(r => this._$EC(r, this[r]))),
      this._$EU()
  }
  updated(i) {}
  firstUpdated(i) {}
}
;(br.elementStyles = []),
  (br.shadowRootOptions = { mode: 'open' }),
  (br[ei('elementProperties')] = /* @__PURE__ */ new Map()),
  (br[ei('finalized')] = /* @__PURE__ */ new Map()),
  ia == null || ia({ ReactiveElement: br }),
  (zn.reactiveElementVersions ?? (zn.reactiveElementVersions = [])).push(
    '2.0.4',
  )
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const ni = globalThis,
  yo = ni.trustedTypes,
  Lu = yo ? yo.createPolicy('lit-html', { createHTML: n => n }) : void 0,
  $h = '$lit$',
  Tn = `lit$${Math.random().toFixed(9).slice(2)}$`,
  xh = '?' + Tn,
  oy = `<${xh}>`,
  Qn = document,
  ri = () => Qn.createComment(''),
  ii = n => n === null || (typeof n != 'object' && typeof n != 'function'),
  Na = Array.isArray,
  sy = n =>
    Na(n) || typeof (n == null ? void 0 : n[Symbol.iterator]) == 'function',
  oa = `[ 	
\f\r]`,
  Jr = /<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,
  Iu = /-->/g,
  Du = />/g,
  Vn = RegExp(
    `>|${oa}(?:([^\\s"'>=/]+)(${oa}*=${oa}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,
    'g',
  ),
  Bu = /'/g,
  Uu = /"/g,
  Sh = /^(?:script|style|textarea|title)$/i,
  ay =
    n =>
    (i, ...r) => ({ _$litType$: n, strings: i, values: r }),
  X = ay(1),
  tr = Symbol.for('lit-noChange'),
  Ft = Symbol.for('lit-nothing'),
  Nu = /* @__PURE__ */ new WeakMap(),
  Zn = Qn.createTreeWalker(Qn, 129)
function Ch(n, i) {
  if (!Na(n) || !n.hasOwnProperty('raw'))
    throw Error('invalid template strings array')
  return Lu !== void 0 ? Lu.createHTML(i) : i
}
const ly = (n, i) => {
  const r = n.length - 1,
    a = []
  let l,
    h = i === 2 ? '<svg>' : i === 3 ? '<math>' : '',
    f = Jr
  for (let v = 0; v < r; v++) {
    const m = n[v]
    let w,
      k,
      C = -1,
      U = 0
    for (; U < m.length && ((f.lastIndex = U), (k = f.exec(m)), k !== null); )
      (U = f.lastIndex),
        f === Jr
          ? k[1] === '!--'
            ? (f = Iu)
            : k[1] !== void 0
            ? (f = Du)
            : k[2] !== void 0
            ? (Sh.test(k[2]) && (l = RegExp('</' + k[2], 'g')), (f = Vn))
            : k[3] !== void 0 && (f = Vn)
          : f === Vn
          ? k[0] === '>'
            ? ((f = l ?? Jr), (C = -1))
            : k[1] === void 0
            ? (C = -2)
            : ((C = f.lastIndex - k[2].length),
              (w = k[1]),
              (f = k[3] === void 0 ? Vn : k[3] === '"' ? Uu : Bu))
          : f === Uu || f === Bu
          ? (f = Vn)
          : f === Iu || f === Du
          ? (f = Jr)
          : ((f = Vn), (l = void 0))
    const T = f === Vn && n[v + 1].startsWith('/>') ? ' ' : ''
    h +=
      f === Jr
        ? m + oy
        : C >= 0
        ? (a.push(w), m.slice(0, C) + $h + m.slice(C) + Tn + T)
        : m + Tn + (C === -2 ? v : T)
  }
  return [
    Ch(
      n,
      h + (n[r] || '<?>') + (i === 2 ? '</svg>' : i === 3 ? '</math>' : ''),
    ),
    a,
  ]
}
class oi {
  constructor({ strings: i, _$litType$: r }, a) {
    let l
    this.parts = []
    let h = 0,
      f = 0
    const v = i.length - 1,
      m = this.parts,
      [w, k] = ly(i, r)
    if (
      ((this.el = oi.createElement(w, a)),
      (Zn.currentNode = this.el.content),
      r === 2 || r === 3)
    ) {
      const C = this.el.content.firstChild
      C.replaceWith(...C.childNodes)
    }
    for (; (l = Zn.nextNode()) !== null && m.length < v; ) {
      if (l.nodeType === 1) {
        if (l.hasAttributes())
          for (const C of l.getAttributeNames())
            if (C.endsWith($h)) {
              const U = k[f++],
                T = l.getAttribute(C).split(Tn),
                O = /([.?@])?(.*)/.exec(U)
              m.push({
                type: 1,
                index: h,
                name: O[2],
                strings: T,
                ctor:
                  O[1] === '.'
                    ? uy
                    : O[1] === '?'
                    ? hy
                    : O[1] === '@'
                    ? dy
                    : Eo,
              }),
                l.removeAttribute(C)
            } else
              C.startsWith(Tn) &&
                (m.push({ type: 6, index: h }), l.removeAttribute(C))
        if (Sh.test(l.tagName)) {
          const C = l.textContent.split(Tn),
            U = C.length - 1
          if (U > 0) {
            l.textContent = yo ? yo.emptyScript : ''
            for (let T = 0; T < U; T++)
              l.append(C[T], ri()),
                Zn.nextNode(),
                m.push({ type: 2, index: ++h })
            l.append(C[U], ri())
          }
        }
      } else if (l.nodeType === 8)
        if (l.data === xh) m.push({ type: 2, index: h })
        else {
          let C = -1
          for (; (C = l.data.indexOf(Tn, C + 1)) !== -1; )
            m.push({ type: 7, index: h }), (C += Tn.length - 1)
        }
      h++
    }
  }
  static createElement(i, r) {
    const a = Qn.createElement('template')
    return (a.innerHTML = i), a
  }
}
function xr(n, i, r = n, a) {
  var f, v
  if (i === tr) return i
  let l = a !== void 0 ? ((f = r._$Co) == null ? void 0 : f[a]) : r._$Cl
  const h = ii(i) ? void 0 : i._$litDirective$
  return (
    (l == null ? void 0 : l.constructor) !== h &&
      ((v = l == null ? void 0 : l._$AO) == null || v.call(l, !1),
      h === void 0 ? (l = void 0) : ((l = new h(n)), l._$AT(n, r, a)),
      a !== void 0 ? ((r._$Co ?? (r._$Co = []))[a] = l) : (r._$Cl = l)),
    l !== void 0 && (i = xr(n, l._$AS(n, i.values), l, a)),
    i
  )
}
class cy {
  constructor(i, r) {
    ;(this._$AV = []), (this._$AN = void 0), (this._$AD = i), (this._$AM = r)
  }
  get parentNode() {
    return this._$AM.parentNode
  }
  get _$AU() {
    return this._$AM._$AU
  }
  u(i) {
    const {
        el: { content: r },
        parts: a,
      } = this._$AD,
      l = ((i == null ? void 0 : i.creationScope) ?? Qn).importNode(r, !0)
    Zn.currentNode = l
    let h = Zn.nextNode(),
      f = 0,
      v = 0,
      m = a[0]
    for (; m !== void 0; ) {
      if (f === m.index) {
        let w
        m.type === 2
          ? (w = new ai(h, h.nextSibling, this, i))
          : m.type === 1
          ? (w = new m.ctor(h, m.name, m.strings, this, i))
          : m.type === 6 && (w = new fy(h, this, i)),
          this._$AV.push(w),
          (m = a[++v])
      }
      f !== (m == null ? void 0 : m.index) && ((h = Zn.nextNode()), f++)
    }
    return (Zn.currentNode = Qn), l
  }
  p(i) {
    let r = 0
    for (const a of this._$AV)
      a !== void 0 &&
        (a.strings !== void 0
          ? (a._$AI(i, a, r), (r += a.strings.length - 2))
          : a._$AI(i[r])),
        r++
  }
}
class ai {
  get _$AU() {
    var i
    return ((i = this._$AM) == null ? void 0 : i._$AU) ?? this._$Cv
  }
  constructor(i, r, a, l) {
    ;(this.type = 2),
      (this._$AH = Ft),
      (this._$AN = void 0),
      (this._$AA = i),
      (this._$AB = r),
      (this._$AM = a),
      (this.options = l),
      (this._$Cv = (l == null ? void 0 : l.isConnected) ?? !0)
  }
  get parentNode() {
    let i = this._$AA.parentNode
    const r = this._$AM
    return (
      r !== void 0 &&
        (i == null ? void 0 : i.nodeType) === 11 &&
        (i = r.parentNode),
      i
    )
  }
  get startNode() {
    return this._$AA
  }
  get endNode() {
    return this._$AB
  }
  _$AI(i, r = this) {
    ;(i = xr(this, i, r)),
      ii(i)
        ? i === Ft || i == null || i === ''
          ? (this._$AH !== Ft && this._$AR(), (this._$AH = Ft))
          : i !== this._$AH && i !== tr && this._(i)
        : i._$litType$ !== void 0
        ? this.$(i)
        : i.nodeType !== void 0
        ? this.T(i)
        : sy(i)
        ? this.k(i)
        : this._(i)
  }
  O(i) {
    return this._$AA.parentNode.insertBefore(i, this._$AB)
  }
  T(i) {
    this._$AH !== i && (this._$AR(), (this._$AH = this.O(i)))
  }
  _(i) {
    this._$AH !== Ft && ii(this._$AH)
      ? (this._$AA.nextSibling.data = i)
      : this.T(Qn.createTextNode(i)),
      (this._$AH = i)
  }
  $(i) {
    var h
    const { values: r, _$litType$: a } = i,
      l =
        typeof a == 'number'
          ? this._$AC(i)
          : (a.el === void 0 &&
              (a.el = oi.createElement(Ch(a.h, a.h[0]), this.options)),
            a)
    if (((h = this._$AH) == null ? void 0 : h._$AD) === l) this._$AH.p(r)
    else {
      const f = new cy(l, this),
        v = f.u(this.options)
      f.p(r), this.T(v), (this._$AH = f)
    }
  }
  _$AC(i) {
    let r = Nu.get(i.strings)
    return r === void 0 && Nu.set(i.strings, (r = new oi(i))), r
  }
  k(i) {
    Na(this._$AH) || ((this._$AH = []), this._$AR())
    const r = this._$AH
    let a,
      l = 0
    for (const h of i)
      l === r.length
        ? r.push((a = new ai(this.O(ri()), this.O(ri()), this, this.options)))
        : (a = r[l]),
        a._$AI(h),
        l++
    l < r.length && (this._$AR(a && a._$AB.nextSibling, l), (r.length = l))
  }
  _$AR(i = this._$AA.nextSibling, r) {
    var a
    for (
      (a = this._$AP) == null ? void 0 : a.call(this, !1, !0, r);
      i && i !== this._$AB;

    ) {
      const l = i.nextSibling
      i.remove(), (i = l)
    }
  }
  setConnected(i) {
    var r
    this._$AM === void 0 &&
      ((this._$Cv = i), (r = this._$AP) == null || r.call(this, i))
  }
}
class Eo {
  get tagName() {
    return this.element.tagName
  }
  get _$AU() {
    return this._$AM._$AU
  }
  constructor(i, r, a, l, h) {
    ;(this.type = 1),
      (this._$AH = Ft),
      (this._$AN = void 0),
      (this.element = i),
      (this.name = r),
      (this._$AM = l),
      (this.options = h),
      a.length > 2 || a[0] !== '' || a[1] !== ''
        ? ((this._$AH = Array(a.length - 1).fill(new String())),
          (this.strings = a))
        : (this._$AH = Ft)
  }
  _$AI(i, r = this, a, l) {
    const h = this.strings
    let f = !1
    if (h === void 0)
      (i = xr(this, i, r, 0)),
        (f = !ii(i) || (i !== this._$AH && i !== tr)),
        f && (this._$AH = i)
    else {
      const v = i
      let m, w
      for (i = h[0], m = 0; m < h.length - 1; m++)
        (w = xr(this, v[a + m], r, m)),
          w === tr && (w = this._$AH[m]),
          f || (f = !ii(w) || w !== this._$AH[m]),
          w === Ft ? (i = Ft) : i !== Ft && (i += (w ?? '') + h[m + 1]),
          (this._$AH[m] = w)
    }
    f && !l && this.j(i)
  }
  j(i) {
    i === Ft
      ? this.element.removeAttribute(this.name)
      : this.element.setAttribute(this.name, i ?? '')
  }
}
class uy extends Eo {
  constructor() {
    super(...arguments), (this.type = 3)
  }
  j(i) {
    this.element[this.name] = i === Ft ? void 0 : i
  }
}
class hy extends Eo {
  constructor() {
    super(...arguments), (this.type = 4)
  }
  j(i) {
    this.element.toggleAttribute(this.name, !!i && i !== Ft)
  }
}
class dy extends Eo {
  constructor(i, r, a, l, h) {
    super(i, r, a, l, h), (this.type = 5)
  }
  _$AI(i, r = this) {
    if ((i = xr(this, i, r, 0) ?? Ft) === tr) return
    const a = this._$AH,
      l =
        (i === Ft && a !== Ft) ||
        i.capture !== a.capture ||
        i.once !== a.once ||
        i.passive !== a.passive,
      h = i !== Ft && (a === Ft || l)
    l && this.element.removeEventListener(this.name, this, a),
      h && this.element.addEventListener(this.name, this, i),
      (this._$AH = i)
  }
  handleEvent(i) {
    var r
    typeof this._$AH == 'function'
      ? this._$AH.call(
          ((r = this.options) == null ? void 0 : r.host) ?? this.element,
          i,
        )
      : this._$AH.handleEvent(i)
  }
}
class fy {
  constructor(i, r, a) {
    ;(this.element = i),
      (this.type = 6),
      (this._$AN = void 0),
      (this._$AM = r),
      (this.options = a)
  }
  get _$AU() {
    return this._$AM._$AU
  }
  _$AI(i) {
    xr(this, i)
  }
}
const sa = ni.litHtmlPolyfillSupport
sa == null || sa(oi, ai),
  (ni.litHtmlVersions ?? (ni.litHtmlVersions = [])).push('3.2.1')
const py = (n, i, r) => {
  const a = (r == null ? void 0 : r.renderBefore) ?? i
  let l = a._$litPart$
  if (l === void 0) {
    const h = (r == null ? void 0 : r.renderBefore) ?? null
    a._$litPart$ = l = new ai(i.insertBefore(ri(), h), h, void 0, r ?? {})
  }
  return l._$AI(n), l
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
let jn = class extends br {
  constructor() {
    super(...arguments),
      (this.renderOptions = { host: this }),
      (this._$Do = void 0)
  }
  createRenderRoot() {
    var r
    const i = super.createRenderRoot()
    return (
      (r = this.renderOptions).renderBefore ?? (r.renderBefore = i.firstChild),
      i
    )
  }
  update(i) {
    const r = this.render()
    this.hasUpdated || (this.renderOptions.isConnected = this.isConnected),
      super.update(i),
      (this._$Do = py(r, this.renderRoot, this.renderOptions))
  }
  connectedCallback() {
    var i
    super.connectedCallback(), (i = this._$Do) == null || i.setConnected(!0)
  }
  disconnectedCallback() {
    var i
    super.disconnectedCallback(), (i = this._$Do) == null || i.setConnected(!1)
  }
  render() {
    return tr
  }
}
var _h
;(jn._$litElement$ = !0),
  (jn.finalized = !0),
  (_h = globalThis.litElementHydrateSupport) == null ||
    _h.call(globalThis, { LitElement: jn })
const aa = globalThis.litElementPolyfillSupport
aa == null || aa({ LitElement: jn })
;(globalThis.litElementVersions ?? (globalThis.litElementVersions = [])).push(
  '4.1.1',
)
class _o extends Error {
  constructor(r = 'Invalid value', a) {
    super(r, a)
    Y(this, 'name', 'ValueError')
  }
}
var se =
  typeof globalThis < 'u'
    ? globalThis
    : typeof window < 'u'
    ? window
    : typeof global < 'u'
    ? global
    : typeof self < 'u'
    ? self
    : {}
function li(n) {
  return n && n.__esModule && Object.prototype.hasOwnProperty.call(n, 'default')
    ? n.default
    : n
}
var Ah = { exports: {} }
;(function (n, i) {
  ;(function (r, a) {
    n.exports = a()
  })(se, function () {
    var r = 1e3,
      a = 6e4,
      l = 36e5,
      h = 'millisecond',
      f = 'second',
      v = 'minute',
      m = 'hour',
      w = 'day',
      k = 'week',
      C = 'month',
      U = 'quarter',
      T = 'year',
      O = 'date',
      _ = 'Invalid Date',
      x =
        /^(\d{4})[-/]?(\d{1,2})?[-/]?(\d{0,2})[Tt\s]*(\d{1,2})?:?(\d{1,2})?:?(\d{1,2})?[.:]?(\d+)?$/,
      P =
        /\[([^\]]+)]|Y{1,4}|M{1,4}|D{1,2}|d{1,4}|H{1,2}|h{1,2}|a|A|m{1,2}|s{1,2}|Z{1,2}|SSS/g,
      G = {
        name: 'en',
        weekdays:
          'Sunday_Monday_Tuesday_Wednesday_Thursday_Friday_Saturday'.split('_'),
        months:
          'January_February_March_April_May_June_July_August_September_October_November_December'.split(
            '_',
          ),
        ordinal: function (W) {
          var D = ['th', 'st', 'nd', 'rd'],
            L = W % 100
          return '[' + W + (D[(L - 20) % 10] || D[L] || D[0]) + ']'
        },
      },
      N = function (W, D, L) {
        var H = String(W)
        return !H || H.length >= D
          ? W
          : '' + Array(D + 1 - H.length).join(L) + W
      },
      it = {
        s: N,
        z: function (W) {
          var D = -W.utcOffset(),
            L = Math.abs(D),
            H = Math.floor(L / 60),
            B = L % 60
          return (D <= 0 ? '+' : '-') + N(H, 2, '0') + ':' + N(B, 2, '0')
        },
        m: function W(D, L) {
          if (D.date() < L.date()) return -W(L, D)
          var H = 12 * (L.year() - D.year()) + (L.month() - D.month()),
            B = D.clone().add(H, C),
            at = L - B < 0,
            rt = D.clone().add(H + (at ? -1 : 1), C)
          return +(-(H + (L - B) / (at ? B - rt : rt - B)) || 0)
        },
        a: function (W) {
          return W < 0 ? Math.ceil(W) || 0 : Math.floor(W)
        },
        p: function (W) {
          return (
            { M: C, y: T, w: k, d: w, D: O, h: m, m: v, s: f, ms: h, Q: U }[
              W
            ] ||
            String(W || '')
              .toLowerCase()
              .replace(/s$/, '')
          )
        },
        u: function (W) {
          return W === void 0
        },
      },
      J = 'en',
      q = {}
    q[J] = G
    var I = '$isDayjsObject',
      M = function (W) {
        return W instanceof mt || !(!W || !W[I])
      },
      nt = function W(D, L, H) {
        var B
        if (!D) return J
        if (typeof D == 'string') {
          var at = D.toLowerCase()
          q[at] && (B = at), L && ((q[at] = L), (B = at))
          var rt = D.split('-')
          if (!B && rt.length > 1) return W(rt[0])
        } else {
          var dt = D.name
          ;(q[dt] = D), (B = dt)
        }
        return !H && B && (J = B), B || (!H && J)
      },
      ot = function (W, D) {
        if (M(W)) return W.clone()
        var L = typeof D == 'object' ? D : {}
        return (L.date = W), (L.args = arguments), new mt(L)
      },
      j = it
    ;(j.l = nt),
      (j.i = M),
      (j.w = function (W, D) {
        return ot(W, { locale: D.$L, utc: D.$u, x: D.$x, $offset: D.$offset })
      })
    var mt = (function () {
        function W(L) {
          ;(this.$L = nt(L.locale, null, !0)),
            this.parse(L),
            (this.$x = this.$x || L.x || {}),
            (this[I] = !0)
        }
        var D = W.prototype
        return (
          (D.parse = function (L) {
            ;(this.$d = (function (H) {
              var B = H.date,
                at = H.utc
              if (B === null) return /* @__PURE__ */ new Date(NaN)
              if (j.u(B)) return /* @__PURE__ */ new Date()
              if (B instanceof Date) return new Date(B)
              if (typeof B == 'string' && !/Z$/i.test(B)) {
                var rt = B.match(x)
                if (rt) {
                  var dt = rt[2] - 1 || 0,
                    Tt = (rt[7] || '0').substring(0, 3)
                  return at
                    ? new Date(
                        Date.UTC(
                          rt[1],
                          dt,
                          rt[3] || 1,
                          rt[4] || 0,
                          rt[5] || 0,
                          rt[6] || 0,
                          Tt,
                        ),
                      )
                    : new Date(
                        rt[1],
                        dt,
                        rt[3] || 1,
                        rt[4] || 0,
                        rt[5] || 0,
                        rt[6] || 0,
                        Tt,
                      )
                }
              }
              return new Date(B)
            })(L)),
              this.init()
          }),
          (D.init = function () {
            var L = this.$d
            ;(this.$y = L.getFullYear()),
              (this.$M = L.getMonth()),
              (this.$D = L.getDate()),
              (this.$W = L.getDay()),
              (this.$H = L.getHours()),
              (this.$m = L.getMinutes()),
              (this.$s = L.getSeconds()),
              (this.$ms = L.getMilliseconds())
          }),
          (D.$utils = function () {
            return j
          }),
          (D.isValid = function () {
            return this.$d.toString() !== _
          }),
          (D.isSame = function (L, H) {
            var B = ot(L)
            return this.startOf(H) <= B && B <= this.endOf(H)
          }),
          (D.isAfter = function (L, H) {
            return ot(L) < this.startOf(H)
          }),
          (D.isBefore = function (L, H) {
            return this.endOf(H) < ot(L)
          }),
          (D.$g = function (L, H, B) {
            return j.u(L) ? this[H] : this.set(B, L)
          }),
          (D.unix = function () {
            return Math.floor(this.valueOf() / 1e3)
          }),
          (D.valueOf = function () {
            return this.$d.getTime()
          }),
          (D.startOf = function (L, H) {
            var B = this,
              at = !!j.u(H) || H,
              rt = j.p(L),
              dt = function (he, Zt) {
                var de = j.w(
                  B.$u ? Date.UTC(B.$y, Zt, he) : new Date(B.$y, Zt, he),
                  B,
                )
                return at ? de : de.endOf(w)
              },
              Tt = function (he, Zt) {
                return j.w(
                  B.toDate()[he].apply(
                    B.toDate('s'),
                    (at ? [0, 0, 0, 0] : [23, 59, 59, 999]).slice(Zt),
                  ),
                  B,
                )
              },
              It = this.$W,
              Yt = this.$M,
              Bt = this.$D,
              Ie = 'set' + (this.$u ? 'UTC' : '')
            switch (rt) {
              case T:
                return at ? dt(1, 0) : dt(31, 11)
              case C:
                return at ? dt(1, Yt) : dt(0, Yt + 1)
              case k:
                var nn = this.$locale().weekStart || 0,
                  De = (It < nn ? It + 7 : It) - nn
                return dt(at ? Bt - De : Bt + (6 - De), Yt)
              case w:
              case O:
                return Tt(Ie + 'Hours', 0)
              case m:
                return Tt(Ie + 'Minutes', 1)
              case v:
                return Tt(Ie + 'Seconds', 2)
              case f:
                return Tt(Ie + 'Milliseconds', 3)
              default:
                return this.clone()
            }
          }),
          (D.endOf = function (L) {
            return this.startOf(L, !1)
          }),
          (D.$set = function (L, H) {
            var B,
              at = j.p(L),
              rt = 'set' + (this.$u ? 'UTC' : ''),
              dt = ((B = {}),
              (B[w] = rt + 'Date'),
              (B[O] = rt + 'Date'),
              (B[C] = rt + 'Month'),
              (B[T] = rt + 'FullYear'),
              (B[m] = rt + 'Hours'),
              (B[v] = rt + 'Minutes'),
              (B[f] = rt + 'Seconds'),
              (B[h] = rt + 'Milliseconds'),
              B)[at],
              Tt = at === w ? this.$D + (H - this.$W) : H
            if (at === C || at === T) {
              var It = this.clone().set(O, 1)
              It.$d[dt](Tt),
                It.init(),
                (this.$d = It.set(O, Math.min(this.$D, It.daysInMonth())).$d)
            } else dt && this.$d[dt](Tt)
            return this.init(), this
          }),
          (D.set = function (L, H) {
            return this.clone().$set(L, H)
          }),
          (D.get = function (L) {
            return this[j.p(L)]()
          }),
          (D.add = function (L, H) {
            var B,
              at = this
            L = Number(L)
            var rt = j.p(H),
              dt = function (Yt) {
                var Bt = ot(at)
                return j.w(Bt.date(Bt.date() + Math.round(Yt * L)), at)
              }
            if (rt === C) return this.set(C, this.$M + L)
            if (rt === T) return this.set(T, this.$y + L)
            if (rt === w) return dt(1)
            if (rt === k) return dt(7)
            var Tt = ((B = {}), (B[v] = a), (B[m] = l), (B[f] = r), B)[rt] || 1,
              It = this.$d.getTime() + L * Tt
            return j.w(It, this)
          }),
          (D.subtract = function (L, H) {
            return this.add(-1 * L, H)
          }),
          (D.format = function (L) {
            var H = this,
              B = this.$locale()
            if (!this.isValid()) return B.invalidDate || _
            var at = L || 'YYYY-MM-DDTHH:mm:ssZ',
              rt = j.z(this),
              dt = this.$H,
              Tt = this.$m,
              It = this.$M,
              Yt = B.weekdays,
              Bt = B.months,
              Ie = B.meridiem,
              nn = function (Zt, de, Ke, Bn) {
                return (Zt && (Zt[de] || Zt(H, at))) || Ke[de].slice(0, Bn)
              },
              De = function (Zt) {
                return j.s(dt % 12 || 12, Zt, '0')
              },
              he =
                Ie ||
                function (Zt, de, Ke) {
                  var Bn = Zt < 12 ? 'AM' : 'PM'
                  return Ke ? Bn.toLowerCase() : Bn
                }
            return at.replace(P, function (Zt, de) {
              return (
                de ||
                (function (Ke) {
                  switch (Ke) {
                    case 'YY':
                      return String(H.$y).slice(-2)
                    case 'YYYY':
                      return j.s(H.$y, 4, '0')
                    case 'M':
                      return It + 1
                    case 'MM':
                      return j.s(It + 1, 2, '0')
                    case 'MMM':
                      return nn(B.monthsShort, It, Bt, 3)
                    case 'MMMM':
                      return nn(Bt, It)
                    case 'D':
                      return H.$D
                    case 'DD':
                      return j.s(H.$D, 2, '0')
                    case 'd':
                      return String(H.$W)
                    case 'dd':
                      return nn(B.weekdaysMin, H.$W, Yt, 2)
                    case 'ddd':
                      return nn(B.weekdaysShort, H.$W, Yt, 3)
                    case 'dddd':
                      return Yt[H.$W]
                    case 'H':
                      return String(dt)
                    case 'HH':
                      return j.s(dt, 2, '0')
                    case 'h':
                      return De(1)
                    case 'hh':
                      return De(2)
                    case 'a':
                      return he(dt, Tt, !0)
                    case 'A':
                      return he(dt, Tt, !1)
                    case 'm':
                      return String(Tt)
                    case 'mm':
                      return j.s(Tt, 2, '0')
                    case 's':
                      return String(H.$s)
                    case 'ss':
                      return j.s(H.$s, 2, '0')
                    case 'SSS':
                      return j.s(H.$ms, 3, '0')
                    case 'Z':
                      return rt
                  }
                  return null
                })(Zt) ||
                rt.replace(':', '')
              )
            })
          }),
          (D.utcOffset = function () {
            return 15 * -Math.round(this.$d.getTimezoneOffset() / 15)
          }),
          (D.diff = function (L, H, B) {
            var at,
              rt = this,
              dt = j.p(H),
              Tt = ot(L),
              It = (Tt.utcOffset() - this.utcOffset()) * a,
              Yt = this - Tt,
              Bt = function () {
                return j.m(rt, Tt)
              }
            switch (dt) {
              case T:
                at = Bt() / 12
                break
              case C:
                at = Bt()
                break
              case U:
                at = Bt() / 3
                break
              case k:
                at = (Yt - It) / 6048e5
                break
              case w:
                at = (Yt - It) / 864e5
                break
              case m:
                at = Yt / l
                break
              case v:
                at = Yt / a
                break
              case f:
                at = Yt / r
                break
              default:
                at = Yt
            }
            return B ? at : j.a(at)
          }),
          (D.daysInMonth = function () {
            return this.endOf(C).$D
          }),
          (D.$locale = function () {
            return q[this.$L]
          }),
          (D.locale = function (L, H) {
            if (!L) return this.$L
            var B = this.clone(),
              at = nt(L, H, !0)
            return at && (B.$L = at), B
          }),
          (D.clone = function () {
            return j.w(this.$d, this)
          }),
          (D.toDate = function () {
            return new Date(this.valueOf())
          }),
          (D.toJSON = function () {
            return this.isValid() ? this.toISOString() : null
          }),
          (D.toISOString = function () {
            return this.$d.toISOString()
          }),
          (D.toString = function () {
            return this.$d.toUTCString()
          }),
          W
        )
      })(),
      Et = mt.prototype
    return (
      (ot.prototype = Et),
      [
        ['$ms', h],
        ['$s', f],
        ['$m', v],
        ['$H', m],
        ['$W', w],
        ['$M', C],
        ['$y', T],
        ['$D', O],
      ].forEach(function (W) {
        Et[W[1]] = function (D) {
          return this.$g(D, W[0], W[1])
        }
      }),
      (ot.extend = function (W, D) {
        return W.$i || (W(D, mt, ot), (W.$i = !0)), ot
      }),
      (ot.locale = nt),
      (ot.isDayjs = M),
      (ot.unix = function (W) {
        return ot(1e3 * W)
      }),
      (ot.en = q[J]),
      (ot.Ls = q),
      (ot.p = {}),
      ot
    )
  })
})(Ah)
var gy = Ah.exports
const ci = /* @__PURE__ */ li(gy)
var Eh = { exports: {} }
;(function (n, i) {
  ;(function (r, a) {
    n.exports = a()
  })(se, function () {
    var r = 'minute',
      a = /[+-]\d\d(?::?\d\d)?/g,
      l = /([+-]|\d\d)/g
    return function (h, f, v) {
      var m = f.prototype
      ;(v.utc = function (_) {
        var x = { date: _, utc: !0, args: arguments }
        return new f(x)
      }),
        (m.utc = function (_) {
          var x = v(this.toDate(), { locale: this.$L, utc: !0 })
          return _ ? x.add(this.utcOffset(), r) : x
        }),
        (m.local = function () {
          return v(this.toDate(), { locale: this.$L, utc: !1 })
        })
      var w = m.parse
      m.parse = function (_) {
        _.utc && (this.$u = !0),
          this.$utils().u(_.$offset) || (this.$offset = _.$offset),
          w.call(this, _)
      }
      var k = m.init
      m.init = function () {
        if (this.$u) {
          var _ = this.$d
          ;(this.$y = _.getUTCFullYear()),
            (this.$M = _.getUTCMonth()),
            (this.$D = _.getUTCDate()),
            (this.$W = _.getUTCDay()),
            (this.$H = _.getUTCHours()),
            (this.$m = _.getUTCMinutes()),
            (this.$s = _.getUTCSeconds()),
            (this.$ms = _.getUTCMilliseconds())
        } else k.call(this)
      }
      var C = m.utcOffset
      m.utcOffset = function (_, x) {
        var P = this.$utils().u
        if (P(_))
          return this.$u ? 0 : P(this.$offset) ? C.call(this) : this.$offset
        if (
          typeof _ == 'string' &&
          ((_ = (function (J) {
            J === void 0 && (J = '')
            var q = J.match(a)
            if (!q) return null
            var I = ('' + q[0]).match(l) || ['-', 0, 0],
              M = I[0],
              nt = 60 * +I[1] + +I[2]
            return nt === 0 ? 0 : M === '+' ? nt : -nt
          })(_)),
          _ === null)
        )
          return this
        var G = Math.abs(_) <= 16 ? 60 * _ : _,
          N = this
        if (x) return (N.$offset = G), (N.$u = _ === 0), N
        if (_ !== 0) {
          var it = this.$u
            ? this.toDate().getTimezoneOffset()
            : -1 * this.utcOffset()
          ;((N = this.local().add(G + it, r)).$offset = G),
            (N.$x.$localOffset = it)
        } else N = this.utc()
        return N
      }
      var U = m.format
      ;(m.format = function (_) {
        var x = _ || (this.$u ? 'YYYY-MM-DDTHH:mm:ss[Z]' : '')
        return U.call(this, x)
      }),
        (m.valueOf = function () {
          var _ = this.$utils().u(this.$offset)
            ? 0
            : this.$offset +
              (this.$x.$localOffset || this.$d.getTimezoneOffset())
          return this.$d.valueOf() - 6e4 * _
        }),
        (m.isUTC = function () {
          return !!this.$u
        }),
        (m.toISOString = function () {
          return this.toDate().toISOString()
        }),
        (m.toString = function () {
          return this.toDate().toUTCString()
        })
      var T = m.toDate
      m.toDate = function (_) {
        return _ === 's' && this.$offset
          ? v(this.format('YYYY-MM-DD HH:mm:ss:SSS')).toDate()
          : T.call(this)
      }
      var O = m.diff
      m.diff = function (_, x, P) {
        if (_ && this.$u === _.$u) return O.call(this, _, x, P)
        var G = this.local(),
          N = v(_).local()
        return O.call(G, N, x, P)
      }
    }
  })
})(Eh)
var vy = Eh.exports
const my = /* @__PURE__ */ li(vy)
var kh = { exports: {} }
;(function (n, i) {
  ;(function (r, a) {
    n.exports = a()
  })(se, function () {
    var r,
      a,
      l = 1e3,
      h = 6e4,
      f = 36e5,
      v = 864e5,
      m =
        /\[([^\]]+)]|Y{1,4}|M{1,4}|D{1,2}|d{1,4}|H{1,2}|h{1,2}|a|A|m{1,2}|s{1,2}|Z{1,2}|SSS/g,
      w = 31536e6,
      k = 2628e6,
      C =
        /^(-|\+)?P(?:([-+]?[0-9,.]*)Y)?(?:([-+]?[0-9,.]*)M)?(?:([-+]?[0-9,.]*)W)?(?:([-+]?[0-9,.]*)D)?(?:T(?:([-+]?[0-9,.]*)H)?(?:([-+]?[0-9,.]*)M)?(?:([-+]?[0-9,.]*)S)?)?$/,
      U = {
        years: w,
        months: k,
        days: v,
        hours: f,
        minutes: h,
        seconds: l,
        milliseconds: 1,
        weeks: 6048e5,
      },
      T = function (q) {
        return q instanceof it
      },
      O = function (q, I, M) {
        return new it(q, M, I.$l)
      },
      _ = function (q) {
        return a.p(q) + 's'
      },
      x = function (q) {
        return q < 0
      },
      P = function (q) {
        return x(q) ? Math.ceil(q) : Math.floor(q)
      },
      G = function (q) {
        return Math.abs(q)
      },
      N = function (q, I) {
        return q
          ? x(q)
            ? { negative: !0, format: '' + G(q) + I }
            : { negative: !1, format: '' + q + I }
          : { negative: !1, format: '' }
      },
      it = (function () {
        function q(M, nt, ot) {
          var j = this
          if (
            ((this.$d = {}),
            (this.$l = ot),
            M === void 0 && ((this.$ms = 0), this.parseFromMilliseconds()),
            nt)
          )
            return O(M * U[_(nt)], this)
          if (typeof M == 'number')
            return (this.$ms = M), this.parseFromMilliseconds(), this
          if (typeof M == 'object')
            return (
              Object.keys(M).forEach(function (W) {
                j.$d[_(W)] = M[W]
              }),
              this.calMilliseconds(),
              this
            )
          if (typeof M == 'string') {
            var mt = M.match(C)
            if (mt) {
              var Et = mt.slice(2).map(function (W) {
                return W != null ? Number(W) : 0
              })
              return (
                (this.$d.years = Et[0]),
                (this.$d.months = Et[1]),
                (this.$d.weeks = Et[2]),
                (this.$d.days = Et[3]),
                (this.$d.hours = Et[4]),
                (this.$d.minutes = Et[5]),
                (this.$d.seconds = Et[6]),
                this.calMilliseconds(),
                this
              )
            }
          }
          return this
        }
        var I = q.prototype
        return (
          (I.calMilliseconds = function () {
            var M = this
            this.$ms = Object.keys(this.$d).reduce(function (nt, ot) {
              return nt + (M.$d[ot] || 0) * U[ot]
            }, 0)
          }),
          (I.parseFromMilliseconds = function () {
            var M = this.$ms
            ;(this.$d.years = P(M / w)),
              (M %= w),
              (this.$d.months = P(M / k)),
              (M %= k),
              (this.$d.days = P(M / v)),
              (M %= v),
              (this.$d.hours = P(M / f)),
              (M %= f),
              (this.$d.minutes = P(M / h)),
              (M %= h),
              (this.$d.seconds = P(M / l)),
              (M %= l),
              (this.$d.milliseconds = M)
          }),
          (I.toISOString = function () {
            var M = N(this.$d.years, 'Y'),
              nt = N(this.$d.months, 'M'),
              ot = +this.$d.days || 0
            this.$d.weeks && (ot += 7 * this.$d.weeks)
            var j = N(ot, 'D'),
              mt = N(this.$d.hours, 'H'),
              Et = N(this.$d.minutes, 'M'),
              W = this.$d.seconds || 0
            this.$d.milliseconds &&
              ((W += this.$d.milliseconds / 1e3),
              (W = Math.round(1e3 * W) / 1e3))
            var D = N(W, 'S'),
              L =
                M.negative ||
                nt.negative ||
                j.negative ||
                mt.negative ||
                Et.negative ||
                D.negative,
              H = mt.format || Et.format || D.format ? 'T' : '',
              B =
                (L ? '-' : '') +
                'P' +
                M.format +
                nt.format +
                j.format +
                H +
                mt.format +
                Et.format +
                D.format
            return B === 'P' || B === '-P' ? 'P0D' : B
          }),
          (I.toJSON = function () {
            return this.toISOString()
          }),
          (I.format = function (M) {
            var nt = M || 'YYYY-MM-DDTHH:mm:ss',
              ot = {
                Y: this.$d.years,
                YY: a.s(this.$d.years, 2, '0'),
                YYYY: a.s(this.$d.years, 4, '0'),
                M: this.$d.months,
                MM: a.s(this.$d.months, 2, '0'),
                D: this.$d.days,
                DD: a.s(this.$d.days, 2, '0'),
                H: this.$d.hours,
                HH: a.s(this.$d.hours, 2, '0'),
                m: this.$d.minutes,
                mm: a.s(this.$d.minutes, 2, '0'),
                s: this.$d.seconds,
                ss: a.s(this.$d.seconds, 2, '0'),
                SSS: a.s(this.$d.milliseconds, 3, '0'),
              }
            return nt.replace(m, function (j, mt) {
              return mt || String(ot[j])
            })
          }),
          (I.as = function (M) {
            return this.$ms / U[_(M)]
          }),
          (I.get = function (M) {
            var nt = this.$ms,
              ot = _(M)
            return (
              ot === 'milliseconds'
                ? (nt %= 1e3)
                : (nt = ot === 'weeks' ? P(nt / U[ot]) : this.$d[ot]),
              nt || 0
            )
          }),
          (I.add = function (M, nt, ot) {
            var j
            return (
              (j = nt ? M * U[_(nt)] : T(M) ? M.$ms : O(M, this).$ms),
              O(this.$ms + j * (ot ? -1 : 1), this)
            )
          }),
          (I.subtract = function (M, nt) {
            return this.add(M, nt, !0)
          }),
          (I.locale = function (M) {
            var nt = this.clone()
            return (nt.$l = M), nt
          }),
          (I.clone = function () {
            return O(this.$ms, this)
          }),
          (I.humanize = function (M) {
            return r().add(this.$ms, 'ms').locale(this.$l).fromNow(!M)
          }),
          (I.valueOf = function () {
            return this.asMilliseconds()
          }),
          (I.milliseconds = function () {
            return this.get('milliseconds')
          }),
          (I.asMilliseconds = function () {
            return this.as('milliseconds')
          }),
          (I.seconds = function () {
            return this.get('seconds')
          }),
          (I.asSeconds = function () {
            return this.as('seconds')
          }),
          (I.minutes = function () {
            return this.get('minutes')
          }),
          (I.asMinutes = function () {
            return this.as('minutes')
          }),
          (I.hours = function () {
            return this.get('hours')
          }),
          (I.asHours = function () {
            return this.as('hours')
          }),
          (I.days = function () {
            return this.get('days')
          }),
          (I.asDays = function () {
            return this.as('days')
          }),
          (I.weeks = function () {
            return this.get('weeks')
          }),
          (I.asWeeks = function () {
            return this.as('weeks')
          }),
          (I.months = function () {
            return this.get('months')
          }),
          (I.asMonths = function () {
            return this.as('months')
          }),
          (I.years = function () {
            return this.get('years')
          }),
          (I.asYears = function () {
            return this.as('years')
          }),
          q
        )
      })(),
      J = function (q, I, M) {
        return q
          .add(I.years() * M, 'y')
          .add(I.months() * M, 'M')
          .add(I.days() * M, 'd')
          .add(I.hours() * M, 'h')
          .add(I.minutes() * M, 'm')
          .add(I.seconds() * M, 's')
          .add(I.milliseconds() * M, 'ms')
      }
    return function (q, I, M) {
      ;(r = M),
        (a = M().$utils()),
        (M.duration = function (j, mt) {
          var Et = M.locale()
          return O(j, { $l: Et }, mt)
        }),
        (M.isDuration = T)
      var nt = I.prototype.add,
        ot = I.prototype.subtract
      ;(I.prototype.add = function (j, mt) {
        return T(j) ? J(this, j, 1) : nt.bind(this)(j, mt)
      }),
        (I.prototype.subtract = function (j, mt) {
          return T(j) ? J(this, j, -1) : ot.bind(this)(j, mt)
        })
    }
  })
})(kh)
var by = kh.exports
const yy = /* @__PURE__ */ li(by)
var Th = { exports: {} }
;(function (n, i) {
  ;(function (r, a) {
    n.exports = a()
  })(se, function () {
    return function (r, a, l) {
      r = r || {}
      var h = a.prototype,
        f = {
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
      function v(w, k, C, U) {
        return h.fromToBase(w, k, C, U)
      }
      ;(l.en.relativeTime = f),
        (h.fromToBase = function (w, k, C, U, T) {
          for (
            var O,
              _,
              x,
              P = C.$locale().relativeTime || f,
              G = r.thresholds || [
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
              N = G.length,
              it = 0;
            it < N;
            it += 1
          ) {
            var J = G[it]
            J.d && (O = U ? l(w).diff(C, J.d, !0) : C.diff(w, J.d, !0))
            var q = (r.rounding || Math.round)(Math.abs(O))
            if (((x = O > 0), q <= J.r || !J.r)) {
              q <= 1 && it > 0 && (J = G[it - 1])
              var I = P[J.l]
              T && (q = T('' + q)),
                (_ =
                  typeof I == 'string' ? I.replace('%d', q) : I(q, k, J.l, x))
              break
            }
          }
          if (k) return _
          var M = x ? P.future : P.past
          return typeof M == 'function' ? M(_) : M.replace('%s', _)
        }),
        (h.to = function (w, k) {
          return v(w, k, this, !0)
        }),
        (h.from = function (w, k) {
          return v(w, k, this)
        })
      var m = function (w) {
        return w.$u ? l.utc() : l()
      }
      ;(h.toNow = function (w) {
        return this.to(m(this), w)
      }),
        (h.fromNow = function (w) {
          return this.from(m(this), w)
        })
    }
  })
})(Th)
var _y = Th.exports
const wy = /* @__PURE__ */ li(_y)
function $y(n) {
  throw new Error(
    'Could not dynamically require "' +
      n +
      '". Please configure the dynamicRequireTargets or/and ignoreDynamicRequires option of @rollup/plugin-commonjs appropriately for this require call to work.',
  )
}
var Oh = { exports: {} }
;(function (n, i) {
  ;(function (r, a) {
    typeof $y == 'function' ? (n.exports = a()) : (r.pluralize = a())
  })(se, function () {
    var r = [],
      a = [],
      l = {},
      h = {},
      f = {}
    function v(_) {
      return typeof _ == 'string' ? new RegExp('^' + _ + '$', 'i') : _
    }
    function m(_, x) {
      return _ === x
        ? x
        : _ === _.toLowerCase()
        ? x.toLowerCase()
        : _ === _.toUpperCase()
        ? x.toUpperCase()
        : _[0] === _[0].toUpperCase()
        ? x.charAt(0).toUpperCase() + x.substr(1).toLowerCase()
        : x.toLowerCase()
    }
    function w(_, x) {
      return _.replace(/\$(\d{1,2})/g, function (P, G) {
        return x[G] || ''
      })
    }
    function k(_, x) {
      return _.replace(x[0], function (P, G) {
        var N = w(x[1], arguments)
        return m(P === '' ? _[G - 1] : P, N)
      })
    }
    function C(_, x, P) {
      if (!_.length || l.hasOwnProperty(_)) return x
      for (var G = P.length; G--; ) {
        var N = P[G]
        if (N[0].test(x)) return k(x, N)
      }
      return x
    }
    function U(_, x, P) {
      return function (G) {
        var N = G.toLowerCase()
        return x.hasOwnProperty(N)
          ? m(G, N)
          : _.hasOwnProperty(N)
          ? m(G, _[N])
          : C(N, G, P)
      }
    }
    function T(_, x, P, G) {
      return function (N) {
        var it = N.toLowerCase()
        return x.hasOwnProperty(it)
          ? !0
          : _.hasOwnProperty(it)
          ? !1
          : C(it, it, P) === it
      }
    }
    function O(_, x, P) {
      var G = x === 1 ? O.singular(_) : O.plural(_)
      return (P ? x + ' ' : '') + G
    }
    return (
      (O.plural = U(f, h, r)),
      (O.isPlural = T(f, h, r)),
      (O.singular = U(h, f, a)),
      (O.isSingular = T(h, f, a)),
      (O.addPluralRule = function (_, x) {
        r.push([v(_), x])
      }),
      (O.addSingularRule = function (_, x) {
        a.push([v(_), x])
      }),
      (O.addUncountableRule = function (_) {
        if (typeof _ == 'string') {
          l[_.toLowerCase()] = !0
          return
        }
        O.addPluralRule(_, '$0'), O.addSingularRule(_, '$0')
      }),
      (O.addIrregularRule = function (_, x) {
        ;(x = x.toLowerCase()), (_ = _.toLowerCase()), (f[_] = x), (h[x] = _)
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
      ].forEach(function (_) {
        return O.addIrregularRule(_[0], _[1])
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
      ].forEach(function (_) {
        return O.addPluralRule(_[0], _[1])
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
      ].forEach(function (_) {
        return O.addSingularRule(_[0], _[1])
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
      ].forEach(O.addUncountableRule),
      O
    )
  })
})(Oh)
var xy = Oh.exports
const Sy = /* @__PURE__ */ li(xy)
ci.extend(my)
ci.extend(yy)
ci.extend(wy)
function Qe(n = Qe('"message" is required')) {
  throw new _o(n)
}
function wt(n) {
  return n === !1
}
function Cy(n) {
  return n === !0
}
function zh(n) {
  return typeof n == 'boolean'
}
function Ay(n) {
  return typeof n != 'boolean'
}
function Kt(n) {
  return [null, void 0].includes(n)
}
function wr(n) {
  return Ge(Kt, wt)(n)
}
function ko(n) {
  return n instanceof Element
}
function Ey(n) {
  return Ge(ko, wt)(n)
}
function In(n) {
  return typeof n == 'string'
}
function Ph(n) {
  return In(n) && n.trim() === ''
}
function ky(n) {
  return Kt(n) || Ph(n)
}
function Ty(n) {
  return In(n) && n.trim() !== ''
}
function Rh(n) {
  return n instanceof Date ? !0 : Ge(isNaN, wt)(new Date(n).getTime())
}
function Oy(n) {
  return Ge(Rh, wt)(n)
}
function zy(n) {
  return Ge(In, wt)(n)
}
function Fa(n) {
  return typeof n == 'function'
}
function ui(n) {
  return Array.isArray(n)
}
function Mh(n) {
  return ui(n) && n.length === 0
}
function hi(n) {
  return ui(n) && n.length > 0
}
function Py(n) {
  return Ge(ui, wt)(n)
}
function Ge(...n) {
  return function (r) {
    return n.reduce((a, l) => l(a), r)
  }
}
function Ha(n) {
  return typeof n == 'number' && Number.isFinite(n)
}
function Lh(n) {
  return Ge(Ha, wt)(n)
}
function Wa(n) {
  return ['string', 'number', 'boolean', 'symbol'].includes(typeof n)
}
function Ry(n) {
  return Ge(Wa, wt)(n)
}
function di(n) {
  return typeof n == 'object' && wr(n) && n.constructor === Object
}
function My(n) {
  return Ge(di, wt)(n)
}
function Ly(n) {
  return di(n) && Mh(Object.keys(n))
}
function Ih(n) {
  return di(n) && hi(Object.keys(n))
}
function Dh(n = 9) {
  const i = new Uint8Array(n)
  return (
    window.crypto.getRandomValues(i),
    Array.from(i, r => r.toString(36))
      .join('')
      .slice(0, n)
  )
}
function Iy(n = 0) {
  return Intl.NumberFormat('en-US', {
    notation: 'compact',
    compactDisplay: 'short',
  }).format(n)
}
function pa(n = 0, i = 'YYYY-MM-DD HH:mm:ss') {
  return ci.utc(n).format(i)
}
function ga(n = 0, i = 'YYYY-MM-DD HH:mm:ss') {
  return ci(n).format(i)
}
function Dy(n = 0, i = !0, r = 2) {
  let a = []
  if (n < 1e3) a = [[n, 'ms', 'millisecond']]
  else {
    const l = Math.floor(n / 1e3),
      h = Math.floor(l / 60),
      f = Math.floor(h / 60),
      v = Math.floor(f / 24),
      m = Math.floor(v / 30),
      w = Math.floor(m / 12),
      k = l % 60,
      C = h % 60,
      U = f % 24,
      T = v % 30,
      O = m % 12,
      _ = w
    a = [
      _ > 0 && [_, 'y', 'year'],
      O > 0 && [O, 'mo', 'month'],
      T > 0 && [T, 'd', 'day'],
      U > 0 && [U, 'h', 'hour'],
      C > 0 && [C, 'm', 'minute'],
      k > 0 && [k, 's', 'second'],
    ]
      .filter(Boolean)
      .filter((x, P) => P < r)
  }
  return a
    .map(([l, h, f]) => (i ? `${l}${h}` : Sy(f, l, !0)))
    .join(' ')
    .trim()
}
function zt(n, i = '') {
  return n ? i : ''
}
function By(n = '') {
  return n.charAt(0).toUpperCase() + n.slice(1)
}
function Uy(n = '', i = 0, r = 5, a = '...', l) {
  const h = n.length
  return (
    (r = Math.abs(r)),
    (l = Kt(l) ? r : Math.abs(l)),
    i > h || r + l >= h
      ? n
      : l === 0
      ? n.substring(0, r) + a
      : n.substring(0, r) + a + n.substring(h - l)
  )
}
function qa(n, i = Error) {
  return n instanceof i
}
function Ny(
  n = Qe('Provide onError callback'),
  i = Qe('Provide onSuccess callback'),
) {
  return r => (qa(r) ? n(r) : i(r))
}
function ae(n, i = 'Invalid value') {
  if (Kt(n) || wt(n)) throw new _o(i, qa(i) ? { cause: i } : void 0)
  return !0
}
function Bh(n) {
  return Kt(n) ? [] : ui(n) ? n : [n]
}
function Fy(n, i = '') {
  return In(n) ? n : i
}
function Hy(n, i = !1) {
  return zh(n) ? n : i
}
const Wy = /* @__PURE__ */ Object.freeze(
  /* @__PURE__ */ Object.defineProperty(
    {
      __proto__: null,
      assert: ae,
      capitalize: By,
      ensureArray: Bh,
      ensureBoolean: Hy,
      ensureString: Fy,
      isArray: ui,
      isBoolean: zh,
      isDate: Rh,
      isElement: ko,
      isEmptyArray: Mh,
      isEmptyObject: Ly,
      isError: qa,
      isFalse: wt,
      isFunction: Fa,
      isNil: Kt,
      isNumber: Ha,
      isObject: di,
      isPrimitive: Wa,
      isString: In,
      isStringEmpty: Ph,
      isStringEmptyOrNil: ky,
      isTrue: Cy,
      maybeError: Ny,
      maybeHTML: zt,
      nonEmptyArray: hi,
      nonEmptyObject: Ih,
      nonEmptyString: Ty,
      notArray: Py,
      notBoolean: Ay,
      notDate: Oy,
      notElement: Ey,
      notNil: wr,
      notNumber: Lh,
      notObject: My,
      notPrimitive: Ry,
      notString: zy,
      pipe: Ge,
      required: Qe,
      toCompactShortNumber: Iy,
      toDuration: Dy,
      toFormattedDateLocal: ga,
      toFormattedDateUTC: pa,
      truncate: Uy,
      uid: Dh,
    },
    Symbol.toStringTag,
    { value: 'Module' },
  ),
)
class qy {
  constructor(i = Qe('EnumValue "key" is required'), r, a) {
    ;(this.key = i), (this.value = r), (this.title = a ?? r ?? this.value)
  }
}
function qt(n = Qe('"obj" is required to create a new Enum')) {
  ae(Ih(n), 'Enum values cannot be empty')
  const i = Object.assign({}, n),
    r = {
      includes: l,
      throwOnMiss: h,
      title: f,
      forEach: k,
      value: v,
      keys: C,
      values: U,
      item: w,
      key: m,
      items: T,
      entries: O,
      getValue: _,
    }
  for (const [x, P] of Object.entries(i)) {
    ae(In(x) && Lh(parseInt(x)), `Key "${x}" is invalid`)
    const G = Bh(P)
    ae(
      G.every(N => Kt(N) || Wa(N)),
      `Value "${P}" is invalid`,
    ) && (i[x] = new qy(x, ...G))
  }
  const a = new Proxy(Object.preventExtensions(i), {
    get(x, P) {
      return P in r ? r[P] : Reflect.get(x, P).value
    },
    set() {
      throw new _o('Cannot change enum property')
    },
    deleteProperty() {
      throw new _o('Cannot delete enum property')
    },
  })
  function l(x) {
    return !!C().find(P => v(P) === x)
  }
  function h(x) {
    ae(l(x), `Value "${x}" does not exist in enum`)
  }
  function f(x) {
    var P
    return (P = T().find(G => G.value === x)) == null ? void 0 : P.title
  }
  function v(x) {
    return a[x]
  }
  function m(x) {
    var P
    return (P = T().find(G => G.value === x || G.title === x)) == null
      ? void 0
      : P.key
  }
  function w(x) {
    return i[x]
  }
  function k(x) {
    C().forEach(P => x(i[P]))
  }
  function C() {
    return Object.keys(i)
  }
  function U() {
    return C().map(x => v(x))
  }
  function T() {
    return C().map(x => w(x))
  }
  function O() {
    return C().map((x, P) => [x, v(x), w(x), P])
  }
  function _(x) {
    return v(m(x))
  }
  return a
}
qt({
  Complete: ['complete', 'Complete'],
  Failed: ['failed', 'Failed'],
  Behind: ['behind', 'Behind'],
  Progress: ['progress', 'Progress'],
  InProgress: ['in progress', 'In Progress'],
  Pending: ['pending', 'Pending'],
  Skipped: ['skipped', 'Skipped'],
  Undefined: ['undefined', 'Undefined'],
})
const mr = qt({
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
  Yy = qt({
    Base: 'base',
    Content: 'content',
    Tagline: 'tagline',
    Before: 'before',
    After: 'after',
    Info: 'info',
    Nav: 'nav',
    Default: void 0,
  }),
  Fu = qt({
    Active: 'active',
    Disabled: 'disabled',
    Open: 'open',
    Closed: 'closed',
  })
qt({
  Form: 'FORM',
})
const lo = qt({
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
  Gy = qt({
    Horizontal: 'horizontal',
    Vertical: 'vertical',
  }),
  Lt = qt({
    XXS: '2xs',
    XS: 'xs',
    S: 's',
    M: 'm',
    L: 'l',
    XL: 'xl',
    XXL: '2xl',
  }),
  Je = qt({
    Left: 'left',
    Right: 'right',
    Center: 'center',
  }),
  Ky = qt({
    Left: 'left',
    Right: 'right',
    Top: 'top',
    Bottom: 'bottom',
  }),
  je = qt({
    Round: 'round',
    Pill: 'pill',
    Square: 'square',
    Circle: 'circle',
    Rect: 'rect',
  }),
  xt = qt({
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
  Hu = qt({
    Auto: 'auto',
    Full: 'full',
    Wide: 'wide',
    Compact: 'compact',
  }),
  Vy = qt({
    Auto: 'auto',
    Full: 'full',
    Tall: 'tall',
    Short: 'short',
  }),
  yr = Object.freeze({
    ...Object.entries(Yy).reduce(
      (n, [i, r]) => ((n[`Part${i}`] = Wu('part', r)), n),
      {},
    ),
    SlotDefault: Wu('slot:not([name])'),
  })
function Wu(n = Qe('"name" is required to create a selector'), i) {
  return Kt(i) ? n : `[${n}="${i}"]`
}
var Xy = typeof se == 'object' && se && se.Object === Object && se,
  Zy = Xy,
  jy = Zy,
  Jy = typeof self == 'object' && self && self.Object === Object && self,
  Qy = jy || Jy || Function('return this')(),
  Ya = Qy,
  t1 = Ya,
  e1 = t1.Symbol,
  Ga = e1,
  qu = Ga,
  Uh = Object.prototype,
  n1 = Uh.hasOwnProperty,
  r1 = Uh.toString,
  Qr = qu ? qu.toStringTag : void 0
function i1(n) {
  var i = n1.call(n, Qr),
    r = n[Qr]
  try {
    n[Qr] = void 0
    var a = !0
  } catch {}
  var l = r1.call(n)
  return a && (i ? (n[Qr] = r) : delete n[Qr]), l
}
var o1 = i1,
  s1 = Object.prototype,
  a1 = s1.toString
function l1(n) {
  return a1.call(n)
}
var c1 = l1,
  Yu = Ga,
  u1 = o1,
  h1 = c1,
  d1 = '[object Null]',
  f1 = '[object Undefined]',
  Gu = Yu ? Yu.toStringTag : void 0
function p1(n) {
  return n == null
    ? n === void 0
      ? f1
      : d1
    : Gu && Gu in Object(n)
    ? u1(n)
    : h1(n)
}
var g1 = p1
function v1(n) {
  var i = typeof n
  return n != null && (i == 'object' || i == 'function')
}
var Nh = v1,
  m1 = g1,
  b1 = Nh,
  y1 = '[object AsyncFunction]',
  _1 = '[object Function]',
  w1 = '[object GeneratorFunction]',
  $1 = '[object Proxy]'
function x1(n) {
  if (!b1(n)) return !1
  var i = m1(n)
  return i == _1 || i == w1 || i == y1 || i == $1
}
var S1 = x1,
  C1 = Ya,
  A1 = C1['__core-js_shared__'],
  E1 = A1,
  la = E1,
  Ku = (function () {
    var n = /[^.]+$/.exec((la && la.keys && la.keys.IE_PROTO) || '')
    return n ? 'Symbol(src)_1.' + n : ''
  })()
function k1(n) {
  return !!Ku && Ku in n
}
var T1 = k1,
  O1 = Function.prototype,
  z1 = O1.toString
function P1(n) {
  if (n != null) {
    try {
      return z1.call(n)
    } catch {}
    try {
      return n + ''
    } catch {}
  }
  return ''
}
var R1 = P1,
  M1 = S1,
  L1 = T1,
  I1 = Nh,
  D1 = R1,
  B1 = /[\\^$.*+?()[\]{}|]/g,
  U1 = /^\[object .+?Constructor\]$/,
  N1 = Function.prototype,
  F1 = Object.prototype,
  H1 = N1.toString,
  W1 = F1.hasOwnProperty,
  q1 = RegExp(
    '^' +
      H1.call(W1)
        .replace(B1, '\\$&')
        .replace(
          /hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g,
          '$1.*?',
        ) +
      '$',
  )
function Y1(n) {
  if (!I1(n) || L1(n)) return !1
  var i = M1(n) ? q1 : U1
  return i.test(D1(n))
}
var G1 = Y1
function K1(n, i) {
  return n == null ? void 0 : n[i]
}
var V1 = K1,
  X1 = G1,
  Z1 = V1
function j1(n, i) {
  var r = Z1(n, i)
  return X1(r) ? r : void 0
}
var Ka = j1,
  J1 = Ka
;(function () {
  try {
    var n = J1(Object, 'defineProperty')
    return n({}, '', {}), n
  } catch {}
})()
function Q1(n, i) {
  return n === i || (n !== n && i !== i)
}
var t_ = Q1,
  e_ = Ka,
  n_ = e_(Object, 'create'),
  To = n_,
  Vu = To
function r_() {
  ;(this.__data__ = Vu ? Vu(null) : {}), (this.size = 0)
}
var i_ = r_
function o_(n) {
  var i = this.has(n) && delete this.__data__[n]
  return (this.size -= i ? 1 : 0), i
}
var s_ = o_,
  a_ = To,
  l_ = '__lodash_hash_undefined__',
  c_ = Object.prototype,
  u_ = c_.hasOwnProperty
function h_(n) {
  var i = this.__data__
  if (a_) {
    var r = i[n]
    return r === l_ ? void 0 : r
  }
  return u_.call(i, n) ? i[n] : void 0
}
var d_ = h_,
  f_ = To,
  p_ = Object.prototype,
  g_ = p_.hasOwnProperty
function v_(n) {
  var i = this.__data__
  return f_ ? i[n] !== void 0 : g_.call(i, n)
}
var m_ = v_,
  b_ = To,
  y_ = '__lodash_hash_undefined__'
function __(n, i) {
  var r = this.__data__
  return (
    (this.size += this.has(n) ? 0 : 1),
    (r[n] = b_ && i === void 0 ? y_ : i),
    this
  )
}
var w_ = __,
  $_ = i_,
  x_ = s_,
  S_ = d_,
  C_ = m_,
  A_ = w_
function Ar(n) {
  var i = -1,
    r = n == null ? 0 : n.length
  for (this.clear(); ++i < r; ) {
    var a = n[i]
    this.set(a[0], a[1])
  }
}
Ar.prototype.clear = $_
Ar.prototype.delete = x_
Ar.prototype.get = S_
Ar.prototype.has = C_
Ar.prototype.set = A_
var E_ = Ar
function k_() {
  ;(this.__data__ = []), (this.size = 0)
}
var T_ = k_,
  O_ = t_
function z_(n, i) {
  for (var r = n.length; r--; ) if (O_(n[r][0], i)) return r
  return -1
}
var Oo = z_,
  P_ = Oo,
  R_ = Array.prototype,
  M_ = R_.splice
function L_(n) {
  var i = this.__data__,
    r = P_(i, n)
  if (r < 0) return !1
  var a = i.length - 1
  return r == a ? i.pop() : M_.call(i, r, 1), --this.size, !0
}
var I_ = L_,
  D_ = Oo
function B_(n) {
  var i = this.__data__,
    r = D_(i, n)
  return r < 0 ? void 0 : i[r][1]
}
var U_ = B_,
  N_ = Oo
function F_(n) {
  return N_(this.__data__, n) > -1
}
var H_ = F_,
  W_ = Oo
function q_(n, i) {
  var r = this.__data__,
    a = W_(r, n)
  return a < 0 ? (++this.size, r.push([n, i])) : (r[a][1] = i), this
}
var Y_ = q_,
  G_ = T_,
  K_ = I_,
  V_ = U_,
  X_ = H_,
  Z_ = Y_
function Er(n) {
  var i = -1,
    r = n == null ? 0 : n.length
  for (this.clear(); ++i < r; ) {
    var a = n[i]
    this.set(a[0], a[1])
  }
}
Er.prototype.clear = G_
Er.prototype.delete = K_
Er.prototype.get = V_
Er.prototype.has = X_
Er.prototype.set = Z_
var j_ = Er,
  J_ = Ka,
  Q_ = Ya,
  tw = J_(Q_, 'Map'),
  ew = tw,
  Xu = E_,
  nw = j_,
  rw = ew
function iw() {
  ;(this.size = 0),
    (this.__data__ = {
      hash: new Xu(),
      map: new (rw || nw)(),
      string: new Xu(),
    })
}
var ow = iw
function sw(n) {
  var i = typeof n
  return i == 'string' || i == 'number' || i == 'symbol' || i == 'boolean'
    ? n !== '__proto__'
    : n === null
}
var aw = sw,
  lw = aw
function cw(n, i) {
  var r = n.__data__
  return lw(i) ? r[typeof i == 'string' ? 'string' : 'hash'] : r.map
}
var zo = cw,
  uw = zo
function hw(n) {
  var i = uw(this, n).delete(n)
  return (this.size -= i ? 1 : 0), i
}
var dw = hw,
  fw = zo
function pw(n) {
  return fw(this, n).get(n)
}
var gw = pw,
  vw = zo
function mw(n) {
  return vw(this, n).has(n)
}
var bw = mw,
  yw = zo
function _w(n, i) {
  var r = yw(this, n),
    a = r.size
  return r.set(n, i), (this.size += r.size == a ? 0 : 1), this
}
var ww = _w,
  $w = ow,
  xw = dw,
  Sw = gw,
  Cw = bw,
  Aw = ww
function kr(n) {
  var i = -1,
    r = n == null ? 0 : n.length
  for (this.clear(); ++i < r; ) {
    var a = n[i]
    this.set(a[0], a[1])
  }
}
kr.prototype.clear = $w
kr.prototype.delete = xw
kr.prototype.get = Sw
kr.prototype.has = Cw
kr.prototype.set = Aw
var Ew = kr,
  Fh = Ew,
  kw = 'Expected a function'
function Va(n, i) {
  if (typeof n != 'function' || (i != null && typeof i != 'function'))
    throw new TypeError(kw)
  var r = function () {
    var a = arguments,
      l = i ? i.apply(this, a) : a[0],
      h = r.cache
    if (h.has(l)) return h.get(l)
    var f = n.apply(this, a)
    return (r.cache = h.set(l, f) || h), f
  }
  return (r.cache = new (Va.Cache || Fh)()), r
}
Va.Cache = Fh
var Tw = Va,
  Ow = Tw,
  zw = 500
function Pw(n) {
  var i = Ow(n, function (a) {
      return r.size === zw && r.clear(), a
    }),
    r = i.cache
  return i
}
var Rw = Pw,
  Mw = Rw,
  Lw =
    /[^.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\\]|\\.)*?)\2)\]|(?=(?:\.|\[\])(?:\.|\[\]|$))/g,
  Iw = /\\(\\)?/g
Mw(function (n) {
  var i = []
  return (
    n.charCodeAt(0) === 46 && i.push(''),
    n.replace(Lw, function (r, a, l, h) {
      i.push(l ? h.replace(Iw, '$1') : a || r)
    }),
    i
  )
})
var Zu = Ga,
  ju = Zu ? Zu.prototype : void 0
ju && ju.toString
const Ye = qt({
  UTC: ['utc', 'UTC'],
  Local: ['local', 'LOCAL'],
})
function fi(n) {
  var i
  return (
    (i = class extends n {}),
    Y(i, 'shadowRootOptions', { ...n.shadowRootOptions, delegatesFocus: !0 }),
    i
  )
}
function Dw(n) {
  var i
  return (
    (i = class extends n {
      connectedCallback() {
        super.connectedCallback(), this.setTimezone(this.timezone)
      }
      get isLocal() {
        return this.timezone === Ye.Local
      }
      get isUTC() {
        return this.timezone === Ye.UTC
      }
      constructor() {
        super(), (this.timezone = Ye.UTC)
      }
      setTimezone(a = Ye.UTC) {
        this.timezone = Ye.includes(a) ? Ye.getValue(a) : Ye.UTC
      }
    }),
    Y(i, 'properties', {
      timezone: { type: Ye, converter: Ye.getValue },
    }),
    i
  )
}
function mn(n = Ju(jn), ...i) {
  return (
    Kt(n._$litElement$) && (i.push(n), (n = Ju(jn))), Ge(...i.flat())(Bw(n))
  )
}
function Ju(n) {
  return class extends n {}
}
class On {
  constructor(i, r, a = {}) {
    Y(this, '_event')
    ;(this.original = r), (this.value = i), (this.meta = a)
  }
  get event() {
    return this._event
  }
  setEvent(i = Qe('"event" is required to set event')) {
    this._event = i
  }
  static assert(i) {
    ae(i instanceof On, 'Event "detail" should be instance of "EventDetail"')
  }
  static assertHandler(i) {
    return (
      ae(Fa(i), '"eventHandler" should be a function'),
      function (r = Qe('"event" is required')) {
        return On.assert(r.detail), i(r)
      }
    )
  }
}
function Bw(n) {
  var i
  return (
    (i = class extends n {
      constructor() {
        super(),
          (this.uid = Dh()),
          (this.disabled = !1),
          (this.emit.EventDetail = On)
      }
      connectedCallback() {
        super.connectedCallback(),
          wr(window.htmx) &&
            (window.htmx.process(this), window.htmx.process(this.renderRoot))
      }
      firstUpdated() {
        var a
        super.firstUpdated(),
          (a = this.elsSlots) == null ||
            a.forEach(l =>
              l.addEventListener(
                'slotchange',
                this._handleSlotChange.bind(this),
              ),
            )
      }
      get elSlot() {
        var a
        return (a = this.renderRoot) == null
          ? void 0
          : a.querySelector(yr.SlotDefault)
      }
      get elsSlots() {
        var a
        return (a = this.renderRoot) == null
          ? void 0
          : a.querySelectorAll('slot')
      }
      get elBase() {
        var a
        return (a = this.renderRoot) == null
          ? void 0
          : a.querySelector(yr.PartBase)
      }
      get elsSlotted() {
        return []
          .concat(
            Array.from(this.elsSlots).map(a =>
              a.assignedElements({ flatten: !0 }),
            ),
          )
          .flat()
      }
      clear() {
        i.clear(this)
      }
      emit(a = 'event', l) {
        if (
          ((l = Object.assign(
            {
              detail: void 0,
              bubbles: !0,
              cancelable: !1,
              composed: !0,
            },
            l,
          )),
          wr(l.detail))
        ) {
          if (wt(l.detail instanceof On) && di(l.detail))
            if ('value' in l.detail) {
              const { value: h, ...f } = l.detail
              l.detail = new On(h, void 0, f)
            } else l.detail = new On(l.detail)
          ae(
            l.detail instanceof On,
            'event "detail" must be instance of "EventDetail"',
          ),
            l.detail.setEvent(a)
        }
        return this.dispatchEvent(new CustomEvent(a, l))
      }
      setHidden(a = !1, l = 0) {
        setTimeout(() => {
          this.hidden = a
        }, l)
      }
      setDisabled(a = !1, l = 0) {
        setTimeout(() => {
          this.disabled = a
        }, l)
      }
      notify(a, l, h) {
        var f
        ;(f = a == null ? void 0 : a.emit) == null || f.call(a, l, h)
      }
      // We may want to ensure event detail when sending custom events from children components
      assertEventHandler(a = Qe('"eventHandler" is required')) {
        return (
          ae(Fa(a), '"eventHandler" should be a function'),
          this.emit.EventDetail.assertHandler(a.bind(this))
        )
      }
      getShadowRoot() {
        return this.renderRoot
      }
      _handleSlotChange(a) {
        wr(a.target) && (a.target.style.position = 'initial')
      }
      static clear(a) {
        ko(a) && (a.innerHTML = '')
      }
    }),
    Y(i, 'properties', {
      uid: { type: String },
      disabled: { type: Boolean, reflect: !0 },
      tabindex: { type: Number, reflect: !0 },
    }),
    i
  )
}
const $e = mn()
function Uw() {
  return pt(`
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
function Nw() {
  return pt(`
        :host { box-sizing: border-box; }
        :host *,
        :host *::before,
        :host *::after { box-sizing: inherit; }
        :host[hidden] { display: none !important; }
    `)
}
function Dt() {
  return pt(`
        ${Nw()}
        ${Uw()}
    `)
}
function Hh(n) {
  return pt(
    `
            :host(:focus-visible:not([disabled])) {
                
        outline: var(--half) solid var(--color-outline);
        outline-offset: var(--half);
        z-index: 1;
    
            }
    `,
  )
}
function Fw(n = ':host') {
  return pt(`
    ${n} {
        scroll-behavior: smooth;
        scrollbar-width: var(--size-scrollbar);
        scrollbar-width: thin;
        scrollbar-color: var(--color-scrollbar) transparent;
        scrollbar-base-color: var(--color-scrollbar);
        scrollbar-face-color: var(--color-scrollbar);
        scrollbar-track-color: transparent;
    }
    
    ${n}::-webkit-scrollbar {
        height: var(--size-scrollbar);
        width: var(--size-scrollbar);
        border-radius: var(--step-4);
        overflow: hidden;
    }
    
    ${n}::-webkit-scrollbar-track {
        background-color: transparent;
    }
    
    ${n}::-webkit-scrollbar-thumb {
        background: var(--color-scrollbar);
        border-radius: var(--step-4);
    }
  `)
}
function Hw(n = 'from-input') {
  return pt(`
        :host {
            --from-input-padding: var(--${n}-padding-y) var(--${n}-padding-x);
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

            --${n}-padding: var(--from-input-padding);
            --${n}-placeholder-color: var(--from-input-placeholder-color);
            --${n}-color: var(--from-input-color);
            --${n}-font-size: var(--from-input-font-size);
            --${n}-text-align: var(--from-input-text-align);
            --${n}-box-shadow: var(--from-input-box-shadow);
            --${n}-radius: var(--from-input-radius);
            --${n}-background: transparent;
            --${n}-font-family: var(--from-input-font-family);
            --${n}-box-shadow-hover: var(--form-input-box-shadow-hover);
            --${n}-box-shadow-focus: var(--form-input-box-shadow-focus);
        }
        :host([error]) {
            --${n}-box-shadow: inset 0 0 0 1px var(--color-scarlet);
        }
      `)
}
function Po(n = 'color') {
  return pt(`
          :host([variant="${xt.Neutral}"]),
          :host([variant="${xt.Undefined}"]) {
            --${n}-variant-10: var(--color-gray-10);
            --${n}-variant-15: var(--color-gray-15);
            --${n}-variant-125: var(--color-gray-125);
            --${n}-variant-150: var(--color-gray-150);
            --${n}-variant-525: var(--color-gray-525);
            --${n}-variant-550: var(--color-gray-550);
            --${n}-variant-725: var(--color-gray-725);
            --${n}-variant-750: var(--color-gray-750);

            --${n}-variant-shadow: var(--color-gray-200);
            --${n}-variant: var(--color-gray-700);
            --${n}-variant-lucid: var(--color-gray-5);
            --${n}-variant-light: var(--color-gray-100);
            --${n}-variant-dark: var(--color-gray-800);
          }
          :host([variant="${xt.Undefined}"]) {
            --${n}-variant-shadow: var(--color-gray-100);
            --${n}-variant: var(--color-gray-200);
            --${n}-variant-lucid: var(--color-gray-5);
            --${n}-variant-light: var(--color-gray-100);
            --${n}-variant-dark: var(--color-gray-500);
          }
          :host([variant="${xt.Success}"]),
          :host([variant="${xt.Complete}"]) {
            --${n}-variant-5: var(--color-emerald-5);
            --${n}-variant-10: var(--color-emerald-10);
            --${n}-variant: var(--color-emerald-500);
            --${n}-variant-lucid: var(--color-emerald-5);
            --${n}-variant-light: var(--color-emerald-100);
            --${n}-variant-dark: var(--color-emerald-800);
          }
          :host([variant="${xt.Warning}"]),
          :host([variant="${xt.Skipped}"]),
          :host([variant="${xt.Pending}"]) {
            --${n}-variant: var(--color-mandarin-500);
            --${n}-variant-lucid: var(--color-mandarin-5);
            --${n}-variant-light: var(--color-mandarin-100);
            --${n}-variant-dark: var(--color-mandarin-800);
          }
          :host([variant="${xt.Danger}"]),
          :host([variant="${xt.Behind}"]),
          :host([variant="${xt.Failed}"]) {
            --${n}-variant-5: var(--color-scarlet-5);
            --${n}-variant-10: var(--color-scarlet-10);
            --${n}-variant: var(--color-scarlet-500);
            --${n}-variant-lucid: var(--color-scarlet-5);
            --${n}-variant-lucid: var(--color-scarlet-5);
            --${n}-variant-light: var(--color-scarlet-100);
            --${n}-variant-dark: var(--color-scarlet-800);
          }
          :host([variant="${xt.ChangeAdd}"]) {
            --${n}-variant: var(--color-change-add);
            --${n}-variant-lucid: var(--color-change-add-5);
            --${n}-variant-light: var(--color-change-add-100);
            --${n}-variant-dark: var(--color-change-add-800);
          }
          :host([variant="${xt.ChangeRemove}"]) {
            --${n}-variant: var(--color-change-remove);
            --${n}-variant-lucid: var(--color-change-remove-5);
            --${n}-variant-light: var(--color-change-remove-100);
            --${n}-variant-dark: var(--color-change-remove-800);
          }
          :host([variant="${xt.ChangeDirectly}"]) {
            --${n}-variant: var(--color-change-directly-modified);
            --${n}-variant-lucid: var(--color-change-directly-modified-5);
            --${n}-variant-light: var(--color-change-directly-modified-100);
            --${n}-variant-dark: var(--color-change-directly-modified-800);
          }
          :host([variant="${xt.ChangeIndirectly}"]) {
            --${n}-variant: var(--color-change-indirectly-modified);
            --${n}-variant-lucid: var(--color-change-indirectly-modified-5);
            --${n}-variant-light: var(--color-change-indirectly-modified-100);
            --${n}-variant-dark: var(--color-change-indirectly-modified-800);
          }
          :host([variant="${xt.ChangeMetadata}"]) {
            --${n}-variant: var(--color-change-metadata);
            --${n}-variant-lucid: var(--color-change-metadata-5);
            --${n}-variant-light: var(--color-change-metadata-100);
            --${n}-variant-dark: var(--color-change-metadata-800);
          }
          :host([variant="${xt.Backfill}"]) {
            --${n}-variant: var(--color-backfill);
            --${n}-variant-lucid: var(--color-backfill-5);
            --${n}-variant-light: var(--color-backfill-100);
            --${n}-variant-dark: var(--color-backfill-800);
          }
          :host([variant="${xt.Model}"]),
          :host([variant="${xt.Primary}"]) {
            --${n}-variant: var(--color-deep-blue);
            --${n}-variant-lucid: var(--color-deep-blue-5);
            --${n}-variant-light: var(--color-deep-blue-100);
            --${n}-variant-dark: var(--color-deep-blue-800);
          }
          :host([variant="${xt.Plan}"]) {
            --${n}-variant: var(--color-plan);
            --${n}-variant-lucid: var(--color-plan-5);
            --${n}-variant-light: var(--color-plan-100);
            --${n}-variant-dark: var(--color-plan-800);
          }
          :host([variant="${xt.Run}"]) {
            --${n}-variant: var(--color-run);
            --${n}-variant-lucid: var(--color-run-5);
            --${n}-variant-light: var(--color-run-100);
            --${n}-variant-dark: var(--color-run-800);
          }
          :host([variant="${xt.Environment}"]) {
            --${n}-variant: var(--color-environment);
            --${n}-variant-lucid: var(--color-environment-5);
            --${n}-variant-light: var(--color-environment-100);
            --${n}-variant-dark: var(--color-environment-800);
          }
        :host([variant="${xt.Progress}"]),
        :host([variant="${xt.InProgress}"]) {
            --${n}-variant: var(--color-status-progress);
            --${n}-variant-lucid: var(--color-status-progress-5);
            --${n}-variant-light: var(--color-status-progress-100);
            --${n}-variant-dark: var(--color-status-progress-800);
        }
      `)
}
function Ww(n = '') {
  return pt(`
        :host([inverse]) {
            --${n}-background: var(--color-variant);
            --${n}-color: var(--color-variant-light);
        }
    `)
}
function qw(n = '') {
  return pt(`
        :host([ghost]:not([inverse])) {
            --${n}-background: transparent;
        }
        :host([disabled][ghost]:not([inverse])) {
            --${n}-background: var(--color-variant-light);
        }
    `)
}
function Yw(n = '') {
  return pt(`
        :host(:hover:not([disabled])) {
            --${n}-background: var(--color-variant-125);
        }
        :host(:active:not([disabled])) {
            --${n}-background: var(--color-variant-150);
        }
        :host([inverse]:hover:not([disabled])) {
            --${n}-background: var(--color-variant-525);
        }
        :host([inverse]:active:not([disabled])) {
            --${n}-background: var(--color-variant-550);
        }
    `)
}
function Ro(n = '', i) {
  return pt(`
        :host([shape="${je.Rect}"]) {
            --${n}-radius: 0;
        }        
        :host([shape="${je.Round}"]) {
            --${n}-radius: var(--from-input-radius, var(--radius-xs));
        }
        :host([shape="${je.Pill}"]) {
            --${n}-radius: calc(var(--${n}-font-size) * 2);
        }
        :host([shape="${je.Circle}"]) {
            --${n}-width: calc(var(--${n}-font-size) * 2);
            --${n}-height: calc(var(--${n}-font-size) * 2);
            --${n}-padding-y: 0;
            --${n}-padding-x: 0;
            --${n}-radius: 100%;
        }
        :host([shape="${je.Square}"]) {
            --${n}-width: calc(var(--${n}-font-size) * 2);
            --${n}-height: calc(var(--${n}-font-size) * 2);
            --${n}-padding-y: 0;
            --${n}-padding-x: 0;
            --${n}-radius: 0;
        }
    `)
}
function Gw() {
  return pt(`
        :host([side="${Je.Left}"]) {
            --text-align: left;
        }
        :host([side="${Je.Center}"]) {
            --text-align: center;
        }
        :host([side="${Je.Right}"]) {
            --text-align: right;
        }
    `)
}
function Kw() {
  return pt(`
        :host([shadow]) {
            --shadow: 0 1px var(--half) 0 var(--color-variant-shadow);
        }
    `)
}
function Vw() {
  return pt(`
        :host([outline]) {
            --shadow-inset: inset 0 0 0 var(--half) var(--color-variant);
        }
    `)
}
function Wh(n = 'label', i = '') {
  return pt(`
        ${n} {
            font-weight: var(--text-semibold);
            color: var(${i ? `--${i}-color` : '--color-gray-700'});
        }
    `)
}
function Dn(n = 'item', i = 1.25, r = 4) {
  return pt(`
        :host {
            --${n}-padding-x: round(up, calc(var(--${n}-font-size) / ${i} * var(--padding-x-factor, 1)), var(--half));
            --${n}-padding-y: round(up, calc(var(--${n}-font-size) / ${r} * var(--padding-y-factor, 1)), var(--half));
        }
    `)
}
function Xw(n = '') {
  return pt(`
        :host([horizontal="compact"]) {
            --padding-x-factor: 0.5;
        }
        :host([horizontal="wide"]) {
            --padding-x-factor: 1.5;
        }
        :host([horizontal="full"]) {
            --${n}-width: 100%;
            width: var(--${n}-width);
        }
        :host([vertical="tall"]) {
            --padding-y-factor: 1.25;
        }
        :host([vertical="short"]) {
            --padding-y-factor: 0.75;
        }
        :host([vertical="full"]) {
            --${n}-height: 100%;
            height: var(--${n}-height);
        }
    `)
}
function Me(n) {
  const i = n ? `--${n}-font-size` : '--font-size',
    r = n ? `--${n}-font-weight` : '--font-size'
  return pt(`
        :host {
            ${r}: var(--text-medium);
            ${i}: var(--text-s);
        }
        :host([size="${Lt.XXS}"]) {
            ${r}: var(--text-semibold);
            ${i}: var(--text-2xs);
        }
        :host([size="${Lt.XS}"]) {
            ${r}: var(--text-semibold);
            ${i}: var(--text-xs);
        }
        :host([size="${Lt.S}"]) {
            ${r}: var(--text-medium);
            ${i}: var(--text-s);
        }
        :host([size="${Lt.M}"]) {
            ${r}: var(--text-medium);
            ${i}: var(--text-m);
        }
        :host([size="${Lt.L}"]) {
            ${r}: var(--text-normal);
            ${i}: var(--text-l);
        }
        :host([size="${Lt.XL}"]) {
            ${r}: var(--text-normal);
            ${i}: var(--text-xl);
        }
        :host([size="${Lt.XXL}"]) {
            ${r}: var(--text-normal);
            ${i}: var(--text-2xl);
        }
    `)
}
const Zw = `:host {
  --badge-background: var(--color-variant-lucid);
  --badge-color: var(--color-variant);
  --badge-font-family: var(--font-mono);
  --badge-font-size: var(--font-size);

  display: inline-flex;
  border-radius: var(--badge-radius);
}
[part='base'] {
  display: flex;
  background: var(--badge-background, var(--color-gray-5));
  font-size: var(--badge-font-size);
  line-height: 1;
  border-radius: var(--badge-radius);
  padding: var(--badge-padding-y) var(--badge-padding-x);
  text-align: center;
  white-space: nowrap;
}
`
class va extends $e {
  constructor() {
    super(),
      (this.size = Lt.M),
      (this.variant = xt.Neutral),
      (this.shape = je.Round)
  }
  render() {
    return X`
      <span part="base">
        <slot></slot>
      </span>
    `
  }
}
Y(va, 'styles', [
  Dt(),
  Me(),
  Po(),
  Wh('[part="base"]', 'badge'),
  Ww('badge'),
  qw('badge'),
  Ro('badge'),
  Dn('badge', 1.75, 4),
  Kw(),
  Vw(),
  pt(Zw),
]),
  Y(va, 'properties', {
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
  })
customElements.define('tbk-badge', va)
var ma = ''
function Qu(n) {
  ma = n
}
function jw(n = '') {
  if (!ma) {
    const i = [...document.getElementsByTagName('script')],
      r = i.find(a => a.hasAttribute('data-shoelace'))
    if (r) Qu(r.getAttribute('data-shoelace'))
    else {
      const a = i.find(
        h =>
          /shoelace(\.min)?\.js($|\?)/.test(h.src) ||
          /shoelace-autoloader(\.min)?\.js($|\?)/.test(h.src),
      )
      let l = ''
      a && (l = a.getAttribute('src')), Qu(l.split('/').slice(0, -1).join('/'))
    }
  }
  return ma.replace(/\/$/, '') + (n ? `/${n.replace(/^\//, '')}` : '')
}
var Jw = {
    name: 'default',
    resolver: n => jw(`assets/icons/${n}.svg`),
  },
  Qw = Jw,
  th = {
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
  t$ = {
    name: 'system',
    resolver: n =>
      n in th ? `data:image/svg+xml,${encodeURIComponent(th[n])}` : '',
  },
  e$ = t$,
  wo = [Qw, e$],
  $o = []
function n$(n) {
  $o.push(n)
}
function r$(n) {
  $o = $o.filter(i => i !== n)
}
function eh(n) {
  return wo.find(i => i.name === n)
}
function qh(n, i) {
  i$(n),
    wo.push({
      name: n,
      resolver: i.resolver,
      mutator: i.mutator,
      spriteSheet: i.spriteSheet,
    }),
    $o.forEach(r => {
      r.library === n && r.setIcon()
    })
}
function i$(n) {
  wo = wo.filter(i => i.name !== n)
}
var o$ = en`
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
  Yh = Object.defineProperty,
  s$ = Object.defineProperties,
  a$ = Object.getOwnPropertyDescriptor,
  l$ = Object.getOwnPropertyDescriptors,
  nh = Object.getOwnPropertySymbols,
  c$ = Object.prototype.hasOwnProperty,
  u$ = Object.prototype.propertyIsEnumerable,
  rh = (n, i, r) =>
    i in n
      ? Yh(n, i, { enumerable: !0, configurable: !0, writable: !0, value: r })
      : (n[i] = r),
  pi = (n, i) => {
    for (var r in i || (i = {})) c$.call(i, r) && rh(n, r, i[r])
    if (nh) for (var r of nh(i)) u$.call(i, r) && rh(n, r, i[r])
    return n
  },
  Gh = (n, i) => s$(n, l$(i)),
  R = (n, i, r, a) => {
    for (
      var l = a > 1 ? void 0 : a ? a$(i, r) : i, h = n.length - 1, f;
      h >= 0;
      h--
    )
      (f = n[h]) && (l = (a ? f(i, r, l) : f(l)) || l)
    return a && l && Yh(i, r, l), l
  },
  Kh = (n, i, r) => {
    if (!i.has(n)) throw TypeError('Cannot ' + r)
  },
  h$ = (n, i, r) => (Kh(n, i, 'read from private field'), i.get(n)),
  d$ = (n, i, r) => {
    if (i.has(n))
      throw TypeError('Cannot add the same private member more than once')
    i instanceof WeakSet ? i.add(n) : i.set(n, r)
  },
  f$ = (n, i, r, a) => (Kh(n, i, 'write to private field'), i.set(n, r), r)
function le(n, i) {
  const r = pi(
    {
      waitUntilFirstUpdate: !1,
    },
    i,
  )
  return (a, l) => {
    const { update: h } = a,
      f = Array.isArray(n) ? n : [n]
    a.update = function (v) {
      f.forEach(m => {
        const w = m
        if (v.has(w)) {
          const k = v.get(w),
            C = this[w]
          k !== C &&
            (!r.waitUntilFirstUpdate || this.hasUpdated) &&
            this[l](k, C)
        }
      }),
        h.call(this, v)
    }
  }
}
var bn = en`
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
const p$ = {
    attribute: !0,
    type: String,
    converter: bo,
    reflect: !1,
    hasChanged: Ua,
  },
  g$ = (n = p$, i, r) => {
    const { kind: a, metadata: l } = r
    let h = globalThis.litPropertyMetadata.get(l)
    if (
      (h === void 0 &&
        globalThis.litPropertyMetadata.set(l, (h = /* @__PURE__ */ new Map())),
      h.set(r.name, n),
      a === 'accessor')
    ) {
      const { name: f } = r
      return {
        set(v) {
          const m = i.get.call(this)
          i.set.call(this, v), this.requestUpdate(f, m, n)
        },
        init(v) {
          return v !== void 0 && this.P(f, void 0, n), v
        },
      }
    }
    if (a === 'setter') {
      const { name: f } = r
      return function (v) {
        const m = this[f]
        i.call(this, v), this.requestUpdate(f, m, n)
      }
    }
    throw Error('Unsupported decorator location: ' + a)
  }
function V(n) {
  return (i, r) =>
    typeof r == 'object'
      ? g$(n, i, r)
      : ((a, l, h) => {
          const f = l.hasOwnProperty(h)
          return (
            l.constructor.createProperty(h, f ? { ...a, wrapped: !0 } : a),
            f ? Object.getOwnPropertyDescriptor(l, h) : void 0
          )
        })(n, i, r)
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function gi(n) {
  return V({ ...n, state: !0, attribute: !1 })
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function v$(n) {
  return (i, r) => {
    const a = typeof i == 'function' ? i : i[r]
    Object.assign(a, n)
  }
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const m$ = (n, i, r) => (
  (r.configurable = !0),
  (r.enumerable = !0),
  Reflect.decorate && typeof i != 'object' && Object.defineProperty(n, i, r),
  r
)
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
function Le(n, i) {
  return (r, a, l) => {
    const h = f => {
      var v
      return ((v = f.renderRoot) == null ? void 0 : v.querySelector(n)) ?? null
    }
    return m$(r, a, {
      get() {
        return h(this)
      },
    })
  }
}
var po,
  xe = class extends jn {
    constructor() {
      super(),
        d$(this, po, !1),
        (this.initialReflectedProperties = /* @__PURE__ */ new Map()),
        Object.entries(this.constructor.dependencies).forEach(([n, i]) => {
          this.constructor.define(n, i)
        })
    }
    emit(n, i) {
      const r = new CustomEvent(
        n,
        pi(
          {
            bubbles: !0,
            cancelable: !1,
            composed: !0,
            detail: {},
          },
          i,
        ),
      )
      return this.dispatchEvent(r), r
    }
    /* eslint-enable */
    static define(n, i = this, r = {}) {
      const a = customElements.get(n)
      if (!a) {
        try {
          customElements.define(n, i, r)
        } catch {
          customElements.define(n, class extends i {}, r)
        }
        return
      }
      let l = ' (unknown version)',
        h = l
      'version' in i && i.version && (l = ' v' + i.version),
        'version' in a && a.version && (h = ' v' + a.version),
        !(l && h && l === h) &&
          console.warn(
            `Attempted to register <${n}>${l}, but <${n}>${h} has already been registered.`,
          )
    }
    attributeChangedCallback(n, i, r) {
      h$(this, po) ||
        (this.constructor.elementProperties.forEach((a, l) => {
          a.reflect &&
            this[l] != null &&
            this.initialReflectedProperties.set(l, this[l])
        }),
        f$(this, po, !0)),
        super.attributeChangedCallback(n, i, r)
    }
    willUpdate(n) {
      super.willUpdate(n),
        this.initialReflectedProperties.forEach((i, r) => {
          n.has(r) && this[r] == null && (this[r] = i)
        })
    }
  }
po = /* @__PURE__ */ new WeakMap()
xe.version = '2.18.0'
xe.dependencies = {}
R([V()], xe.prototype, 'dir', 2)
R([V()], xe.prototype, 'lang', 2)
/**
 * @license
 * Copyright 2020 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const b$ = (n, i) => (n == null ? void 0 : n._$litType$) !== void 0
var ti = Symbol(),
  co = Symbol(),
  ca,
  ua = /* @__PURE__ */ new Map(),
  Pe = class extends xe {
    constructor() {
      super(...arguments),
        (this.initialRender = !1),
        (this.svg = null),
        (this.label = ''),
        (this.library = 'default')
    }
    /** Given a URL, this function returns the resulting SVG element or an appropriate error symbol. */
    async resolveIcon(n, i) {
      var r
      let a
      if (i != null && i.spriteSheet)
        return (
          (this.svg = X`<svg part="svg">
        <use part="use" href="${n}"></use>
      </svg>`),
          this.svg
        )
      try {
        if (((a = await fetch(n, { mode: 'cors' })), !a.ok))
          return a.status === 410 ? ti : co
      } catch {
        return co
      }
      try {
        const l = document.createElement('div')
        l.innerHTML = await a.text()
        const h = l.firstElementChild
        if (
          ((r = h == null ? void 0 : h.tagName) == null
            ? void 0
            : r.toLowerCase()) !== 'svg'
        )
          return ti
        ca || (ca = new DOMParser())
        const v = ca
          .parseFromString(h.outerHTML, 'text/html')
          .body.querySelector('svg')
        return v ? (v.part.add('svg'), document.adoptNode(v)) : ti
      } catch {
        return ti
      }
    }
    connectedCallback() {
      super.connectedCallback(), n$(this)
    }
    firstUpdated() {
      ;(this.initialRender = !0), this.setIcon()
    }
    disconnectedCallback() {
      super.disconnectedCallback(), r$(this)
    }
    getIconSource() {
      const n = eh(this.library)
      return this.name && n
        ? {
            url: n.resolver(this.name),
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
      var n
      const { url: i, fromLibrary: r } = this.getIconSource(),
        a = r ? eh(this.library) : void 0
      if (!i) {
        this.svg = null
        return
      }
      let l = ua.get(i)
      if (
        (l || ((l = this.resolveIcon(i, a)), ua.set(i, l)), !this.initialRender)
      )
        return
      const h = await l
      if ((h === co && ua.delete(i), i === this.getIconSource().url)) {
        if (b$(h)) {
          if (((this.svg = h), a)) {
            await this.updateComplete
            const f = this.shadowRoot.querySelector("[part='svg']")
            typeof a.mutator == 'function' && f && a.mutator(f)
          }
          return
        }
        switch (h) {
          case co:
          case ti:
            ;(this.svg = null), this.emit('sl-error')
            break
          default:
            ;(this.svg = h.cloneNode(!0)),
              (n = a == null ? void 0 : a.mutator) == null ||
                n.call(a, this.svg),
              this.emit('sl-load')
        }
      }
    }
    render() {
      return this.svg
    }
  }
Pe.styles = [bn, o$]
R([gi()], Pe.prototype, 'svg', 2)
R([V({ reflect: !0 })], Pe.prototype, 'name', 2)
R([V()], Pe.prototype, 'src', 2)
R([V()], Pe.prototype, 'label', 2)
R([V({ reflect: !0 })], Pe.prototype, 'library', 2)
R([le('label')], Pe.prototype, 'handleLabelChange', 1)
R([le(['name', 'src', 'library'])], Pe.prototype, 'setIcon', 1)
qh('heroicons', {
  resolver: n =>
    `https://cdn.jsdelivr.net/npm/heroicons@2.1.5/24/outline/${n}.svg`,
})
qh('heroicons-micro', {
  resolver: n =>
    `https://cdn.jsdelivr.net/npm/heroicons@2.1.5/16/solid/${n}.svg`,
})
class ba extends mn(Pe, fi) {}
Y(ba, 'styles', [Pe.styles, Dt()]),
  Y(ba, 'properties', {
    ...Pe.properties,
  })
customElements.define('tbk-icon', ba)
var y$ = en`
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
  _$ = en`
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
const ya = /* @__PURE__ */ new Set(),
  w$ = new MutationObserver(jh),
  _r = /* @__PURE__ */ new Map()
let Vh = document.documentElement.dir || 'ltr',
  Xh = document.documentElement.lang || navigator.language,
  Xn
w$.observe(document.documentElement, {
  attributes: !0,
  attributeFilter: ['dir', 'lang'],
})
function Zh(...n) {
  n.map(i => {
    const r = i.$code.toLowerCase()
    _r.has(r)
      ? _r.set(r, Object.assign(Object.assign({}, _r.get(r)), i))
      : _r.set(r, i),
      Xn || (Xn = i)
  }),
    jh()
}
function jh() {
  ;(Vh = document.documentElement.dir || 'ltr'),
    (Xh = document.documentElement.lang || navigator.language),
    [...ya.keys()].map(n => {
      typeof n.requestUpdate == 'function' && n.requestUpdate()
    })
}
let $$ = class {
  constructor(i) {
    ;(this.host = i), this.host.addController(this)
  }
  hostConnected() {
    ya.add(this.host)
  }
  hostDisconnected() {
    ya.delete(this.host)
  }
  dir() {
    return `${this.host.dir || Vh}`.toLowerCase()
  }
  lang() {
    return `${this.host.lang || Xh}`.toLowerCase()
  }
  getTranslationData(i) {
    var r, a
    const l = new Intl.Locale(i.replace(/_/g, '-')),
      h = l == null ? void 0 : l.language.toLowerCase(),
      f =
        (a =
          (r = l == null ? void 0 : l.region) === null || r === void 0
            ? void 0
            : r.toLowerCase()) !== null && a !== void 0
          ? a
          : '',
      v = _r.get(`${h}-${f}`),
      m = _r.get(h)
    return { locale: l, language: h, region: f, primary: v, secondary: m }
  }
  exists(i, r) {
    var a
    const { primary: l, secondary: h } = this.getTranslationData(
      (a = r.lang) !== null && a !== void 0 ? a : this.lang(),
    )
    return (
      (r = Object.assign({ includeFallback: !1 }, r)),
      !!((l && l[i]) || (h && h[i]) || (r.includeFallback && Xn && Xn[i]))
    )
  }
  term(i, ...r) {
    const { primary: a, secondary: l } = this.getTranslationData(this.lang())
    let h
    if (a && a[i]) h = a[i]
    else if (l && l[i]) h = l[i]
    else if (Xn && Xn[i]) h = Xn[i]
    else
      return console.error(`No translation found for: ${String(i)}`), String(i)
    return typeof h == 'function' ? h(...r) : h
  }
  date(i, r) {
    return (i = new Date(i)), new Intl.DateTimeFormat(this.lang(), r).format(i)
  }
  number(i, r) {
    return (
      (i = Number(i)),
      isNaN(i) ? '' : new Intl.NumberFormat(this.lang(), r).format(i)
    )
  }
  relativeTime(i, r, a) {
    return new Intl.RelativeTimeFormat(this.lang(), a).format(i, r)
  }
}
var Jh = {
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
  goToSlide: (n, i) => `Go to slide ${n} of ${i}`,
  hidePassword: 'Hide password',
  loading: 'Loading',
  nextSlide: 'Next slide',
  numOptionsSelected: n =>
    n === 0
      ? 'No options selected'
      : n === 1
      ? '1 option selected'
      : `${n} options selected`,
  previousSlide: 'Previous slide',
  progress: 'Progress',
  remove: 'Remove',
  resize: 'Resize',
  scrollToEnd: 'Scroll to end',
  scrollToStart: 'Scroll to start',
  selectAColorFromTheScreen: 'Select a color from the screen',
  showPassword: 'Show password',
  slideNum: n => `Slide ${n}`,
  toggleColorFormat: 'Toggle color format',
}
Zh(Jh)
var x$ = Jh,
  vi = class extends $$ {}
Zh(x$)
const Pn = Math.min,
  ye = Math.max,
  xo = Math.round,
  uo = Math.floor,
  Rn = n => ({
    x: n,
    y: n,
  }),
  S$ = {
    left: 'right',
    right: 'left',
    bottom: 'top',
    top: 'bottom',
  },
  C$ = {
    start: 'end',
    end: 'start',
  }
function _a(n, i, r) {
  return ye(n, Pn(i, r))
}
function Tr(n, i) {
  return typeof n == 'function' ? n(i) : n
}
function Mn(n) {
  return n.split('-')[0]
}
function Or(n) {
  return n.split('-')[1]
}
function Qh(n) {
  return n === 'x' ? 'y' : 'x'
}
function Xa(n) {
  return n === 'y' ? 'height' : 'width'
}
function mi(n) {
  return ['top', 'bottom'].includes(Mn(n)) ? 'y' : 'x'
}
function Za(n) {
  return Qh(mi(n))
}
function A$(n, i, r) {
  r === void 0 && (r = !1)
  const a = Or(n),
    l = Za(n),
    h = Xa(l)
  let f =
    l === 'x'
      ? a === (r ? 'end' : 'start')
        ? 'right'
        : 'left'
      : a === 'start'
      ? 'bottom'
      : 'top'
  return i.reference[h] > i.floating[h] && (f = So(f)), [f, So(f)]
}
function E$(n) {
  const i = So(n)
  return [wa(n), i, wa(i)]
}
function wa(n) {
  return n.replace(/start|end/g, i => C$[i])
}
function k$(n, i, r) {
  const a = ['left', 'right'],
    l = ['right', 'left'],
    h = ['top', 'bottom'],
    f = ['bottom', 'top']
  switch (n) {
    case 'top':
    case 'bottom':
      return r ? (i ? l : a) : i ? a : l
    case 'left':
    case 'right':
      return i ? h : f
    default:
      return []
  }
}
function T$(n, i, r, a) {
  const l = Or(n)
  let h = k$(Mn(n), r === 'start', a)
  return l && ((h = h.map(f => f + '-' + l)), i && (h = h.concat(h.map(wa)))), h
}
function So(n) {
  return n.replace(/left|right|bottom|top/g, i => S$[i])
}
function O$(n) {
  return {
    top: 0,
    right: 0,
    bottom: 0,
    left: 0,
    ...n,
  }
}
function td(n) {
  return typeof n != 'number'
    ? O$(n)
    : {
        top: n,
        right: n,
        bottom: n,
        left: n,
      }
}
function Co(n) {
  return {
    ...n,
    top: n.y,
    left: n.x,
    right: n.x + n.width,
    bottom: n.y + n.height,
  }
}
function ih(n, i, r) {
  let { reference: a, floating: l } = n
  const h = mi(i),
    f = Za(i),
    v = Xa(f),
    m = Mn(i),
    w = h === 'y',
    k = a.x + a.width / 2 - l.width / 2,
    C = a.y + a.height / 2 - l.height / 2,
    U = a[v] / 2 - l[v] / 2
  let T
  switch (m) {
    case 'top':
      T = {
        x: k,
        y: a.y - l.height,
      }
      break
    case 'bottom':
      T = {
        x: k,
        y: a.y + a.height,
      }
      break
    case 'right':
      T = {
        x: a.x + a.width,
        y: C,
      }
      break
    case 'left':
      T = {
        x: a.x - l.width,
        y: C,
      }
      break
    default:
      T = {
        x: a.x,
        y: a.y,
      }
  }
  switch (Or(i)) {
    case 'start':
      T[f] -= U * (r && w ? -1 : 1)
      break
    case 'end':
      T[f] += U * (r && w ? -1 : 1)
      break
  }
  return T
}
const z$ = async (n, i, r) => {
  const {
      placement: a = 'bottom',
      strategy: l = 'absolute',
      middleware: h = [],
      platform: f,
    } = r,
    v = h.filter(Boolean),
    m = await (f.isRTL == null ? void 0 : f.isRTL(i))
  let w = await f.getElementRects({
      reference: n,
      floating: i,
      strategy: l,
    }),
    { x: k, y: C } = ih(w, a, m),
    U = a,
    T = {},
    O = 0
  for (let _ = 0; _ < v.length; _++) {
    const { name: x, fn: P } = v[_],
      {
        x: G,
        y: N,
        data: it,
        reset: J,
      } = await P({
        x: k,
        y: C,
        initialPlacement: a,
        placement: U,
        strategy: l,
        middlewareData: T,
        rects: w,
        platform: f,
        elements: {
          reference: n,
          floating: i,
        },
      })
    if (
      ((k = G ?? k),
      (C = N ?? C),
      (T = {
        ...T,
        [x]: {
          ...T[x],
          ...it,
        },
      }),
      J && O <= 50)
    ) {
      O++,
        typeof J == 'object' &&
          (J.placement && (U = J.placement),
          J.rects &&
            (w =
              J.rects === !0
                ? await f.getElementRects({
                    reference: n,
                    floating: i,
                    strategy: l,
                  })
                : J.rects),
          ({ x: k, y: C } = ih(w, U, m))),
        (_ = -1)
      continue
    }
  }
  return {
    x: k,
    y: C,
    placement: U,
    strategy: l,
    middlewareData: T,
  }
}
async function ja(n, i) {
  var r
  i === void 0 && (i = {})
  const { x: a, y: l, platform: h, rects: f, elements: v, strategy: m } = n,
    {
      boundary: w = 'clippingAncestors',
      rootBoundary: k = 'viewport',
      elementContext: C = 'floating',
      altBoundary: U = !1,
      padding: T = 0,
    } = Tr(i, n),
    O = td(T),
    x = v[U ? (C === 'floating' ? 'reference' : 'floating') : C],
    P = Co(
      await h.getClippingRect({
        element:
          (r = await (h.isElement == null ? void 0 : h.isElement(x))) == null ||
          r
            ? x
            : x.contextElement ||
              (await (h.getDocumentElement == null
                ? void 0
                : h.getDocumentElement(v.floating))),
        boundary: w,
        rootBoundary: k,
        strategy: m,
      }),
    ),
    G =
      C === 'floating'
        ? {
            ...f.floating,
            x: a,
            y: l,
          }
        : f.reference,
    N = await (h.getOffsetParent == null
      ? void 0
      : h.getOffsetParent(v.floating)),
    it = (await (h.isElement == null ? void 0 : h.isElement(N)))
      ? (await (h.getScale == null ? void 0 : h.getScale(N))) || {
          x: 1,
          y: 1,
        }
      : {
          x: 1,
          y: 1,
        },
    J = Co(
      h.convertOffsetParentRelativeRectToViewportRelativeRect
        ? await h.convertOffsetParentRelativeRectToViewportRelativeRect({
            rect: G,
            offsetParent: N,
            strategy: m,
          })
        : G,
    )
  return {
    top: (P.top - J.top + O.top) / it.y,
    bottom: (J.bottom - P.bottom + O.bottom) / it.y,
    left: (P.left - J.left + O.left) / it.x,
    right: (J.right - P.right + O.right) / it.x,
  }
}
const P$ = n => ({
    name: 'arrow',
    options: n,
    async fn(i) {
      const {
          x: r,
          y: a,
          placement: l,
          rects: h,
          platform: f,
          elements: v,
          middlewareData: m,
        } = i,
        { element: w, padding: k = 0 } = Tr(n, i) || {}
      if (w == null) return {}
      const C = td(k),
        U = {
          x: r,
          y: a,
        },
        T = Za(l),
        O = Xa(T),
        _ = await f.getDimensions(w),
        x = T === 'y',
        P = x ? 'top' : 'left',
        G = x ? 'bottom' : 'right',
        N = x ? 'clientHeight' : 'clientWidth',
        it = h.reference[O] + h.reference[T] - U[T] - h.floating[O],
        J = U[T] - h.reference[T],
        q = await (f.getOffsetParent == null ? void 0 : f.getOffsetParent(w))
      let I = q ? q[N] : 0
      ;(!I || !(await (f.isElement == null ? void 0 : f.isElement(q)))) &&
        (I = v.floating[N] || h.floating[O])
      const M = it / 2 - J / 2,
        nt = I / 2 - _[O] / 2 - 1,
        ot = Pn(C[P], nt),
        j = Pn(C[G], nt),
        mt = ot,
        Et = I - _[O] - j,
        W = I / 2 - _[O] / 2 + M,
        D = _a(mt, W, Et),
        L =
          !m.arrow &&
          Or(l) != null &&
          W != D &&
          h.reference[O] / 2 - (W < mt ? ot : j) - _[O] / 2 < 0,
        H = L ? (W < mt ? W - mt : W - Et) : 0
      return {
        [T]: U[T] + H,
        data: {
          [T]: D,
          centerOffset: W - D - H,
          ...(L && {
            alignmentOffset: H,
          }),
        },
        reset: L,
      }
    },
  }),
  R$ = function (n) {
    return (
      n === void 0 && (n = {}),
      {
        name: 'flip',
        options: n,
        async fn(i) {
          var r, a
          const {
              placement: l,
              middlewareData: h,
              rects: f,
              initialPlacement: v,
              platform: m,
              elements: w,
            } = i,
            {
              mainAxis: k = !0,
              crossAxis: C = !0,
              fallbackPlacements: U,
              fallbackStrategy: T = 'bestFit',
              fallbackAxisSideDirection: O = 'none',
              flipAlignment: _ = !0,
              ...x
            } = Tr(n, i)
          if ((r = h.arrow) != null && r.alignmentOffset) return {}
          const P = Mn(l),
            G = Mn(v) === v,
            N = await (m.isRTL == null ? void 0 : m.isRTL(w.floating)),
            it = U || (G || !_ ? [So(v)] : E$(v))
          !U && O !== 'none' && it.push(...T$(v, _, O, N))
          const J = [v, ...it],
            q = await ja(i, x),
            I = []
          let M = ((a = h.flip) == null ? void 0 : a.overflows) || []
          if ((k && I.push(q[P]), C)) {
            const mt = A$(l, f, N)
            I.push(q[mt[0]], q[mt[1]])
          }
          if (
            ((M = [
              ...M,
              {
                placement: l,
                overflows: I,
              },
            ]),
            !I.every(mt => mt <= 0))
          ) {
            var nt, ot
            const mt = (((nt = h.flip) == null ? void 0 : nt.index) || 0) + 1,
              Et = J[mt]
            if (Et)
              return {
                data: {
                  index: mt,
                  overflows: M,
                },
                reset: {
                  placement: Et,
                },
              }
            let W =
              (ot = M.filter(D => D.overflows[0] <= 0).sort(
                (D, L) => D.overflows[1] - L.overflows[1],
              )[0]) == null
                ? void 0
                : ot.placement
            if (!W)
              switch (T) {
                case 'bestFit': {
                  var j
                  const D =
                    (j = M.map(L => [
                      L.placement,
                      L.overflows.filter(H => H > 0).reduce((H, B) => H + B, 0),
                    ]).sort((L, H) => L[1] - H[1])[0]) == null
                      ? void 0
                      : j[0]
                  D && (W = D)
                  break
                }
                case 'initialPlacement':
                  W = v
                  break
              }
            if (l !== W)
              return {
                reset: {
                  placement: W,
                },
              }
          }
          return {}
        },
      }
    )
  }
async function M$(n, i) {
  const { placement: r, platform: a, elements: l } = n,
    h = await (a.isRTL == null ? void 0 : a.isRTL(l.floating)),
    f = Mn(r),
    v = Or(r),
    m = mi(r) === 'y',
    w = ['left', 'top'].includes(f) ? -1 : 1,
    k = h && m ? -1 : 1,
    C = Tr(i, n)
  let {
    mainAxis: U,
    crossAxis: T,
    alignmentAxis: O,
  } = typeof C == 'number'
    ? {
        mainAxis: C,
        crossAxis: 0,
        alignmentAxis: null,
      }
    : {
        mainAxis: 0,
        crossAxis: 0,
        alignmentAxis: null,
        ...C,
      }
  return (
    v && typeof O == 'number' && (T = v === 'end' ? O * -1 : O),
    m
      ? {
          x: T * k,
          y: U * w,
        }
      : {
          x: U * w,
          y: T * k,
        }
  )
}
const L$ = function (n) {
    return (
      n === void 0 && (n = 0),
      {
        name: 'offset',
        options: n,
        async fn(i) {
          const { x: r, y: a } = i,
            l = await M$(i, n)
          return {
            x: r + l.x,
            y: a + l.y,
            data: l,
          }
        },
      }
    )
  },
  I$ = function (n) {
    return (
      n === void 0 && (n = {}),
      {
        name: 'shift',
        options: n,
        async fn(i) {
          const { x: r, y: a, placement: l } = i,
            {
              mainAxis: h = !0,
              crossAxis: f = !1,
              limiter: v = {
                fn: x => {
                  let { x: P, y: G } = x
                  return {
                    x: P,
                    y: G,
                  }
                },
              },
              ...m
            } = Tr(n, i),
            w = {
              x: r,
              y: a,
            },
            k = await ja(i, m),
            C = mi(Mn(l)),
            U = Qh(C)
          let T = w[U],
            O = w[C]
          if (h) {
            const x = U === 'y' ? 'top' : 'left',
              P = U === 'y' ? 'bottom' : 'right',
              G = T + k[x],
              N = T - k[P]
            T = _a(G, T, N)
          }
          if (f) {
            const x = C === 'y' ? 'top' : 'left',
              P = C === 'y' ? 'bottom' : 'right',
              G = O + k[x],
              N = O - k[P]
            O = _a(G, O, N)
          }
          const _ = v.fn({
            ...i,
            [U]: T,
            [C]: O,
          })
          return {
            ..._,
            data: {
              x: _.x - r,
              y: _.y - a,
            },
          }
        },
      }
    )
  },
  oh = function (n) {
    return (
      n === void 0 && (n = {}),
      {
        name: 'size',
        options: n,
        async fn(i) {
          const { placement: r, rects: a, platform: l, elements: h } = i,
            { apply: f = () => {}, ...v } = Tr(n, i),
            m = await ja(i, v),
            w = Mn(r),
            k = Or(r),
            C = mi(r) === 'y',
            { width: U, height: T } = a.floating
          let O, _
          w === 'top' || w === 'bottom'
            ? ((O = w),
              (_ =
                k ===
                ((await (l.isRTL == null ? void 0 : l.isRTL(h.floating)))
                  ? 'start'
                  : 'end')
                  ? 'left'
                  : 'right'))
            : ((_ = w), (O = k === 'end' ? 'top' : 'bottom'))
          const x = T - m[O],
            P = U - m[_],
            G = !i.middlewareData.shift
          let N = x,
            it = P
          if (C) {
            const q = U - m.left - m.right
            it = k || G ? Pn(P, q) : q
          } else {
            const q = T - m.top - m.bottom
            N = k || G ? Pn(x, q) : q
          }
          if (G && !k) {
            const q = ye(m.left, 0),
              I = ye(m.right, 0),
              M = ye(m.top, 0),
              nt = ye(m.bottom, 0)
            C
              ? (it =
                  U - 2 * (q !== 0 || I !== 0 ? q + I : ye(m.left, m.right)))
              : (N =
                  T - 2 * (M !== 0 || nt !== 0 ? M + nt : ye(m.top, m.bottom)))
          }
          await f({
            ...i,
            availableWidth: it,
            availableHeight: N,
          })
          const J = await l.getDimensions(h.floating)
          return U !== J.width || T !== J.height
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
function Ln(n) {
  return ed(n) ? (n.nodeName || '').toLowerCase() : '#document'
}
function _e(n) {
  var i
  return (
    (n == null || (i = n.ownerDocument) == null ? void 0 : i.defaultView) ||
    window
  )
}
function yn(n) {
  var i
  return (i = (ed(n) ? n.ownerDocument : n.document) || window.document) == null
    ? void 0
    : i.documentElement
}
function ed(n) {
  return n instanceof Node || n instanceof _e(n).Node
}
function vn(n) {
  return n instanceof Element || n instanceof _e(n).Element
}
function tn(n) {
  return n instanceof HTMLElement || n instanceof _e(n).HTMLElement
}
function sh(n) {
  return typeof ShadowRoot > 'u'
    ? !1
    : n instanceof ShadowRoot || n instanceof _e(n).ShadowRoot
}
function bi(n) {
  const { overflow: i, overflowX: r, overflowY: a, display: l } = Re(n)
  return (
    /auto|scroll|overlay|hidden|clip/.test(i + a + r) &&
    !['inline', 'contents'].includes(l)
  )
}
function D$(n) {
  return ['table', 'td', 'th'].includes(Ln(n))
}
function Ja(n) {
  const i = Qa(),
    r = Re(n)
  return (
    r.transform !== 'none' ||
    r.perspective !== 'none' ||
    (r.containerType ? r.containerType !== 'normal' : !1) ||
    (!i && (r.backdropFilter ? r.backdropFilter !== 'none' : !1)) ||
    (!i && (r.filter ? r.filter !== 'none' : !1)) ||
    ['transform', 'perspective', 'filter'].some(a =>
      (r.willChange || '').includes(a),
    ) ||
    ['paint', 'layout', 'strict', 'content'].some(a =>
      (r.contain || '').includes(a),
    )
  )
}
function B$(n) {
  let i = Sr(n)
  for (; tn(i) && !Mo(i); ) {
    if (Ja(i)) return i
    i = Sr(i)
  }
  return null
}
function Qa() {
  return typeof CSS > 'u' || !CSS.supports
    ? !1
    : CSS.supports('-webkit-backdrop-filter', 'none')
}
function Mo(n) {
  return ['html', 'body', '#document'].includes(Ln(n))
}
function Re(n) {
  return _e(n).getComputedStyle(n)
}
function Lo(n) {
  return vn(n)
    ? {
        scrollLeft: n.scrollLeft,
        scrollTop: n.scrollTop,
      }
    : {
        scrollLeft: n.pageXOffset,
        scrollTop: n.pageYOffset,
      }
}
function Sr(n) {
  if (Ln(n) === 'html') return n
  const i =
    // Step into the shadow DOM of the parent of a slotted node.
    n.assignedSlot || // DOM Element detected.
    n.parentNode || // ShadowRoot detected.
    (sh(n) && n.host) || // Fallback.
    yn(n)
  return sh(i) ? i.host : i
}
function nd(n) {
  const i = Sr(n)
  return Mo(i)
    ? n.ownerDocument
      ? n.ownerDocument.body
      : n.body
    : tn(i) && bi(i)
    ? i
    : nd(i)
}
function si(n, i, r) {
  var a
  i === void 0 && (i = []), r === void 0 && (r = !0)
  const l = nd(n),
    h = l === ((a = n.ownerDocument) == null ? void 0 : a.body),
    f = _e(l)
  return h
    ? i.concat(
        f,
        f.visualViewport || [],
        bi(l) ? l : [],
        f.frameElement && r ? si(f.frameElement) : [],
      )
    : i.concat(l, si(l, [], r))
}
function rd(n) {
  const i = Re(n)
  let r = parseFloat(i.width) || 0,
    a = parseFloat(i.height) || 0
  const l = tn(n),
    h = l ? n.offsetWidth : r,
    f = l ? n.offsetHeight : a,
    v = xo(r) !== h || xo(a) !== f
  return (
    v && ((r = h), (a = f)),
    {
      width: r,
      height: a,
      $: v,
    }
  )
}
function tl(n) {
  return vn(n) ? n : n.contextElement
}
function $r(n) {
  const i = tl(n)
  if (!tn(i)) return Rn(1)
  const r = i.getBoundingClientRect(),
    { width: a, height: l, $: h } = rd(i)
  let f = (h ? xo(r.width) : r.width) / a,
    v = (h ? xo(r.height) : r.height) / l
  return (
    (!f || !Number.isFinite(f)) && (f = 1),
    (!v || !Number.isFinite(v)) && (v = 1),
    {
      x: f,
      y: v,
    }
  )
}
const U$ = /* @__PURE__ */ Rn(0)
function id(n) {
  const i = _e(n)
  return !Qa() || !i.visualViewport
    ? U$
    : {
        x: i.visualViewport.offsetLeft,
        y: i.visualViewport.offsetTop,
      }
}
function N$(n, i, r) {
  return i === void 0 && (i = !1), !r || (i && r !== _e(n)) ? !1 : i
}
function er(n, i, r, a) {
  i === void 0 && (i = !1), r === void 0 && (r = !1)
  const l = n.getBoundingClientRect(),
    h = tl(n)
  let f = Rn(1)
  i && (a ? vn(a) && (f = $r(a)) : (f = $r(n)))
  const v = N$(h, r, a) ? id(h) : Rn(0)
  let m = (l.left + v.x) / f.x,
    w = (l.top + v.y) / f.y,
    k = l.width / f.x,
    C = l.height / f.y
  if (h) {
    const U = _e(h),
      T = a && vn(a) ? _e(a) : a
    let O = U.frameElement
    for (; O && a && T !== U; ) {
      const _ = $r(O),
        x = O.getBoundingClientRect(),
        P = Re(O),
        G = x.left + (O.clientLeft + parseFloat(P.paddingLeft)) * _.x,
        N = x.top + (O.clientTop + parseFloat(P.paddingTop)) * _.y
      ;(m *= _.x),
        (w *= _.y),
        (k *= _.x),
        (C *= _.y),
        (m += G),
        (w += N),
        (O = _e(O).frameElement)
    }
  }
  return Co({
    width: k,
    height: C,
    x: m,
    y: w,
  })
}
function F$(n) {
  let { rect: i, offsetParent: r, strategy: a } = n
  const l = tn(r),
    h = yn(r)
  if (r === h) return i
  let f = {
      scrollLeft: 0,
      scrollTop: 0,
    },
    v = Rn(1)
  const m = Rn(0)
  if (
    (l || (!l && a !== 'fixed')) &&
    ((Ln(r) !== 'body' || bi(h)) && (f = Lo(r)), tn(r))
  ) {
    const w = er(r)
    ;(v = $r(r)), (m.x = w.x + r.clientLeft), (m.y = w.y + r.clientTop)
  }
  return {
    width: i.width * v.x,
    height: i.height * v.y,
    x: i.x * v.x - f.scrollLeft * v.x + m.x,
    y: i.y * v.y - f.scrollTop * v.y + m.y,
  }
}
function H$(n) {
  return Array.from(n.getClientRects())
}
function od(n) {
  return er(yn(n)).left + Lo(n).scrollLeft
}
function W$(n) {
  const i = yn(n),
    r = Lo(n),
    a = n.ownerDocument.body,
    l = ye(i.scrollWidth, i.clientWidth, a.scrollWidth, a.clientWidth),
    h = ye(i.scrollHeight, i.clientHeight, a.scrollHeight, a.clientHeight)
  let f = -r.scrollLeft + od(n)
  const v = -r.scrollTop
  return (
    Re(a).direction === 'rtl' && (f += ye(i.clientWidth, a.clientWidth) - l),
    {
      width: l,
      height: h,
      x: f,
      y: v,
    }
  )
}
function q$(n, i) {
  const r = _e(n),
    a = yn(n),
    l = r.visualViewport
  let h = a.clientWidth,
    f = a.clientHeight,
    v = 0,
    m = 0
  if (l) {
    ;(h = l.width), (f = l.height)
    const w = Qa()
    ;(!w || (w && i === 'fixed')) && ((v = l.offsetLeft), (m = l.offsetTop))
  }
  return {
    width: h,
    height: f,
    x: v,
    y: m,
  }
}
function Y$(n, i) {
  const r = er(n, !0, i === 'fixed'),
    a = r.top + n.clientTop,
    l = r.left + n.clientLeft,
    h = tn(n) ? $r(n) : Rn(1),
    f = n.clientWidth * h.x,
    v = n.clientHeight * h.y,
    m = l * h.x,
    w = a * h.y
  return {
    width: f,
    height: v,
    x: m,
    y: w,
  }
}
function ah(n, i, r) {
  let a
  if (i === 'viewport') a = q$(n, r)
  else if (i === 'document') a = W$(yn(n))
  else if (vn(i)) a = Y$(i, r)
  else {
    const l = id(n)
    a = {
      ...i,
      x: i.x - l.x,
      y: i.y - l.y,
    }
  }
  return Co(a)
}
function sd(n, i) {
  const r = Sr(n)
  return r === i || !vn(r) || Mo(r)
    ? !1
    : Re(r).position === 'fixed' || sd(r, i)
}
function G$(n, i) {
  const r = i.get(n)
  if (r) return r
  let a = si(n, [], !1).filter(v => vn(v) && Ln(v) !== 'body'),
    l = null
  const h = Re(n).position === 'fixed'
  let f = h ? Sr(n) : n
  for (; vn(f) && !Mo(f); ) {
    const v = Re(f),
      m = Ja(f)
    !m && v.position === 'fixed' && (l = null),
      (
        h
          ? !m && !l
          : (!m &&
              v.position === 'static' &&
              !!l &&
              ['absolute', 'fixed'].includes(l.position)) ||
            (bi(f) && !m && sd(n, f))
      )
        ? (a = a.filter(k => k !== f))
        : (l = v),
      (f = Sr(f))
  }
  return i.set(n, a), a
}
function K$(n) {
  let { element: i, boundary: r, rootBoundary: a, strategy: l } = n
  const f = [...(r === 'clippingAncestors' ? G$(i, this._c) : [].concat(r)), a],
    v = f[0],
    m = f.reduce(
      (w, k) => {
        const C = ah(i, k, l)
        return (
          (w.top = ye(C.top, w.top)),
          (w.right = Pn(C.right, w.right)),
          (w.bottom = Pn(C.bottom, w.bottom)),
          (w.left = ye(C.left, w.left)),
          w
        )
      },
      ah(i, v, l),
    )
  return {
    width: m.right - m.left,
    height: m.bottom - m.top,
    x: m.left,
    y: m.top,
  }
}
function V$(n) {
  return rd(n)
}
function X$(n, i, r) {
  const a = tn(i),
    l = yn(i),
    h = r === 'fixed',
    f = er(n, !0, h, i)
  let v = {
    scrollLeft: 0,
    scrollTop: 0,
  }
  const m = Rn(0)
  if (a || (!a && !h))
    if (((Ln(i) !== 'body' || bi(l)) && (v = Lo(i)), a)) {
      const w = er(i, !0, h, i)
      ;(m.x = w.x + i.clientLeft), (m.y = w.y + i.clientTop)
    } else l && (m.x = od(l))
  return {
    x: f.left + v.scrollLeft - m.x,
    y: f.top + v.scrollTop - m.y,
    width: f.width,
    height: f.height,
  }
}
function lh(n, i) {
  return !tn(n) || Re(n).position === 'fixed' ? null : i ? i(n) : n.offsetParent
}
function ad(n, i) {
  const r = _e(n)
  if (!tn(n)) return r
  let a = lh(n, i)
  for (; a && D$(a) && Re(a).position === 'static'; ) a = lh(a, i)
  return a &&
    (Ln(a) === 'html' ||
      (Ln(a) === 'body' && Re(a).position === 'static' && !Ja(a)))
    ? r
    : a || B$(n) || r
}
const Z$ = async function (n) {
  let { reference: i, floating: r, strategy: a } = n
  const l = this.getOffsetParent || ad,
    h = this.getDimensions
  return {
    reference: X$(i, await l(r), a),
    floating: {
      x: 0,
      y: 0,
      ...(await h(r)),
    },
  }
}
function j$(n) {
  return Re(n).direction === 'rtl'
}
const go = {
  convertOffsetParentRelativeRectToViewportRelativeRect: F$,
  getDocumentElement: yn,
  getClippingRect: K$,
  getOffsetParent: ad,
  getElementRects: Z$,
  getClientRects: H$,
  getDimensions: V$,
  getScale: $r,
  isElement: vn,
  isRTL: j$,
}
function J$(n, i) {
  let r = null,
    a
  const l = yn(n)
  function h() {
    clearTimeout(a), r && r.disconnect(), (r = null)
  }
  function f(v, m) {
    v === void 0 && (v = !1), m === void 0 && (m = 1), h()
    const { left: w, top: k, width: C, height: U } = n.getBoundingClientRect()
    if ((v || i(), !C || !U)) return
    const T = uo(k),
      O = uo(l.clientWidth - (w + C)),
      _ = uo(l.clientHeight - (k + U)),
      x = uo(w),
      G = {
        rootMargin: -T + 'px ' + -O + 'px ' + -_ + 'px ' + -x + 'px',
        threshold: ye(0, Pn(1, m)) || 1,
      }
    let N = !0
    function it(J) {
      const q = J[0].intersectionRatio
      if (q !== m) {
        if (!N) return f()
        q
          ? f(!1, q)
          : (a = setTimeout(() => {
              f(!1, 1e-7)
            }, 100))
      }
      N = !1
    }
    try {
      r = new IntersectionObserver(it, {
        ...G,
        // Handle <iframe>s
        root: l.ownerDocument,
      })
    } catch {
      r = new IntersectionObserver(it, G)
    }
    r.observe(n)
  }
  return f(!0), h
}
function Q$(n, i, r, a) {
  a === void 0 && (a = {})
  const {
      ancestorScroll: l = !0,
      ancestorResize: h = !0,
      elementResize: f = typeof ResizeObserver == 'function',
      layoutShift: v = typeof IntersectionObserver == 'function',
      animationFrame: m = !1,
    } = a,
    w = tl(n),
    k = l || h ? [...(w ? si(w) : []), ...si(i)] : []
  k.forEach(P => {
    l &&
      P.addEventListener('scroll', r, {
        passive: !0,
      }),
      h && P.addEventListener('resize', r)
  })
  const C = w && v ? J$(w, r) : null
  let U = -1,
    T = null
  f &&
    ((T = new ResizeObserver(P => {
      let [G] = P
      G &&
        G.target === w &&
        T &&
        (T.unobserve(i),
        cancelAnimationFrame(U),
        (U = requestAnimationFrame(() => {
          T && T.observe(i)
        }))),
        r()
    })),
    w && !m && T.observe(w),
    T.observe(i))
  let O,
    _ = m ? er(n) : null
  m && x()
  function x() {
    const P = er(n)
    _ &&
      (P.x !== _.x ||
        P.y !== _.y ||
        P.width !== _.width ||
        P.height !== _.height) &&
      r(),
      (_ = P),
      (O = requestAnimationFrame(x))
  }
  return (
    r(),
    () => {
      k.forEach(P => {
        l && P.removeEventListener('scroll', r),
          h && P.removeEventListener('resize', r)
      }),
        C && C(),
        T && T.disconnect(),
        (T = null),
        m && cancelAnimationFrame(O)
    }
  )
}
const tx = (n, i, r) => {
  const a = /* @__PURE__ */ new Map(),
    l = {
      platform: go,
      ...r,
    },
    h = {
      ...l.platform,
      _c: a,
    }
  return z$(n, i, {
    ...l,
    platform: h,
  })
}
/**
 * @license
 * Copyright 2017 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const ex = {
    ATTRIBUTE: 1,
    CHILD: 2,
    PROPERTY: 3,
    BOOLEAN_ATTRIBUTE: 4,
    EVENT: 5,
    ELEMENT: 6,
  },
  nx =
    n =>
    (...i) => ({ _$litDirective$: n, values: i })
let rx = class {
  constructor(i) {}
  get _$AU() {
    return this._$AM._$AU
  }
  _$AT(i, r, a) {
    ;(this._$Ct = i), (this._$AM = r), (this._$Ci = a)
  }
  _$AS(i, r) {
    return this.update(i, r)
  }
  update(i, r) {
    return this.render(...r)
  }
}
/**
 * @license
 * Copyright 2018 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const gn = nx(
  class extends rx {
    constructor(n) {
      var i
      if (
        (super(n),
        n.type !== ex.ATTRIBUTE ||
          n.name !== 'class' ||
          ((i = n.strings) == null ? void 0 : i.length) > 2)
      )
        throw Error(
          '`classMap()` can only be used in the `class` attribute and must be the only part in the attribute.',
        )
    }
    render(n) {
      return (
        ' ' +
        Object.keys(n)
          .filter(i => n[i])
          .join(' ') +
        ' '
      )
    }
    update(n, [i]) {
      var a, l
      if (this.st === void 0) {
        ;(this.st = /* @__PURE__ */ new Set()),
          n.strings !== void 0 &&
            (this.nt = new Set(
              n.strings
                .join(' ')
                .split(/\s/)
                .filter(h => h !== ''),
            ))
        for (const h in i)
          i[h] && !((a = this.nt) != null && a.has(h)) && this.st.add(h)
        return this.render(i)
      }
      const r = n.element.classList
      for (const h of this.st) h in i || (r.remove(h), this.st.delete(h))
      for (const h in i) {
        const f = !!i[h]
        f === this.st.has(h) ||
          ((l = this.nt) != null && l.has(h)) ||
          (f ? (r.add(h), this.st.add(h)) : (r.remove(h), this.st.delete(h)))
      }
      return tr
    }
  },
)
function ix(n) {
  return ox(n)
}
function ha(n) {
  return n.assignedSlot
    ? n.assignedSlot
    : n.parentNode instanceof ShadowRoot
    ? n.parentNode.host
    : n.parentNode
}
function ox(n) {
  for (let i = n; i; i = ha(i))
    if (i instanceof Element && getComputedStyle(i).display === 'none')
      return null
  for (let i = ha(n); i; i = ha(i)) {
    if (!(i instanceof Element)) continue
    const r = getComputedStyle(i)
    if (
      r.display !== 'contents' &&
      (r.position !== 'static' || r.filter !== 'none' || i.tagName === 'BODY')
    )
      return i
  }
  return null
}
function sx(n) {
  return (
    n !== null &&
    typeof n == 'object' &&
    'getBoundingClientRect' in n &&
    ('contextElement' in n ? n instanceof Element : !0)
  )
}
var Ct = class extends xe {
  constructor() {
    super(...arguments),
      (this.localize = new vi(this)),
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
          const n = this.anchorEl.getBoundingClientRect(),
            i = this.popup.getBoundingClientRect(),
            r =
              this.placement.includes('top') ||
              this.placement.includes('bottom')
          let a = 0,
            l = 0,
            h = 0,
            f = 0,
            v = 0,
            m = 0,
            w = 0,
            k = 0
          r
            ? n.top < i.top
              ? ((a = n.left),
                (l = n.bottom),
                (h = n.right),
                (f = n.bottom),
                (v = i.left),
                (m = i.top),
                (w = i.right),
                (k = i.top))
              : ((a = i.left),
                (l = i.bottom),
                (h = i.right),
                (f = i.bottom),
                (v = n.left),
                (m = n.top),
                (w = n.right),
                (k = n.top))
            : n.left < i.left
            ? ((a = n.right),
              (l = n.top),
              (h = i.left),
              (f = i.top),
              (v = n.right),
              (m = n.bottom),
              (w = i.left),
              (k = i.bottom))
            : ((a = i.right),
              (l = i.top),
              (h = n.left),
              (f = n.top),
              (v = i.right),
              (m = i.bottom),
              (w = n.left),
              (k = n.bottom)),
            this.style.setProperty('--hover-bridge-top-left-x', `${a}px`),
            this.style.setProperty('--hover-bridge-top-left-y', `${l}px`),
            this.style.setProperty('--hover-bridge-top-right-x', `${h}px`),
            this.style.setProperty('--hover-bridge-top-right-y', `${f}px`),
            this.style.setProperty('--hover-bridge-bottom-left-x', `${v}px`),
            this.style.setProperty('--hover-bridge-bottom-left-y', `${m}px`),
            this.style.setProperty('--hover-bridge-bottom-right-x', `${w}px`),
            this.style.setProperty('--hover-bridge-bottom-right-y', `${k}px`)
        }
      })
  }
  async connectedCallback() {
    super.connectedCallback(), await this.updateComplete, this.start()
  }
  disconnectedCallback() {
    super.disconnectedCallback(), this.stop()
  }
  async updated(n) {
    super.updated(n),
      n.has('active') && (this.active ? this.start() : this.stop()),
      n.has('anchor') && this.handleAnchorChange(),
      this.active && (await this.updateComplete, this.reposition())
  }
  async handleAnchorChange() {
    if ((await this.stop(), this.anchor && typeof this.anchor == 'string')) {
      const n = this.getRootNode()
      this.anchorEl = n.getElementById(this.anchor)
    } else
      this.anchor instanceof Element || sx(this.anchor)
        ? (this.anchorEl = this.anchor)
        : (this.anchorEl = this.querySelector('[slot="anchor"]'))
    this.anchorEl instanceof HTMLSlotElement &&
      (this.anchorEl = this.anchorEl.assignedElements({ flatten: !0 })[0]),
      this.anchorEl && this.active && this.start()
  }
  start() {
    this.anchorEl &&
      (this.cleanup = Q$(this.anchorEl, this.popup, () => {
        this.reposition()
      }))
  }
  async stop() {
    return new Promise(n => {
      this.cleanup
        ? (this.cleanup(),
          (this.cleanup = void 0),
          this.removeAttribute('data-current-placement'),
          this.style.removeProperty('--auto-size-available-width'),
          this.style.removeProperty('--auto-size-available-height'),
          requestAnimationFrame(() => n()))
        : n()
    })
  }
  /** Forces the popup to recalculate and reposition itself. */
  reposition() {
    if (!this.active || !this.anchorEl) return
    const n = [
      // The offset middleware goes first
      L$({ mainAxis: this.distance, crossAxis: this.skidding }),
    ]
    this.sync
      ? n.push(
          oh({
            apply: ({ rects: r }) => {
              const a = this.sync === 'width' || this.sync === 'both',
                l = this.sync === 'height' || this.sync === 'both'
              ;(this.popup.style.width = a ? `${r.reference.width}px` : ''),
                (this.popup.style.height = l ? `${r.reference.height}px` : '')
            },
          }),
        )
      : ((this.popup.style.width = ''), (this.popup.style.height = '')),
      this.flip &&
        n.push(
          R$({
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
        n.push(
          I$({
            boundary: this.shiftBoundary,
            padding: this.shiftPadding,
          }),
        ),
      this.autoSize
        ? n.push(
            oh({
              boundary: this.autoSizeBoundary,
              padding: this.autoSizePadding,
              apply: ({ availableWidth: r, availableHeight: a }) => {
                this.autoSize === 'vertical' || this.autoSize === 'both'
                  ? this.style.setProperty(
                      '--auto-size-available-height',
                      `${a}px`,
                    )
                  : this.style.removeProperty('--auto-size-available-height'),
                  this.autoSize === 'horizontal' || this.autoSize === 'both'
                    ? this.style.setProperty(
                        '--auto-size-available-width',
                        `${r}px`,
                      )
                    : this.style.removeProperty('--auto-size-available-width')
              },
            }),
          )
        : (this.style.removeProperty('--auto-size-available-width'),
          this.style.removeProperty('--auto-size-available-height')),
      this.arrow &&
        n.push(
          P$({
            element: this.arrowEl,
            padding: this.arrowPadding,
          }),
        )
    const i =
      this.strategy === 'absolute'
        ? r => go.getOffsetParent(r, ix)
        : go.getOffsetParent
    tx(this.anchorEl, this.popup, {
      placement: this.placement,
      middleware: n,
      strategy: this.strategy,
      platform: Gh(pi({}, go), {
        getOffsetParent: i,
      }),
    }).then(({ x: r, y: a, middlewareData: l, placement: h }) => {
      const f = this.localize.dir() === 'rtl',
        v = { top: 'bottom', right: 'left', bottom: 'top', left: 'right' }[
          h.split('-')[0]
        ]
      if (
        (this.setAttribute('data-current-placement', h),
        Object.assign(this.popup.style, {
          left: `${r}px`,
          top: `${a}px`,
        }),
        this.arrow)
      ) {
        const m = l.arrow.x,
          w = l.arrow.y
        let k = '',
          C = '',
          U = '',
          T = ''
        if (this.arrowPlacement === 'start') {
          const O =
            typeof m == 'number'
              ? `calc(${this.arrowPadding}px - var(--arrow-padding-offset))`
              : ''
          ;(k =
            typeof w == 'number'
              ? `calc(${this.arrowPadding}px - var(--arrow-padding-offset))`
              : ''),
            (C = f ? O : ''),
            (T = f ? '' : O)
        } else if (this.arrowPlacement === 'end') {
          const O =
            typeof m == 'number'
              ? `calc(${this.arrowPadding}px - var(--arrow-padding-offset))`
              : ''
          ;(C = f ? '' : O),
            (T = f ? O : ''),
            (U =
              typeof w == 'number'
                ? `calc(${this.arrowPadding}px - var(--arrow-padding-offset))`
                : '')
        } else
          this.arrowPlacement === 'center'
            ? ((T =
                typeof m == 'number'
                  ? 'calc(50% - var(--arrow-size-diagonal))'
                  : ''),
              (k =
                typeof w == 'number'
                  ? 'calc(50% - var(--arrow-size-diagonal))'
                  : ''))
            : ((T = typeof m == 'number' ? `${m}px` : ''),
              (k = typeof w == 'number' ? `${w}px` : ''))
        Object.assign(this.arrowEl.style, {
          top: k,
          right: C,
          bottom: U,
          left: T,
          [v]: 'calc(var(--arrow-size-diagonal) * -1)',
        })
      }
    }),
      requestAnimationFrame(() => this.updateHoverBridge()),
      this.emit('sl-reposition')
  }
  render() {
    return X`
      <slot name="anchor" @slotchange=${this.handleAnchorChange}></slot>

      <span
        part="hover-bridge"
        class=${gn({
          'popup-hover-bridge': !0,
          'popup-hover-bridge--visible': this.hoverBridge && this.active,
        })}
      ></span>

      <div
        part="popup"
        class=${gn({
          popup: !0,
          'popup--active': this.active,
          'popup--fixed': this.strategy === 'fixed',
          'popup--has-arrow': this.arrow,
        })}
      >
        <slot></slot>
        ${
          this.arrow
            ? X`<div part="arrow" class="popup__arrow" role="presentation"></div>`
            : ''
        }
      </div>
    `
  }
}
Ct.styles = [bn, _$]
R([Le('.popup')], Ct.prototype, 'popup', 2)
R([Le('.popup__arrow')], Ct.prototype, 'arrowEl', 2)
R([V()], Ct.prototype, 'anchor', 2)
R([V({ type: Boolean, reflect: !0 })], Ct.prototype, 'active', 2)
R([V({ reflect: !0 })], Ct.prototype, 'placement', 2)
R([V({ reflect: !0 })], Ct.prototype, 'strategy', 2)
R([V({ type: Number })], Ct.prototype, 'distance', 2)
R([V({ type: Number })], Ct.prototype, 'skidding', 2)
R([V({ type: Boolean })], Ct.prototype, 'arrow', 2)
R([V({ attribute: 'arrow-placement' })], Ct.prototype, 'arrowPlacement', 2)
R(
  [V({ attribute: 'arrow-padding', type: Number })],
  Ct.prototype,
  'arrowPadding',
  2,
)
R([V({ type: Boolean })], Ct.prototype, 'flip', 2)
R(
  [
    V({
      attribute: 'flip-fallback-placements',
      converter: {
        fromAttribute: n =>
          n
            .split(' ')
            .map(i => i.trim())
            .filter(i => i !== ''),
        toAttribute: n => n.join(' '),
      },
    }),
  ],
  Ct.prototype,
  'flipFallbackPlacements',
  2,
)
R(
  [V({ attribute: 'flip-fallback-strategy' })],
  Ct.prototype,
  'flipFallbackStrategy',
  2,
)
R([V({ type: Object })], Ct.prototype, 'flipBoundary', 2)
R(
  [V({ attribute: 'flip-padding', type: Number })],
  Ct.prototype,
  'flipPadding',
  2,
)
R([V({ type: Boolean })], Ct.prototype, 'shift', 2)
R([V({ type: Object })], Ct.prototype, 'shiftBoundary', 2)
R(
  [V({ attribute: 'shift-padding', type: Number })],
  Ct.prototype,
  'shiftPadding',
  2,
)
R([V({ attribute: 'auto-size' })], Ct.prototype, 'autoSize', 2)
R([V()], Ct.prototype, 'sync', 2)
R([V({ type: Object })], Ct.prototype, 'autoSizeBoundary', 2)
R(
  [V({ attribute: 'auto-size-padding', type: Number })],
  Ct.prototype,
  'autoSizePadding',
  2,
)
R(
  [V({ attribute: 'hover-bridge', type: Boolean })],
  Ct.prototype,
  'hoverBridge',
  2,
)
var ld = /* @__PURE__ */ new Map(),
  ax = /* @__PURE__ */ new WeakMap()
function lx(n) {
  return n ?? { keyframes: [], options: { duration: 0 } }
}
function ch(n, i) {
  return i.toLowerCase() === 'rtl'
    ? {
        keyframes: n.rtlKeyframes || n.keyframes,
        options: n.options,
      }
    : n
}
function Io(n, i) {
  ld.set(n, lx(i))
}
function uh(n, i, r) {
  const a = ax.get(n)
  if (a != null && a[i]) return ch(a[i], r.dir)
  const l = ld.get(i)
  return l
    ? ch(l, r.dir)
    : {
        keyframes: [],
        options: { duration: 0 },
      }
}
function hh(n, i) {
  return new Promise(r => {
    function a(l) {
      l.target === n && (n.removeEventListener(i, a), r())
    }
    n.addEventListener(i, a)
  })
}
function dh(n, i, r) {
  return new Promise(a => {
    if ((r == null ? void 0 : r.duration) === 1 / 0)
      throw new Error('Promise-based animations must be finite.')
    const l = n.animate(
      i,
      Gh(pi({}, r), {
        duration: cx() ? 0 : r.duration,
      }),
    )
    l.addEventListener('cancel', a, { once: !0 }),
      l.addEventListener('finish', a, { once: !0 })
  })
}
function fh(n) {
  return (
    (n = n.toString().toLowerCase()),
    n.indexOf('ms') > -1
      ? parseFloat(n)
      : n.indexOf('s') > -1
      ? parseFloat(n) * 1e3
      : parseFloat(n)
  )
}
function cx() {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches
}
function ph(n) {
  return Promise.all(
    n.getAnimations().map(
      i =>
        new Promise(r => {
          i.cancel(), requestAnimationFrame(r)
        }),
    ),
  )
}
var Ht = class extends xe {
  constructor() {
    super(),
      (this.localize = new vi(this)),
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
      (this.handleDocumentKeyDown = n => {
        n.key === 'Escape' && (n.stopPropagation(), this.hide())
      }),
      (this.handleMouseOver = () => {
        if (this.hasTrigger('hover')) {
          const n = fh(getComputedStyle(this).getPropertyValue('--show-delay'))
          clearTimeout(this.hoverTimeout),
            (this.hoverTimeout = window.setTimeout(() => this.show(), n))
        }
      }),
      (this.handleMouseOut = () => {
        if (this.hasTrigger('hover')) {
          const n = fh(getComputedStyle(this).getPropertyValue('--hide-delay'))
          clearTimeout(this.hoverTimeout),
            (this.hoverTimeout = window.setTimeout(() => this.hide(), n))
        }
      }),
      this.addEventListener('blur', this.handleBlur, !0),
      this.addEventListener('focus', this.handleFocus, !0),
      this.addEventListener('click', this.handleClick),
      this.addEventListener('mouseover', this.handleMouseOver),
      this.addEventListener('mouseout', this.handleMouseOut)
  }
  disconnectedCallback() {
    var n
    super.disconnectedCallback(),
      (n = this.closeWatcher) == null || n.destroy(),
      document.removeEventListener('keydown', this.handleDocumentKeyDown)
  }
  firstUpdated() {
    ;(this.body.hidden = !this.open),
      this.open && ((this.popup.active = !0), this.popup.reposition())
  }
  hasTrigger(n) {
    return this.trigger.split(' ').includes(n)
  }
  async handleOpenChange() {
    var n, i
    if (this.open) {
      if (this.disabled) return
      this.emit('sl-show'),
        'CloseWatcher' in window
          ? ((n = this.closeWatcher) == null || n.destroy(),
            (this.closeWatcher = new CloseWatcher()),
            (this.closeWatcher.onclose = () => {
              this.hide()
            }))
          : document.addEventListener('keydown', this.handleDocumentKeyDown),
        await ph(this.body),
        (this.body.hidden = !1),
        (this.popup.active = !0)
      const { keyframes: r, options: a } = uh(this, 'tooltip.show', {
        dir: this.localize.dir(),
      })
      await dh(this.popup.popup, r, a),
        this.popup.reposition(),
        this.emit('sl-after-show')
    } else {
      this.emit('sl-hide'),
        (i = this.closeWatcher) == null || i.destroy(),
        document.removeEventListener('keydown', this.handleDocumentKeyDown),
        await ph(this.body)
      const { keyframes: r, options: a } = uh(this, 'tooltip.hide', {
        dir: this.localize.dir(),
      })
      await dh(this.popup.popup, r, a),
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
    if (!this.open) return (this.open = !0), hh(this, 'sl-after-show')
  }
  /** Hides the tooltip */
  async hide() {
    if (this.open) return (this.open = !1), hh(this, 'sl-after-hide')
  }
  //
  // NOTE: Tooltip is a bit unique in that we're using aria-live instead of aria-labelledby to trick screen readers into
  // announcing the content. It works really well, but it violates an accessibility rule. We're also adding the
  // aria-describedby attribute to a slot, which is required by <sl-popup> to correctly locate the first assigned
  // element, otherwise positioning is incorrect.
  //
  render() {
    return X`
      <sl-popup
        part="base"
        exportparts="
          popup:base__popup,
          arrow:base__arrow
        "
        class=${gn({
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
Ht.styles = [bn, y$]
Ht.dependencies = { 'sl-popup': Ct }
R([Le('slot:not([name])')], Ht.prototype, 'defaultSlot', 2)
R([Le('.tooltip__body')], Ht.prototype, 'body', 2)
R([Le('sl-popup')], Ht.prototype, 'popup', 2)
R([V()], Ht.prototype, 'content', 2)
R([V()], Ht.prototype, 'placement', 2)
R([V({ type: Boolean, reflect: !0 })], Ht.prototype, 'disabled', 2)
R([V({ type: Number })], Ht.prototype, 'distance', 2)
R([V({ type: Boolean, reflect: !0 })], Ht.prototype, 'open', 2)
R([V({ type: Number })], Ht.prototype, 'skidding', 2)
R([V()], Ht.prototype, 'trigger', 2)
R([V({ type: Boolean })], Ht.prototype, 'hoist', 2)
R(
  [le('open', { waitUntilFirstUpdate: !0 })],
  Ht.prototype,
  'handleOpenChange',
  1,
)
R(
  [le(['content', 'distance', 'hoist', 'placement', 'skidding'])],
  Ht.prototype,
  'handleOptionsChange',
  1,
)
R([le('disabled')], Ht.prototype, 'handleDisabledChange', 1)
Io('tooltip.show', {
  keyframes: [
    { opacity: 0, scale: 0.8 },
    { opacity: 1, scale: 1 },
  ],
  options: { duration: 150, easing: 'ease' },
})
Io('tooltip.hide', {
  keyframes: [
    { opacity: 1, scale: 1 },
    { opacity: 0, scale: 0.8 },
  ],
  options: { duration: 150, easing: 'ease' },
})
const ux = `:host {
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
Io('tooltip.show', {
  keyframes: [{ opacity: 0 }, { opacity: 1 }],
  options: { duration: 150, easing: 'ease-in-out' },
})
Io('tooltip.hide', {
  keyframes: [{ opacity: 1 }, { opacity: 0 }],
  options: { duration: 200, transorm: '', easing: 'ease-in-out' },
})
class $a extends mn(Ht, fi) {
  constructor() {
    super(), (this.enterable = !1)
  }
}
Y($a, 'styles', [Dt(), Ht.styles, pt(ux)]),
  Y($a, 'properties', {
    ...Ht.properties,
    enterable: { type: Boolean, reflect: !0 },
  })
customElements.define('tbk-tooltip', $a)
const hx = `:host {
  --information-font-size: var(--font-size);

  display: block;
}
[part='base'] {
  color: var(--color-header);
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
class xa extends $e {
  constructor() {
    super(),
      (this.text = ''),
      (this.side = Je.Right),
      (this.size = Lt.S),
      (this._hasTooltip = !1)
  }
  _renderInfo() {
    return this._hasTooltip || this.text
      ? X`
        <tbk-tooltip
          part="info"
          placement="right"
          hoist
        >
          <div slot="content">
            <slot
              @slotchange="${this.handleSlotchange}"
              name="content"
              >${this.text}</slot
            >
          </div>
          <tbk-icon
            library="heroicons"
            name="information-circle"
          ></tbk-icon>
        </tbk-tooltip>
      `
      : X`<slot
        @slotchange="${this.handleSlotchange}"
        name="content"
      ></slot>`
  }
  handleSlotchange(i) {
    this._hasTooltip = !0
  }
  render() {
    return X`
      <span part="base">
        ${zt(this.side === Je.Left, this._renderInfo())}
        <slot></slot>
        ${zt(this.side === Je.Right, this._renderInfo())}
      </span>
    `
  }
}
Y(xa, 'styles', [Dt(), Me(), pt(hx)]),
  Y(xa, 'properties', {
    text: { type: String },
    side: { type: String, reflect: !0 },
    size: { type: String, reflect: !0 },
    _hasTooltip: { type: Boolean, state: !0 },
  })
customElements.define('tbk-information', xa)
const dx = `:host {
  --model-name-font-size: var(--font-size);
  --model-name-font-weight: var(--font-weight);
  --model-name-font-family: var(--font-accent);

  display: inline-flex;
  align-items: center;
  gap: var(--step);
  min-height: var(--step-2);
  line-height: 1;
  white-space: nowrap;
  font-family: var(--model-name-font-family);
  font-weight: var(--model-name-font-weight);
  width: 100%;
}
[part='icon'] {
  display: flex;
  flex-shrink: 0;
  font-size: calc(var(--model-name-font-size) * 1.25);
  color: var(--color-gray-500);
}
:host([hide-catalog]) [part='icon'],
:host([hide-catalog]) [part='icon'],
:host([collapse-catalog]) [part='icon'],
:host([collapse-schema]) [part='icon'] {
  color: var(--color-deep-blue-800);
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
  background: var(--color-gray-5);
  padding: var(--half) var(--step);
  border-radius: var(--radius-2xs);
}
:host([highlighted-model]) [part='model'] > span {
  background: var(--color-gray-5);
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
}
[part='icon'],
[part='model'] {
  overflow: hidden;
  display: flex;
  align-items: center;
  color: var(--color-deep-blue-500);
}
[part='schema'] {
  color: var(--color-deep-blue-600);
}
:host([reduce-color]) [part='schema'] {
  color: var(--color-gray-500);
}
[part='catalog'] {
  color: var(--color-deep-blue-800);
}
:host([reduce-color]) [part='catalog'] {
  color: var(--color-gray-500);
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
  background: var(--color-gray-5);
  flex-shrink: 0;
  padding: 0 var(--step);
  border-radius: var(--radius-2xs);
}
tbk-icon[part='ellipsis']:hover {
  background: var(--color-gray-10);
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
  background: var(--color-gray-5);
  border-radius: var(--radius-2xs);
}
`
var Ao = { exports: {} }
/**
 * @license
 * Lodash <https://lodash.com/>
 * Copyright OpenJS Foundation and other contributors <https://openjsf.org/>
 * Released under MIT license <https://lodash.com/license>
 * Based on Underscore.js 1.8.3 <http://underscorejs.org/LICENSE>
 * Copyright Jeremy Ashkenas, DocumentCloud and Investigative Reporters & Editors
 */
Ao.exports
;(function (n, i) {
  ;(function () {
    var r,
      a = '4.17.21',
      l = 200,
      h = 'Unsupported core-js use. Try https://npms.io/search?q=ponyfill.',
      f = 'Expected a function',
      v = 'Invalid `variable` option passed into `_.template`',
      m = '__lodash_hash_undefined__',
      w = 500,
      k = '__lodash_placeholder__',
      C = 1,
      U = 2,
      T = 4,
      O = 1,
      _ = 2,
      x = 1,
      P = 2,
      G = 4,
      N = 8,
      it = 16,
      J = 32,
      q = 64,
      I = 128,
      M = 256,
      nt = 512,
      ot = 30,
      j = '...',
      mt = 800,
      Et = 16,
      W = 1,
      D = 2,
      L = 3,
      H = 1 / 0,
      B = 9007199254740991,
      at = 17976931348623157e292,
      rt = NaN,
      dt = 4294967295,
      Tt = dt - 1,
      It = dt >>> 1,
      Yt = [
        ['ary', I],
        ['bind', x],
        ['bindKey', P],
        ['curry', N],
        ['curryRight', it],
        ['flip', nt],
        ['partial', J],
        ['partialRight', q],
        ['rearg', M],
      ],
      Bt = '[object Arguments]',
      Ie = '[object Array]',
      nn = '[object AsyncFunction]',
      De = '[object Boolean]',
      he = '[object Date]',
      Zt = '[object DOMException]',
      de = '[object Error]',
      Ke = '[object Function]',
      Bn = '[object GeneratorFunction]',
      Be = '[object Map]',
      Pr = '[object Number]',
      hd = '[object Null]',
      rn = '[object Object]',
      el = '[object Promise]',
      dd = '[object Proxy]',
      Rr = '[object RegExp]',
      Ue = '[object Set]',
      Mr = '[object String]',
      yi = '[object Symbol]',
      fd = '[object Undefined]',
      Lr = '[object WeakMap]',
      pd = '[object WeakSet]',
      Ir = '[object ArrayBuffer]',
      nr = '[object DataView]',
      Do = '[object Float32Array]',
      Bo = '[object Float64Array]',
      Uo = '[object Int8Array]',
      No = '[object Int16Array]',
      Fo = '[object Int32Array]',
      Ho = '[object Uint8Array]',
      Wo = '[object Uint8ClampedArray]',
      qo = '[object Uint16Array]',
      Yo = '[object Uint32Array]',
      gd = /\b__p \+= '';/g,
      vd = /\b(__p \+=) '' \+/g,
      md = /(__e\(.*?\)|\b__t\)) \+\n'';/g,
      nl = /&(?:amp|lt|gt|quot|#39);/g,
      rl = /[&<>"']/g,
      bd = RegExp(nl.source),
      yd = RegExp(rl.source),
      _d = /<%-([\s\S]+?)%>/g,
      wd = /<%([\s\S]+?)%>/g,
      il = /<%=([\s\S]+?)%>/g,
      $d = /\.|\[(?:[^[\]]*|(["'])(?:(?!\1)[^\\]|\\.)*?\1)\]/,
      xd = /^\w*$/,
      Sd =
        /[^.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\\]|\\.)*?)\2)\]|(?=(?:\.|\[\])(?:\.|\[\]|$))/g,
      Go = /[\\^$.*+?()[\]{}|]/g,
      Cd = RegExp(Go.source),
      Ko = /^\s+/,
      Ad = /\s/,
      Ed = /\{(?:\n\/\* \[wrapped with .+\] \*\/)?\n?/,
      kd = /\{\n\/\* \[wrapped with (.+)\] \*/,
      Td = /,? & /,
      Od = /[^\x00-\x2f\x3a-\x40\x5b-\x60\x7b-\x7f]+/g,
      zd = /[()=,{}\[\]\/\s]/,
      Pd = /\\(\\)?/g,
      Rd = /\$\{([^\\}]*(?:\\.[^\\}]*)*)\}/g,
      ol = /\w*$/,
      Md = /^[-+]0x[0-9a-f]+$/i,
      Ld = /^0b[01]+$/i,
      Id = /^\[object .+?Constructor\]$/,
      Dd = /^0o[0-7]+$/i,
      Bd = /^(?:0|[1-9]\d*)$/,
      Ud = /[\xc0-\xd6\xd8-\xf6\xf8-\xff\u0100-\u017f]/g,
      _i = /($^)/,
      Nd = /['\n\r\u2028\u2029\\]/g,
      wi = '\\ud800-\\udfff',
      Fd = '\\u0300-\\u036f',
      Hd = '\\ufe20-\\ufe2f',
      Wd = '\\u20d0-\\u20ff',
      sl = Fd + Hd + Wd,
      al = '\\u2700-\\u27bf',
      ll = 'a-z\\xdf-\\xf6\\xf8-\\xff',
      qd = '\\xac\\xb1\\xd7\\xf7',
      Yd = '\\x00-\\x2f\\x3a-\\x40\\x5b-\\x60\\x7b-\\xbf',
      Gd = '\\u2000-\\u206f',
      Kd =
        ' \\t\\x0b\\f\\xa0\\ufeff\\n\\r\\u2028\\u2029\\u1680\\u180e\\u2000\\u2001\\u2002\\u2003\\u2004\\u2005\\u2006\\u2007\\u2008\\u2009\\u200a\\u202f\\u205f\\u3000',
      cl = 'A-Z\\xc0-\\xd6\\xd8-\\xde',
      ul = '\\ufe0e\\ufe0f',
      hl = qd + Yd + Gd + Kd,
      Vo = "['’]",
      Vd = '[' + wi + ']',
      dl = '[' + hl + ']',
      $i = '[' + sl + ']',
      fl = '\\d+',
      Xd = '[' + al + ']',
      pl = '[' + ll + ']',
      gl = '[^' + wi + hl + fl + al + ll + cl + ']',
      Xo = '\\ud83c[\\udffb-\\udfff]',
      Zd = '(?:' + $i + '|' + Xo + ')',
      vl = '[^' + wi + ']',
      Zo = '(?:\\ud83c[\\udde6-\\uddff]){2}',
      jo = '[\\ud800-\\udbff][\\udc00-\\udfff]',
      rr = '[' + cl + ']',
      ml = '\\u200d',
      bl = '(?:' + pl + '|' + gl + ')',
      jd = '(?:' + rr + '|' + gl + ')',
      yl = '(?:' + Vo + '(?:d|ll|m|re|s|t|ve))?',
      _l = '(?:' + Vo + '(?:D|LL|M|RE|S|T|VE))?',
      wl = Zd + '?',
      $l = '[' + ul + ']?',
      Jd = '(?:' + ml + '(?:' + [vl, Zo, jo].join('|') + ')' + $l + wl + ')*',
      Qd = '\\d*(?:1st|2nd|3rd|(?![123])\\dth)(?=\\b|[A-Z_])',
      tf = '\\d*(?:1ST|2ND|3RD|(?![123])\\dTH)(?=\\b|[a-z_])',
      xl = $l + wl + Jd,
      ef = '(?:' + [Xd, Zo, jo].join('|') + ')' + xl,
      nf = '(?:' + [vl + $i + '?', $i, Zo, jo, Vd].join('|') + ')',
      rf = RegExp(Vo, 'g'),
      of = RegExp($i, 'g'),
      Jo = RegExp(Xo + '(?=' + Xo + ')|' + nf + xl, 'g'),
      sf = RegExp(
        [
          rr + '?' + pl + '+' + yl + '(?=' + [dl, rr, '$'].join('|') + ')',
          jd + '+' + _l + '(?=' + [dl, rr + bl, '$'].join('|') + ')',
          rr + '?' + bl + '+' + yl,
          rr + '+' + _l,
          tf,
          Qd,
          fl,
          ef,
        ].join('|'),
        'g',
      ),
      af = RegExp('[' + ml + wi + sl + ul + ']'),
      lf = /[a-z][A-Z]|[A-Z]{2}[a-z]|[0-9][a-zA-Z]|[a-zA-Z][0-9]|[^a-zA-Z0-9 ]/,
      cf = [
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
      uf = -1,
      At = {}
    ;(At[Do] =
      At[Bo] =
      At[Uo] =
      At[No] =
      At[Fo] =
      At[Ho] =
      At[Wo] =
      At[qo] =
      At[Yo] =
        !0),
      (At[Bt] =
        At[Ie] =
        At[Ir] =
        At[De] =
        At[nr] =
        At[he] =
        At[de] =
        At[Ke] =
        At[Be] =
        At[Pr] =
        At[rn] =
        At[Rr] =
        At[Ue] =
        At[Mr] =
        At[Lr] =
          !1)
    var St = {}
    ;(St[Bt] =
      St[Ie] =
      St[Ir] =
      St[nr] =
      St[De] =
      St[he] =
      St[Do] =
      St[Bo] =
      St[Uo] =
      St[No] =
      St[Fo] =
      St[Be] =
      St[Pr] =
      St[rn] =
      St[Rr] =
      St[Ue] =
      St[Mr] =
      St[yi] =
      St[Ho] =
      St[Wo] =
      St[qo] =
      St[Yo] =
        !0),
      (St[de] = St[Ke] = St[Lr] = !1)
    var hf = {
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
      df = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
      },
      ff = {
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&#39;': "'",
      },
      pf = {
        '\\': '\\',
        "'": "'",
        '\n': 'n',
        '\r': 'r',
        '\u2028': 'u2028',
        '\u2029': 'u2029',
      },
      gf = parseFloat,
      vf = parseInt,
      Sl = typeof se == 'object' && se && se.Object === Object && se,
      mf = typeof self == 'object' && self && self.Object === Object && self,
      Vt = Sl || mf || Function('return this')(),
      Qo = i && !i.nodeType && i,
      Un = Qo && !0 && n && !n.nodeType && n,
      Cl = Un && Un.exports === Qo,
      ts = Cl && Sl.process,
      Se = (function () {
        try {
          var b = Un && Un.require && Un.require('util').types
          return b || (ts && ts.binding && ts.binding('util'))
        } catch {}
      })(),
      Al = Se && Se.isArrayBuffer,
      El = Se && Se.isDate,
      kl = Se && Se.isMap,
      Tl = Se && Se.isRegExp,
      Ol = Se && Se.isSet,
      zl = Se && Se.isTypedArray
    function fe(b, S, $) {
      switch ($.length) {
        case 0:
          return b.call(S)
        case 1:
          return b.call(S, $[0])
        case 2:
          return b.call(S, $[0], $[1])
        case 3:
          return b.call(S, $[0], $[1], $[2])
      }
      return b.apply(S, $)
    }
    function bf(b, S, $, K) {
      for (var st = -1, bt = b == null ? 0 : b.length; ++st < bt; ) {
        var Ut = b[st]
        S(K, Ut, $(Ut), b)
      }
      return K
    }
    function Ce(b, S) {
      for (
        var $ = -1, K = b == null ? 0 : b.length;
        ++$ < K && S(b[$], $, b) !== !1;

      );
      return b
    }
    function yf(b, S) {
      for (var $ = b == null ? 0 : b.length; $-- && S(b[$], $, b) !== !1; );
      return b
    }
    function Pl(b, S) {
      for (var $ = -1, K = b == null ? 0 : b.length; ++$ < K; )
        if (!S(b[$], $, b)) return !1
      return !0
    }
    function _n(b, S) {
      for (
        var $ = -1, K = b == null ? 0 : b.length, st = 0, bt = [];
        ++$ < K;

      ) {
        var Ut = b[$]
        S(Ut, $, b) && (bt[st++] = Ut)
      }
      return bt
    }
    function xi(b, S) {
      var $ = b == null ? 0 : b.length
      return !!$ && ir(b, S, 0) > -1
    }
    function es(b, S, $) {
      for (var K = -1, st = b == null ? 0 : b.length; ++K < st; )
        if ($(S, b[K])) return !0
      return !1
    }
    function kt(b, S) {
      for (var $ = -1, K = b == null ? 0 : b.length, st = Array(K); ++$ < K; )
        st[$] = S(b[$], $, b)
      return st
    }
    function wn(b, S) {
      for (var $ = -1, K = S.length, st = b.length; ++$ < K; ) b[st + $] = S[$]
      return b
    }
    function ns(b, S, $, K) {
      var st = -1,
        bt = b == null ? 0 : b.length
      for (K && bt && ($ = b[++st]); ++st < bt; ) $ = S($, b[st], st, b)
      return $
    }
    function _f(b, S, $, K) {
      var st = b == null ? 0 : b.length
      for (K && st && ($ = b[--st]); st--; ) $ = S($, b[st], st, b)
      return $
    }
    function rs(b, S) {
      for (var $ = -1, K = b == null ? 0 : b.length; ++$ < K; )
        if (S(b[$], $, b)) return !0
      return !1
    }
    var wf = is('length')
    function $f(b) {
      return b.split('')
    }
    function xf(b) {
      return b.match(Od) || []
    }
    function Rl(b, S, $) {
      var K
      return (
        $(b, function (st, bt, Ut) {
          if (S(st, bt, Ut)) return (K = bt), !1
        }),
        K
      )
    }
    function Si(b, S, $, K) {
      for (var st = b.length, bt = $ + (K ? 1 : -1); K ? bt-- : ++bt < st; )
        if (S(b[bt], bt, b)) return bt
      return -1
    }
    function ir(b, S, $) {
      return S === S ? Lf(b, S, $) : Si(b, Ml, $)
    }
    function Sf(b, S, $, K) {
      for (var st = $ - 1, bt = b.length; ++st < bt; )
        if (K(b[st], S)) return st
      return -1
    }
    function Ml(b) {
      return b !== b
    }
    function Ll(b, S) {
      var $ = b == null ? 0 : b.length
      return $ ? ss(b, S) / $ : rt
    }
    function is(b) {
      return function (S) {
        return S == null ? r : S[b]
      }
    }
    function os(b) {
      return function (S) {
        return b == null ? r : b[S]
      }
    }
    function Il(b, S, $, K, st) {
      return (
        st(b, function (bt, Ut, $t) {
          $ = K ? ((K = !1), bt) : S($, bt, Ut, $t)
        }),
        $
      )
    }
    function Cf(b, S) {
      var $ = b.length
      for (b.sort(S); $--; ) b[$] = b[$].value
      return b
    }
    function ss(b, S) {
      for (var $, K = -1, st = b.length; ++K < st; ) {
        var bt = S(b[K])
        bt !== r && ($ = $ === r ? bt : $ + bt)
      }
      return $
    }
    function as(b, S) {
      for (var $ = -1, K = Array(b); ++$ < b; ) K[$] = S($)
      return K
    }
    function Af(b, S) {
      return kt(S, function ($) {
        return [$, b[$]]
      })
    }
    function Dl(b) {
      return b && b.slice(0, Fl(b) + 1).replace(Ko, '')
    }
    function pe(b) {
      return function (S) {
        return b(S)
      }
    }
    function ls(b, S) {
      return kt(S, function ($) {
        return b[$]
      })
    }
    function Dr(b, S) {
      return b.has(S)
    }
    function Bl(b, S) {
      for (var $ = -1, K = b.length; ++$ < K && ir(S, b[$], 0) > -1; );
      return $
    }
    function Ul(b, S) {
      for (var $ = b.length; $-- && ir(S, b[$], 0) > -1; );
      return $
    }
    function Ef(b, S) {
      for (var $ = b.length, K = 0; $--; ) b[$] === S && ++K
      return K
    }
    var kf = os(hf),
      Tf = os(df)
    function Of(b) {
      return '\\' + pf[b]
    }
    function zf(b, S) {
      return b == null ? r : b[S]
    }
    function or(b) {
      return af.test(b)
    }
    function Pf(b) {
      return lf.test(b)
    }
    function Rf(b) {
      for (var S, $ = []; !(S = b.next()).done; ) $.push(S.value)
      return $
    }
    function cs(b) {
      var S = -1,
        $ = Array(b.size)
      return (
        b.forEach(function (K, st) {
          $[++S] = [st, K]
        }),
        $
      )
    }
    function Nl(b, S) {
      return function ($) {
        return b(S($))
      }
    }
    function $n(b, S) {
      for (var $ = -1, K = b.length, st = 0, bt = []; ++$ < K; ) {
        var Ut = b[$]
        ;(Ut === S || Ut === k) && ((b[$] = k), (bt[st++] = $))
      }
      return bt
    }
    function Ci(b) {
      var S = -1,
        $ = Array(b.size)
      return (
        b.forEach(function (K) {
          $[++S] = K
        }),
        $
      )
    }
    function Mf(b) {
      var S = -1,
        $ = Array(b.size)
      return (
        b.forEach(function (K) {
          $[++S] = [K, K]
        }),
        $
      )
    }
    function Lf(b, S, $) {
      for (var K = $ - 1, st = b.length; ++K < st; ) if (b[K] === S) return K
      return -1
    }
    function If(b, S, $) {
      for (var K = $ + 1; K--; ) if (b[K] === S) return K
      return K
    }
    function sr(b) {
      return or(b) ? Bf(b) : wf(b)
    }
    function Ne(b) {
      return or(b) ? Uf(b) : $f(b)
    }
    function Fl(b) {
      for (var S = b.length; S-- && Ad.test(b.charAt(S)); );
      return S
    }
    var Df = os(ff)
    function Bf(b) {
      for (var S = (Jo.lastIndex = 0); Jo.test(b); ) ++S
      return S
    }
    function Uf(b) {
      return b.match(Jo) || []
    }
    function Nf(b) {
      return b.match(sf) || []
    }
    var Ff = function b(S) {
        S = S == null ? Vt : ar.defaults(Vt.Object(), S, ar.pick(Vt, cf))
        var $ = S.Array,
          K = S.Date,
          st = S.Error,
          bt = S.Function,
          Ut = S.Math,
          $t = S.Object,
          us = S.RegExp,
          Hf = S.String,
          Ae = S.TypeError,
          Ai = $.prototype,
          Wf = bt.prototype,
          lr = $t.prototype,
          Ei = S['__core-js_shared__'],
          ki = Wf.toString,
          _t = lr.hasOwnProperty,
          qf = 0,
          Hl = (function () {
            var t = /[^.]+$/.exec((Ei && Ei.keys && Ei.keys.IE_PROTO) || '')
            return t ? 'Symbol(src)_1.' + t : ''
          })(),
          Ti = lr.toString,
          Yf = ki.call($t),
          Gf = Vt._,
          Kf = us(
            '^' +
              ki
                .call(_t)
                .replace(Go, '\\$&')
                .replace(
                  /hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g,
                  '$1.*?',
                ) +
              '$',
          ),
          Oi = Cl ? S.Buffer : r,
          xn = S.Symbol,
          zi = S.Uint8Array,
          Wl = Oi ? Oi.allocUnsafe : r,
          Pi = Nl($t.getPrototypeOf, $t),
          ql = $t.create,
          Yl = lr.propertyIsEnumerable,
          Ri = Ai.splice,
          Gl = xn ? xn.isConcatSpreadable : r,
          Br = xn ? xn.iterator : r,
          Nn = xn ? xn.toStringTag : r,
          Mi = (function () {
            try {
              var t = Yn($t, 'defineProperty')
              return t({}, '', {}), t
            } catch {}
          })(),
          Vf = S.clearTimeout !== Vt.clearTimeout && S.clearTimeout,
          Xf = K && K.now !== Vt.Date.now && K.now,
          Zf = S.setTimeout !== Vt.setTimeout && S.setTimeout,
          Li = Ut.ceil,
          Ii = Ut.floor,
          hs = $t.getOwnPropertySymbols,
          jf = Oi ? Oi.isBuffer : r,
          Kl = S.isFinite,
          Jf = Ai.join,
          Qf = Nl($t.keys, $t),
          Nt = Ut.max,
          jt = Ut.min,
          tp = K.now,
          ep = S.parseInt,
          Vl = Ut.random,
          np = Ai.reverse,
          ds = Yn(S, 'DataView'),
          Ur = Yn(S, 'Map'),
          fs = Yn(S, 'Promise'),
          cr = Yn(S, 'Set'),
          Nr = Yn(S, 'WeakMap'),
          Fr = Yn($t, 'create'),
          Di = Nr && new Nr(),
          ur = {},
          rp = Gn(ds),
          ip = Gn(Ur),
          op = Gn(fs),
          sp = Gn(cr),
          ap = Gn(Nr),
          Bi = xn ? xn.prototype : r,
          Hr = Bi ? Bi.valueOf : r,
          Xl = Bi ? Bi.toString : r
        function u(t) {
          if (Pt(t) && !lt(t) && !(t instanceof gt)) {
            if (t instanceof Ee) return t
            if (_t.call(t, '__wrapped__')) return Zc(t)
          }
          return new Ee(t)
        }
        var hr = /* @__PURE__ */ (function () {
          function t() {}
          return function (e) {
            if (!Ot(e)) return {}
            if (ql) return ql(e)
            t.prototype = e
            var o = new t()
            return (t.prototype = r), o
          }
        })()
        function Ui() {}
        function Ee(t, e) {
          ;(this.__wrapped__ = t),
            (this.__actions__ = []),
            (this.__chain__ = !!e),
            (this.__index__ = 0),
            (this.__values__ = r)
        }
        ;(u.templateSettings = {
          /**
           * Used to detect `data` property values to be HTML-escaped.
           *
           * @memberOf _.templateSettings
           * @type {RegExp}
           */
          escape: _d,
          /**
           * Used to detect code to be evaluated.
           *
           * @memberOf _.templateSettings
           * @type {RegExp}
           */
          evaluate: wd,
          /**
           * Used to detect `data` property values to inject.
           *
           * @memberOf _.templateSettings
           * @type {RegExp}
           */
          interpolate: il,
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
            _: u,
          },
        }),
          (u.prototype = Ui.prototype),
          (u.prototype.constructor = u),
          (Ee.prototype = hr(Ui.prototype)),
          (Ee.prototype.constructor = Ee)
        function gt(t) {
          ;(this.__wrapped__ = t),
            (this.__actions__ = []),
            (this.__dir__ = 1),
            (this.__filtered__ = !1),
            (this.__iteratees__ = []),
            (this.__takeCount__ = dt),
            (this.__views__ = [])
        }
        function lp() {
          var t = new gt(this.__wrapped__)
          return (
            (t.__actions__ = ne(this.__actions__)),
            (t.__dir__ = this.__dir__),
            (t.__filtered__ = this.__filtered__),
            (t.__iteratees__ = ne(this.__iteratees__)),
            (t.__takeCount__ = this.__takeCount__),
            (t.__views__ = ne(this.__views__)),
            t
          )
        }
        function cp() {
          if (this.__filtered__) {
            var t = new gt(this)
            ;(t.__dir__ = -1), (t.__filtered__ = !0)
          } else (t = this.clone()), (t.__dir__ *= -1)
          return t
        }
        function up() {
          var t = this.__wrapped__.value(),
            e = this.__dir__,
            o = lt(t),
            s = e < 0,
            c = o ? t.length : 0,
            d = $g(0, c, this.__views__),
            p = d.start,
            g = d.end,
            y = g - p,
            A = s ? g : p - 1,
            E = this.__iteratees__,
            z = E.length,
            F = 0,
            Z = jt(y, this.__takeCount__)
          if (!o || (!s && c == y && Z == y)) return yc(t, this.__actions__)
          var tt = []
          t: for (; y-- && F < Z; ) {
            A += e
            for (var ut = -1, et = t[A]; ++ut < z; ) {
              var ft = E[ut],
                vt = ft.iteratee,
                me = ft.type,
                ee = vt(et)
              if (me == D) et = ee
              else if (!ee) {
                if (me == W) continue t
                break t
              }
            }
            tt[F++] = et
          }
          return tt
        }
        ;(gt.prototype = hr(Ui.prototype)), (gt.prototype.constructor = gt)
        function Fn(t) {
          var e = -1,
            o = t == null ? 0 : t.length
          for (this.clear(); ++e < o; ) {
            var s = t[e]
            this.set(s[0], s[1])
          }
        }
        function hp() {
          ;(this.__data__ = Fr ? Fr(null) : {}), (this.size = 0)
        }
        function dp(t) {
          var e = this.has(t) && delete this.__data__[t]
          return (this.size -= e ? 1 : 0), e
        }
        function fp(t) {
          var e = this.__data__
          if (Fr) {
            var o = e[t]
            return o === m ? r : o
          }
          return _t.call(e, t) ? e[t] : r
        }
        function pp(t) {
          var e = this.__data__
          return Fr ? e[t] !== r : _t.call(e, t)
        }
        function gp(t, e) {
          var o = this.__data__
          return (
            (this.size += this.has(t) ? 0 : 1),
            (o[t] = Fr && e === r ? m : e),
            this
          )
        }
        ;(Fn.prototype.clear = hp),
          (Fn.prototype.delete = dp),
          (Fn.prototype.get = fp),
          (Fn.prototype.has = pp),
          (Fn.prototype.set = gp)
        function on(t) {
          var e = -1,
            o = t == null ? 0 : t.length
          for (this.clear(); ++e < o; ) {
            var s = t[e]
            this.set(s[0], s[1])
          }
        }
        function vp() {
          ;(this.__data__ = []), (this.size = 0)
        }
        function mp(t) {
          var e = this.__data__,
            o = Ni(e, t)
          if (o < 0) return !1
          var s = e.length - 1
          return o == s ? e.pop() : Ri.call(e, o, 1), --this.size, !0
        }
        function bp(t) {
          var e = this.__data__,
            o = Ni(e, t)
          return o < 0 ? r : e[o][1]
        }
        function yp(t) {
          return Ni(this.__data__, t) > -1
        }
        function _p(t, e) {
          var o = this.__data__,
            s = Ni(o, t)
          return s < 0 ? (++this.size, o.push([t, e])) : (o[s][1] = e), this
        }
        ;(on.prototype.clear = vp),
          (on.prototype.delete = mp),
          (on.prototype.get = bp),
          (on.prototype.has = yp),
          (on.prototype.set = _p)
        function sn(t) {
          var e = -1,
            o = t == null ? 0 : t.length
          for (this.clear(); ++e < o; ) {
            var s = t[e]
            this.set(s[0], s[1])
          }
        }
        function wp() {
          ;(this.size = 0),
            (this.__data__ = {
              hash: new Fn(),
              map: new (Ur || on)(),
              string: new Fn(),
            })
        }
        function $p(t) {
          var e = Ji(this, t).delete(t)
          return (this.size -= e ? 1 : 0), e
        }
        function xp(t) {
          return Ji(this, t).get(t)
        }
        function Sp(t) {
          return Ji(this, t).has(t)
        }
        function Cp(t, e) {
          var o = Ji(this, t),
            s = o.size
          return o.set(t, e), (this.size += o.size == s ? 0 : 1), this
        }
        ;(sn.prototype.clear = wp),
          (sn.prototype.delete = $p),
          (sn.prototype.get = xp),
          (sn.prototype.has = Sp),
          (sn.prototype.set = Cp)
        function Hn(t) {
          var e = -1,
            o = t == null ? 0 : t.length
          for (this.__data__ = new sn(); ++e < o; ) this.add(t[e])
        }
        function Ap(t) {
          return this.__data__.set(t, m), this
        }
        function Ep(t) {
          return this.__data__.has(t)
        }
        ;(Hn.prototype.add = Hn.prototype.push = Ap), (Hn.prototype.has = Ep)
        function Fe(t) {
          var e = (this.__data__ = new on(t))
          this.size = e.size
        }
        function kp() {
          ;(this.__data__ = new on()), (this.size = 0)
        }
        function Tp(t) {
          var e = this.__data__,
            o = e.delete(t)
          return (this.size = e.size), o
        }
        function Op(t) {
          return this.__data__.get(t)
        }
        function zp(t) {
          return this.__data__.has(t)
        }
        function Pp(t, e) {
          var o = this.__data__
          if (o instanceof on) {
            var s = o.__data__
            if (!Ur || s.length < l - 1)
              return s.push([t, e]), (this.size = ++o.size), this
            o = this.__data__ = new sn(s)
          }
          return o.set(t, e), (this.size = o.size), this
        }
        ;(Fe.prototype.clear = kp),
          (Fe.prototype.delete = Tp),
          (Fe.prototype.get = Op),
          (Fe.prototype.has = zp),
          (Fe.prototype.set = Pp)
        function Zl(t, e) {
          var o = lt(t),
            s = !o && Kn(t),
            c = !o && !s && kn(t),
            d = !o && !s && !c && gr(t),
            p = o || s || c || d,
            g = p ? as(t.length, Hf) : [],
            y = g.length
          for (var A in t)
            (e || _t.call(t, A)) &&
              !(
                p && // Safari 9 has enumerable `arguments.length` in strict mode.
                (A == 'length' || // Node.js 0.10 has enumerable non-index properties on buffers.
                  (c && (A == 'offset' || A == 'parent')) || // PhantomJS 2 has enumerable non-index properties on typed arrays.
                  (d &&
                    (A == 'buffer' ||
                      A == 'byteLength' ||
                      A == 'byteOffset')) || // Skip index properties.
                  un(A, y))
              ) &&
              g.push(A)
          return g
        }
        function jl(t) {
          var e = t.length
          return e ? t[Ss(0, e - 1)] : r
        }
        function Rp(t, e) {
          return Qi(ne(t), Wn(e, 0, t.length))
        }
        function Mp(t) {
          return Qi(ne(t))
        }
        function ps(t, e, o) {
          ;((o !== r && !He(t[e], o)) || (o === r && !(e in t))) && an(t, e, o)
        }
        function Wr(t, e, o) {
          var s = t[e]
          ;(!(_t.call(t, e) && He(s, o)) || (o === r && !(e in t))) &&
            an(t, e, o)
        }
        function Ni(t, e) {
          for (var o = t.length; o--; ) if (He(t[o][0], e)) return o
          return -1
        }
        function Lp(t, e, o, s) {
          return (
            Sn(t, function (c, d, p) {
              e(s, c, o(c), p)
            }),
            s
          )
        }
        function Jl(t, e) {
          return t && Xe(e, Gt(e), t)
        }
        function Ip(t, e) {
          return t && Xe(e, ie(e), t)
        }
        function an(t, e, o) {
          e == '__proto__' && Mi
            ? Mi(t, e, {
                configurable: !0,
                enumerable: !0,
                value: o,
                writable: !0,
              })
            : (t[e] = o)
        }
        function gs(t, e) {
          for (var o = -1, s = e.length, c = $(s), d = t == null; ++o < s; )
            c[o] = d ? r : Xs(t, e[o])
          return c
        }
        function Wn(t, e, o) {
          return (
            t === t &&
              (o !== r && (t = t <= o ? t : o),
              e !== r && (t = t >= e ? t : e)),
            t
          )
        }
        function ke(t, e, o, s, c, d) {
          var p,
            g = e & C,
            y = e & U,
            A = e & T
          if ((o && (p = c ? o(t, s, c, d) : o(t)), p !== r)) return p
          if (!Ot(t)) return t
          var E = lt(t)
          if (E) {
            if (((p = Sg(t)), !g)) return ne(t, p)
          } else {
            var z = Jt(t),
              F = z == Ke || z == Bn
            if (kn(t)) return $c(t, g)
            if (z == rn || z == Bt || (F && !c)) {
              if (((p = y || F ? {} : Fc(t)), !g))
                return y ? fg(t, Ip(p, t)) : dg(t, Jl(p, t))
            } else {
              if (!St[z]) return c ? t : {}
              p = Cg(t, z, g)
            }
          }
          d || (d = new Fe())
          var Z = d.get(t)
          if (Z) return Z
          d.set(t, p),
            vu(t)
              ? t.forEach(function (et) {
                  p.add(ke(et, e, o, et, t, d))
                })
              : pu(t) &&
                t.forEach(function (et, ft) {
                  p.set(ft, ke(et, e, o, ft, t, d))
                })
          var tt = A ? (y ? Ls : Ms) : y ? ie : Gt,
            ut = E ? r : tt(t)
          return (
            Ce(ut || t, function (et, ft) {
              ut && ((ft = et), (et = t[ft])), Wr(p, ft, ke(et, e, o, ft, t, d))
            }),
            p
          )
        }
        function Dp(t) {
          var e = Gt(t)
          return function (o) {
            return Ql(o, t, e)
          }
        }
        function Ql(t, e, o) {
          var s = o.length
          if (t == null) return !s
          for (t = $t(t); s--; ) {
            var c = o[s],
              d = e[c],
              p = t[c]
            if ((p === r && !(c in t)) || !d(p)) return !1
          }
          return !0
        }
        function tc(t, e, o) {
          if (typeof t != 'function') throw new Ae(f)
          return Zr(function () {
            t.apply(r, o)
          }, e)
        }
        function qr(t, e, o, s) {
          var c = -1,
            d = xi,
            p = !0,
            g = t.length,
            y = [],
            A = e.length
          if (!g) return y
          o && (e = kt(e, pe(o))),
            s
              ? ((d = es), (p = !1))
              : e.length >= l && ((d = Dr), (p = !1), (e = new Hn(e)))
          t: for (; ++c < g; ) {
            var E = t[c],
              z = o == null ? E : o(E)
            if (((E = s || E !== 0 ? E : 0), p && z === z)) {
              for (var F = A; F--; ) if (e[F] === z) continue t
              y.push(E)
            } else d(e, z, s) || y.push(E)
          }
          return y
        }
        var Sn = Ec(Ve),
          ec = Ec(ms, !0)
        function Bp(t, e) {
          var o = !0
          return (
            Sn(t, function (s, c, d) {
              return (o = !!e(s, c, d)), o
            }),
            o
          )
        }
        function Fi(t, e, o) {
          for (var s = -1, c = t.length; ++s < c; ) {
            var d = t[s],
              p = e(d)
            if (p != null && (g === r ? p === p && !ve(p) : o(p, g)))
              var g = p,
                y = d
          }
          return y
        }
        function Up(t, e, o, s) {
          var c = t.length
          for (
            o = ct(o),
              o < 0 && (o = -o > c ? 0 : c + o),
              s = s === r || s > c ? c : ct(s),
              s < 0 && (s += c),
              s = o > s ? 0 : bu(s);
            o < s;

          )
            t[o++] = e
          return t
        }
        function nc(t, e) {
          var o = []
          return (
            Sn(t, function (s, c, d) {
              e(s, c, d) && o.push(s)
            }),
            o
          )
        }
        function Xt(t, e, o, s, c) {
          var d = -1,
            p = t.length
          for (o || (o = Eg), c || (c = []); ++d < p; ) {
            var g = t[d]
            e > 0 && o(g)
              ? e > 1
                ? Xt(g, e - 1, o, s, c)
                : wn(c, g)
              : s || (c[c.length] = g)
          }
          return c
        }
        var vs = kc(),
          rc = kc(!0)
        function Ve(t, e) {
          return t && vs(t, e, Gt)
        }
        function ms(t, e) {
          return t && rc(t, e, Gt)
        }
        function Hi(t, e) {
          return _n(e, function (o) {
            return hn(t[o])
          })
        }
        function qn(t, e) {
          e = An(e, t)
          for (var o = 0, s = e.length; t != null && o < s; ) t = t[Ze(e[o++])]
          return o && o == s ? t : r
        }
        function ic(t, e, o) {
          var s = e(t)
          return lt(t) ? s : wn(s, o(t))
        }
        function Qt(t) {
          return t == null
            ? t === r
              ? fd
              : hd
            : Nn && Nn in $t(t)
            ? wg(t)
            : Mg(t)
        }
        function bs(t, e) {
          return t > e
        }
        function Np(t, e) {
          return t != null && _t.call(t, e)
        }
        function Fp(t, e) {
          return t != null && e in $t(t)
        }
        function Hp(t, e, o) {
          return t >= jt(e, o) && t < Nt(e, o)
        }
        function ys(t, e, o) {
          for (
            var s = o ? es : xi,
              c = t[0].length,
              d = t.length,
              p = d,
              g = $(d),
              y = 1 / 0,
              A = [];
            p--;

          ) {
            var E = t[p]
            p && e && (E = kt(E, pe(e))),
              (y = jt(E.length, y)),
              (g[p] =
                !o && (e || (c >= 120 && E.length >= 120)) ? new Hn(p && E) : r)
          }
          E = t[0]
          var z = -1,
            F = g[0]
          t: for (; ++z < c && A.length < y; ) {
            var Z = E[z],
              tt = e ? e(Z) : Z
            if (((Z = o || Z !== 0 ? Z : 0), !(F ? Dr(F, tt) : s(A, tt, o)))) {
              for (p = d; --p; ) {
                var ut = g[p]
                if (!(ut ? Dr(ut, tt) : s(t[p], tt, o))) continue t
              }
              F && F.push(tt), A.push(Z)
            }
          }
          return A
        }
        function Wp(t, e, o, s) {
          return (
            Ve(t, function (c, d, p) {
              e(s, o(c), d, p)
            }),
            s
          )
        }
        function Yr(t, e, o) {
          ;(e = An(e, t)), (t = Yc(t, e))
          var s = t == null ? t : t[Ze(Oe(e))]
          return s == null ? r : fe(s, t, o)
        }
        function oc(t) {
          return Pt(t) && Qt(t) == Bt
        }
        function qp(t) {
          return Pt(t) && Qt(t) == Ir
        }
        function Yp(t) {
          return Pt(t) && Qt(t) == he
        }
        function Gr(t, e, o, s, c) {
          return t === e
            ? !0
            : t == null || e == null || (!Pt(t) && !Pt(e))
            ? t !== t && e !== e
            : Gp(t, e, o, s, Gr, c)
        }
        function Gp(t, e, o, s, c, d) {
          var p = lt(t),
            g = lt(e),
            y = p ? Ie : Jt(t),
            A = g ? Ie : Jt(e)
          ;(y = y == Bt ? rn : y), (A = A == Bt ? rn : A)
          var E = y == rn,
            z = A == rn,
            F = y == A
          if (F && kn(t)) {
            if (!kn(e)) return !1
            ;(p = !0), (E = !1)
          }
          if (F && !E)
            return (
              d || (d = new Fe()),
              p || gr(t) ? Bc(t, e, o, s, c, d) : yg(t, e, y, o, s, c, d)
            )
          if (!(o & O)) {
            var Z = E && _t.call(t, '__wrapped__'),
              tt = z && _t.call(e, '__wrapped__')
            if (Z || tt) {
              var ut = Z ? t.value() : t,
                et = tt ? e.value() : e
              return d || (d = new Fe()), c(ut, et, o, s, d)
            }
          }
          return F ? (d || (d = new Fe()), _g(t, e, o, s, c, d)) : !1
        }
        function Kp(t) {
          return Pt(t) && Jt(t) == Be
        }
        function _s(t, e, o, s) {
          var c = o.length,
            d = c,
            p = !s
          if (t == null) return !d
          for (t = $t(t); c--; ) {
            var g = o[c]
            if (p && g[2] ? g[1] !== t[g[0]] : !(g[0] in t)) return !1
          }
          for (; ++c < d; ) {
            g = o[c]
            var y = g[0],
              A = t[y],
              E = g[1]
            if (p && g[2]) {
              if (A === r && !(y in t)) return !1
            } else {
              var z = new Fe()
              if (s) var F = s(A, E, y, t, e, z)
              if (!(F === r ? Gr(E, A, O | _, s, z) : F)) return !1
            }
          }
          return !0
        }
        function sc(t) {
          if (!Ot(t) || Tg(t)) return !1
          var e = hn(t) ? Kf : Id
          return e.test(Gn(t))
        }
        function Vp(t) {
          return Pt(t) && Qt(t) == Rr
        }
        function Xp(t) {
          return Pt(t) && Jt(t) == Ue
        }
        function Zp(t) {
          return Pt(t) && oo(t.length) && !!At[Qt(t)]
        }
        function ac(t) {
          return typeof t == 'function'
            ? t
            : t == null
            ? oe
            : typeof t == 'object'
            ? lt(t)
              ? uc(t[0], t[1])
              : cc(t)
            : Tu(t)
        }
        function ws(t) {
          if (!Xr(t)) return Qf(t)
          var e = []
          for (var o in $t(t)) _t.call(t, o) && o != 'constructor' && e.push(o)
          return e
        }
        function jp(t) {
          if (!Ot(t)) return Rg(t)
          var e = Xr(t),
            o = []
          for (var s in t)
            (s == 'constructor' && (e || !_t.call(t, s))) || o.push(s)
          return o
        }
        function $s(t, e) {
          return t < e
        }
        function lc(t, e) {
          var o = -1,
            s = re(t) ? $(t.length) : []
          return (
            Sn(t, function (c, d, p) {
              s[++o] = e(c, d, p)
            }),
            s
          )
        }
        function cc(t) {
          var e = Ds(t)
          return e.length == 1 && e[0][2]
            ? Wc(e[0][0], e[0][1])
            : function (o) {
                return o === t || _s(o, t, e)
              }
        }
        function uc(t, e) {
          return Us(t) && Hc(e)
            ? Wc(Ze(t), e)
            : function (o) {
                var s = Xs(o, t)
                return s === r && s === e ? Zs(o, t) : Gr(e, s, O | _)
              }
        }
        function Wi(t, e, o, s, c) {
          t !== e &&
            vs(
              e,
              function (d, p) {
                if ((c || (c = new Fe()), Ot(d))) Jp(t, e, p, o, Wi, s, c)
                else {
                  var g = s ? s(Fs(t, p), d, p + '', t, e, c) : r
                  g === r && (g = d), ps(t, p, g)
                }
              },
              ie,
            )
        }
        function Jp(t, e, o, s, c, d, p) {
          var g = Fs(t, o),
            y = Fs(e, o),
            A = p.get(y)
          if (A) {
            ps(t, o, A)
            return
          }
          var E = d ? d(g, y, o + '', t, e, p) : r,
            z = E === r
          if (z) {
            var F = lt(y),
              Z = !F && kn(y),
              tt = !F && !Z && gr(y)
            ;(E = y),
              F || Z || tt
                ? lt(g)
                  ? (E = g)
                  : Rt(g)
                  ? (E = ne(g))
                  : Z
                  ? ((z = !1), (E = $c(y, !0)))
                  : tt
                  ? ((z = !1), (E = xc(y, !0)))
                  : (E = [])
                : jr(y) || Kn(y)
                ? ((E = g),
                  Kn(g) ? (E = yu(g)) : (!Ot(g) || hn(g)) && (E = Fc(y)))
                : (z = !1)
          }
          z && (p.set(y, E), c(E, y, s, d, p), p.delete(y)), ps(t, o, E)
        }
        function hc(t, e) {
          var o = t.length
          if (o) return (e += e < 0 ? o : 0), un(e, o) ? t[e] : r
        }
        function dc(t, e, o) {
          e.length
            ? (e = kt(e, function (d) {
                return lt(d)
                  ? function (p) {
                      return qn(p, d.length === 1 ? d[0] : d)
                    }
                  : d
              }))
            : (e = [oe])
          var s = -1
          e = kt(e, pe(Q()))
          var c = lc(t, function (d, p, g) {
            var y = kt(e, function (A) {
              return A(d)
            })
            return { criteria: y, index: ++s, value: d }
          })
          return Cf(c, function (d, p) {
            return hg(d, p, o)
          })
        }
        function Qp(t, e) {
          return fc(t, e, function (o, s) {
            return Zs(t, s)
          })
        }
        function fc(t, e, o) {
          for (var s = -1, c = e.length, d = {}; ++s < c; ) {
            var p = e[s],
              g = qn(t, p)
            o(g, p) && Kr(d, An(p, t), g)
          }
          return d
        }
        function tg(t) {
          return function (e) {
            return qn(e, t)
          }
        }
        function xs(t, e, o, s) {
          var c = s ? Sf : ir,
            d = -1,
            p = e.length,
            g = t
          for (t === e && (e = ne(e)), o && (g = kt(t, pe(o))); ++d < p; )
            for (
              var y = 0, A = e[d], E = o ? o(A) : A;
              (y = c(g, E, y, s)) > -1;

            )
              g !== t && Ri.call(g, y, 1), Ri.call(t, y, 1)
          return t
        }
        function pc(t, e) {
          for (var o = t ? e.length : 0, s = o - 1; o--; ) {
            var c = e[o]
            if (o == s || c !== d) {
              var d = c
              un(c) ? Ri.call(t, c, 1) : Es(t, c)
            }
          }
          return t
        }
        function Ss(t, e) {
          return t + Ii(Vl() * (e - t + 1))
        }
        function eg(t, e, o, s) {
          for (var c = -1, d = Nt(Li((e - t) / (o || 1)), 0), p = $(d); d--; )
            (p[s ? d : ++c] = t), (t += o)
          return p
        }
        function Cs(t, e) {
          var o = ''
          if (!t || e < 1 || e > B) return o
          do e % 2 && (o += t), (e = Ii(e / 2)), e && (t += t)
          while (e)
          return o
        }
        function ht(t, e) {
          return Hs(qc(t, e, oe), t + '')
        }
        function ng(t) {
          return jl(vr(t))
        }
        function rg(t, e) {
          var o = vr(t)
          return Qi(o, Wn(e, 0, o.length))
        }
        function Kr(t, e, o, s) {
          if (!Ot(t)) return t
          e = An(e, t)
          for (
            var c = -1, d = e.length, p = d - 1, g = t;
            g != null && ++c < d;

          ) {
            var y = Ze(e[c]),
              A = o
            if (y === '__proto__' || y === 'constructor' || y === 'prototype')
              return t
            if (c != p) {
              var E = g[y]
              ;(A = s ? s(E, y, g) : r),
                A === r && (A = Ot(E) ? E : un(e[c + 1]) ? [] : {})
            }
            Wr(g, y, A), (g = g[y])
          }
          return t
        }
        var gc = Di
            ? function (t, e) {
                return Di.set(t, e), t
              }
            : oe,
          ig = Mi
            ? function (t, e) {
                return Mi(t, 'toString', {
                  configurable: !0,
                  enumerable: !1,
                  value: Js(e),
                  writable: !0,
                })
              }
            : oe
        function og(t) {
          return Qi(vr(t))
        }
        function Te(t, e, o) {
          var s = -1,
            c = t.length
          e < 0 && (e = -e > c ? 0 : c + e),
            (o = o > c ? c : o),
            o < 0 && (o += c),
            (c = e > o ? 0 : (o - e) >>> 0),
            (e >>>= 0)
          for (var d = $(c); ++s < c; ) d[s] = t[s + e]
          return d
        }
        function sg(t, e) {
          var o
          return (
            Sn(t, function (s, c, d) {
              return (o = e(s, c, d)), !o
            }),
            !!o
          )
        }
        function qi(t, e, o) {
          var s = 0,
            c = t == null ? s : t.length
          if (typeof e == 'number' && e === e && c <= It) {
            for (; s < c; ) {
              var d = (s + c) >>> 1,
                p = t[d]
              p !== null && !ve(p) && (o ? p <= e : p < e)
                ? (s = d + 1)
                : (c = d)
            }
            return c
          }
          return As(t, e, oe, o)
        }
        function As(t, e, o, s) {
          var c = 0,
            d = t == null ? 0 : t.length
          if (d === 0) return 0
          e = o(e)
          for (
            var p = e !== e, g = e === null, y = ve(e), A = e === r;
            c < d;

          ) {
            var E = Ii((c + d) / 2),
              z = o(t[E]),
              F = z !== r,
              Z = z === null,
              tt = z === z,
              ut = ve(z)
            if (p) var et = s || tt
            else
              A
                ? (et = tt && (s || F))
                : g
                ? (et = tt && F && (s || !Z))
                : y
                ? (et = tt && F && !Z && (s || !ut))
                : Z || ut
                ? (et = !1)
                : (et = s ? z <= e : z < e)
            et ? (c = E + 1) : (d = E)
          }
          return jt(d, Tt)
        }
        function vc(t, e) {
          for (var o = -1, s = t.length, c = 0, d = []; ++o < s; ) {
            var p = t[o],
              g = e ? e(p) : p
            if (!o || !He(g, y)) {
              var y = g
              d[c++] = p === 0 ? 0 : p
            }
          }
          return d
        }
        function mc(t) {
          return typeof t == 'number' ? t : ve(t) ? rt : +t
        }
        function ge(t) {
          if (typeof t == 'string') return t
          if (lt(t)) return kt(t, ge) + ''
          if (ve(t)) return Xl ? Xl.call(t) : ''
          var e = t + ''
          return e == '0' && 1 / t == -H ? '-0' : e
        }
        function Cn(t, e, o) {
          var s = -1,
            c = xi,
            d = t.length,
            p = !0,
            g = [],
            y = g
          if (o) (p = !1), (c = es)
          else if (d >= l) {
            var A = e ? null : mg(t)
            if (A) return Ci(A)
            ;(p = !1), (c = Dr), (y = new Hn())
          } else y = e ? [] : g
          t: for (; ++s < d; ) {
            var E = t[s],
              z = e ? e(E) : E
            if (((E = o || E !== 0 ? E : 0), p && z === z)) {
              for (var F = y.length; F--; ) if (y[F] === z) continue t
              e && y.push(z), g.push(E)
            } else c(y, z, o) || (y !== g && y.push(z), g.push(E))
          }
          return g
        }
        function Es(t, e) {
          return (
            (e = An(e, t)), (t = Yc(t, e)), t == null || delete t[Ze(Oe(e))]
          )
        }
        function bc(t, e, o, s) {
          return Kr(t, e, o(qn(t, e)), s)
        }
        function Yi(t, e, o, s) {
          for (
            var c = t.length, d = s ? c : -1;
            (s ? d-- : ++d < c) && e(t[d], d, t);

          );
          return o
            ? Te(t, s ? 0 : d, s ? d + 1 : c)
            : Te(t, s ? d + 1 : 0, s ? c : d)
        }
        function yc(t, e) {
          var o = t
          return (
            o instanceof gt && (o = o.value()),
            ns(
              e,
              function (s, c) {
                return c.func.apply(c.thisArg, wn([s], c.args))
              },
              o,
            )
          )
        }
        function ks(t, e, o) {
          var s = t.length
          if (s < 2) return s ? Cn(t[0]) : []
          for (var c = -1, d = $(s); ++c < s; )
            for (var p = t[c], g = -1; ++g < s; )
              g != c && (d[c] = qr(d[c] || p, t[g], e, o))
          return Cn(Xt(d, 1), e, o)
        }
        function _c(t, e, o) {
          for (var s = -1, c = t.length, d = e.length, p = {}; ++s < c; ) {
            var g = s < d ? e[s] : r
            o(p, t[s], g)
          }
          return p
        }
        function Ts(t) {
          return Rt(t) ? t : []
        }
        function Os(t) {
          return typeof t == 'function' ? t : oe
        }
        function An(t, e) {
          return lt(t) ? t : Us(t, e) ? [t] : Xc(yt(t))
        }
        var ag = ht
        function En(t, e, o) {
          var s = t.length
          return (o = o === r ? s : o), !e && o >= s ? t : Te(t, e, o)
        }
        var wc =
          Vf ||
          function (t) {
            return Vt.clearTimeout(t)
          }
        function $c(t, e) {
          if (e) return t.slice()
          var o = t.length,
            s = Wl ? Wl(o) : new t.constructor(o)
          return t.copy(s), s
        }
        function zs(t) {
          var e = new t.constructor(t.byteLength)
          return new zi(e).set(new zi(t)), e
        }
        function lg(t, e) {
          var o = e ? zs(t.buffer) : t.buffer
          return new t.constructor(o, t.byteOffset, t.byteLength)
        }
        function cg(t) {
          var e = new t.constructor(t.source, ol.exec(t))
          return (e.lastIndex = t.lastIndex), e
        }
        function ug(t) {
          return Hr ? $t(Hr.call(t)) : {}
        }
        function xc(t, e) {
          var o = e ? zs(t.buffer) : t.buffer
          return new t.constructor(o, t.byteOffset, t.length)
        }
        function Sc(t, e) {
          if (t !== e) {
            var o = t !== r,
              s = t === null,
              c = t === t,
              d = ve(t),
              p = e !== r,
              g = e === null,
              y = e === e,
              A = ve(e)
            if (
              (!g && !A && !d && t > e) ||
              (d && p && y && !g && !A) ||
              (s && p && y) ||
              (!o && y) ||
              !c
            )
              return 1
            if (
              (!s && !d && !A && t < e) ||
              (A && o && c && !s && !d) ||
              (g && o && c) ||
              (!p && c) ||
              !y
            )
              return -1
          }
          return 0
        }
        function hg(t, e, o) {
          for (
            var s = -1,
              c = t.criteria,
              d = e.criteria,
              p = c.length,
              g = o.length;
            ++s < p;

          ) {
            var y = Sc(c[s], d[s])
            if (y) {
              if (s >= g) return y
              var A = o[s]
              return y * (A == 'desc' ? -1 : 1)
            }
          }
          return t.index - e.index
        }
        function Cc(t, e, o, s) {
          for (
            var c = -1,
              d = t.length,
              p = o.length,
              g = -1,
              y = e.length,
              A = Nt(d - p, 0),
              E = $(y + A),
              z = !s;
            ++g < y;

          )
            E[g] = e[g]
          for (; ++c < p; ) (z || c < d) && (E[o[c]] = t[c])
          for (; A--; ) E[g++] = t[c++]
          return E
        }
        function Ac(t, e, o, s) {
          for (
            var c = -1,
              d = t.length,
              p = -1,
              g = o.length,
              y = -1,
              A = e.length,
              E = Nt(d - g, 0),
              z = $(E + A),
              F = !s;
            ++c < E;

          )
            z[c] = t[c]
          for (var Z = c; ++y < A; ) z[Z + y] = e[y]
          for (; ++p < g; ) (F || c < d) && (z[Z + o[p]] = t[c++])
          return z
        }
        function ne(t, e) {
          var o = -1,
            s = t.length
          for (e || (e = $(s)); ++o < s; ) e[o] = t[o]
          return e
        }
        function Xe(t, e, o, s) {
          var c = !o
          o || (o = {})
          for (var d = -1, p = e.length; ++d < p; ) {
            var g = e[d],
              y = s ? s(o[g], t[g], g, o, t) : r
            y === r && (y = t[g]), c ? an(o, g, y) : Wr(o, g, y)
          }
          return o
        }
        function dg(t, e) {
          return Xe(t, Bs(t), e)
        }
        function fg(t, e) {
          return Xe(t, Uc(t), e)
        }
        function Gi(t, e) {
          return function (o, s) {
            var c = lt(o) ? bf : Lp,
              d = e ? e() : {}
            return c(o, t, Q(s, 2), d)
          }
        }
        function dr(t) {
          return ht(function (e, o) {
            var s = -1,
              c = o.length,
              d = c > 1 ? o[c - 1] : r,
              p = c > 2 ? o[2] : r
            for (
              d = t.length > 3 && typeof d == 'function' ? (c--, d) : r,
                p && te(o[0], o[1], p) && ((d = c < 3 ? r : d), (c = 1)),
                e = $t(e);
              ++s < c;

            ) {
              var g = o[s]
              g && t(e, g, s, d)
            }
            return e
          })
        }
        function Ec(t, e) {
          return function (o, s) {
            if (o == null) return o
            if (!re(o)) return t(o, s)
            for (
              var c = o.length, d = e ? c : -1, p = $t(o);
              (e ? d-- : ++d < c) && s(p[d], d, p) !== !1;

            );
            return o
          }
        }
        function kc(t) {
          return function (e, o, s) {
            for (var c = -1, d = $t(e), p = s(e), g = p.length; g--; ) {
              var y = p[t ? g : ++c]
              if (o(d[y], y, d) === !1) break
            }
            return e
          }
        }
        function pg(t, e, o) {
          var s = e & x,
            c = Vr(t)
          function d() {
            var p = this && this !== Vt && this instanceof d ? c : t
            return p.apply(s ? o : this, arguments)
          }
          return d
        }
        function Tc(t) {
          return function (e) {
            e = yt(e)
            var o = or(e) ? Ne(e) : r,
              s = o ? o[0] : e.charAt(0),
              c = o ? En(o, 1).join('') : e.slice(1)
            return s[t]() + c
          }
        }
        function fr(t) {
          return function (e) {
            return ns(Eu(Au(e).replace(rf, '')), t, '')
          }
        }
        function Vr(t) {
          return function () {
            var e = arguments
            switch (e.length) {
              case 0:
                return new t()
              case 1:
                return new t(e[0])
              case 2:
                return new t(e[0], e[1])
              case 3:
                return new t(e[0], e[1], e[2])
              case 4:
                return new t(e[0], e[1], e[2], e[3])
              case 5:
                return new t(e[0], e[1], e[2], e[3], e[4])
              case 6:
                return new t(e[0], e[1], e[2], e[3], e[4], e[5])
              case 7:
                return new t(e[0], e[1], e[2], e[3], e[4], e[5], e[6])
            }
            var o = hr(t.prototype),
              s = t.apply(o, e)
            return Ot(s) ? s : o
          }
        }
        function gg(t, e, o) {
          var s = Vr(t)
          function c() {
            for (var d = arguments.length, p = $(d), g = d, y = pr(c); g--; )
              p[g] = arguments[g]
            var A = d < 3 && p[0] !== y && p[d - 1] !== y ? [] : $n(p, y)
            if (((d -= A.length), d < o))
              return Mc(t, e, Ki, c.placeholder, r, p, A, r, r, o - d)
            var E = this && this !== Vt && this instanceof c ? s : t
            return fe(E, this, p)
          }
          return c
        }
        function Oc(t) {
          return function (e, o, s) {
            var c = $t(e)
            if (!re(e)) {
              var d = Q(o, 3)
              ;(e = Gt(e)),
                (o = function (g) {
                  return d(c[g], g, c)
                })
            }
            var p = t(e, o, s)
            return p > -1 ? c[d ? e[p] : p] : r
          }
        }
        function zc(t) {
          return cn(function (e) {
            var o = e.length,
              s = o,
              c = Ee.prototype.thru
            for (t && e.reverse(); s--; ) {
              var d = e[s]
              if (typeof d != 'function') throw new Ae(f)
              if (c && !p && ji(d) == 'wrapper') var p = new Ee([], !0)
            }
            for (s = p ? s : o; ++s < o; ) {
              d = e[s]
              var g = ji(d),
                y = g == 'wrapper' ? Is(d) : r
              y &&
              Ns(y[0]) &&
              y[1] == (I | N | J | M) &&
              !y[4].length &&
              y[9] == 1
                ? (p = p[ji(y[0])].apply(p, y[3]))
                : (p = d.length == 1 && Ns(d) ? p[g]() : p.thru(d))
            }
            return function () {
              var A = arguments,
                E = A[0]
              if (p && A.length == 1 && lt(E)) return p.plant(E).value()
              for (var z = 0, F = o ? e[z].apply(this, A) : E; ++z < o; )
                F = e[z].call(this, F)
              return F
            }
          })
        }
        function Ki(t, e, o, s, c, d, p, g, y, A) {
          var E = e & I,
            z = e & x,
            F = e & P,
            Z = e & (N | it),
            tt = e & nt,
            ut = F ? r : Vr(t)
          function et() {
            for (var ft = arguments.length, vt = $(ft), me = ft; me--; )
              vt[me] = arguments[me]
            if (Z)
              var ee = pr(et),
                be = Ef(vt, ee)
            if (
              (s && (vt = Cc(vt, s, c, Z)),
              d && (vt = Ac(vt, d, p, Z)),
              (ft -= be),
              Z && ft < A)
            ) {
              var Mt = $n(vt, ee)
              return Mc(t, e, Ki, et.placeholder, o, vt, Mt, g, y, A - ft)
            }
            var We = z ? o : this,
              fn = F ? We[t] : t
            return (
              (ft = vt.length),
              g ? (vt = Lg(vt, g)) : tt && ft > 1 && vt.reverse(),
              E && y < ft && (vt.length = y),
              this && this !== Vt && this instanceof et && (fn = ut || Vr(fn)),
              fn.apply(We, vt)
            )
          }
          return et
        }
        function Pc(t, e) {
          return function (o, s) {
            return Wp(o, t, e(s), {})
          }
        }
        function Vi(t, e) {
          return function (o, s) {
            var c
            if (o === r && s === r) return e
            if ((o !== r && (c = o), s !== r)) {
              if (c === r) return s
              typeof o == 'string' || typeof s == 'string'
                ? ((o = ge(o)), (s = ge(s)))
                : ((o = mc(o)), (s = mc(s))),
                (c = t(o, s))
            }
            return c
          }
        }
        function Ps(t) {
          return cn(function (e) {
            return (
              (e = kt(e, pe(Q()))),
              ht(function (o) {
                var s = this
                return t(e, function (c) {
                  return fe(c, s, o)
                })
              })
            )
          })
        }
        function Xi(t, e) {
          e = e === r ? ' ' : ge(e)
          var o = e.length
          if (o < 2) return o ? Cs(e, t) : e
          var s = Cs(e, Li(t / sr(e)))
          return or(e) ? En(Ne(s), 0, t).join('') : s.slice(0, t)
        }
        function vg(t, e, o, s) {
          var c = e & x,
            d = Vr(t)
          function p() {
            for (
              var g = -1,
                y = arguments.length,
                A = -1,
                E = s.length,
                z = $(E + y),
                F = this && this !== Vt && this instanceof p ? d : t;
              ++A < E;

            )
              z[A] = s[A]
            for (; y--; ) z[A++] = arguments[++g]
            return fe(F, c ? o : this, z)
          }
          return p
        }
        function Rc(t) {
          return function (e, o, s) {
            return (
              s && typeof s != 'number' && te(e, o, s) && (o = s = r),
              (e = dn(e)),
              o === r ? ((o = e), (e = 0)) : (o = dn(o)),
              (s = s === r ? (e < o ? 1 : -1) : dn(s)),
              eg(e, o, s, t)
            )
          }
        }
        function Zi(t) {
          return function (e, o) {
            return (
              (typeof e == 'string' && typeof o == 'string') ||
                ((e = ze(e)), (o = ze(o))),
              t(e, o)
            )
          }
        }
        function Mc(t, e, o, s, c, d, p, g, y, A) {
          var E = e & N,
            z = E ? p : r,
            F = E ? r : p,
            Z = E ? d : r,
            tt = E ? r : d
          ;(e |= E ? J : q), (e &= ~(E ? q : J)), e & G || (e &= ~(x | P))
          var ut = [t, e, c, Z, z, tt, F, g, y, A],
            et = o.apply(r, ut)
          return Ns(t) && Gc(et, ut), (et.placeholder = s), Kc(et, t, e)
        }
        function Rs(t) {
          var e = Ut[t]
          return function (o, s) {
            if (
              ((o = ze(o)), (s = s == null ? 0 : jt(ct(s), 292)), s && Kl(o))
            ) {
              var c = (yt(o) + 'e').split('e'),
                d = e(c[0] + 'e' + (+c[1] + s))
              return (c = (yt(d) + 'e').split('e')), +(c[0] + 'e' + (+c[1] - s))
            }
            return e(o)
          }
        }
        var mg =
          cr && 1 / Ci(new cr([, -0]))[1] == H
            ? function (t) {
                return new cr(t)
              }
            : ea
        function Lc(t) {
          return function (e) {
            var o = Jt(e)
            return o == Be ? cs(e) : o == Ue ? Mf(e) : Af(e, t(e))
          }
        }
        function ln(t, e, o, s, c, d, p, g) {
          var y = e & P
          if (!y && typeof t != 'function') throw new Ae(f)
          var A = s ? s.length : 0
          if (
            (A || ((e &= ~(J | q)), (s = c = r)),
            (p = p === r ? p : Nt(ct(p), 0)),
            (g = g === r ? g : ct(g)),
            (A -= c ? c.length : 0),
            e & q)
          ) {
            var E = s,
              z = c
            s = c = r
          }
          var F = y ? r : Is(t),
            Z = [t, e, o, s, c, E, z, d, p, g]
          if (
            (F && Pg(Z, F),
            (t = Z[0]),
            (e = Z[1]),
            (o = Z[2]),
            (s = Z[3]),
            (c = Z[4]),
            (g = Z[9] = Z[9] === r ? (y ? 0 : t.length) : Nt(Z[9] - A, 0)),
            !g && e & (N | it) && (e &= ~(N | it)),
            !e || e == x)
          )
            var tt = pg(t, e, o)
          else
            e == N || e == it
              ? (tt = gg(t, e, g))
              : (e == J || e == (x | J)) && !c.length
              ? (tt = vg(t, e, o, s))
              : (tt = Ki.apply(r, Z))
          var ut = F ? gc : Gc
          return Kc(ut(tt, Z), t, e)
        }
        function Ic(t, e, o, s) {
          return t === r || (He(t, lr[o]) && !_t.call(s, o)) ? e : t
        }
        function Dc(t, e, o, s, c, d) {
          return (
            Ot(t) && Ot(e) && (d.set(e, t), Wi(t, e, r, Dc, d), d.delete(e)), t
          )
        }
        function bg(t) {
          return jr(t) ? r : t
        }
        function Bc(t, e, o, s, c, d) {
          var p = o & O,
            g = t.length,
            y = e.length
          if (g != y && !(p && y > g)) return !1
          var A = d.get(t),
            E = d.get(e)
          if (A && E) return A == e && E == t
          var z = -1,
            F = !0,
            Z = o & _ ? new Hn() : r
          for (d.set(t, e), d.set(e, t); ++z < g; ) {
            var tt = t[z],
              ut = e[z]
            if (s) var et = p ? s(ut, tt, z, e, t, d) : s(tt, ut, z, t, e, d)
            if (et !== r) {
              if (et) continue
              F = !1
              break
            }
            if (Z) {
              if (
                !rs(e, function (ft, vt) {
                  if (!Dr(Z, vt) && (tt === ft || c(tt, ft, o, s, d)))
                    return Z.push(vt)
                })
              ) {
                F = !1
                break
              }
            } else if (!(tt === ut || c(tt, ut, o, s, d))) {
              F = !1
              break
            }
          }
          return d.delete(t), d.delete(e), F
        }
        function yg(t, e, o, s, c, d, p) {
          switch (o) {
            case nr:
              if (t.byteLength != e.byteLength || t.byteOffset != e.byteOffset)
                return !1
              ;(t = t.buffer), (e = e.buffer)
            case Ir:
              return !(t.byteLength != e.byteLength || !d(new zi(t), new zi(e)))
            case De:
            case he:
            case Pr:
              return He(+t, +e)
            case de:
              return t.name == e.name && t.message == e.message
            case Rr:
            case Mr:
              return t == e + ''
            case Be:
              var g = cs
            case Ue:
              var y = s & O
              if ((g || (g = Ci), t.size != e.size && !y)) return !1
              var A = p.get(t)
              if (A) return A == e
              ;(s |= _), p.set(t, e)
              var E = Bc(g(t), g(e), s, c, d, p)
              return p.delete(t), E
            case yi:
              if (Hr) return Hr.call(t) == Hr.call(e)
          }
          return !1
        }
        function _g(t, e, o, s, c, d) {
          var p = o & O,
            g = Ms(t),
            y = g.length,
            A = Ms(e),
            E = A.length
          if (y != E && !p) return !1
          for (var z = y; z--; ) {
            var F = g[z]
            if (!(p ? F in e : _t.call(e, F))) return !1
          }
          var Z = d.get(t),
            tt = d.get(e)
          if (Z && tt) return Z == e && tt == t
          var ut = !0
          d.set(t, e), d.set(e, t)
          for (var et = p; ++z < y; ) {
            F = g[z]
            var ft = t[F],
              vt = e[F]
            if (s) var me = p ? s(vt, ft, F, e, t, d) : s(ft, vt, F, t, e, d)
            if (!(me === r ? ft === vt || c(ft, vt, o, s, d) : me)) {
              ut = !1
              break
            }
            et || (et = F == 'constructor')
          }
          if (ut && !et) {
            var ee = t.constructor,
              be = e.constructor
            ee != be &&
              'constructor' in t &&
              'constructor' in e &&
              !(
                typeof ee == 'function' &&
                ee instanceof ee &&
                typeof be == 'function' &&
                be instanceof be
              ) &&
              (ut = !1)
          }
          return d.delete(t), d.delete(e), ut
        }
        function cn(t) {
          return Hs(qc(t, r, Qc), t + '')
        }
        function Ms(t) {
          return ic(t, Gt, Bs)
        }
        function Ls(t) {
          return ic(t, ie, Uc)
        }
        var Is = Di
          ? function (t) {
              return Di.get(t)
            }
          : ea
        function ji(t) {
          for (
            var e = t.name + '', o = ur[e], s = _t.call(ur, e) ? o.length : 0;
            s--;

          ) {
            var c = o[s],
              d = c.func
            if (d == null || d == t) return c.name
          }
          return e
        }
        function pr(t) {
          var e = _t.call(u, 'placeholder') ? u : t
          return e.placeholder
        }
        function Q() {
          var t = u.iteratee || Qs
          return (
            (t = t === Qs ? ac : t),
            arguments.length ? t(arguments[0], arguments[1]) : t
          )
        }
        function Ji(t, e) {
          var o = t.__data__
          return kg(e) ? o[typeof e == 'string' ? 'string' : 'hash'] : o.map
        }
        function Ds(t) {
          for (var e = Gt(t), o = e.length; o--; ) {
            var s = e[o],
              c = t[s]
            e[o] = [s, c, Hc(c)]
          }
          return e
        }
        function Yn(t, e) {
          var o = zf(t, e)
          return sc(o) ? o : r
        }
        function wg(t) {
          var e = _t.call(t, Nn),
            o = t[Nn]
          try {
            t[Nn] = r
            var s = !0
          } catch {}
          var c = Ti.call(t)
          return s && (e ? (t[Nn] = o) : delete t[Nn]), c
        }
        var Bs = hs
            ? function (t) {
                return t == null
                  ? []
                  : ((t = $t(t)),
                    _n(hs(t), function (e) {
                      return Yl.call(t, e)
                    }))
              }
            : na,
          Uc = hs
            ? function (t) {
                for (var e = []; t; ) wn(e, Bs(t)), (t = Pi(t))
                return e
              }
            : na,
          Jt = Qt
        ;((ds && Jt(new ds(new ArrayBuffer(1))) != nr) ||
          (Ur && Jt(new Ur()) != Be) ||
          (fs && Jt(fs.resolve()) != el) ||
          (cr && Jt(new cr()) != Ue) ||
          (Nr && Jt(new Nr()) != Lr)) &&
          (Jt = function (t) {
            var e = Qt(t),
              o = e == rn ? t.constructor : r,
              s = o ? Gn(o) : ''
            if (s)
              switch (s) {
                case rp:
                  return nr
                case ip:
                  return Be
                case op:
                  return el
                case sp:
                  return Ue
                case ap:
                  return Lr
              }
            return e
          })
        function $g(t, e, o) {
          for (var s = -1, c = o.length; ++s < c; ) {
            var d = o[s],
              p = d.size
            switch (d.type) {
              case 'drop':
                t += p
                break
              case 'dropRight':
                e -= p
                break
              case 'take':
                e = jt(e, t + p)
                break
              case 'takeRight':
                t = Nt(t, e - p)
                break
            }
          }
          return { start: t, end: e }
        }
        function xg(t) {
          var e = t.match(kd)
          return e ? e[1].split(Td) : []
        }
        function Nc(t, e, o) {
          e = An(e, t)
          for (var s = -1, c = e.length, d = !1; ++s < c; ) {
            var p = Ze(e[s])
            if (!(d = t != null && o(t, p))) break
            t = t[p]
          }
          return d || ++s != c
            ? d
            : ((c = t == null ? 0 : t.length),
              !!c && oo(c) && un(p, c) && (lt(t) || Kn(t)))
        }
        function Sg(t) {
          var e = t.length,
            o = new t.constructor(e)
          return (
            e &&
              typeof t[0] == 'string' &&
              _t.call(t, 'index') &&
              ((o.index = t.index), (o.input = t.input)),
            o
          )
        }
        function Fc(t) {
          return typeof t.constructor == 'function' && !Xr(t) ? hr(Pi(t)) : {}
        }
        function Cg(t, e, o) {
          var s = t.constructor
          switch (e) {
            case Ir:
              return zs(t)
            case De:
            case he:
              return new s(+t)
            case nr:
              return lg(t, o)
            case Do:
            case Bo:
            case Uo:
            case No:
            case Fo:
            case Ho:
            case Wo:
            case qo:
            case Yo:
              return xc(t, o)
            case Be:
              return new s()
            case Pr:
            case Mr:
              return new s(t)
            case Rr:
              return cg(t)
            case Ue:
              return new s()
            case yi:
              return ug(t)
          }
        }
        function Ag(t, e) {
          var o = e.length
          if (!o) return t
          var s = o - 1
          return (
            (e[s] = (o > 1 ? '& ' : '') + e[s]),
            (e = e.join(o > 2 ? ', ' : ' ')),
            t.replace(
              Ed,
              `{
/* [wrapped with ` +
                e +
                `] */
`,
            )
          )
        }
        function Eg(t) {
          return lt(t) || Kn(t) || !!(Gl && t && t[Gl])
        }
        function un(t, e) {
          var o = typeof t
          return (
            (e = e ?? B),
            !!e &&
              (o == 'number' || (o != 'symbol' && Bd.test(t))) &&
              t > -1 &&
              t % 1 == 0 &&
              t < e
          )
        }
        function te(t, e, o) {
          if (!Ot(o)) return !1
          var s = typeof e
          return (
            s == 'number' ? re(o) && un(e, o.length) : s == 'string' && e in o
          )
            ? He(o[e], t)
            : !1
        }
        function Us(t, e) {
          if (lt(t)) return !1
          var o = typeof t
          return o == 'number' ||
            o == 'symbol' ||
            o == 'boolean' ||
            t == null ||
            ve(t)
            ? !0
            : xd.test(t) || !$d.test(t) || (e != null && t in $t(e))
        }
        function kg(t) {
          var e = typeof t
          return e == 'string' ||
            e == 'number' ||
            e == 'symbol' ||
            e == 'boolean'
            ? t !== '__proto__'
            : t === null
        }
        function Ns(t) {
          var e = ji(t),
            o = u[e]
          if (typeof o != 'function' || !(e in gt.prototype)) return !1
          if (t === o) return !0
          var s = Is(o)
          return !!s && t === s[0]
        }
        function Tg(t) {
          return !!Hl && Hl in t
        }
        var Og = Ei ? hn : ra
        function Xr(t) {
          var e = t && t.constructor,
            o = (typeof e == 'function' && e.prototype) || lr
          return t === o
        }
        function Hc(t) {
          return t === t && !Ot(t)
        }
        function Wc(t, e) {
          return function (o) {
            return o == null ? !1 : o[t] === e && (e !== r || t in $t(o))
          }
        }
        function zg(t) {
          var e = ro(t, function (s) {
              return o.size === w && o.clear(), s
            }),
            o = e.cache
          return e
        }
        function Pg(t, e) {
          var o = t[1],
            s = e[1],
            c = o | s,
            d = c < (x | P | I),
            p =
              (s == I && o == N) ||
              (s == I && o == M && t[7].length <= e[8]) ||
              (s == (I | M) && e[7].length <= e[8] && o == N)
          if (!(d || p)) return t
          s & x && ((t[2] = e[2]), (c |= o & x ? 0 : G))
          var g = e[3]
          if (g) {
            var y = t[3]
            ;(t[3] = y ? Cc(y, g, e[4]) : g), (t[4] = y ? $n(t[3], k) : e[4])
          }
          return (
            (g = e[5]),
            g &&
              ((y = t[5]),
              (t[5] = y ? Ac(y, g, e[6]) : g),
              (t[6] = y ? $n(t[5], k) : e[6])),
            (g = e[7]),
            g && (t[7] = g),
            s & I && (t[8] = t[8] == null ? e[8] : jt(t[8], e[8])),
            t[9] == null && (t[9] = e[9]),
            (t[0] = e[0]),
            (t[1] = c),
            t
          )
        }
        function Rg(t) {
          var e = []
          if (t != null) for (var o in $t(t)) e.push(o)
          return e
        }
        function Mg(t) {
          return Ti.call(t)
        }
        function qc(t, e, o) {
          return (
            (e = Nt(e === r ? t.length - 1 : e, 0)),
            function () {
              for (
                var s = arguments, c = -1, d = Nt(s.length - e, 0), p = $(d);
                ++c < d;

              )
                p[c] = s[e + c]
              c = -1
              for (var g = $(e + 1); ++c < e; ) g[c] = s[c]
              return (g[e] = o(p)), fe(t, this, g)
            }
          )
        }
        function Yc(t, e) {
          return e.length < 2 ? t : qn(t, Te(e, 0, -1))
        }
        function Lg(t, e) {
          for (var o = t.length, s = jt(e.length, o), c = ne(t); s--; ) {
            var d = e[s]
            t[s] = un(d, o) ? c[d] : r
          }
          return t
        }
        function Fs(t, e) {
          if (
            !(e === 'constructor' && typeof t[e] == 'function') &&
            e != '__proto__'
          )
            return t[e]
        }
        var Gc = Vc(gc),
          Zr =
            Zf ||
            function (t, e) {
              return Vt.setTimeout(t, e)
            },
          Hs = Vc(ig)
        function Kc(t, e, o) {
          var s = e + ''
          return Hs(t, Ag(s, Ig(xg(s), o)))
        }
        function Vc(t) {
          var e = 0,
            o = 0
          return function () {
            var s = tp(),
              c = Et - (s - o)
            if (((o = s), c > 0)) {
              if (++e >= mt) return arguments[0]
            } else e = 0
            return t.apply(r, arguments)
          }
        }
        function Qi(t, e) {
          var o = -1,
            s = t.length,
            c = s - 1
          for (e = e === r ? s : e; ++o < e; ) {
            var d = Ss(o, c),
              p = t[d]
            ;(t[d] = t[o]), (t[o] = p)
          }
          return (t.length = e), t
        }
        var Xc = zg(function (t) {
          var e = []
          return (
            t.charCodeAt(0) === 46 && e.push(''),
            t.replace(Sd, function (o, s, c, d) {
              e.push(c ? d.replace(Pd, '$1') : s || o)
            }),
            e
          )
        })
        function Ze(t) {
          if (typeof t == 'string' || ve(t)) return t
          var e = t + ''
          return e == '0' && 1 / t == -H ? '-0' : e
        }
        function Gn(t) {
          if (t != null) {
            try {
              return ki.call(t)
            } catch {}
            try {
              return t + ''
            } catch {}
          }
          return ''
        }
        function Ig(t, e) {
          return (
            Ce(Yt, function (o) {
              var s = '_.' + o[0]
              e & o[1] && !xi(t, s) && t.push(s)
            }),
            t.sort()
          )
        }
        function Zc(t) {
          if (t instanceof gt) return t.clone()
          var e = new Ee(t.__wrapped__, t.__chain__)
          return (
            (e.__actions__ = ne(t.__actions__)),
            (e.__index__ = t.__index__),
            (e.__values__ = t.__values__),
            e
          )
        }
        function Dg(t, e, o) {
          ;(o ? te(t, e, o) : e === r) ? (e = 1) : (e = Nt(ct(e), 0))
          var s = t == null ? 0 : t.length
          if (!s || e < 1) return []
          for (var c = 0, d = 0, p = $(Li(s / e)); c < s; )
            p[d++] = Te(t, c, (c += e))
          return p
        }
        function Bg(t) {
          for (
            var e = -1, o = t == null ? 0 : t.length, s = 0, c = [];
            ++e < o;

          ) {
            var d = t[e]
            d && (c[s++] = d)
          }
          return c
        }
        function Ug() {
          var t = arguments.length
          if (!t) return []
          for (var e = $(t - 1), o = arguments[0], s = t; s--; )
            e[s - 1] = arguments[s]
          return wn(lt(o) ? ne(o) : [o], Xt(e, 1))
        }
        var Ng = ht(function (t, e) {
            return Rt(t) ? qr(t, Xt(e, 1, Rt, !0)) : []
          }),
          Fg = ht(function (t, e) {
            var o = Oe(e)
            return (
              Rt(o) && (o = r), Rt(t) ? qr(t, Xt(e, 1, Rt, !0), Q(o, 2)) : []
            )
          }),
          Hg = ht(function (t, e) {
            var o = Oe(e)
            return Rt(o) && (o = r), Rt(t) ? qr(t, Xt(e, 1, Rt, !0), r, o) : []
          })
        function Wg(t, e, o) {
          var s = t == null ? 0 : t.length
          return s
            ? ((e = o || e === r ? 1 : ct(e)), Te(t, e < 0 ? 0 : e, s))
            : []
        }
        function qg(t, e, o) {
          var s = t == null ? 0 : t.length
          return s
            ? ((e = o || e === r ? 1 : ct(e)),
              (e = s - e),
              Te(t, 0, e < 0 ? 0 : e))
            : []
        }
        function Yg(t, e) {
          return t && t.length ? Yi(t, Q(e, 3), !0, !0) : []
        }
        function Gg(t, e) {
          return t && t.length ? Yi(t, Q(e, 3), !0) : []
        }
        function Kg(t, e, o, s) {
          var c = t == null ? 0 : t.length
          return c
            ? (o && typeof o != 'number' && te(t, e, o) && ((o = 0), (s = c)),
              Up(t, e, o, s))
            : []
        }
        function jc(t, e, o) {
          var s = t == null ? 0 : t.length
          if (!s) return -1
          var c = o == null ? 0 : ct(o)
          return c < 0 && (c = Nt(s + c, 0)), Si(t, Q(e, 3), c)
        }
        function Jc(t, e, o) {
          var s = t == null ? 0 : t.length
          if (!s) return -1
          var c = s - 1
          return (
            o !== r && ((c = ct(o)), (c = o < 0 ? Nt(s + c, 0) : jt(c, s - 1))),
            Si(t, Q(e, 3), c, !0)
          )
        }
        function Qc(t) {
          var e = t == null ? 0 : t.length
          return e ? Xt(t, 1) : []
        }
        function Vg(t) {
          var e = t == null ? 0 : t.length
          return e ? Xt(t, H) : []
        }
        function Xg(t, e) {
          var o = t == null ? 0 : t.length
          return o ? ((e = e === r ? 1 : ct(e)), Xt(t, e)) : []
        }
        function Zg(t) {
          for (var e = -1, o = t == null ? 0 : t.length, s = {}; ++e < o; ) {
            var c = t[e]
            s[c[0]] = c[1]
          }
          return s
        }
        function tu(t) {
          return t && t.length ? t[0] : r
        }
        function jg(t, e, o) {
          var s = t == null ? 0 : t.length
          if (!s) return -1
          var c = o == null ? 0 : ct(o)
          return c < 0 && (c = Nt(s + c, 0)), ir(t, e, c)
        }
        function Jg(t) {
          var e = t == null ? 0 : t.length
          return e ? Te(t, 0, -1) : []
        }
        var Qg = ht(function (t) {
            var e = kt(t, Ts)
            return e.length && e[0] === t[0] ? ys(e) : []
          }),
          tv = ht(function (t) {
            var e = Oe(t),
              o = kt(t, Ts)
            return (
              e === Oe(o) ? (e = r) : o.pop(),
              o.length && o[0] === t[0] ? ys(o, Q(e, 2)) : []
            )
          }),
          ev = ht(function (t) {
            var e = Oe(t),
              o = kt(t, Ts)
            return (
              (e = typeof e == 'function' ? e : r),
              e && o.pop(),
              o.length && o[0] === t[0] ? ys(o, r, e) : []
            )
          })
        function nv(t, e) {
          return t == null ? '' : Jf.call(t, e)
        }
        function Oe(t) {
          var e = t == null ? 0 : t.length
          return e ? t[e - 1] : r
        }
        function rv(t, e, o) {
          var s = t == null ? 0 : t.length
          if (!s) return -1
          var c = s
          return (
            o !== r && ((c = ct(o)), (c = c < 0 ? Nt(s + c, 0) : jt(c, s - 1))),
            e === e ? If(t, e, c) : Si(t, Ml, c, !0)
          )
        }
        function iv(t, e) {
          return t && t.length ? hc(t, ct(e)) : r
        }
        var ov = ht(eu)
        function eu(t, e) {
          return t && t.length && e && e.length ? xs(t, e) : t
        }
        function sv(t, e, o) {
          return t && t.length && e && e.length ? xs(t, e, Q(o, 2)) : t
        }
        function av(t, e, o) {
          return t && t.length && e && e.length ? xs(t, e, r, o) : t
        }
        var lv = cn(function (t, e) {
          var o = t == null ? 0 : t.length,
            s = gs(t, e)
          return (
            pc(
              t,
              kt(e, function (c) {
                return un(c, o) ? +c : c
              }).sort(Sc),
            ),
            s
          )
        })
        function cv(t, e) {
          var o = []
          if (!(t && t.length)) return o
          var s = -1,
            c = [],
            d = t.length
          for (e = Q(e, 3); ++s < d; ) {
            var p = t[s]
            e(p, s, t) && (o.push(p), c.push(s))
          }
          return pc(t, c), o
        }
        function Ws(t) {
          return t == null ? t : np.call(t)
        }
        function uv(t, e, o) {
          var s = t == null ? 0 : t.length
          return s
            ? (o && typeof o != 'number' && te(t, e, o)
                ? ((e = 0), (o = s))
                : ((e = e == null ? 0 : ct(e)), (o = o === r ? s : ct(o))),
              Te(t, e, o))
            : []
        }
        function hv(t, e) {
          return qi(t, e)
        }
        function dv(t, e, o) {
          return As(t, e, Q(o, 2))
        }
        function fv(t, e) {
          var o = t == null ? 0 : t.length
          if (o) {
            var s = qi(t, e)
            if (s < o && He(t[s], e)) return s
          }
          return -1
        }
        function pv(t, e) {
          return qi(t, e, !0)
        }
        function gv(t, e, o) {
          return As(t, e, Q(o, 2), !0)
        }
        function vv(t, e) {
          var o = t == null ? 0 : t.length
          if (o) {
            var s = qi(t, e, !0) - 1
            if (He(t[s], e)) return s
          }
          return -1
        }
        function mv(t) {
          return t && t.length ? vc(t) : []
        }
        function bv(t, e) {
          return t && t.length ? vc(t, Q(e, 2)) : []
        }
        function yv(t) {
          var e = t == null ? 0 : t.length
          return e ? Te(t, 1, e) : []
        }
        function _v(t, e, o) {
          return t && t.length
            ? ((e = o || e === r ? 1 : ct(e)), Te(t, 0, e < 0 ? 0 : e))
            : []
        }
        function wv(t, e, o) {
          var s = t == null ? 0 : t.length
          return s
            ? ((e = o || e === r ? 1 : ct(e)),
              (e = s - e),
              Te(t, e < 0 ? 0 : e, s))
            : []
        }
        function $v(t, e) {
          return t && t.length ? Yi(t, Q(e, 3), !1, !0) : []
        }
        function xv(t, e) {
          return t && t.length ? Yi(t, Q(e, 3)) : []
        }
        var Sv = ht(function (t) {
            return Cn(Xt(t, 1, Rt, !0))
          }),
          Cv = ht(function (t) {
            var e = Oe(t)
            return Rt(e) && (e = r), Cn(Xt(t, 1, Rt, !0), Q(e, 2))
          }),
          Av = ht(function (t) {
            var e = Oe(t)
            return (
              (e = typeof e == 'function' ? e : r), Cn(Xt(t, 1, Rt, !0), r, e)
            )
          })
        function Ev(t) {
          return t && t.length ? Cn(t) : []
        }
        function kv(t, e) {
          return t && t.length ? Cn(t, Q(e, 2)) : []
        }
        function Tv(t, e) {
          return (
            (e = typeof e == 'function' ? e : r),
            t && t.length ? Cn(t, r, e) : []
          )
        }
        function qs(t) {
          if (!(t && t.length)) return []
          var e = 0
          return (
            (t = _n(t, function (o) {
              if (Rt(o)) return (e = Nt(o.length, e)), !0
            })),
            as(e, function (o) {
              return kt(t, is(o))
            })
          )
        }
        function nu(t, e) {
          if (!(t && t.length)) return []
          var o = qs(t)
          return e == null
            ? o
            : kt(o, function (s) {
                return fe(e, r, s)
              })
        }
        var Ov = ht(function (t, e) {
            return Rt(t) ? qr(t, e) : []
          }),
          zv = ht(function (t) {
            return ks(_n(t, Rt))
          }),
          Pv = ht(function (t) {
            var e = Oe(t)
            return Rt(e) && (e = r), ks(_n(t, Rt), Q(e, 2))
          }),
          Rv = ht(function (t) {
            var e = Oe(t)
            return (e = typeof e == 'function' ? e : r), ks(_n(t, Rt), r, e)
          }),
          Mv = ht(qs)
        function Lv(t, e) {
          return _c(t || [], e || [], Wr)
        }
        function Iv(t, e) {
          return _c(t || [], e || [], Kr)
        }
        var Dv = ht(function (t) {
          var e = t.length,
            o = e > 1 ? t[e - 1] : r
          return (o = typeof o == 'function' ? (t.pop(), o) : r), nu(t, o)
        })
        function ru(t) {
          var e = u(t)
          return (e.__chain__ = !0), e
        }
        function Bv(t, e) {
          return e(t), t
        }
        function to(t, e) {
          return e(t)
        }
        var Uv = cn(function (t) {
          var e = t.length,
            o = e ? t[0] : 0,
            s = this.__wrapped__,
            c = function (d) {
              return gs(d, t)
            }
          return e > 1 ||
            this.__actions__.length ||
            !(s instanceof gt) ||
            !un(o)
            ? this.thru(c)
            : ((s = s.slice(o, +o + (e ? 1 : 0))),
              s.__actions__.push({
                func: to,
                args: [c],
                thisArg: r,
              }),
              new Ee(s, this.__chain__).thru(function (d) {
                return e && !d.length && d.push(r), d
              }))
        })
        function Nv() {
          return ru(this)
        }
        function Fv() {
          return new Ee(this.value(), this.__chain__)
        }
        function Hv() {
          this.__values__ === r && (this.__values__ = mu(this.value()))
          var t = this.__index__ >= this.__values__.length,
            e = t ? r : this.__values__[this.__index__++]
          return { done: t, value: e }
        }
        function Wv() {
          return this
        }
        function qv(t) {
          for (var e, o = this; o instanceof Ui; ) {
            var s = Zc(o)
            ;(s.__index__ = 0),
              (s.__values__ = r),
              e ? (c.__wrapped__ = s) : (e = s)
            var c = s
            o = o.__wrapped__
          }
          return (c.__wrapped__ = t), e
        }
        function Yv() {
          var t = this.__wrapped__
          if (t instanceof gt) {
            var e = t
            return (
              this.__actions__.length && (e = new gt(this)),
              (e = e.reverse()),
              e.__actions__.push({
                func: to,
                args: [Ws],
                thisArg: r,
              }),
              new Ee(e, this.__chain__)
            )
          }
          return this.thru(Ws)
        }
        function Gv() {
          return yc(this.__wrapped__, this.__actions__)
        }
        var Kv = Gi(function (t, e, o) {
          _t.call(t, o) ? ++t[o] : an(t, o, 1)
        })
        function Vv(t, e, o) {
          var s = lt(t) ? Pl : Bp
          return o && te(t, e, o) && (e = r), s(t, Q(e, 3))
        }
        function Xv(t, e) {
          var o = lt(t) ? _n : nc
          return o(t, Q(e, 3))
        }
        var Zv = Oc(jc),
          jv = Oc(Jc)
        function Jv(t, e) {
          return Xt(eo(t, e), 1)
        }
        function Qv(t, e) {
          return Xt(eo(t, e), H)
        }
        function tm(t, e, o) {
          return (o = o === r ? 1 : ct(o)), Xt(eo(t, e), o)
        }
        function iu(t, e) {
          var o = lt(t) ? Ce : Sn
          return o(t, Q(e, 3))
        }
        function ou(t, e) {
          var o = lt(t) ? yf : ec
          return o(t, Q(e, 3))
        }
        var em = Gi(function (t, e, o) {
          _t.call(t, o) ? t[o].push(e) : an(t, o, [e])
        })
        function nm(t, e, o, s) {
          ;(t = re(t) ? t : vr(t)), (o = o && !s ? ct(o) : 0)
          var c = t.length
          return (
            o < 0 && (o = Nt(c + o, 0)),
            so(t) ? o <= c && t.indexOf(e, o) > -1 : !!c && ir(t, e, o) > -1
          )
        }
        var rm = ht(function (t, e, o) {
            var s = -1,
              c = typeof e == 'function',
              d = re(t) ? $(t.length) : []
            return (
              Sn(t, function (p) {
                d[++s] = c ? fe(e, p, o) : Yr(p, e, o)
              }),
              d
            )
          }),
          im = Gi(function (t, e, o) {
            an(t, o, e)
          })
        function eo(t, e) {
          var o = lt(t) ? kt : lc
          return o(t, Q(e, 3))
        }
        function om(t, e, o, s) {
          return t == null
            ? []
            : (lt(e) || (e = e == null ? [] : [e]),
              (o = s ? r : o),
              lt(o) || (o = o == null ? [] : [o]),
              dc(t, e, o))
        }
        var sm = Gi(
          function (t, e, o) {
            t[o ? 0 : 1].push(e)
          },
          function () {
            return [[], []]
          },
        )
        function am(t, e, o) {
          var s = lt(t) ? ns : Il,
            c = arguments.length < 3
          return s(t, Q(e, 4), o, c, Sn)
        }
        function lm(t, e, o) {
          var s = lt(t) ? _f : Il,
            c = arguments.length < 3
          return s(t, Q(e, 4), o, c, ec)
        }
        function cm(t, e) {
          var o = lt(t) ? _n : nc
          return o(t, io(Q(e, 3)))
        }
        function um(t) {
          var e = lt(t) ? jl : ng
          return e(t)
        }
        function hm(t, e, o) {
          ;(o ? te(t, e, o) : e === r) ? (e = 1) : (e = ct(e))
          var s = lt(t) ? Rp : rg
          return s(t, e)
        }
        function dm(t) {
          var e = lt(t) ? Mp : og
          return e(t)
        }
        function fm(t) {
          if (t == null) return 0
          if (re(t)) return so(t) ? sr(t) : t.length
          var e = Jt(t)
          return e == Be || e == Ue ? t.size : ws(t).length
        }
        function pm(t, e, o) {
          var s = lt(t) ? rs : sg
          return o && te(t, e, o) && (e = r), s(t, Q(e, 3))
        }
        var gm = ht(function (t, e) {
            if (t == null) return []
            var o = e.length
            return (
              o > 1 && te(t, e[0], e[1])
                ? (e = [])
                : o > 2 && te(e[0], e[1], e[2]) && (e = [e[0]]),
              dc(t, Xt(e, 1), [])
            )
          }),
          no =
            Xf ||
            function () {
              return Vt.Date.now()
            }
        function vm(t, e) {
          if (typeof e != 'function') throw new Ae(f)
          return (
            (t = ct(t)),
            function () {
              if (--t < 1) return e.apply(this, arguments)
            }
          )
        }
        function su(t, e, o) {
          return (
            (e = o ? r : e),
            (e = t && e == null ? t.length : e),
            ln(t, I, r, r, r, r, e)
          )
        }
        function au(t, e) {
          var o
          if (typeof e != 'function') throw new Ae(f)
          return (
            (t = ct(t)),
            function () {
              return (
                --t > 0 && (o = e.apply(this, arguments)), t <= 1 && (e = r), o
              )
            }
          )
        }
        var Ys = ht(function (t, e, o) {
            var s = x
            if (o.length) {
              var c = $n(o, pr(Ys))
              s |= J
            }
            return ln(t, s, e, o, c)
          }),
          lu = ht(function (t, e, o) {
            var s = x | P
            if (o.length) {
              var c = $n(o, pr(lu))
              s |= J
            }
            return ln(e, s, t, o, c)
          })
        function cu(t, e, o) {
          e = o ? r : e
          var s = ln(t, N, r, r, r, r, r, e)
          return (s.placeholder = cu.placeholder), s
        }
        function uu(t, e, o) {
          e = o ? r : e
          var s = ln(t, it, r, r, r, r, r, e)
          return (s.placeholder = uu.placeholder), s
        }
        function hu(t, e, o) {
          var s,
            c,
            d,
            p,
            g,
            y,
            A = 0,
            E = !1,
            z = !1,
            F = !0
          if (typeof t != 'function') throw new Ae(f)
          ;(e = ze(e) || 0),
            Ot(o) &&
              ((E = !!o.leading),
              (z = 'maxWait' in o),
              (d = z ? Nt(ze(o.maxWait) || 0, e) : d),
              (F = 'trailing' in o ? !!o.trailing : F))
          function Z(Mt) {
            var We = s,
              fn = c
            return (s = c = r), (A = Mt), (p = t.apply(fn, We)), p
          }
          function tt(Mt) {
            return (A = Mt), (g = Zr(ft, e)), E ? Z(Mt) : p
          }
          function ut(Mt) {
            var We = Mt - y,
              fn = Mt - A,
              Ou = e - We
            return z ? jt(Ou, d - fn) : Ou
          }
          function et(Mt) {
            var We = Mt - y,
              fn = Mt - A
            return y === r || We >= e || We < 0 || (z && fn >= d)
          }
          function ft() {
            var Mt = no()
            if (et(Mt)) return vt(Mt)
            g = Zr(ft, ut(Mt))
          }
          function vt(Mt) {
            return (g = r), F && s ? Z(Mt) : ((s = c = r), p)
          }
          function me() {
            g !== r && wc(g), (A = 0), (s = y = c = g = r)
          }
          function ee() {
            return g === r ? p : vt(no())
          }
          function be() {
            var Mt = no(),
              We = et(Mt)
            if (((s = arguments), (c = this), (y = Mt), We)) {
              if (g === r) return tt(y)
              if (z) return wc(g), (g = Zr(ft, e)), Z(y)
            }
            return g === r && (g = Zr(ft, e)), p
          }
          return (be.cancel = me), (be.flush = ee), be
        }
        var mm = ht(function (t, e) {
            return tc(t, 1, e)
          }),
          bm = ht(function (t, e, o) {
            return tc(t, ze(e) || 0, o)
          })
        function ym(t) {
          return ln(t, nt)
        }
        function ro(t, e) {
          if (typeof t != 'function' || (e != null && typeof e != 'function'))
            throw new Ae(f)
          var o = function () {
            var s = arguments,
              c = e ? e.apply(this, s) : s[0],
              d = o.cache
            if (d.has(c)) return d.get(c)
            var p = t.apply(this, s)
            return (o.cache = d.set(c, p) || d), p
          }
          return (o.cache = new (ro.Cache || sn)()), o
        }
        ro.Cache = sn
        function io(t) {
          if (typeof t != 'function') throw new Ae(f)
          return function () {
            var e = arguments
            switch (e.length) {
              case 0:
                return !t.call(this)
              case 1:
                return !t.call(this, e[0])
              case 2:
                return !t.call(this, e[0], e[1])
              case 3:
                return !t.call(this, e[0], e[1], e[2])
            }
            return !t.apply(this, e)
          }
        }
        function _m(t) {
          return au(2, t)
        }
        var wm = ag(function (t, e) {
            e =
              e.length == 1 && lt(e[0])
                ? kt(e[0], pe(Q()))
                : kt(Xt(e, 1), pe(Q()))
            var o = e.length
            return ht(function (s) {
              for (var c = -1, d = jt(s.length, o); ++c < d; )
                s[c] = e[c].call(this, s[c])
              return fe(t, this, s)
            })
          }),
          Gs = ht(function (t, e) {
            var o = $n(e, pr(Gs))
            return ln(t, J, r, e, o)
          }),
          du = ht(function (t, e) {
            var o = $n(e, pr(du))
            return ln(t, q, r, e, o)
          }),
          $m = cn(function (t, e) {
            return ln(t, M, r, r, r, e)
          })
        function xm(t, e) {
          if (typeof t != 'function') throw new Ae(f)
          return (e = e === r ? e : ct(e)), ht(t, e)
        }
        function Sm(t, e) {
          if (typeof t != 'function') throw new Ae(f)
          return (
            (e = e == null ? 0 : Nt(ct(e), 0)),
            ht(function (o) {
              var s = o[e],
                c = En(o, 0, e)
              return s && wn(c, s), fe(t, this, c)
            })
          )
        }
        function Cm(t, e, o) {
          var s = !0,
            c = !0
          if (typeof t != 'function') throw new Ae(f)
          return (
            Ot(o) &&
              ((s = 'leading' in o ? !!o.leading : s),
              (c = 'trailing' in o ? !!o.trailing : c)),
            hu(t, e, {
              leading: s,
              maxWait: e,
              trailing: c,
            })
          )
        }
        function Am(t) {
          return su(t, 1)
        }
        function Em(t, e) {
          return Gs(Os(e), t)
        }
        function km() {
          if (!arguments.length) return []
          var t = arguments[0]
          return lt(t) ? t : [t]
        }
        function Tm(t) {
          return ke(t, T)
        }
        function Om(t, e) {
          return (e = typeof e == 'function' ? e : r), ke(t, T, e)
        }
        function zm(t) {
          return ke(t, C | T)
        }
        function Pm(t, e) {
          return (e = typeof e == 'function' ? e : r), ke(t, C | T, e)
        }
        function Rm(t, e) {
          return e == null || Ql(t, e, Gt(e))
        }
        function He(t, e) {
          return t === e || (t !== t && e !== e)
        }
        var Mm = Zi(bs),
          Lm = Zi(function (t, e) {
            return t >= e
          }),
          Kn = oc(
            /* @__PURE__ */ (function () {
              return arguments
            })(),
          )
            ? oc
            : function (t) {
                return Pt(t) && _t.call(t, 'callee') && !Yl.call(t, 'callee')
              },
          lt = $.isArray,
          Im = Al ? pe(Al) : qp
        function re(t) {
          return t != null && oo(t.length) && !hn(t)
        }
        function Rt(t) {
          return Pt(t) && re(t)
        }
        function Dm(t) {
          return t === !0 || t === !1 || (Pt(t) && Qt(t) == De)
        }
        var kn = jf || ra,
          Bm = El ? pe(El) : Yp
        function Um(t) {
          return Pt(t) && t.nodeType === 1 && !jr(t)
        }
        function Nm(t) {
          if (t == null) return !0
          if (
            re(t) &&
            (lt(t) ||
              typeof t == 'string' ||
              typeof t.splice == 'function' ||
              kn(t) ||
              gr(t) ||
              Kn(t))
          )
            return !t.length
          var e = Jt(t)
          if (e == Be || e == Ue) return !t.size
          if (Xr(t)) return !ws(t).length
          for (var o in t) if (_t.call(t, o)) return !1
          return !0
        }
        function Fm(t, e) {
          return Gr(t, e)
        }
        function Hm(t, e, o) {
          o = typeof o == 'function' ? o : r
          var s = o ? o(t, e) : r
          return s === r ? Gr(t, e, r, o) : !!s
        }
        function Ks(t) {
          if (!Pt(t)) return !1
          var e = Qt(t)
          return (
            e == de ||
            e == Zt ||
            (typeof t.message == 'string' &&
              typeof t.name == 'string' &&
              !jr(t))
          )
        }
        function Wm(t) {
          return typeof t == 'number' && Kl(t)
        }
        function hn(t) {
          if (!Ot(t)) return !1
          var e = Qt(t)
          return e == Ke || e == Bn || e == nn || e == dd
        }
        function fu(t) {
          return typeof t == 'number' && t == ct(t)
        }
        function oo(t) {
          return typeof t == 'number' && t > -1 && t % 1 == 0 && t <= B
        }
        function Ot(t) {
          var e = typeof t
          return t != null && (e == 'object' || e == 'function')
        }
        function Pt(t) {
          return t != null && typeof t == 'object'
        }
        var pu = kl ? pe(kl) : Kp
        function qm(t, e) {
          return t === e || _s(t, e, Ds(e))
        }
        function Ym(t, e, o) {
          return (o = typeof o == 'function' ? o : r), _s(t, e, Ds(e), o)
        }
        function Gm(t) {
          return gu(t) && t != +t
        }
        function Km(t) {
          if (Og(t)) throw new st(h)
          return sc(t)
        }
        function Vm(t) {
          return t === null
        }
        function Xm(t) {
          return t == null
        }
        function gu(t) {
          return typeof t == 'number' || (Pt(t) && Qt(t) == Pr)
        }
        function jr(t) {
          if (!Pt(t) || Qt(t) != rn) return !1
          var e = Pi(t)
          if (e === null) return !0
          var o = _t.call(e, 'constructor') && e.constructor
          return typeof o == 'function' && o instanceof o && ki.call(o) == Yf
        }
        var Vs = Tl ? pe(Tl) : Vp
        function Zm(t) {
          return fu(t) && t >= -B && t <= B
        }
        var vu = Ol ? pe(Ol) : Xp
        function so(t) {
          return typeof t == 'string' || (!lt(t) && Pt(t) && Qt(t) == Mr)
        }
        function ve(t) {
          return typeof t == 'symbol' || (Pt(t) && Qt(t) == yi)
        }
        var gr = zl ? pe(zl) : Zp
        function jm(t) {
          return t === r
        }
        function Jm(t) {
          return Pt(t) && Jt(t) == Lr
        }
        function Qm(t) {
          return Pt(t) && Qt(t) == pd
        }
        var tb = Zi($s),
          eb = Zi(function (t, e) {
            return t <= e
          })
        function mu(t) {
          if (!t) return []
          if (re(t)) return so(t) ? Ne(t) : ne(t)
          if (Br && t[Br]) return Rf(t[Br]())
          var e = Jt(t),
            o = e == Be ? cs : e == Ue ? Ci : vr
          return o(t)
        }
        function dn(t) {
          if (!t) return t === 0 ? t : 0
          if (((t = ze(t)), t === H || t === -H)) {
            var e = t < 0 ? -1 : 1
            return e * at
          }
          return t === t ? t : 0
        }
        function ct(t) {
          var e = dn(t),
            o = e % 1
          return e === e ? (o ? e - o : e) : 0
        }
        function bu(t) {
          return t ? Wn(ct(t), 0, dt) : 0
        }
        function ze(t) {
          if (typeof t == 'number') return t
          if (ve(t)) return rt
          if (Ot(t)) {
            var e = typeof t.valueOf == 'function' ? t.valueOf() : t
            t = Ot(e) ? e + '' : e
          }
          if (typeof t != 'string') return t === 0 ? t : +t
          t = Dl(t)
          var o = Ld.test(t)
          return o || Dd.test(t)
            ? vf(t.slice(2), o ? 2 : 8)
            : Md.test(t)
            ? rt
            : +t
        }
        function yu(t) {
          return Xe(t, ie(t))
        }
        function nb(t) {
          return t ? Wn(ct(t), -B, B) : t === 0 ? t : 0
        }
        function yt(t) {
          return t == null ? '' : ge(t)
        }
        var rb = dr(function (t, e) {
            if (Xr(e) || re(e)) {
              Xe(e, Gt(e), t)
              return
            }
            for (var o in e) _t.call(e, o) && Wr(t, o, e[o])
          }),
          _u = dr(function (t, e) {
            Xe(e, ie(e), t)
          }),
          ao = dr(function (t, e, o, s) {
            Xe(e, ie(e), t, s)
          }),
          ib = dr(function (t, e, o, s) {
            Xe(e, Gt(e), t, s)
          }),
          ob = cn(gs)
        function sb(t, e) {
          var o = hr(t)
          return e == null ? o : Jl(o, e)
        }
        var ab = ht(function (t, e) {
            t = $t(t)
            var o = -1,
              s = e.length,
              c = s > 2 ? e[2] : r
            for (c && te(e[0], e[1], c) && (s = 1); ++o < s; )
              for (var d = e[o], p = ie(d), g = -1, y = p.length; ++g < y; ) {
                var A = p[g],
                  E = t[A]
                ;(E === r || (He(E, lr[A]) && !_t.call(t, A))) && (t[A] = d[A])
              }
            return t
          }),
          lb = ht(function (t) {
            return t.push(r, Dc), fe(wu, r, t)
          })
        function cb(t, e) {
          return Rl(t, Q(e, 3), Ve)
        }
        function ub(t, e) {
          return Rl(t, Q(e, 3), ms)
        }
        function hb(t, e) {
          return t == null ? t : vs(t, Q(e, 3), ie)
        }
        function db(t, e) {
          return t == null ? t : rc(t, Q(e, 3), ie)
        }
        function fb(t, e) {
          return t && Ve(t, Q(e, 3))
        }
        function pb(t, e) {
          return t && ms(t, Q(e, 3))
        }
        function gb(t) {
          return t == null ? [] : Hi(t, Gt(t))
        }
        function vb(t) {
          return t == null ? [] : Hi(t, ie(t))
        }
        function Xs(t, e, o) {
          var s = t == null ? r : qn(t, e)
          return s === r ? o : s
        }
        function mb(t, e) {
          return t != null && Nc(t, e, Np)
        }
        function Zs(t, e) {
          return t != null && Nc(t, e, Fp)
        }
        var bb = Pc(function (t, e, o) {
            e != null && typeof e.toString != 'function' && (e = Ti.call(e)),
              (t[e] = o)
          }, Js(oe)),
          yb = Pc(function (t, e, o) {
            e != null && typeof e.toString != 'function' && (e = Ti.call(e)),
              _t.call(t, e) ? t[e].push(o) : (t[e] = [o])
          }, Q),
          _b = ht(Yr)
        function Gt(t) {
          return re(t) ? Zl(t) : ws(t)
        }
        function ie(t) {
          return re(t) ? Zl(t, !0) : jp(t)
        }
        function wb(t, e) {
          var o = {}
          return (
            (e = Q(e, 3)),
            Ve(t, function (s, c, d) {
              an(o, e(s, c, d), s)
            }),
            o
          )
        }
        function $b(t, e) {
          var o = {}
          return (
            (e = Q(e, 3)),
            Ve(t, function (s, c, d) {
              an(o, c, e(s, c, d))
            }),
            o
          )
        }
        var xb = dr(function (t, e, o) {
            Wi(t, e, o)
          }),
          wu = dr(function (t, e, o, s) {
            Wi(t, e, o, s)
          }),
          Sb = cn(function (t, e) {
            var o = {}
            if (t == null) return o
            var s = !1
            ;(e = kt(e, function (d) {
              return (d = An(d, t)), s || (s = d.length > 1), d
            })),
              Xe(t, Ls(t), o),
              s && (o = ke(o, C | U | T, bg))
            for (var c = e.length; c--; ) Es(o, e[c])
            return o
          })
        function Cb(t, e) {
          return $u(t, io(Q(e)))
        }
        var Ab = cn(function (t, e) {
          return t == null ? {} : Qp(t, e)
        })
        function $u(t, e) {
          if (t == null) return {}
          var o = kt(Ls(t), function (s) {
            return [s]
          })
          return (
            (e = Q(e)),
            fc(t, o, function (s, c) {
              return e(s, c[0])
            })
          )
        }
        function Eb(t, e, o) {
          e = An(e, t)
          var s = -1,
            c = e.length
          for (c || ((c = 1), (t = r)); ++s < c; ) {
            var d = t == null ? r : t[Ze(e[s])]
            d === r && ((s = c), (d = o)), (t = hn(d) ? d.call(t) : d)
          }
          return t
        }
        function kb(t, e, o) {
          return t == null ? t : Kr(t, e, o)
        }
        function Tb(t, e, o, s) {
          return (
            (s = typeof s == 'function' ? s : r), t == null ? t : Kr(t, e, o, s)
          )
        }
        var xu = Lc(Gt),
          Su = Lc(ie)
        function Ob(t, e, o) {
          var s = lt(t),
            c = s || kn(t) || gr(t)
          if (((e = Q(e, 4)), o == null)) {
            var d = t && t.constructor
            c
              ? (o = s ? new d() : [])
              : Ot(t)
              ? (o = hn(d) ? hr(Pi(t)) : {})
              : (o = {})
          }
          return (
            (c ? Ce : Ve)(t, function (p, g, y) {
              return e(o, p, g, y)
            }),
            o
          )
        }
        function zb(t, e) {
          return t == null ? !0 : Es(t, e)
        }
        function Pb(t, e, o) {
          return t == null ? t : bc(t, e, Os(o))
        }
        function Rb(t, e, o, s) {
          return (
            (s = typeof s == 'function' ? s : r),
            t == null ? t : bc(t, e, Os(o), s)
          )
        }
        function vr(t) {
          return t == null ? [] : ls(t, Gt(t))
        }
        function Mb(t) {
          return t == null ? [] : ls(t, ie(t))
        }
        function Lb(t, e, o) {
          return (
            o === r && ((o = e), (e = r)),
            o !== r && ((o = ze(o)), (o = o === o ? o : 0)),
            e !== r && ((e = ze(e)), (e = e === e ? e : 0)),
            Wn(ze(t), e, o)
          )
        }
        function Ib(t, e, o) {
          return (
            (e = dn(e)),
            o === r ? ((o = e), (e = 0)) : (o = dn(o)),
            (t = ze(t)),
            Hp(t, e, o)
          )
        }
        function Db(t, e, o) {
          if (
            (o && typeof o != 'boolean' && te(t, e, o) && (e = o = r),
            o === r &&
              (typeof e == 'boolean'
                ? ((o = e), (e = r))
                : typeof t == 'boolean' && ((o = t), (t = r))),
            t === r && e === r
              ? ((t = 0), (e = 1))
              : ((t = dn(t)), e === r ? ((e = t), (t = 0)) : (e = dn(e))),
            t > e)
          ) {
            var s = t
            ;(t = e), (e = s)
          }
          if (o || t % 1 || e % 1) {
            var c = Vl()
            return jt(t + c * (e - t + gf('1e-' + ((c + '').length - 1))), e)
          }
          return Ss(t, e)
        }
        var Bb = fr(function (t, e, o) {
          return (e = e.toLowerCase()), t + (o ? Cu(e) : e)
        })
        function Cu(t) {
          return js(yt(t).toLowerCase())
        }
        function Au(t) {
          return (t = yt(t)), t && t.replace(Ud, kf).replace(of, '')
        }
        function Ub(t, e, o) {
          ;(t = yt(t)), (e = ge(e))
          var s = t.length
          o = o === r ? s : Wn(ct(o), 0, s)
          var c = o
          return (o -= e.length), o >= 0 && t.slice(o, c) == e
        }
        function Nb(t) {
          return (t = yt(t)), t && yd.test(t) ? t.replace(rl, Tf) : t
        }
        function Fb(t) {
          return (t = yt(t)), t && Cd.test(t) ? t.replace(Go, '\\$&') : t
        }
        var Hb = fr(function (t, e, o) {
            return t + (o ? '-' : '') + e.toLowerCase()
          }),
          Wb = fr(function (t, e, o) {
            return t + (o ? ' ' : '') + e.toLowerCase()
          }),
          qb = Tc('toLowerCase')
        function Yb(t, e, o) {
          ;(t = yt(t)), (e = ct(e))
          var s = e ? sr(t) : 0
          if (!e || s >= e) return t
          var c = (e - s) / 2
          return Xi(Ii(c), o) + t + Xi(Li(c), o)
        }
        function Gb(t, e, o) {
          ;(t = yt(t)), (e = ct(e))
          var s = e ? sr(t) : 0
          return e && s < e ? t + Xi(e - s, o) : t
        }
        function Kb(t, e, o) {
          ;(t = yt(t)), (e = ct(e))
          var s = e ? sr(t) : 0
          return e && s < e ? Xi(e - s, o) + t : t
        }
        function Vb(t, e, o) {
          return (
            o || e == null ? (e = 0) : e && (e = +e),
            ep(yt(t).replace(Ko, ''), e || 0)
          )
        }
        function Xb(t, e, o) {
          return (
            (o ? te(t, e, o) : e === r) ? (e = 1) : (e = ct(e)), Cs(yt(t), e)
          )
        }
        function Zb() {
          var t = arguments,
            e = yt(t[0])
          return t.length < 3 ? e : e.replace(t[1], t[2])
        }
        var jb = fr(function (t, e, o) {
          return t + (o ? '_' : '') + e.toLowerCase()
        })
        function Jb(t, e, o) {
          return (
            o && typeof o != 'number' && te(t, e, o) && (e = o = r),
            (o = o === r ? dt : o >>> 0),
            o
              ? ((t = yt(t)),
                t &&
                (typeof e == 'string' || (e != null && !Vs(e))) &&
                ((e = ge(e)), !e && or(t))
                  ? En(Ne(t), 0, o)
                  : t.split(e, o))
              : []
          )
        }
        var Qb = fr(function (t, e, o) {
          return t + (o ? ' ' : '') + js(e)
        })
        function t0(t, e, o) {
          return (
            (t = yt(t)),
            (o = o == null ? 0 : Wn(ct(o), 0, t.length)),
            (e = ge(e)),
            t.slice(o, o + e.length) == e
          )
        }
        function e0(t, e, o) {
          var s = u.templateSettings
          o && te(t, e, o) && (e = r), (t = yt(t)), (e = ao({}, e, s, Ic))
          var c = ao({}, e.imports, s.imports, Ic),
            d = Gt(c),
            p = ls(c, d),
            g,
            y,
            A = 0,
            E = e.interpolate || _i,
            z = "__p += '",
            F = us(
              (e.escape || _i).source +
                '|' +
                E.source +
                '|' +
                (E === il ? Rd : _i).source +
                '|' +
                (e.evaluate || _i).source +
                '|$',
              'g',
            ),
            Z =
              '//# sourceURL=' +
              (_t.call(e, 'sourceURL')
                ? (e.sourceURL + '').replace(/\s/g, ' ')
                : 'lodash.templateSources[' + ++uf + ']') +
              `
`
          t.replace(F, function (et, ft, vt, me, ee, be) {
            return (
              vt || (vt = me),
              (z += t.slice(A, be).replace(Nd, Of)),
              ft &&
                ((g = !0),
                (z +=
                  `' +
__e(` +
                  ft +
                  `) +
'`)),
              ee &&
                ((y = !0),
                (z +=
                  `';
` +
                  ee +
                  `;
__p += '`)),
              vt &&
                (z +=
                  `' +
((__t = (` +
                  vt +
                  `)) == null ? '' : __t) +
'`),
              (A = be + et.length),
              et
            )
          }),
            (z += `';
`)
          var tt = _t.call(e, 'variable') && e.variable
          if (!tt)
            z =
              `with (obj) {
` +
              z +
              `
}
`
          else if (zd.test(tt)) throw new st(v)
          ;(z = (y ? z.replace(gd, '') : z)
            .replace(vd, '$1')
            .replace(md, '$1;')),
            (z =
              'function(' +
              (tt || 'obj') +
              `) {
` +
              (tt
                ? ''
                : `obj || (obj = {});
`) +
              "var __t, __p = ''" +
              (g ? ', __e = _.escape' : '') +
              (y
                ? `, __j = Array.prototype.join;
function print() { __p += __j.call(arguments, '') }
`
                : `;
`) +
              z +
              `return __p
}`)
          var ut = ku(function () {
            return bt(d, Z + 'return ' + z).apply(r, p)
          })
          if (((ut.source = z), Ks(ut))) throw ut
          return ut
        }
        function n0(t) {
          return yt(t).toLowerCase()
        }
        function r0(t) {
          return yt(t).toUpperCase()
        }
        function i0(t, e, o) {
          if (((t = yt(t)), t && (o || e === r))) return Dl(t)
          if (!t || !(e = ge(e))) return t
          var s = Ne(t),
            c = Ne(e),
            d = Bl(s, c),
            p = Ul(s, c) + 1
          return En(s, d, p).join('')
        }
        function o0(t, e, o) {
          if (((t = yt(t)), t && (o || e === r))) return t.slice(0, Fl(t) + 1)
          if (!t || !(e = ge(e))) return t
          var s = Ne(t),
            c = Ul(s, Ne(e)) + 1
          return En(s, 0, c).join('')
        }
        function s0(t, e, o) {
          if (((t = yt(t)), t && (o || e === r))) return t.replace(Ko, '')
          if (!t || !(e = ge(e))) return t
          var s = Ne(t),
            c = Bl(s, Ne(e))
          return En(s, c).join('')
        }
        function a0(t, e) {
          var o = ot,
            s = j
          if (Ot(e)) {
            var c = 'separator' in e ? e.separator : c
            ;(o = 'length' in e ? ct(e.length) : o),
              (s = 'omission' in e ? ge(e.omission) : s)
          }
          t = yt(t)
          var d = t.length
          if (or(t)) {
            var p = Ne(t)
            d = p.length
          }
          if (o >= d) return t
          var g = o - sr(s)
          if (g < 1) return s
          var y = p ? En(p, 0, g).join('') : t.slice(0, g)
          if (c === r) return y + s
          if ((p && (g += y.length - g), Vs(c))) {
            if (t.slice(g).search(c)) {
              var A,
                E = y
              for (
                c.global || (c = us(c.source, yt(ol.exec(c)) + 'g')),
                  c.lastIndex = 0;
                (A = c.exec(E));

              )
                var z = A.index
              y = y.slice(0, z === r ? g : z)
            }
          } else if (t.indexOf(ge(c), g) != g) {
            var F = y.lastIndexOf(c)
            F > -1 && (y = y.slice(0, F))
          }
          return y + s
        }
        function l0(t) {
          return (t = yt(t)), t && bd.test(t) ? t.replace(nl, Df) : t
        }
        var c0 = fr(function (t, e, o) {
            return t + (o ? ' ' : '') + e.toUpperCase()
          }),
          js = Tc('toUpperCase')
        function Eu(t, e, o) {
          return (
            (t = yt(t)),
            (e = o ? r : e),
            e === r ? (Pf(t) ? Nf(t) : xf(t)) : t.match(e) || []
          )
        }
        var ku = ht(function (t, e) {
            try {
              return fe(t, r, e)
            } catch (o) {
              return Ks(o) ? o : new st(o)
            }
          }),
          u0 = cn(function (t, e) {
            return (
              Ce(e, function (o) {
                ;(o = Ze(o)), an(t, o, Ys(t[o], t))
              }),
              t
            )
          })
        function h0(t) {
          var e = t == null ? 0 : t.length,
            o = Q()
          return (
            (t = e
              ? kt(t, function (s) {
                  if (typeof s[1] != 'function') throw new Ae(f)
                  return [o(s[0]), s[1]]
                })
              : []),
            ht(function (s) {
              for (var c = -1; ++c < e; ) {
                var d = t[c]
                if (fe(d[0], this, s)) return fe(d[1], this, s)
              }
            })
          )
        }
        function d0(t) {
          return Dp(ke(t, C))
        }
        function Js(t) {
          return function () {
            return t
          }
        }
        function f0(t, e) {
          return t == null || t !== t ? e : t
        }
        var p0 = zc(),
          g0 = zc(!0)
        function oe(t) {
          return t
        }
        function Qs(t) {
          return ac(typeof t == 'function' ? t : ke(t, C))
        }
        function v0(t) {
          return cc(ke(t, C))
        }
        function m0(t, e) {
          return uc(t, ke(e, C))
        }
        var b0 = ht(function (t, e) {
            return function (o) {
              return Yr(o, t, e)
            }
          }),
          y0 = ht(function (t, e) {
            return function (o) {
              return Yr(t, o, e)
            }
          })
        function ta(t, e, o) {
          var s = Gt(e),
            c = Hi(e, s)
          o == null &&
            !(Ot(e) && (c.length || !s.length)) &&
            ((o = e), (e = t), (t = this), (c = Hi(e, Gt(e))))
          var d = !(Ot(o) && 'chain' in o) || !!o.chain,
            p = hn(t)
          return (
            Ce(c, function (g) {
              var y = e[g]
              ;(t[g] = y),
                p &&
                  (t.prototype[g] = function () {
                    var A = this.__chain__
                    if (d || A) {
                      var E = t(this.__wrapped__),
                        z = (E.__actions__ = ne(this.__actions__))
                      return (
                        z.push({ func: y, args: arguments, thisArg: t }),
                        (E.__chain__ = A),
                        E
                      )
                    }
                    return y.apply(t, wn([this.value()], arguments))
                  })
            }),
            t
          )
        }
        function _0() {
          return Vt._ === this && (Vt._ = Gf), this
        }
        function ea() {}
        function w0(t) {
          return (
            (t = ct(t)),
            ht(function (e) {
              return hc(e, t)
            })
          )
        }
        var $0 = Ps(kt),
          x0 = Ps(Pl),
          S0 = Ps(rs)
        function Tu(t) {
          return Us(t) ? is(Ze(t)) : tg(t)
        }
        function C0(t) {
          return function (e) {
            return t == null ? r : qn(t, e)
          }
        }
        var A0 = Rc(),
          E0 = Rc(!0)
        function na() {
          return []
        }
        function ra() {
          return !1
        }
        function k0() {
          return {}
        }
        function T0() {
          return ''
        }
        function O0() {
          return !0
        }
        function z0(t, e) {
          if (((t = ct(t)), t < 1 || t > B)) return []
          var o = dt,
            s = jt(t, dt)
          ;(e = Q(e)), (t -= dt)
          for (var c = as(s, e); ++o < t; ) e(o)
          return c
        }
        function P0(t) {
          return lt(t) ? kt(t, Ze) : ve(t) ? [t] : ne(Xc(yt(t)))
        }
        function R0(t) {
          var e = ++qf
          return yt(t) + e
        }
        var M0 = Vi(function (t, e) {
            return t + e
          }, 0),
          L0 = Rs('ceil'),
          I0 = Vi(function (t, e) {
            return t / e
          }, 1),
          D0 = Rs('floor')
        function B0(t) {
          return t && t.length ? Fi(t, oe, bs) : r
        }
        function U0(t, e) {
          return t && t.length ? Fi(t, Q(e, 2), bs) : r
        }
        function N0(t) {
          return Ll(t, oe)
        }
        function F0(t, e) {
          return Ll(t, Q(e, 2))
        }
        function H0(t) {
          return t && t.length ? Fi(t, oe, $s) : r
        }
        function W0(t, e) {
          return t && t.length ? Fi(t, Q(e, 2), $s) : r
        }
        var q0 = Vi(function (t, e) {
            return t * e
          }, 1),
          Y0 = Rs('round'),
          G0 = Vi(function (t, e) {
            return t - e
          }, 0)
        function K0(t) {
          return t && t.length ? ss(t, oe) : 0
        }
        function V0(t, e) {
          return t && t.length ? ss(t, Q(e, 2)) : 0
        }
        return (
          (u.after = vm),
          (u.ary = su),
          (u.assign = rb),
          (u.assignIn = _u),
          (u.assignInWith = ao),
          (u.assignWith = ib),
          (u.at = ob),
          (u.before = au),
          (u.bind = Ys),
          (u.bindAll = u0),
          (u.bindKey = lu),
          (u.castArray = km),
          (u.chain = ru),
          (u.chunk = Dg),
          (u.compact = Bg),
          (u.concat = Ug),
          (u.cond = h0),
          (u.conforms = d0),
          (u.constant = Js),
          (u.countBy = Kv),
          (u.create = sb),
          (u.curry = cu),
          (u.curryRight = uu),
          (u.debounce = hu),
          (u.defaults = ab),
          (u.defaultsDeep = lb),
          (u.defer = mm),
          (u.delay = bm),
          (u.difference = Ng),
          (u.differenceBy = Fg),
          (u.differenceWith = Hg),
          (u.drop = Wg),
          (u.dropRight = qg),
          (u.dropRightWhile = Yg),
          (u.dropWhile = Gg),
          (u.fill = Kg),
          (u.filter = Xv),
          (u.flatMap = Jv),
          (u.flatMapDeep = Qv),
          (u.flatMapDepth = tm),
          (u.flatten = Qc),
          (u.flattenDeep = Vg),
          (u.flattenDepth = Xg),
          (u.flip = ym),
          (u.flow = p0),
          (u.flowRight = g0),
          (u.fromPairs = Zg),
          (u.functions = gb),
          (u.functionsIn = vb),
          (u.groupBy = em),
          (u.initial = Jg),
          (u.intersection = Qg),
          (u.intersectionBy = tv),
          (u.intersectionWith = ev),
          (u.invert = bb),
          (u.invertBy = yb),
          (u.invokeMap = rm),
          (u.iteratee = Qs),
          (u.keyBy = im),
          (u.keys = Gt),
          (u.keysIn = ie),
          (u.map = eo),
          (u.mapKeys = wb),
          (u.mapValues = $b),
          (u.matches = v0),
          (u.matchesProperty = m0),
          (u.memoize = ro),
          (u.merge = xb),
          (u.mergeWith = wu),
          (u.method = b0),
          (u.methodOf = y0),
          (u.mixin = ta),
          (u.negate = io),
          (u.nthArg = w0),
          (u.omit = Sb),
          (u.omitBy = Cb),
          (u.once = _m),
          (u.orderBy = om),
          (u.over = $0),
          (u.overArgs = wm),
          (u.overEvery = x0),
          (u.overSome = S0),
          (u.partial = Gs),
          (u.partialRight = du),
          (u.partition = sm),
          (u.pick = Ab),
          (u.pickBy = $u),
          (u.property = Tu),
          (u.propertyOf = C0),
          (u.pull = ov),
          (u.pullAll = eu),
          (u.pullAllBy = sv),
          (u.pullAllWith = av),
          (u.pullAt = lv),
          (u.range = A0),
          (u.rangeRight = E0),
          (u.rearg = $m),
          (u.reject = cm),
          (u.remove = cv),
          (u.rest = xm),
          (u.reverse = Ws),
          (u.sampleSize = hm),
          (u.set = kb),
          (u.setWith = Tb),
          (u.shuffle = dm),
          (u.slice = uv),
          (u.sortBy = gm),
          (u.sortedUniq = mv),
          (u.sortedUniqBy = bv),
          (u.split = Jb),
          (u.spread = Sm),
          (u.tail = yv),
          (u.take = _v),
          (u.takeRight = wv),
          (u.takeRightWhile = $v),
          (u.takeWhile = xv),
          (u.tap = Bv),
          (u.throttle = Cm),
          (u.thru = to),
          (u.toArray = mu),
          (u.toPairs = xu),
          (u.toPairsIn = Su),
          (u.toPath = P0),
          (u.toPlainObject = yu),
          (u.transform = Ob),
          (u.unary = Am),
          (u.union = Sv),
          (u.unionBy = Cv),
          (u.unionWith = Av),
          (u.uniq = Ev),
          (u.uniqBy = kv),
          (u.uniqWith = Tv),
          (u.unset = zb),
          (u.unzip = qs),
          (u.unzipWith = nu),
          (u.update = Pb),
          (u.updateWith = Rb),
          (u.values = vr),
          (u.valuesIn = Mb),
          (u.without = Ov),
          (u.words = Eu),
          (u.wrap = Em),
          (u.xor = zv),
          (u.xorBy = Pv),
          (u.xorWith = Rv),
          (u.zip = Mv),
          (u.zipObject = Lv),
          (u.zipObjectDeep = Iv),
          (u.zipWith = Dv),
          (u.entries = xu),
          (u.entriesIn = Su),
          (u.extend = _u),
          (u.extendWith = ao),
          ta(u, u),
          (u.add = M0),
          (u.attempt = ku),
          (u.camelCase = Bb),
          (u.capitalize = Cu),
          (u.ceil = L0),
          (u.clamp = Lb),
          (u.clone = Tm),
          (u.cloneDeep = zm),
          (u.cloneDeepWith = Pm),
          (u.cloneWith = Om),
          (u.conformsTo = Rm),
          (u.deburr = Au),
          (u.defaultTo = f0),
          (u.divide = I0),
          (u.endsWith = Ub),
          (u.eq = He),
          (u.escape = Nb),
          (u.escapeRegExp = Fb),
          (u.every = Vv),
          (u.find = Zv),
          (u.findIndex = jc),
          (u.findKey = cb),
          (u.findLast = jv),
          (u.findLastIndex = Jc),
          (u.findLastKey = ub),
          (u.floor = D0),
          (u.forEach = iu),
          (u.forEachRight = ou),
          (u.forIn = hb),
          (u.forInRight = db),
          (u.forOwn = fb),
          (u.forOwnRight = pb),
          (u.get = Xs),
          (u.gt = Mm),
          (u.gte = Lm),
          (u.has = mb),
          (u.hasIn = Zs),
          (u.head = tu),
          (u.identity = oe),
          (u.includes = nm),
          (u.indexOf = jg),
          (u.inRange = Ib),
          (u.invoke = _b),
          (u.isArguments = Kn),
          (u.isArray = lt),
          (u.isArrayBuffer = Im),
          (u.isArrayLike = re),
          (u.isArrayLikeObject = Rt),
          (u.isBoolean = Dm),
          (u.isBuffer = kn),
          (u.isDate = Bm),
          (u.isElement = Um),
          (u.isEmpty = Nm),
          (u.isEqual = Fm),
          (u.isEqualWith = Hm),
          (u.isError = Ks),
          (u.isFinite = Wm),
          (u.isFunction = hn),
          (u.isInteger = fu),
          (u.isLength = oo),
          (u.isMap = pu),
          (u.isMatch = qm),
          (u.isMatchWith = Ym),
          (u.isNaN = Gm),
          (u.isNative = Km),
          (u.isNil = Xm),
          (u.isNull = Vm),
          (u.isNumber = gu),
          (u.isObject = Ot),
          (u.isObjectLike = Pt),
          (u.isPlainObject = jr),
          (u.isRegExp = Vs),
          (u.isSafeInteger = Zm),
          (u.isSet = vu),
          (u.isString = so),
          (u.isSymbol = ve),
          (u.isTypedArray = gr),
          (u.isUndefined = jm),
          (u.isWeakMap = Jm),
          (u.isWeakSet = Qm),
          (u.join = nv),
          (u.kebabCase = Hb),
          (u.last = Oe),
          (u.lastIndexOf = rv),
          (u.lowerCase = Wb),
          (u.lowerFirst = qb),
          (u.lt = tb),
          (u.lte = eb),
          (u.max = B0),
          (u.maxBy = U0),
          (u.mean = N0),
          (u.meanBy = F0),
          (u.min = H0),
          (u.minBy = W0),
          (u.stubArray = na),
          (u.stubFalse = ra),
          (u.stubObject = k0),
          (u.stubString = T0),
          (u.stubTrue = O0),
          (u.multiply = q0),
          (u.nth = iv),
          (u.noConflict = _0),
          (u.noop = ea),
          (u.now = no),
          (u.pad = Yb),
          (u.padEnd = Gb),
          (u.padStart = Kb),
          (u.parseInt = Vb),
          (u.random = Db),
          (u.reduce = am),
          (u.reduceRight = lm),
          (u.repeat = Xb),
          (u.replace = Zb),
          (u.result = Eb),
          (u.round = Y0),
          (u.runInContext = b),
          (u.sample = um),
          (u.size = fm),
          (u.snakeCase = jb),
          (u.some = pm),
          (u.sortedIndex = hv),
          (u.sortedIndexBy = dv),
          (u.sortedIndexOf = fv),
          (u.sortedLastIndex = pv),
          (u.sortedLastIndexBy = gv),
          (u.sortedLastIndexOf = vv),
          (u.startCase = Qb),
          (u.startsWith = t0),
          (u.subtract = G0),
          (u.sum = K0),
          (u.sumBy = V0),
          (u.template = e0),
          (u.times = z0),
          (u.toFinite = dn),
          (u.toInteger = ct),
          (u.toLength = bu),
          (u.toLower = n0),
          (u.toNumber = ze),
          (u.toSafeInteger = nb),
          (u.toString = yt),
          (u.toUpper = r0),
          (u.trim = i0),
          (u.trimEnd = o0),
          (u.trimStart = s0),
          (u.truncate = a0),
          (u.unescape = l0),
          (u.uniqueId = R0),
          (u.upperCase = c0),
          (u.upperFirst = js),
          (u.each = iu),
          (u.eachRight = ou),
          (u.first = tu),
          ta(
            u,
            (function () {
              var t = {}
              return (
                Ve(u, function (e, o) {
                  _t.call(u.prototype, o) || (t[o] = e)
                }),
                t
              )
            })(),
            { chain: !1 },
          ),
          (u.VERSION = a),
          Ce(
            [
              'bind',
              'bindKey',
              'curry',
              'curryRight',
              'partial',
              'partialRight',
            ],
            function (t) {
              u[t].placeholder = u
            },
          ),
          Ce(['drop', 'take'], function (t, e) {
            ;(gt.prototype[t] = function (o) {
              o = o === r ? 1 : Nt(ct(o), 0)
              var s = this.__filtered__ && !e ? new gt(this) : this.clone()
              return (
                s.__filtered__
                  ? (s.__takeCount__ = jt(o, s.__takeCount__))
                  : s.__views__.push({
                      size: jt(o, dt),
                      type: t + (s.__dir__ < 0 ? 'Right' : ''),
                    }),
                s
              )
            }),
              (gt.prototype[t + 'Right'] = function (o) {
                return this.reverse()[t](o).reverse()
              })
          }),
          Ce(['filter', 'map', 'takeWhile'], function (t, e) {
            var o = e + 1,
              s = o == W || o == L
            gt.prototype[t] = function (c) {
              var d = this.clone()
              return (
                d.__iteratees__.push({
                  iteratee: Q(c, 3),
                  type: o,
                }),
                (d.__filtered__ = d.__filtered__ || s),
                d
              )
            }
          }),
          Ce(['head', 'last'], function (t, e) {
            var o = 'take' + (e ? 'Right' : '')
            gt.prototype[t] = function () {
              return this[o](1).value()[0]
            }
          }),
          Ce(['initial', 'tail'], function (t, e) {
            var o = 'drop' + (e ? '' : 'Right')
            gt.prototype[t] = function () {
              return this.__filtered__ ? new gt(this) : this[o](1)
            }
          }),
          (gt.prototype.compact = function () {
            return this.filter(oe)
          }),
          (gt.prototype.find = function (t) {
            return this.filter(t).head()
          }),
          (gt.prototype.findLast = function (t) {
            return this.reverse().find(t)
          }),
          (gt.prototype.invokeMap = ht(function (t, e) {
            return typeof t == 'function'
              ? new gt(this)
              : this.map(function (o) {
                  return Yr(o, t, e)
                })
          })),
          (gt.prototype.reject = function (t) {
            return this.filter(io(Q(t)))
          }),
          (gt.prototype.slice = function (t, e) {
            t = ct(t)
            var o = this
            return o.__filtered__ && (t > 0 || e < 0)
              ? new gt(o)
              : (t < 0 ? (o = o.takeRight(-t)) : t && (o = o.drop(t)),
                e !== r &&
                  ((e = ct(e)), (o = e < 0 ? o.dropRight(-e) : o.take(e - t))),
                o)
          }),
          (gt.prototype.takeRightWhile = function (t) {
            return this.reverse().takeWhile(t).reverse()
          }),
          (gt.prototype.toArray = function () {
            return this.take(dt)
          }),
          Ve(gt.prototype, function (t, e) {
            var o = /^(?:filter|find|map|reject)|While$/.test(e),
              s = /^(?:head|last)$/.test(e),
              c = u[s ? 'take' + (e == 'last' ? 'Right' : '') : e],
              d = s || /^find/.test(e)
            c &&
              (u.prototype[e] = function () {
                var p = this.__wrapped__,
                  g = s ? [1] : arguments,
                  y = p instanceof gt,
                  A = g[0],
                  E = y || lt(p),
                  z = function (ft) {
                    var vt = c.apply(u, wn([ft], g))
                    return s && F ? vt[0] : vt
                  }
                E &&
                  o &&
                  typeof A == 'function' &&
                  A.length != 1 &&
                  (y = E = !1)
                var F = this.__chain__,
                  Z = !!this.__actions__.length,
                  tt = d && !F,
                  ut = y && !Z
                if (!d && E) {
                  p = ut ? p : new gt(this)
                  var et = t.apply(p, g)
                  return (
                    et.__actions__.push({ func: to, args: [z], thisArg: r }),
                    new Ee(et, F)
                  )
                }
                return tt && ut
                  ? t.apply(this, g)
                  : ((et = this.thru(z)),
                    tt ? (s ? et.value()[0] : et.value()) : et)
              })
          }),
          Ce(
            ['pop', 'push', 'shift', 'sort', 'splice', 'unshift'],
            function (t) {
              var e = Ai[t],
                o = /^(?:push|sort|unshift)$/.test(t) ? 'tap' : 'thru',
                s = /^(?:pop|shift)$/.test(t)
              u.prototype[t] = function () {
                var c = arguments
                if (s && !this.__chain__) {
                  var d = this.value()
                  return e.apply(lt(d) ? d : [], c)
                }
                return this[o](function (p) {
                  return e.apply(lt(p) ? p : [], c)
                })
              }
            },
          ),
          Ve(gt.prototype, function (t, e) {
            var o = u[e]
            if (o) {
              var s = o.name + ''
              _t.call(ur, s) || (ur[s] = []), ur[s].push({ name: e, func: o })
            }
          }),
          (ur[Ki(r, P).name] = [
            {
              name: 'wrapper',
              func: r,
            },
          ]),
          (gt.prototype.clone = lp),
          (gt.prototype.reverse = cp),
          (gt.prototype.value = up),
          (u.prototype.at = Uv),
          (u.prototype.chain = Nv),
          (u.prototype.commit = Fv),
          (u.prototype.next = Hv),
          (u.prototype.plant = qv),
          (u.prototype.reverse = Yv),
          (u.prototype.toJSON = u.prototype.valueOf = u.prototype.value = Gv),
          (u.prototype.first = u.prototype.head),
          Br && (u.prototype[Br] = Wv),
          u
        )
      },
      ar = Ff()
    Un ? (((Un.exports = ar)._ = ar), (Qo._ = ar)) : (Vt._ = ar)
  }).call(se)
})(Ao, Ao.exports)
var zr = Ao.exports
const pn = qt({
  Ellipsis: 'ellipsis',
  Short: 'short',
  None: 'none',
})
class Sa extends $e {
  constructor() {
    super()
    Y(this, '_catalog')
    Y(this, '_schema', '')
    Y(this, '_model', '')
    Y(this, '_widthCatalog', 0)
    Y(this, '_widthSchema', 0)
    Y(this, '_widthModel', 0)
    Y(this, '_widthOriginal', 0)
    Y(this, '_widthAdditional', 0)
    Y(this, '_widthIconEllipsis', 26)
    Y(this, '_widthIcon', 0)
    Y(
      this,
      '_toggleNamePartsDebounced',
      zr.debounce(r => {
        const l =
          (this._hideCatalog
            ? 0
            : this._collapseCatalog
            ? this._widthIconEllipsis
            : this._widthCatalog) +
          this._widthSchema +
          this._widthModel +
          this._widthAdditional
        ;(this._hasCollapsedParts = r < this._widthOriginal),
          this._hasCollapsedParts
            ? (this.mode === pn.None
                ? (this._hideCatalog = !0)
                : (this._collapseCatalog = !0),
              r < l
                ? this.mode === pn.None
                  ? (this._hideSchema = !0)
                  : (this._collapseSchema = !0)
                : this.mode === pn.None
                ? (this._hideSchema = !1)
                : (this._collapseSchema = this.collapseSchema))
            : this.mode === pn.None
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
      (this.mode = pn.None)
  }
  async firstUpdated() {
    await super.firstUpdated()
    const r = 28
    ;(this._widthIcon = wt(this.hideIcon) ? 24 : 0),
      (this._widthAdditional = this._widthIcon + r),
      setTimeout(() => {
        const a = this.shadowRoot.querySelector('[part="hidden"]'),
          [l, h, f] = Array.from(a.children)
        ;(this._widthCatalog = l.clientWidth),
          (this._widthSchema = h.clientWidth),
          (this._widthModel = f.clientWidth),
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
  willUpdate(r) {
    r.has('text')
      ? this._setNameParts()
      : r.has('collapse-catalog')
      ? (this._collapseCatalog = this.collapseCatalog)
      : r.has('collapse-schema')
      ? (this._collapseSchema = this.collapseSchema)
      : r.has('mode')
      ? this.mode === pn.None &&
        ((this._hideCatalog = !0), (this._hideSchema = !0))
      : r.has('hide-catalog')
      ? (this._hideCatalog = this.mode === pn.None ? !0 : this.hideCatalog)
      : r.has('hide-schema') &&
        (this._hideSchema = this.mode === pn.None ? !0 : this.hideSchema)
  }
  async resize() {
    this.hideCatalog && this.hideSchema
      ? ((this._hideCatalog = !0),
        (this._hideSchema = !0),
        (this._collapseCatalog = !0),
        (this._collapseSchema = !0),
        (this._hasCollapsedParts = !0))
      : this._toggleNamePartsDebounced(this.clientWidth)
  }
  _setNameParts() {
    const r = this.text.split('.')
    ;(this._model = r.pop()),
      (this._schema = r.pop()),
      (this._catalog = r.pop()),
      Kt(this._catalog) && (this.hideCatalog = !0),
      ae(
        this._model,
        'Model Name does not satisfy the pattern: catalog.schema.model or schema.model',
      ),
      ae(
        this._schema,
        'Model Name does not satisfy the pattern: catalog.schema.model or schema.model',
      )
  }
  _renderCatalog() {
    return zt(
      wt(this._hideCatalog) && wt(this.hideCatalog),
      X`
        <span part="catalog">
          ${
            this._collapseCatalog
              ? this._renderIconEllipsis(this.shortCatalog)
              : X`<span>${this._catalog}</span>`
          }
          .
        </span>
      `,
    )
  }
  _renderSchema() {
    return zt(
      wt(this._hideSchema) && wt(this.hideSchema),
      X`
        <span part="schema">
          ${
            this._collapseSchema
              ? this._renderIconEllipsis(this.shortSchema)
              : X`<span>${this._schema}</span>`
          }
          .
        </span>
      `,
    )
  }
  _renderModel() {
    return X`
      <span
        title="${this._model}"
        part="model"
      >
        <span>${this._model}</span>
      </span>
    `
  }
  _renderIconEllipsis(r = '') {
    return this.mode === pn.Ellipsis
      ? X`
          <tbk-icon
            part="ellipsis"
            library="heroicons-micro"
            name="ellipsis-horizontal"
          ></tbk-icon>
        `
      : X`<small part="ellipsis">${r}</small>`
  }
  _renderIconModel() {
    if (this.hideIcon) return ''
    const r = X`
      <tbk-icon
        part="icon"
        library="heroicons"
        name="cube"
      ></tbk-icon>
    `
    return this.hideTooltip
      ? X`<span title="${this.text}">${r}</span>`
      : this._hasCollapsedParts
      ? X`
          <tbk-tooltip
            content="${this.text}"
            placement="right"
            distance="0"
          >
            ${r}
          </tbk-tooltip>
        `
      : r
  }
  render() {
    return X`
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
  static categorize(r = []) {
    return r.reduce((a, l) => {
      ae(In(l.name), 'Model name must be present')
      const h = l.name.split('.')
      h.pop(), ae(Wy, hi(h))
      const f = h.join('.')
      return a[f] || (a[f] = []), a[f].push(l), a
    }, {})
  }
}
Y(Sa, 'styles', [Dt(), Me(), pt(dx)]),
  Y(Sa, 'properties', {
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
customElements.define('tbk-model-name', Sa)
var fx = en`
  :host {
    display: contents;
  }
`,
  Cr = class extends xe {
    constructor() {
      super(...arguments), (this.observedElements = []), (this.disabled = !1)
    }
    connectedCallback() {
      super.connectedCallback(),
        (this.resizeObserver = new ResizeObserver(n => {
          this.emit('sl-resize', { detail: { entries: n } })
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
      const n = this.shadowRoot.querySelector('slot')
      if (n !== null) {
        const i = n.assignedElements({ flatten: !0 })
        this.observedElements.forEach(r => this.resizeObserver.unobserve(r)),
          (this.observedElements = []),
          i.forEach(r => {
            this.resizeObserver.observe(r), this.observedElements.push(r)
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
      return X` <slot @slotchange=${this.handleSlotChange}></slot> `
    }
  }
Cr.styles = [bn, fx]
R([V({ type: Boolean, reflect: !0 })], Cr.prototype, 'disabled', 2)
R(
  [le('disabled', { waitUntilFirstUpdate: !0 })],
  Cr.prototype,
  'handleDisabledChange',
  1,
)
var ho
let px =
  ((ho = class extends mn(Cr) {
    constructor() {
      super()
      Y(this, '_items', [])
      Y(
        this,
        '_updateItems',
        zr.debounce(() => {
          this._items = Array.from(this.querySelectorAll(this.updateSelector))
        }, 500),
      )
      this.updateSelectors = []
    }
    firstUpdated() {
      super.firstUpdated(),
        this.addEventListener('sl-resize', this._handleResize.bind(this))
    }
    _handleResize(r) {
      r.stopPropagation()
      const a = r.detail.value.entries[0]
      this._updateItems(),
        this._items.forEach(l => {
          var h
          return (h = l.resize) == null ? void 0 : h.call(l, a)
        }),
        this.emit('resize', {
          detail: new this.emit.EventDetail(void 0, r),
        })
    }
  }),
  Y(ho, 'styles', [Dt()]),
  Y(ho, 'properties', {
    ...Cr.properties,
    updateSelector: { type: String, reflect: !0, attribute: 'update-selector' },
  }),
  ho)
customElements.define('tbk-resize-observer', px)
const gx = `:host {
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
  gap: var(--step);
  padding: 0 calc(var(--source-list-font-size) * 0.5) var(--step-2);
}
`,
  vx = `:host {
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
class cd extends $e {
  render() {
    return X`<slot></slot>`
  }
}
Y(cd, 'styles', [Dt(), Fw(), pt(vx)])
customElements.define('tbk-scroll', cd)
const mx = `:host {
  --source-list-item-gap: var(--step-2);
  --source-list-item-border-radius: var(--source-list-item-radius);
  --source-list-item-background: transparent;
  --source-list-item-background-active: var(--source-list-item-variant-5, var(--color-pacific-10));
  --source-list-item-background-hover: var(--source-list-item-variant-5, var(--color-pacific-5));
  --source-list-item-color: var(--color-gray-800);
  --source-list-item-color-hover: var(--color-pacific-600);
  --source-list-item-color-active: var(--source-list-item-variant, var(--color-pacific-600));

  display: inline-flex;
  flex-direction: column;
  border-radius: var(--source-list-item-border-radius);
  overflow: hidden;
  cursor: pointer;
  width: 100%;
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
  margin-top: var(--step);
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
  overflow: hidden;
  gap: var(--step-3);
  font-weight: inherit;
  color: var(--source-list-item-color);
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
  padding: var(--step) var(--step-2);
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
`,
  vo = qt({
    Select: 'select-source-list-item',
    Open: 'open-source-list-item',
  })
class Ca extends $e {
  constructor() {
    super()
    Y(this, '_items', [])
    ;(this.size = Lt.S),
      (this.shape = je.Round),
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
      this.addEventListener('mousedown', this._handleMouseDown.bind(this)),
      this.addEventListener('keydown', this._handleKeyDown.bind(this)),
      this.addEventListener(vo.Select, this._handleSelect.bind(this))
  }
  willUpdate(r) {
    super.willUpdate(r), r.has('short') && this.toggle(!1)
  }
  toggle(r) {
    this.open = Kt(r) ? !this.open : r
  }
  setActive(r) {
    this.active = Kt(r) ? !this.active : r
  }
  _handleMouseDown(r) {
    r.preventDefault(),
      r.stopPropagation(),
      this._hasItems
        ? (this.toggle(),
          this.emit(vo.Open, {
            detail: new this.emit.EventDetail(this.value, r, {
              id: this.id,
              open: this.open,
              active: this.active,
              name: this.name,
              value: this.value,
            }),
          }))
        : this.selectable &&
          this.emit(vo.Select, {
            detail: new this.emit.EventDetail(this.value, r, {
              id: this.id,
              open: this.open,
              active: this.active,
              name: this.name,
              value: this.value,
            }),
          })
  }
  _handleKeyDown(r) {
    ;(r.key === 'Enter' || r.key === ' ') && this._handleMouseDown(r)
  }
  _handleSelect(r) {
    r.target !== this &&
      requestAnimationFrame(() => {
        this.active = this._items.some(a => a.active) || this.active
      })
  }
  _handleSlotChange(r) {
    r.stopPropagation(),
      (this._items = []
        .concat(
          Array.from(
            this.renderRoot.querySelectorAll('slot[name="items"]'),
          ).map(a => a.assignedElements({ flatten: !0 })),
        )
        .flat()),
      (this._hasItems = hi(this._items))
  }
  render() {
    return (
      console.log(this.hideItemsCounter),
      X`
      <div part="base">
        <span part="header">
          ${zt(
            this.hasActiveIcon && wt(this.hideActiveIcon),
            X`
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
            ${zt(
              wt(this.hideIcon),
              X`
                <slot name="icon">
                  <tbk-icon
                    library="heroicons"
                    name="${this.icon}"
                  ></tbk-icon>
                </slot>
              `,
            )}
            ${zt(
              wt(this.short),
              X`
                <span part="text">
                  <slot></slot>
                </span>
              `,
            )}
          </span>
          <span part="badge">
            <slot name="badge"></slot>
          </span>
          ${zt(
            this._hasItems,
            X`
              <span part="toggle">
                ${zt(
                  wt(this.hideItemsCounter),
                  X` <tbk-badge .size="${Lt.XS}">${this._items.length}</tbk-badge> `,
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
          @slotchange="${zr.debounce(this._handleSlotChange, 200)}"
        ></slot>
      </div>
    `
    )
  }
}
Y(Ca, 'styles', [
  Dt(),
  Ro('source-list-item'),
  Po('source-list-item'),
  Me('source-list-item'),
  Dn('source-list-item'),
  pt(mx),
]),
  Y(Ca, 'properties', {
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
customElements.define('tbk-source-list-item', Ca)
class Aa extends $e {
  constructor() {
    super()
    Y(this, '_sections', [])
    Y(this, '_items', [])
    ;(this.short = !1),
      (this.selectable = !1),
      (this.allowUnselect = !1),
      (this.hasActiveIcon = !1)
  }
  connectedCallback() {
    super.connectedCallback(),
      this.addEventListener(vo.Select, r => {
        r.stopPropagation(),
          this._items.forEach(a => {
            a !== r.target
              ? a.setActive(!1)
              : this.allowUnselect
              ? a.setActive()
              : a.setActive(!0)
          }),
          this.emit('change', { detail: r.detail })
      })
  }
  willUpdate(r) {
    super.willUpdate(r),
      (r.has('short') || r.has('size')) && this._toggleChildren()
  }
  toggle(r) {
    this.short = Kt(r) ? !this.short : r
  }
  _toggleChildren() {
    this._sections.forEach(r => {
      r.short = this.short
    }),
      this._items.forEach(r => {
        ;(r.short = this.short),
          (r.size = this.size ?? r.size),
          this.selectable &&
            ((r.hasActiveIcon = this.hasActiveIcon ? wt(r.hideActiveIcon) : !1),
            (r.selectable = Kt(r.selectable) ? this.selectable : r.selectable))
      })
  }
  _handleSlotChange(r) {
    r.stopPropagation(),
      (this._sections = Array.from(
        this.querySelectorAll('tbk-source-list-section'),
      )),
      (this._items = Array.from(this.querySelectorAll('tbk-source-list-item'))),
      this._toggleChildren(),
      hi(this._sections) && (this._sections[0].open = !0)
  }
  render() {
    return X`
      <tbk-scroll part="content">
        <slot @slotchange="${zr.debounce(
          this._handleSlotChange.bind(this),
          200,
        )}"></slot>
      </tbk-scroll>
    `
  }
}
Y(Aa, 'styles', [Dt(), Hh(), Me('source-list'), Dn('source-list'), pt(gx)]),
  Y(Aa, 'properties', {
    short: { type: Boolean, reflect: !0 },
    size: { type: String, reflect: !0 },
    selectable: { type: Boolean, reflect: !0 },
    allowUnselect: { type: Boolean, reflect: !0, attribute: 'allow-unselect' },
    hasActiveIcon: { type: Boolean, reflect: !0, attribute: 'has-active-icon' },
  })
customElements.define('tbk-source-list', Aa)
const bx = `:host {
  display: block;
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
  color: var(--color-gray-500);
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
  background: var(--color-gray-5);
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
  yx = `:host {
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
  da = qt({
    Button: 'button',
    Submit: 'submit',
    Reset: 'reset',
  }),
  _x = qt({
    Primary: 'primary',
    Secondary: 'secondary',
    Alternative: 'alternative',
    Destructive: 'destructive',
    Danger: 'danger',
    Transparent: 'transparent',
  })
class mo extends $e {
  constructor() {
    super(),
      (this.type = da.Button),
      (this.size = Lt.M),
      (this.side = Je.Left),
      (this.variant = xt.Primary),
      (this.shape = je.Round),
      (this.horizontal = Hu.Auto),
      (this.vertical = Vy.Auto),
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
    var i
    return (i = this.renderRoot) == null
      ? void 0
      : i.querySelector(yr.PartContent)
  }
  get elTagline() {
    var i
    return (i = this.renderRoot) == null
      ? void 0
      : i.querySelector(yr.PartTagline)
  }
  get elBefore() {
    var i
    return (i = this.renderRoot) == null
      ? void 0
      : i.querySelector(yr.PartBefore)
  }
  get elAfter() {
    var i
    return (i = this.renderRoot) == null
      ? void 0
      : i.querySelector(yr.PartAfter)
  }
  connectedCallback() {
    super.connectedCallback(),
      this.addEventListener(mr.Click, this._onClick.bind(this)),
      this.addEventListener(mr.Keydown, this._onKeyDown.bind(this)),
      this.addEventListener(mr.Keyup, this._onKeyUp.bind(this))
  }
  disconnectedCallback() {
    super.disconnectedCallback(),
      this.removeEventListener(mr.Click, this._onClick),
      this.removeEventListener(mr.Keydown, this._onKeyDown),
      this.removeEventListener(mr.Keyup, this._onKeyUp)
  }
  firstUpdated() {
    super.firstUpdated(), this.autofocus && this.setFocus()
  }
  willUpdate(i) {
    return (
      i.has('link') &&
        (this.horizontal = this.link ? Hu.Compact : this.horizontal),
      super.willUpdate(i)
    )
  }
  click() {
    const i = this.getForm()
    ko(i) &&
      [da.Submit, da.Reset].includes(this.type) &&
      i.reportValidity() &&
      this.handleFormSubmit(i)
  }
  getForm() {
    return this.internals.form
  }
  _onClick(i) {
    var r
    if (this.readonly) {
      i.preventDefault(), i.stopPropagation(), i.stopImmediatePropagation()
      return
    }
    if (this.link)
      return (
        i.stopPropagation(),
        i.stopImmediatePropagation(),
        (r = this.querySelector('a')) == null ? void 0 : r.click()
      )
    if ((i.preventDefault(), this.disabled)) {
      i.stopPropagation(), i.stopImmediatePropagation()
      return
    }
    this.click()
  }
  _onKeyDown(i) {
    ;[lo.Enter, lo.Space].includes(i.code) &&
      (i.preventDefault(), i.stopPropagation(), this.classList.add(Fu.Active))
  }
  _onKeyUp(i) {
    var r
    ;[lo.Enter, lo.Space].includes(i.code) &&
      (i.preventDefault(),
      i.stopPropagation(),
      this.classList.remove(Fu.Active),
      (r = this.elBase) == null || r.click())
  }
  handleFormSubmit(i) {
    if (Kt(i)) return
    const r = document.createElement('input')
    ;(r.type = this.type),
      (r.style.position = 'absolute'),
      (r.style.width = '0'),
      (r.style.height = '0'),
      (r.style.clipPath = 'inset(50%)'),
      (r.style.overflow = 'hidden'),
      (r.style.whiteSpace = 'nowrap'),
      [
        'name',
        'value',
        'formaction',
        'formenctype',
        'formmethod',
        'formnovalidate',
        'formtarget',
      ].forEach(a => {
        wr(this[a]) && r.setAttribute(a, this[a])
      }),
      i.append(r),
      r.click(),
      r.remove()
  }
  setOverlayText(i = '') {
    this._overlayText = i
  }
  showOverlay(i = 0) {
    setTimeout(() => {
      this.overlay = !0
    }, i)
  }
  hideOverlay(i = 0) {
    setTimeout(() => {
      ;(this.overlay = !1), (this._overlayText = '')
    }, i)
  }
  setFocus(i = 200) {
    setTimeout(() => {
      this.focus()
    }, i)
  }
  setBlur(i = 200) {
    setTimeout(() => {
      this.blur()
    }, i)
  }
  render() {
    return X`
      ${zt(
        this.overlay && this._overlayText,
        X`<span part="overlay">${this._overlayText}</span>`,
      )}
      <div part="base">
        <slot name="before"></slot>
        <div part="content">
          <slot tabindex="-1"></slot>
          <slot name="tagline">${this.tagline}</slot>
        </div>
        <slot name="after">
          ${zt(
            this.link,
            X`<tbk-icon
              library="heroicons"
              name="arrow-up-right"
            ></tbk-icon>`,
          )}
        </slot>
      </div>
    `
  }
}
Y(mo, 'formAssociated', !0),
  Y(mo, 'styles', [
    Dt(),
    Hh(),
    Me(),
    Hw('button'),
    Yw('button'),
    Ro('button'),
    Xw('button'),
    Dn('button', 1.25, 2),
    Gw(),
    pt(yx),
  ]),
  Y(mo, 'properties', {
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
customElements.define('tbk-button', mo)
class Ea extends $e {
  constructor() {
    super()
    Y(this, '_open', !1)
    Y(this, '_cache', /* @__PURE__ */ new WeakMap())
    ;(this._childrenCount = 0),
      (this._showMore = !1),
      (this.open = !1),
      (this.inert = !1),
      (this.short = !1),
      (this.limit = 1 / 0)
  }
  willUpdate(r) {
    super.willUpdate(r),
      r.has('short') &&
        (this.short
          ? ((this._open = this.open), (this.open = !0))
          : (this.open = this._open))
  }
  toggle(r) {
    this.open = Kt(r) ? !this.open : r
  }
  _handleClick(r) {
    r.preventDefault(), r.stopPropagation(), this.toggle()
  }
  _toggleChildren() {
    this.elsSlotted.forEach((r, a) => {
      wt(this._cache.has(r)) && this._cache.set(r, r.style.display),
        this._showMore || a < this.limit
          ? (r.style.display = this._cache.get(r, r.style.display))
          : (r.style.display = 'none')
    })
  }
  _renderShowMore() {
    return this.short
      ? X`
          <div part="actions">
            <tbk-icon
              library="heroicons-micro"
              name="ellipsis-horizontal"
            ></tbk-icon>
          </div>
        `
      : X`
          <div part="actions">
            <tbk-button
              shape="pill"
              size="2xs"
              variant="secondary"
              @click="${() => {
                ;(this._showMore = !this._showMore), this._toggleChildren()
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
  _handleSlotChange(r) {
    r.stopPropagation(),
      (this._childrenCount = this.elsSlotted.length),
      this._toggleChildren()
  }
  render() {
    return X`
      <div part="base">
        ${zt(
          this.headline && wt(this.short),
          X`
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
          <slot @slotchange="${zr.debounce(
            this._handleSlotChange,
            200,
          )}"></slot>
          ${zt(this._childrenCount > this.limit, this._renderShowMore())}
        </div>
      </div>
    `
  }
}
Y(Ea, 'styles', [Dt(), Me('source-list-item'), Dn('source-list-item'), pt(bx)]),
  Y(Ea, 'properties', {
    headline: { type: String },
    open: { type: Boolean, reflect: !0 },
    inert: { type: Boolean, reflect: !0 },
    short: { type: Boolean, reflect: !0 },
    limit: { type: Number },
    _showMore: { type: String, state: !0 },
    _childrenCount: { type: Number, state: !0 },
  })
customElements.define('tbk-source-list-section', Ea)
const wx = `:host {
  --metadata-font-size: var(--font-size);

  height: 100%;
  width: 100%;
}

[part='base'] {
  display: flex;
  flex-direction: column;
  gap: var(--step-2);
  font-size: var(--metadata-font-size);
}
`
class ka extends $e {
  constructor() {
    super(), (this.size = Lt.S)
  }
  render() {
    return X`
      <tbk-scroll>
        <div part="base">
          <slot></slot>
        </div>
      </tbk-scroll>
    `
  }
}
Y(ka, 'styles', [Dt(), Me(), pt(wx)]),
  Y(ka, 'properties', {
    size: { type: String, reflect: !0 },
  })
customElements.define('tbk-metadata', ka)
const $x = `:host {
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
}
:host(:hover) {
  background-color: var(--metadata-item-background);
}
[part='base'] {
  width: 100%;
  display: flex;
  align-items: baseline;
  justify-content: inherit;
  overflow: hidden;
  gap: var(--step-4);
}
[part='key'] {
  display: block;
  color: var(--metadata-item-color-key);
  font-family: var(--font-mono);
  font-size: inherit;
}
[part='value'] {
  color: var(--metadata-item-color-value);
  font-family: var(--font-sans);
  font-weight: var(--text-bold);
  font-size: inherit;
}
a[part='value'] {
  color: var(--color-link);
  text-decoration: none;
}
a[part='value']:hover {
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
class Ta extends $e {
  constructor() {
    super(), (this.size = Lt.M)
  }
  _renderValue() {
    return this.href
      ? X`<a
          href="${this.href}"
          part="value"
          >${this.value}</a
        >`
      : X`<span part="value">${this.value}</span>`
  }
  render() {
    return X`
      ${zt(
        this.key || this.value,
        X`
          <div part="base">
            ${zt(this.key, X`<span part="key">${this.key}</span>`)}
            ${zt(this.value, this._renderValue())}
          </div>
        `,
      )}
      ${zt(
        this.description,
        X`<span part="description">${this.description}</span>`,
      )}
      <slot></slot>
    `
  }
}
Y(Ta, 'styles', [Dt(), Me(), pt($x)]),
  Y(Ta, 'properties', {
    size: { type: String, reflect: !0 },
    key: { type: String },
    value: { type: String },
    href: { type: String },
    description: { type: String },
  })
customElements.define('tbk-metadata-item', Ta)
const xx = `:host {
  font-family: var(--font-sans);
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
  border-bottom: 1px solid var(--color-divider);
  margin-bottom: var(--step);
  align-items: baseline;
  justify-content: space-between;
}
::slotted(:last-child) {
  border-bottom: none;
}
`
class Oa extends $e {
  constructor() {
    super()
    Y(this, '_cache', /* @__PURE__ */ new WeakMap())
    ;(this._children = []),
      (this._showMore = !1),
      (this.orientation = Gy.Vertical),
      (this.limit = 1 / 0),
      (this.hideActions = !1)
  }
  _handleSlotChange(r) {
    r.stopPropagation(),
      (this._children = this.elsSlotted),
      this._toggleChildren()
  }
  _toggleChildren() {
    this._children.forEach((r, a) => {
      wt(this._cache.has(r)) && this._cache.set(r, r.style.display),
        this._showMore || a < this.limit
          ? (r.style.display = this._cache.get(r, r.style.display))
          : (r.style.display = 'none')
    })
  }
  _renderShowMore() {
    return X`
      <div part="actions">
        <tbk-button
          shape="${je.Pill}"
          size="${Lt.XXS}"
          variant="${_x.Secondary}"
          @click="${() => {
            ;(this._showMore = !this._showMore), this._toggleChildren()
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
    return X`
      <div part="base">
        ${zt(this.label, X`<p part="label">${this.label}</p>`)}
        <div part="content">
          <slot @slotchange="${zr.debounce(
            this._handleSlotChange,
            200,
          )}"></slot>
          ${zt(
            this._children.length > this.limit && wt(this.hideActions),
            this._renderShowMore(),
          )}
        </div>
      </div>
    `
  }
}
Y(Oa, 'styles', [Dt(), pt(xx)]),
  Y(Oa, 'properties', {
    orientation: { type: String, reflect: !0 },
    label: { type: String },
    limit: { type: Number },
    hideActions: { type: Boolean, reflect: !0, attribute: 'hide-actions' },
    _showMore: { type: String, state: !0 },
    _children: { type: Array, state: !0 },
  })
customElements.define('tbk-metadata-section', Oa)
var Sx = en`
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
function Cx(n, i) {
  function r(l) {
    const h = n.getBoundingClientRect(),
      f = n.ownerDocument.defaultView,
      v = h.left + f.scrollX,
      m = h.top + f.scrollY,
      w = l.pageX - v,
      k = l.pageY - m
    i != null && i.onMove && i.onMove(w, k)
  }
  function a() {
    document.removeEventListener('pointermove', r),
      document.removeEventListener('pointerup', a),
      i != null && i.onStop && i.onStop()
  }
  document.addEventListener('pointermove', r, { passive: !0 }),
    document.addEventListener('pointerup', a),
    (i == null ? void 0 : i.initialEvent) instanceof PointerEvent &&
      r(i.initialEvent)
}
function gh(n, i, r) {
  const a = l => (Object.is(l, -0) ? 0 : l)
  return n < i ? a(i) : n > r ? a(r) : a(n)
}
/**
 * @license
 * Copyright 2018 Google LLC
 * SPDX-License-Identifier: BSD-3-Clause
 */
const qe = n => n ?? Ft
var ce = class extends xe {
  constructor() {
    super(...arguments),
      (this.localize = new vi(this)),
      (this.position = 50),
      (this.vertical = !1),
      (this.disabled = !1),
      (this.snapThreshold = 12)
  }
  connectedCallback() {
    super.connectedCallback(),
      (this.resizeObserver = new ResizeObserver(n => this.handleResize(n))),
      this.updateComplete.then(() => this.resizeObserver.observe(this)),
      this.detectSize(),
      (this.cachedPositionInPixels = this.percentageToPixels(this.position))
  }
  disconnectedCallback() {
    var n
    super.disconnectedCallback(),
      (n = this.resizeObserver) == null || n.unobserve(this)
  }
  detectSize() {
    const { width: n, height: i } = this.getBoundingClientRect()
    this.size = this.vertical ? i : n
  }
  percentageToPixels(n) {
    return this.size * (n / 100)
  }
  pixelsToPercentage(n) {
    return (n / this.size) * 100
  }
  handleDrag(n) {
    const i = this.localize.dir() === 'rtl'
    this.disabled ||
      (n.cancelable && n.preventDefault(),
      Cx(this, {
        onMove: (r, a) => {
          let l = this.vertical ? a : r
          this.primary === 'end' && (l = this.size - l),
            this.snap &&
              this.snap.split(' ').forEach(f => {
                let v
                f.endsWith('%')
                  ? (v = this.size * (parseFloat(f) / 100))
                  : (v = parseFloat(f)),
                  i && !this.vertical && (v = this.size - v),
                  l >= v - this.snapThreshold &&
                    l <= v + this.snapThreshold &&
                    (l = v)
              }),
            (this.position = gh(this.pixelsToPercentage(l), 0, 100))
        },
        initialEvent: n,
      }))
  }
  handleKeyDown(n) {
    if (
      !this.disabled &&
      [
        'ArrowLeft',
        'ArrowRight',
        'ArrowUp',
        'ArrowDown',
        'Home',
        'End',
      ].includes(n.key)
    ) {
      let i = this.position
      const r = (n.shiftKey ? 10 : 1) * (this.primary === 'end' ? -1 : 1)
      n.preventDefault(),
        ((n.key === 'ArrowLeft' && !this.vertical) ||
          (n.key === 'ArrowUp' && this.vertical)) &&
          (i -= r),
        ((n.key === 'ArrowRight' && !this.vertical) ||
          (n.key === 'ArrowDown' && this.vertical)) &&
          (i += r),
        n.key === 'Home' && (i = this.primary === 'end' ? 100 : 0),
        n.key === 'End' && (i = this.primary === 'end' ? 0 : 100),
        (this.position = gh(i, 0, 100))
    }
  }
  handleResize(n) {
    const { width: i, height: r } = n[0].contentRect
    ;(this.size = this.vertical ? r : i),
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
    const n = this.vertical ? 'gridTemplateRows' : 'gridTemplateColumns',
      i = this.vertical ? 'gridTemplateColumns' : 'gridTemplateRows',
      r = this.localize.dir() === 'rtl',
      a = `
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
      l = 'auto'
    return (
      this.primary === 'end'
        ? r && !this.vertical
          ? (this.style[n] = `${a} var(--divider-width) ${l}`)
          : (this.style[n] = `${l} var(--divider-width) ${a}`)
        : r && !this.vertical
        ? (this.style[n] = `${l} var(--divider-width) ${a}`)
        : (this.style[n] = `${a} var(--divider-width) ${l}`),
      (this.style[i] = ''),
      X`
      <slot name="start" part="panel start" class="start"></slot>

      <div
        part="divider"
        class="divider"
        tabindex=${qe(this.disabled ? void 0 : '0')}
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
ce.styles = [bn, Sx]
R([Le('.divider')], ce.prototype, 'divider', 2)
R([V({ type: Number, reflect: !0 })], ce.prototype, 'position', 2)
R(
  [V({ attribute: 'position-in-pixels', type: Number })],
  ce.prototype,
  'positionInPixels',
  2,
)
R([V({ type: Boolean, reflect: !0 })], ce.prototype, 'vertical', 2)
R([V({ type: Boolean, reflect: !0 })], ce.prototype, 'disabled', 2)
R([V()], ce.prototype, 'primary', 2)
R([V()], ce.prototype, 'snap', 2)
R(
  [V({ type: Number, attribute: 'snap-threshold' })],
  ce.prototype,
  'snapThreshold',
  2,
)
R([le('position')], ce.prototype, 'handlePositionChange', 1)
R([le('positionInPixels')], ce.prototype, 'handlePositionInPixelsChange', 1)
R([le('vertical')], ce.prototype, 'handleVerticalChange', 1)
var fa = ce
ce.define('sl-split-panel')
const Ax = `:host {
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
class za extends mn(fa, fi) {}
Y(za, 'styles', [Dt(), fa.styles, pt(Ax)]),
  Y(za, 'properties', {
    ...fa.properties,
  })
customElements.define('tbk-split-pane', za)
const Ex = `summary {
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--step) var(--step-3);
  background: var(--color-gray-3);
  color: var(--color-gray-700);
  font-size: var(--text-xs);
}
summary::marker {
  content: none;
}
:host([outline]) summary {
  border: 1px solid var(--color-gray-200);
}
:host([ghost]) summary {
  background: transparent;
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
  padding: var(--step-2) 0;
  color: var(--color-gray-600);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
`,
  kx = [Dt(), pt(Ex)]
class Pa extends $e {
  constructor() {
    super(),
      (this.summary = 'Details'),
      (this.outline = !1),
      (this.ghost = !1),
      (this.open = !1)
  }
  connectedCallback() {
    super.connectedCallback(),
      ae(In(this.summary), '"summary" must be a string')
  }
  render() {
    return X`
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
Y(Pa, 'styles', kx),
  Y(Pa, 'properties', {
    summary: { type: String },
    outline: { type: Boolean, reflect: !0 },
    ghost: { type: Boolean, reflect: !0 },
    open: { type: Boolean, reflect: !0 },
  })
customElements.define('tbk-details', Pa)
var Tx = en`
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
  Ox = en`
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
const ud = Symbol.for(''),
  zx = n => {
    if ((n == null ? void 0 : n.r) === ud)
      return n == null ? void 0 : n._$litStatic$
  },
  vh = (n, ...i) => ({
    _$litStatic$: i.reduce(
      (r, a, l) =>
        r +
        (h => {
          if (h._$litStatic$ !== void 0) return h._$litStatic$
          throw Error(`Value passed to 'literal' function must be a 'literal' result: ${h}. Use 'unsafeStatic' to pass non-literal values, but
            take care to ensure page security.`)
        })(a) +
        n[l + 1],
      n[0],
    ),
    r: ud,
  }),
  mh = /* @__PURE__ */ new Map(),
  Px =
    n =>
    (i, ...r) => {
      const a = r.length
      let l, h
      const f = [],
        v = []
      let m,
        w = 0,
        k = !1
      for (; w < a; ) {
        for (m = i[w]; w < a && ((h = r[w]), (l = zx(h)) !== void 0); )
          (m += l + i[++w]), (k = !0)
        w !== a && v.push(h), f.push(m), w++
      }
      if ((w === a && f.push(i[a]), k)) {
        const C = f.join('$$lit$$')
        ;(i = mh.get(C)) === void 0 && ((f.raw = f), mh.set(C, (i = f))),
          (r = v)
      }
      return n(i, ...r)
    },
  Rx = Px(X)
var ue = class extends xe {
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
  handleClick(n) {
    this.disabled && (n.preventDefault(), n.stopPropagation())
  }
  /** Simulates a click on the icon button. */
  click() {
    this.button.click()
  }
  /** Sets focus on the icon button. */
  focus(n) {
    this.button.focus(n)
  }
  /** Removes focus from the icon button. */
  blur() {
    this.button.blur()
  }
  render() {
    const n = !!this.href,
      i = n ? vh`a` : vh`button`
    return Rx`
      <${i}
        part="base"
        class=${gn({
          'icon-button': !0,
          'icon-button--disabled': !n && this.disabled,
          'icon-button--focused': this.hasFocus,
        })}
        ?disabled=${qe(n ? void 0 : this.disabled)}
        type=${qe(n ? void 0 : 'button')}
        href=${qe(n ? this.href : void 0)}
        target=${qe(n ? this.target : void 0)}
        download=${qe(n ? this.download : void 0)}
        rel=${qe(n && this.target ? 'noreferrer noopener' : void 0)}
        role=${qe(n ? void 0 : 'button')}
        aria-disabled=${this.disabled ? 'true' : 'false'}
        aria-label="${this.label}"
        tabindex=${this.disabled ? '-1' : '0'}
        @blur=${this.handleBlur}
        @focus=${this.handleFocus}
        @click=${this.handleClick}
      >
        <sl-icon
          class="icon-button__icon"
          name=${qe(this.name)}
          library=${qe(this.library)}
          src=${qe(this.src)}
          aria-hidden="true"
        ></sl-icon>
      </${i}>
    `
  }
}
ue.styles = [bn, Ox]
ue.dependencies = { 'sl-icon': Pe }
R([Le('.icon-button')], ue.prototype, 'button', 2)
R([gi()], ue.prototype, 'hasFocus', 2)
R([V()], ue.prototype, 'name', 2)
R([V()], ue.prototype, 'library', 2)
R([V()], ue.prototype, 'src', 2)
R([V()], ue.prototype, 'href', 2)
R([V()], ue.prototype, 'target', 2)
R([V()], ue.prototype, 'download', 2)
R([V()], ue.prototype, 'label', 2)
R([V({ type: Boolean, reflect: !0 })], ue.prototype, 'disabled', 2)
var Mx = 0,
  we = class extends xe {
    constructor() {
      super(...arguments),
        (this.localize = new vi(this)),
        (this.attrId = ++Mx),
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
    handleCloseClick(n) {
      n.stopPropagation(), this.emit('sl-close')
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
        X`
      <div
        part="base"
        class=${gn({
          tab: !0,
          'tab--active': this.active,
          'tab--closable': this.closable,
          'tab--disabled': this.disabled,
        })}
      >
        <slot></slot>
        ${
          this.closable
            ? X`
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
we.styles = [bn, Tx]
we.dependencies = { 'sl-icon-button': ue }
R([Le('.tab')], we.prototype, 'tab', 2)
R([V({ reflect: !0 })], we.prototype, 'panel', 2)
R([V({ type: Boolean, reflect: !0 })], we.prototype, 'active', 2)
R([V({ type: Boolean, reflect: !0 })], we.prototype, 'closable', 2)
R([V({ type: Boolean, reflect: !0 })], we.prototype, 'disabled', 2)
R([V({ type: Number, reflect: !0 })], we.prototype, 'tabIndex', 2)
R([le('active')], we.prototype, 'handleActiveChange', 1)
R([le('disabled')], we.prototype, 'handleDisabledChange', 1)
const Lx = `:host {
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
class Ra extends mn(we, fi) {
  constructor() {
    super()
    Y(this, 'componentId', `tbk-tab-${this.attrId}`)
    ;(this.size = Lt.M), (this.shape = je.Round), (this.inverse = !1)
  }
}
Y(Ra, 'styles', [
  we.styles,
  Dt(),
  Me(),
  Po(),
  Ro('tab'),
  Dn('tab', 1.75, 4),
  pt(Lx),
]),
  Y(Ra, 'properties', {
    ...we.properties,
    size: { type: String, reflect: !0 },
    shape: { type: String, reflect: !0 },
    inverse: { type: Boolean, reflect: !0 },
    variant: { type: String },
  })
customElements.define('tbk-tab', Ra)
const Ix = `:host {
  --indicator-color: transparent;
  --track-color: var(--color-gray-5);
  --track-width: var(--half);
  --tabs-font-size: var(--font-size);
  --tabs-color: var(--color-variant);

  width: 100%;
  height: 100%;
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
var Dx = en`
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
function Bx(n, i) {
  return {
    top: Math.round(
      n.getBoundingClientRect().top - i.getBoundingClientRect().top,
    ),
    left: Math.round(
      n.getBoundingClientRect().left - i.getBoundingClientRect().left,
    ),
  }
}
function bh(n, i, r = 'vertical', a = 'smooth') {
  const l = Bx(n, i),
    h = l.top + i.scrollTop,
    f = l.left + i.scrollLeft,
    v = i.scrollLeft,
    m = i.scrollLeft + i.offsetWidth,
    w = i.scrollTop,
    k = i.scrollTop + i.offsetHeight
  ;(r === 'horizontal' || r === 'both') &&
    (f < v
      ? i.scrollTo({ left: f, behavior: a })
      : f + n.clientWidth > m &&
        i.scrollTo({ left: f - i.offsetWidth + n.clientWidth, behavior: a })),
    (r === 'vertical' || r === 'both') &&
      (h < w
        ? i.scrollTo({ top: h, behavior: a })
        : h + n.clientHeight > k &&
          i.scrollTo({ top: h - i.offsetHeight + n.clientHeight, behavior: a }))
}
var Wt = class extends xe {
  constructor() {
    super(...arguments),
      (this.tabs = []),
      (this.focusableTabs = []),
      (this.panels = []),
      (this.localize = new vi(this)),
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
    const n = Promise.all([
      customElements.whenDefined('sl-tab'),
      customElements.whenDefined('sl-tab-panel'),
    ])
    super.connectedCallback(),
      (this.resizeObserver = new ResizeObserver(() => {
        this.repositionIndicator(), this.updateScrollControls()
      })),
      (this.mutationObserver = new MutationObserver(i => {
        i.some(
          r => !['aria-labelledby', 'aria-controls'].includes(r.attributeName),
        ) && setTimeout(() => this.setAriaLabels()),
          i.some(r => r.attributeName === 'disabled') &&
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
          n.then(() => {
            new IntersectionObserver((r, a) => {
              var l
              r[0].intersectionRatio > 0 &&
                (this.setAriaLabels(),
                this.setActiveTab(
                  (l = this.getActiveTab()) != null ? l : this.tabs[0],
                  { emitEvents: !1 },
                ),
                a.unobserve(r[0].target))
            }).observe(this.tabGroup)
          })
      })
  }
  disconnectedCallback() {
    var n, i
    super.disconnectedCallback(),
      (n = this.mutationObserver) == null || n.disconnect(),
      this.nav && ((i = this.resizeObserver) == null || i.unobserve(this.nav))
  }
  getAllTabs() {
    return this.shadowRoot.querySelector('slot[name="nav"]').assignedElements()
  }
  getAllPanels() {
    return [...this.body.assignedElements()].filter(
      n => n.tagName.toLowerCase() === 'sl-tab-panel',
    )
  }
  getActiveTab() {
    return this.tabs.find(n => n.active)
  }
  handleClick(n) {
    const r = n.target.closest('sl-tab')
    ;(r == null ? void 0 : r.closest('sl-tab-group')) === this &&
      r !== null &&
      this.setActiveTab(r, { scrollBehavior: 'smooth' })
  }
  handleKeyDown(n) {
    const r = n.target.closest('sl-tab')
    if (
      (r == null ? void 0 : r.closest('sl-tab-group')) === this &&
      (['Enter', ' '].includes(n.key) &&
        r !== null &&
        (this.setActiveTab(r, { scrollBehavior: 'smooth' }),
        n.preventDefault()),
      [
        'ArrowLeft',
        'ArrowRight',
        'ArrowUp',
        'ArrowDown',
        'Home',
        'End',
      ].includes(n.key))
    ) {
      const l = this.tabs.find(v => v.matches(':focus')),
        h = this.localize.dir() === 'rtl'
      let f = null
      if ((l == null ? void 0 : l.tagName.toLowerCase()) === 'sl-tab') {
        if (n.key === 'Home') f = this.focusableTabs[0]
        else if (n.key === 'End')
          f = this.focusableTabs[this.focusableTabs.length - 1]
        else if (
          (['top', 'bottom'].includes(this.placement) &&
            n.key === (h ? 'ArrowRight' : 'ArrowLeft')) ||
          (['start', 'end'].includes(this.placement) && n.key === 'ArrowUp')
        ) {
          const v = this.tabs.findIndex(m => m === l)
          f = this.findNextFocusableTab(v, 'backward')
        } else if (
          (['top', 'bottom'].includes(this.placement) &&
            n.key === (h ? 'ArrowLeft' : 'ArrowRight')) ||
          (['start', 'end'].includes(this.placement) && n.key === 'ArrowDown')
        ) {
          const v = this.tabs.findIndex(m => m === l)
          f = this.findNextFocusableTab(v, 'forward')
        }
        if (!f) return
        ;(f.tabIndex = 0),
          f.focus({ preventScroll: !0 }),
          this.activation === 'auto'
            ? this.setActiveTab(f, { scrollBehavior: 'smooth' })
            : this.tabs.forEach(v => {
                v.tabIndex = v === f ? 0 : -1
              }),
          ['top', 'bottom'].includes(this.placement) &&
            bh(f, this.nav, 'horizontal'),
          n.preventDefault()
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
  setActiveTab(n, i) {
    if (
      ((i = pi(
        {
          emitEvents: !0,
          scrollBehavior: 'auto',
        },
        i,
      )),
      n !== this.activeTab && !n.disabled)
    ) {
      const r = this.activeTab
      ;(this.activeTab = n),
        this.tabs.forEach(a => {
          ;(a.active = a === this.activeTab),
            (a.tabIndex = a === this.activeTab ? 0 : -1)
        }),
        this.panels.forEach(a => {
          var l
          return (a.active =
            a.name === ((l = this.activeTab) == null ? void 0 : l.panel))
        }),
        this.syncIndicator(),
        ['top', 'bottom'].includes(this.placement) &&
          bh(this.activeTab, this.nav, 'horizontal', i.scrollBehavior),
        i.emitEvents &&
          (r && this.emit('sl-tab-hide', { detail: { name: r.panel } }),
          this.emit('sl-tab-show', { detail: { name: this.activeTab.panel } }))
    }
  }
  setAriaLabels() {
    this.tabs.forEach(n => {
      const i = this.panels.find(r => r.name === n.panel)
      i &&
        (n.setAttribute('aria-controls', i.getAttribute('id')),
        i.setAttribute('aria-labelledby', n.getAttribute('id')))
    })
  }
  repositionIndicator() {
    const n = this.getActiveTab()
    if (!n) return
    const i = n.clientWidth,
      r = n.clientHeight,
      a = this.localize.dir() === 'rtl',
      l = this.getAllTabs(),
      f = l.slice(0, l.indexOf(n)).reduce(
        (v, m) => ({
          left: v.left + m.clientWidth,
          top: v.top + m.clientHeight,
        }),
        { left: 0, top: 0 },
      )
    switch (this.placement) {
      case 'top':
      case 'bottom':
        ;(this.indicator.style.width = `${i}px`),
          (this.indicator.style.height = 'auto'),
          (this.indicator.style.translate = a
            ? `${-1 * f.left}px`
            : `${f.left}px`)
        break
      case 'start':
      case 'end':
        ;(this.indicator.style.width = 'auto'),
          (this.indicator.style.height = `${r}px`),
          (this.indicator.style.translate = `0 ${f.top}px`)
        break
    }
  }
  // This stores tabs and panels so we can refer to a cache instead of calling querySelectorAll() multiple times.
  syncTabsAndPanels() {
    ;(this.tabs = this.getAllTabs()),
      (this.focusableTabs = this.tabs.filter(n => !n.disabled)),
      (this.panels = this.getAllPanels()),
      this.syncIndicator(),
      this.updateComplete.then(() => this.updateScrollControls())
  }
  findNextFocusableTab(n, i) {
    let r = null
    const a = i === 'forward' ? 1 : -1
    let l = n + a
    for (; n < this.tabs.length; ) {
      if (((r = this.tabs[l] || null), r === null)) {
        i === 'forward'
          ? (r = this.focusableTabs[0])
          : (r = this.focusableTabs[this.focusableTabs.length - 1])
        break
      }
      if (!r.disabled) break
      l += a
    }
    return r
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
  show(n) {
    const i = this.tabs.find(r => r.panel === n)
    i && this.setActiveTab(i, { scrollBehavior: 'smooth' })
  }
  render() {
    const n = this.localize.dir() === 'rtl'
    return X`
      <div
        part="base"
        class=${gn({
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
              ? X`
                <sl-icon-button
                  part="scroll-button scroll-button--start"
                  exportparts="base:scroll-button__base"
                  class=${gn({
                    'tab-group__scroll-button': !0,
                    'tab-group__scroll-button--start': !0,
                    'tab-group__scroll-button--start--hidden':
                      this.shouldHideScrollStartButton,
                  })}
                  name=${n ? 'chevron-right' : 'chevron-left'}
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
              ? X`
                <sl-icon-button
                  part="scroll-button scroll-button--end"
                  exportparts="base:scroll-button__base"
                  class=${gn({
                    'tab-group__scroll-button': !0,
                    'tab-group__scroll-button--end': !0,
                    'tab-group__scroll-button--end--hidden':
                      this.shouldHideScrollEndButton,
                  })}
                  name=${n ? 'chevron-left' : 'chevron-right'}
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
Wt.styles = [bn, Dx]
Wt.dependencies = { 'sl-icon-button': ue, 'sl-resize-observer': Cr }
R([Le('.tab-group')], Wt.prototype, 'tabGroup', 2)
R([Le('.tab-group__body')], Wt.prototype, 'body', 2)
R([Le('.tab-group__nav')], Wt.prototype, 'nav', 2)
R([Le('.tab-group__indicator')], Wt.prototype, 'indicator', 2)
R([gi()], Wt.prototype, 'hasScrollControls', 2)
R([gi()], Wt.prototype, 'shouldHideScrollStartButton', 2)
R([gi()], Wt.prototype, 'shouldHideScrollEndButton', 2)
R([V()], Wt.prototype, 'placement', 2)
R([V()], Wt.prototype, 'activation', 2)
R(
  [V({ attribute: 'no-scroll-controls', type: Boolean })],
  Wt.prototype,
  'noScrollControls',
  2,
)
R(
  [V({ attribute: 'fixed-scroll-controls', type: Boolean })],
  Wt.prototype,
  'fixedScrollControls',
  2,
)
R([v$({ passive: !0 })], Wt.prototype, 'updateScrollButtons', 1)
R(
  [le('noScrollControls', { waitUntilFirstUpdate: !0 })],
  Wt.prototype,
  'updateScrollControls',
  1,
)
R(
  [le('placement', { waitUntilFirstUpdate: !0 })],
  Wt.prototype,
  'syncIndicator',
  1,
)
var Ux = (n, i) => {
    let r = 0
    return function (...a) {
      window.clearTimeout(r),
        (r = window.setTimeout(() => {
          n.call(this, ...a)
        }, i))
    }
  },
  yh = (n, i, r) => {
    const a = n[i]
    n[i] = function (...l) {
      a.call(this, ...l), r.call(this, a, ...l)
    }
  },
  Nx = 'onscrollend' in window
if (!Nx) {
  const n = /* @__PURE__ */ new Set(),
    i = /* @__PURE__ */ new WeakMap(),
    r = l => {
      for (const h of l.changedTouches) n.add(h.identifier)
    },
    a = l => {
      for (const h of l.changedTouches) n.delete(h.identifier)
    }
  document.addEventListener('touchstart', r, !0),
    document.addEventListener('touchend', a, !0),
    document.addEventListener('touchcancel', a, !0),
    yh(EventTarget.prototype, 'addEventListener', function (l, h) {
      if (h !== 'scrollend') return
      const f = Ux(() => {
        n.size ? f() : this.dispatchEvent(new Event('scrollend'))
      }, 100)
      l.call(this, 'scroll', f, { passive: !0 }), i.set(this, f)
    }),
    yh(EventTarget.prototype, 'removeEventListener', function (l, h) {
      if (h !== 'scrollend') return
      const f = i.get(this)
      f && l.call(this, 'scroll', f, { passive: !0 })
    })
}
class Ma extends mn(Wt) {
  constructor() {
    super(),
      (this.size = Lt.M),
      (this.inverse = !1),
      (this.placement = Ky.Top),
      (this.variant = xt.Neutral),
      (this.fullwidth = !1)
  }
  get elSlotNav() {
    var i
    return (i = this.renderRoot) == null
      ? void 0
      : i.querySelector('slot[name="nav"]')
  }
  async firstUpdated() {
    ;(this.resizeObserver = new ResizeObserver(() => {
      this.repositionIndicator(), this.updateScrollControls()
    })),
      (this.mutationObserver = new MutationObserver(r => {
        r.some(
          a => !['aria-labelledby', 'aria-controls'].includes(a.attributeName),
        ) && setTimeout(() => this.setAriaLabels()),
          r.some(a => a.attributeName === 'disabled') &&
            this.syncTabsAndPanels()
      }))
    const i = Promise.all([
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
          i.then(r => {
            setTimeout(() => {
              new IntersectionObserver((l, h) => {
                l[0].intersectionRatio > 0 &&
                  (this.setAriaLabels(),
                  this.setActiveTab(this.getActiveTab() ?? this.tabs[0], {
                    emitEvents: !1,
                  }),
                  h.unobserve(l[0].target))
              }).observe(this.tabGroup),
                this.getAllTabs().forEach(l => {
                  l.setAttribute('size', this.size),
                    Kt(l.variant) && (l.variant = this.variant),
                    this.inverse && (l.inverse = this.inverse)
                }),
                this.getAllPanels().forEach(l => {
                  l.setAttribute('size', this.size),
                    Kt(l.getAttribute('variant')) &&
                      l.setAttribute('variant', this.variant)
                })
            }, 500)
          })
      })
  }
  getAllTabs(i = { includeDisabled: !0 }) {
    var r
    return [
      ...((r = this.elSlotNav) == null ? void 0 : r.assignedElements()),
    ].filter(a =>
      i.includeDisabled
        ? a.tagName.toLowerCase() === 'tbk-tab'
        : a.tagName.toLowerCase() === 'tbk-tab' && !a.disabled,
    )
  }
  getAllPanels() {
    return [...this.body.assignedElements()].filter(
      i => i.tagName.toLowerCase() === 'tbk-tab-panel',
    )
  }
  handleClick(i) {
    const a = i.target.closest('tbk-tab')
    ;(a == null ? void 0 : a.closest('tbk-tabs')) === this &&
      a !== null &&
      (this.setActiveTab(a, { scrollBehavior: 'smooth' }),
      this.emit('tab-change', {
        detail: new this.emit.EventDetail(a.panel, i),
      }))
  }
  handleKeyDown(i) {
    const a = i.target.closest('tbk-tab')
    if (
      (a == null ? void 0 : a.closest('tbk-tabs')) === this &&
      (['Enter', ' '].includes(i.key) &&
        a !== null &&
        (this.setActiveTab(a, { scrollBehavior: 'smooth' }),
        i.preventDefault()),
      [
        'ArrowLeft',
        'ArrowRight',
        'ArrowUp',
        'ArrowDown',
        'Home',
        'End',
      ].includes(i.key))
    ) {
      const h = this.tabs.find(v => v.matches(':focus')),
        f = this.localize.dir() === 'rtl'
      if ((h == null ? void 0 : h.tagName.toLowerCase()) === 'tbk-tab') {
        let v = this.tabs.indexOf(h)
        i.key === 'Home'
          ? (v = 0)
          : i.key === 'End'
          ? (v = this.tabs.length - 1)
          : (['top', 'bottom'].includes(this.placement) &&
              i.key === (f ? 'ArrowRight' : 'ArrowLeft')) ||
            (['start', 'end'].includes(this.placement) && i.key === 'ArrowUp')
          ? v--
          : ((['top', 'bottom'].includes(this.placement) &&
              i.key === (f ? 'ArrowLeft' : 'ArrowRight')) ||
              (['start', 'end'].includes(this.placement) &&
                i.key === 'ArrowDown')) &&
            v++,
          v < 0 && (v = this.tabs.length - 1),
          v > this.tabs.length - 1 && (v = 0),
          this.tabs[v].focus({ preventScroll: !0 }),
          this.activation === 'auto' &&
            this.setActiveTab(this.tabs[v], { scrollBehavior: 'smooth' }),
          ['top', 'bottom'].includes(this.placement) &&
            scrollIntoView(this.tabs[v], this.nav, 'horizontal'),
          i.preventDefault()
      }
    }
  }
}
Y(Ma, 'styles', [Wt.styles, Dt(), Me(), Po(), Dn('tabs', 1.25, 4), pt(Ix)]),
  Y(Ma, 'properties', {
    ...Wt.properties,
    size: { type: String, reflect: !0 },
    variant: { type: String, reflect: !0 },
    inverse: { type: Boolean, reflect: !0 },
    fullwidth: { type: Boolean, reflect: !0 },
  })
customElements.define('tbk-tabs', Ma)
var Fx = en`
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
  Hx = 0,
  Jn = class extends xe {
    constructor() {
      super(...arguments),
        (this.attrId = ++Hx),
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
      return X`
      <slot
        part="base"
        class=${gn({
          'tab-panel': !0,
          'tab-panel--active': this.active,
        })}
      ></slot>
    `
    }
  }
Jn.styles = [bn, Fx]
R([V({ reflect: !0 })], Jn.prototype, 'name', 2)
R([V({ type: Boolean, reflect: !0 })], Jn.prototype, 'active', 2)
R([le('active')], Jn.prototype, 'handleActiveChange', 1)
const Wx = `:host {
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
class La extends mn(Jn, fi) {
  constructor() {
    super()
    Y(this, 'componentId', `tbk-tab-panel-${this.attrId}`)
    this.size = Lt.M
  }
}
Y(La, 'styles', [Jn.styles, Dt(), Me(), Dn('tab-panel', 1.5, 3), pt(Wx)]),
  Y(La, 'properties', {
    ...Jn.properties,
    size: { type: String, reflect: !0 },
  })
customElements.define('tbk-tab-panel', La)
const qx = `:host {
  display: inline-flex;
  align-items: center;
  color: var(--color-header);
  white-space: nowrap;
}
tbk-badge {
  --badge-font-family: var(--font-mono);

  font-variant-numeric: slashed-zero;
}
tbk-badge::part(base) {
  margin: 0;
  padding: 0 0 0 0;
  display: flex;
  align-items: center;
  font-weight: var(--text-light);
  color: inherit;
}
span {
  line-height: 1;
  background: var(--color-gray-10);
  color: var(--color-gray-700);
  border-radius: calc(var(--font-size) / 3);
  font-weight: var(--text-black);
  padding: calc(var(--step)) calc(var(--step) + var(--step-3) / 4) calc(var(--step) - var(--half))
    var(--step-2);
  text-transform: uppercase;
  font-size: calc(var(--font-size) * 0.8);
}
b {
  color: var(--color-gray-700);
  padding: calc(var(--step)) var(--step-2) calc(var(--step) - var(--half)) var(--step-2);
  font-weight: var(--text-semibold);
}
p {
  color: var(--color-gray-700);
  margin: 0;
  padding: calc(var(--step)) var(--step-2) calc(var(--step) - var(--half)) 0;
}
[part='base'] {
  display: flex;
  align-items: center;
  gap: var(--step);
}
[part='base'] small {
  font-size: var(--text-xs);
  color: var(--color-gray-500);
  margin-right: var(--step-2);
}
`
function Yx(n) {
  const i = {
    timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    timeZoneName: 'short',
  }
  return new Intl.DateTimeFormat(Gx(), i)
    .formatToParts(n)
    .find(r => r.type === 'timeZoneName').value
}
function Gx() {
  var n
  return (
    (typeof window < 'u' &&
      window.navigator &&
      (((n = window.navigator.languages) == null ? void 0 : n[0]) ||
        window.navigator.language ||
        window.navigator.userLanguage ||
        window.navigator.browserLanguage)) ||
    'en-US'
  )
}
class Ia extends mn($e, Dw) {
  constructor() {
    super()
    Y(this, '_defaultFormat', 'YYYY-MM-DD HH:mm:ss')
    ;(this.label = ''),
      (this.date = Date.now()),
      (this.format = this._defaultFormat),
      (this.size = Lt.XXS),
      (this.side = Je.Right),
      (this.realtime = !1),
      (this.hideTimezone = !1),
      (this.hideDate = !1)
  }
  get hasTime() {
    const r = this.format.toLowerCase()
    return r.includes('h') || r.includes('m') || r.includes('s')
  }
  connectedCallback() {
    super.connectedCallback()
    try {
      this._timestamp = Ha(+this.date)
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
  willUpdate(r) {
    r.has('timezone') &&
      (this._displayTimezone = this.isUTC
        ? Ye.title(Ye.UTC)
        : Yx(new Date(this._timestamp)))
  }
  render() {
    const r = this.isUTC
      ? pa(this._timestamp, this.format)
      : ga(this._timestamp, this.format)
    let a = X`<b>${r}</b>`
    if (this.format === this._defaultFormat) {
      const [l, h] = (
        this.isUTC
          ? pa(this._timestamp, this.format)
          : ga(this._timestamp, this.format)
      ).split(' ')
      a = [X`<b>${l}</b>`, h && X`<p>${h}</p>`].filter(Boolean)
    }
    return X`<tbk-badge
      title="${this._timestamp}"
      size="${this.size}"
    >
      ${zt(
        wt(this.hideTimezone) && this.side === Je.Left,
        X`<span>${this._displayTimezone}</span>`,
      )}
      ${zt(wt(this.hideDate), X`${a}`)}
      ${zt(
        wt(this.hideTimezone) && this.side === Je.Right,
        X`<span>${this._displayTimezone}</span>`,
      )}
    </tbk-badge>`
  }
}
Y(Ia, 'styles', [Dt(), Wh('label'), pt(qx)]),
  Y(Ia, 'properties', {
    date: { type: [String, Number] },
    format: { type: String },
    size: { type: String, reflect: !0 },
    side: { type: String, reflect: !0 },
    realtime: { type: Boolean },
    hideTimezone: { type: Boolean, attribute: 'hide-timezone' },
    hideDate: { type: Boolean, attribute: 'hide-date' },
    _timestamp: { type: Number, state: !0 },
    _displayTimezone: { type: String, state: !0 },
  })
customElements.define('tbk-datetime', Ia)
export {
  va as Badge,
  Ia as Datetime,
  Pa as Details,
  ka as Metadata,
  Ta as MetadataItem,
  Oa as MetadataSection,
  Sa as ModelName,
  px as ResizeObserver,
  cd as Scroll,
  Aa as SourceList,
  Ca as SourceListItem,
  Ea as SourceListSection,
  za as SplitPane,
  Ra as Tab,
  La as TabPanel,
  Ma as Tabs,
}
