const { Readable } = require('streamx')
const tape = require('tape')
const Union = require('./')

const { LEFT, RIGHT, EQ_LEFT, EQ_RIGHT } = require('./constants')

tape('numbers', function (t) {
  const a = Readable.from([4, 6, 10, 14, 15, 20, 22])
  const b = Readable.from([6, 11, 20])

  const u = new Union(a, b)
  const expected = [4, 6, 10, 11, 14, 15, 20, 22]

  u.on('data', function (data) {
    t.same(data, expected.shift())
  })

  u.on('end', function () {
    t.same(expected.length, 0, 'no more data')
    t.end()
  })
})

tape('string', function (t) {
  const a = Readable.from(['04', '06', '10', '14', '15', '20', '22'])
  const b = Readable.from(['06', '11', '20'])

  const u = new Union(a, b)
  const expected = ['04', '06', '10', '11', '14', '15', '20', '22']

  u.on('data', function (data) {
    t.same(data, expected.shift())
  })

  u.on('end', function () {
    t.same(expected.length, 0, 'no more data')
    t.end()
  })
})

tape('objects', function (t) {
  const a = Readable.from([{ key: '04' }, { key: '06' }, { key: '10' }, { key: '14' }, { key: '15' }, { key: '20' }, { key: '22' }])
  const b = Readable.from([{ key: '06' }, { key: '11' }, { key: '20' }])

  const u = new Union(a, b, (a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : 0)

  const expected = [{ key: '04' }, { key: '06' }, { key: '10' }, { key: '11' }, { key: '14' }, { key: '15' }, { key: '20' }, { key: '22' }]

  u.on('data', function (data) {
    t.same(data, expected.shift())
  })

  u.on('end', function () {
    t.same(expected.length, 0, 'no more data')
    t.end()
  })
})

tape('custom objects', function (t) {
  const a = Readable.from([{ bar: '04' }, { bar: '06' }, { bar: '10' }, { bar: '14' }, { bar: '15' }, { bar: '20' }, { bar: '22' }])
  const b = Readable.from([{ bar: '06' }, { bar: '11' }, { bar: '20' }])

  const u = new Union(a, b, (a, b) => a.bar < b.bar ? -1 : a.bar > b.bar ? 1 : 0)

  const expected = [{ bar: '04' }, { bar: '06' }, { bar: '10' }, { bar: '11' }, { bar: '14' }, { bar: '15' }, { bar: '20' }, { bar: '22' }]

  u.on('data', function (data) {
    t.same(data, expected.shift())
  })

  u.on('end', function () {
    t.same(expected.length, 0, 'no more data')
    t.end()
  })
})

tape('custom objects - simulation of DB index', function (t) {
  const a = Readable.from([
   {bar: '04', t: 1},
   {bar: '06', t: 1},
   {bar: '10', t: 1},
   {bar: '14', t: 1},
   {bar: '15', t: 1},
   {bar: '20', t: 1},
   {bar: '22', t: 1}
   ])
 const b = Readable.from([
   {bar: '05', t: 1},
   {bar: '14', t: 10},
   {bar: '16', t: 1},
   {bar: '20', t: 20}]
   )

  const u = new Union(a, b, (a, b) => {
    if (a.bar < b.bar)
      return LEFT
    if (a.bar > b.bar)
      return RIGHT
    if (a.t < b.t)
      return EQ_RIGHT
    return EQ_LEFT
  })
  const expected = [
   {bar: '04', t: 1},
   {bar: '05', t: 1},
   {bar: '06', t: 1},
   {bar: '10', t: 1},
   {bar: '14', t: 10},
   {bar: '15', t: 1},
   {bar: '16', t: 1},
   {bar: '20', t: 20},
   {bar: '22', t: 1}
   ]

  u.on('data', function (data) {
    // console.log(data)
    t.same(data, expected.shift())
  })

  u.on('end', function () {
    t.same(expected.length, 0, 'no more data')
    t.end()
  })
})

tape('destroy stream', function (t) {
  var a = new Readable()
  var b = new Readable()

  t.plan(2)

  a.on('close', function () {
    t.ok(true)
  })

  b.on('close', function () {
    t.ok(true)
  })

  new Union(a, b).destroy()
})
/*
tape.skip('custom objects - simulation of DB index', function (t) {
  const a = Readable.from([
   { a: {bar: '04'} },
   { b: {bar: '06'} },
   { c: {bar: '10'} },
   { d: {bar: '14'} },
   { e: {bar: '15'} },
   { f: {bar: '20'} },
   { g: {bar: '22'} }
   ])
 const b = Readable.from([
   { b: {bar: '05'} },
   { d: {bar: '16'} },
   { e: {bar: '20'} }]
   )

  const u = new Union(a, b, (a, b) => {
    let akey = Object.keys(a)[0]
    let bkey = Object.keys(b)[0]
    if (akey < bkey)
      return LEFT
    if (akey > bkey)
      return RIGHT
    if (a[akey].bar < b[bkey].bar)
      return EQ_RIGHT
    return EQ_LEFT
  })
  const expected = [
   { a: {bar: '04'} },
   { b: {bar: '06'} },
   { c: {bar: '10'} },
   { d: {bar: '16'} },
   { e: {bar: '20'} },
   { f: {bar: '20'} },
   { g: {bar: '22'} }
   ]

  u.on('data', function (data) {
    console.log(data)
    t.same(data, expected.shift())
  })

  u.on('end', function () {
    t.same(expected.length, 0, 'no more data')
    t.end()
  })
})

 */