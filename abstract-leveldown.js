/* Copyright (c) 2017 Rod Vagg, MIT License */

const hasOwnProperty = Object.prototype.hasOwnProperty

const AbstractIterator = require('./abstract-iterator')
const AbstractChainedBatch = require('./abstract-chained-batch')
const IteratorStream = require('./iterator-stream')

const rangeOptions = 'start end gt gte lt lte'.split(' ')

function AbstractLevelDOWN (location, options) {
  if (!arguments.length || location === undefined) {
    throw new Error('constructor requires at least a location argument')
  }

  if (typeof location !== 'string') {
    throw new Error('constructor requires a location string argument')
  }

  this.location = location
  this.status = 'new'

  switch (options && options.valueEncoding) {
    case 'string':
      this._serializeValue = serializeStringOrBuffer
      this._deserializeValue = deserializeString
      break
    case 'json':
      this._serializeValue = serializeJSON
      this._deserializeValue = deserializeJSON
      break
    case 'buffer':
    default:
      this._serializeValue = serializeStringOrBuffer
      this._deserializeValue = deserializeBuffer
  }

  switch (options && options.keyEncoding) {
    case 'string':
      this._deserializeKey = deserializeString
      break
    case 'buffer':
    default:
      this._deserializeKey = deserializeBuffer
  }
}

AbstractLevelDOWN.prototype.open = function (options, callback) {
  const self = this
  const oldStatus = this.status

  if (typeof options === 'function') { callback = options }

  if (typeof callback !== 'function') {
    throw new Error('open() requires a callback argument')
  }

  if (typeof options !== 'object') { options = {} }

  options.createIfMissing = options.createIfMissing !== false
  options.errorIfExists = !!options.errorIfExists

  this.status = 'opening'
  this._open(options, function (err) {
    if (err) {
      self.status = oldStatus
      return callback(err)
    }
    self.status = 'open'
    callback()
  })
}

AbstractLevelDOWN.prototype._open = function (options, callback) {
  process.nextTick(callback)
}

AbstractLevelDOWN.prototype.close = function (callback) {
  const self = this
  const oldStatus = this.status

  if (typeof callback !== 'function') {
    throw new Error('close() requires a callback argument')
  }

  this.status = 'closing'
  this._close(function (err) {
    if (err) {
      self.status = oldStatus
      return callback(err)
    }
    self.status = 'closed'
    callback()
  })
}

AbstractLevelDOWN.prototype._close = function (callback) {
  process.nextTick(callback)
}

AbstractLevelDOWN.prototype.get = function (key, options, callback) {
  if (typeof options === 'function') { callback = options }

  if (typeof callback !== 'function') {
    throw new Error('get() requires a callback argument')
  }

  const err = this._checkKey(key, 'key')
  if (err) return process.nextTick(callback, err)

  key = this._serializeKey(key)

  if (typeof options !== 'object') { options = {} }

  options.asBuffer = options.asBuffer !== false

  const self = this
  this._get(key, options, (err, value) => {
    if (err) {
      if ((/notfound/i).test(err) || err.notFound) {
        callback({ name: 'NotFoundError' })  // eslint-disable-line
      } else {
        callback(err)
      }
    } else {
      callback(err, value != null ? self._deserializeValue(value) : value)
    }
  })
}

AbstractLevelDOWN.prototype._get = function (key, options, callback) {
  process.nextTick(function () { callback(new Error('NotFound')) })
}

AbstractLevelDOWN.prototype.put = function (key, value, options, callback) {
  if (typeof options === 'function') { callback = options }

  if (typeof callback !== 'function') {
    throw new Error('put() requires a callback argument')
  }

  const err = this._checkKey(key, 'key')
  if (err) return process.nextTick(callback, err)

  key = this._serializeKey(key)
  value = this._serializeValue(value)

  if (typeof options !== 'object') { options = {} }

  this._put(key, value, options, callback)
}

AbstractLevelDOWN.prototype._put = function (key, value, options, callback) {
  process.nextTick(callback)
}

AbstractLevelDOWN.prototype.del = function (key, options, callback) {
  if (typeof options === 'function') { callback = options }

  if (typeof callback !== 'function') {
    throw new Error('del() requires a callback argument')
  }

  const err = this._checkKey(key, 'key')
  if (err) return process.nextTick(callback, err)

  key = this._serializeKey(key)

  if (typeof options !== 'object') { options = {} }

  this._del(key, options, callback)
}

AbstractLevelDOWN.prototype._del = function (key, options, callback) {
  process.nextTick(callback)
}

AbstractLevelDOWN.prototype.batch = function (array, options, callback) {
  if (!arguments.length) { return this._chainedBatch() }

  if (typeof options === 'function') { callback = options }

  if (typeof array === 'function') { callback = array }

  if (typeof callback !== 'function') {
    throw new Error('batch(array) requires a callback argument')
  }

  if (!Array.isArray(array)) {
    return process.nextTick(callback, new Error('batch(array) requires an array argument'))
  }

  if (!options || typeof options !== 'object') { options = {} }

  const serialized = new Array(array.length)

  for (let i = 0; i < array.length; i++) {
    if (typeof array[i] !== 'object' || array[i] === null) {
      return process.nextTick(callback, new Error('batch(array) element must be an object and not `null`'))
    }

    const e = { ...array[i] }

    if (e.type !== 'put' && e.type !== 'del') {
      return process.nextTick(callback, new Error("`type` must be 'put' or 'del'"))
    }

    const err = this._checkKey(e.key, 'key')
    if (err) return process.nextTick(callback, err)

    e.key = this._serializeKey(e.key)

    if (e.type === 'put') { e.value = this._serializeValue(e.value) }

    serialized[i] = e
  }

  this._batch(serialized, options, callback)
}

AbstractLevelDOWN.prototype._batch = function (array, options, callback) {
  process.nextTick(callback)
}

AbstractLevelDOWN.prototype._setupIteratorOptions = function (options) {
  options = cleanRangeOptions(options)

  options.reverse = !!options.reverse
  options.keys = options.keys !== false
  options.values = options.values !== false
  options.limit = 'limit' in options ? options.limit : -1
  options.keyAsBuffer = options.keyAsBuffer !== false
  options.valueAsBuffer = options.valueAsBuffer !== false

  return options
}

function cleanRangeOptions (options) {
  const result = {}

  for (const k in options) {
    if (!hasOwnProperty.call(options, k)) continue
    if (isRangeOption(k) && isEmptyRangeOption(options[k])) continue

    result[k] = options[k]
  }

  return result
}

function isRangeOption (k) {
  return rangeOptions.indexOf(k) !== -1
}

function isEmptyRangeOption (v) {
  return v === '' || v == null || isEmptyBuffer(v)
}

function isEmptyBuffer (v) {
  return Buffer.isBuffer(v) && v.length === 0
}

AbstractLevelDOWN.prototype.iterator = function (options) {
  if (typeof options !== 'object') { options = {} }
  options = this._setupIteratorOptions(options)
  return this._iterator(options)
}

AbstractLevelDOWN.prototype._iterator = function (options) {
  return new AbstractIterator(this)
}

AbstractLevelDOWN.prototype._chainedBatch = function () {
  return new AbstractChainedBatch(this)
}

AbstractLevelDOWN.prototype._serializeKey = function (key) {
  return Buffer.isBuffer(key) ? key : String(key)
}

AbstractLevelDOWN.prototype._deserializeKey = function (key) {
  return String(key)
}

AbstractLevelDOWN.prototype._serializeValue = serializeStringOrBuffer

AbstractLevelDOWN.prototype._checkKey = function (obj, type) {
  if (obj === null || obj === undefined) {
    return new Error(type + ' cannot be `null` or `undefined`')
  }

  if (Buffer.isBuffer(obj) && obj.length === 0) {
    return new Error(type + ' cannot be an empty Buffer')
  }

  if (String(obj) === '') {
    return new Error(type + ' cannot be an empty String')
  }
}

AbstractLevelDOWN.prototype.createReadStream = function (options) {
  options = { keys: true, values: true, ...options }
  if (typeof options.limit !== 'number') { options.limit = -1 }
  return new IteratorStream(this.iterator(options), options)
}

AbstractLevelDOWN.prototype.createKeyStream = function (options) {
  options = { keys: true, values: false, ...options }
  if (typeof options.limit !== 'number') { options.limit = -1 }
  return new IteratorStream(this.iterator(options), options)
}

AbstractLevelDOWN.prototype.createValueStream = function (options) {
  options = { keys: false, values: true, ...options }
  if (typeof options.limit !== 'number') { options.limit = -1 }
  return new IteratorStream(this.iterator(options), options)
}

function serializeStringOrBuffer (value) {
  if (value == null) return ''
  return Buffer.isBuffer(value) || process.browser ? value : String(value)
}

function serializeJSON (value) {
  return value != null ? JSON.stringify(value) : ''
}

function deserializeString (buffer) {
  return String(buffer)
}

function deserializeBuffer (buffer) {
  return buffer
}

function deserializeJSON (buffer) {
  try {
    return buffer.length ? JSON.parse(buffer) : null
  } catch (err) {
    console.warn('LevelDB: Invalid JSON value', String(buffer))
    return null
  }
}

module.exports = AbstractLevelDOWN
