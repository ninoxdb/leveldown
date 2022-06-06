'use strict'

const util = require('util')
const AbstractLevelDOWN = require('./abstract-leveldown')
const binding = require('./binding')
const ChainedBatch = require('./chained-batch')
const Iterator = require('./iterator')
const Snapshot = require('./snapshot')

function LevelDOWN (location, options) {
  if (!(this instanceof LevelDOWN)) {
    return new LevelDOWN(location, options)
  }

  if (typeof location !== 'string') {
    throw new Error('constructor requires a location string argument')
  }

  AbstractLevelDOWN.call(this, location, {
    bufferKeys: true,
    snapshots: true,
    permanence: true,
    seek: true,
    clear: true,
    createIfMissing: true,
    errorIfExists: true,
    additionalMethods: {
      approximateSize: true,
      compactRange: true
    },
    ...options
  })

  this.location = location
  this.context = binding.db_init()
}

util.inherits(LevelDOWN, AbstractLevelDOWN)

LevelDOWN.prototype._open = function (options, callback) {
  binding.db_open(this.context, this.location, options, callback)
}

LevelDOWN.prototype._close = function (callback) {
  binding.db_close(this.context, callback)
}

LevelDOWN.prototype._serializeKey = function (key) {
  return Buffer.isBuffer(key) ? key : String(key)
}

LevelDOWN.prototype._serializeValue = function (value) {
  return Buffer.isBuffer(value) ? value : String(value)
}

LevelDOWN.prototype._put = function (key, value, options, callback) {
  binding.db_put(this.context, key, value, options, callback)
}

LevelDOWN.prototype._get = function (key, options, callback) {
  binding.db_get(this.context, key, options, callback)
}

LevelDOWN.prototype._del = function (key, options, callback) {
  binding.db_del(this.context, key, options, callback)
}

LevelDOWN.prototype._chainedBatch = function () {
  return new ChainedBatch(this)
}

LevelDOWN.prototype._batch = function (operations, options, callback) {
  binding.batch_do(this.context, operations, options, callback)
}

/* LevelDOWN.prototype.createBatch = function (options) {
  const batch = binding.batch_init(this.context);
  return {
    put: (key, value) => {
      key = this._serializeKey(key)
      value = this._serializeValue(value)
      binding.batch_put(batch, key, value);
    },
    del: (key) => {
      key = this._serializeKey(key)
      binding.batch_del(batch, key);
    },
    clear: () => {
      binding.batch_clear(batch);
    },
    write: (callback) => {
      binding.batch_write(batch, options, callback);
    }
  }
} */

LevelDOWN.prototype.approximateSize = function (start, end, callback) {
  if (start == null ||
      end == null ||
      typeof start === 'function' ||
      typeof end === 'function') {
    throw new Error('approximateSize() requires valid `start` and `end` arguments')
  }

  if (typeof callback !== 'function') {
    throw new Error('approximateSize() requires a callback argument')
  }

  start = this._serializeKey(start)
  end = this._serializeKey(end)

  binding.db_approximate_size(this.context, start, end, callback)
}

LevelDOWN.prototype.compactRange = function (start, end, callback) {
  if (start == null ||
      end == null ||
      typeof start === 'function' ||
      typeof end === 'function') {
    throw new Error('compactRange() requires valid `start` and `end` arguments')
  }

  if (typeof callback !== 'function') {
    throw new Error('compactRange() requires a callback argument')
  }

  start = this._serializeKey(start)
  end = this._serializeKey(end)

  binding.db_compact_range(this.context, start, end, callback)
}

LevelDOWN.prototype.getProperty = function (property) {
  if (typeof property !== 'string') {
    throw new Error('getProperty() requires a valid `property` argument')
  }

  return binding.db_get_property(this.context, property)
}

LevelDOWN.prototype._iterator = function (options) {
  if (this.status !== 'open') {
    // Prevent segfault
    throw new Error('cannot call iterator() before open()')
  }

  return new Iterator(this, options)
}

LevelDOWN.prototype.snapshot = function (options) {
  if (this.status !== 'open') {
    // Prevent segfault
    throw new Error('cannot call iterator() before open()')
  }

  return new Snapshot(this, options)
}

LevelDOWN.destroy = function (location, callback) {
  if (arguments.length < 2) {
    throw new Error('destroy() requires `location` and `callback` arguments')
  }
  if (typeof location !== 'string') {
    throw new Error('destroy() requires a location string argument')
  }
  if (typeof callback !== 'function') {
    throw new Error('destroy() requires a callback function argument')
  }

  binding.destroy_db(location, callback)
}

LevelDOWN.repair = function (location, callback) {
  if (arguments.length < 2) {
    throw new Error('repair() requires `location` and `callback` arguments')
  }
  if (typeof location !== 'string') {
    throw new Error('repair() requires a location string argument')
  }
  if (typeof callback !== 'function') {
    throw new Error('repair() requires a callback function argument')
  }

  binding.repair_db(location, callback)
}

module.exports = LevelDOWN
