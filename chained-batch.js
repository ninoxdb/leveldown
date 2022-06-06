'use strict'

const util = require('util')
const binding = require('./binding')
const AbstractChainedBatch = require('./abstract-chained-batch')

function ChainedBatch (db) {
  AbstractChainedBatch.call(this, db)
  this.context = binding.batch_init(db.context)
}

util.inherits(ChainedBatch, AbstractChainedBatch)

ChainedBatch.prototype._put = function (key, value) {
  console.log('ChainedBatch._put', key, value)
  binding.batch_put(this.context, key, value)
}

ChainedBatch.prototype._del = function (key) {
  binding.batch_del(this.context, key)
}

ChainedBatch.prototype._clear = function () {
  binding.batch_clear(this.context)
}

ChainedBatch.prototype._write = function (options, callback) {
  binding.batch_write(this.context, options, callback)
}

module.exports = ChainedBatch
