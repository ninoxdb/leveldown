const util = require('util')
const Readable = require('readable-stream').Readable

module.exports = ReadStream

function ReadStream (iterator, options) {
  if (!(this instanceof ReadStream)) return new ReadStream(iterator, options)
  options = options || {}
  Readable.call(this, { ...options, objectMode: true })
  this._iterator = iterator
  this._destroyed = false
  this._options = options
  this.on('end', this._cleanup.bind(this))
}
util.inherits(ReadStream, Readable)

ReadStream.prototype._read = function () {
  const self = this
  const options = this._options
  if (this._destroyed) return

  this._iterator.next(function (err, key, value) {
    if (self._destroyed) return
    if (err) return self.emit('error', err)
    if (key === undefined && value === undefined) {
      self.push(null)
    } else if (options.keys !== false && options.values === false) {
      self.push(key)
    } else if (options.keys === false && options.values !== false) {
      self.push(value)
    } else {
      self.push({ key: key, value: value })
    }
  })
}

ReadStream.prototype.destroy =
ReadStream.prototype._cleanup = function () {
  const self = this
  if (this._destroyed) return
  this._destroyed = true

  this._iterator.end(function (err) {
    if (err) return self.emit('error', err)
    self.emit('close')
  })
}
