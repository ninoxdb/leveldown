const make = require('./make')

make('snapshot', function (db, t, done) {
  const snapshot = db.snapshot()
  t.ok(snapshot != null, 'has created snapshot')
  done()
})
