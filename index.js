const Corestore = require('corestore')
const Protocol = require('hypercore-protocol')
const Nanoresource = require('nanoresource/emitter')
const ram = require('random-access-memory')
const collect = require('stream-collector')
const hypercore = require('hypercore')
const debug = require('debug')('multifeed')
const raf = require('random-access-file')
const through = require('through2')

const { CorestoreMuxerTopic } = require('./corestore')
// Key-less constant hypercore to bootstrap hypercore-protocol replication.
const defaultEncryptionKey = Buffer.from('bee80ff3a4ee5e727dc44197cb9d25bf8f19d50b0f3ad2984cfe5b7d14e75de7', 'hex')

const LISTFEED_NAMESPACE = 'multifeed-feedlist'

module.exports = (...args) => new CorestoreMultifeed(...args)

class CorestoreMultifeed extends Nanoresource {
  constructor (storage, opts) {
    super()
    this._opts = opts
    this._rootKey = opts.encryptionKey || opts.key
    if (!this._rootKey) {
      debug('WARNING: Using insecure default encryption key')
      this._rootKey = defaultEncryptionKey
    }
    this._corestore = defaultCorestore(storage, opts).namespace(this._rootKey)
    this._handlers = opts.handlers || defaultPersistHandlers(this._corestore)
    this._feedsByKey = new Map()
    this._feedsByName = new Map()
    this.ready = this.open.bind(this)
  }

  _open (cb) {
    this._corestore.ready(err => {
      if (err) return cb(err)
      this._root = hypercore(ram, this._rootKey)
      this._muxer = new CorestoreMuxerTopic(this._corestore, this._root.key)
      this._muxer.on('feed', feed => {
        this._cache(feed)
      })
      this._root.ready(err => {
        if (err) return cb(err)
        this._loadFeeds(cb)
      })
    })
  }

  _close (cb) {
    const self = this
    const feeds = Array.from(this._feedsByKey.values())
    if (this._root) feeds.push(this._root)
    if (this._handlers.close) feeds.push(this._handlers)
    let pending = feeds.length + 1
    feeds.forEach(feed => feed.close(onclose))
    onclose()
    function onclose () {
      if (--pending !== 0) return
      self._feedsByKey = new Map()
      self._feedsByName = new Map()
      self._root = null
      cb()
    }
  }

  _cache (feed, name, save = false) {
    if (!name) name = String(this._feedsByKey.size)
    if (save) this._saveFeed(feed, name)
    this._feedsByName.set(name, feed)
    this._feedsByKey.set(feed.key.toString('hex'), feed)
    this._muxer.addFeed(feed.key)
    feed.setMaxListeners(Infinity)
    this.emit('feed', feed, name)
  }

  _saveFeed (feed, name) {
    if (this._feedsByKey.has(feed.key.toString('hex'))) return
    const info = { key: feed.key.toString('hex'), name }
    this._handler.saveFeed(info, err => {
      if (err) this.emit('error', err)
    })
  }

  _loadFeeds (cb) {
    this._handlers.loadFeeds((err, infos) => {
      if (err) return cb(err)
      for (const info of infos) {
        const feed = this._corestore.get(info.key)
        this._cache(feed, info.name, false)
      }
      cb()
    })
  }

  writer (name, opts, cb) {
    if (!this.opened) return this.ready(() => this.writer(name, opts, cb))
    if (typeof name === 'function' && !cb) {
      cb = name
      name = undefined
      opts = {}
    }
    if (typeof opts === 'function' && !cb) {
      cb = opts
      opts = {}
    }
    if (this._feedsByName.has(name)) return cb(null, this._feedsByName.get(name))
    if (opts.keypair) opts.keyPair = opts.keypair
    const feed = this._corestore.namespace(name).default(opts)
    feed.ready(() => {
      this._cache(feed, name)
      cb(null, feed)
    })
  }

  feeds () {
    return Array.from(this._feedsByKey.values())
  }

  feed (key) {
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    if (typeof key === 'string') return this._feedsByKey.get(key)
    else return null
  }

  replicate (isInitiator, opts = {}) {
    if (!this._root) {
      var tmp = through()
      process.nextTick(function () {
        tmp.emit('error', new Error('tried to use "replicate" before multifeed is ready'))
      })
      return tmp
    }

    const stream = opts.stream || new Protocol(isInitiator, opts)
    this._muxer.addStream(stream, opts)
    return stream
  }
}

function defaultCorestore (storage, opts) {
  if (isCorestore(storage)) return storage
  if (typeof storage === 'function') {
    var factory = path => storage(path)
  } else if (typeof storage === 'string') {
    factory = path => raf(storage + '/' + path)
  }
  return new Corestore(factory, opts)
}

function isCorestore (storage) {
  return storage.default && storage.get && storage.replicate && storage.close
}

function defaultPersistHandlers (corestore) {
  let feed
  return {
    loadFeeds (cb) {
      feed = corestore.namespace(LISTFEED_NAMESPACE).default({
        valueEncoding: 'json'
      })
      const rs = feed.createReadStream()
      collect(rs, cb)
    },

    saveFeed (info, cb) {
      feed.append(info, cb)
    },

    close (cb) {
      if (feed) feed.close(cb)
    }
  }
}
