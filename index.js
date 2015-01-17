"use strict";

var codec = require('js-git/lib/object-codec.js');
var bodec = require('bodec');
var inflate = require('js-git/lib/inflate');
var deflate = require('js-git/lib/deflate');

var sha1 = require('git-sha1');
var modes = require('js-git/lib/modes.js');

mixin.init = init;

mixin.loadAs = loadAs;
mixin.saveAs = saveAs;
mixin.loadRaw = loadRaw;
mixin.saveRaw = saveRaw;
module.exports = mixin;

function mixin(repo, repoName, mongoDb) {
  if (!repoName) throw new Error("Repository name required");
  if (!mongoDb) throw new Error("MongoDB object required");
  
  repo.mongoDb = mongoDb;
  repo.repoName = repoName;
  
  repo.saveAs = saveAs;
  repo.saveRaw = saveRaw;
  repo.loadAs = loadAs;
  repo.loadRaw = loadRaw;
  repo.readRef = readRef;
  repo.updateRef = updateRef;
  repo.hasHash = hasHash;
}

function init(callback) {
  this.objectsGrid = new Grid(this.db, "objects");
  this.objectsCollection = this.db.collection("objects.files");
  
  callback();
}

function saveAs(type, body, callback) {
  /*jshint: validthis: true */
  if (!callback) return saveAs.bind(this, type, body);
  var hash, buffer;
  try {
    buffer = codec.frame({type:type,body:body});
    hash = sha1(buffer);
  }
  catch (err) { return callback(err); }
  this.saveRaw(hash, buffer, callback);
}

function saveRaw(hash, buffer, callback) {
  /*jshint: validthis: true */
  if (!callback) return saveRaw.bind(this, hash, buffer);
  
  this.objectsGrid.put(buffer, { repoName: repoName, hash: hash }, function (err) {
    if (err) return callback(err);
    
    callback(null, hash);
  });
}

function loadAs(type, hash, callback) {
  /*jshint: validthis: true */
  if (!callback) return loadAs.bind(this, type, hash);
  loadRaw(hash, function (err, buffer) {
    if (!buffer) return callback(err);
    var parts, body;
    try {
      parts = codec.deframe(buffer);
      if (parts.type !== type) throw new Error("Type mismatch");
      body = codec.decoders[type](parts.body);
    }
    catch (err) {
      return callback(err);
    }
    callback(null, body);
  });
}

function loadRaw(hash, callback) {
  /*jshint: validthis: true */
  if (!callback) return loadRaw.bind(this, hash);
  
  this.objectsCollection.findOne({ 'metadata.hash': hash, 'metadata.repoName': this.repoName }, function (err, doc) {
    if (err) return callback(err);
    
    if (!doc) return callback(new Error('Invalid hash'));
    
    grid.get(doc._id, callback);
  });
}

function hasHash(type, hash, callback) {
  /*jshint: validthis: true */
  loadAs(type, hash, function (err, value) {
    if (err) return callback(err);
    if (value === undefined) return callback(null, false);
    if (type !== "tree") return callback(null, true);
    var names = Object.keys(value);
    next();
    function next() {
      if (!names.length) return callback(null, true);
      var name = names.pop();
      var entry = value[name];
      hasHash(modes.toType(entry.mode), entry.hash, function (err, has) {
        if (err) return callback(err);
        if (has) return next();
        callback(null, false);
      });
    }
  });
}

function readRef(ref, callback) {
  /*jshint: validthis: true */
  
  this.refsCollection.findOne({ repoName: this.repoName, ref: ref }, function (err, doc) {
    if (err) return callback(err);
    
    if (!doc) return callback(new Error('Invalid ref'));
    
    callback(null, doc.hash);
  });
}

function updateRef(ref, hash, callback) {
  /*jshint: validthis: true */
  
  this.refsCollection.update({ repoName: this.repoName, ref: ref }, { repoName: this.repoName, ref: ref, hash: hash }, { upsert: true, w: 1 }, callback);
}
