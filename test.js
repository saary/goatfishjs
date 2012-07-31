var JsonDB = require('./index').JsonDB;
var async = require('async');
var sqlite3 = require('sqlite3');

setTimeout(function() {}, 2000);

var db;
async.series([
  function(callback) {
    db = new JsonDB('./test.db', function(err) {
      console.log('db opened');
      callback(err);
    });
  },
  function(callback) {
    db.initialize('obj', [['a']], function(err) {
      if (err) {
        console.error('Error initializing db', err);
      }
      else {
        console.log('done initializing');
      }
      callback(err);
    });
  },
  function(callback) {
    console.log('saving ...');
    var val = 1;

    async.until(
      function() { return val > 10000; },
      function(ucb) { 
        db.save({ a: val, b: val}, 'obj', ucb);
        console.log('saved', val);
        val++;
      },
      callback 
      );
  },
  function(callback) {
    console.log('finding without index ...');
    var count = 0;
    var sum = 0;
    async.until(
      function() { return count > 10000; },
      function(ucb) {
        var start = Date.now();
        db.find('obj', { b: 50 }, function(err, items) {
          if (items[0].b !== 50) throw new Error('item not found');
          sum += Date.now() - start;
          ucb(err);
        });
        count++;
      },
      function(err) {
        console.log('avg find time', sum/count);
        callback(err);
      }
      );
  },
  function(callback) {
    console.log('finding with index ...');
    var count = 0;
    var sum = 0;
    async.until(
      function() { return count > 10000; },
      function(ucb) {
        var start = Date.now();
        db.find('obj', { a: 50 }, function(err, items) {
          if (items[0].a !== 50) throw new Error('item not found');
          sum += Date.now() - start;
          ucb(err);
        });
        count++;
      },
      function(err) {
        console.log('avg find time', sum/count);
        callback(err);
      }
      );
  },
  function(callback) {
    db.close(function(err) {
      console.log('db closed', err);
      callback(err);
    });  
  },
], function(err) {
  console.log('error', err);
});



