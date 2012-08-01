var JsonDB = require('./index').JsonDB;
var async = require('async');
var sqlite3 = require('sqlite3');

var db;
var limit = 100;

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
    var sum = 0;

    async.until(
      function() { return val > limit; },
      function(ucb) { 
        var start = Date.now();
        db.save({ a: val, b: val, lat: 37.11111111111, lon: 44.44444444444 }, 'obj', function(err) {
          ucb(err);
          sum += Date.now() - start;
          val++;
        });
      },
      function(err) {
        console.log('avg save time', sum/(val - 1));
        callback(err);
      }
      );
  },
  function(callback) {
    console.log('finding without index ...');
    var count = 0;
    var sum = 0;
    async.until(
      function() { return count > limit; },
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
      function() { return count > limit; },
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



