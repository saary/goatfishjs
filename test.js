var JsonDB = require('./index').JsonDB;
var async = require('async');
var sqlite3 = require('sqlite3');

setTimeout(function() {}, 2000);

var step = 0;
var db = new JsonDB(':memory:', function() { 
  console.log('db created');
  async.series([
    // function(callback) {
    //   step++;
    //   db = new JsonDB('./test.db', function(err) {
    //     console.log('db opened');
    //     callback(err);
    //   });
    // },
    function(callback) {
      step++;
      db.initialize('obj', function(err) {
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
      step++;
      db.save({ a: 1, b: 2}, 'obj', callback);
    },
    function(callback) {
      step++;
      db.find('obj', { a: 1 }, function(err, items) {
        console.log('ITEMS');
        console.log(items);

        callback(err);
      });
    },
    function(callback) {
      step++;
      db.close(function(err) {
        console.log('db closed', err);
        callback(err);
      });  
    },
  ], function(err) {
    console.log('error in step', step, err);
  });
});



