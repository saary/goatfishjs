# goatfishJS

goatfishJS is a straight translation of [goatfish](https://github.com/stochastic-technologies/goatfish) to node.js.

## In short
A simple JSON data store on top of sqlite db.

## Example

```javascript
var JsonDB = require('./index').JsonDB;
var async = require('async');

var db;

async.series([
  function(callback) {
    db = new JsonDB(':memory:', function(err) {
      console.log('db opened');
      callback(err);
    });
  },
  function(callback) {
    // create a store for 'obj' type object with an index for the field 'a'
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
    db.save({ a: 50, b: 60, lat: 37.11111111111, lon: 44.44444444444 }, 'obj', function(err) {
      console.log('saved');
      callback(err);
    });
  },
  function(callback) {
    console.log('find without index ...');
    db.find('obj', { b: 60 }, function(err, items) {
      if (items[0].b !== 60) throw new Error('item not found');
      callback(err);
    });
  },
  function(callback) {
    console.log('find with index ...');
    db.find('obj', { a: 50 }, function(err, items) {
      if (items[0].a !== 50) throw new Error('item not found');
      callback(err);
    });
  },
  function(callback) {
    db.close(function(err) {
      console.log('db closed');
      callback(err);
    });  
  },
]);
```

# Recognition
* Stavros Korokithakis - [goatfish](https://github.com/stochastic-technologies/goatfish)
* [node-sqlite3] (https://github.com/developmentseed/node-sqlite3)
* [async](https://github.com/caolan/async/)
* [msgpack](https://github.com/creationix/msgpack-js)
* [node-uuid](https://github.com/broofa/node-uuid)
 
# License
MIT


