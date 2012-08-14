var sqlite3 = require('sqlite3').verbose();
var uuid = require('node-uuid');
var sets = require('simplesets');
var util = require('util');
var async = require('async');
var msgpack = require('msgpack');

var findMin = function(list, iterator) {
  var m = Number.MAX_VALUE;
  var minItem;

  iterator = iterator || function(n) { return n; };

  list.forEach(function(item) {
    var num = iterator(item);
    if (num !== Number.NaN && num < m) { 
      m = num;
      minItem = item;
    }
  });

  return minItem;
}

var noop = function() {};

var JSONSerializer = {
  serialize : function(obj) {
    return JSON.stringify(obj);
  },
  deserialize : function(buffer) {
    return JSON.parse(buffer);
  }
};

var MsgPackSerializer = {
  serialize : function(obj) {
    return msgpack.encode(obj);
  },
  deserialize : function(buffer) {
    return msgpack.decode(buffer);
  }
};

var JsonDB = function(filename, callback) {
  this._serializer = MsgPackSerializer;
  this.indexes = {};
  this.db = new sqlite3.Database(filename, sqlite3.OPEN_CREATE | sqlite3.OPEN_READWRITE, callback);

  var self = this;

  // Get the names for the index tables in this model.
  
  // @returns
  // {index_name: [index,]} - A tuple of table and field names.
  function _get_index_table_names(type) {
    var indexTables = {};
    self.indexes[type].forEach(function(index) {
      var tableName = type + '_';
      tableName += index.join('_');
      indexTables[tableName] = index;
    });

    return indexTables;
  }

  // Return the largest index that can serve a query on these fields.
  function _get_largest_index(type, fields) {
    // Turn the attributes and indexes into sets.
    var field_set = new sets.Set(fields);
    var indexes = self.indexes[type].map(function(index) { return [new sets.Set(index), index];});

    // Compare each index set to the field set only if it contains all parameters (super set).
    // XXX: If the database can use partial indexes, we might not need to only
    // select subsets of the parameters here.

    var min = findMin(indexes, function(index) {
      if (field_set.issuperset(index[0])) {
        return field_set.difference(index[0]).size();
      }

      return Number.NaN;      
    });

    if (!min) {
      return [];
    }

    // Return the index that has the most fields in common with the query.
    return min[1]
  }

  function _serialize(cb) {
    if (!self.db) throw new Error('Cannot proceed without a database connection.');

    return self.db.serialize(cb);
  }

  function _getOperators(parameters) {
    var operators = {};
    for (var key in parameters) {
      var results = /(.*)(<|<=|>|>=|~=)$/ig.exec(key);
      var parameter = key;
      var operator = '=';
      if (results) {
        parameter = results[1];
        operator = results[2];
      }

      operators[parameter] = { op: operator, val: parameters[key] };
    }

    return operators;
  }

  function _testOperator(operator, val) {
    if (operator.op === '=') return val == operator.val;
    if (operator.op === '<') return val < operator.val;
    if (operator.op === '<=') return val <= operator.val;
    if (operator.op === '>') return val > operator.val;
    if (operator.op === '>=') return val >= operator.val;
    if (operator.op === '~=') return val.indexOf(operator.val) >= 0;

    return false;
  }

  function _buildWherePart(operators, index) {
    var wherePart = [];
    var bindings = [];

    index.forEach(function(field) {
      var operator = operators[field].op;
      if (operator === '~=') {
        operator = 'LIKE'
        bindings.push('%' + operators[field].val + '%');
      } 
      else {
        bindings.push(operators[field].val);
      }

      wherePart.push(util.format('%s %s ? AND', field, operator));
    });

    if (wherePart.length > 0) {
      var last = wherePart[index.length - 1];
      wherePart[index.length - 1] = last.substr(0, last.length - 4);
    }

    return [wherePart.join(' '), bindings];
  }

  self.find_one = function(type, parameters, cb) {
    // Return just one item.
    self.find(type, parameters, true, cb);
  } 

  self.find = function(type, parameters, first, cb) {
    if (!type) throw new Error('Missing type parameter');
    type = type.toLowerCase();

    // If we can use an index, we will offload some of the fields to query
    // to the database, and handle the rest here.
    if (typeof first === 'function') {
      cb = first;
      first = undefined;
    }

    if (typeof parameters === 'function') {
      cb = parameters;
      parameters = undefined;
    }

    parameters = parameters || {};
    cb = cb || noop;

    var queryMethod = first ? 'get' : 'all';
    var operators = _getOperators(parameters);
    var index = [];

    if (parameters.id) {
      index = ['id'];
    }
    else {
      index = _get_largest_index(type, Object.keys(operators));
    }

    var table_name = type;
    var statement;

    function findCallback(err, rows) {
      if (err) {
        return cb(err);
      }

      if (!util.isArray(rows)) rows = [rows];
      var items = [];
      rows.forEach(function(row) {
        var item = self._serializer.deserialize(row.data);
        item.__type = type;

        var allMatch = true;
        for (var field in operators) {
          if (!_testOperator(operators[field], item[field])) {
            allMatch = false;
          }
        }

        if (allMatch) {
          items.push(item);
        }
      });

      cb(null, items);
    }

    var action;

    if (index.length === 0) {
      // Look through every row.

      statement = util.format('SELECT * FROM %s;', table_name);
      action = function() {
        return self.db[queryMethod](statement, findCallback);
      };
    }
    else if (index.length === 1 && index[0] === 'id') {
      // If the object id is in the parameters, use only that, since it's
      // the fastest thing we can do.
      statement = util.format('SELECT * FROM %s WHERE uuid=?;', table_name);
      var id = parameters.id;
      delete parameters.id;

      action = function() {
        return self.db.get(statement, id, findCallback);
      };
    }
    else {
      var wherePart = _buildWherePart(operators, index);
      statement = util.format('SELECT x.id, x.uuid, x.data FROM %s x INNER JOIN %s y ON x.uuid = y.uuid WHERE %s;', 
        table_name,
        table_name + '_' + index.join('_'),
        wherePart[0]);

      // console.log('SQL', statement);

      // Delete the (now) unnecessary parameters, because the database
      // made sure they match.
      for (var field in index) {
        delete parameters[field];
      }
          
      action = function() {
        return self.db[queryMethod](statement, wherePart[1], findCallback);
      };
    }

    _serialize(action);
  };
  
  self.initialize = function(type, indexes, cb) {
    // Create the necessary tables in the database.
    if (!type) throw new Error('Missing type parameter');
    type = type.toLowerCase();

    cb = cb || noop;
    if (typeof indexes === 'function') {
      cb = indexes;
      indexes = undefined;
    }

    var actions = [];
    indexes = indexes || [];

    // store only flat indexes without the field type hints
    self.indexes[type] = indexes.map(function(index) {
      if (typeof index === 'string') return [index];
      if (typeof index === 'object') return Object.keys(index);
      if (util.isArray(index)) return index;

      return [];
    });

    var count = 0;
    var stopCondition = 2 * (1 + self.indexes[type].length);
    var errors = [];
    var checkIfDone = function(err) {
      if (err) errors.push(err);
      count++;
      if (count === stopCondition) {
        self.db.run("COMMIT");

        cb( errors.length > 0 ? errors : null);
      }
    }

    _serialize(function() {
      self.db.run("BEGIN");
      self.db.run(util.format('CREATE TABLE IF NOT EXISTS %s ( "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "uuid" TEXT NOT NULL, "data" BLOB NOT NULL);', type), checkIfDone);
      self.db.run(util.format('CREATE UNIQUE INDEX IF NOT EXISTS "%s_uuid_index" on %s (uuid ASC);', type, type), checkIfDone);

      indexes.forEach(function(index) {
        // Create an index table.
        var fields;

        if (typeof index === 'string') {
          fields = [ index ];
        }
        else if (typeof index === 'object') {
          fields = Object.keys(index);
        } 
        else if (util.isArray(index)) {
          fields = index;
        }
        else {
          return;
        }
        
        var table_name = util.format('%s_%s', type,  fields.join('_'));
        var statement = util.format('CREATE TABLE IF NOT EXISTS %s ( "uuid" TEXT NOT NULL', table_name);
        fields.forEach(function(field) {
          var fieldType = index[field] || '';
          if (['int', 'integer', 'long', 'date'].indexOf(fieldType.toLowerCase()) >= 0) {
            fieldType = 'INTEGER';
          }
          else if (['float', 'real'].indexOf(fieldType.toLowerCase()) >= 0) {
            fieldType = 'REAL';
          }
          else {
            fieldType = 'TEXT';
          }
          statement +=', "' + field + '" ' + fieldType;
        });
        statement += ' )';

        console.log('INDEX', statement);

        self.db.run(statement, checkIfDone);

        // Create the index table index.
        var fieldsPart = fields.join(' ASC, ');
        statement = util.format('CREATE INDEX IF NOT EXISTS "%s_index" on %s (%s ASC)',table_name, table_name, fieldsPart);
        self.db.run(statement, checkIfDone);
      });
    });
  };

  function _populate_index(obj, table_name, field_names) {
    // Get the values of the indexed attributes from the current object.
    var values = [];
    field_names.forEach(function(field_name) {
      // Abort if the attribute doesn't exist, we don't need to add it.
      // We check this way to make sure the attribute doesn't exist and is
      // set to None.
        if (!field_name in obj) {
          return
        }
        
        values.push(obj[field_name]);
    });

    values.unshift(obj.id);

    var qMarks = values.map(function() { return '?, '}).join('');
    qMarks = qMarks.substr(0, qMarks.length - 2);

    // Construct the SQL statement.
    var statement = util.format('INSERT OR REPLACE INTO %s ("uuid", "%s") VALUES (%s);', table_name, field_names.join('", "'), qMarks);

    return self.db.prepare(statement, values);
  }

  this.save = function(obj, type, cb) {
    if (typeof type === 'function') {
      cb = type;
      type = undefined;
    }

    type = type || obj.__type;
    delete obj.__type;

    if (!type) throw new Error('Missing type parameter');

    type = type.toLowerCase();

    // Persist an object to the database.
    var object_id;
    var statement;
    var actions = [];

    if (!obj.id) {
      object_id =  uuid.v4();
      statement = util.format('INSERT INTO %s ("uuid", "data") VALUES (?, ?)', type);
      actions.push(
        self.db.prepare(statement, [object_id, self._serializer.serialize(obj)])
        );
    }
    else {
      // Temporarily delete the id so it doesn't get stored.
      object_id = obj.id;
      delete obj.id;

      statement = util.format('UPDATE %s SET "data" = ? WHERE "uuid" = ?', type);
      actions.push(
        self.db.prepare(statement, [self._serializer.serialize(obj), object_id])
        );
    }

    // Restore the id.
    obj.id = object_id;

    // Insert into all indexes:
    var indexTables = _get_index_table_names(type);
    var field_names = [];

    for (table_name in indexTables) {
      field_names = indexTables[table_name];
      actions.push(
        _populate_index(obj, table_name, field_names, callback)
        );
    }
    
    var functions = actions.map(function(action) { return function(callback) { action.run(callback)};});
    async.parallel(functions, cb);
  };
    
  self.delete = function(obj, type, cb) {
    if (typeof type === 'function') {
      cb = type;
      type = undefined;
    }

    type = type || obj.__type;
    delete obj.__type;

    if (!type) throw new Error('Missing type parameter');

    type = type.toLowerCase();

    // Get the name of the main table.
    var table_names = [type];

    // The names of all the index tables.
    var indexTables = Object.keys(_get_index_table_names(type));

    indexTables.forEach(function(indexTable) { table_names.push(indexTable); });

    var actions = [];

    // And delete the rows from all of them.
    table_names.forEach(function(table_name) {
      var statement = util.format('DELETE FROM %s WHERE "uuid" == ?', table_name);
      actions.push(
        self.prepare.run(statement, [self.id])
        );
    });

    var functions = actions.map(function(action) { return function(callback) {action.run(callback)};});
    async.parallel(functions, cb);
  };
  
  self.close = function(cb) {
    _serialize(function() {
      self.db.close(cb);
    });
  };
};

exports.JsonDB = JsonDB;

