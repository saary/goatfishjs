var sqlite3 = require('sqlite3');
var uuid = require('uuid');
var sets = require('simplesets');
var util = require('util');

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

var JsonDB = function(filename, callback) {
  this._serializer = JSON;
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
        return field_set.difference(index[0]).length;
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

  self.find_one = function(type, parameters, cb) {
    // Return just one item.
    self.find(type, parameters, cb, true);
  } 

  self.find = function(type, parameters, cb, first) {
    type = type.toLowerCase();

    var queryMethod = first ? 'get' : 'all';

    // If we can use an index, we will offload some of the fields to query
    // to the database, and handle the rest here.
    parameters = parameters || {};
    cb = cb || noop;

    var index = [];

    if (parameters.id) {
      index = ['id'];
    }
    else {
      index = _get_largest_index(type, Object.keys(parameters));
    }

    var table_name = type;
    var statement;

    function findCallback(err, rows) {
      if (err) {
        return cb(err);
      }

      if (!util.isArray(rows)) rows = [rows];
      var items = [];
      rows.forEach(function(row)) {
        var item = self._serializer.parse(row.data);
        var allMatch = true;
        for (var field in parameters) {
          if (item[field] !== parameters[field]) {
            allMatch = false;
          }
        }

        if (allMatch) {
          items.push(item);
        }
      });

      cb(null, items);
    }
    for id, uuid, data in cursor:
        loaded_dict = cls._serializer.loads(data.encode("utf-8"))
        loaded_dict["id"] = uuid

        if parameters:
            # If there are fields left to match, match them.
            if all((loaded_dict.get(field, None) == parameters[field]) for field in parameters):
                yield cls._unmarshal(loaded_dict)
        else:
            # Otherwise, just return the object.
            yield cls._unmarshal(loaded_dict)


    if (!index) {
      // Look through every row.
      statement = util.format('SELECT * FROM %s;', table_name);
      return self.db.run(statement, findCallback);
    }
    else if (index.length === 1 && index[0] === 'id') {
      // If the object id is in the parameters, use only that, since it's
      // the fastest thing we can do.
      statement = util.format('SELECT * FROM %s WHERE uuid=?;', table_name);
      var id = parameters.id;
      delete parameters.id;
      return self.db.get(statement, id, findCallback);
    }
    else {
      statement = util.format('SELECT x.id, x.uuid, x.data FROM %s x INNER JOIN %s y ON x.uuid = y.uuid WHERE %s;', 
        table_name,
        table_name + '_' + index.join('_'),
        index.join(' = ? AND ') + ' = ?');

      var paramBindings = index.map(function(field) { return parameters[field]});
      // Delete the (now) unnecessary parameters, because the database
      // made sure they match.
      for (var field in index) {
        delete parameters[field];
      }
          
      return self.db[queryMethod](statement, paramBindings, findCallback);
    }
  };
  
  self.initialize = function(type) {
    // Create the necessary tables in the database.
    type = type.toLowerCase();

    var cursor = _get_cursor();
    cursor.execute(util.format('CREATE TABLE IF NOT EXISTS %s ( "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "uuid" TEXT NOT NULL, "data" BLOB NOT NULL);', type));
    cursor.execute(util.format('CREATE UNIQUE INDEX IF NOT EXISTS "%s_uuid_index" on %s (uuid ASC);', type, type));

    if (!self.indexes[type]) self.indexes[type] = [];

    self.indexes[type].forEach(function(index) {
      // Create an index table.
      var table_name = util.format('%s_%s', type,  index.join('_'));
      var statement = 'CREATE TABLE IF NOT EXISTS %s ( "uuid" TEXT NOT NULL '+ table_name;
      index.forEach(function(field) {
        statement +=', "' + field + '" TEXT';
      });
      statement += ')';

      cursor.execute(statement);

      // Create the index table index.
      var fields = index.join(' ASC, ');
      statement = util.format('CREATE INDEX IF NOT EXISTS "%s_index" on %s (%s ASC)',table_name, table_name, fields);
      cursor.execute(statement);
    });
  };

  function _populate_index(obj, cursor, table_name, field_names) {
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

    var qMarks = field_names.map(function() { return '?, '}).join('');
    qMarks.substr(0, qMarks.length - 2);

    // Construct the SQL statement.
    var statement = 'INSERT OR REPLACE INTO %s ("uuid", "%s") VALUES (%s);', table_name, field_names.join('", "'), qMarks);

    cursor.execute(statement, values);
  }

  this.save = function(obj, type, commit) {
    type = type || obj.__type;
    delete obj.__type;

    if (!type) throw new Error('Missing type parameter');

    commit = commit === undefined ? true : commit;

    type = type.toLowerCase();

    // Persist an object to the database.
    var cursor = _get_cursor();

    var object_id;
    var statement;

    if (!obj.id) {
      object_id =  uuid.v4();
      statement = util.format('INSERT INTO %s ("uuid", "data") VALUES (?, ?)', type);
      cursor.execute(statement, [object_id, self._serializer.stringify(obj)))
    }
    else {
      // Temporarily delete the id so it doesn't get stored.
      object_id = obj.id;
      delete obj.id;

      statement = util.format('UPDATE %s SET "data" = ? WHERE "uuid" = ?', type);
      cursor.execute(statement, [self._serializer.stringify(obj), object_id]);
    }

    // Restore the id.
    obj.id = object_id;

    // Insert into all indexes:
    var indexTables = _get_index_table_names(type);
    var field_names = [];

    for (table_name in indexTables) {
      field_names = indexTables[table_name];
    }
    
    _populate_index(cursor, table_name, field_names);

    if (commit) self.commit();
  };
    
  self.delete = function(obj, type, commit) {
    type = type || obj.__type;
    delete obj.__type;

    if (!type) throw new Error('Missing type parameter');

    commit = commit === undefined ? true : commit;

    type = type.toLowerCase();

    // Delete an object from the database.
    var cursor = _get_cursor();

    // Get the name of the main table.
    var table_names = [type];

    // The names of all the index tables.
    var indexTables = Object.keys(_get_index_table_names(type));

    indexTables.forEach(function(indexTable) { table_names.push(indexTable); });

    // And delete the rows from all of them.
    table_names.forEach(function(table_name) {
      var statement = util.format('DELETE FROM %s WHERE "uuid" == ?', table_name);
      cursor.execute(statement, [self.id]);
    });

    if (commit) self.commit();
  };
};