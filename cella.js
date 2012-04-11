var Cella = Cella || {};

Cella.db = function() {
    var dataStore;

    var Collection = function(name) {

        //private functions
        var addIndexes = function(collection, entity) {
            var indexes = Cella.db.system.indexes,
                parts, hashTable;

            for (var i = 0; i < indexes.length; i++) {
                parts = indexes[i].split(".");

                if (parts[1] === collection && parts[2] !== "_id") {
                    hashTable = dataStore.get(indexes[i]) || {};

                    if(typeof hashTable[entity[parts[2]]] == "undefined") {
                        hashTable[entity[parts[2]]] = [];
                    }
                    
                    hashTable[entity[parts[2]]].push(entity._id);

                    dataStore.set(indexes[i], hashTable);
                }
            }
        };

        this.ensureIndex = function(options) {
            var unique = false,
                index;

            for (var o in options) {
                if (o === "unique") {
                    unique = options[o];
                } else {
                    index = o;
                }
            }

            if (index) {
                var _indexes = dataStore.get("_indexes") || [];


                if (_indexes.indexOf("_index." + name + "." + index) == -1) {
                    _indexes.push("_index." + name + "." + index);
                    dataStore.set("_indexes", _indexes);
                    
                    // Update system.indexes
                    Cella.db.system.indexes.push("_index." + name + "." + index);
                }
            }

        };

        this.save = function(entity) {
            var primaryKeys = dataStore.get("_index." + name + "._id") || [];

            entity._id = primaryKeys.length > 0 ? primaryKeys[primaryKeys.length - 1] + 1 : 1;

            dataStore.set(name + "[" + entity._id + "]", entity);

            //Update id indexes
            primaryKeys.push(entity._id);
            dataStore.set("_index." + name + "._id", primaryKeys);

            addIndexes(name, entity);
        };

        this.find = function(criteria) {
            var result = [],
                index;

            for (var c in criteria) {
                if (c === "_id") {
                    result.push(dataStore.get(name + "[" + criteria[c] + "]"));
                } else {
                    index = dataStore.get("_index." + name + "." + c);
                    
                    if(index) {
                        for (var i = 0; i < index[criteria[c]].length; i++) {
                            result.push(dataStore.get(name + "[" + index[criteria[c]][i] + "]"));
                        }
                    }
                }
            }

            return result;
        };
    };

    var DataStore = function(storage) {

        this.set = function(key, value) {
            storage.setItem(key, JSON.stringify(value));
        };

        this.get = function(key) {
            return JSON.parse(storage.getItem(key));
        };
    };

    return {

        open: function(options) {
            dataStore = new DataStore(options.storage || localStorage);

            var _collections = dataStore.get("_collections") || [],
                _indexes = dataStore.get("_indexes") || [];

            for (var i = 0; i < _collections.length; i++) {
                this[_collections[i]] = new Collection(_collections[i]);
            }

            this.system = {
                indexes: _indexes,
                collections: _collections
            };
        },

        createCollection: function(name) {
            if (this.system.collections.indexOf(name) === -1) {

                this[name] = new Collection(name);

                // Update system.collections
                this.system.collections.push(name);
                dataStore.set("_collections", this.system.collections);

                // Update system.indexes
                this.system.indexes.push("_index." + name + "._id");
                dataStore.set("_indexes", this.system.indexes);
            }
        }
    };
}();