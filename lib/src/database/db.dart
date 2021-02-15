part of mongo_dart;

/// [WriteConcern] control the acknowledgment of write operations with various paramaters.
class WriteConcern {
  /// Denotes the Write Concern level that takes the following values ([int] or [String]):
  ///
  /// * -1 Disables all acknowledgment of write operations, and suppresses all errors, including network and socket errors.
  /// * 0: Disables basic acknowledgment of write operations, but returns information about socket exceptions and networking errors to the application.
  /// * 1: Provides acknowledgment of write operations on a standalone mongod or the primary in a replica set.
  /// * A number greater than 1: Guarantees that write operations have propagated successfully to the specified number of replica set members including the primary.
  /// * "majority": Confirms that write operations have propagated to the majority of configured replica set
  /// * A tag set: Fine-grained control over which replica set members must acknowledge a write operation
  final w;

  /// Specifies a timeout for this Write Concern in milliseconds, or infinite if equal to 0.
  final int wtimeout;

  /// Enables or disable fsync() operation before acknowledgement of the requested write operation.
  /// If [true], wait for mongod instance to write data to disk before returning.
  final bool fsync;

  /// Enables or disable journaling of the requested write operation before acknowledgement.
  /// If [true], wait for mongod instance to write data to the on-disk journal before returning.
  final bool j;

  /// Creates a WriteConcern object
  const WriteConcern({this.w, this.wtimeout, this.fsync, this.j});

  /// No exceptions are raised, even for network issues.
  static const ERRORS_IGNORED =
      WriteConcern(w: -1, wtimeout: 0, fsync: false, j: false);

  /// Write operations that use this write concern will return as soon as the message is written to the socket.
  /// Exceptions are raised for network issues, but not server errors.
  static const UNACKNOWLEDGED =
      WriteConcern(w: 0, wtimeout: 0, fsync: false, j: false);

  /// Write operations that use this write concern will wait for acknowledgement from the primary server before returning.
  /// Exceptions are raised for network issues, and server errors.
  static const ACKNOWLEDGED =
      WriteConcern(w: 1, wtimeout: 0, fsync: false, j: false);

  /// Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.
  static const REPLICA_ACKNOWLEDGED =
      WriteConcern(w: 2, wtimeout: 0, fsync: false, j: false);

  /// Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush
  /// the data to disk.
  static const FSYNCED = WriteConcern(w: 1, wtimeout: 0, fsync: true, j: false);

  /// Exceptions are raised for network issues, and server errors; the write operation waits for the server to
  /// group commit to the journal file on disk.
  static const JOURNALED =
      WriteConcern(w: 1, wtimeout: 0, fsync: false, j: true);

  /// Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation.
  static const MAJORITY =
      WriteConcern(w: "majority", wtimeout: 0, fsync: false, j: false);

  /// Gets the getlasterror command for this write concern.
  Map<String, dynamic> get lastErrorCommand {
    var map = Map<String, dynamic>();
    map["getlasterror"] = 1;
    map = _writeFields(map);
    if (fsync != null) {
      map["fsync"] = fsync;
    }
    return map;
  }

  Map<String, dynamic> get toCommand => _writeFields({});

  Map<String, dynamic> _writeFields(Map<String, dynamic> map) {
    if (w != null)
      map["w"] = w;
    if (wtimeout != null)
      map["wtimeout"] = wtimeout;
    if (j != null)
      map["j"] = j;
    return map;
  }
}

class _UriParameters {
  static const authMechanism = 'authMechanism';
  static const authSource = 'authSource';
  static const useSsl = 'ssl';
}

class Db {
  final MONGO_DEFAULT_PORT = 27017;
  final _log = Logger('MongoDart.Db');
  final List<String> _uriList = List<String>();

  State state = State.INIT;
  String databaseName;
  String _debugInfo;
  Db authSourceDb;
  _ConnectionManager _connectionManager;

  _Connection get _masterConnection => _connectionManager.masterConnection;

  _Connection get _masterConnectionVerified =>
      _connectionManager.masterConnectionVerified;
  WriteConcern _writeConcern;
  AuthenticationScheme _authenticationScheme;
  bool useLegacyErrorChecking;

  String toString() => 'Db($databaseName,$_debugInfo)';

  /// Db constructor expects [valid mongodb URI] (http://www.mongodb.org/display/DOCS/Connections).
  /// For example next code points to local mongodb server on default mongodb port, database *testdb*
  ///     var db = new Db('mongodb://127.0.0.1/testdb');
  /// And that code direct to MongoLab server on 37637 port, database *testdb*, username *dart*, password *test*
  ///     var db = new Db('mongodb://dart:test@ds037637-a.mongolab.com:37637/objectory_blog');

  Db(String uriString, [this._debugInfo]) {
    _uriList.add(uriString);
  }

  Db.pool(List<String> uriList, [this._debugInfo]) {
    _uriList.addAll(uriList);
  }

  Db._authDb(this.databaseName);

  ServerConfig _parseUri(String uriString) {
    var uri = Uri.parse(uriString);

    if (uri.scheme != 'mongodb') {
      throw MongoDartError('Invalid scheme in uri: $uriString ${uri.scheme}');
    }

    var serverConfig = ServerConfig();
    serverConfig.host = uri.host;
    serverConfig.port = uri.port;

    if (serverConfig.port == null || serverConfig.port == 0) {
      serverConfig.port = MONGO_DEFAULT_PORT;
    }

    if (uri.userInfo.isNotEmpty) {
      var userInfo = uri.userInfo.split(':');

      if (userInfo.length != 2) {
        throw MongoDartError('Invalid format of userInfo field: $uri.userInfo');
      }

      serverConfig.userName = Uri.decodeComponent(userInfo[0]);
      serverConfig.password = Uri.decodeComponent(userInfo[1]);
    }

    if (uri.path.isNotEmpty) {
      databaseName = uri.path.replaceAll('/', '');
    }

    uri.queryParameters.forEach((String queryParam, String value) {
	    if (queryParam == _UriParameters.authMechanism) {
		    selectAuthenticationMechanism(value);
	    }
	    if (queryParam == _UriParameters.useSsl) {
		    serverConfig.isSecure = queryParam == 'true' ? true : false;
	    }

      if (queryParam == _UriParameters.authSource) {
        authSourceDb = Db._authDb(value);
      }
    });

    return serverConfig;
  }

  void selectAuthenticationMechanism(String authenticationSchemeName) {
    if (authenticationSchemeName == ScramSha1Authenticator.name) {
      _authenticationScheme = AuthenticationScheme.SCRAM_SHA_1;
    } else if (authenticationSchemeName == MongoDbCRAuthenticator.name) {
      _authenticationScheme = AuthenticationScheme.MONGODB_CR;
    } else {
      throw MongoDartError(
          "Provided authentication scheme is not supported : $authenticationSchemeName");
    }
  }

  DbCollection collection(String collectionName) {
    return DbCollection(this, collectionName);
  }

  Future<MongoReplyMessage> queryMessage(MongoMessage queryMessage,
      {_Connection connection}) {
    return Future.sync(() {
      if (state != State.OPEN) {
        throw MongoDartError('Db is in the wrong state: $state');
      }

      if (connection == null) {
        connection = _masterConnectionVerified;
      }

      return connection.query(_getMessageToSend(queryMessage));
    });
  }

  executeMessage(MongoMessage message, WriteConcern writeConcern,
      {_Connection connection}) {
    if (state != State.OPEN) {
      throw MongoDartError('DB is not open. $state');
    }

    if (connection == null) {
      connection = _masterConnectionVerified;
    }

    if (writeConcern == null) {
      writeConcern = _writeConcern;
    }

    connection.execute(_getMessageToSend(message), writeConcern == WriteConcern.ERRORS_IGNORED);
  }

  Future<Response> executeWithAcknowledgement(MongoMessage message, [WriteConcern writeConcern]) {
    if (writeConcern == null)
      writeConcern = _writeConcern;

    if (writeConcern == WriteConcern.ERRORS_IGNORED) {
      executeMessage(message, writeConcern);
      return Future.value(Response(success: true));
    }
    if (!useLegacyErrorChecking && _masterConnection.serverCapabilities.opMsg)
      return queryMessage(message).then((response) => _parseCommandResponse(response, (doc) => Response.fromCommand(doc)));
    executeMessage(message, writeConcern);
    return getLastError(writeConcern);
  }

  /// ## [useLegacyErrorChecking]
  /// when calling newer commands like insert, update and delete that return their result with errors rely on the legacy getLastError command as a response
  Future open({WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED,
	  bool useLegacyErrorChecking = false, bool secure = false, TimeoutConfig timeoutConfig}) {
    this.useLegacyErrorChecking = useLegacyErrorChecking;
    return Future.sync(() {
      if (state == State.OPENING) {
        throw MongoDartError('Attempt to open db in state $state');
      }

      state = State.OPENING;
      _writeConcern = writeConcern;
      _connectionManager = _ConnectionManager(this, timeoutConfig: timeoutConfig);

      _uriList.forEach((uri) {
        _connectionManager.addConnection(_parseUri(uri));
      });

      return _connectionManager.open(writeConcern);
    });
  }

  Future<Response> executeDbCommand(MongoMessage message,
      {_Connection connection}) async {
    if (connection == null) {
      connection = _masterConnectionVerified;
    }
    var replyMessage = await connection.query(_getMessageToSend(message));
    return _parseCommandResponse(replyMessage, (doc) => Response.fromMessage(doc));
  }

  Future<Response> _parseCommandResponse(MongoReplyMessage replyMessage, Response Function(Map<String, dynamic>) getResponse) {
    Completer<Response> result = Completer();

    if (replyMessage.documents.isEmpty) {
      var errorMessage = "Error executing command, documents are empty $replyMessage";
      print("Error: $errorMessage");

      result.completeError(Response.fromError(RequestError(errorMessage)));
    } else {
      var response = getResponse(replyMessage.documents[0]);
      if (response.errors.isEmpty)
        result.complete(response);
      else
        result.completeError(response.errors[0]);
    }
    return result.future;
  }

  Future<bool> dropCollection(String collectionName) async {
    var collectionInfos = await getCollectionInfos({'name': collectionName});

    if (collectionInfos.length == 1) {
      return executeDbCommand(
              DbCommand.createDropCollectionCommand(this, collectionName))
          .then((_) => true);
    }

    return true;
  }

  ///   Drop current database
  Future drop() {
    return executeDbCommand(DbCommand.createDropDatabaseCommand(this));
  }

  Future<Response> removeFromCollection(String collectionName,
      [Map<String, dynamic> selector = const {}, WriteConcern writeConcern]) {
    return executeWithAcknowledgement(MongoRemoveMessage("$databaseName.$collectionName", selector, writeConcern), writeConcern);
  }

  Future<Response> getLastError([WriteConcern writeConcern]) {
    if (writeConcern == null) {
      writeConcern = _writeConcern;
    }
    return executeDbCommand(
        DbCommand.createGetLastErrorCommand(this, writeConcern));
  }

  Future<Response> getNonce({_Connection connection}) {
    return executeDbCommand(DbCommand.createGetNonceCommand(this),
        connection: connection);
  }

  Future<Response> getBuildInfo({_Connection connection}) {
    return executeDbCommand(DbCommand.createBuildInfoCommand(this),
        connection: connection);
  }

  Future<Response> isMaster({_Connection connection}) {
    return executeDbCommand(DbCommand.createIsMasterCommand(this),
        connection: connection);
  }

  Future<Response> wait() {
    return getLastError();
  }

  Future close() {
  	if (_connectionManager == null)
  		return Future.value();
    _log.fine(() => '$this closed');
    state = State.CLOSED;
    var closeFuture = _connectionManager.close();
    _connectionManager = null;
    return closeFuture;
  }

  /// Analogue to shell's `show dbs`. Helper for `listDatabases` mongodb command.
  Future<List> listDatabases() async {
    var commandResult = await executeDbCommand(
        DbCommand.createQueryAdminCommand({"listDatabases": 1}));

    var result = [];

    for (var each in commandResult["databases"]) {
      result.add(each["name"]);
    }

    return result;
  }

  MongoMessage _getMessageToSend(MongoMessage message) {
    // OP_MSG can be used only after completed handshake
    if (_masterConnection.serverCapabilities.opMsg && state != State.INIT && state != State.OPENING)
      message = MongoOpMessage.fromMessage(message);
    return message;
  }

  Stream<Map<String, dynamic>> _listCollectionsCursor(
      [Map<String, dynamic> filter = const {}]) {
    if (this._masterConnection.serverCapabilities.listCollections) {
      return ListCollectionsCursor(this, filter).stream;
    } else {
      // Using system collections (pre v3.0 API)
      Map<String, dynamic> selector = {};
      // If we are limiting the access to a specific collection name
      if (filter.containsKey('name')) {
        selector["name"] = "${this.databaseName}.${filter['name']}";
      }
      return Cursor(
              this,
              DbCollection(this, DbCommand.SYSTEM_NAMESPACE_COLLECTION),
              selector)
          .stream;
    }
  }

  /// This method uses system collections and therefore do not work on MongoDB v3.0 with and upward
  /// with WiredTiger
  /// Use `getCollectionInfos` instead
  @deprecated
  Stream<Map<String, dynamic>> collectionsInfoCursor([String collectionName]) {
    return _collectionsInfoCursor(collectionName);
  }

  Stream<Map<String, dynamic>> _collectionsInfoCursor([String collectionName]) {
    Map<String, dynamic> selector = {};
    // If we are limiting the access to a specific collection name
    if (collectionName != null) {
      selector["name"] = "${this.databaseName}.$collectionName";
    }
    // Return Cursor
    return Cursor(this,
            DbCollection(this, DbCommand.SYSTEM_NAMESPACE_COLLECTION), selector)
        .stream;
  }

  /// Analogue to shell's `show collections`
  /// This method uses system collections and therefore do not work on MongoDB v3.0 with and upward
  /// with WiredTiger
  /// Use `getCollectionNames` instead
  @deprecated
  Future<List<String>> listCollections() {
    return _collectionsInfoCursor()
        .map((map) => map['name']?.toString()?.split('.'))
        .where((arr) => arr.length == 2)
        .map((arr) => arr.last)
        .toList();
  }

  Future<List<Map<String, dynamic>>> getCollectionInfos(
      [Map<String, dynamic> filter = const {}]) {
    return _listCollectionsCursor(filter).toList();
  }

  Future<List<String>> getCollectionNames(
      [Map<String, dynamic> filter = const {}]) {
    return _listCollectionsCursor(filter)
        .map((map) => map['name']?.toString())
        .toList();
  }

  Future<bool> authenticate(String userName, String password,
      {_Connection connection}) async {
    var credential = UsernamePasswordCredential()
      ..username = userName
      ..password = password;

    var authenticator =
        createAuthenticator(_authenticationScheme, this, credential);

    await authenticator.authenticate(connection ?? _masterConnection);

    return true;
  }

  /// This method uses system collections and therefore do not work on MongoDB v3.0 with and upward
  /// with WiredTiger
  /// Use `DbCollection.getIndexes()` instead
  @deprecated
  Future<List> indexInformation([String collectionName]) {
    var selector = {};

    if (collectionName != null) {
      selector['ns'] = '$databaseName.$collectionName';
    }

    return Cursor(this, DbCollection(this, DbCommand.SYSTEM_INDEX_COLLECTION),
            selector)
        .stream
        .toList();
  }

  String _createIndexName(Map<String, dynamic> keys) {
    var name = '';

    keys.forEach((key, value) {
      name = '${name}_${key}_$value';
    });

    return name;
  }

  Future<Response> createIndex(String collectionName,
      {String key, Map<String, dynamic> keys,
      bool unique, bool sparse, bool background, bool dropDups,
      Map<String, dynamic> partialFilterExpression,
      String name, WriteConcern writeConcern}) {
    return Future.sync(() async {
      var selector = <String, dynamic>{};
      keys = _setKeys(key, keys);
      selector['key'] = keys;
      if (name == null) {
        name = _createIndexName(keys);
      }
      selector['name'] = name;
      selector['unique'] = unique;
      if (sparse == true) {
        selector['sparse'] = true;
      }
      if (background == true) {
        selector['background'] = true;
      }
      if (partialFilterExpression != null) {
        selector['partialFilterExpression'] = partialFilterExpression;
      }
      MongoMessage message;
      if (_masterConnection.serverCapabilities.indexesCommands) {
        var command = {
          'createIndexes': collectionName,
          'indexes': [selector]
        };
        if (writeConcern != null)
          command['writeConcern'] = writeConcern.toCommand;
        message = DbCommand.createIndexCommand(this, collectionName, command);
      } else {
        selector['ns'] = '$databaseName.$collectionName';
        if (dropDups == true) {
          selector['dropDups'] = true;
        }
        message = MongoInsertMessage(
            '$databaseName.${DbCommand.SYSTEM_INDEX_COLLECTION}', [selector]);
      }
      await executeMessage(message, _writeConcern);
      return getLastError();
    });
  }

  /// Removes indexes from collection
  /// ##[name]
  /// Name of the index to remove, specify * to remove all but the default _id index
  Future<Response> removeIndex(String collectionName, {String name, WriteConcern writeConcern}) {
    Map<String, dynamic> command = {
      'dropIndexes': collectionName,
      'index': name
    };
    if (writeConcern != null)
      command['writeConcern'] = writeConcern.toCommand;
    return Future.sync(() {
      executeMessage(DbCommand.createIndexCommand(this, collectionName, command), writeConcern);
      return getLastError();
    });
  }

  Map<String, dynamic> _setKeys(String key, Map<String, dynamic> keys) {
    if (key != null && keys != null) {
      throw ArgumentError('Only one parameter must be set: key or keys');
    }

    if (key != null) {
      keys = Map();
      keys['$key'] = 1;
    }

    if (keys == null) {
      throw ArgumentError('key or keys parameter must be set');
    }

    return keys;
  }

  Future<Response> ensureIndex(String collectionName,
      {String key,
      Map<String, dynamic> keys,
      bool unique,
      bool sparse,
      bool background,
      bool dropDups,
      Map<String, dynamic> partialFilterExpression,
      String name}) async {
    keys = _setKeys(key, keys);
    var indexInfos = await collection(collectionName).getIndexes();

    if (name == null) {
      name = _createIndexName(keys);
    }

    if (indexInfos.any((info) => info['name'] == name)) {
      return Response(success: true);
    }

    var createdIndex = await createIndex(collectionName,
        keys: keys,
        unique: unique,
        sparse: sparse,
        background: background,
        dropDups: dropDups,
        partialFilterExpression: partialFilterExpression,
        name: name);

    return createdIndex;
  }
}
