part of mongo_dart;

class MongoDartError extends Error {
  final String message;
  MongoDartError(this.message);
  String toString() => "MongoDart Error: $message";
}

class MongoQueryTimeout implements Exception {
	@override
  String toString() => 'MongoDart timed out while waiting for database to reply';
}
