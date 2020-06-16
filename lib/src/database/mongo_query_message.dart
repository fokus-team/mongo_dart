part of mongo_dart;

class MongoQueryMessage extends MongoMessage {
  static final OPTS_NONE = 0;
  static final OPTS_TAILABLE_CURSOR = 1 << 1;
  static final OPTS_SLAVE = 1 << 2;
  static final OPTS_OPLOG_REPLY = 1 << 3;
  static final OPTS_NO_CURSOR_TIMEOUT = 1 << 4;
  static final OPTS_AWAIT_DATA = 1 << 5;
  static final OPTS_EXHAUST = 1 << 6;
  static final OPTS_PARTIAL = 1 << 7;

  int flags;
  int numberToSkip;
  int numberToReturn;
  BsonMap _query;
  BsonMap _fields;
  BsonCString get collectionNameBson => _collectionFullName;

  MongoQueryMessage(
      String collectionFullName,
      this.flags,
      this.numberToSkip,
      this.numberToReturn,
      Map<String, dynamic> query,
      Map<String, dynamic> fields) {
    _collectionFullName = BsonCString(collectionFullName);
    _query = BsonMap(query);
    if (fields != null) {
      _fields = BsonMap(fields);
    }
    opcode = MongoMessage.Query;
  }

  int get messageLength {
    int result =
        16 + 4 + _collectionFullName.byteLength() + 4 + 4 + _query.byteLength();
    if (_fields != null) {
      result += _fields.byteLength();
    }
    return result;
  }

  BsonBinary serialize() {
    BsonBinary buffer = BsonBinary(messageLength);
    writeMessageHeaderTo(buffer);
    buffer.writeInt(flags);
    _collectionFullName.packValue(buffer);
    buffer.writeInt(numberToSkip);
    buffer.writeInt(numberToReturn);
    _query.packValue(buffer);
    if (_fields != null) {
      _fields.packValue(buffer);
    }
    buffer.offset = 0;
    return buffer;
  }

  String toString() {
    return "MongoQueryMessage($requestId, ${_collectionFullName.value},numberToReturn:$numberToReturn, ${_query.value})";
  }
}
