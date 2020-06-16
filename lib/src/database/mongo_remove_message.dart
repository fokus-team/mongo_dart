part of mongo_dart;

class MongoRemoveMessage extends MongoMessage {
  int flags;
  BsonMap _selector;

  MongoRemoveMessage(String collectionFullName,
      [Map<String, dynamic> selector = const {}, this.flags = 0]) {
    _collectionFullName = BsonCString(collectionFullName);
    _selector = BsonMap(selector);
    opcode = MongoMessage.Delete;
  }

  @override
  Map<String, dynamic> toCommand() {
	  return {
		  'delete': _collectionName(),
		  'deletes': [
			  {'q': _selector, 'limit': 0}
		  ]
	  };
  }

  int get messageLength {
    return 16 +
        4 +
        _collectionFullName.byteLength() +
        4 +
        _selector.byteLength();
  }

  BsonBinary serialize() {
    BsonBinary buffer = BsonBinary(messageLength);
    writeMessageHeaderTo(buffer);
    buffer.writeInt(0);
    _collectionFullName.packValue(buffer);
    buffer.writeInt(flags);
    _selector.packValue(buffer);
    buffer.offset = 0;
    return buffer;
  }

  String toString() {
    return "MongoRemoveMessage($requestId, ${_collectionFullName.value}, ${_selector.value})";
  }
}
