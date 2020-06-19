part of mongo_dart;

class MongoRemoveMessage extends MongoMessage {
	static final OPTS_SINGLE = 1 << 0;

  int flags;
  BsonMap _selector;
  WriteConcern writeConcern;

  MongoRemoveMessage(String collectionFullName,
      [Map<String, dynamic> selector = const {}, this.writeConcern, this.flags = 0]) {
    _collectionFullName = BsonCString(collectionFullName);
    _selector = BsonMap(selector);
    opcode = MongoMessage.Delete;
  }

  @override
  List<Section> toCommand() {
	  Map<String, dynamic> command = {'delete': _collectionName()};
	  if (writeConcern != null)
		  command['writeConcern'] = writeConcern.toCommand;
	  return [
		  MainSection(BsonMap(command)),
		  PayloadSection('deletes', [
			  BsonMap({'q': _selector, 'limit': (flags & OPTS_SINGLE > 0 ? 1 : 0)})
		  ])
	  ];
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
