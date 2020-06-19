part of mongo_dart;

class MongoUpdateMessage extends MongoMessage {
	static final OPTS_UPSERT = 1 << 0;
	static final OPTS_MULTI_UPDATE = 1 << 1;

  int flags;
  int numberToSkip;
  int numberToReturn;
  BsonMap _selector;
  BsonMap _document;
	WriteConcern writeConcern;

  MongoUpdateMessage(String collectionFullName, Map<String, dynamic> selector,
      document, this.flags, {this.writeConcern}) {
    _collectionFullName = BsonCString(collectionFullName);
    _selector = BsonMap(selector);
    if (document is ModifierBuilder) {
      document = document.map;
    }
    _document = BsonMap(document as Map<String, dynamic>);
    opcode = MongoMessage.Update;
  }

  @override
  List<Section> toCommand() {
	  Map<String, dynamic> command = {'update': _collectionName()};
	  if (writeConcern != null)
		  command['writeConcern'] = writeConcern.toCommand;
  	Map<String, dynamic> updates = {'q': _selector, 'u': _document};
	  if (flags & OPTS_UPSERT > 0)
		  updates['upsert'] = true;
	  if (flags & OPTS_MULTI_UPDATE > 0)
		  updates['multi'] = true;
	  return [
		  MainSection(BsonMap(command)),
		  PayloadSection('updates', [BsonMap(updates)])
	  ];
  }

  int get messageLength {
    return 16 +
        4 +
        _collectionFullName.byteLength() +
        4 +
        _selector.byteLength() +
        _document.byteLength();
  }

  BsonBinary serialize() {
    BsonBinary buffer = BsonBinary(messageLength);
    writeMessageHeaderTo(buffer);
    buffer.writeInt(0);
    _collectionFullName.packValue(buffer);
    buffer.writeInt(flags);
    _selector.packValue(buffer);
    _document.packValue(buffer);
    buffer.offset = 0;
    return buffer;
  }

  String toString() {
    return "MongoUpdateMessage($requestId, ${_collectionFullName.value}, ${_selector.value}, ${_document.value})";
  }
}
