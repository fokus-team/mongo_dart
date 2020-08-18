part of mongo_dart;

class MongoUpdateMessage extends MongoMessage {
  static final OPTS_UPSERT = 1 << 0;
  static final OPTS_MULTI_UPDATE = 1 << 1;

  int flags;
  int numberToSkip;
  int numberToReturn;
  List<BsonMap> _selectors = [];
  List<BsonMap> _documents = [];
  WriteConcern writeConcern;

  MongoUpdateMessage(String collectionFullName, List<Map<String, dynamic>> selectors,
		  List<dynamic> documents, this.flags, {this.writeConcern}) : assert(selectors.length == documents.length) {
    _collectionFullName = BsonCString(collectionFullName);
    for (var selector in selectors)
    	_selectors.add(BsonMap(selector));
    for (var document in documents) {
	    if (document is ModifierBuilder)
		    document = document.map;
	    _documents.add(BsonMap(document as Map<String, dynamic>));
    }
    opcode = MongoMessage.Update;
  }

  @override
  List<Section> toCommand() {
    Map<String, dynamic> command = {'update': _collectionName()};
    if (writeConcern != null)
      command['writeConcern'] = writeConcern.toCommand;

    List<BsonMap> payload = [];
    for (int i = 0; i < _documents.length; i++) {
	    Map<String, dynamic> update = {'q': _selectors[i], 'u': _documents[i]};
	    if (flags & OPTS_UPSERT > 0)
		    update['upsert'] = true;
	    if (flags & OPTS_MULTI_UPDATE > 0)
		    update['multi'] = true;
	    payload.add(BsonMap(update));
    }
    return [
      MainSection(BsonMap(command)),
      PayloadSection('updates', payload)
    ];
  }

  int get messageLength {
    var length = 16 + 4 + _collectionFullName.byteLength() + 4;
    for (var _doc in _documents)
	    length += _doc.byteLength();
    for (var _sel in _selectors)
	    length += _sel.byteLength();
    return length;
  }

  BsonBinary serialize() {
    BsonBinary buffer = BsonBinary(messageLength);
    writeMessageHeaderTo(buffer);
    buffer.writeInt(0);
    _collectionFullName.packValue(buffer);
    buffer.writeInt(flags);
    _selectors[0].packValue(buffer);
    _documents[0].packValue(buffer);
    buffer.offset = 0;
    return buffer;
  }

  String toString() {
    return "MongoUpdateMessage($requestId, ${_collectionFullName.value}, ${_documents.length} documents)";
  }
}
