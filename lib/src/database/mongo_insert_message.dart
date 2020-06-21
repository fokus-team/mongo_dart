part of mongo_dart;

class MongoInsertMessage extends MongoMessage {
  int flags;
  List<BsonMap> _documents;
  WriteConcern writeConcern;

  MongoInsertMessage(
      String collectionFullName, List<Map<String, dynamic>> documents,
      {this.flags = 0, this.writeConcern}) {
    _collectionFullName = BsonCString(collectionFullName);
    _documents = List();
    for (var document in documents) {
      _documents.add(BsonMap(document));
    }
    opcode = MongoMessage.Insert;
  }

  @override
  List<Section> toCommand() {
    Map<String, dynamic> command = {'insert': _collectionName()};
    if (writeConcern != null)
      command['writeConcern'] = writeConcern.toCommand;
    return [
      MainSection(BsonMap(command)),
      PayloadSection('documents', _documents)
    ];
  }

  int get messageLength {
    int docsSize = 0;
    for (var _doc in _documents) {
      docsSize += _doc.byteLength();
    }
    int result = 16 + 4 + _collectionFullName.byteLength() + docsSize;
    return result;
  }

  BsonBinary serialize() {
    BsonBinary buffer = BsonBinary(messageLength);
    writeMessageHeaderTo(buffer);
    buffer.writeInt(flags);
    _collectionFullName.packValue(buffer);
    for (var _doc in _documents) {
      _doc.packValue(buffer);
    }
    buffer.offset = 0;
    return buffer;
  }

  String toString() {
    if (_documents.length == 1) {
      return "MongoInsertMessage($requestId, ${_collectionFullName.value}, ${_documents[0].value})";
    }
    return "MongoInsertMessage($requestId, ${_collectionFullName.value}, ${_documents.length} documents)";
  }
}
