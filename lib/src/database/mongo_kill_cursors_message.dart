part of mongo_dart;

class MongoKillCursorsMessage extends MongoMessage {
  int cursorId;

  MongoKillCursorsMessage(String collectionFullName, this.cursorId) {
    opcode = MongoMessage.KillCursors;
    _collectionFullName = BsonCString(collectionFullName);
  }

  int get messageLength {
    return 16 + 4 + 4 + 8;
  }

  BsonBinary serialize() {
    BsonBinary buffer = BsonBinary(messageLength);
    writeMessageHeaderTo(buffer);
    buffer.writeInt(0);
    buffer.writeInt(1);
    buffer.writeInt64(cursorId);
    buffer.offset = 0;
    return buffer;
  }

  String toString() {
    return "MongoKillCursorsMessage($requestId, $cursorId)";
  }
}
