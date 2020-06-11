part of mongo_dart;

class MongoMessageHandler {
  final _log = Logger('MongoMessageTransformer');
  final converter = PacketConverter();

  void handleData(List<int> data, EventSink<MongoReplyMessage> sink) {
    converter.addPacket(data);
    while (!converter.messages.isEmpty) {
      var buffer = BsonBinary.from(converter.messages.removeFirst());
      MongoReplyMessage reply = _parseResponseMessage(buffer);
      _log.fine(() => reply.toString());
      sink.add(reply);
    }
  }

  static MongoReplyMessage _parseResponseMessage(BsonBinary buffer) {
	  buffer.offset = 12;
	  int opcodeFromWire = buffer.readInt32();
	  buffer.offset = 0;
	  if (opcodeFromWire == MongoMessage.OpMsg)
		  return MongoOpMessage.fromResponse(buffer).unpack();
	  if (opcodeFromWire == MongoMessage.Reply)
		  return MongoReplyMessage()..deserialize(buffer);
	  throw MongoDartError('Unexpected response message opcode $opcodeFromWire');
  }

  void handleDone(EventSink<MongoReplyMessage> sink) {
    if (!converter.isClear) {
      _log.warning(
          'Invalid state of PacketConverter in handleDone: $converter');
    }
    sink.close();
  }

  StreamTransformer<List<int>, MongoReplyMessage> get transformer =>
      StreamTransformer<List<int>, MongoReplyMessage>.fromHandlers(
          handleData: handleData, handleDone: handleDone);
}
