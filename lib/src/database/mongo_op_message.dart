part of mongo_dart;

class MongoOpMessage extends MongoMessage {
  static final OPTS_CHECKSUM_PRESENT = 1 << 0;
  static final OPTS_MORE_TO_COME = 1 << 1;
  static final OPTS_EXHAUST_ALLOWED = 1 << 16;

  int flags = 0;
  List<Section> sections = [];

  MongoOpMessage.fromMessage(MongoMessage message) {
    opcode = MongoMessage.OpMsg;
    sections = message.toCommand();
    if (!(sections.first is MainSection))
      throw MongoDartError('${message.runtimeType} first section must be a MainSection');
    var mainPayload = (sections.first as MainSection).payload.data;
    mainPayload['\$db'] = message._dbName();
  }

  MongoOpMessage.fromResponse(BsonBinary buffer) {
    opcode = MongoMessage.OpMsg;
    deserialize(buffer);
  }

  @override
  List<Section> toCommand() => sections;

  MongoReplyMessage unpack() {
    var answer = MongoReplyMessage();
    answer.responseTo = responseTo;
    answer._requestId = _requestId;
    answer.responseFlags = 0;
    answer.documents = [];
    var mainPayload = (sections.first as MainSection).payload.data;
    if (mainPayload.containsKey('ok') && mainPayload['ok'] == 0)
      answer.responseFlags |= MongoReplyMessage.FLAGS_QUERY_FAILURE;
    sections.skip(1).forEach((sec) => (sec as PayloadSection).asMapElement(mainPayload));
    answer.documents.add(mainPayload);
    return answer;
  }

  @override
  int get messageLength => 16 + 4 + sections.fold<int>(0, (len, sec) => len += sec.byteLength);

  @override
  BsonBinary serialize() {
    BsonBinary buffer = BsonBinary(messageLength);
    writeMessageHeaderTo(buffer);
    buffer.writeInt(flags);
    sections.forEach((element) => element.serialize(buffer));
    buffer.offset = 0;
    return buffer;
  }

  @override
  void deserialize(BsonBinary buffer) {
    readMessageHeaderFrom(buffer);
    flags = buffer.readInt32();
    sections.add(MainSection.fromBuffer(buffer));
    while (buffer.byteList.length - buffer.offset > 4)
      sections.add(PayloadSection.fromBuffer(buffer));
  }

  @override
  String toString() => 'MongoOpMessage{${sections.length} sections, flags: $flags}';
}
