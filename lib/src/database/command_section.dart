part of mongo_dart;

enum PayloadType {
  ZERO, ONE
}

abstract class Section {
  PayloadType _type;
  Section(this._type);

  int get byteLength => 1;

  void serialize(BsonBinary buffer) => buffer.writeByte(_type.index);
  Section.fromBuffer(BsonBinary buffer) : _type = PayloadType.values[buffer.readByte()];
}

class MainSection extends Section {
  BsonMap payload;

  MainSection(this.payload) : super(PayloadType.ZERO);

  @override
  void serialize(BsonBinary buffer) {
    super.serialize(buffer);
    payload.packValue(buffer);
  }

  MainSection.fromBuffer(BsonBinary buffer) : super.fromBuffer(buffer) {
    payload = BsonMap({})..unpackValue(buffer);
  }

  @override
  int get byteLength => super.byteLength + payload.byteLength();
}

class PayloadSection extends Section {
  BsonCString identifier;
  List<BsonMap> payload;

  PayloadSection(String identifier, this.payload) :
        identifier = BsonCString(identifier), super(PayloadType.ONE);

  void asMapElement(Map<String, dynamic> map) => map[identifier.data] = payload.map((doc) => doc.data);

  @override
  void serialize(BsonBinary buffer) {
    super.serialize(buffer);
    buffer.writeInt(byteLength - 1);
    identifier.packValue(buffer);
    for (var document in payload)
      document.packValue(buffer);
  }

  @override
  PayloadSection.fromBuffer(BsonBinary buffer) : super.fromBuffer(buffer) {
    int payloadLength = buffer.readInt32();
    identifier = BsonCString('')..unpackValue(buffer);
    int payloadStartOffset = buffer.offset;
    payloadLength -= 4 + identifier.byteLength();
    payload = [];
    while (buffer.offset - payloadStartOffset < payloadLength)
      payload.add(BsonMap({})..unpackValue(buffer));
  }

  @override
  int get byteLength {
    int payloadLength = payload.fold(0, (len, sec) => len += sec.byteLength());
    return super.byteLength + 4 + identifier.byteLength() + payloadLength;
  }
}
