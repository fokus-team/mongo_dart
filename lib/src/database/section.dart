part of mongo_dart;

enum PayloadType {
	ZERO, ONE
}

abstract class Section {
	PayloadType _type;
	Section(this._type);

	int get byteLength => 1;
	void serialize(BsonBinary buffer) => buffer.writeByte(_type.index);
}

class MainSection extends Section {
	BsonMap payload;

  MainSection(this.payload) : super(PayloadType.ZERO);

  @override
  void serialize(BsonBinary buffer) {
		super.serialize(buffer);
		payload.packValue(buffer);
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
  int get byteLength {
		int payloadLength = payload.fold(0, (value, element) => value += element.byteLength());
		return super.byteLength + 4 + identifier.byteLength() + payloadLength;
  }
}
