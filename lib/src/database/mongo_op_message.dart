part of mongo_dart;

class MongoOpMessage extends MongoMessage {
	static final OPTS_CHECKSUM_PRESENT = 1 << 0;
	static final OPTS_MORE_TO_COME = 1 << 1;
	static final OPTS_EXHAUST_ALLOWED = 1 << 16;

	int flags = 0;
	List<BsonMap> documents = [];
	List<Section> sections = [];

	MongoOpMessage.fromMessage(MongoMessage message) {
		opcode = MongoMessage.OpMsg;
		if (message is BulkCommand) {
			sections = (message as BulkCommand).getSections();
		} else {
			BsonMap command = BsonMap(message.toCommand());
			command.data.addAll(toCommand());
		  sections.insert(0, MainSection(command));
		}
		(sections[0] as MainSection).payload.data['\$db'] = message._dbName();
	}

	MongoOpMessage.fromResponse(BsonBinary buffer) {
		opcode = MongoMessage.OpMsg;
		deserialize(buffer);
	}

	@override
	Map<String, dynamic> toCommand() => {};

	MongoReplyMessage unpack() {
		var answer = MongoReplyMessage();
		answer.responseTo = responseTo;
		answer._requestId = _requestId;
		answer.responseFlags = 0;
		answer.documents = [];
		if (documents[0].data.containsKey('cursor')) {
			var cursor = documents[0].data['cursor'] as Map<String, dynamic>;
			answer.cursorId = cursor['id'] as int;
			var batchParam = cursor.containsKey('firstBatch') ? 'firstBatch' : 'nextBatch';
			answer.documents.addAll(List.from(cursor[batchParam] as List));
		} else
			answer.documents.addAll(documents.map((bson) => bson.data));
		return answer;
	}

	@override
	int get messageLength => 16 + 4 + sections.fold<int>(0, (value, element) => value += element.byteLength);

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
		int payloadType = buffer.readByte();
		documents.add(BsonMap({}));
		documents[0].unpackValue(buffer);
		while (buffer.byteLength() - buffer.offset > 4) {
			buffer.offset++;

		}
		if (payloadType == 1)
			throw MongoDartError('OpMsg payload type 1 is not yet supported');
	}
}
