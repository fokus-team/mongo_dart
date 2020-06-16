part of mongo_dart;

class MongoOpMessage extends MongoMessage {
	static final OPTS_CHECKSUM_PRESENT = 1 << 0;
	static final OPTS_MORE_TO_COME = 1 << 1;
	static final OPTS_EXHAUST_ALLOWED = 1 << 16;

	int flags = 0;
	int payloadType = 0;
	List<BsonMap> documents = [];

	MongoOpMessage.fromMessage(MongoMessage message) {
		opcode = MongoMessage.OpMsg;
		var command = message.toCommand();
		command['\$db'] = message._dbName();
		command.addAll(toCommand());
		documents.add(BsonMap(command));
	}

	MongoOpMessage.fromResponse(BsonBinary buffer) {
		opcode = MongoMessage.OpMsg;
		deserialize(buffer);
	}

	@override
	Map<String, dynamic> toCommand() {
		return {};
	}

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
	int get messageLength {
		return 16 + 4 + 1 + documents[0].byteLength();
	}

	@override
	BsonBinary serialize() {
		BsonBinary buffer = BsonBinary(messageLength);
		writeMessageHeaderTo(buffer);
		buffer.writeInt(flags);
		buffer.writeByte(0);
		documents[0].packValue(buffer);
		if (payloadType == 1)
			throw MongoDartError('OpMsg payload type 1 is not yet supported');
		buffer.offset = 0;
		return buffer;
	}

	@override
	void deserialize(BsonBinary buffer) {
		readMessageHeaderFrom(buffer);
		flags = buffer.readInt32();
		payloadType = buffer.readByte();
		documents.add(BsonMap({}));
		documents[0].unpackValue(buffer);
		if (payloadType == 1)
			throw MongoDartError('OpMsg payload type 1 is not yet supported');
	}
}
