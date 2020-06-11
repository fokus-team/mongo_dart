part of mongo_dart;

class MongoOpMessage extends MongoMessage {
	static final CHECKSUM_PRESENT = 0;
	static final MORE_TO_COME = 1;
	static final EXHAUST_ALLOWED = 16;

	int flags = 0;
	int payloadType = 0;
	List<BsonMap> documents = [];

	MongoOpMessage.fromMessage(MongoMessage message) {
		opcode = MongoMessage.OpMsg;
		var namespace = message._collectionFullName.data.split('.');
		BsonMap document = BsonMap({});
		if (message is DbCommand) {

		} else if (message is MongoQueryMessage) {
			document.data['find'] = namespace[1];
			document.data['filter'] = message._query.data['\$query'];
		}
		document.data['\$db'] = namespace[0];
		documents.add(document);
	}

	MongoOpMessage.fromResponse(BsonBinary buffer) {
		opcode = MongoMessage.OpMsg;
		deserialize(buffer);
	}

	MongoReplyMessage unpack() {
		var answer = MongoReplyMessage();
		var cursor = documents[0].data['cursor'];
		answer.responseTo = responseTo;
		answer._requestId = _requestId;
		answer.cursorId = cursor['id'] as int;
		answer.documents = [];
		for (var document in cursor['firstBatch'])
			answer.documents.add(document as Map<String, dynamic>);
		answer.responseFlags = 0;
		answer.startingFrom = 0;
		answer.numberReturned = answer.documents.length;
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
