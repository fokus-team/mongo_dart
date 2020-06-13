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
			document.data.addAll(message._query.data);
		} else if (message is MongoQueryMessage) {
			document.data['find'] = namespace[1];
			document.data['filter'] = message._query.data['\$query'];
			if (message.numberToReturn > 0)
				document.data['limit'] = message.numberToReturn;
			document.data['skip'] = message.numberToSkip;
			if (message._fields != null)
				document.data['projection'] = message._fields;
		} else if (message is MongoGetMoreMessage) {
			document.data['getMore'] = message.cursorId;
			document.data['collection'] = namespace[1];
		} else if (message is MongoInsertMessage) {
			document.data['insert'] = namespace[1];
			document.data['documents'] = message._documents;
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
