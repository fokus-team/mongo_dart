part of mongo_dart;

abstract class SaslMechanism {
  String get name;

  SaslStep initialize(Connection connection);
}

abstract class SaslStep {
  Uint8List bytesToSendToServer;
  bool isComplete = false;

  SaslStep(this.bytesToSendToServer, {this.isComplete = false});
  SaslStep transition(
      SaslConversation conversation, List<int> bytesReceivedFromServer);
}

class SaslConversation {
  Connection connection;

  SaslConversation(this.connection);
}

abstract class SaslAuthenticator extends Authenticator {
  SaslMechanism mechanism;
  Db db;

  SaslAuthenticator(this.mechanism, this.db);

  @override
  Future authenticate(Connection connection) async {
    var conversation = SaslConversation(connection);

    var currentStep = mechanism.initialize(connection);

    var command = DbCommand.createSaslStartCommand(
        db.authSourceDb ?? db, mechanism.name, currentStep.bytesToSendToServer);

    while (true) {
      Map<String, dynamic> result;

      result = await db.executeDbCommand(command, connection: connection);

      if (result['done'] == true && currentStep.isComplete) {
        break;
      }

      var payload = result['payload'];

      var payloadAsBytes = payload.byteList;

      currentStep = currentStep.transition(conversation, payloadAsBytes);

      var conversationId = result['conversationId'] as int;

      command = DbCommand.createSaslContinueCommand(db.authSourceDb ?? db,
          conversationId, currentStep.bytesToSendToServer);
    }
  }
}
