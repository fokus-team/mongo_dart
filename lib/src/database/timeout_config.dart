part of mongo_dart;

class TimeoutConfig {
	int connectionTimeout;
	int socketTimeout;
	int keepAliveTime;

	/// Creates MongoDB connection timeout configuration.
	/// [connectionTimeout] specifies how long to wait for establishing db connection before returning in milliseconds. After that time a [SocketException] will be thrown.
	/// [socketTimeout] specifies the maximum waiting time for response to any db request in milliseconds. After that time a [TimeoutException] will be thrown.
	/// [keepAliveTime] specifies the time after the connection will be closed in case of no activity in seconds.
	/// The default values of 0 mean no timeout.
	TimeoutConfig({this.connectionTimeout = 0, this.socketTimeout = 0, this.keepAliveTime = 0});
}
