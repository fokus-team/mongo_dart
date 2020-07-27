part of mongo_dart;

class Response with ResponseWrapper {
  Map<String, dynamic> _rawResponse = {};

  bool success;
  int matchedCount;
  List<RequestError> errors = [];

  Response({this.success = true, this.errors = const []}) {
    _rawResponse['ok'] = success ? 1 : 0;
  }

  Response.fromError(RequestError error) : this(success: false, errors: [error]);

  Response.fromCommand(this._rawResponse) {
    _unpackCommon();
    if (_rawResponse.containsKey('writeErrors'))
      for (var error in _rawResponse['writeErrors'])
        errors.add(RequestError.fromCommand(error as Map<String, dynamic>));
  }

  Response.fromMessage(this._rawResponse) {
    _unpackCommon();
    if (_rawResponse['err'] != null || _rawResponse['errmsg'] != null)
      errors.add(RequestError.fromMessage(_rawResponse));
  }

  void _unpackCommon() {
    if (_rawResponse.containsKey('ok'))
      success = _rawResponse['ok'] as num == 1;
    if (_rawResponse.containsKey('n'))
      matchedCount = _rawResponse['n'] as int;
  }

  @override
  String toString() => 'Response{success: $success, errors: $errors}';
}

class RequestError with ResponseWrapper implements Exception {
  Map<String, dynamic> _rawResponse = {};

  int index;
  int code;
  String message;
  String description;

  RequestError(this.message);

  RequestError.fromCommand(this._rawResponse) {
    if (_rawResponse.containsKey('index'))
      index = _rawResponse['index'] as int;
    if (_rawResponse.containsKey('code'))
      code = _rawResponse['code'] as int;
    if (_rawResponse.containsKey('errmsg'))
      message = _rawResponse['errmsg'] as String;
  }

  RequestError.fromMessage(this._rawResponse) {
    if (_rawResponse.containsKey('code'))
      code = _rawResponse['code'] as int;
    if (_rawResponse.containsKey('err'))
      message = _rawResponse['err'] as String;
    if (_rawResponse.containsKey('errmsg'))
      description = _rawResponse['errmsg'] as String;
  }

  @override
  String toString() => 'RequestError{code: $code, message: $message}';
}

mixin ResponseWrapper {
  Map<String, dynamic> get _rawResponse;

  dynamic operator [](String key) => _rawResponse[key];
  bool containsKey(String key) => _rawResponse.containsKey(key);
}
