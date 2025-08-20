class Config {
  static const env = String.fromEnvironment('ENV', defaultValue: 'dev');
  static const logLevel = String.fromEnvironment(
    'LOG_LEVEL',
    defaultValue: 'debug',
  );
  static const redirectUrl = String.fromEnvironment(
    'REDIRECT_URL',
    defaultValue: 'https://dev.ratel.foundation',
  );
  static const apiEndpoint = String.fromEnvironment(
    'API_ENDPOINT',
    defaultValue: 'http://hackartist.iptime.org:3000',
  );
  static const signDomain = String.fromEnvironment(
    'SIGN_DOMAIN',
    defaultValue: 'dev.ratel.foundation',
  );
}
