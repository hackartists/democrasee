const serverlessExpress = require('@codegenie/serverless-express');

const nextServer = require('./server.js');

exports.handler = serverlessExpress({
  app: nextServer,
});
