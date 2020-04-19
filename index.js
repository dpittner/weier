'use strict';

const log4js = require('log4js');
const nconf = require('nconf');

const app = require('./lib/application');
const net = require('net');

const logger = log4js.getLogger('index');

nconf.argv({parseValues: true})
    .env({
      separator: '_',
      parseValues: true,
      lowerCase: true,
    })
    .file({file: 'config/app.json'});

nconf.required(['sensors', 'auth', 'influx', 'port']);

log4js.configure({
  appenders: {console: {type: 'console'}},
  categories: {default: {appenders: ['console'], level: 'debug'}},
});

app.create().then((listener) => {
  const port = nconf.get('port');
  net.createServer(listener).listen(port, () => {
    logger.info(`server listening on port ${port}`);
  });
});
