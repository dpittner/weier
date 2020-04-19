const aedes = require('aedes')();
const log4js = require('log4js');
const Influx = require('influx');
const nconf = require('nconf');
const util = require('util');
const server = require('net').createServer(aedes.handle);

const logger = log4js.getLogger('broker');
const PORT = 1883;

nconf.argv({parseValues: true})
    .env({
      parseValues: true,
      lowerCase: true,
    })
    .file({file: 'config/app.json'});

nconf.required(['sensors']);

const clientIds = nconf.get('sensors');

log4js.configure({
  appenders: {console: {type: 'console'}},
  categories: {default: {appenders: ['console'], level: 'debug'}},
});

const influx = new Influx.InfluxDB({
  database: 'sensors',
  hosts: [
    {host: 'localhost'},
  ],
  schema: [
    {
      measurement: 'schilf',
      fields: {
        temperature: Influx.FieldType.FLOAT,
        humidity: Influx.FieldType.FLOAT,
        carbonDioxide: Influx.FieldType.INTEGER,
        pressure: Influx.FieldType.INTEGER,
      },
      tags: ['name'],
    },
  ],
});


aedes.authenticate = function(client, username, password, callback) {
  if (clientIds.includes(client.id)) {
    logger.info(`Client is known: ${client.id}`);
  }
  const error = new Error('Auth error');
  error.returnCode = 4;
  callback(null, true);
  // callback(error, null);
};

aedes.authorizePublish = function(client, packet, callback) {
  logger.info(`authz: ${client.id} ${packet.topic} ${packet.payload}`);
  if (packet.topic === 'aaaa') {
    return callback(new Error('wrong topic'));
  }
  if (packet.topic === 'bbb') {
    packet.payload = Buffer.from('overwrite packet payload');
  }
  callback(null);
};

aedes.published = function(packet, client, callback) {
  logger.info(`publish: ${client ? client.id : 'internal'} ${packet.topic} ${packet.payload}`);

  if (client) {
    const keyValue = packet.payload.toString().split('&');
    const data = keyValue.map((entry) => entry.split('=')[1]);

    influx.writePoints([
      {
        measurement: 'schilf',
        tags: {
          name: 'büro',
        },
        fields: {
          temperature: parseFloat(data[0]),
          humidity: parseFloat(data[1]),
          carbonDioxide: parseInt(data[2], 10),
          pressure: parseInt(data[3], 10),
        },
      },
    ], {
      database: 'sensors',
      precision: 's',
    })
        .then(() => {
          logger.info('Data sent to influx');
          callback();
          return;
        })
        .catch((error) => {
          logger.error(`Error saving data to InfluxDB! ${error}`);
        });
  } else {
    callback();
    return;
  }
};

influx.getDatabaseNames()
    .then((names) => {
      if (!names.includes('sensors')) {
        return influx.createDatabase('sensors');
      } else {
        logger.info('InfluxDB exists');
      }
    })
    .then(() => {
      server.listen(PORT, function() {
        logger.info('server listening on port', PORT);
      });
    });
