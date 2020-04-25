'use strict';

const nconf = require('nconf');
const log4js = require('log4js');
const aedes = require('aedes');
const moment = require('moment');
const influx = require('./influxClient');
const exporter = require('./cosExporter');

// Globals

const logger = log4js.getLogger('application');

// Public Methods ------------------------------------------------------------->

module.exports.create = _create;


// Private Methods ------------------------------------------------------------>

/**
 * Create MQTT broker listener.
 * @return {connectionListener} broker listener to be used for MQTT.
 */
async function _create() {
  const client = aedes();
  client.authenticate = _authenticate;
  client.published = _published;

  // connect to influx
  await influx.create();
  await exporter.init();

  // schedule export for every 24h
  _runExport();
  setInterval(_runExport, moment.duration(1, 'd').asMilliseconds());

  return client.handle;
}

/**
 * Runs the data exporter.
 */
function _runExport() {
  logger.info(`Running data export`);
  influx.export(exporter);
}

/**
 * Called when a new client want's to auth.
 * @param {Client} client Client object.
 * @param {String} username username.
 * @param {String} password password.
 * @param {Function} callback callback.
 */
function _authenticate(client, username, password, callback) {
  // get the list of client ids that we accept
  const clientIds = nconf.get('sensors').map((entry) => entry.id);

  if (clientIds.includes(client.id)) {
    logger.info(`Client is known: ${client.id}, checking auth`);

    const auth = nconf.get('auth');
    // check authentication
    if (auth.username == username && auth.password == password) {
      logger.info(`Client is okay: ${client.id}`);
      callback(null, true);
      return;
    } else {
      logger.info(`Client is bad: ${client.id}`);
      const error = new Error('Wrong username or password');
      error.returnCode = 4;

      callback(error, null);
      return;
    }
  }

  // The client isn't known, so we reject any attempt to connect
  const error = new Error('Auth error');
  error.returnCode = 5;
  callback(error, null);
  return;
};

/**
 * Called when data has been published
 * @param {Packet} packet Packet object.
 * @param {Client} client Client object.
 * @param {Function} callback callback.
 */
function _published(packet, client, callback) {
  if (!client) {
    callback();
    return;
  }

  // we only care about "measurement right now"
  if ('channels/1027883/publish/4R1RML85532YUYHD' !== packet.topic) {
    logger.warn(`Dropping submission for unknown topic: ${packet.topic}`);
    callback();
    return;
  }

  const keyValue = packet.payload.toString().split('&');
  const data = keyValue.map((entry) => entry.split('=')[1]);

  // format a data row
  const row = {
    temperature: parseFloat(data[0]),
    humidity: parseFloat(data[1]),
    carbonDioxide: parseInt(data[2], 10),
    pressure: parseInt(data[3], 10),
  };

  const sensorConfig = nconf.get('sensors').find((e) => e.id === client.id);

  influx.write(sensorConfig.tag, row)
      .then(() => {
        logger.info('Data sent to influx');
        callback();
        return;
      })
      .catch((error) => {
        logger.error(`Error saving data to InfluxDB! ${error}`);
      });
};
