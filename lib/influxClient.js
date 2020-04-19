'use strict';

const nconf = require('nconf');
const log4js = require('log4js');
const Influx = require('influx');
// Globals

const logger = log4js.getLogger('influx-client');
let influx;
let dbName;

// Public Methods ------------------------------------------------------------->

module.exports.create = _create;
module.exports.write = _write;

/**
 * Initialize influx connection.
 * @return {Promise}
 */
async function _create() {
  const config = nconf.get('influx');
  dbName = config.database;

  influx = new Influx.InfluxDB({
    database: dbName,
    hosts: [
      {host: config.host},
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
  const names = await influx.getDatabaseNames();
  if (!names.includes(dbName)) {
    logger.info(`created database ${dbname}`);
    await influx.createDatabase(dbName);
  }
}

/**
 * Write datapoints.
 * @param {String} name name tag for data.
 * @param {Object} data fields to write.
 * @return {Promise}
 */
async function _write(name, data) {
  await influx.writePoints([
    {
      measurement: 'schilf',
      tags: {
        name: name,
      },
      fields: data,
    },
  ], {
    database: dbName,
    precision: 's',
  });
}
