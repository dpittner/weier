'use strict';

const nconf = require('nconf');
const log4js = require('log4js');
const Influx = require('influx');
const moment = require('moment');
// Globals

const logger = log4js.getLogger('influx-client');
const MEASUREMENT = 'schilf';
const snooze = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let influx;
let dbName;
const initialized = false;

// Public Methods ------------------------------------------------------------->

module.exports.create = _create;
module.exports.write = _write;
module.exports.export = _export;

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
        measurement: MEASUREMENT,
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

  do {
    try {
      const names = await influx.getDatabaseNames();
      if (!names.includes(dbName)) {
        logger.info(`created database ${dbName}`);
        await influx.createDatabase(dbName);
      }
      initialized = true;
    } catch (err) {
      logger.info(`failed to check database ${dbName}`, err);
      await snooze(10000);
    }
  } while (!initialized);
}

/**
 * Write datapoints.
 * @param {String} name name tag for data.
 * @param {Object} data fields to write.
 * @return {Promise}
 */
async function _write(name, data) {
  while (!initialized) {
    await snooze(5000);
  }
  await influx.writePoints([
    {
      measurement: MEASUREMENT,
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

/**
   * Generator for exporting data for each day available in influx.
   * @return {Object}
   */
async function* _dayClauseGenerator() {
  const oldest = await influx.query(`
      select * from ${MEASUREMENT} LIMIT 1
  `, {
    database: dbName,
    precision: 's',
  });
  logger.info('First data point present:'+ oldest[0].time);
  let mOldest = moment.utc(oldest[0].time).startOf('day');
  const mNow = moment.utc().startOf('day');

  while (mOldest.isBefore(mNow)) {
    // time >= '2020-04-23 00:00:00' and time < '2020-04-24 00:00:00'
    const mNext = moment(mOldest).add(1, 'day');

    yield {
      dayOfYear: mOldest.dayOfYear(),
      clause: `time >= '${mOldest.format('YYYY-MM-DD HH:mm:SS')}' and
               time < '${mNext.format('YYYY-MM-DD HH:mm:SS')}'`,
    };
    mOldest = mNext;
  }
}

/**
   * Write datapoints.
   * @param {Object} dataSink dataSink to export to,
   * has to support has() and export() method
   */
async function _export(dataSink) {
  while (!initialized) {
    await snooze(5000);
  }
  const sequence = _dayClauseGenerator();
  for await (const slice of sequence) {
    const {dayOfYear, clause} = slice;
    if (await dataSink.has(dayOfYear)) {
      logger.debug(`Data for dayofyear ${dayOfYear} already exported`);
    } else {
      try {
        const dataSlice = await influx.query(`
            select * from ${MEASUREMENT} WHERE ${clause}`, {
          database: dbName,
          precision: 's',
        });
        await dataSink.export(dayOfYear, dataSlice);
      } catch (err) {
        logger.warn(`Failed to export data for ${dayOfYear}`, err);
      }
    }
  }
}
