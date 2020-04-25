'use strict';
const fs = require('fs').promises;
const log4js = require('log4js');

// Globals

const logger = log4js.getLogger('local-exporter');

// Public Methods ------------------------------------------------------------->

module.exports.has = () => false;
module.exports.export = _export;

/**
 * Exports a data slice to storage
 * @param {Object} key key to use for this export
 * @param {Object} data data to export.
 * @return {Promise}
 */
async function _export(key, data) {
  let filehandle;
  try {
    filehandle = await fs.open(`export/dayOfYear=${key}.json`, 'w');
    data.forEach(async (object) => {
      await fs.writeFile(filehandle, JSON.stringify(object) + '\n');
    });

    logger.info(`Successfully exported data slice ${key}`);
  } finally {
    if (filehandle) {
      await filehandle.close();
    }
  }
}
