'use strict';

const nconf = require('nconf');
const ibm = require('ibm-cos-sdk');
const log4js = require('log4js');

// Globals
let cos;

const logger = log4js.getLogger('cos-exporter');

// Public Methods ------------------------------------------------------------->

module.exports.has = _has;
module.exports.export = _export;
module.exports.init = _init;

/**
 * Initialize COS exporter
 * @return {Promise}
 */
async function _init() {
  const config = nconf.get('cos');
  const cosConfig = {
    endpoint: config.endpoint,
    apiKeyId: config.apikeyid,
    serviceInstanceId: config.serviceinstanceid,
  };
  cos = new ibm.S3(cosConfig);
  await _ensureBucketExists(config);
}

/**
 * Validates bucket exists or creates it.
 * @param {Object} config bucket config to use.
 * @return {Promise}
 */
async function _ensureBucketExists(config) {
  try {
    await cos.headBucket({
      Bucket: config.bucket,
    }).promise();
    logger.info('Bucket exists');
    return;
  } catch (err) {
    if (err.statusCode && err.statusCode === 404) {
      logger.info('Bucket does not exist, creating it');
      try {
        await cos.createBucket({
          Bucket: config.bucket,
          CreateBucketConfiguration: {
            LocationConstraint: config.LocationConstraint,
          },
        }).promise();
        logger.warn('Bucket has been created');
        return;
      } catch (errCreate) {
        logger.warn('Failed to create bucket');
        throw errCreate;
      }
    }
    logger.warn('Could not check bucket state');
    throw err;
  }
}

/**
 * Checks if key exists in storage, if truthy, not export will be done
 * @param {Object} key key to check on storage
 * @return {Promise}
 */
async function _has(key) {
  const config = nconf.get('cos');
  const keyName = config.namingPattern.replace(/%d/g, key);
  logger.debug(`Checking if key ${keyName} is already exported`);
  try {
    await cos.headObject({
      Bucket: config.bucket,
      Key: keyName,
    }).promise();
    return true;
  } catch (err) {
    if (err.statusCode && err.statusCode === 404) {
      logger.info('Export does not exist');
      return false;
    }
    logger.info(err);
    throw err;
  }
  return false;
}
/**
 * Exports a data slice to storage
 * @param {Object} key key to use for this export
 * @param {Object} data data to export.
 * @return {Promise}
 */
async function _export(key, data) {
  const config = nconf.get('cos');
  const keyName = config.namingPattern.replace(/%d/g, key);

  let cosObject = '';
  data.forEach( (row) => {
    cosObject = cosObject + JSON.stringify(row) + '\n';
  });
  logger.debug(`Uploading data export ${keyName} , size ${cosObject.length}`);
  await cos.putObject({
    Bucket: config.bucket,
    Key: keyName,
    Body: cosObject,
  }).promise();
}
