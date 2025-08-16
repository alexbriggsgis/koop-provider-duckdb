/*
  model.js

  This file is required. It must export a class with at least one public function called `getData`

  Documentation: http://koopjs.github.io/docs/usage/provider
*/
const request = require('request').defaults({ gzip: true, json: true })
const config = require('config')
const { DuckDBConnection } = require('@duckdb/node-api');
const { convertGeoserviceParamsToDbParams } = require('./helpers');

let duckdbConnection = null;

// Initialize DuckDB connection and extensions
async function initializeDuckDB() {
  try {
    duckdbConnection = await DuckDBConnection.create();
    await duckdbConnection.run("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;");
    console.log('DuckDB initialized: Extensions installed and loaded successfully');
  } catch (error) {
    console.error('Failed to initialize DuckDB:', error);
    throw error;
  }
}

function getOutFields(parquetFieldNames, outFields) {

  // If outFields is '*', return all fields
  if (outFields === '*') {
    return parquetFieldNames;
  }

  // Split outFields by comma and trim whitespace
  const requestedFields = outFields.split(',').map(field => field.trim());

  // Filter parquetFieldNames to include only requested fields
  return parquetFieldNames.filter(field => requestedFields.includes(field));
}

function Model() {
  // Initialize DuckDB when the model is created
  initializeDuckDB().catch(error => {
    console.error('Error during DuckDB initialization:', error);
  });
}

Model.prototype.getMetadata = function () {
  // Lookup collection config
  const configLookup = config?.duckdb || {};
  return configLookup
}
// Public function to return data from the
// Return: GeoJSON FeatureCollection
//
// Config parameters (config/default.json)
// req.
//
// URL path parameters:
// req.params.host (if index.js:hosts true)
// req.params.id  (if index.js:disableIdParam false)
// req.params.layer
// req.params.method
Model.prototype.getData = async function (req, callback) {

  // Get request query/body params
  const geoserviceParams = req.query;


  // Fetch metadata
  const {
    parquetFile,
    fields,
    idField,
    geometryField = "geometry",
    cacheTtl = 0,
    crs = 4326,
    maxRecordCount = 2000,

  } = this.getMetadata();

  if (!fields || !Array.isArray(fields)) {
    return callback(new Error('Invalid or missing parquetFields in metadata'), null);
  }

  const parquetFieldNames = fields.map(field => field.name);

  const sql_fields = getOutFields(parquetFieldNames, geoserviceParams.outFields || '*');


  // Convert Geoservice params to MongoDB query equivalents
  const dbParams = convertGeoserviceParamsToDbParams({
    ...geoserviceParams,
    parquetFile,
    geometryField,
    crs,
    resultRecordCount: maxRecordCount,
    outFields: sql_fields
  });

  try {
    if (!duckdbConnection) {
      throw new Error('DuckDB connection not initialized');
    }


    // for aggregate requests, aggregate directly with MongoDB
    if (geoserviceParams.returnCountOnly) {
      const countreader = await duckdbConnection.runAndReadAll(dbParams.countSql);
      const countrawData = countreader.getRows()[0][0];
      return callback(null, { "count": Number(countrawData) });
    }

    const geojsonreader = await duckdbConnection.runAndReadAll(dbParams.sql);
    const geojsonrawData = geojsonreader.getRows()[0][0];

    // Parse the result if it's a string, or use as-is if it's already an object
    const geojson = typeof geojsonrawData === 'string' ? JSON.parse(geojsonrawData) : geojsonrawData;
    // Add metadata at the top level of the object

    //catch empty features response 
    if (!Array.isArray(geojson.features)) {
      geojson.features = [];
    }


    const result = {
      ...geojson,
      metadata: {
        name: 'duckdb',
        description: 'DuckDB Provider',
        idField: idField,
        fields: fields,
        maxRecordCount: maxRecordCount,
        returnExceededLimitFeatures: true,
        supportsPagination: true

      }
    };

    callback(null, result);

  } catch (error) {
    console.error('Error executing query:', error);
    callback(error, null);
  }
}

module.exports = Model
