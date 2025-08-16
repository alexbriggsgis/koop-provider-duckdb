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
    parquetFields,
    idField,
    geometryField = "geometry",
    cacheTtl = 0,
    crs = 4326,
    maxRecordCount = 2000,

  } = this.getMetadata();


  const parquetFieldNames = parquetFields.map(field => field.name);

  const sql_fields = getOutFields(parquetFieldNames, geoserviceParams.outFields || '*');

  const metadataFields = [...(parquetFields || []), {
    name: "OBJECTID",
    type: "Integer",
    alias: "OBJECTID"
  }];

  // Convert Geoservice params to MongoDB query equivalents
  const dbParams = convertGeoserviceParamsToDbParams({
    ...geoserviceParams,
    parquetFile,
    geometryField,
    crs,
    resultRecordCount: maxRecordCount,
    outFields: sql_fields
  });

  const query = dbParams.sql;

  try {
    if (!duckdbConnection) {
      throw new Error('DuckDB connection not initialized');
    }
    const reader = await duckdbConnection.runAndReadAll(query);
    const rawData = reader.getRows()[0][0];

    // for aggregate requests, aggregate directly with MongoDB
    if (geoserviceParams.returnCountOnly) {
      return callback(null, { "count": Number(rawData) });
    }

    // Parse the result if it's a string, or use as-is if it's already an object
    const geojson = typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
    // Add metadata at the top level of the object

    const result = {
      ...geojson,
      metadata: {
        name: 'duckdb',
        description: 'DuckDB Provider',
        idField: idField,
        fields: metadataFields,
      }
    };

    callback(null, result);

  } catch (error) {
    console.error('Error executing query:', error);
    callback(error, null);
  }
}

module.exports = Model

/* Example provider API:
   - needs to be converted to GeoJSON Feature Collection
{
  "resultSet": {
  "queryTime": 1488465776220,
  "vehicle": [
    {
      "tripID": "7144393",
      "signMessage": "Red Line to Beaverton",
      "expires": 1488466246000,
      "serviceDate": 1488441600000,
      "time": 1488465767051,
      "latitude": 45.5873117,
      "longitude": -122.5927705,
    }
  ]
}

Converted to GeoJSON:

{
  "type": "FeatureCollection",
  "features": [
    "type": "Feature",
    "properties": {
      "tripID": "7144393",
      "signMessage": "Red Line to Beaverton",
      "expires": "2017-03-02T14:50:46.000Z",
      "serviceDate": "2017-03-02T08:00:00.000Z",
      "time": "2017-03-02T14:42:47.051Z",
    },
    "geometry": {
      "type": "Point",
      "coordinates": [-122.5927705, 45.5873117]
    }
  ]
}
*/
