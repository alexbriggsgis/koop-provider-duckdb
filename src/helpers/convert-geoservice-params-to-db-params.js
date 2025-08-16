const SQLParser = require('@synatic/noql');
const {
  standardizeGeometryFilter,
  combineObjectIdsAndWhere,
} = require('@koopjs/geoservice-utils');
const { addGeoFilterToPipeline } = require('./add-geo-filter-to-pipeline');
const { addGeoFilterToQuery } = require('./add-geo-filter-to-query');
const { extentCalculatorStage } = require('./extent-calculator-stage');

const relationLookup = {
  esriSpatialRelIntersects: '$geoIntersects',
  esriSpatialRelWithin: '$geoWithin',
};

function convertGeoserviceParamsToDbParams(params) {
  const {
    geometry,
    returnExtentOnly,
    geometryField,
    idField,
    inSR,
    spatialRel,
    crs,
    parquetFile,
    outFields
  } = params;

  // Convert geoservice where, objectIds, resultRecordCount, result offset to SQL equivalent
  const sql = buildSqlQuery(params);


  return {
    sql
  };
}

function buildSqlQuery(params) {
  const {
    where,
    orderByFields,
    objectIds,
    resultRecordCount,
    resultOffset,
    idField,
    returnCountOnly,
    returnExtentOnly,
    parquetFile,
    geometry,
    inSR,
    spatialRel,
    crs,
    outFields,
    geometryField

  } = params;
  var geometryFilterduckdb = null;
  if (geometry) {
    const geometryFilter = standardizeGeometryFilter({
      geometry,
      inSR,
      spatialRel,
      reprojectionSR: crs,
    });

    geometryFilterduckdb =  `ST_Intersects(geometry, ST_GeomFromGeoJSON('${JSON.stringify(geometryFilter.geometry)}'))`
  }

  

  const limitClause = ` LIMIT ${resultRecordCount}`;

  const orderByClause = orderByFields ? ` ORDER BY ${orderByFields}` : '';

  const objectIDClause = resultOffset ? `${resultOffset} + ROW_NUMBER() OVER () as OBJECTID`  : `ROW_NUMBER() OVER () as OBJECTID`;

  const offsetClause = resultOffset ? ` OFFSET ${resultOffset}` : '';

  // combine the "where" and "objectIds"
  const combinedWhere = combineObjectIdsAndWhere({ where, objectIds, idField });
  const whereClause = combinedWhere ? ` WHERE ${combinedWhere}` : '';
  var whereClausewithgeom = null
  if (geometryFilterduckdb) {
    if(whereClause === '') {
     whereClausewithgeom = whereClause + ` WHERE ${geometryFilterduckdb}`; // If no where clause, just add geometry filter
    }
    else {} {
     whereClausewithgeom = whereClause + ` AND ${geometryFilterduckdb}`; // If there is a where clause, add geometry filter to it
    }
  }
  const finalWhereClause = whereClausewithgeom ? whereClausewithgeom : whereClause;

  if (returnCountOnly || returnExtentOnly) {
    return `SELECT COUNT(*) AS count FROM read_parquet('${parquetFile}', filename=true, hive_partitioning=1) AS data ${finalWhereClause}`;
  }

  return `WITH geodata AS (SELECT ${outFields},${geometryField}, ${objectIDClause} FROM read_parquet('${parquetFile}', filename=true, hive_partitioning=1) AS data ${finalWhereClause} ${orderByClause} ${limitClause} ${offsetClause})
  SELECT json_object(
    'type', 'FeatureCollection',
    'features', array_agg(
      json_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(geometry)::JSON,
        'properties', json_object(
          'OBJECTID', OBJECTID,
          'id', id,
          'confidence', confidence,
          'socials', socials
        )
      )
    )
  ) AS geojson_featurecollection
  FROM geodata;`;
}

module.exports = {
  convertGeoserviceParamsToDbParams,
};
