# Koop Provider DuckDB

Experimental koop provider for DuckDB, specifically for reading parquet data sources. 

## Run the Example App (Requires Docker)

1. Download a parquet file and place parquet file in docker-demo/data. Consider using https://docs.overturemaps.org/getting-data/overturemaps-py/
e.g. all places in australia 
```
  overturemaps download \
  --bbox=112.9211,-43.7405,153.6383,-10.6840 \
  -f geoparquet \
  --type=place \
  -o australia_places.parquet
```
2. Update `docker-demos/config/deault.json` with your parquet details
3. Run `docker compose build`
4. Run `docker compose up`
5. Test out the app on `http://localhost:9001/`

![Gif of Demo](/docs/assets/demo.gif)


## Koop provider file structure

| File | | Description |
| --- | --- | --- |
| `src/index.js` | Mandatory | Configures provider for usage by Koop |
| `src/model.js` | Mandatory | Translates remote API to GeoJSON |
| `src/routes.js` | Optional | Specifies additional routes to be handled by this provider |
| `src/controller.js` | Optional | Handles additional routes specified in `routes.js` |
| `test/model-test.js` | Optional | tests the `getData` function on the model |
| `test/fixtures/input.json` | Optional | a sample of the raw input from the 3rd party API |
| `config/default.json` | Optional | used for advanced configuration, usually API keys. |


## Test it out
Run server:
- `npm install`
- `npm start`

Example API Query:
- `curl localhost:8080/example/FeatureServer/0/query?returnCountOnly=true`

Tests:
- `npm test`

### Development output callstack logs

During development you can output error callstack with

- `NODE_ENV=test npm start`

## Publish to npm

- run `npm init` and update the fields
  - Choose a name like `koop-provider-foo`
- run `npm publish`
