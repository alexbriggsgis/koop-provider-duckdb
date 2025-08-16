
const Koop = require('@koopjs/koop-core');
const routes = require('./routes')
const plugins = require('./plugins')
const config = require('config');

// initiate a koop app
const koop = new Koop()
//koop.log.info(`DuckDB config ${config.mongodb.connectString}`)

// register koop plugins
plugins.forEach((plugin) => {
  koop.register(plugin.instance, plugin.options)
})

// add additional routes
routes.forEach((route) => {
  route.methods.forEach((method) => {
    koop.server[method](route.path, route.handler)
  })
})

// start the server
const port =  9001;
koop.server.listen(port, () => koop.log.info(`Koop server listening at ${port}`))
