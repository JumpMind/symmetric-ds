def syncPath = '/sync'

symmetric {

  // Each Grails application that embeds SymmetricDS should set this field.  The external id is used usually used as all or part of the node id.
  external.id = 'setme'

  // Each Grails application that embeds SymmetricDS should set this field.  The node group id is used to setup which nodes synchronize to each other.
  group.id = 'setme'

  // Grails specific parameter.  Indicates whether SymmetricDS should be started when Grails is bootstrapped.
  auto.startup = false

  // force SymmetricDS to look in parent application context in order to use the Grails DataSource
  db.spring.bean.name = 'parent.dataSource'

  // This is the servlet path that is used for SymmetricDS HTTP syncrhonizations.  Do not override this property.
  web.base.servlet.path = syncPath

  // Indicates that the server configuration should be auto inserted into the database if it doesn't already exist.
  auto.insert.registration.svr.if.not.found = true

  // The URL of SymmetricDS.
  sync.url = 'http://localhost:8080/symmetric' + syncPath

  // When symmetric tables are created and accessed, this is the prefix to use for the tables.
  sync.table.prefix = 'sym'

  // If this is true, when symmetric starts up it will try to create the necessary tables
  auto.config.database = true

  // If this is true. when symmetric starts up it will make sure the triggers in the database are up to date
  auto.sync.triggers = true

  // This is how often the push job will be run.
  job.push.period.time.ms = 60000

  // This is how often the pull job will be run.
  job.pull.period.time.ms = 60000

  // This is how often the purge job will be run.
  job.purge.period.time.ms = 600000

  // This is how often accumulated statistics will be flushed out to the database from memory
  job.stat.flush.period.time.ms = 600000

  // This is how often the router will run in the background
  job.routing.period.time.ms = 10000

}

environments {
  root {
    symmetric {
      auto.startup = true
      external.id = 'root'
      sync.url = 'http://localhost:8080/symmetric' + syncPath
      group.id = 'root'
      auto.registration = true
      auto.reload = true
      auto.config.registration.svr.sql.script = '/test-root-config.sql'
    }
  }
  client {
    symmetric {
      auto.startup = true
      external.id = 'client'
      group.id = 'client'
      sync.url = 'http://localhost:8090/symmetric' + syncPath
      registration.url = 'http://localhost:8080/symmetric' + syncPath
    }
  }
  development {
    symmetric {
      auto.startup = true
      external.id = 'root'
      sync.url = 'http://localhost:8080/symmetric' + syncPath
      group.id = 'root'
      auto.registration = true
      auto.reload = true
      auto.config.registration.svr.sql.script = '/dev-server-config.sql'
    }
  }
}