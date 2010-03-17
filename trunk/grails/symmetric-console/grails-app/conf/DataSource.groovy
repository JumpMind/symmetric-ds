dataSource {
  pooled = true
  maxActive = 10
  initialSize = 3
  driverClassName = "org.h2.Driver"
  url = "jdbc:h2:file:target/h2/test"
  username = "sa"
  password = ""
}
hibernate {
  cache.use_second_level_cache = false
  cache.use_query_cache = false
  cache.provider_class = 'net.sf.ehcache.hibernate.EhCacheProvider'
}
// environment specific settings
environments {
  root {
    dataSource {
      dbCreate = "update"
      url = "jdbc:h2:file:target/h2/root"
    }
  }
  client {
    dataSource {
      dbCreate = "update"
      url = "jdbc:h2:file:target/h2/client"
    }
  }
  development {
    dataSource {
      dbCreate = "update"
      url = "jdbc:h2:file:target/h2/dev"
    }
  }
}