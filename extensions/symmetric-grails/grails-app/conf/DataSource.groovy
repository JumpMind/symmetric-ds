dataSource {
	pooled = true
	driverClassName = "org.h2.Driver"
    url = "jdbc:h2:file:target/h2/test"
	username = "sa"
	password = ""
}
hibernate {
    cache.use_second_level_cache=true
    cache.use_query_cache=true
    cache.provider_class='com.opensymphony.oscache.hibernate.OSCacheProvider'
}
// environment specific settings
environments {
	root {
		dataSource {
			url = "jdbc:h2:file:target/h2/root"
		}
	}
	client {
		dataSource {
			url = "jdbc:h2:file:target/h2/client"
		}
	}
	production {
		dataSource {
			dbCreate = "update"
			url = "jdbc:h2:file:target/h2/symmetric-prod"
		}
	}
}