dataSource {
	pooled = true
	driverClassName = "org.h2.Driver"
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
	development {
		dataSource {
			dbCreate = "create-drop" // one of 'create', 'create-drop','update'
			url = "jdbc:h2:file:target/h2/symmetric-dev"
		}
	}
	test {
		dataSource {
			dbCreate = "update"
			url = "jdbc:h2:file:target/h2/symmetric-test"
		}
	}
	production {
		dataSource {
			dbCreate = "update"
			url = "jdbc:h2:file:target/h2/symmetric-prod"
		}
	}
}