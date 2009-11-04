
symmetric {
    auto.startup = true
    db.spring.bean.name='parent.dataSource'
    auto.insert.registration.svr.if.not.found=true
    web.base.servlet.path='/sync'
    sync.url = 'http://localhost:8080/symmetric-grails/sync'
    external.id = 'setme'
    group.id = 'setme'    
}

environments {
    root {
        symmetric {
            sync.url = 'http://localhost:8080/symmetric-grails/sync'
            external.id = 'root'
            group.id = 'root'     
            auto.registration=true            
            auto.reload=true
            auto.config.registration.svr.sql.script='classpath:/test-root-config.sql'
        }
    }
    client {
        symmetric {
            sync.url = 'http://localhost:8090/symmetric-grails/sync'
            external.id = 'client'
            group.id = 'client'
            registration.url='http://localhost:8080/symmetric-grails/sync'
        }
    }    
}