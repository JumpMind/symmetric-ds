
symmetric {
    auto.startup = true
    db.spring.bean.name='parent.dataSource'
    auto.insert.registration.svr.if.not.found=true
    sync.url = 'http://localhost:8080/symmetric-grails/sync'
    external.id = 'setme'
    group.id = 'setme'    
}

environments {
    development {
        symmetric {
            sync.url = 'http://localhost:8080/symmetric-grails/sync'
            external.id = 'test-external'
            group.id = 'test-group'            
        }
    }
}