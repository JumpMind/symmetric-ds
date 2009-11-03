
symmetric {
    SymmetricConfig.startup = true
    sync.url = 'http://localhost:8080/symmetric-grails/sync'
    external.id = 'setme'
    group.id = 'setme'    
}

environments {
    development {
        symmetric {
            auto.startup = true            
            sync.url = 'http://localhost:8080/symmetric-grails/sync'
            external.id = 'test-external'
            group.id = 'test-group'            
        }
    }
}