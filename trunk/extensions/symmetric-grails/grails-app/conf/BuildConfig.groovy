grails.project.class.dir = "target/classes"
grails.project.test.class.dir = "target/test-classes"
grails.project.test.reports.dir = "target/test-reports"
grails.project.dependency.resolution = {
    inherits "global" // inherit Grails' default dependencies
    log "warn" // log level of Ivy resolver, either 'error', 'warn', 'info', 'debug' or 'verbose'  
    repositories {
        grailsHome()
        // uncomment the below to enable remote dependency resolution
        // from public Maven repositories
        mavenLocal()
        //mavenCentral()
        //mavenRepo "http://snapshots.repository.codehaus.org"
        //mavenRepo "http://repository.codehaus.org"
        //mavenRepo "http://download.java.net/maven/2/"
        //mavenRepo "http://repository.jboss.com/maven2/"
    }
    dependencies {
        // specify dependencies here under either 'build', 'compile', 'runtime', 'test' or 'provided' scopes eg.
        // runtime 'com.mysql:mysql-connector-java:5.1.5'
        runtime 'org.jumpmind.symmetric:symmetric:2.0.0-SNAPSHOT'     
        runtime 'org.apache.ddlutils:ddlutils:1.0'
        runtime 'mx4j:mx4j-tools:3.0.1'
        runtime 'rome:rome:0.9'
        runtime 'org.jdom:jdom:1.1'
        runtime 'com.h2database:h2:1.2.122'
        runtime 'org.apache.derby:derby:10.4.2.0'
        runtime 'org.apache.derby:derbytools:10.4.2.0'
        runtime 'org.beanshell:bsh:2.0b4'
        runtime 'commons-betwixt:commons-betwixt:0.8'
        runtime 'commons-digester:commons-digester:1.7'
    }

}
