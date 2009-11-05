// For some reason symmetricds is generating bad checksums for it's pom file
System.setProperty  "ivy.checksums", ""
grails.project.class.dir = "target/classes"
grails.project.test.class.dir = "target/test-classes"
grails.project.test.reports.dir = "target/test-reports"
grails.plugin.repos.distribution.symmetric="https://symmetricds.svn.sourceforge.net/svnroot/symmetricds/trunk/extensions"
grails.plugin.repos.resolveOrder=['symmetric','default','core']
grails.project.dependency.resolution = {
    inherits "global" // inherit Grails' default dependencies
    log "warn" // log level of Ivy resolver, either 'error', 'warn', 'info', 'debug' or 'verbose'  
    repositories {
        grailsHome()
        // uncomment the below to enable remote dependency resolution
        // from public Maven repositories
        mavenLocal()
        mavenCentral()
        mavenRepo "http://snapshots.repository.codehaus.org"
        mavenRepo "http://repository.codehaus.org"
        mavenRepo "http://download.java.net/maven/2/"
        mavenRepo "http://repository.jboss.com/maven2/"
    }
    dependencies {
        // specify dependencies here under either 'build', 'compile', 'runtime', 'test' or 'provided' scopes eg.        
        runtime (
        'org.jumpmind.symmetric:symmetric:2.0.beta.1',     
        'org.apache.ddlutils:ddlutils:1.0',
        'mx4j:mx4j-tools:3.0.1',
        'rome:rome:0.9',
        'org.jdom:jdom:1.1',
        'com.h2database:h2:1.2.122',
        'org.apache.derby:derby:10.4.2.0',
        'org.apache.derby:derbytools:10.4.2.0',
        'org.beanshell:bsh:2.0b4',
        'commons-betwixt:commons-betwixt:0.8',
        'commons-digester:commons-digester:1.7',
        'commons-math:commons-math:1.1') { 
            transitive=false
        }
    }
    
}
