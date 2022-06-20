I am here to satisfy the need to create a build/resources/main directory for Eclipse
because of the way that the classpath is created.

See symmetric-core/build.gradle, dependency on testImplementation project(':symmetric-jdbc').sourceSets.main.output