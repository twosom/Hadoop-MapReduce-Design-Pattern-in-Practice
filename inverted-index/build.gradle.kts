dependencies {
    implementation("org.jsoup:jsoup:1.15.3")
}

tasks.jar {
    manifest {
        attributes("Main-Class" to "com.icloud.WikipediaExtractorDriver")
    }
}