task("build-v1") {
    dependsOn("build")
    tasks.jar {
        manifest {
            attributes("Main-Class" to "com.icloud.v1.CommentsMedianStdDriver")
        }
    }
}

task("build-v2") {
    dependsOn("build")
    tasks.jar {
        manifest {
            attributes("Main-Class" to "com.icloud.v2.CommentsMedianStdDriver")
        }
    }
}