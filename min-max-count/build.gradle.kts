/**
 * 실행 가능한 Jar 파일 생성을 위해 Main Class 지정
 */
tasks.jar {
    manifest {
        attributes("Main-Class" to "com.icloud.MinMaxCountDriver")
    }
}