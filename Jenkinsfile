pipeline {
    agent any
    stages {
        stage('Unit tests') {
            steps { sh 'mvn -B test' }
        }
        stage('Kafka + ClickHouse integration test') {
            steps { sh 'mvn -B verify' } // needs Docker on the agent
        }
    }
    post {
        always { junit allowEmptyResults: true, testResults: 'target/*-reports/*.xml' }
    }
}
