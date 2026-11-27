pipeline {
    agent none

    stages {
        stage('Lint') {
            agent {
                docker {
                    image 'golang:1.27'
                }
            }
            steps {
                sh 'golangci-lint run'
            }
        }

        stage('Test') {
            agent {
                docker {
                    image 'golang:1.27'
                    args '-v /var/run/docker.sock:/var/run/docker.sock'
                }
            }
            options {
                timeout(time: 15, unit: 'MINUTES')
            }
            steps {
                sh '''
                    set -o pipefail
                    go test ./... 2>&1 | tee test-output.txt
                    go test -tags=integration -race ./... 2>&1 | tee -a test-output.txt
                '''
            }
        }

        stage('Docker Build') {
            agent any
            steps {
                sh "docker build -t eventpulse:${env.BUILD_NUMBER} ."
            }
        }
    }

    post {
        always {
            archiveArtifacts artifacts: 'test-output.txt', allowEmptyArchive: true
        }
    }
}
