pipeline {
    agent none

    stages {
        stage('Lint') {
            agent {
                docker {
                    image 'golangci/golangci-lint:v2.15.0'
                }
            }
            steps {
                sh 'golangci-lint run --build-tags=integration'
            }
        }

        stage('Test') {
            agent {
                docker {
                    image 'golang:1.27'
                    args '-v /var/run/docker.sock:/var/run/docker.sock'
                }
            }
            environment {
                GOCACHE = '/tmp/.cache'
                GOPATH  = '/tmp/go'
            }
            options {
                timeout(time: 15, unit: 'MINUTES')
            }
            steps {
                sh '''#!/bin/bash
                    set -o pipefail
                    go test -tags=integration -race -timeout 12m ./... 2>&1 | tee test-output.txt
                '''
            }
            post {
                always {
                    archiveArtifacts artifacts: 'test-output.txt', allowEmptyArchive: true
                }
            }
        }

        stage('Docker Build') {
            agent any
            steps {
                sh "docker build -t eventpulse:${env.BUILD_NUMBER} ."
            }
        }
    }
}
