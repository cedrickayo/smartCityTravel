pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
               git branch: 'main',
                   url: 'https://github.com/cedrickayo/smartCityTravel.git',
                   credentialsId: '450d3f5d-b61a-4c4b-af1c-2cb4956d8adc'
				}
			}
		stage('Installation des dependances'){
			steps {
                script {
                    if (isUnix()) {
                        bat 'echo "Running on Unix"'
						bat 'pip install -r test/requirements.txt'
                    } else {
                        bat 'echo "Running on Windows"'
						//bat 'pip install -r test/requirements.txt'
                        // Add your Windows-specific build commands here
                    }
                }
            bat 'pip install -r test/requirements.txt'
            }

		}
		stage('Set up Docker Compose'){
			steps{
				bat 'docker-compose -f docker-compose.ci.yml build'
                bat 'docker-compose -f docker-compose.ci.yml up -d'
				bat 'sleep 30'
			}
		}

		stage('Run unit tests'){
			steps{
				bat 'pytest -v tests/test_spark_functions.py'
				bat 'sleep 30'
			}

		}
		stage('Run integration tests'){
			steps{
				bat 'pytest -v tests/test_kafka_integrations.py'
				bat 'sleep 30'
			}

		}

		stage('end test shutdown docker-compose services'){
			steps {
                bat 'docker-compose -f docker-compose.ci.yml down'
            }

		}


    }
}
