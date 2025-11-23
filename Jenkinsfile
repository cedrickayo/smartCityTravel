pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
               git branch: 'main', 
                   url: https://github.com/cedrickayo/smartCityTravel.git,
                   credentialsId: '450d3f5d-b61a-4c4b-af1c-2cb4956d8adc'
				}
			}
		stage('Installation des dependances'){
			steps {
                script {
                    if (isUnix()) {
                        sh 'echo "Running on Unix"'
						sh 'pip install -r test/requirements.txt'
                    } else {
                        bat 'echo "Running on Windows"'
                        // Add your Windows-specific build commands here
                    }
                }

            }
		
		}
		stage('Set up Docker Compose'){
			steps{
				sh 'docker-compose -f docker-compose.ci.yml build'
                sh 'docker-compose -f docker-compose.ci.yml up -d'
				sh 'sleep 30'
			}
		}
		
		stage('Run unit tests'){
			steps{
				sh 'pytest -v tests/test_spark_functions.py'
				sh 'sleep 30'
			}
		
		}
		stage('Run integration tests'){
			steps{
				sh 'pytest -v tests/test_kafka_integrations.py'
				sh 'sleep 30'
			}
		
		}
		
		stage('end test shutdown docker-compose services'){
			steps {
                sh 'docker-compose -f docker-compose.ci.yml down'
            }
		
		}
		
		
    }
}
