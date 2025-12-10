pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
               git branch: 'main',
                   url: 'https://github.com/cedrickayo/smartCityTravel.git',
                   credentialsId: '0f9c7855-b671-4909-936f-d6d5c1613821'
				}
			}
		stage('Initialisation des variables'){
			steps {
                script {
                    // definition de lavariable selon l'environnement
                    CMD = isUnix() ? "sh" : "bat"
                    echo "Commande utilisée : ${CMD}"

//                     if (isUnix()) {
//                         bat 'echo "Running on Unix"'
// 						bat 'pip install -r test/requirements.txt'
//                     } else {
//                         bat 'echo "Running on Windows"'
// 						//bat 'pip install -r test/requirements.txt'
//                         // Add your Windows-specific build commands here
//                     }
                }
            }

		}
		stage('Set up Docker Compose'){
			steps{
				this."${CMD}" 'docker-compose -f docker-compose.ci.yml build'
                this."${CMD}" 'docker-compose -f docker-compose.ci.yml up -d'
				this."${CMD}" (isUnix() ? 'sleep 30' : 'timeout /T 30')
			}
		}

		stage('Run unit tests'){
			steps{
				this."${CMD}" 'pytest -v tests/test_spark_functions.py'
				this."${CMD}" (isUnix() ? 'sleep 30' : 'timeout /T 30')
			}

		}
		stage('Run integration tests'){
			steps{
				this."${CMD}" 'pytest -v tests/test_kafka_integrations.py'
				this."${CMD}" (isUnix() ? 'sleep 30' : 'timeout /T 30')
			}

		}

		stage('end test shutdown docker-compose services'){
			steps {
                this."${CMD}" 'docker-compose -f docker-compose.ci.yml down'
            }

		}
    }
}
