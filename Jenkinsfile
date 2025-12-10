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
                    //def runCMD = isUnix() ? {cmd -> sh cmd} : {cmd -> bat cmd}
                    //CMD = isUnix() ? 'sh' : 'bat'
                    //echo "Commande utilisée :" ${CMD}

                    if (isUnix()) {
                        comande = 'sh'
//                      bat 'echo "Running on Unix"'
// 						bat 'pip install -r test/requirements.txt'
                    } else {
                        comande = 'bat'
                        //bat 'echo "Running on Windows"'
						//bat 'pip install -r test/requirements.txt'
                        // Add your Windows-specific build commands here
                    }
                    echo "Commande utilisée : ${comande}"
                }
            }

		}
		stage('Set up Docker Compose'){
			steps{
			    script{
                    this."${comande}" 'docker-compose -f docker-compose.ci.yml build'
                    this."${comande}" 'docker-compose -f docker-compose.ci.yml up -d'
                    this."${comande}" (isUnix() ? 'sleep 30' : 'timeout /T 30')
    // 				runCMD('docker-compose -f docker-compose.ci.yml build')
    //              runCMD('docker-compose -f docker-compose.ci.yml up -d')
    // 				runCMD((isUnix() ? 'sleep 30' : 'timeout /T 30'))
			    }
			}
		}

		stage('Run unit tests'){
			steps{
			    script{
                    this."${comande}" 'pytest -v tests/test_spark_functions.py'
                    this."${comande}" (isUnix() ? 'sleep 30' : 'timeout /T 30')
    // 				runCMD('pytest -v tests/test_spark_functions.py')
    // 				runCMD((isUnix() ? 'sleep 30' : 'timeout /T 30'))
			    }
			}
		}

		stage('Run integration tests'){
			steps{
			    script{
                    this."${comande}" 'pytest -v tests/test_kafka_integrations.py'
                    this."${comande}" (isUnix() ? 'sleep 30' : 'timeout /T 30')
    // 				runCMD('pytest -v tests/test_kafka_integrations.py')
    // 				runCMD((isUnix() ? 'sleep 30' : 'timeout /T 30'))
			    }
			}
		}

		stage('end test shutdown docker-compose services'){
			steps {
			    script{
                   this."${comande}" 'docker-compose -f docker-compose.ci.yml down'
    //             runCMD('docker-compose -f docker-compose.ci.yml down')
			    }
            }
		}
    }
}
