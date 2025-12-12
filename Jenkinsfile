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

        stage('Loading .env secret'){
            steps{
                withCredentials([file(credentialsId:'6898428c-8f59-4396-b2db-c75599ce7b8f',variable: 'ENV_FILE')]){
                    script{
                        // load .env file in
                        def props = readProperties file: "${ENV_FILE}"

                        // Injecter chaque variable dans l'env global de Jenkins
                        props.each{
                            key, value -> env."${key}" = value
                        }

                        echo "🔐 Variables .env chargées et disponibles dans tout le pipeline"
                    }
                }
            }
        }

        stage('Utilisation des variables'){
            environment{
                INFLUXDB_ADMIN_USER="${env.INFLUXDB_ADMIN_USER}"
                INFLUXDB_ADMIN_PASSWORD="${env.INFLUXDB_ADMIN_PASSWORD}"
                INFLUXDB_TOKEN="${env.INFLUXDB_TOKEN}"
                INFLUXDB_ORG="${env.INFLUXDB_ORG}"
                INFLUXDB_BUCKET="${env.INFLUXDB_BUCKET}"
                INFLUXDB_USER="${env.INFLUXDB_USER}"
                INFLUXDB_USER_PASSWORD="${env.INFLUXDB_USER_PASSWORD}"
            }
            steps{
                echo "Org = $INFLUXDB_ORG"
            }
        }

        stage('Initialisation des variables'){
            steps {
                script {
                    // definition de lavariable selon l'environnement
                    //def runCMD = isUnix() ? {cmd -> sh cmd} : {cmd -> bat cmd}
                    //def comande = isUnix() ? 'sh' : 'bat' declaration d'une  variable locale au stage, mettre env.CMD = comande puis remplacer commande par env.CMD dans this.$"{comande}"
                    //echo "Commande utilisée :" ${CMD}

                    if (isUnix()) {
                        comande = 'sh'
//                      bat 'echo "Running on Unix"'
//                         bat 'pip install -r test/requirements.txt'
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
    //              runCMD('docker-compose -f docker-compose.ci.yml build')
    //              runCMD('docker-compose -f docker-compose.ci.yml up -d')
    //              runCMD((isUnix() ? 'sleep 30' : 'timeout /T 30'))
                }
            }
        }

        stage('Run unit tests'){
            steps{
                script{
                    this."${comande}" 'pytest -v tests/test_spark_functions.py'
                    this."${comande}" (isUnix() ? 'sleep 30' : 'timeout /T 30')
    //              runCMD('pytest -v tests/test_spark_functions.py')
    //              runCMD((isUnix() ? 'sleep 30' : 'timeout /T 30'))
                }
            }
        }

        stage('Run integration tests'){
            steps{
                script{
                    this."${comande}" 'pytest -v tests/test_kafka_integrations.py'
                    this."${comande}" (isUnix() ? 'sleep 30' : 'timeout /T 30')
    //              runCMD('pytest -v tests/test_kafka_integrations.py')
    //              runCMD((isUnix() ? 'sleep 30' : 'timeout /T 30'))
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
