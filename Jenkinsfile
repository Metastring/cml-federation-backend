pipeline {
    agent any

    environment {
        PORT = '8000'
        HOST = '0.0.0.0'
        PYTHON_ENV = "${WORKSPACE}/env"
        VENV_ACTIVATE = "${PYTHON_ENV}/bin/activate"
    }

    stages {
        stage('Checkout') {
            steps {
                echo 'Checking out code...'
                checkout scm
            }
        }

        stage('Setup Environment') {
            steps {
                echo 'Setting up Python virtual environment...'
                sh '''
                    if [ ! -d "env" ]; then
                        python3 -m venv env
                    fi
                    . ${VENV_ACTIVATE}
                    pip install --upgrade pip
                    pip install -r requirements.txt
                '''
            }
        }

        stage('Run Tests') {
            steps {
                echo 'Running tests...'
                sh '''
                    . ${VENV_ACTIVATE}
                    # Uncomment if you have pytest configured
                    # pytest tests/
                    echo "Tests completed (configure pytest as needed)"
                '''
            }
        }

        stage('Deploy') {
            steps {
                echo 'Starting FastAPI application...'
                sh '''
                    . ${VENV_ACTIVATE}
                    # Kill any existing process on port 8000
                    lsof -ti :${PORT} | xargs kill -9 2>/dev/null || true
                    sleep 2
                    
                    # Start the application
                    nohup uvicorn app.main:app \
                        --host ${HOST} \
                        --port ${PORT} \
                        --log-level info \
                        > ${WORKSPACE}/app-${BUILD_NUMBER}.log 2>&1 &
                    
                    # Give it time to start
                    sleep 3
                    
                    # Check if it started successfully
                    if lsof -Pi :${PORT} -sTCP:LISTEN -t >/dev/null ; then
                        echo "Application successfully started on port ${PORT}"
                    else
                        echo "Failed to start application"
                        cat ${WORKSPACE}/app-${BUILD_NUMBER}.log
                        exit 1
                    fi
                '''
            }
        }
    }

    post {
        success {
            echo 'Pipeline executed successfully!'
            sh '''
                . ${VENV_ACTIVATE}
                echo "Application running on http://0.0.0.0:${PORT}"
                echo "API docs available at http://0.0.0.0:${PORT}/docs"
            '''
        }
        failure {
            echo 'Pipeline failed!'
            sh '''
                echo "=== Recent logs ==="
                tail -50 ${WORKSPACE}/app-${BUILD_NUMBER}.log 2>/dev/null || echo "No logs available"
            '''
        }
        always {
            echo 'Cleaning up workspace...'
            cleanWs()
        }
    }
}
