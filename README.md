Periodic shared task execution service
=====

Project description
----------
The project utilizes a shared task queue in Redis and supports multiple independent workers for easy scaling.

The service is deployed in the following Docker containers:

PostgreSQL database
Redis database
One or more workers for executing scheduled tasks
Error logging is configured, and task execution history, including statuses, is stored in the database.

System requirements
----------

* Python 3.12+
* Docker
* Works on Linux

Technology stack
----------

* Python 3.12+
* SQLAlchemy 2.0.38
* alembic 1.14.1
* pydantic 2.10.6
* Postgres 15.2
* Redis 7.2.0
* Docker, Docker Compose

Installing the project from the repository
----------
1. Clone the repository:
```bash
git clone git@github.com:NikitaChalykh/scheduler_task_service.git

cd scheduler_task_service # Go to the directory with the project
```

2. Create the ```.env``` file using ```env.example``` as a template

3. Installing and running the service in a container:
```bash
docker-compose up -d # To run one worker

docker-compose up --scale scheduler=2 -d # To run multiple workers (e.g. two)
```

4. Viewing logs:
```bash
docker exec -it scheduler-service log # Logs when running one worker

docker exec -it scheduler-service_1 log # Logs when running multiple workers (specify worker number)
```