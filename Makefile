# FinStream Developer Tools

COMPOSE_FILE=docker/docker-compose.yml

.PHONY: up down restart-schema logs ps

# Start the local cluster
up:
	docker compose -f $(COMPOSE_FILE) up -d

# Stop and remove the local cluster
down:
	docker compose -f $(COMPOSE_FILE) down

# Restart just the Schema Registry (for crash recovery)
restart-schema:
	docker compose -f $(COMPOSE_FILE) restart schema-registry

# View logs for all containers in real-time
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

# Check the status of the containers
ps:
	docker ps -a

stop:
	docker compose -f $(COMPOSE_FILE) stop

# Wake up a paused cluster
start:
	docker compose -f $(COMPOSE_FILE) start

airflow-up:
	docker compose -f docker/airflow-compose.yml up -d

# Stop the Airflow orchestration layer
airflow-start:
	docker compose -f docker/airflow-compose.yml start

# Stop Airflow (pauses the containers without deleting them)
airflow-stop:
	docker compose -f docker/airflow-compose.yml stop

# The full teardown (deletes the containers)
airflow-down:
	docker compose -f docker/airflow-compose.yml down