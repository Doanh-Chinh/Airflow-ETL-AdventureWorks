# Setups
repo_dirs:
	bash ./scripts/create_repo_dirs.sh
download_data: repo_dirs
	bash ./scripts/download_data.sh ./data
setup: repo_dirs download_data

# Docker-compose related
up: setup
	docker compose up -d
down:
	docker compose down
start:
	docker compose start
stop:
	docker compose stop

# Make shells
airflow_shell:
	docker exec -it etl-adventureworks-airflow-1 /bin/bash
beeline:
	docker exec -it etl-adventureworks-hive-server-1 beeline -u 'jdbc:hive2://hive-server:10000'	