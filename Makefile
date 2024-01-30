PGSERV_DIR := ./02-test-intermediate/01-vol-pgserv-ny-taxi
PGADMIN_DIR := ./02-test-intermediate/02-vol-pgadmin

MOD01_LAB01_COMPOSE_FILE := ./04-docker/mod01-lab01-docker-compose.yaml

MOD01_LAB01_ETL01_CFG_SOURCE_FILE := ./02-test-intermediate/green_tripdata_2019-09.csv.gz
MOD01_LAB01_ETL01_CFG_DEST_TABLE := green_tripdata_2019_09

MOD01_LAB01_ETL02_CFG_SOURCE_FILE := https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
MOD01_LAB01_ETL02_CFG_DEST_TABLE := green_tripdata_2019_09

MOD01_LAB01_ETL03_CFG_SOURCE_FILE := ./02-test-intermediate/taxi+_zone_lookup.csv
MOD01_LAB01_ETL03_CFG_DEST_TABLE := taxi_plus_zone_lookup

MOD01_LAB01_ETL04_CFG_SOURCE_FILE := https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
MOD01_LAB01_ETL04_CFG_DEST_TABLE := taxi_plus_zone_lookup

.PHONY: help init-repo run-mod01-lab01-local-service down-mod01-lab01-local-service run-mod01-lab01-etl03-zone-lookup run-mod01-lab01-etl04-zone-lookup run-mod01-lab01-etl01-green-tripdata run-mod01-lab01-etl02-green-tripdata

help:
	@echo "Usage:"
	@echo "  make init-repo  - Initializes the repository by setting up necessary folder and config"
	@echo "  make run-mod01-lab01-local-service  - Start the Docker Compose service"
	@echo "  make down-mod01-lab01-local-service - Stop the Docker Compose service"
	@echo "  make run-mod01-lab01-etl01-green-tripdata  - Runs the ETL process for the Green Taxi Trip Data from local"
	@echo "  make run-mod01-lab01-etl02-green-tripdata  - Runs the ETL process for the Green Taxi Trip Data from URL"
	@echo "  make run-mod01-lab01-etl03-zone-lookup  - Runs the ETL process for the 'Zone Lookup' data from local"
	@echo "  make run-mod01-lab01-etl04-zone-lookup  - Runs the ETL process for the 'Zone Lookup' data from URL"

init-repo: init-pgserv init-pgadmin
	poetry install

init-pgserv:
	@if [ ! -d "$(PGSERV_DIR)" ]; then \
		echo "Creating directory $(PGSERV_DIR)..."; \
		mkdir -p $(PGSERV_DIR); \
	else \
		echo "Directory $(PGSERV_DIR) already exists."; \
	fi

init-pgadmin:
	@if [ ! -d "$(PGADMIN_DIR)" ]; then \
		echo "Creating directory $(PGADMIN_DIR) and setting permissions..."; \
		mkdir -p $(PGADMIN_DIR); \
		sudo chown -R 5050:5050 $(PGADMIN_DIR); \
		sudo chmod -R 700 $(PGADMIN_DIR); \
	else \
		echo "Directory $(PGADMIN_DIR) already exists."; \
	fi

run-mod01-lab01-local-service:
	@docker compose \
		-f $(MOD01_LAB01_COMPOSE_FILE) \
		--project-directory . \
		up

down-mod01-lab01-local-service:
	@docker compose \
		-f $(MOD01_LAB01_COMPOSE_FILE) \
		--project-directory . \
		down

run-mod01-lab01-etl01-green-tripdata:
	@poetry run \
		01-run-etl-green-tripdata \
		-ds $(MOD01_LAB01_ETL01_CFG_SOURCE_FILE) \
		-dt $(MOD01_LAB01_ETL01_CFG_DEST_TABLE)

run-mod01-lab01-etl02-green-tripdata:
	@poetry run \
		01-run-etl-green-tripdata \
		-ds $(MOD01_LAB01_ETL02_CFG_SOURCE_FILE) \
		-dt $(MOD01_LAB01_ETL02_CFG_DEST_TABLE)

run-mod01-lab01-etl03-zone-lookup:
	@poetry run \
		02-run-etl-zone-lookup \
		-ds $(MOD01_LAB01_ETL03_CFG_SOURCE_FILE) \
		-dt $(MOD01_LAB01_ETL03_CFG_DEST_TABLE)

run-mod01-lab01-etl04-zone-lookup:
	@poetry run \
		02-run-etl-zone-lookup \
		-ds $(MOD01_LAB01_ETL04_CFG_SOURCE_FILE) \
		-dt $(MOD01_LAB01_ETL04_CFG_DEST_TABLE)