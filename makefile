# Variables
CONTAINER_NAME = postgres-container
POSTGRES_USER = root
POSTGRES_PASSWORD = password
POSTGRES_DB = testdb
POSTGRES_IMAGE = postgres
POSTGRES_PORT = 5432

# SQL Files
SCHEMA_FILE = schema.sql

# Docker commands
.PHONY: all build start stop remove recreate reset exec run-alter-schema

all: build start setup

build:
	@echo "Building and running the PostgreSQL container..."
	docker run --name $(CONTAINER_NAME) \
		-e POSTGRES_USER=$(POSTGRES_USER) \
		-e POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
		-e POSTGRES_DB=$(POSTGRES_DB) \
		-p $(POSTGRES_PORT):5432 \
		-d $(POSTGRES_IMAGE)

start:
	@echo "Starting the PostgreSQL container..."
	docker start $(CONTAINER_NAME)

stop:
	@echo "Stopping the PostgreSQL container..."
	docker stop $(CONTAINER_NAME)

remove:
	@echo "Removing the PostgreSQL container..."
	docker rm -f $(CONTAINER_NAME)

recreate: remove build

reset:
	@echo "Resetting the database..."
	docker exec -i $(CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

exec:
	@echo "Accessing PostgreSQL interactive terminal..."
	docker exec -it $(CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

setup:
	@echo "Setting up the database schema with file ${SCHEMA_FILE}"
	docker exec -i $(CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) < $(SCHEMA_FILE)

run-alter-schema:
	@echo "Altering the database schema..."
	docker exec -i $(CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "$$ALTER_SQL"

# Run on a connected environment to download all of the dependencies locally such that they can be decompressed
# in a disconnected environment and installed in order to execute the utility
offline-package:
	@echo "Freezing pip dependencies into requirements.txt"
	pip freeze > requirements.txt
	@echo "Downloading pip dependencies to vendor directory"
	pip download --requirement requirements.txt --dest vendor
	@echo "Compressing vendor directory into vendor.tar.gz"
	tar -czf vendor.tar.gz vendor
	@echo "Offline package created: vendor.tar.gz"

# Deletes the downloaded/packaged dependencies
clean-vendor:
	@echo "Cleaning up vendor directory and tarball"
	rm -rf vendor vendor.tar.gz
	@echo "Cleanup completed"

# Installation for dependencies in a disconnected environment
install-offline:
	@echo "Installing pip dependencies for data ingestion utility"
	tar -xzf vendor.tar.gz
	pip install --no-index --find-links=vendor -r requirements.txt

