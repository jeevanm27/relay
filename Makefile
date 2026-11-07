# Convenience wrappers. No make on Windows? Run the node commands directly (they are the whole recipe).
N ?= 100000
WORKERS ?= 8

.PHONY: build test chaos bench up down

build:
	mvn -B -DskipTests package

test:
	mvn -B verify

# make chaos N=1000000      (needs: mvn package, Docker, Node 18+)
chaos: build
	node bench/chaos/chaos.mjs --n $(N) --workers $(WORKERS)

bench: build
	node bench/run.mjs

up: build
	docker compose -f deploy/docker-compose.yml up -d --build

down:
	docker compose -f deploy/docker-compose.yml down -v
