up:
	docker network create multi-subsidary-net 2>/dev/null || true
	docker-compose -f docker-compose.yml -f docker-compose.dept.yml up -d

down:
	docker-compose -f docker-compose.yml -f docker-compose.dept.yml down -v --remove-orphans

build:
	docker-compose -f docker-compose.yml -f docker-compose.dept.yml up -d --build