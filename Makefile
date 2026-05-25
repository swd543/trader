refresh-db:
	@docker-compose -p trading -f docker-compose.yml down -v
	@rm -rf data
	@docker-compose -p trading -f docker-compose.yml up -d --remove-orphans