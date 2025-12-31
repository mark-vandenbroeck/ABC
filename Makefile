SHELL := /bin/bash
.PHONY: start stop restart status

start:
	@bash scripts/start_all.sh

stop:
	@bash scripts/stop_all.sh

restart: stop start
	@echo "Restarted"

status:
	@bash scripts/status.sh
