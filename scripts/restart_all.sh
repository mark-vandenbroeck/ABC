#!/bin/bash
make stop
python scripts/reset_db.py
make start
