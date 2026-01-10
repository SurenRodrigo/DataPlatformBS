#!/bin/bash
dbt snapshot --threads 6
dbt seed -f --threads 6
dbt run --full-refresh --exclude  tag:push_candidate_tables tag:push_ditio_candidate_tables  --threads 6