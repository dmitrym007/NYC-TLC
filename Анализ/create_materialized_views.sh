#!/bin/bash

parent_path=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd -P)
cd "$parent_path"

clickhouse-client --database=nyc_tlc_data --queries-file=create_materialized_views.sql --progress