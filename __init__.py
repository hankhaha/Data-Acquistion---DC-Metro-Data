#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###############################################################################

# import modules ##############################################################
from airflow.models import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from wmata.bus.bus_routes import bus_routes
from wmata.bus.bus_stops import bus_stops
from wmata.bus.bus_route_stops import bus_route_stops

from wmata.rail.rail_station_entrances import rail_station_entrances
from wmata.rail.rail_stations import rail_stations
from wmata.rail.rail_transit_estimates import rail_transit_estimates

import requests, pandas, time

# dag instantiation ###########################################################
default_args = {
    'owner': 'clo@dcpcsb.org',
    'start_date' : days_ago(1),
    'catchup' : False,
    'postgres_conn_id' : 'warehouse',
    'retries' : 1
}

sql_conn_id = 'warehouse'

MAIN_DAG_NAME = 'wmata_data_acquisition'
dag = DAG(
    MAIN_DAG_NAME,
    catchup=False,
    schedule_interval='0 0 1 * *',
    default_args=default_args)

###############################################################################

headers = {'api_key': 'a2d3eedb7b804592b09ed95ac11ffc9f'}

# bus data #####################################################################
bus_routes = bus_routes(dag)
bus_route_stops = bus_route_stops(dag)
bus_stops = bus_stops(dag)

# rail data ####################################################################
rail_station_entrances = rail_station_entrances(dag)
rail_stations = rail_stations(dag)
rail_transit_estimates = rail_transit_estimates(dag)

# dag order
bus_routes >> bus_route_stops
bus_stops >> bus_route_stops

rail_stations >> rail_station_entrances
rail_stations >> rail_transit_estimates