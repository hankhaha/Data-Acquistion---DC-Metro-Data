#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from wmata.common import headers, sql_hook, return_data, write_api_table, execute_sql_statement, drop_api_table
import requests, pandas, time
# wmata.bus_stops ##############################################################
def bus_stops(dag):

    def _bus_stops(ds,**kwargs):
        # retrieve data from wmata api
        bus_stops = requests.get('http://api.wmata.com/Bus.svc/json/jStops', headers)
        bus_stops = return_data(bus_stops, 'Stops')

        bus_stops = bus_stops.loc[:,['StopID', 'Name', 'Lat', 'Lon']]
        bus_stops.loc[:, 'Name'] = bus_stops.loc[:, 'Name'].str.upper()
        bus_stops = bus_stops.rename(axis = 'columns', mapper = {
        'StopID' : 'wmata_stop_id',
        'Name' : 'stop',
        'Lat' : 'latitude',
        'Lon' : 'longitude'})
        # lookup address_ids
        bus_stops = pandas.merge(
                        bus_stops,
                        pandas.read_sql('''
                            SELECT
                              address_id
                            , latitude
                            , longitude
                            FROM map.master_addresses
                            ''',
                            con = sql_hook.get_sqlalchemy_engine()),
                        how = 'left', on = ['latitude', 'longitude'], indicator=True)
        # keep the rows with matched latitudes and longitudes against map.master_addresses
        bus_stops = bus_stops[bus_stops._merge == 'both']
        bus_stops_unknown = bus_stops[~(bus_stops._merge == 'both')]
        bus_stops = bus_stops.loc[:, [    'wmata_stop_id'
                                        , 'stop'
                                        , 'address_id']]
        write_api_table(bus_stops, 'wmata', 'bus_stops')
        # handle unmatched latitude & longitude
        # store the unknown locations
        bus_stops_unknown['wmata_code'] = bus_stops_unknown['wmata_stop_id']
        bus_stops_unknown['location_type'] = 'bus_stops'
        bus_stops_unknown = bus_stops_unknown.loc[:, [    'location_type'
                                                        , 'wmata_code'
                                                        , 'latitude'
                                                        , 'longitude']]
        write_api_table(bus_stops_unknown, 'wmata','bus_stops_unknown')
        # store the unknown location data in `wmata.unknown_locations`
        store_unknown_location = [
        '''
        TRUNCATE TABLE wmata.unknown_locations;
        '''
        ,
        '''
        INSERT INTO wmata.unknown_locations (
              location_type
            , wmata_code
            , latitude
            , longitude
        )
        SELECT
              location_type
            , wmata_code
            , latitude
            , longitude
        FROM wmata.bus_stops_unknown_api
        '''
        ]

        for s in store_unknown_location:
            execute_sql_statement(s)

        drop_api_table('wmata', 'bus_stops_unknown')
        # write the error message
        if bus_stops_unknown.shape[0] > 0:
            raise Exception('There are {} unknown locations of bus stops'.format(bus_stops_unknown.shape[0]))

        # Check if the stop existed before, if so, make the current value to be TRUE
        update_reopened_stops_sql = \
        '''
        UPDATE wmata.bus_stops AS e
        SET current = TRUE
        WHERE e.current = FALSE
        AND EXISTS (
            SELECT 1
            FROM wmata.bus_stops_api AS n
            WHERE e.wmata_stop_id = n.wmata_stop_id
            AND e.address_id = n.address_id
            )
        '''
        # first scenario: add a new bus stop to wmata.bus_stops
        add_bus_stops_sql = \
        '''
        INSERT INTO wmata.bus_stops (
              wmata_stop_id
            , stop
            , address_id
        )(
            SELECT
                  n.wmata_stop_id
                , n.stop
                , n.address_id
            FROM wmata.bus_stops_api AS n
            LEFT JOIN (
                SELECT
                      wmata_stop_id
                    , stop
                    , address_id
                FROM wmata.bus_stops
                WHERE current
            ) AS e USING (wmata_stop_id, address_id)
            WHERE e.wmata_stop_id IS NULL
        )
        '''
        execute_sql_statement(add_bus_stops_sql)

        # second scenario: update the name of the bus stop
        update_bus_stops_name_sql = \
        '''
        UPDATE wmata.bus_stops AS e
        SET stop = (
                SELECT n.stop
                FROM wmata.bus_stops_api AS n
                WHERE n.wmata_stop_id = e.wmata_stop_id
                AND   n.address_id = e.address_id
                )
        WHERE e.current
        AND EXISTS (
            SELECT 1
            FROM wmata.bus_stops_api AS n
            WHERE n.wmata_stop_id = e.wmata_stop_id
            AND   n.address_id = e.address_id
            )
        '''
        execute_sql_statement(update_bus_stops_name_sql)

        # third scenario: take out the old bus stops
        mark_expired_stop = \
        '''
        UPDATE wmata.bus_stops AS e
        SET current = FALSE
        WHERE NOT EXISTS (
            SELECT 1
            FROM wmata.bus_stops_api AS n
            WHERE e.wmata_stop_id = n.wmata_stop_id
            )
        '''
        execute_sql_statement(mark_expired_stop)
        drop_api_table('wmata', 'bus_stops')

    return PythonOperator(
        task_id='bus_stops',
        python_callable=_bus_stops,
        provide_context=True,
        dag=dag
        )