#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from wmata.common import headers, sql_hook, warehouse, return_data, write_api_table, execute_sql_statement, drop_api_table
import requests, pandas ,numpy, time
# wmata.bus_route_stops #############################################################
def bus_route_stops(dag):

    def _bus_route_stops(ds,**kwargs):
        # retrieve bus_routes data
        # sql_hook = PostgresHook(postgres_conn_id='warehouse')
        # warehouse = PostgresHook(postgres_conn_id='warehouse').get_conn()
        bus_routes = pandas.read_sql(
            '''
            SELECT * FROM wmata.bus_routes WHERE current
            ''',
            con=warehouse)

        bus_route_stops = []
        for route in bus_routes.wmata_route_id.unique():
            payload = {'RouteID' : route }
            response = requests.get('http://api.wmata.com/Bus.svc/json/jRouteDetails',
                                        payload, headers = headers).json()
            processed = pandas.DataFrame()
            for key in (set(response.keys()) - set({'Name', 'RouteID'})):
                if response[key] is not None:
                    if 'Stops' in response[key].keys():
                        direction = pandas.io.json.json_normalize(
                                  response[key]).loc[0, 'DirectionText'].upper()
                        processed = pandas.io.json.json_normalize(response[key]['Stops'])
                        processed['wmata_route_id'] = route
                        processed['direction'] = direction
                        processed['stop_number'] = list(numpy.arange(1,len(response[key]['Stops'])+1))
                        bus_route_stops = bus_route_stops + [processed]
                time.sleep(.25) # wait for rate limiting

        bus_route_stops = pandas.concat(bus_route_stops)
        bus_route_stops = bus_route_stops.rename(axis = 'columns', mapper = {
        'StopID' : 'wmata_stop_id',
        'Name' : 'stop',
        'Lat' : 'latitude',
        'Lon' : 'longitude'})
        # bus_route_directions
        bus_route_directions = pandas.DataFrame(
                                  bus_route_stops.direction.unique(), columns = ['direction'])

        write_api_table(bus_route_directions, 'wmata', 'bus_route_directions')

        # update directions table with any new directions
        add_new_bus_route_directions_sql = \
        '''
        INSERT INTO wmata.bus_route_directions (direction)(
        SELECT n.direction
        FROM wmata.bus_route_directions_api AS n
        LEFT JOIN (
            SELECT direction
            FROM wmata.bus_route_directions
        ) AS e USING (direction)
        WHERE e.direction IS NULL
        )
        '''
        execute_sql_statement(add_new_bus_route_directions_sql)
        drop_api_table('wmata', 'bus_route_directions')

        # map direction_id
        bus_route_stops = pandas.merge(
                            bus_route_stops,
                            pandas.read_sql('''
                                  SELECT
                                        direction
                                      , direction_id
                                  FROM wmata.bus_route_directions
                                  ''',
                                  con = sql_hook.get_sqlalchemy_engine()),
                            how = 'left', on = 'direction')
        # map stop_id
        bus_route_stops = pandas.merge(
                            bus_route_stops,
                            pandas.read_sql('''
                                  SELECT
                                        stop_id
                                      , wmata_stop_id
                                  FROM wmata.bus_stops
                                  ''',
                                  con = sql_hook.get_sqlalchemy_engine()),
                            how = 'left', on = 'wmata_stop_id')
        # map route_id
        bus_route_stops = pandas.merge(
                            bus_route_stops,
                            pandas.read_sql('''
                                  SELECT
                                      route_id
                                    , wmata_route_id
                                  FROM wmata.bus_routes
                                  ''',
                                  con = sql_hook.get_sqlalchemy_engine()),
                            how = 'left', on = 'wmata_route_id')

        # make sure there are nulls in both stop_id & route_id
        bus_route_stops = bus_route_stops[
                            (bus_route_stops['stop_id'].notnull())
                          & (bus_route_stops['route_id'].notnull())
                          ]

        bus_route_stops = bus_route_stops.loc[:, [  'route_id'
                                                  , 'stop_id'
                                                  , 'direction_id'
                                                  , 'stop_number']]

        write_api_table(bus_route_stops, 'wmata', 'bus_route_stops')
        # update wmata.bus_route_stops
        # first scenario: add a new bus stop to wmata.bus_stops
        add_bus_route_stops_sql = \
        '''
        INSERT INTO wmata.bus_route_stops (
              route_id
            , stop_id
            , direction_id
            , stop_number
        )(
            SELECT
                n.route_id
              , n.stop_id
              , n.direction_id
              , n.stop_number
            FROM wmata.bus_route_stops_api AS n
            LEFT JOIN (
                SELECT
                    route_id
                  , stop_id
                  , direction_id
                  , stop_number
                FROM wmata.bus_route_stops
                WHERE current
            ) AS e USING (route_id, stop_id, direction_id)
            WHERE e.stop_id IS NULL OR e.route_id IS NULL
        )
        '''
        execute_sql_statement(add_bus_route_stops_sql)
        drop_api_table('wmata', 'bus_route_stops')

    return PythonOperator(
    task_id='bus_route_stops',
    python_callable=_bus_route_stops,
    provide_context=True,
    dag=dag
    )