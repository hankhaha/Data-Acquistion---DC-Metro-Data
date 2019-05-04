#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from wmata.common import headers, sql_hook, return_data, write_api_table, execute_sql_statement, drop_api_table
import requests, pandas, time
# wmata.rail_stations ##########################################################
def rail_stations(dag):

    def _rail_stations(ds,**kwargs):
        # sql_hook = PostgresHook(postgres_conn_id='warehouse')
        # wmata.rail_stations ##################################################
        rail_stations = requests.get('http://api.wmata.com/Rail.svc/json/jStations', headers)
        rail_stations = return_data(rail_stations, 'Stations')

        rail_stations = rail_stations.loc[:, [  'Code'
                                              , 'Name'
                                              , 'Lat'
                                              , 'Lon'
                                              , 'LineCode1'
                                              , 'LineCode2'
                                              , 'LineCode3'
                                              , 'LineCode4']]
        # rail_lines_served
        rail_lines_served = pandas.melt(rail_stations,
                                id_vars = [  'Code'
                                           , 'Name'
                                           , 'Lat'
                                           , 'Lon'],
                                value_vars = [  'LineCode1'
                                              , 'LineCode2'
                                              , 'LineCode3'
                                              , 'LineCode4'],
                                var_name = 'Split',
                                value_name = 'LineCode').loc[:, [  'Code'
                                                                 , 'LineCode']]
        rail_lines_served = rail_lines_served[rail_lines_served['LineCode'].notnull()]
        rail_lines_served = rail_lines_served.rename(axis = 'columns', mapper = {
        'Code' : 'wmata_station_code',
        'LineCode' : 'wmata_line_code',})

        # rail stations
        rail_stations = rail_stations.loc[:, [  'Code'
                                              , 'Name'
                                              , 'Lat'
                                              , 'Lon']]

        rail_stations = rail_stations.rename(axis = 'columns', mapper = {
        'Code' : 'wmata_station_code',
        'Name' : 'station',
        'Lat' : 'latitude',
        'Lon' : 'longitude'})
        # uppercase the station name
        rail_stations.loc[:, 'station'] = rail_stations.loc[:, 'station'].str.upper()
        # map the address_id
        rail_stations = pandas.merge(
                          rail_stations,
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
        rail_stations = rail_stations[rail_stations._merge == 'both']
        rail_stations_unknown = rail_stations[~(rail_stations._merge == 'both')]
        rail_stations = rail_stations.loc[:, [  'wmata_station_code'
                                              , 'station'
                                              , 'address_id']]
        # write rail_stations_api
        write_api_table(rail_stations, 'wmata', 'rail_stations')
        # handle unmatched latitude & longitude
        # store the unknown locations
        rail_stations_unknown['wmata_code'] = rail_stations_unknown['wmata_station_code']
        rail_stations_unknown['location_type'] = 'rail_stations'
        rail_stations_unknown = rail_stations_unknown.loc[:, [  'location_type'
                                                              , 'wmata_code'
                                                              , 'latitude'
                                                              , 'longitude']]
        write_api_table(rail_stations_unknown, 'wmata','rail_stations_unknown')
        # store the unknown location data in `wmata.unknown_locations`
        store_unknown_location = \
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
        FROM wmata.rail_stations_unknown_api
        '''

        execute_sql_statement(store_unknown_location)
        drop_api_table('wmata', 'rail_stations_unknown')
        # write error message
        if rail_stations_unknown.shape[0] > 0:
            raise Exception('There are {} unknown locations of rail stations'.format(rail_stations_unknown.shape[0]))

        # if the station existed before, make the current value to be TRUE
        update_reopened_station_sql = \
        '''
        UPDATE wmata.rail_stations AS e
        SET current = TRUE
        WHERE e.current = FALSE
        AND EXISTS (
            SELECT 1
            FROM wmata.rail_stations_api AS n
            WHERE e.wmata_station_code = n.wmata_station_code
            )
        '''
        execute_sql_statement(update_reopened_station_sql)
        # first scenario: add a rail station to wmata.rail_stations
        add_new_rail_station_sql = \
        '''
        INSERT INTO wmata.rail_stations (
                      wmata_station_code
                    , station
                    , address_id
                    )(
        SELECT
              n.wmata_station_code
            , n.station
            , n.address_id
        FROM wmata.rail_stations_api AS n
        LEFT JOIN (
            SELECT
                    wmata_station_code
                  , station
                  , address_id
            FROM wmata.rail_stations
            WHERE current
        ) AS e USING (wmata_station_code)
        WHERE e.wmata_station_code IS NULL
        )
        '''
        execute_sql_statement(add_new_rail_station_sql)

        # second scenario: update the name of rail station
        update_rail_station_name_sql = \
        '''
        UPDATE wmata.rail_stations AS e
        SET station = (
                SELECT n.station
                FROM wmata.rail_stations_api AS n
                WHERE n.wmata_station_code = e.wmata_station_code
                AND   n.address_id = e.address_id
                )
        WHERE EXISTS (
            SELECT 1
            FROM wmata.rail_stations_api AS n
            WHERE n.wmata_station_code = e.wmata_station_code
            AND   n.address_id = e.address_id
            ) AND e.current
        '''
        execute_sql_statement(update_rail_station_name_sql)
        # third scenario: take out the old station out
        mark_expired_station = \
        '''
        UPDATE wmata.rail_stations AS e
        SET current = FALSE
        WHERE NOT EXISTS (
            SELECT 1
            FROM wmata.rail_stations_api AS n
            WHERE e.wmata_station_code = n.wmata_station_code
            )
        '''
        execute_sql_statement(mark_expired_station)
        drop_api_table('wmata', 'rail_stations')

        ## rail line############################################################
        rail_lines = requests.get('http://api.wmata.com/Rail.svc/json/jLines?', headers)
        rail_lines = return_data(rail_lines, 'Lines')
        rail_lines = rail_lines.rename(axis= 'columns', mapper = {
        'LineCode': 'wmata_line_code',
        'DisplayName': 'line',
        'StartStationCode': 'starting_station_code',
        'EndStationCode': 'ending_station_code'
        })
        # Map starting and ending station ids
        # get starting_station_id
        rail_lines = pandas.merge(
                        rail_lines,
                        pandas.read_sql('''
                          SELECT
                                station_id
                              , wmata_station_code
                              , station
                              , address_id
                          FROM wmata.rail_stations
                          WHERE current
                          ''',
                          con = sql_hook.get_sqlalchemy_engine()),
                      how = 'left', left_on = 'starting_station_code', right_on = 'wmata_station_code')

        rail_lines = rail_lines.rename(axis= 'columns', mapper = {'station_id': 'starting_station_id'})
        # get ending_station_id
        rail_lines = pandas.merge(
                            rail_lines,
                            pandas.read_sql('''
                                      SELECT
                                            station_id
                                          , wmata_station_code
                                          , station
                                          , address_id
                                      FROM wmata.rail_stations
                                      WHERE current
                                      ''',
                                      con = sql_hook.get_sqlalchemy_engine()),
                                      how = 'left', left_on = 'ending_station_code', right_on = 'wmata_station_code')
        rail_lines = rail_lines.rename(axis= 'columns', mapper = {'station_id': 'ending_station_id'})

        rail_lines = rail_lines.loc[:, [  'wmata_line_code'
                                        , 'line'
                                        , 'starting_station_id'
                                        , 'ending_station_id']]
        write_api_table(rail_lines, 'wmata','rail_lines')
        # Add new rail line
        add_new_rail_lines_sql = \
        '''
        INSERT INTO wmata.rail_lines (
                      wmata_line_code
                    , line
                    )(
        SELECT
              n.wmata_line_code
            , n.line
        FROM wmata.rail_lines_api AS n
        LEFT JOIN (
            SELECT
                  wmata_line_code
                , line
            FROM wmata.rail_lines
            WHERE current
        ) AS e USING (wmata_line_code)
        WHERE e.wmata_line_code IS NULL
        )
        '''
        execute_sql_statement(add_new_rail_lines_sql)

        # second scenario: update the starting and ending station
        update_rail_line_starting_ending_station_sql = \
        '''
        UPDATE wmata.rail_lines AS e
        SET   starting_station_id = (
                SELECT n.starting_station_id
                FROM wmata.rail_lines_api AS n
                WHERE n.wmata_line_code = e.wmata_line_code
                )
            , ending_station_id = (
                SELECT n.ending_station_id
                FROM wmata.rail_lines_api AS n
                WHERE n.wmata_line_code = e.wmata_line_code
                )
        WHERE e.current
        AND EXISTS (
            SELECT 1
            FROM wmata.rail_lines_api AS n
            WHERE n.wmata_line_code = e.wmata_line_code
            )
        '''
        execute_sql_statement(update_rail_line_starting_ending_station_sql)
        drop_api_table('wmata','rail_lines')

        # rail_lines_served
        # retrieve station_ids
        rail_lines_served = pandas.merge(
                                rail_lines_served,
                                pandas.read_sql('''
                                    SELECT
                                            station_id
                                          , wmata_station_code
                                    FROM wmata.rail_stations
                                    ''',
                                    con = sql_hook.get_sqlalchemy_engine()),
                                how = 'left', on = ['wmata_station_code'])
        # retrieve line_ids
        rail_lines_served = pandas.merge(
                                rail_lines_served,
                                pandas.read_sql('''
                                    SELECT
                                            line_id
                                          , wmata_line_code
                                    FROM wmata.rail_lines
                                    ''',
                                    con = sql_hook.get_sqlalchemy_engine()),
                                how = 'left', on = ['wmata_line_code'])

        write_api_table(rail_lines_served, 'wmata', 'rail_lines_served')
        # add new rail lines served
        add_new_rail_lines_served_sql = \
        '''
        INSERT INTO wmata.rail_lines_served (
                      station_id
                    , line_id
                    )(
        SELECT
              n.station_id
            , n.line_id
        FROM wmata.rail_lines_served_api AS n
        LEFT JOIN (
            SELECT
                  station_id
                , line_id
            FROM wmata.rail_lines_served
            WHERE current
        ) AS e USING (station_id, line_id)
        WHERE e.station_id IS NULL OR e.line_id IS NULL
        )
        '''
        execute_sql_statement(add_new_rail_lines_served_sql)

        # third scenario: mark expired rail lines served
        mark_expired_rail_lines_served = \
        '''
        UPDATE wmata.rail_lines_served AS e
        SET current = FALSE
        WHERE e.current
        AND NOT EXISTS (
            SELECT 1
            FROM wmata.rail_lines_served_api AS n
            WHERE e.station_id = n.station_id
              AND e.line_id = n.line_id
            )
        '''
        execute_sql_statement(mark_expired_rail_lines_served)
        drop_api_table('wmata', 'rail_lines_served')

    return PythonOperator(
        task_id='rail_stations',
        python_callable=_rail_stations,
        provide_context=True,
        dag=dag
        )      # add/update/expire