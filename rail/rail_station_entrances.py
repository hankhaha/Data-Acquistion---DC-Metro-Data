#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from wmata.common import headers, sql_hook, return_data, write_api_table, execute_sql_statement, drop_api_table
import requests, pandas, time, numpy
# wmata.rail_station_entrances #################################################
def rail_station_entrances(dag):

    def _rail_station_entrances(ds,**kwargs):
        # sql_hook = PostgresHook(postgres_conn_id='warehouse')
        # retrieve data from wmata api
        rail_station_entrances = requests.get('http://api.wmata.com/Rail.svc/json/jStationEntrances?', headers)
        rail_station_entrances = return_data(rail_station_entrances, 'Entrances')

        rail_station_entrances = pandas.melt(rail_station_entrances,
                                        id_vars = [  'ID'
                                                   , 'Name'
                                                   , 'Description'
                                                   , 'Lat'
                                                   , 'Lon'],
                                        value_vars = [  'StationCode1'
                                                      , 'StationCode2'],
                                        var_name = 'Split',
                                        value_name = 'StationCode').loc[:, [  'ID'
                                                                            , 'Name'
                                                                            , 'Description'
                                                                            , 'Lat'
                                                                            , 'Lon'
                                                                            , 'StationCode']]

        rail_station_entrances['StationCode'] = rail_station_entrances['StationCode'].replace("", numpy.nan)
        rail_station_entrances = rail_station_entrances[rail_station_entrances['StationCode'].notnull()]
        # rename columns
        rail_station_entrances = rail_station_entrances.rename(axis = 'columns', mapper = {
                                                        'ID' : 'wmata_entrance_id',
                                                        'StationCode' : 'wmata_station_code',
                                                        'Name' : 'station_entrance',
                                                        'Description' : 'description',
                                                        'Lat': 'latitude',
                                                        'Lon': 'longitude'})
        # lookup the station_id and address_id
        rail_station_entrances = pandas.merge(
                                rail_station_entrances,
                                pandas.read_sql('''
                                    SELECT
                                            station_id
                                          , wmata_station_code
                                          , address_id
                                    FROM wmata.rail_stations
                                    ''',
                                    con = sql_hook.get_sqlalchemy_engine()),
                                how = 'left', on = ['wmata_station_code'])

        rail_station_entrances = rail_station_entrances.loc[:,[
                                                        'station_id',
                                                        'wmata_entrance_id',
                                                        'station_entrance',
                                                        'description',
                                                        'address_id']]
        # write api table
        write_api_table(rail_station_entrances, 'wmata', 'rail_station_entrances')
        # match this api table on station, wmata entrance id, station entrance, description, address_id
        # add new values to table
        add_new_rail_station_entrances_sql = \
        '''
        INSERT INTO wmata.rail_station_entrances (
              station_id
            , wmata_entrance_id
            , station_entrance
            , description
            , address_id
        )(
            SELECT
                  n.station_id
                , n.wmata_entrance_id
                , n.station_entrance
                , n.description
                , n.address_id
            FROM wmata.rail_station_entrances_api AS n
            LEFT JOIN (
                SELECT
                      station_id
                    , wmata_entrance_id
                    , station_entrance
                    , description
                    , address_id
                FROM wmata.rail_station_entrances
                WHERE current
            ) AS e USING (station_id, wmata_entrance_id, address_id)
            WHERE e.wmata_entrance_id IS NULL
        )
        '''
        execute_sql_statement(add_new_rail_station_entrances_sql)
        # second scenario: update the station_entrance and description
        update_station_entrance_description_sql = \
        '''
        UPDATE wmata.rail_station_entrances AS e
        SET   station_entrance = (
                SELECT n.station_entrance
                FROM wmata.rail_station_entrances_api AS n
                WHERE n.wmata_entrance_id = e.wmata_entrance_id
                AND   n.station_id = e.station_id
                )
            , description = (
                SELECT n.description
                FROM wmata.rail_station_entrances_api AS n
                WHERE n.wmata_entrance_id = e.wmata_entrance_id
                AND   n.station_id = e.station_id
                )
        WHERE e.current
        AND EXISTS (
            SELECT 1
            FROM wmata.rail_station_entrances_api AS n
            WHERE n.wmata_entrance_id = e.wmata_entrance_id
            AND   n.station_id = e.station_id
            AND   n.address_id = e.address_id
            )
        '''
        execute_sql_statement(update_station_entrance_description_sql)

        # third scenario: take out the old rail station entrances
        mark_expired_station_entrances_sql = \
        '''
        UPDATE wmata.rail_station_entrances AS e
        SET current = FALSE
        WHERE e.current
        AND e.current
        AND NOT EXISTS (
            SELECT 1
            FROM wmata.rail_station_entrances_api AS n
            WHERE e.station_id = n.station_id
            AND   e.wmata_entrance_id = n.wmata_entrance_id
            AND   e.address_id = n.address_id
            )
        '''
        execute_sql_statement(mark_expired_station_entrances_sql)
        drop_api_table('wmata', 'rail_station_entrances')



    return PythonOperator(
    task_id='rail_station_entrances',
    python_callable=_rail_station_entrances,
    provide_context=True,
    dag=dag
    )