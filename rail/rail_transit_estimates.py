#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from wmata.common import headers, sql_hook, return_data, write_api_table, execute_sql_statement, drop_api_table
import requests, pandas, time
# wmata.rail_transit_estimates #################################################
def rail_transit_estimates(dag):

    def _rail_transit_estimates(ds,**kwargs):
        # sql_hook = PostgresHook(postgres_conn_id='warehouse')
        # retrieve data from wmata api
        rail_transit_estimates = requests.get('http://api.wmata.com/Rail.svc/json/jSrcStationToDstStationInfo', headers)
        rail_transit_estimates = return_data(rail_transit_estimates, 'StationToStationInfos')
        # rename columns
        rail_transit_estimates = rail_transit_estimates.rename(axis = 'columns', mapper = {
                                                        'SourceStation' : 'starting_station_code',
                                                        'DestinationStation' : 'ending_station_code',
                                                        'CompositeMiles' : 'distance_miles',
                                                        'RailTime' : 'time_minutes',
                                                        'RailFare.OffPeakTime': 'fare_off_peak',
                                                        'RailFare.PeakTime': 'fare_peak',
                                                        'RailFare.SeniorDisabled': 'fare_disabled'})
        # lookup station ids for starting / ending stations from wmata.rail_stations
        # get starting_station_id
        rail_transit_estimates = pandas.merge(
                                    rail_transit_estimates,
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
        rail_transit_estimates = rail_transit_estimates.rename(axis= 'columns', mapper = {
        'station_id': 'starting_station_id',
        })
        # get ending_station_id
        rail_transit_estimates = pandas.merge(
                                    rail_transit_estimates,
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
        rail_transit_estimates = rail_transit_estimates.rename(axis= 'columns', mapper = {
        'station_id': 'ending_station_id',
        })

        rail_transit_estimates = rail_transit_estimates.loc[:,[
                                                        'starting_station_id',
                                                        'ending_station_id',
                                                        'distance_miles',
                                                        'time_minutes',
                                                        'fare_off_peak',
                                                        'fare_peak',
                                                        'fare_disabled']]
        # remove any records where both aren't matched                                                                        , 'LineCode']]
        rail_transit_estimates = rail_transit_estimates[
                                 rail_transit_estimates['starting_station_id'].notnull() &
                                 rail_transit_estimates['ending_station_id'].notnull()]
        # write api table
        write_api_table(rail_transit_estimates, 'wmata', 'rail_transit_estimates')

        # first scenario: add a new rail transit estimates to wmata.rail_transit_estimates
        add_new_rail_transit_estimates_sql = \
        '''
        INSERT INTO wmata.rail_transit_estimates (
                      starting_station_id
                    , ending_station_id
                    , distance_miles
                    , time_minutes
                    , fare_off_peak
                    , fare_peak
                    , fare_disabled
                )(
                SELECT
                      n.starting_station_id
                    , n.ending_station_id
                    , n.distance_miles
                    , n.time_minutes
                    , n.fare_off_peak
                    , n.fare_peak
                    , n.fare_disabled
                FROM wmata.rail_transit_estimates_api AS n
                LEFT JOIN (
                    SELECT
                          starting_station_id
                        , ending_station_id
                        , distance_miles
                        , time_minutes
                        , fare_off_peak
                        , fare_peak
                        , fare_disabled
                    FROM wmata.rail_transit_estimates
                    WHERE current
                ) AS e USING (starting_station_id, ending_station_id)
                WHERE e.starting_station_id IS NULL AND e.ending_station_id IS NULL
            )
        '''
        execute_sql_statement(add_new_rail_transit_estimates_sql)

        update_fare_distance_time_sql = \
        '''
        UPDATE wmata.rail_transit_estimates AS e
        SET   distance_miles = (
                SELECT n.distance_miles
                FROM wmata.rail_transit_estimates_api AS n
                WHERE n.starting_station_id = e.starting_station_id
                AND   n.ending_station_id = e.ending_station_id
                )
            , time_minutes = (
                SELECT n.time_minutes
                FROM wmata.rail_transit_estimates_api AS n
                WHERE n.starting_station_id = e.starting_station_id
                AND   n.ending_station_id = e.ending_station_id
                )
            , fare_off_peak = (
                SELECT n.fare_off_peak
                FROM wmata.rail_transit_estimates_api AS n
                WHERE n.starting_station_id = e.starting_station_id
                AND   n.ending_station_id = e.ending_station_id
                )
            , fare_peak = (
                SELECT n.fare_peak
                FROM wmata.rail_transit_estimates_api AS n
                WHERE n.starting_station_id = e.starting_station_id
                AND   n.ending_station_id = e.ending_station_id
                )
            , fare_disabled = (
                SELECT n.fare_disabled
                FROM wmata.rail_transit_estimates_api AS n
                WHERE n.starting_station_id = e.starting_station_id
                AND   n.ending_station_id = e.ending_station_id
                )
        WHERE e.current
        AND EXISTS (
            SELECT 1
            FROM wmata.rail_transit_estimates_api AS n
            WHERE n.starting_station_id = e.starting_station_id
            AND   n.ending_station_id = e.ending_station_id
            )
        '''
        execute_sql_statement(update_fare_distance_time_sql)
        # third scenario: take out the old station out
        mark_expired_rail_transit_estimates_sql = \
        '''
        UPDATE wmata.rail_transit_estimates AS e
        SET current = FALSE
        WHERE e.current
        AND NOT EXISTS (
            SELECT 1
            FROM wmata.rail_transit_estimates_api AS n
            WHERE e.starting_station_id = n.starting_station_id
            AND   e.ending_station_id = n.ending_station_id
            )
        '''
        execute_sql_statement(mark_expired_rail_transit_estimates_sql)
        drop_api_table('wmata', 'rail_transit_estimates')

    return PythonOperator(
        task_id='rail_transit_estimates',
        python_callable=_rail_transit_estimates,
        provide_context=True,
        dag=dag
        )
