#!/usr/bin/env python3
# -*- coding: utf-8 -*-
################################################################################
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from wmata.common import headers, sql_hook, return_data, write_api_table, execute_sql_statement, drop_api_table
import requests, pandas, time
# wmata.bus_routes #############################################################
def bus_routes(dag):

    def _bus_routes(ds,**kwargs):
        # retrieve data from wmata api
        bus_routes = requests.get('http://api.wmata.com/Bus.svc/json/jRoutes', headers)
        bus_routes = return_data(bus_routes, 'Routes')
        # define the column names
        bus_routes.loc[:, 'LineDescription'] = bus_routes.loc[:, 'LineDescription'].str.upper()
        bus_routes.loc[:, 'Name'] = bus_routes.loc[:, 'Name'].str.upper()
        bus_routes.loc[:, 'RouteID'] = bus_routes.loc[:, 'RouteID'].str.upper()
        bus_routes = bus_routes.rename(axis = 'columns', mapper = {
        'LineDescription': 'route_group',
        'Name' : 'route',
        'RouteID' : 'wmata_route_id'})

        bus_route_groups = pandas.DataFrame(bus_routes.route_group.unique(), columns = ['route_group'])

        write_api_table(bus_route_groups, 'wmata', 'bus_route_groups')
        # Add new bus route groups
        add_new_bus_route_groups_sql = \
        '''
        INSERT INTO wmata.bus_route_groups (route_group)(
        SELECT n.route_group
        FROM wmata.bus_route_groups_api AS n
        LEFT JOIN (
            SELECT route_group
            FROM wmata.bus_route_groups
        ) AS e USING (route_group)
        WHERE e.route_group IS NULL
        )
        '''
        execute_sql_statement(add_new_bus_route_groups_sql)
        drop_api_table('wmata', 'bus_route_groups')

        # lookup route_group_id for bus_routes
        bus_routes = pandas.merge(bus_routes,
                        pandas.read_sql('''
                            SELECT
                                  route_group_id
                                , route_group
                            FROM wmata.bus_route_groups
                            ''',
                            con = sql_hook.get_sqlalchemy_engine()),
                        how = 'left', on = 'route_group')

        bus_routes = bus_routes.loc[:, [  'wmata_route_id'
                                        , 'route'
                                        , 'route_group_id']]
        write_api_table(bus_routes, 'wmata', 'bus_routes')

        # Check if the route existed before
        update_reopened_routes_sql = \
        '''
        UPDATE wmata.bus_routes AS e
        SET current = TRUE
        WHERE e.current = FALSE
        AND EXISTS (
            SELECT 1
            FROM wmata.bus_routes_api AS n
            WHERE e.wmata_route_id = n.wmata_route_id
            )
        '''
        execute_sql_statement(update_reopened_routes_sql)
        # first scenario: add a new bus_route to wmata.bus_routes
        add_bus_routes_sql = \
        '''
        INSERT INTO wmata.bus_routes (
              wmata_route_id
            , route
            , route_group_id
        )(
            SELECT
                  n.wmata_route_id
                , n.route
                , n.route_group_id
            FROM wmata.bus_routes_api AS n
            LEFT JOIN (
                SELECT
                      wmata_route_id
                    , route
                    , route_group_id
                FROM wmata.bus_routes
                WHERE current
            ) AS e USING (wmata_route_id, route_group_id)
            WHERE e.wmata_route_id IS NULL
        )
        '''
        execute_sql_statement(add_bus_routes_sql)
        # second scenario: update the name of the bus route
        update_bus_routes_name_sql = \
        '''
        UPDATE wmata.bus_routes AS e
        SET route = (
                SELECT n.route
                FROM wmata.bus_routes_api AS n
                WHERE n.wmata_route_id = e.wmata_route_id
                AND   n.route_group_id = e.route_group_id
                )
        WHERE e.current
        AND EXISTS (
            SELECT 1
            FROM wmata.bus_routes_api AS n
            WHERE n.wmata_route_id = e.wmata_route_id
            AND   n.route_group_id = e.route_group_id
            )
        '''
        execute_sql_statement(update_bus_routes_name_sql)
        # third scenario: take out the old bus routes
        mark_expired_route = \
        '''
        UPDATE wmata.bus_routes AS e
        SET current = FALSE
        WHERE e.current
        AND NOT EXISTS (
            SELECT 1
            FROM wmata.bus_routes_api AS n
            WHERE e.wmata_route_id = n.wmata_route_id
            )
        '''
        execute_sql_statement(mark_expired_route)
        drop_api_table('wmata', 'bus_routes')

    return PythonOperator(
        task_id='bus_routes',
        python_callable=_bus_routes,
        provide_context=True,
        dag=dag
        )