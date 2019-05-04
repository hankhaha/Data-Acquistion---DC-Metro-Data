
import requests, pandas, time

headers = {'api_key': 'a2d3eedb7b804592b09ed95ac11ffc9f'}

def return_data(response, key):
    return pandas.io.json.json_normalize(response.json()[key])


# pattern =====================================================================
cursor.execute('''DROP TABLE IF EXISTS {schema}.{table};''')

data.to_sql(
    schema = '',
    name = '',
    index = False,
    con = sql_hook.get_sqlalchemy_engine())

cursor.execute(s)
cursor.execute('''DROP TABLE IF EXISTS {schema}.{table};''')

connection.commit()

# bus data ####################################################################
# wmata.bus_route_groups ######################################################
# wmata.bus_routes ############################################################
bus_routes = requests.get('http://api.wmata.com/Bus.svc/json/jRoutes', headers)
bus_routes = return_data(bus_routes, 'Routes')

bus_routes.loc[:, 'LineDescription'] = bus_routes.loc[:, 'LineDescription'].str.upper()
bus_routes.loc[:, 'Name'] = bus_routes.loc[:, 'Name'].str.upper()
bus_routes.loc[:, 'RouteID'] = bus_routes.loc[:, 'RouteID'].str.upper()
bus_routes = bus_routes.rename(axis = 'columns', mapper = {
    'LineDescription': 'route_group',
    'Name' : 'route',
    'RouteID' : 'wmata_route_id'})

bus_route_groups = pandas.DataFrame(
        bus_routes.route_group.unique(),
        columns = ['route_group'])

# add new bus route groups
'''
INSERT INTO wmata.bus_route_groups (route_group)(
    SELECT n.route_group
    FROM wmata.bus_route_groups_api AS n
    LEFT JOIN (
        SELECT route_group
        FROM wmata.bus_route_groups
    ) AS e (route_group)
    WHERE e.route_group IS NULL
)
'''

# prepare bus_routes
# lookup route_group_id
bus_routes = pandas.merge(
        bus_routes,
        pandas.read_sql('''
                        SELECT
                              route_group_id
                            , route_group
                        FROM wmata.bus_route_groups
                        ''',
                        con = sql_hook.get_sqlalchemy_engine()),
        how = 'left', on = 'route_group')

# lookup mar_id
bus_routes = pandas.merge(
        bus_routes,
        pandas.read_sql('''
                        SELECT
                              mar_id
                            , latitude
                            , longitude
                        FROM map.master_addresses
                        ''',
                        con = sql_hook.get_sqlalchemy_engine()),
        how = 'left', on = ['latitude', 'longitude'])

# remove and log missing mar_ids
# TODO: writing

# format table structure
bus_routes = bus_routes.loc[:, [  'wmata_route_id'
                                , 'route'
                                , 'route_group_id']]

# add new route_ids for changed combinations
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
    ) AS e (wmata_route_id, route, route_group_id)
    WHERE e.wmata_route_id IS NULL
)
'''

# add route_id to bus_routes
bus_routes = pandas.merge(
        bus_routes,
        pandas.read_sql('''
                        SELECT
                              route_id
                            , wmata_route_id
                            , route
                            , route_group_id
                        FROM wmata.bus_routes
                        ''',
                        con = sql_hook.get_sqlalchemy_engine()),
        how = 'left', on = ['wmata_route_id', 'route', 'route_group_id'])

# update wmata.bus_routes_operational
# expire route for old records
'''
UPDATE wmata.bus_routes_operational AS e
SET
      expiration_date = CURRENT_DATE - 1
    , current = FALSE
WHERE NOT EXISTS (
    SELECT 1
    FROM wmata.bus_routes_api AS n
    WHERE e.route_id = n.route_id
);
'''
# update route for matched records
'''
UPDATE wmata.bus_routes_operational AS e
SET expiration_date = CURRENT_DATE + INTERVAL '3 MONTHS'
FROM wmata.bus_routes_api AS n
WHERE
        e.route_id = n.route_id
    AND e.current;
'''
# add route for new records
'''
INSERT INTO wmata.bus_routes_operational (route_id)(
    SELECT route_id
    FROM wmata.bus_routes_api
    LEFT JOIN (
        SELECT route_id
        FROM wmata.bus_routes_operational
        WHERE current
    ) AS e USING (route_id)
    WHERE e.route_id IS NULL
)
'''   
  
# wmata.bus_stops #############################################################
bus_stops = requests.get('http://api.wmata.com/Bus.svc/json/jStops', headers) 
bus_stops = return_data(bus_stops, 'Stops') 

bus_stops = bus_stops.loc[:, ['StopID', 'Name', 'Lat', 'Lon']]
bus_stops.loc[:, 'Name'] = bus_stops.loc[:, 'Name'].str.upper()
bus_stops = bus_stops.rename(axis = 'columns', mapper = {
    'StopID' : 'wmata_stop_id',
    'Name' : 'stop' })
    
# match against MAR to get MAR IDs
  # remove records where bad match against MAR ID
  # log removed records to wmata.unknown_locations
# add non-matches
# match this table on stop id, name, mar_id columns with wmata.bus_stops
# update wmata.bus_stops_operational
  # where stop is current, but not in new table, expire and mark not current
  # extend expiration date for current record in new table + 1 YR
  # add stops where stop in new table but not in wmata.bus_stops_operational

# wmata.bus_route_stops #######################################################
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
                processed['RouteID'] = route
                processed['Direction'] = direction

                bus_route_stops = bus_route_stops + [processed]

    time.sleep(.25) # wait for rate limiting

bus_route_stops = pandas.concat(bus_route_stops)

# update directions table with any new directions
# use joined routes/routes_operational to get current route_ids, effective_date
  # join those onto the table
# repeat process with stops/stops_operational
# confirm both route and stop exist in operational tables
# update wmata.bus_route_stops
  # where route/stop/stop #/direction is current, but not in the new table, expire and mark not current
  # extend expiration date for exact matches
  # add route/stops where no exact match

# rail data ###################################################################
# wmata.rail_lines ############################################################
rail_lines = requests.get('http://api.wmata.com/Rail.svc/json/jLines?', headers)
rail_lines = return_data(rail_lines, 'Lines')

rail_lines = rail_lines.loc[:, [  'LineCode'
                                , 'DisplayName'
                                , 'StartStationCode'
                                , 'EndStationCode']]


# add any new lines to wmata.rail_lines

# wmata.rail_stations #########################################################
# wmata.rail_lines_served #####################################################
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
rail_lines_served = rail_lines_served[
        rail_lines_served['LineCode'].notnull()]

rail_stations = rail_stations.loc[:, [  'Code'
                                      , 'Name'
                                      , 'Lat'
                                      , 'Lon']]

# rail stations
  # uppercase the station name
  # map the MAR ID
    # for unknown records, add to unknown locations table
  # for records where no match on wmata_station_code, station, and mar_id, add
  # map back the station ids
  # update rail_stations_operational
    # expire stations where no record in new table, record in operational
    # extend expiration date where record in both
    # add new records where new record exists
    
# use the new operational table to map starting and ending station codes for rail_lines_operational
    # drop records where station id is missing from operational
    # add / update rail_lines_operational
      # expire missing lines
      # extend existing lines
      # add new lines
      
# rail_lines_served
      # map operational rail lines to rail lines served w/ eff date
      # map operaional stations to rail lines served w/ eff date
      # confirm both are operational / remove non operational
      # add/update/expire 
  

# wmata.rail_station_entrances ################################################
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
rail_station_entrances = rail_station_entrances[
        rail_station_entrances['StationCode'] != '']

# rail_station_entrances
  # lookup stations
  # map mar id, remove missing, log
  # based on station, wmata entrance id, station entrance, description, mar id
  # add new  values to table
  
# rail_station_entrances_operational
  # pull back entrance ids
  # expire/update/add new operational station entrances

# wmata.rail_transit_estimates ################################################
rail_transit_estimates = requests.get('http://api.wmata.com/Rail.svc/json/jSrcStationToDstStationInfo', headers)
rail_transit_estimates = return_data(rail_transit_estimates, 'StationToStationInfos')

# lookup station ids for starting / ending stations from operational stations
# remove any records where both aren't matched
# on stationid, station id, distance/miles, time/minutes, fare off peak, fare peak fare disabled
  # expire / update / add