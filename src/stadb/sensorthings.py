#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/10/23
"""

from .postgresql import PgDatabaseConnector
from .logger import LoggerSuperclass
from .utils import reverse_dictionary, dataframe_to_dict, run_subprocess, merge_dataframes_by_columns, slice_dataframes
import json
import pandas as pd
import numpy as np
from .timescaledb import TimescaleDB
import rich
from rich.progress import Progress
import time
import os
import gc


def rsync_files(host: str, folder, files: list):
    """
    Uses rsync to copy some files to a remote folder
    """
    assert type(host) is str, "invalid type"
    assert type(folder) is str, "invalid type"
    assert type(files) is list, "invalid type"
    run_subprocess(["ssh", host, f"mkdir -p {folder} -m=777"], fail_exit=True)
    run_subprocess(f"rsync -azh {' '.join(files)} {host}:{folder}")


def rm_remote_files(host, files):
    """
    Runs remove file over ssh
    """
    assert type(host) is str, "invalid type"
    assert type(files) is list, "invalid type"
    run_subprocess(["ssh", host, f"rm  {' '.join(files)}"], fail_exit=True)


class SensorThingsApiDB(PgDatabaseConnector, LoggerSuperclass):
    def __init__(self, host, port, db_name, db_user, db_password, logger, timescaledb=False):
        """
        initializes  DB connector specific for SensorThings API database (FROST implementation)
        :param host:
        :param port:
        :param db_name:
        :param db_user:
        :param db_password:
        :param logger:
        """
        PgDatabaseConnector.__init__(self, host, port, db_name, db_user, db_password, logger)
        self.host = host
        LoggerSuperclass.__init__(self, logger, "STA DB")
        self.info("Initialize database connector...")
        self.__sensor_properties = {}

        if timescaledb:
            self.timescale = TimescaleDB(self, logger)
        else:
            self.timescale = None

        # This dicts provide a quick way to get the relations between elements in the database without doing a query
        # they are initialized with __initialize_dicts()
        self.datastream_id_sensor_name = {}  # key: datastream_id, value: sensor name
        self.sensor_id_name = {}  # key: sensor id, value: name
        self.thing_id_name = {}  # key: sensor id, value: name
        self.datastream_name_id = {}  # key: datastream name, value: datastream id
        self.obs_prop_name_id = {}  # key: observed property name, value: observed property id

        # dictionaries where sensors key is name and value is ID
        self.__initialize_dicts()

        # dictionaries where key is ID and value is name
        self.sensor_name_id = reverse_dictionary(self.sensor_id_name)
        self.datastream_id_name = reverse_dictionary(self.datastream_name_id)
        self.thing_name_id = reverse_dictionary(self.thing_id_name)
        self.obs_prop_id_name = reverse_dictionary(self.obs_prop_name_id)
        self.datastream_properties = self.get_datastream_properties()

        self.__last_observation_index = -1
        self.get_last_observation_id()

    def __initialize_dicts(self):
        """
        Initialize the dicts used for quickly access to relations without querying the database
        """
        self.info("Initializing internal structures...")
        # DATASTREAM -> SENSOR relation
        query = """
            select "DATASTREAMS"."ID" as datastream_id, "SENSORS"."ID" as sensor_id, "SENSORS"."NAME" as sensor_name, 
            "DATASTREAMS"."NAME" as datastream_name
            from "DATASTREAMS", "SENSORS" 
            where "DATASTREAMS"."SENSOR_ID" = "SENSORS"."ID" order by datastream_id asc;"""
        df = self.dataframe_from_query(query)

        # key: datastream_id ; value: sensor name
        self.datastream_id_sensor_name = dataframe_to_dict(df, "datastream_id", "sensor_name")
        datastream_name_dict = dataframe_to_dict(df, "datastream_id", "datastream_name")

        # SENSOR ID -> SENSOR NAME
        self.sensor_id_name = self.get_sensors()  # key: sensor_id, value: sensor_name
        self.thing_id_name = self.get_things()  # key: sensor_id, value: sensor_name

        # DATASTREAM_NAME -> DATASTREAM_ID
        df = self.dataframe_from_query(f'select "ID", "NAME" from "DATASTREAMS";')
        self.datastream_name_id = dataframe_to_dict(df, "NAME", "ID")

        # OBS_PROPERTY NAME -> OBS_PROPERTY ID
        df = self.dataframe_from_query('select "ID", "NAME" from "OBS_PROPERTIES";')
        self.obs_prop_name_id = dataframe_to_dict(df, "NAME", "ID")

        self.datastream_fois = self.get_datastream_fois()

    def get_sensor_datastreams(self, sensor_id):
        """
        Returns a dataframe with all datastreams belonging to a sensor
        :param sensor_id: ID of a sensor
        :return: dataframe with datastreams ID, NAME and PROPERTIES
        """
        query = (f'select "ID" as id , "NAME" as name, "THING_ID" as thing_id, "OBS_PROPERTY_ID" AS obs_prop_id,'
                 f' "PROPERTIES" as properties from "DATASTREAMS" where "SENSOR_ID" = {sensor_id};')
        df = self.dataframe_from_query(query)
        return df

    def get_sensors(self):
        """
        Returns a dictionary with sensor's names and their id, e.g. {"SBE37": 3, "AWAC": 5}
        :return: dictionary
        """
        df = self.dataframe_from_query('select "ID", "NAME" from "SENSORS";')
        return dataframe_to_dict(df, "NAME", "ID")

    def get_things(self):
        df = self.dataframe_from_query('select "ID", "NAME" from "THINGS";')
        return dataframe_to_dict(df, "NAME", "ID")

    def get_sensor_properties(self):
        """
        Returns a dictionary with sensor's names and their properties
        :return: dictionary
        """
        if self.__sensor_properties:
            return self.__sensor_properties
        df = self.dataframe_from_query('select "NAME", "PROPERTIES" from "SENSORS";')
        self.__sensor_properties = dataframe_to_dict(df, "NAME", "PROPERTIES")
        return self.__sensor_properties

    def get_datastream_sensor(self, fields=["ID", "SENSOR_ID"]):
        select_fields = ", ".join(fields)
        df = self.dataframe_from_query(f'select {select_fields} from "DATASTREAMS";')
        return dataframe_to_dict(df, "NAME", "SENSOR_ID")

    def get_datastream_properties(self, fields=["ID", "PROPERTIES"]):
        select_fields = f'"{fields[0]}"'
        for f in fields[1:]:
            select_fields += f', "{f}"'

        df = self.dataframe_from_query(f'select {select_fields} from "DATASTREAMS";')
        return dataframe_to_dict(df, "ID", "PROPERTIES")

    def get_datastream_fois(self):
        """
        Generates a dictionary with key datastream_id and value foi_id. The FOI is determined by the following rules,
        from higher priority to lower priority
            1. Get the ID from the database LAST_FOI_ID
            2. Look for a FOI that has the name name as the Datastream's THING
        :return:
        """

        datastreams_things = self.dict_from_query(
            'select "ID", "THING_ID" from "DATASTREAMS";'
        )

        query = '''
        select "DATASTREAM_ID", "GEN_FOI_ID" from
            (select "DATASTREAMS"."ID" AS "DATASTREAM_ID", "LOCATION_ID" from
            "DATASTREAMS" join "THINGS_LOCATIONS" on "DATASTREAMS"."THING_ID" = "THINGS_LOCATIONS"."THING_ID") as q1
            join
            "LOCATIONS" as q2
            on q1."LOCATION_ID" = q2."ID"
        '''
        df_gen_fois = self.dict_from_query(query)
        query = '''
        select
            "THINGS"."ID" as thing_id, "FEATURES"."ID" as foi_id 
        from "FEATURES" 
        left join "THINGS" 
        on
            "THINGS"."NAME" = "FEATURES"."NAME";
        '''
        thing_foi = self.dict_from_query(query)
        datastream_features = {}

        for datastream_id, thing_id in datastreams_things.items():
            # First option: FOI has been updated in the table
            if datastream_id in df_gen_fois.keys() and df_gen_fois[datastream_id]:  # could be null...
                datastream_features[datastream_id] = df_gen_fois[datastream_id]
                continue
            # Second option: FOI has the same name than a THING
            elif thing_id in thing_foi.keys() and thing_foi[thing_id]:  # could be null...
                datastream_features[datastream_id] = thing_foi[thing_id]
                continue
            else:
                self.warning(f"Could not get FOI for datastream {datastream_id}! this may crash the code later")
        return datastream_features

    def get_data(self, identifier, time_start: str, time_end: str):
        """
        Access the 0BSERVATIONS data table and exports all data between time_start and time_end
        :param identifier: datasream name (str) or datastream id (int)
        :param time_start: start time
        :param time_end: end time  (not included)
        """
        if type(identifier) == int:
            pass
        elif type(identifier) == str:  # if string, convert from name to ID
            identifier = self.datastream_name_id[identifier]

        query = f' select ' \
                f'    "PHENOMENON_TIME_START" AS timestamp, ' \
                f'    "RESULT_NUMBER" AS value,' \
                f'    ("RESULT_QUALITY" ->> \'qc_flag\'::text)::integer AS qc_flag,' \
                f'    ("RESULT_QUALITY" ->> \'stdev\'::text)::double precision AS stdev ' \
                f'from "OBSERVATIONS" ' \
                f'where "OBSERVATIONS"."DATASTREAM_ID" = {identifier} ' \
                f'and "PHENOMENON_TIME_START" >= \'{time_start}\' and  "PHENOMENON_TIME_START" < \'{time_end}\'' \
                f'order by timestamp asc;'

        df = self.dataframe_from_query(query)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        if not df.empty and np.isnan(df["stdev"].max()):
            self.debug(f"Dropping stdev for {self.datastream_id_name[identifier]}")
            del df["stdev"]
        return df.set_index("timestamp")

    def check_if_table_exists(self, view_name):
        """
        Checks if a view already exists
        :param view_name: database view to check if exists
        :return: True if exists, False if it doesn't
        """
        # Select all from information_schema
        query = "SELECT table_name FROM information_schema.tables"
        df = self.dataframe_from_query(query)
        table_names = df["table_name"].values
        if view_name in table_names:
            return True
        return False

    def dict_from_query(self, query, debug=False):
        response = self.exec_query(query, debug=debug, fetch=True)
        if len(response) == 0:
            return {}
        elif len(response[0]) != 2:
            raise ValueError(f"Expected two fields in response, got {len(response[0])}")

        return {key: value for key, value in response}

    def value_from_query(self, query, debug=False):
        """
        Run a single value from a query
        """
        response = self.exec_query(query, debug=debug)
        try:
            r = response[0][0]
        except IndexError:
            raise LookupError("query produced no results")
        return r

    def inject_to_timeseries(self, df, datastreams, max_rows=100000, disable_triggers=False,
                             tmp_folder="/tmp/sta_db_copy/data"):
        """
        Inject all data in df into the timeseries table via SQL copy
        """

        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows / len(datastreams))
        dataframes = slice_dataframes(df, max_rows=rows)
        files = self.dataframes_to_timeseries_csv(dataframes, datastreams, tmp_folder)
        rich.print("Generating all files took %0.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            t = time.time()
            rich.print("rsync files to remote server...", end="")
            rsync_files(self.host, tmp_folder, files)
            rich.print(f"[green]done![/green] took {time.time() - t:.02f} s")

        if disable_triggers:
            self.disable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to timeseries hypertable...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "timeseries")
                progress.advance(task1, advance=1)

        if disable_triggers:
            self.enable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)

        rich.print("[magenta]Inserting all via SQL COPY took %.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            rm_remote_files(self.host, files)

    def inject_to_detections(self, df, max_rows=100000, disable_triggers=False, tmp_folder="/tmp/sta_db_copy/data"):
        """
        Inject all data in df into the timeseries table via SQL copy
        """

        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows)
        dataframes = slice_dataframes(df, max_rows=rows)
        files = self.dataframes_to_detections_csv(dataframes, tmp_folder)
        rich.print("Generating all files took %0.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            t = time.time()
            rich.print("rsync files to remote server...", end="")
            rsync_files(self.host, tmp_folder, files)
            rich.print(f"[green]done![/green] took {time.time() - t:.02f} s")

        if disable_triggers:
            self.disable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to profiles hypertable...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "detections")
                progress.advance(task1, advance=1)

        if disable_triggers:
            self.enable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)

        rich.print("[magenta]Inserting all detections via SQL COPY took %.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            rm_remote_files(self.host, files)

    def inject_to_files(self, df, max_rows=10000, disable_triggers=False, tmp_folder="/tmp/sta_db_copy/data"):
        """
        Inject all data in df into the timeseries table via SQL copy
        """

        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows)
        dataframes = slice_dataframes(df, max_rows=rows)
        files = self.dataframes_to_files_csv(dataframes, tmp_folder)
        rich.print("Generating all files took %0.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            t = time.time()
            rich.print("rsync files to remote server...", end="")
            rsync_files(self.host, tmp_folder, files)
            rich.print(f"[green]done![/green] took {time.time() - t:.02f} s")

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to OBSERVATIONS ...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "OBSERVATIONS")
                progress.advance(task1, advance=1)

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)

        rich.print("[magenta]Inserting all detections via SQL COPY took %.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            rm_remote_files(self.host, files)

    def inject_to_inference(self, df, max_rows=10000, tmp_folder="/tmp/sta_db_copy/data"):
        """
        Inject all data in df into the timeseries table via SQL copy
        """

        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows)
        dataframes = slice_dataframes(df, max_rows=rows)
        files = self.dataframes_to_inference_csv(dataframes, tmp_folder)
        rich.print("Generating all files took %0.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            t = time.time()
            rich.print("rsync files to remote server...", end="")
            rsync_files(self.host, tmp_folder, files)
            rich.print(f"[green]done![/green] took {time.time() - t:.02f} s")

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to OBSERVATIONS ...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "OBSERVATIONS")
                progress.advance(task1, advance=1)

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)

        rich.print("[magenta]Inserting all detections via SQL COPY took %.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            rm_remote_files(self.host, files)

    def inject_to_profiles(self, df, datastreams, max_rows=100000, disable_triggers=False,
                           tmp_folder="/tmp/sta_db_copy/data"):
        """
        Inject all data in df into the timeseries table via SQL copy
        """

        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows / len(datastreams))
        dataframes = slice_dataframes(df, max_rows=rows)
        files = self.dataframes_to_profile_csv(dataframes, datastreams, tmp_folder)
        rich.print("Generating all files took %0.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            t = time.time()
            rich.print("rsync files to remote server...", end="")
            rsync_files(self.host, tmp_folder, files)
            rich.print(f"[green]done![/green] took {time.time() - t:.02f} s")

        if disable_triggers:
            self.disable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to profiles hypertable...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "profiles")
                progress.advance(task1, advance=1)

        if disable_triggers:
            self.enable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)

        rich.print("[magenta]Inserting all via SQL COPY took %.02f seconds" % (time.time() - init))

        if self.host != "localhost" and self.host != "127.0.0.1":
            rm_remote_files(self.host, files)

    def inject_to_observations(self, df: pd.DataFrame, datastreams: dict, url: str, foi_id: int, avg_period: str,
                               max_rows=10000, disable_triggers=False, tmp_folder="/tmp/sta_db_copy/data",
                               profile=False):
        """
        Injects all data in a dataframe using SQL copy.
        """
        init = time.time()
        os.makedirs(tmp_folder, exist_ok=True)
        os.chown(tmp_folder, os.getuid(), os.getgid())
        if foi_id == -1:
            # We need to insert the first row using the API, so SensorThings will automatically generate the FOI
            rich.print("POSTing the first row to get the Feature ID")
            # feature_id = self.post_via_api(df, api_url, df.iloc[0], column_mapper, avg_period=avg_period)
            foi_id = self.post_via_api(df, url, df.iloc[0], datastreams, avg_period=avg_period, parameters={})
            df = df.iloc[1:-1]

        rich.print("Splitting input dataframe into smaller ones")
        rows = int(max_rows / len(datastreams))
        dataframes = slice_dataframes(df, max_rows=rows)

        files = self.dataframes_to_observations_csv(dataframes, datastreams, tmp_folder, foi_id, avg_period=avg_period,
                                                    profile=profile)
        rich.print(f"Generating all files took {time.time() - init:0.02f} seconds")

        if self.host != "localhost" and self.host != "127.0.0.1":
            t = time.time()
            rich.print("rsync files to remote server...", end="")
            rsync_files(self.host, tmp_folder, files)
            rich.print(f"[green]done![/green] took {time.time() - t:.02f} s")

        if disable_triggers:
            self.disable_all_triggers()

        with Progress() as progress:
            task1 = progress.add_task("SQL COPY to OBSERVATIONS table...", total=len(dataframes))
            for file in files:
                self.sql_copy_csv(file, "OBSERVATIONS")
                progress.advance(task1, advance=1)

        if disable_triggers:
            self.enable_all_triggers()

        rich.print("Forcing PostgreSQL to update Observation ID...")
        self.exec_query("select setval('\"OBSERVATIONS_ID_seq\"', (select max(\"ID\") from \"OBSERVATIONS\") );")

        with Progress() as progress:
            task1 = progress.add_task("remove temp files...", total=len(dataframes))
            for file in files:
                os.remove(file)
                progress.advance(task1, advance=1)

        if self.host != "localhost" and self.host != "127.0.0.1":
            rm_remote_files(self.host, files)

    def dataframes_to_observations_csv(self, dataframes: list, column_mapper: dict, folder: str, feature_id: int,
                                       avg_period: str = "", profile=False):
        """
        Write dataframes into local csv files ready for sql copy following the syntax in table OBSERVATIONS
        """
        files = []
        i = 0
        with Progress() as progress:
            task = progress.add_task("converting dataframes to OBSERVATIONS csv", total=len(dataframes))
            for dataframe in dataframes:
                progress.advance(task, 1)
                file = os.path.join(folder, f"observations_copy_{i:04d}.csv")
                i += 1
                self.format_csv_sta(dataframe, column_mapper, file, feature_id, avg_period=avg_period, profile=profile)
                files.append(file)

        return files

    def dataframes_to_timeseries_csv(self, dataframes: list, column_mapper: dict, folder: str):
        """
        Write dataframes into local csv files ready for sql copy following the syntax in table OBSERVATIONS
        """
        i = 0
        files = []
        with Progress() as progress:
            task = progress.add_task("converting data to timeseries csv", total=len(dataframes))
            for dataframe in dataframes:
                progress.advance(task, 1)
                file = os.path.join(folder, f"timeseries_copy_{i:04d}.csv")
                i += 1
                rich.print(f"format timeseries CSV {i:04d} of {len(dataframes)}")
                self.format_timeseries_csv(dataframe, column_mapper, file)
                files.append(file)
        return files

    def dataframes_to_detections_csv(self, dataframes: list, folder: str):
        """
        Write dataframes into local csv files ready for sql copy following the syntax in table OBSERVATIONS
        """
        i = 0
        files = []
        with Progress() as progress:
            task = progress.add_task("converting data to detections csv", total=len(dataframes))
            for dataframe in dataframes:
                progress.advance(task, 1)
                file = os.path.join(folder, f"timeseries_copy_{i:04d}.csv")
                i += 1
                rich.print(f"format timeseries CSV {i:04d} of {len(dataframes)}")
                self.format_detections_csv(dataframe, file)
                files.append(file)
        return files

    def dataframes_to_files_csv(self, dataframes: list, folder):
        i = 0
        files = []
        with Progress() as progress:
            task = progress.add_task("converting data to 'files' csv", total=len(dataframes))
            for dataframe in dataframes:
                progress.advance(task, 1)
                file = os.path.join(folder, f"files_copy_{i:04d}.csv")
                i += 1
                rich.print(f"format timeseries CSV {i:04d} of {len(dataframes)}")
                self.format_files_csv(dataframe, file)
                files.append(file)
        return files

    def dataframes_to_inference_csv(self, dataframes: list, folder):
        i = 0
        files = []
        with Progress() as progress:
            task = progress.add_task("converting data to 'inference' csv", total=len(dataframes))
            for dataframe in dataframes:
                progress.advance(task, 1)
                file = os.path.join(folder, f"files_copy_{i:04d}.csv")
                i += 1
                rich.print(f"format timeseries CSV {i:04d} of {len(dataframes)}")
                self.format_inference_csv(dataframe, file)
                files.append(file)
        return files

    def dataframes_to_profile_csv(self, dataframes: list, column_mapper: dict, folder: str):
        """
        Write dataframes into local csv files ready for sql copy following the syntax in table OBSERVATIONS
        """
        i = 0
        files = []
        with Progress() as progress:
            task = progress.add_task("converting data to profiles csv", total=len(dataframes))
            for dataframe in dataframes:
                progress.advance(task, 1)
                file = os.path.join(folder, f"profile_copy_{i:04d}.csv")
                i += 1
                rich.print(f"format profile CSV {i:04d} of {len(dataframes)}")
                self.format_profile_csv(dataframe, column_mapper, file)
                files.append(file)
        return files

    def format_csv_sta(self, df_in, column_mapper, filename, feature_id, avg_period: str = "", profile=False):
        """
        Takes a dataframe and arranges it accordingly to the OBSERVATIONS table from a SensorThings API, preparing the
        data to be inserted by a COPY statement
        :param df_in: input dataframe
        :param column_mapper: structure that maps datastreams with dataframe columns
        :param filename: name of the file to be generated
        :param feature_id: ID of the FeatureOfInterst
        :param avg_period: if set, the phenomenon time end will be timestamp + avg_period to generate a timerange.
                           used in averaged data.
        """
        init = False
        if self.__last_observation_index < 0:  # not initialized
            self.__last_observation_index = self.get_last_observation_id()

        for colname, datastream_id in column_mapper.items():
            if colname not in df_in.columns:
                continue

            df = df_in.copy(deep=True)
            quality_control = False
            stdev = False
            keep = ["timestamp", colname]
            if colname + "_qc" in df_in.columns:
                quality_control = True
                keep += [colname + "_qc"]

            if colname + "_std" in df_in.columns:
                stdev = True
                keep += [colname + "_std"]

            if profile:
                keep += ["depth"]

            df["timestamp"] = df.index.values
            for c in df.columns:
                if c not in keep:
                    del df[c]

            if df.empty:
                rich.print(f"[yellow]Got empty dataframe for {colname}")
                continue

            df["PHENOMENON_TIME_START"] = np.datetime_as_string(df["timestamp"], unit="s", timezone="UTC")

            if avg_period:  # if we have the average period
                df["PHENOMENON_TIME_END"] = np.datetime_as_string(df["timestamp"] + pd.to_timedelta(avg_period),
                                                                  unit="s", timezone="UTC")
            else:
                df["PHENOMENON_TIME_END"] = df["PHENOMENON_TIME_START"]
            df["RESULT_TIME"] = df["PHENOMENON_TIME_START"]
            df["RESULT_TYPE"] = 0
            df["RESULT_NUMBER"] = df[colname]
            df["RESULT_BOOLEAN"] = np.nan
            df["RESULT_JSON"] = np.nan
            df["RESULT_STRING"] = df[colname].astype(str)
            df["RESULT_QUALITY"] = "{\"qc_flag\": 2}"
            df["VALID_TIME_START"] = np.nan
            df["VALID_TIME_END"] = np.nan
            if profile:
                df["PARAMETERS"] = ""  # in case of profile we need to add depth as parameter
            else:
                df["PARAMETERS"] = np.nan
            df["DATASTREAM_ID"] = datastream_id
            df["FEATURE_ID"] = feature_id
            df["ID"] = np.arange(0, len(df.index.values), dtype=int) + self.__last_observation_index + 1
            self.__last_observation_index = df["ID"].values[-1]

            # Quality control and standard deviation
            for i in range(0, len(df.index.values)):
                qc_value = np.nan
                std_value = np.nan
                if stdev:
                    std_value = df[colname + "_std"].values[i]
                if qc_value:
                    qc_value = df[colname + "_qc"].values[i]
                if not np.isnan(qc_value) and stdev and not np.isnan(std_value):
                    # If we have QC and STD, put both
                    df["RESULT_QUALITY"].values[i] = "{\"qc_flag\": %d, \"stdev\": %f}" % \
                                                     (df[colname + "_qc"].values[i], df[colname + "_std"].values[i])
                elif not np.isnan(qc_value):
                    # If we only have QC, put it
                    if df[colname + "_qc"].values[i]:
                        df["RESULT_QUALITY"].values[i] = "{\"qc_flag\": %d}" % (df[colname + "_qc"].values[i])
                elif not np.isnan(std_value):
                    # If we only have STD, put it
                    if df[colname + "_std"].values[i]:
                        df["RESULT_QUALITY"].values[i] = "{\"stdev\": %d}" % (df[colname + "_std"].values[i])

            if profile:
                df["depth"] = df["depth"].values.astype(float).round(2)  # force conversion to integer
                for i in range(0, len(df.index.values)):
                    df["PARAMETERS"].values[i] = "{\"depth\": %.02f}" % (df["depth"].values[i])

            del df["timestamp"]
            del df[colname]
            if quality_control:
                del df[colname + "_qc"]
            if stdev:
                del df[colname + "_std"]
            if profile:
                del df["depth"]

            if not init:
                df_final = df
                init = True
            else:
                df_final = pd.concat([df_final, df])

        df_final.to_csv(filename, index=False)

    def format_timeseries_csv(self, df_in, column_mapper, filename):
        """
        Format from a regular dataframe to a Dataframe ready to be copied into a TimescaleDB simple table
        :param df_in:
        :param column_mapper:
        :return:
        """
        df_final = None
        init = False
        for colname, datastream_id in column_mapper.items():
            if colname not in df_in.columns:  # if column is not in dataset, just ignore this datastream
                continue
            df = df_in.copy(deep=True)
            keep = ["timestamp", colname, colname + "_qc"]
            df["timestamp"] = df.index.values
            df = df[keep]
            df = df.dropna(subset=[colname], how='all')  # drop NaNs in column name
            df["time"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            df["datastream_id"] = datastream_id
            df = df.set_index("time")
            df = df.rename(columns={colname: "value", colname + "_qc": "qc_flag"})
            df["qc_flag"] = df["qc_flag"].values.astype(int)
            del df["timestamp"]
            if not init:
                df_final = df
                init = True
            else:
                df_final = pd.concat([df_final, df])
        df_final.to_csv(filename)
        del df_final
        gc.collect()

    def format_profile_csv(self, df_in, column_mapper, filename):
        """
        Format from a regular dataframe to a Dataframe ready to be copied into a TimescaleDB simple table
        :param df_in:
        :param column_mapper:
        :return:
        """
        df_final = None
        init = False
        for colname, datastream_id in column_mapper.items():
            if colname not in df_in.columns:  # if column is not in dataset, just ignore this datastream
                continue
            df = df_in.copy(deep=True)
            keep = ["timestamp", "depth", colname, colname + "_qc"]
            df["timestamp"] = df.index.values
            df = df[keep]
            df = df.dropna(subset=[colname], how='all')  # drop NaNs in column name
            df["time"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            df["datastream_id"] = datastream_id
            df = df.set_index("time")
            df = df.rename(columns={colname: "value", colname + "_qc": "qc_flag"})
            df["qc_flag"] = df["qc_flag"].values.astype(int)
            del df["timestamp"]
            if not init:
                df_final = df
                init = True
            else:
                df_final = pd.concat([df_final, df])
        df_final.to_csv(filename)
        del df_final
        gc.collect()

    def format_detections_csv(self, df_in, filename):
        """
        Format from a regular dataframe to a Dataframe ready to be copied into a TimescaleDB simple table
        :param df_in:
        :param filename:
        :return:
        """
        df = df_in.copy(deep=True)
        df = df.rename(columns={"results": "value"})
        df["timestamp"] = df.index.values
        df = df[["timestamp", "value", "datastream_id"]]
        df = df.dropna(subset=["value"], how='all')  # drop NaNs in column name
        df["time"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df = df.set_index("time")
        df["value"] = df["value"].values.astype(int)
        del df["timestamp"]
        df.to_csv(filename)
        del df
        gc.collect()

    def format_files_csv(self, df_in, filename):
        """
        Takes a dataframe and arranges it accordingly to the OBSERVATIONS table from a SensorThings API, preparing the
        data to be inserted by a COPY statement
        :param df_in: input dataframe
        :param column_mapper: structure that maps datastreams with dataframe columns
        :param filename: name of the file to be generated
        :param feature_id: ID of the FeatureOfInterst
        :param avg_period: if set, the phenomenon time end will be timestamp + avg_period to generate a timerange.
                           used in averaged data.
        """
        if self.__last_observation_index < 0:  # not initialized
            self.__last_observation_index = self.get_last_observation_id()

        df = df_in.copy(deep=True)
        df = df.dropna(subset=["results"], how='all')  # drop NaNs in column name

        df["PHENOMENON_TIME_START"] = np.datetime_as_string(df.index.values, unit="s", timezone="UTC")
        if "timeEnd" in df.columns:  # if we have the average period
            df["PHENOMENON_TIME_END"] = np.datetime_as_string(df["timeEnd"], unit="s", timezone="UTC")
        else:
            df["PHENOMENON_TIME_END"] = df["PHENOMENON_TIME_START"]
        df["RESULT_TIME"] = df["PHENOMENON_TIME_START"]
        df["RESULT_TYPE"] = 3  # Strings are type 3 (2 is json)
        df["RESULT_NUMBER"] = np.nan
        df["RESULT_BOOLEAN"] = np.nan
        df["RESULT_JSON"] = np.nan
        df["RESULT_STRING"] = df["results"].astype(str)
        df["RESULT_QUALITY"] = np.nan
        df["VALID_TIME_START"] = np.nan
        df["VALID_TIME_END"] = np.nan
        if "parameters" in df.columns:
            df["PARAMETERS"] = df["parameters"]
        else:
            df["PARAMETERS"] = np.nan
        df["DATASTREAM_ID"] = df["datastream_id"]
        df["FEATURE_ID"] = df["foi_id"]
        df["ID"] = np.arange(0, len(df.index.values), dtype=int) + self.__last_observation_index + 1
        self.__last_observation_index = df["ID"].values[-1]

        # Keep only columns as in the Database

        df = df[["PHENOMENON_TIME_START", "PHENOMENON_TIME_END", "RESULT_TIME", "RESULT_TYPE", "RESULT_NUMBER",
                 "RESULT_BOOLEAN", "RESULT_JSON", "RESULT_STRING", "RESULT_QUALITY", "VALID_TIME_START",
                 "VALID_TIME_END", "PARAMETERS", "DATASTREAM_ID", "FEATURE_ID", "ID"]]
        df.to_csv(filename, index=False)

    def format_inference_csv(self, df_in, filename):
        """
        Takes a dataframe and arranges it accordingly to the OBSERVATIONS table from a SensorThings API, preparing the
        data to be inserted by a COPY statement
        :param df_in: input dataframe
        :param column_mapper: structure that maps datastreams with dataframe columns
        :param filename: name of the file to be generated
        :param feature_id: ID of the FeatureOfInterst
        :param avg_period: if set, the phenomenon time end will be timestamp + avg_period to generate a timerange.
                           used in averaged data.
        """
        if self.__last_observation_index < 0:  # not initialized
            self.__last_observation_index = self.get_last_observation_id()

        df = df_in.copy(deep=True)
        df = df.dropna(subset=["results"], how='all')  # drop NaNs in column name

        df["PHENOMENON_TIME_START"] = np.datetime_as_string(df.index.values, unit="s", timezone="UTC")
        if "timeEnd" in df.columns:  # if we have the average period
            df["PHENOMENON_TIME_END"] = np.datetime_as_string(df["timeEnd"], unit="s", timezone="UTC")
        else:
            df["PHENOMENON_TIME_END"] = df["PHENOMENON_TIME_START"]
        df["RESULT_TIME"] = df["PHENOMENON_TIME_START"]
        df["RESULT_TYPE"] = 2  # Strings are type 3 (2 is json)
        df["RESULT_NUMBER"] = np.nan
        df["RESULT_BOOLEAN"] = np.nan
        values = []
        for v in df["results"].values:
            # Force JSON structures to be like: "{\"key\": \"value\"}"
            v = v.replace("'", "\"")
            values.append(v)

        df["RESULT_JSON"] = values
        df["RESULT_STRING"] = np.nan
        df["RESULT_QUALITY"] = np.nan
        df["VALID_TIME_START"] = np.nan
        df["VALID_TIME_END"] = np.nan
        if "parameters" in df.columns:
            values = []
            for v in df["parameters"].values:
                # Force JSON structures to be like: "{\"key\": \"value\"}"
                v = v.replace("'", "\"")
                values.append(v)

            df["PARAMETERS"] = values
        else:
            df["PARAMETERS"] = np.nan
        df["DATASTREAM_ID"] = df["datastream_id"]
        df["FEATURE_ID"] = df["foi_id"]
        df["ID"] = np.arange(0, len(df.index.values), dtype=int) + self.__last_observation_index + 1
        self.__last_observation_index = df["ID"].values[-1]

        # Keep only columns as in the Database

        df = df[["PHENOMENON_TIME_START", "PHENOMENON_TIME_END", "RESULT_TIME", "RESULT_TYPE", "RESULT_NUMBER",
                 "RESULT_BOOLEAN", "RESULT_JSON", "RESULT_STRING", "RESULT_QUALITY", "VALID_TIME_START",
                 "VALID_TIME_END", "PARAMETERS", "DATASTREAM_ID", "FEATURE_ID", "ID"]]
        df.to_csv(filename, index=False)

    def sql_copy_csv(self, filename, table="OBSERVATIONS", delimiter=","):
        """
        Execute a COPY query to copy from a local CSV file to a database
        :return:
        """
        query = "COPY public.\"%s\" FROM '%s' DELIMITER '%s' CSV HEADER;" % (table, filename, delimiter)
        self.exec_query(query, fetch=False)

    def get_last_observation_id(self):
        """
        Gets last observation in database
        :return:
        """
        query = "SELECT \"ID\" FROM public.\"OBSERVATIONS\" ORDER BY \"ID\" DESC LIMIT 1"
        df = self.dataframe_from_query(query)
        #  Check if the table is empty
        if df.empty:
            return 0
        return int(df["ID"].values[0])

    def get_data_type(self, datastream_id):
        """
        Returns the data type of a datastream
        :param datastream_id: (int) id of the datastream
        :returns: (data_type: str, average: bool)
        """
        props = self.datastream_properties[datastream_id]
        data_type = props["dataType"]
        if "averagePeriod" in props.keys():
            average = True
        else:
            average = False
        return data_type, average

