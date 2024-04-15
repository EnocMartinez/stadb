#!/usr/bin/env python3
"""
This file contains the TimescaleDB extension for the SensorThings Database. If

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 23/3/21
"""

from .logger import LoggerSuperclass, PRL
import psycopg2
import pandas as pd


class TimescaleDB(LoggerSuperclass):
    def __init__(self, sta_db_connector, logger):
        """
        Creates a TimescaleDB object, which adds timeseries capabilities to the SensorThings Database.
        The following hypertables will be created:
            timeseries: regular timeseries data (timestamp, value, qc, datastream_id)
            profiles: depth-specific data (timestamp, depth, value, qc, datastream_id)
            detections: integer data (timestamp, counts, datastream_id)

        """
        timeseries_table = "timeseries"
        profiles_table = "profiles"
        detections_table = "detections"

        LoggerSuperclass.__init__(self, logger, "TSDB", colour=PRL)

        self.db = sta_db_connector
        self.timeseries_hypertable = timeseries_table
        self.profiles_hypertable = profiles_table
        self.detections_hypertable = detections_table

        default_interval = "30days"

        if not self.db.check_if_table_exists(self.timeseries_hypertable):
            self.info(f"TimescaleDB, initializing {self.timeseries_hypertable} as a hypertable")
            self.create_timeseries_hypertable(timeseries_table, chunk_interval_time=default_interval)
            self.add_compression_policy(timeseries_table, policy="30d")

        if not self.db.check_if_table_exists(profiles_table):
            self.info(f"TimescaleDB, initializing {profiles_table} as a hypertable")
            self.create_profiles_hypertable(profiles_table, chunk_interval_time=default_interval)
            self.add_compression_policy(profiles_table, policy="30d")

        if not self.db.check_if_table_exists(detections_table):
            self.info(f"TimescaleDB, initializing {detections_table} as a hypertable")
            self.create_detections_hypertable(detections_table, chunk_interval_time=default_interval)
            self.add_compression_policy(detections_table, policy="30d")

    def create_timeseries_hypertable(self, name, chunk_interval_time="30days"):
        """
        Creates a table with four parameters, the timestamp, the value, the qc_flag and aa datastream_id as foreing key
        :return:
        """
        if self.db.check_if_table_exists(name):
            self.warning(f"table '{name}' already exists")
            return None
        query = """
        CREATE TABLE {table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        value DOUBLE PRECISION NOT NULL,   
        qc_flag smallint,
        datastream_id smallint NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (timestamp, datastream_id),        
        CONSTRAINT "{table_name}_datastream_id_fkey" FOREIGN KEY (datastream_id)
            REFERENCES public."DATASTREAMS" ("ID") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        );""".format(table_name=name)
        self.info(f"creating table '{name}'...")
        self.db.exec_query(query)
        self.info("converting to hypertable...")
        query = f"SELECT create_hypertable('{name}', 'timestamp', 'datastream_id', 4, " \
                f"chunk_time_interval => INTERVAL '{chunk_interval_time}');"
        self.db.exec_query(query)

    def create_profiles_hypertable(self, name, chunk_interval_time="30days"):
        """
        Creates a table with four parameters, the timestamp, the value, the qc_flag and aa datastream_id as foreing key
        :return:
        """
        if self.db.check_if_table_exists(name):
            self.info("[yellow]WARNING: table already exists")
            return None
        query = """
        CREATE TABLE {table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        depth DOUBLE PRECISION NOT NULL,
        value DOUBLE PRECISION NOT NULL,   
        qc_flag INT8,
        datastream_id smallint NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (timestamp, depth, datastream_id),        
        CONSTRAINT "{table_name}_datastream_id_fkey" FOREIGN KEY (datastream_id)
            REFERENCES public."DATASTREAMS" ("ID") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        );""".format(table_name=name)
        self.info("creating table...")
        self.db.exec_query(query)
        self.info("converting to hypertable...")
        query = f"SELECT create_hypertable('{name}', 'timestamp', 'datastream_id', 4, " \
                f"chunk_time_interval => INTERVAL '{chunk_interval_time}');"
        self.db.exec_query(query)

    def create_detections_hypertable(self, name, chunk_interval_time="30days"):
        """
        Creates a hypertable to store AI predictions with their
        :return:
        """
        if self.db.check_if_table_exists(name):
            self.info("[yellow]WARNING: table already exists")
            return None
        query = """
        CREATE TABLE {table_name} (
        timestamp TIMESTAMPTZ NOT NULL,
        count smallint,                
        datastream_id smallint NOT NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (timestamp, datastream_id),        
        CONSTRAINT "{table_name}_datastream_id_fkey" FOREIGN KEY (datastream_id)
            REFERENCES public."DATASTREAMS" ("ID") MATCH SIMPLE
            ON UPDATE CASCADE
            ON DELETE CASCADE
        );""".format(table_name=name)
        self.info("creating table...")
        self.db.exec_query(query)
        self.info("converting to hypertable...")
        query = f"SELECT create_hypertable('{name}', 'timestamp', 'datastream_id', 4, " \
                f"chunk_time_interval => INTERVAL '{chunk_interval_time}');"
        self.db.exec_query(query)

    def add_compression_policy(self, table_name, policy="30d"):
        """
        Adds compression policy to a hypertable
        """

        query = f"ALTER TABLE {table_name} SET (timescaledb.compress, timescaledb.compress_orderby = " \
                "'timestamp DESC', timescaledb.compress_segmentby = 'datastream_id');"
        self.db.exec_query(query)
        query = f"SELECT add_compression_policy('{table_name}', INTERVAL '{policy}', if_not_exists=>True); "
        self.db.exec_query(query)

    def compress_all(self, table_name, older_than="30days"):
        query = f"""
            SELECT compress_chunk(i, if_not_compressed => true) from 
            show_chunks(
            '{table_name}',
             now() - interval '{older_than}',
              now() - interval '100 years') i;
              """
        self.db.exec_query(query)

    def compression_stats(self, table) -> (float, float, float):
        """
        Returns compression stats
        :param table: hypertable name
        :returns: tuple like (MBytes before, MBytes after, compression ratio)
        """
        df = self.db.dataframe_from_query(f"SELECT * FROM hypertable_compression_stats('{table}');")
        try:
            bytes_before = df["before_compression_total_bytes"].values[0]
        except IndexError:
            self.warning(f"Compression not set for table '{table}'")
            bytes_before = -1
        try:
            bytes_after = df["after_compression_total_bytes"].values[0]
        except IndexError:
            bytes_after = -1
        if type(bytes_after) is type(None):
            return 0, 0, 0
        else:
            ratio = bytes_before/bytes_after
            return round(bytes_before/1e6, 2), round(bytes_after/1e6, 2), round(ratio, 2)

    def insert_to_timeseries(self,  timestamp: str, value: float, qc_flag: int, datastream_id: int):
        """
        Insert a single data point into the timeseries hypertable
        """
        query = f"insert into timeseries (timestamp, value, qc_flag, datastream_id) VALUES('{timestamp}', " \
                f"{value}, {qc_flag}, {datastream_id})"
        try:
            self.db.exec_query(query, fetch=False)
        except psycopg2.errors.UniqueViolation as e:
            return str(e)
        return None

    def insert_to_profiles(self, timestamp: str, depth: float, value: float, qc_flag: int, datastream_id: int, depth_precision=2):
        """
        Insert a single data point into the profiles hypertable
        """
        depth = round(float(depth), depth_precision)
        query = f"insert into profiles (timestamp, depth, value, datastream_id) " \
                 f"VALUES('{timestamp}', {depth}, {value}, {qc_flag}, {datastream_id})"
        try:
            self.db.exec_query(query, fetch=False)
        except psycopg2.errors.UniqueViolation as e:
            return str(e)
        return None

    def insert_to_detections(self, timestamp: str, value: int, datastream_id: int):
        """
        Insert a single data point into the timeseries hypertable
        """
        query = f"insert into detections (timestamp, value, datastream_id) VALUES('{timestamp}', " \
                f"{value},{datastream_id})"
        try:
            self.db.exec_query(query, fetch=False)
        except psycopg2.errors.UniqueViolation as e:
            return str(e)
        return None

    # ---- Get data from hypertables ---- #

    def get_timeseries_data(self, identifier, top=100, skip=0, ascending=True, debug=False, format="dataframe",
                            filters="", orderby=""):
        return self.get_data_from_hypertable(self.timeseries_hypertable, identifier, top=top, skip=skip,
                                             ascending=ascending, debug=debug, format=format, filters=filters,
                                             orderby=orderby)

    def get_profiles_data(self, identifier, top=100, skip=0, ascending=True, debug=False, format="dataframe",
                            filters="", orderby=""):
        return self.get_data_from_hypertable(self.profiles_hypertable, identifier, top=top, skip=skip,
                                             ascending=ascending, debug=debug, format=format, filters=filters,
                                             orderby=orderby)
    def get_detections_data(self, identifier, top=100, skip=0, ascending=True, debug=False, format="dataframe",
                            filters="", orderby=""):
        return self.get_data_from_hypertable(self.detections_hypertable, identifier, top=top, skip=skip,
                                             ascending=ascending, debug=debug, format=format, filters=filters,
                                             orderby=orderby)

    def get_data_from_hypertable(self, hypertable, identifier, time_start="", time_end="", top=100, skip=0,
                                 ascending=True, debug=False, format="dataframe", filters="", orderby=""):
        """
        Access the raw data table and exports all data between time_start and time_end
        :param identifier: datastream name (str) or datastream id (int)
        :param time_start: start time
        :param time_end: end time (not included)
        """
        if type(identifier) is int:
            pass
        elif type(identifier) is str:  # if string, convert from name to ID
            identifier = self.datastreams_ids[identifier]

        query = f"select timestamp, value, qc_flag from {hypertable} where datastream_id = {identifier}"

        if filters:
            query += f" and {filters} "  # add custom filters

        if time_start:
            query += f" and timestamp >= '{time_start}'"
        if time_end:
            query += f" and timestamp <'{time_end}'"

        if orderby:
            query += f" {orderby}"
        else:
            if ascending:
                query += " order by timestamp asc"
            else:
                query += " order by timestamp desc"

        query = f"select * from ({query}) as e"
        if skip:
            query += f" offset {skip}"

        query += f" limit {top};"

        if format == "dataframe":
            df = self.db.dataframe_from_query(query, debug=debug)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df.set_index("timestamp")
        elif format == "list":
            return self.db.list_from_query(query, debug=debug)
        else:
            raise ValueError(f"format {format} not valid!")



if __name__ == "__main__":
    pass
