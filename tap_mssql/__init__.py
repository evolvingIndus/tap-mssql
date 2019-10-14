#!/usr/bin/env python3
import collections
import copy
import itertools
import os
import json

# import pymssql
import singer
from singer import utils, Catalog, Schema, metadata, metrics
from singer.catalog import CatalogEntry

from tap_mssql.connection import MSSQLConnection, connect_with_backoff
from tap_mssql.sync_strategies import common, full_table

Column = collections.namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "constraint_type"
])

REQUIRED_CONFIG_KEYS = ["server", "user", "password", "database"]
LOGGER = singer.get_logger()

STRING_TYPES = set([
    'char',
    'enum',
    'longtext',
    'mediumtext',
    'text',
    'varchar'
])

BYTES_FOR_INTEGER_TYPE = {
    'tinyint': 1,
    'smallint': 2,
    'mediumint': 3,
    'int': 4,
    'bigint': 8
}

FLOAT_TYPES = set(['float', 'double'])

DATETIME_TYPES = set(['datetime', 'timestamp', 'date', 'time'])

def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    data_type = c.data_type.lower()
    # column_type = c.column_type.lower()

    inclusion = 'available'
    # We want to automatically include all primary key columns
    # if c.column_key.lower() == 'pri':
    #     inclusion = 'automatic'

    result = Schema(inclusion=inclusion)

    # if data_type == 'bit' or column_type.startswith('tinyint(1)'):
    #     result.type = ['null', 'boolean']

    if data_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ['null', 'integer']
        bits = BYTES_FOR_INTEGER_TYPE[data_type] * 8
        # if 'unsigned' in c.column_type:
        #     result.minimum = 0
        #     result.maximum = 2 ** bits - 1
        # else:
        #     result.minimum = 0 - 2 ** (bits - 1)
        #     result.maximum = 2 ** (bits - 1) - 1

    elif data_type in FLOAT_TYPES:
        result.type = ['null', 'number']

    elif data_type == 'decimal':
        result.type = ['null', 'number']
        result.multipleOf = 10 ** (0 - c.numeric_scale)
        return result

    elif data_type in STRING_TYPES:
        result.type = ['null', 'string']
        result.maxLength = c.character_maximum_length

    elif data_type in DATETIME_TYPES:
        result.type = ['null', 'string']
        result.format = 'date-time'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported column type')
    return result

def create_column_metadata(cols):
    mdata = {}
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        # mdata = metadata.write(mdata,
        #                        ('properties', c.column_name),
        #                        'sql-datatype',
        #                        c.column_type.lower())

    return metadata.to_list(mdata)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover(conn, config):

    with connect_with_backoff(conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("""
            SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE From INFORMATION_SCHEMA.TABLES
            """)

            table_info = {}

            schemas = cur.fetchall()
            for (db, schema, table, table_type) in schemas:
                if db not in table_info:
                    table_info[db] = {}
                if schema not in table_info[db]:
                    table_info[db][schema] = {}

                table_info[db][schema][table] = {
                    # 'row_count': rows,
                    'is_view': table_type == 'VIEW'
                }

            cur.execute("""
            SELECT
       C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE, C.CHARACTER_MAXIMUM_LENGTH, C.NUMERIC_PRECISION,
       C.NUMERIC_PRECISION, TC.CONSTRAINT_TYPE
FROM INFORMATION_SCHEMA.COLUMNS C
    LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU On C.COLUMN_NAME = CCU.COLUMN_NAME
    LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON CCU.CONSTRAINT_NAME = Tc.CONSTRAINT_NAME
ORDER BY C.TABLE_SCHEMA, C.TABLE_NAME
            """)
            # res = cur.fetchall()

            columns = []
            rec = cur.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = cur.fetchone()

            entries = []
            for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
                cols = list(cols)
                (table_schema, table_name) = k
                schema = Schema(type='object',
                                properties={c.column_name: schema_for_column(c) for c in cols})
                md = create_column_metadata(cols)
                md_map = metadata.to_map(md)

                md_map = metadata.write(md_map,
                                        (),
                                        'database-name',
                                        table_schema)

                is_view = table_info[db][table_schema][table_name]['is_view']

                if table_schema in table_info and table_name in table_info[table_schema]:
                    row_count = table_info[table_schema][table_name].get('row_count')

                    if row_count is not None:
                        md_map = metadata.write(md_map,
                                                (),
                                                'row-count',
                                                row_count)

                    md_map = metadata.write(md_map,
                                            (),
                                            'is-view',
                                            is_view)

                column_is_key_prop = lambda c, s: (
                    c.constraint_type == 'PRI' and
                    s.properties[c.column_name].inclusion != 'unsupported'
                )

                key_properties = [c.column_name for c in cols if column_is_key_prop(c, schema)]

                if not is_view:
                    md_map = metadata.write(md_map,
                                            (),
                                            'table-key-properties',
                                            key_properties)

                entry = CatalogEntry(
                    table=table_name,
                    stream=table_name,
                    metadata=metadata.to_list(md_map),
                    tap_stream_id=common.generate_tap_stream_id(table_schema, table_name),
                    schema=schema)

                entries.append(entry)

        return Catalog(entries)

    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': [],
            'key_properties': []
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams


def write_schema_message(catalog_entry, bookmark_properties=[]):
    key_properties = common.get_key_properties(catalog_entry)

    singer.write_message(singer.SchemaMessage(
        stream=catalog_entry.stream,
        schema=catalog_entry.schema.to_dict(),
        key_properties=key_properties,
        bookmark_properties=bookmark_properties
    ))


def do_sync_full_table(mssql_conn, config, catalog_entry, state, columns):
    LOGGER.info("Stream %s is using full table replication", catalog_entry.stream)
    key_properties = common.get_key_properties(catalog_entry)

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    full_table.sync_table(mssql_conn, catalog_entry, state, columns, stream_version)

    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'initial_full_table_complete',
                                  True)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_stream(mssql_conn, catalog, config, state):
    for catalog_entry in catalog.streams:
        columns = list(catalog_entry.schema.properties.keys())

        if not columns:
            LOGGER.warning('There are no columns selected for stream %s, skipping it.', catalog_entry.stream)
            continue

        state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)

        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        md_map = metadata.to_map(catalog_entry.metadata)

        replication_method = md_map.get((), {}).get('replication-method')

        database_name = common.get_database_name(catalog_entry)

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database_name
            timer.tags['table'] = catalog_entry.table

            if replication_method == 'INCREMENTAL':
                optional_limit = config.get('incremental_limit')
                # do_sync_incremental(mssql_conn, catalog_entry, state, columns, optional_limit)
            elif replication_method == 'LOG_BASED':
                pass
                # do_sync_historical_binlog(mssql_conn, config, catalog_entry, state, columns)
            elif replication_method == 'FULL_TABLE':
                do_sync_full_table(mssql_conn, config, catalog_entry, state, columns)


def sync(mssql_conn, config, state, catalog):
    data = catalog.to_dict()
    selected_stream_ids = get_selected_streams(data)

    # Loop over streams in catalog
    # data = catalog.to_dict()
    for stream in data['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        if stream_id in selected_stream_ids:
            sync_stream(mssql_conn, catalog, config, state)
            # TODO: sync code for stream goes here...
            LOGGER.info('Syncing stream:' + stream_id)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    mssql_conn = MSSQLConnection(args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(mssql_conn, args.config)
        print(json.dumps(catalog.to_dict(), indent=2))
    # Otherwise run in sync mode
    else:

        # 'properties' is the legacy name of the catalog
        if args.properties:
            catalog = args.properties
        # 'catalog' is the current name
        elif args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(mssql_conn, args.config)

        sync(mssql_conn, args.config, args.state, catalog)


if __name__ == "__main__":
    main()
