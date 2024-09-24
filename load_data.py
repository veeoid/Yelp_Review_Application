# __author__ : Vismay Chaudhari
"""
This module creates the schema for the yelp_data keyspace.
"""
import json
import glob
import tarfile
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.metadata import KeyspaceMetadata
from cassandra.query import BatchStatement, ConsistencyLevel

class YelpDataExtractor:
    """
    This class extracts data from the .tar archive and loads it into Cassandra.
    """
    def __init__(self, keyspace, tar_file):
        """
        Initializes the YelpDataExtractor class.
        :param keyspace:
        :param tar_file:
        """
        self.keyspace = keyspace
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect(keyspace)
        self.tar_file = tar_file

    def get_table_meta(self, table_name):
        """
        Gets the metadata for the specified table.
        :param table_name:
        :return:
        """
        keyspace_metadata = self.cluster.metadata.keyspaces[self.keyspace]
        if isinstance(keyspace_metadata, KeyspaceMetadata):
            table_metadata = keyspace_metadata.tables.get(table_name)
            if table_metadata:
                all_columns = list(table_metadata.columns.keys())
                primary_key_columns = [col.name for col in table_metadata.primary_key]
                return all_columns, primary_key_columns
        return [], []

    def get_columns(self, table_name):
        """
        Gets the columns for the specified table.
        :param table_name:
        :return:
        """
        keyspace_data = self.cluster.metadata.keyspaces[self.keyspace]
        if isinstance(keyspace_data, KeyspaceMetadata):
            table_metadata = keyspace_data.tables.get(table_name)
            if table_metadata:
                return table_metadata.columns.keys()
        return []

    def load_data(self, table_name, filepath):
        """
        Loads the data from the specified file into the specified table.
        :param table_name:
        :param filepath:
        :return:
        """
        records_inserted = 0
        not_inserted_count = 0
        columns, primary_key_columns = self.get_table_meta(table_name)
        data_columns = ', '.join(['?'] * len(columns))
        print(f'COLUMNS: {columns}')
        statement = self.session.prepare(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({data_columns})")

        with (open(filepath, 'r') as file):
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_size = 0

            for line in file:
                record = json.loads(line)
                if 'date' in record and record['date'] is not None:
                    record['date'] = datetime.strptime(record['date'], '%Y-%m-%d %H:%M:%S')

                values = [record.get(column) for column in columns]

                # Corrected list comprehension for checking None values in primary key columns
                if any(value is None for column, value in zip(columns, values) if column in primary_key_columns):
                    not_inserted_count += 1
                    continue

                batch.add(statement, values)
                batch_size += 1
                records_inserted += 1
                # Execute batch after every 100 statements or if it's the last record
                if batch_size >=5:  # Adjust batch size as needed
                    self.session.execute(batch)
                    batch.clear()
                    batch_size = 0
            # Execute any remaining statements in the batch
            if batch_size > 0:
                self.session.execute(batch)
        print(f"Loaded {records_inserted} records into {table_name}")
        print(f"Skipped {not_inserted_count} records due to None in primary key column")

    def extract_tar_file(self):
        """
        Extracts the data from the .tar archive and loads it into Cassandra.
        :return:
        """
        # Extract JSON files from the .tar archive
        with tarfile.open(self.tar_file, 'r') as tar:
            tar.extractall()
        table_list = ['review']

        # Assuming JSON filenames match the table names
        json_files = glob.glob('*.json')
        for json_file in json_files:
            print(f"Loading data from {json_file}")
            table_name = json_file.split('.')[0]
            table_name = table_name.split('_')[-1]
            # Load data into the table
            if table_name in table_list:

                self.load_data(table_name, json_file)  # Updated call
                print(f"Data loaded into {table_name} from {json_file}")


if __name__ == '__main__':
    keyspace = 'yelp_data'
    tar_file = 'yelp_dataset.tar'  # Update with the path to your .tar file
    extractor = YelpDataExtractor(keyspace, tar_file)
    extractor.extract_tar_file()
