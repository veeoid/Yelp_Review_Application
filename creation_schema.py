# __author__ : Vismay Chaudhari
"""
This module creates the schema for the yelp_data keyspace.
"""
from cassandra.cluster import Cluster
class CassandraSchemaGenerator:
    """
    This class creates the schema for the yelp_data keyspace.
    """
    def __init__(self, keyspace):
        """
        Initializes the CassandraSchemaGenerator class.
        :param keyspace:
        """
        self.keyspace = keyspace
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect()

    def create_yelp_schema(self):
        """
        Creates the schema for the yelp_data keyspace.
        :return:
        """
        self.session.execute(f"DROP KEYSPACE IF EXISTS {self.keyspace}")
        self.session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )
        print(f"Created keyspace: {self.keyspace}")

        tables = {
            "business": """
                CREATE TABLE IF NOT EXISTS {keyspace}.business (
                    business_id text PRIMARY KEY,
                    name text,
                    address text,
                    city text,
                    state text,
                    postal_code text,
                    stars float,
                    review_count int,
                )
            """,
            "review": """
                CREATE TABLE IF NOT EXISTS {keyspace}.review (
                    review_id text PRIMARY KEY,
                    user_id text,
                    business_id text,
                    stars float,
                    useful int,
                    funny int,
                    cool int,
                    text text,
                    date timestamp
                )
            """,
            "user": """
                CREATE TABLE IF NOT EXISTS {keyspace}.user (
                    user_id text PRIMARY KEY,
                    name text,
                    review_count int,
                    yelping_since text,
                    average_stars float,
                )
            """
        }
        print("Creating tables...")
        for table_name, create_stmt in tables.items():
            self.session.execute(create_stmt.format(keyspace=self.keyspace))
            print(f"Created table: {table_name}")

if __name__ == "__main__":
    keyspace = "yelp_data"
    schema_generator = CassandraSchemaGenerator(keyspace)
    schema_generator.create_yelp_schema()
