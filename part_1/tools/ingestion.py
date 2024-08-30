import snowflake.connector
import logging
import ntpath

logger = logging.getLogger(__name__)


class Ingestion():
    """ Ingest the contents of a data file into a Snowflake database table """

    def __init__(self, account:str, database:str, user:str, password:str, schema:str):

        self.account = account
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema

    
    def _connect(self) -> snowflake.connector.connection.SnowflakeConnection:
        """ Create a connection object for the database """

        # Create connection object
        con = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            database=self.database,
            schema=self.schema,
            authenticator="username_password_mfa",
            client_store_temporary_credential=True,
            client_request_mfa_token=True
        )

        return con


    def _cursor(self, con:snowflake.connector.connection.SnowflakeConnection) -> snowflake.connector.cursor.SnowflakeCursor:
        """ Create a connection cursor for the database """

        # Create connection cursor
        cs = con.cursor()
        logger.info("Connected to Snowflake successfully")

        return cs


    def _path_leaf(self, path:str) -> str:
        """ Extract final element in path """

        head, tail = ntpath.split(path)

        return tail or ntpath.basename(head)


    def ingest(self, table_name:str, file_name:str, file_type:str="CSV", date_format:str="YYYY-MM-DD") -> None:
        """ Ingest a data file into a database table

            :table_name: the name of the table the data is to be ingested into
            :file_name: the name of your file containing the data for ingestion
            :file_type: the type of the file to be ingested (e.g. CSV, JSON, etc)
            :date_format: date format for any data columns
        """
        logger.info(f"Ingesting data from file {file_name} into table {table_name}")
        con = self._connect()
        cs = self._cursor(con)

        try:
            # Select the database and schema that will be used
            logger.debug("Selecting database and schema...")
            cs.execute(f" USE DATABASE {self.database};")
            cs.execute(f" USE SCHEMA {self.schema};")

            # Truncate the table so that no previously ingested data are retained
            logger.debug("Truncating target table...")
            cs.execute(f"TRUNCATE TABLE {self.database}.{self.schema}.{table_name};")

            # Remove all the files from user-specific snowflake stage so that no duplicate or other files are uploaded
            logger.debug("Clearing residual files from stage...")
            cs.execute("REMOVE @~;")

            # Put the file from the local machine onto the user-specific snowflake stage
            logger.debug("Staging data file...")
            cs.execute(f"PUT file://{file_name} @~ OVERWRITE=TRUE;")

            # Copy the contents of the file into the table from the user stage
            logger.debug("Copying data into table...")
            cs.execute(f"COPY INTO {table_name} FROM @~ FILES = ('{self._path_leaf(file_name)}.gz') file_format = (type={file_type}, SKIP_HEADER=1, DATE_FORMAT='{date_format}');")
            logger.info("Data successfully ingested.")

        except Exception as e:
            raise e

        finally:
            cs.close()
            con.close()