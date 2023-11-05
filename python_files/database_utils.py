import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    @classmethod
    def read_db_creds(self):
        ''' This method gathers the database credentials from the yaml file, db_creds.yaml.

        Returns 
        -------
        creds : dict
            Database connection credentials.
        '''
        with open('../.gitignore/db_creds.yaml','r') as file:
                try:
                    creds = yaml.safe_load(file)
                    return creds
                except yaml.YAMLError as e:
                    print(e)

    @classmethod
    def init_db_engine(self):
        ''' This method initialises a connection to the database, through a sqlalchemy engine.

        Returns
        -------
        self.engine : sqlalchemy engine
            Engine that connects to the database.
        '''
        # reads db credentials into a list
        creds_dict = self.read_db_creds()
        creds_list = list(creds_dict.values())

        # separates credentials for input into engine
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds_list[0]
        PASSWORD = creds_list[1]
        USER = creds_list[2]
        DATABASE = creds_list[3]
        PORT = creds_list[4]

        # creates engine
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return self.engine
    
    @classmethod
    def list_db_tables(self):
        ''' This method retrieves and prints the table names from the database. '''
        # gets table names from database
        inspector = inspect(self.engine)
        self.table_names = inspector.get_table_names()
        print(self.table_names)

    @classmethod
    def upload_to_db(self, data_frame, table_name, index = False, index_name = None):
        ''' This method uploads a pandas dataframe to the local database.

        Parameters
        ----------
        data_frame : Pandas DataFrame
            The dataframe to upload as a database table.
        table_name : str
            The name for the table in the database.
        index : bool, default = False
            Whether to include an index column in table.
        index_name: str, optional
            Name for index column.
        '''
        # details for local database
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        PASSWORD = 'emmalisa'
        USER = 'postgres'
        DATABASE = 'sales_data'
        PORT = 5432

        #creates engine for local database
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        data_frame.to_sql(name = table_name, con = engine, if_exists = 'append', index = index, index_label = index_name)
