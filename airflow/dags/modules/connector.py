from sqlalchemy import create_engine

class Connector():
    def __init__(self):
        pass

    def connect_mysql(self, user, password, host, db, port):
        engine = create_engine("mysql+mysqlconnector://root:erik@localhost:3306/data_staging".format(
            user, password, host, port, db
        ))
        return engine
    
    def connect_postgres(self, user, password, host, db, port):
        engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:5432/postgres-dwh".format(
            user, password, host, port, db
        ))
        return engine   
