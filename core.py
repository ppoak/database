import sqlalchemy


class Loader:

    def __init__(self, config) -> None:
        self.table = config['table']
        self.database = config['database']
        self.addindex = config.get('addindex', None)
        if isinstance(self.database, str):
            self.database = sqlalchemy.engine.create_engine(self.database)
    
    def post(self):
        if self.addindex is not None:
            with self.database.connect() as connect:
                if self.database.name == 'sqlite':
                    idxnames = connect.execute(f'SELECT name FROM sqlite_master WHERE type ' 
                        f'= "index" and tbl_name = "{self.table}"').fetchall()
                elif self.database.name == 'mysql':
                    idxnames = connect.execute(f'SHOW INDEX FROM {self.table}').fetchall()
                idxnames = list(map(lambda x: x[0], idxnames))
                for idxname, column in self.addindex.items():
                    if idxname not in idxnames:
                        sqlcol = str(column).replace("'", "`")
                        connect.execute(f'CREATE INDEX `{idxname}` on {self.table} ({sqlcol})')

    def __call__(self):
        raise NotImplementedError
