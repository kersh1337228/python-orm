from mysql.connector import connect, Error
from settings import db_data
from . import fields as fld, model as mdl, query as qr


class QuerySet:
    class __QuerySetIterator:
        def __init__(self, container):
            self.__container = container
            self.index = -1

        def __iter__(self):
            return self

        def __next__(self):
            self.index += 1
            try:
                return self.__container[self.index]
            except IndexError:
                raise StopIteration

    def __init__(self, model, query):
        self.model = model
        self.query = query
        self.__container = tuple()

    def __exec(self):  # Lazy query execution
        self.model.check_table()
        try:  # Select database logs
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(f"""SELECT {', '.join(
                        f'{self.model.table_name}0.{fname}' for fname, fval in
                        self.model.fields.items()
                        if not isinstance(fval, fld.ManyToManyField)

                    )} FROM {self.model.table_name} AS {self.model.table_name}0{qr.assemble_query(
                        self.model,
                        *self.query['args'],
                        **self.query['kwargs']
                    )}""")
                    results = cursor.fetchall()
                    self.__container = tuple(
                        mdl.ModelInstance(self.model, **res) for res in results
                    )
        except Error as err:
            print(err)

    def __iter__(self):
        self.__exec()
        return QuerySet.__QuerySetIterator(self.__container)

    def __getitem__(self, key: int):
        self.__exec()
        return self.__container[key]

    def __str__(self):
        return f'<QuerySet{self.__container}>'

    def filter(self, *args, **kwargs):
        self.query['args'] += args
        self.query['kwargs'].update(kwargs)
        return self

    def exclude(self, *args, **kwargs):
        self.query['args'] += (~qr.Q.And(
            *(args + [qr.Q(*{name: val}) for name, val in kwargs.items()])
        ),)
        return self

    def update(self, **kwargs):  # TEST
        self.model.check_table()
        update_set = []
        for name, val in kwargs.items():
            if not name in self.model.fields:  # Checking if all fields specified right
                raise Exception('Wrong fields specified in update method')
            else:
                update_set.append(f'{self.model.table_name}.{name} = {self.model.fields[name].to_sql(val)}')
                for mi in self.__container:
                    setattr(mi, name, val)
        try:  # Select database logs
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(f"""UPDATE {self.model.table_name}, (SELECT {', '.join(
                        f'{self.model.table_name}0.{fname}' for fname, fval in
                        self.model.fields.items()
                        if not isinstance(fval, fld.ManyToManyField)
                    )} FROM {self.model.table_name} AS {self.model.table_name}0{qr.assemble_query(
                        self.model,
                        *self.query['args'],
                        **self.query['kwargs']
                    )}) AS __tab SET {', '.join(
                        update_set
                    )} WHERE {self.model.table_name}.id = __tab.id""")
                    connection.commit()
        except Error as err:
            print(err)

    def delete(self):  # TEST
        self.model.check_table()
        try:  # Select database logs
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(f"""DELETE {self.model.table_name}, (SELECT {', '.join(
                        f'{self.model.table_name}0.{fname}' for fname, fval in
                        self.model.fields.items()
                        if not isinstance(fval, fld.ManyToManyField)
                    )} FROM {self.model.table_name} AS {self.model.table_name}0{qr.assemble_query(
                        self.model,
                        *self.query['args'],
                        **self.query['kwargs']
                    )}) AS __tab WHERE {self.model.table_name}.id = __tab.id""")
                    connection.commit()
        except Error as err:
            print(err)
