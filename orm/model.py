from mysql.connector import connect, Error
from functools import reduce
from .exceptions import ModelException
from . import fields as f


class ModelInstance:
    def __init__(self, model, **kwargs):
        for name, value in kwargs.items():
            try:
                attr = getattr(model, name)
                setattr(self, name, attr.from_sql(value))
            except AttributeError:
                setattr(self, name, value)
        self.__model = model
        for name, value in model.get_fields(model).items():
            if isinstance(value, f.ManyToManyField):
                setattr(self, name, f.ManyToManyFieldInstance(value, self.id))

    def save(self):
        self.__model.update(self.__model, **self.__dict__)

    def delete(self):
        self.__model.delete(self.__model, self.id)


class Model:
    __host = "94.228.124.234"
    __login = 'pmi_cav'
    __password = '2WVGvLrB!'
    __db_name = 'pmi_cav'

    def get_fields(self):
        fields = {}
        for name in dir(self):
            try:
                attr = getattr(self, name)
                if issubclass(type(attr), f.Field):
                    fields[name] = attr
            except:
                pass
        [field.set_name(name) if hasattr(field, 'set_name') else
         None for name, field in fields.items()]
        return fields

    def __init__(self):  # Check if db table exists. If not creates one.
        try:  # Getting fields list either from classmethod...
            fields = self.get_fields(self)
        except TypeError:  # ... or from regular method
            fields = self.get_fields()
        try:
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor() as cursor:
                    query = f'''CREATE TABLE IF NOT EXISTS {type(self).__name__ 
                    if type(self).__name__ != 'type' else self.__name__}s (
                    id int NOT NULL UNIQUE AUTO_INCREMENT,
                    PRIMARY KEY (id),
                    {reduce(
                        lambda prev, next: prev + f'{next[0]} {next[1].sql_init()}, ' 
                        if not isinstance(next[1], f.ManyToManyField) else prev,
                        fields.items(), ''
                    )[:-2]})'''
                    cursor.execute(query)
        except Error as err:
            print(err)
        [(m2m.set_m1(self), m2m.create_table()) for m2m in fields.values() if isinstance(m2m, f.ManyToManyField)]

    @classmethod
    def create(cls, **kwargs):
        cls.__init__(cls)
        fields = cls.get_fields(cls)
        for name in kwargs.keys():
            if not name in fields:  # Checking if all fields specified right
                raise ModelException('Wrong fields specified in create method')
        try:  # Creating database log
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor() as cursor:
                    query = f'''INSERT INTO {cls.__name__}s ({reduce(
                        lambda prev, next: prev + next + ', ' 
                        if not isinstance(fields[next], f.ManyToManyField) else prev,
                        kwargs.keys(), ''
                    )[:-2]}) VALUES ({reduce(
                        lambda prev, next: prev + fields[next[0]].to_sql(next[1]) + ', ' 
                        if not isinstance(fields[next[0]], f.ManyToManyField) else prev,
                        kwargs.items(), ''
                    )[:-2]})'''
                    cursor.execute(query)
                    connection.commit()
        except Error as err:
            print(err)
        return cls.get(**kwargs)

    @classmethod
    def filter(cls, **kwargs):
        cls.__init__(cls)
        fields = cls.get_fields(cls)
        id = kwargs.pop('id', None)
        for name in kwargs.keys():
            if not name in fields:  # Checking if all fields specified right
                raise ModelException('Wrong fields specified in read method')
        try:  # Select database logs
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    query = f'''SELECT * FROM {cls.__name__}s WHERE
                    {f'id = {id}' if id else ''} {reduce(
                        lambda prev, next: prev + next[0] + ' = ' + fields[next[0]].to_sql(next[1]) + ' AND ',
                        kwargs.items(), ''
                    )[:-4]}'''
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return [ModelInstance(cls, **res) for res in results]
        except Error as err:
            print(err)

    @classmethod
    def get(cls, **kwargs):
        try:
            return cls.filter(**kwargs)[0]
        except IndexError:
            return None

    def update(self, **kwargs):
        self.__init__()
        fields = self.get_fields(self)
        id = kwargs.pop('id')
        kwargs.pop('_ModelInstance__model', None)
        for name in kwargs.keys():
            if not name in fields:  # Checking if all fields specified right
                raise ModelException('Wrong fields specified in update method')
        try:  # Update database log
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor() as cursor:
                    query = f'''UPDATE {self.__name__}s SET {reduce(
                        lambda prev, next: prev + next[0] + ' = ' + fields[next[0]].to_sql(next[1]) + ', ',
                        kwargs.items(), ''
                    )[:-2]} WHERE id = {id}'''
                    cursor.execute(query)
                    connection.commit()
        except Error as err:
            print(err)

    def delete(self, id: int):
        self.__init__()
        try:  # Delete database log
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor() as cursor:
                    query = f'''DELETE FROM {self.__name__}s WHERE id = {id}'''
                    cursor.execute(query)
                    connection.commit()
        except Error as err:
            print(err)

    @classmethod  # Describes database table
    def describe(cls):
        cls.__init__(cls)
        try:
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    # Executing query and fetching results
                    cursor.execute(f'DESCRIBE {cls.__name__}s')
                    results = cursor.fetchall()
                    # Finding the longest statement in every column
                    cnames = ('Field name', 'Field type', 'Null', 'Key', 'Default value', 'Extra statement')
                    maxlens = [len(str(max(results, key=lambda res: len(str(res[k])))[k])) for k in results[0].keys()]
                    maxlens = [maxlens[i] if maxlens[i] > len(cnames[i]) else len(cnames[i]) for i in range(len(maxlens))]
                    # Console output
                    print(f'{cls.__name__}s table description:')
                    print(''.join([cnames[i] + ' ' * (maxlens[i] - len(cnames[i])) + '\t\t' for i in range(len(cnames))]))
                    for field in results:
                        vals = list(field.values())
                        print(''.join([
                            (
                                str(vals[i]) if str(vals[i]) else '-'
                            ) + ' ' * (
                                maxlens[i] - ((len(str(vals[i]))) if str(vals[i]) else 1)
                            ) + '\t\t' for i in range(len(field))
                        ]))
        except Error as err:
            print(err)

    @classmethod  # Simply executes query given
    def sql_query(cls, query: str):
        cls.__init__(cls)
        try:
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return results
        except Error as err:
            print(err)

    @staticmethod
    def create_m2m_table(m1, m2):
        try:
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor() as cursor:
                    query = f'''CREATE TABLE IF NOT EXISTS {m1.__name__}_{m2.__name__} (
                    {m1.__name__.lower()}_id int,
                    FOREIGN KEY ({m1.__name__.lower()}_id) REFERENCES {m1.__name__}s (id),
                    {m2.__name__.lower()}_id int,
                    FOREIGN KEY ({m2.__name__.lower()}_id) REFERENCES {m2.__name__}s (id),
                    CONSTRAINT unique_together UNIQUE ({m1.__name__.lower()}_id, {m2.__name__.lower()}_id)
                    )'''
                    cursor.execute(query)
        except Error as err:
            print(err)

    @staticmethod
    def read_m2m_table(m1, m2, m1_id):
        try:
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    query = f'''SELECT * FROM {m1.__name__}_{m2.__name__} WHERE {m1.__name__.lower()}_id = {m1_id}'''
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return sum([m2.filter(id=r[f'{m2.__name__.lower()}_id']) for r in results], [])
        except Error as err:
            print(err)

    @staticmethod
    def insert_m2m_table(m1, m2, m1_id, m2_id):
        try:
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    query = f'''INSERT INTO {m1.__name__}_{m2.__name__} (
                    {m1.__name__.lower()}_id, {m2.__name__.lower()}_id) VALUES ({m1_id}, {m2_id})'''
                    cursor.execute(query)
                    connection.commit()
        except Error as err:
            print(err)

    @staticmethod
    def delete_m2m_table(m1, m2, m2_id):
        try:
            with connect(
                    host=Model.__host,
                    user=Model.__login,
                    password=Model.__password,
                    database=Model.__db_name
            ) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    query = f'''DELETE FROM {m1.__name__}_{m2.__name__} WHERE {m2.__name__.lower()}_id = {m2_id}'''
                    cursor.execute(query)
                    connection.commit()
        except Error as err:
            print(err)
