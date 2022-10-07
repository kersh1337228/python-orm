from mysql.connector import connect, Error
from settings import db_data
from functools import reduce
from . import fields as fld, query as qr, containers as cont


class ModelInstance:
    def __init__(self, model, **kwargs):
        fields = model.fields
        for name, value in kwargs.items():
            try:
                attr = getattr(model, name)
                setattr(self, name, attr.from_sql(value))
            except AttributeError:
                setattr(self, name, value)
        self.__model = model
        for name, value in fields.items():
            if isinstance(value, fld.ManyToManyField):
                fields[name].set_m1(model)
                setattr(self, name, fld.ManyToManyFieldInstance(value, self.id))

    def save(self):  # Saves changes manually appended to model instance via <model>.<field> = <value>
        self.__model.check_table()
        try:  # UPDATE command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        f"""UPDATE {self.__model.table_name} SET {', '.join([
                        f'''{self.__model.table_name}.{name} = {
                        self.__model.fields[name].to_sql(val)}'''
                        for name, val in self.__model.fields.items() 
                        if name != 'id'
                        ])} WHERE {self.__model.table_name}.id = {self.id}"""
                    )
                    connection.commit()
        except Error as err:
            print(err)

    def delete(self):  # Deletes model instance row by id
        self.__model.check_table()
        try:  # DELETE command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        f"""DELETE FROM {self.__model.table_name
                        } WHERE id = {self.id}"""
                    )
                    connection.commit()
        except Error as err:
            print(err)


class Model:
    def __validate_field_names(self):  # Validating model field names
        for name in self.fields.keys():
            if '__' in name:
                raise Exception('Field name must not contain "__" symbol combination')

    @classmethod
    @property
    def table_name(cls):  # Returns model table name
        return f'{cls.__name__}s'

    @classmethod
    @property
    def fields(cls):
        try:
            return cls.__fields
        except AttributeError:
            fields = {'id': fld.IntField(null=False, unique=True)}
            for name in dir(cls):
                try:
                    if name == 'fields':
                        raise AttributeError
                    attr = getattr(cls, name)
                    if issubclass(type(attr), fld.Field):
                        fields[name] = attr
                except:
                    continue
            for name, field in fields.items():
                if isinstance(field, fld.ForeignKey):
                    field.set_name(name)
            cls.__fields = fields
            return fields

    @classmethod
    def check_table(cls):
        try:  # Check if model table exists, create if not
            with connect(**db_data) as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SHOW TABLES')
                    tables = cursor.fetchall()
                    if not (cls.table_name,) in tables:
                        cls.__init__(cls)
        except Error as err:
            print(err)

    def __init__(self):  # Check if db table exists. If not creates one.
        try:  # Validating field list either in @classmethod...
            self.__validate_field_names(self)
        except TypeError:  # ... or in regular method
            self.__validate_field_names()
        try:
            with connect(**db_data) as connection:
                with connection.cursor() as cursor:
                    query = f'''CREATE TABLE IF NOT EXISTS {self.table_name()} (
                    id int NOT NULL UNIQUE AUTO_INCREMENT,
                    PRIMARY KEY (id),
                    {reduce(
                        lambda prev, next: prev + f'{next[0]} {next[1].sql_init()}, ' 
                        if not isinstance(next[1], fld.ManyToManyField) else prev,
                        fields.items(), ''
                    )[:-2]})'''
                    cursor.execute(query)
        except Error as err:
            print(err)
        [   # Creating m2m field tables and setting current model as m1
            (m2m.set_m1(self), m2m.create_table())
            for m2m in fields.values()
            if isinstance(m2m, fld.ManyToManyField)
        ]

    @classmethod
    def create(cls, **kwargs):  # Creating new row in table
        cls.check_table()
        vals = []  # Placeholder for SQL-friendly fields values
        for name, val in kwargs.items():
            if not name in cls.fields:  # Checking if all fields specified right
                raise Exception('Wrong fields specified in create method')
            else:  # Converting fields value to SQL-friendly form
                vals.append(cls.fields[name].to_sql(val))
        try:  # Creating database log
            with connect(**db_data) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f'''INSERT INTO {cls.table_name} ({', '.join(
                        kwargs.keys()
                    )}) VALUES ({', '.join(vals)})''')
                    connection.commit()
        except Error as err:
            print(err)
        return ModelInstance(cls, **kwargs)

    @classmethod
    def filter(cls, *args, **kwargs):
        return cont.QuerySet(cls, {'args': args, 'kwargs': kwargs})

    @classmethod
    def get(cls, **kwargs):
        try:
            return cls.filter(**kwargs)[0]
        except IndexError:
            return None

    @classmethod  # Drops database table associated with model
    def drop(cls):
        cls.__check_table()
        try:
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(f'DROP TABLE IF EXISTS {cls.__name__}s CASCADE')
        except Error as err:
            print(err)

    @classmethod  # Describes database table
    def describe(cls):
        cls.__check_table()
        try:
            with connect(**db_data) as connection:
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
                    for field in results:  # Adding spaces to fill max column length
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
        cls.__check_table()
        try:
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(query.replace('%s', f'{cls.__name__}s'))
                    results = cursor.fetchall()
                    return results
        except Error as err:
            print(err)
