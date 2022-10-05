from mysql.connector import connect, Error
from .exceptions import ModelException
from . import model as m
import datetime
import json



class Field:
    def __init__(self, default=None, null: bool=True, unique: bool=False):
        self.null = null
        self.unique = unique
        self.default = default

    def sql_init(self):
        return f'''
        {' UNIQUE' if self.unique else ''}
        {' NOT NULL' if not self.null else ''}
        {f' DEFAULT {self.default}' if self.default else ''}
        '''


class IntField(Field):
    def __init__(self, default=None, null: bool=True, unique: bool=False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''int''' + super().sql_init()

    def to_sql(self, value: int | str):
        return str(value) if isinstance(value, int) else value

    def from_sql(self, value: int):
        return value


class CharField(Field):
    def __init__(self, default=None, size: int=255, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)
        self.size = size

    def sql_init(self):
        return f'''VARCHAR({self.size})''' + super().sql_init()

    def to_sql(self, value: str):
        return f'\'{value}\'' if isinstance(value, str) else value

    def from_sql(self, value: str):
        return value


class TextField(Field):
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''TEXT''' + super().sql_init()

    def to_sql(self, value: int):
        return f'\'{value}\'' if isinstance(value, str) else value

    def from_sql(self, value: str):
        return value


class DateTimeField(Field):
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''DATETIME''' + super().sql_init()

    def to_sql(self, value: datetime.datetime):
        return f'\'{value.strftime("%Y-%m-%d %H:%M:%S")}\'' if isinstance(value, datetime.datetime) else value

    def from_sql(self, value: datetime.datetime):
        return value


class BooleanField(Field):
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''bit''' + super().sql_init()

    def to_sql(self, value: bool):
        return str(int(value)) if isinstance(value, bool) else value

    def from_sql(self, value: bool):
        return value


class JSONField(Field):
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''JSON''' + super().sql_init()

    def to_sql(self, value: dict):
        return f'\'{json.dumps(value)}\'' if isinstance(value, dict) else value

    def from_sql(self, value: dict):
        return value


class DurationField(Field):
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''int''' + super().sql_init()

    def to_sql(self, value: datetime.timedelta):
        return str(int(datetime.timedelta.total_seconds(value))) if isinstance(value, datetime.timedelta) else value

    def from_sql(self, value: int):
        return datetime.timedelta(seconds=value)


class ForeignKey(IntField):
    def __init__(self, ref, null: bool = True, unique: bool = False):
        super().__init__(None, null, unique)
        self.ref = ref

    def to_sql(self, value: int):
        return str(value.id)

    def from_sql(self, value: str):
        return self.ref.get(id=value)

    def set_name(self, name: str):
        self.name = name

    def sql_init(self):
        return super().sql_init() + f''', FOREIGN KEY ({self.name}) REFERENCES {self.ref.__name__}s (id)'''


class ManyToManyField(IntField):
    def __init__(self, ref, null: bool = True, unique: bool = False):
        super().__init__(None, null, unique)
        self.m2 = ref

    def create_table(self):
        try:
            with connect(**m.db_data) as connection:
                with connection.cursor() as cursor:
                    query = f'''CREATE TABLE IF NOT EXISTS {self.m1.__name__}_{self.m2.__name__} (
                    {self.m1.__name__.lower()}_id int,
                    FOREIGN KEY ({self.m1.__name__.lower()}_id) REFERENCES {self.m1.__name__}s (id),
                    {self.m2.__name__.lower()}_id int,
                    FOREIGN KEY ({self.m2.__name__.lower()}_id) REFERENCES {self.m2.__name__}s (id),
                    CONSTRAINT unique_together UNIQUE ({self.m1.__name__.lower()}_id, {self.m2.__name__.lower()}_id)
                    )'''
                    cursor.execute(query)
        except Error as err:
            print(err)

    def read_table(self, m1_id: int):
        try:
            with connect(**m.db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    query = f'''SELECT * FROM {self.m1.__name__}_{self.m1.__name__} 
                    WHERE {self.m1.__name__.lower()}_id = {m1_id}'''
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return sum([self.m2.filter(id=r[f'{self.m2.__name__.lower()}_id']) for r in results], [])
        except Error as err:
            print(err)

    def insert_table(self, m1_id: int, m2_id: int):
        try:
            with connect(**m.db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    query = f'''INSERT INTO {self.m1.__name__}_{self.m2.__name__} (
                    {self.m1.__name__.lower()}_id, {self.m2.__name__.lower()}_id)
                     VALUES ({m1_id}, {m2_id})'''
                    cursor.execute(query)
                    connection.commit()
        except Error as err:
            print(err)

    def delete_table(self, m2_id: int):
        try:
            with connect(**m.db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    query = f'''DELETE FROM {self.m1.__name__}_{self.m2.__name__}
                     WHERE {self.m2.__name__.lower()}_id = {m2_id}'''
                    cursor.execute(query)
                    connection.commit()
        except Error as err:
            print(err)

    def set_m1(self, m1):
        self.m1 = m1

    def sql_init(self):
        return ''


class ManyToManyFieldInstance:
    def __init__(self, m2m: ManyToManyField, m1_id: int):
        self.m2m = m2m
        self.m1_id = m1_id
        self.refs = m2m.read_table(m1_id)

    def append(self, ref):
        self.m2m.insert_table(self.m1_id, ref.id)
        self.refs.append(ref)

    def filter(self, **kwargs):  # MAKE SQL BASED
        fields = self.m2m.m2.get_fields(self.m2m.m2)
        id = kwargs.pop('id', None)
        for name in kwargs.keys():
            if not name in fields:  # Checking if all fields specified right
                raise ModelException('Wrong fields specified in m2m get method')
        if id: kwargs['id'] = id
        def constraint(ref):
            return all([getattr(ref, name) == value for name, value in kwargs.items()])
        return list(filter(constraint, self.refs))

    def get(self, **kwargs):
        try:
            return self.filter(**kwargs)[0]
        except IndexError:
            return None

    def delete(self, ref):
        if ref.id in [r.id for r in self.refs]:
            self.m2m.delete_table(ref.id)
            self.refs = [r for r in self.refs if r.id != ref.id]
        else:
            raise ModelException('No model found in m2m field')
