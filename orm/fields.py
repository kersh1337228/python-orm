from mysql.connector import connect, Error
from . import model as m
from .query import Q
import datetime
import json
from abc import ABC, abstractmethod


class Field(ABC):  # Basic model field class to inherit from
    def __init__(self, default=None, null: bool=True, unique: bool=False):
        self.null = null        # null => {True => "", False => "NOT NULL"}
        self.unique = unique    # unique => {True => "UNIQUE", False => ""}
        self.default = default  # default = <value> => "DEFAULT <value>"
        super().__init__()

    def sql_init(self):  # Used in CREATE query to form column description
        return f"{' UNIQUE' if self.unique else ''}" \
               f"{' NOT NULL' if not self.null else ''}" \
               f"{f' DEFAULT {self.default}' if self.default else ''}"

    @abstractmethod  # Used to transform python type to sql type
    def to_sql(self, value):
        pass

    @abstractmethod  # Used to transform sql type to python type
    def from_sql(self, value):
        pass


class IntField(Field):  # Field to store int value aka SQL INTEGER
    def __init__(self, default=None, null: bool=True, unique: bool=False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''int''' + super().sql_init()

    def to_sql(self, value: int | str):
        return str(value) if isinstance(value, int) else value

    def from_sql(self, value: int):
        return value


class CharField(Field):  # Field to store short string value aka SQL VARCHAR
    def __init__(self, default=None, size: int=255, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)
        self.size = size

    def sql_init(self):
        return f'''VARCHAR({self.size})''' + super().sql_init()

    def to_sql(self, value: str):
        return f'\'{value}\'' if isinstance(value, str) else value

    def from_sql(self, value: str):
        return value


class TextField(Field):  # Field to store long string value aka SQL TEXT
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''TEXT''' + super().sql_init()

    def to_sql(self, value: int):
        return f'\'{value}\'' if isinstance(value, str) else value

    def from_sql(self, value: str):
        return value


class DateTimeField(Field):  # Field to store datetime value aka SQL DATETIME
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''DATETIME''' + super().sql_init()

    def to_sql(self, value: datetime.datetime):
        return f'\'{value.strftime("%Y-%m-%d %H:%M:%S")}\'' if isinstance(value, datetime.datetime) else value

    def from_sql(self, value: datetime.datetime):
        return value


class BooleanField(Field):  # Field to store bool value aka SQL BIT
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''bit''' + super().sql_init()

    def to_sql(self, value: bool):
        return str(int(value)) if isinstance(value, bool) else value

    def from_sql(self, value: bool):
        return value


class JSONField(Field):  # Field to store dict value aka SQL JSON
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''JSON''' + super().sql_init()

    def to_sql(self, value: dict):
        return f'\'{json.dumps(value)}\'' if isinstance(value, dict) else value

    def from_sql(self, value: dict):
        return value


class DurationField(Field):  # Field to store timedelta value aka SQL INT
    def __init__(self, default=None, null: bool = True, unique: bool = False):
        super().__init__(self.to_sql(default) if default else None, null, unique)

    def sql_init(self):
        return f'''int''' + super().sql_init()

    def to_sql(self, value: datetime.timedelta):
        return str(int(datetime.timedelta.total_seconds(value))) if isinstance(value, datetime.timedelta) else value

    def from_sql(self, value: int):
        return datetime.timedelta(seconds=value)


class ForeignKey(IntField):  # Field to link models via many-to-one relationships aka SQL FOREIGN KEY
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
        return super().sql_init() + f', FOREIGN KEY ({self.name}) REFERENCES {self.ref.table_name()} (id)'


class ManyToManyField(IntField):  # Field to link models via many-to-many relationships aka SQL TABLE m1_m2
    def __init__(self, ref, null: bool = True, unique: bool = False):
        super().__init__(None, null, unique)
        self.m2 = ref

    def create_table(self):
        try:
            with connect(**m.db_data) as connection:
                with connection.cursor() as cursor:
                    m1_name, m2_name = self.m1.__name__, self.m2.__name__
                    query = f'''CREATE TABLE IF NOT EXISTS {m1_name}_{m2_name} (
                    {m1_name.lower()}_id int,
                    FOREIGN KEY ({m1_name.lower()}_id) REFERENCES {m1_name}s (id),
                    {m2_name.lower()}_id int,
                    FOREIGN KEY ({m2_name.lower()}_id) REFERENCES {m2_name}s (id),
                    CONSTRAINT unique_together UNIQUE ({m1_name.lower()}_id, {m2_name.lower()}_id)
                    )'''
                    cursor.execute(query)
        except Error as err:
            print(err)

    def read_table(self, m1_id: int):
        try:
            with connect(**m.db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    m1_name, m2_name = self.m1.__name__, self.m2.__name__
                    query = f'SELECT * FROM {m1_name}_{m2_name} ' \
                            f'WHERE {m1_name.lower()}_id = {m1_id}'
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return self.m2.filter(Q.Or(*[Q(id=r[f'{m2_name.lower()}_id']) for r in results]))
        except Error as err:
            print(err)

    def insert_table(self, m1_id: int, m2_id: int):
        try:
            with connect(**m.db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    m1_name, m2_name = self.m1.__name__, self.m2.__name__
                    query = f'''INSERT INTO {m1_name}_{m2_name} (
                    {m1_name.lower()}_id, {m2_name.lower()}_id)
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


class ManyToManyFieldInstance:  # Wrapper to work with M2M field using model instance
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
