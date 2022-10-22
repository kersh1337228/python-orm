from mysql.connector import connect, Error
from settings import db_data
from . import model as mdl, query as qr, containers as cont
import datetime
import json
from itertools import chain
from abc import ABC, abstractmethod


class Field(ABC):  # Basic model field class to inherit from
    def __init__(
            self, dtype: type,
            default=None,
            null: bool=True,
            unique: bool=False,
            choices: tuple=()
    ):
        if not isinstance(choices, tuple):
            raise TypeError(
                f'Wrong Field choices parameter value:'
                f' expected "tuple" but got "{type(choices).__name__}"'
            )
        if not all(map(lambda c: type(c) == dtype, choices)):
            raise TypeError(
                f'{type(self).__name__} choices parameter must contain only {dtype} values'
            )
        self.__name = None       # field name sometimes required in sql syntax
        self._choices = choices  # choices parameter restricts set of values attribute can take
        self._null = null        # null => {True => "", False => "NOT NULL"}
        self._unique = unique    # unique => {True => "UNIQUE", False => ""}
        self._default = default  # default = <value> => "DEFAULT <value>"
        super().__init__()

    def sql_init(self, name: str):  # Used in CREATE query to form column description
        self._name = name
        return ' ' + ' '.join((
            'UNIQUE' if self._unique else '',
            'NOT NULL' if not self._null else '',
            f'DEFAULT {self._default}' if self._default else '',
            f'CHECK ({name} IN {self._choices})' if self._choices else ''
        ))

    @abstractmethod  # Used to transform python type to sql type
    def to_sql(self, value):
        pass

    @abstractmethod  # Used to transform sql type to python type
    def from_sql(self, value):
        pass


class IntField(Field):  # Field to store int value aka SQL INTEGER
    def __init__(
            self,
            default: int=None,
            null: bool=True,
            unique: bool=False,
            choices: tuple[int]=()
    ):
        super().__init__(
            int, self.to_sql(default) if default else None,
            null, unique, choices
        )

    def sql_init(self, name: str):
        return f'{name} int' + super().sql_init(name)

    def to_sql(self, value: int | str):
        return str(value) if isinstance(value, int) else value

    def from_sql(self, value: int):
        return value


class UnsignedIntField(IntField):  # Field to store unsigned int value aka SQL INTEGER UNSIGNED
    def __init__(
            self,
            default: int=None,
            null: bool=True,
            unique: bool=False,
            choices: tuple[int]=()
    ):
        super().__init__(
            self.to_sql(default) if default else None,
            null, unique, choices
        )

    def sql_init(self, name: str):
        return f'{name} int unsigned' + super().sql_init(name)


class FloatField(Field):  # Field to store float value aka SQL FLOAT
    def __init__(
            self,
            default: float=None,
            null: bool=True,
            unique: bool=False,
            choices: tuple[int]=()
    ):
        super().__init__(
            int, self.to_sql(default) if default else None,
            null, unique, choices
        )

    def sql_init(self, name: str):
        return f'{name} float' + super().sql_init(name)

    def to_sql(self, value: float | str):
        return str(value) if isinstance(value, float) else value

    def from_sql(self, value: int):
        return value


class CharField(Field):  # Field to store short string value aka SQL VARCHAR
    def __init__(
            self,
            size: int=255,
            default: str=None,
            null: bool = True,
            unique: bool = False,
            choices: tuple[str]=()
    ):
        super().__init__(
            str, self.to_sql(default) if default else None,
            null, unique, choices
        )
        self.size = size

    def sql_init(self, name: str):
        return f'{name} VARCHAR({self.size})' + super().sql_init(name)

    def to_sql(self, value: str):
        return f'\'{value}\'' if isinstance(value, str) else value

    def from_sql(self, value: str):
        return value


class TextField(Field):  # Field to store long string value aka SQL TEXT
    def __init__(
            self,
            default=None,
            null: bool=True,
            unique: bool=False
    ):
        super().__init__(
            str, self.to_sql(default) if default else None,
            null, unique
        )

    def sql_init(self, name: str):
        return f'{name} TEXT' + super().sql_init(name)

    def to_sql(self, value: int):
        return f'\'{value}\'' if isinstance(value, str) else value

    def from_sql(self, value: str):
        return value


class DateTimeField(Field):  # Field to store datetime value aka SQL DATETIME
    def __init__(
            self,
            default: datetime.datetime=None,
            null: bool=True,
            unique: bool=False,
    ):
        super().__init__(
            datetime.datetime,
            self.to_sql(default) if default else None,
            null, unique
        )

    def sql_init(self, name: str):
        return f'{name} DATETIME' + super().sql_init(name)

    def to_sql(self, value: datetime.datetime):
        return f'\'{value.strftime("%Y-%m-%d %H:%M:%S")}\'' if isinstance(value, datetime.datetime) else value

    def from_sql(self, value: datetime.datetime):
        return value


class BooleanField(Field):  # Field to store bool value aka SQL BIT
    def __init__(
            self,
            default: bool=None,
            null: bool = True,
            unique: bool = False
    ):
        super().__init__(
            bool, self.to_sql(default) if default else None,
            null, unique
        )

    def sql_init(self, name: str):
        return f'{name} bit' + super().sql_init(name)

    def to_sql(self, value: bool):
        return str(int(value)) if isinstance(value, bool) else value

    def from_sql(self, value: bool):
        return value


class JSONField(Field):  # Field to store dict value aka SQL JSON
    def __init__(
            self,
            default: dict=None,
            null: bool = True,
            unique: bool = False,
    ):
        super().__init__(
            dict, self.to_sql(default) if default else None,
            null, unique
        )

    def sql_init(self, name: str):
        return f'{name} JSON' + super().sql_init(name)

    def to_sql(self, value: dict):
        return f'\'{json.dumps(value)}\'' if isinstance(value, dict) else value

    def from_sql(self, value: dict):
        return value


class DurationField(Field):  # Field to store timedelta value aka SQL INT
    def __init__(
            self,
            default: datetime.timedelta=None,
            null: bool = True,
            unique: bool = False
    ):
        super().__init__(
            datetime.timedelta,
            self.to_sql(default) if default else None,
            null, unique
        )

    def sql_init(self, name: str):
        return f'''{name} int''' + super().sql_init(name)

    def to_sql(self, value: datetime.timedelta):
        return str(int(datetime.timedelta.total_seconds(value))) if isinstance(value, datetime.timedelta) else value

    def from_sql(self, value: int):
        return datetime.timedelta(seconds=value)

# ON DELETE and ON UPDATE options list
CASCADE = 'CASCADE'
RESTRICT = 'RESTRICT'
SET_NULL = 'SET NULL'
SET_DEFAULT = 'SET DEFAULT'
NO_ACTION = 'NO ACTION'


class LinkField(ABC):  # Field to inherit from for table linking fields
    def __init__(
            self,
            on_delete: str=NO_ACTION,
            on_update: str=NO_ACTION
    ):
        self._on_delete = on_delete
        self._on_update = on_update

    def sql_init(self):
        return f""" ON DELETE {self._on_delete} ON UPDATE {self._on_update}"""

    @abstractmethod
    def get_joins(  # Get data to assemble SQL JOIN in primary SELECT query
            self,
            parent: str,  # Outer table
            field: str,  # Field name
            primary_join_index: int,  # Join unique identifier for regular SELECT
            annotate_join_index: int  # Join unique identifier for annotated fields
    ):
        pass


class ForeignKey(IntField, LinkField):  # Field to link models via many-to-one relationships aka SQL FOREIGN KEY
    def __init__(
            self,
            ref: mdl.Model,  # Reference model
            null: bool = True,
            unique: bool = False,
            on_delete: str=NO_ACTION,
            on_update: str=NO_ACTION
    ):
        IntField.__init__(self, None, null, unique)
        LinkField.__init__(self, on_delete, on_update)
        self.ref = ref

    def to_sql(self, value: mdl.ModelInstance):
        if self.ref != value.model:
            raise TypeError(
                f'Wrong model type for ForeignKey: '
                f'expected {self.ref.__name__} but got {value.model.__name__}'
            )
        else:
            return str(value.id)

    def from_sql(self, value: int):
        return value

    def sql_init(self, name: str):
        return IntField.sql_init(self, name) + f""", FOREIGN KEY ({name
        }) REFERENCES {self.ref.table_name} (id)""" + LinkField.sql_init(self)

    def get_joins(
            self,
            parent: str,
            field: str,
            primary_join_index: int,
            annotate_join_index: int
    ):
        return {
            'type': 'LEFT',
            'table': self.ref.table_name,
            'alias': f'{self.ref.table_name}'
                     f'{primary_join_index}'
                     f'{annotate_join_index}',
            'on': f'{parent}.{field} = {self.ref.table_name}'
                  f'{primary_join_index}'
                  f'{annotate_join_index}.id',
            'field': field
        },


class ManyToManyField(IntField, LinkField):  # Field to link models via many-to-many relationships aka SQL TABLE m1_m2
    def __init__(
            self,
            ref: mdl.Model,  # Reference model
            on_delete: str = NO_ACTION,
            on_update: str = NO_ACTION
    ):
        IntField.__init__(self)
        LinkField.__init__(self, on_delete, on_update)
        self.__m1 = None
        self.__m2 = ref

    @property
    def m1(self):
        return self.__m1

    @m1.setter
    def m1(self, value):
        self.__m1 = value

    @property
    def ref (self):
        return self.__m2

    def sql_init(self, name: str):
        super().sql_init(name)
        return ''

    def get_joins(
            self,
            parent: str,
            field: str,
            primary_join_index: int,
            annotate_join_index: int
    ):
        return {
            'type': 'LEFT',
            'table': f'{self.__m1.__name__}_{self.__m2.__name__}',
            'alias': f'joint_table{primary_join_index}'
                     f'{annotate_join_index}',
            'on': f'{parent}.id = joint_table{primary_join_index}'
                  f'{annotate_join_index}.{self.__m1.__name__.lower()}_id',
            'field': f'{field}_joint'
        }, {
            'type': 'LEFT',
            'table': self.__m2.table_name,
            'alias': f'{self.__m2.table_name}{primary_join_index}'
                     f'{annotate_join_index}',
            'on': f'joint_table{primary_join_index}'
                  f'{annotate_join_index}.{self.__m2.__name__.lower()}_id = '
                  f'{self.__m2.table_name}'
                  f'{primary_join_index}'
                  f'{annotate_join_index}.id',
            'field': field
        }


    def create(self):
        try:  # Creating junction table
            with connect(**db_data) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {
                    self.__m1.__name__}_{self.__m2.__name__} ({
                    self.__m1.__name__.lower()}_id int,
                    FOREIGN KEY ({self.__m1.__name__.lower()}_id) REFERENCES {
                    self.__m1.table_name} (id) ON DELETE CASCADE ON UPDATE CASCADE,
                    {self.__m2.__name__.lower()}_id int,
                    FOREIGN KEY ({self.__m2.__name__.lower()}_id) REFERENCES {
                    self.__m2.table_name} (id) {LinkField.sql_init(self)
                    }, CONSTRAINT unique_together UNIQUE ({
                    self.__m1.__name__.lower()}_id, {self.__m2.__name__.lower()}_id)
                    )''')
        except Error as err:
            print(err)

    def select(self, m1_id: int):
        try:  # Selecting rows from junction table
            with connect(**db_data) as connection:
                with connection.cursor() as cursor:
                    m1_name, m2_name = self.__m1.__name__, self.__m2.__name__
                    cursor.execute(
                        f"""SELECT {m2_name.lower()}_id FROM {m1_name}_{m2_name} WHERE {
                        m1_name.lower()}_id = {m1_id}"""
                    )
                    return cont.QuerySet(self.__m2, {
                        'args': (),
                        'kwargs': {'id__in': tuple(chain(*cursor.fetchall()))}
                    })
        except Error as err:
            print(err)

    def insert(self, m1_id: int, m2_id: int):
        try:  # Inserting row into junction table
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    m1_name, m2_name = self.__m1.__name__, self.__m2.__name__
                    cursor.execute(
                        f'''INSERT INTO {m1_name}_{m2_name} ({
                        m1_name.lower()}_id, {m2_name.lower()
                        }_id) VALUES ({m1_id}, {m2_id})'''
                    )
                    connection.commit()
        except Error as err:
            print(err)

    def delete(self, m2_id: int):
        try:  # Deleting row from junction table
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        f'''DELETE FROM {self.__m1.__name__}_{self.__m2.__name__
                        } WHERE {self.m2.__name__.lower()}_id = {m2_id}'''
                    )
                    connection.commit()
        except Error as err:
            print(err)


class LinkFieldInstance:  # Field instance lazy wrapper for nested models fields
    def __init__(self, cache: dict=None):
        self._cache = cache if cache else {}


class ForeignKeyInstance(LinkFieldInstance):  # Wrapper to work with ForeignKey field using model instance
    def __init__(self, fk: ForeignKey, id: int, cache: dict=None):
        self.__fk = fk
        self.__id = id
        self.__ref = cache.get(
            self.__fk.ref.__name__.lower(), None
        ) if cache else None
        super().__init__(cache)

    def __getattr__(self, item):
        if not self.__ref:  # Make lazy database select
            self.__ref = self.__fk.ref.get(id=self.__id)
        # Dragging cache inside nested models fields
        for name, val in self._cache.items():
            getattr(  # Getting nested model field by name
                self.__ref, name.split('__')[1]
            )._cache.update(  # Direct cache update
                {'__'.join(name.split('__')[1:]): val}
            )
        return getattr(self.__ref, item)


class ManyToManyFieldInstance(LinkFieldInstance):  # Wrapper to work with M2M field using model instance
    def __init__(self, m2m: ManyToManyField, m1_id: int, cache: dict=None):
        self.__m2m = m2m
        self.__m1_id = m1_id
        self.__refs = cont.QuerySet(self.__m2m.ref, cache.get(
            self.__m2m.ref.__name__.lower(), None
        )) if cache else m2m.select(m1_id)
        super().__init__(cache)

    def __data_access(self):
        for el in self.__refs:
            # Dragging cache inside nested models fields
            for name, val in self._cache.items():
                fnames = name.split('__')[1:]
                attr = el
                for fname in fnames:  # Getting proper nested model
                    try:
                        attr = getattr(attr, fname)
                    except AttributeError:
                        break
                getattr(  # Getting nested model field by name
                    el, name.split('__')[1]
                )._cache.update(  # Direct cache update
                    {'__'.join(name.split('__')[1:]): next(filter(
                        lambda x: x.id == attr.id, val
                    ))}
                )

    def append(self, ref):  # Appending model instance to model's m2m
        if not issubclass(type(ref), mdl.ModelInstance):
            raise TypeError(
                f'You can only store model instances in ManyToManyField:'
                f' got type "{type(ref).__name__}"'
            )
        elif ref.model != self.__m2m.m2:
            raise TypeError(
                f"Model type does not match ManyToManyField's one:"
                f" expected {self.__m2m.m2.__name__} but got {ref.model.__name__}"
            )
        self.__m2m.insert(self.__m1_id, ref.id)
        self.__refs = self.__refs + ref.model.filter(id=ref.id)

    def delete(self, ref):  # Deleting model instance from model's m2m
        if ref in self.__refs:
            self.__m2m.delete(ref.id)
            self.__refs = self.__refs.exclude(id=ref.id)
        else:
            raise KeyError('No submodel found in ManyToManyField')
    # Next methods make projection on nested QuerySet object
    def filter(self, **kwargs):
        return self.__refs.filter(**kwargs)

    def get(self, *args, **kwargs):
        return self.__refs.get(*args, **kwargs)

    def exclude(self, *args, **kwargs):
        return self.__refs.exclude(*args, **kwargs)

    def __iter__(self):
        result = self.__refs.__iter__()
        self.__data_access()
        return result

    def __getitem__(self, key: int | slice):
        result = self.__refs.__getitem__(key)
        self.__data_access()
        return result

    def __contains__(self, item):
        return self.__refs.__contains__(item)

    def __str__(self) -> str:
        return self.__refs.__str__()

    def __len__(self):
        return self.__refs.__len__()
