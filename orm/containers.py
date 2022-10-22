import mysql.connector.cursor
from mysql.connector import connect, Error
from settings import db_data
from . import fields as fld, model as mdl, query as qr, aggregate as aggr
import re


# Container for queries and Model instances selected. Core of ORM API.
# Wraps query parameters and model to perform further lazy select.
# Select only occurs after direct data access attempt.
class QuerySet:
    # Custom iterator wrapping QuerySet container (lazy).
    class __QuerySetIterator:
        def __init__(self, container):
            self.__container = container
            self.__index = -1  # Current iteration index

        def __iter__(self):
            return self

        def __next__(self):
            self.__index += 1
            try:
                return self.__container[self.__index]
            except IndexError:
                raise StopIteration

    # QuerySet wrapper object to hide methods that
    # cannot be converted into SQL due to slice (non-lazy).
    class __QuerySetSlice:
        def __init__(self, query_set):
            self.__query_set = query_set

        def __iter__(self):
            return self.__query_set.__iter__()

        def __getitem__(self, key: int | slice):  # Item access causes query instantaneous execution
            if isinstance(key, (int, slice)):  # Only slice and int indexes are allowed
                self.__query_set.execute()
                return self.__query_set[key]
            else:
                raise TypeError(
                    'QuerySetSlice __getitem__() method supports '
                    'only int and slice argument types'
                )

        def __str__(self) -> str:
            return f'<QuerySetSlice({self.__query_set.__str__()})>'

        def __len__(self):
            return self.__query_set.__len__()

        def update(self, **kwargs) -> None:  # Updates elements matching query
            self.__query_set.update(**kwargs)

        def delete(self) -> None:  # Deletes elements matching query
            self.__query_set.delete()

    def __init__(self, model, container: tuple = (), *args, **kwargs):
        self.__model = model  # Inner model class allowing to gain access to fields list, table name, etc.
        self.__query = {
            'args': args,  # Q class query aka Q, Q.Not, Q.And, Q.Or
            'kwargs': kwargs,  # Keyword query
            'order_by': [],  # Fields list for ORDER BY command
            'annotate': {  # Annotated fields list
                'args': (),  # Automatically aliased annotations
                'kwargs': {}  # Manually aliased annotations
            },
            'select_related': [],  # ForeignKey fields list for early select
            'prefetch_related': [],  # ManyToMany fields list for early select
        }
        self.__union = []  # Storage for QuerySets to be united aka UNION command
        self.__executed = False  # Inner query execution indicator
        self.__container = container  # Query selected data storage

    def __exec(self) -> None:  # Lazy query execution
        self.__model.check_table()  # Check if necessary table exists
        try:  # SELECT command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        ' UNION '.join(  # Custom query assembly and execution
                            qr.assemble_query(self.__model, q)
                            for q in [self.__query] + self.__union
                        )  # Fetching results first not to lose if
                    )      # another query executes inside __prefetch() method
                    results = cursor.fetchall()
                    self.__container = tuple(  # Filling inner container with model instances
                        mdl.ModelInstance(
                            self.__model,  # Model template
                            self.__query['select_related'],  # select_related() fields (ForeignKey)
                            self.__prefetch(cursor),  # prefetch_related() fields (ManyToManyField)
                            **res  # Rows fetched
                        ) for res in results
                    )  # Toggling execution indicator
                    self.__executed = True
        except Error as err:
            print(err)

    def __iter__(self) -> __QuerySetIterator:
        if not self.__executed:  # Iterating requires direct data access
            self.__exec()
        return QuerySet.__QuerySetIterator(self.__container)

    def __getitem__(self, key: int | slice):  # Slice and int index selection
        if isinstance(key, int):  # ModelInstance select
            if key < 0:  # Reverse order and ordinary select
                self.__query['order_by'].insert(0, '-id')
                return self.__getitem__(-key)
            else:  # Ordinary select
                self.__query['offset'] = key
                self.__query['limit'] = 1
                self.__exec()
                return self.__container[0]
        elif isinstance(key, slice):  # QuerySet or QuerySetSlice select
            if not self.__executed:  # If container is empty then altering query
                if not key.start and not key.stop and key.step == -1:  # Simply reversing order
                    self.__query['order_by'].insert(0, '-id')
                    return self
                elif key.start and key.stop and 0 <= key.start < key.stop:
                    match key.start, key.stop:
                        case start, None:  # Start only specified -> OFFSET <start>
                            self.__query['offset'] = start
                        case None, stop:  # Stop only specified -> LIMIT <stop>
                            self.__query['limit'] = stop
                        case start, stop:  # Both start and stop specified -> LIMIT <stop> OFFSET <start>
                            self.__query['offset'] = start
                            self.__query['limit'] = stop - start
                    return QuerySet.__QuerySetSlice(self)
                else:  # If no SQL-converting found just making query
                    self.__exec()
                    return self.__container[key]
            else:  # Direct access if already executed
                return self.__container[key]
        else:  # Wrong ket format
            raise TypeError(
                'QuerySet __getitem__() method supports '
                'only int and slice argument types.'
            )

    def __contains__(self, item) -> bool:  # Check if ModelInstance in QuerySet
        if not issubclass(type(item), mdl.ModelInstance):
            raise TypeError(
                'QuerySet object can only store model instances.'
            )
        elif item.model.__name__ != self.__model.__name__:  # QuerySet is homogeneous
            raise TypeError(
                f'''Wrong model for this QuerySet: expected "{
                self.__model.__name__}" but got "{item.model.__name__}".'''
            )
        else:
            if not self.__executed:
                self.__exec()
            return item.id in (el.id for el in self.__container)

    def __str__(self) -> str:
        if not self.__executed:
            return '<QuerySet(unexecuted)>'
        else:
            return f'<QuerySet{self.__container}>'

    def __len__(self) -> int:
        if not self.__executed:
            self.__model.check_table()
            try:  # SELECT COUNT command
                with connect(**db_data) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            qr.assemble_query(
                                model=self.__model,
                                query=self.__query,
                                aggregate_fields={
                                    'args': (aggr.Count('id'),),
                                    'kwargs': {}
                                }
                            )
                        )
                        results = cursor.fetchall()
                        return results[0][0]
            except Error as err:
                print(err)
        else:
            return len(self.__container)

    def filter(self, *args, **kwargs):  # SELECT WHERE ...
        self.__executed = False
        self.__container = ()
        self.__query['args'] += args
        self.__query['kwargs'].update(kwargs)
        return self

    def get(self, *args, **kwargs):  # SELECT WHERE ... LIMIT 1
        try:  # Returns model instance matching query...
            return self.filter(*args, **kwargs)[0]
        except IndexError:  # ... or None if nothing was found
            return None

    def exclude(self, *args, **kwargs):  # SELECT WHERE NOT ...
        self.__executed = False
        self.__container = ()
        self.__query['args'] += (~qr.Q.And(
            *(args + tuple(qr.Q(**{name: val}) for name, val in kwargs.items()))
        ),)
        return self

    def order_by(self, *args):  # *args format: '(-)<field>__<subfield>__...__<subfield>'
        if not all(map(lambda arg: isinstance(arg, str), args)):
            raise TypeError(
                f'Got wrong argument type for order_by() method.'
            )
        self.__query['order_by'].extend(args)
        return self

    @staticmethod
    def __validate_aggregate(*args, **kwargs):  # Used by aggregate() and annotate() methods
        if len(args) + len(kwargs) > 0:  # Check if is enough arguments
            if not all(map(lambda arg: issubclass(  # Arguments type check
                    type(arg), aggr.BaseAggregate  # Single aggregate function
            ) or isinstance(
                arg, aggr.AggregateOperationWrapper  # Operation with multiple aggregate functions
            ), args + tuple(kwargs.values()))):
                raise TypeError(  # Check if arguments specified are aggregate wrappers
                    'Expected BasicAggregate subclass or AggregateOperationWrapper '
                    'class instances as args values for aggregate() method.'
                )
        else:
            raise ValueError(
                'At least one argument '
                'required for aggregate() method.'
            )

    def aggregate(self, *args, **kwargs):  # SELECT Aggr(...) as alias, ... command
        QuerySet.__validate_aggregate(*args, **kwargs)
        try:  # SELECT command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        qr.assemble_query(
                            model=self.__model,
                            query=self.__query,
                            aggregate_fields={
                                'args': args,  # Auto alias expressions
                                'kwargs': kwargs  # Alias-specified expressions
                            }
                        )
                    )
                    result = cursor.fetchall()
                    return result
        except Error as err:
            print(err)

    def annotate(self, *args, **kwargs):  # SELECT ..., (SELECT Aggr(...) ...) as alias, ... command
        QuerySet.__validate_aggregate(*args, **kwargs)
        self.__query['annotate']['args'] += args
        self.__query['annotate']['kwargs'].update(kwargs)
        return self

    def __validate_related(
            self,
            method_name: str,
            types_allowed: tuple[type],
            *args
    ):
        if len(args) > 0:
            if not all(
                map(lambda arg: isinstance(arg, str) and
                arg.split('__')[0] in self.__model.fields, args)
            ):
                raise ValueError(
                    'Wrong arguments format for '
                    f'{method_name}_related() method.'
                )
            for arg in args:
                current_model, fnames = self.__model, arg.split('__')
                for fname in fnames:  # Getting proper nested model
                    try:
                        attr = getattr(current_model, fname)
                        current_model = attr.ref
                        if not isinstance(attr, types_allowed):
                            raise TypeError(
                                f'Wrong type found in {method_name}_related() method argument. '
                                f'Expected {" or ".join(t.__name__ for t in types_allowed)} '
                                f'but got {type(attr).__name__}.'
                            )
                    except AttributeError:
                        break
        else:
            raise ValueError(
                'At least one argument required '
                f'by {method_name}_related() method.'
            )

    def select_related(self, *args):  # SELECT with ForeignKey fields
        self.__validate_related('select', (fld.ForeignKey,), *args)
        self.__query['select_related'].extend(args)
        return self

    def __prefetch(
            self,
            cursor: mysql.connector.cursor.MySQLCursorBufferedDict
    ) -> tuple[list[dict], dict]:  # Separating ManyToMany fields from others
        prefetched = {}
        if self.__query['prefetch_related']:
            joins, fields, _ = qr.Q.make_related_fields(
                self.__model, 0, 0, *self.__query['prefetch_related']
            )
            cursor.execute(
                f"""SELECT {', '.join(fields)} FROM {self.__model.table_name} AS {
                self.__model.table_name}00{''.join(
                    f" {j['type']} JOIN {j['table']} AS {j['alias']} ON {j['on']}"
                    for j in joins
                )}"""
            )
            rows = cursor.fetchall()
            # Separating M2M data for each row fetched
            for pfield in self.__query['prefetch_related']:
                fnames = pfield.split('__')  # Splitting to get subfields sequence
                current_model = self.__model
                for fname in fnames:  # Getting proper nested model
                    try:
                        attr = getattr(current_model, fname)
                        current_model = attr.ref
                    except AttributeError:
                        break
                prefetched[pfield] = tuple(
                    mdl.ModelInstance(
                        current_model,
                        **{
                            key.replace(f'{pfield}__', ''): row.pop(key)
                            for key in tuple(row.keys())
                            if re.search(f'^{pfield}__([a-z]+_?)+$', key)
                        }
                    ) for row in rows
                )
        return prefetched

    def prefetch_related(self, *args):  # SELECT with ManyToMany fields
        self.__validate_related('prefetch', (fld.ForeignKey, fld.ManyToManyField), *args)
        self.__query['prefetch_related'].extend(args)
        return self

    def update(self, **kwargs) -> None:  # Updating all the QuerySet members according to the kwargs given
        self.__model.check_table()
        update_set = []
        for name, val in kwargs.items():
            if name not in self.__model.fields:  # Checking if all fields specified right
                raise Exception('Wrong fields specified in update method')
            else:
                update_set.append(
                    f'{self.__model.table_name}.{name} = '
                    f'{self.__model.fields[name].to_sql(val)}'
                )
                for mi in self.__container:
                    setattr(mi, name, val)
        try:  # UPDATE command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        f"""UPDATE {self.__model.table_name}, ({
                        qr.assemble_query(self.__model, self.__query)
                        }) AS __tab SET {', '.join(
                            update_set
                        )} WHERE {self.__model.table_name}.id = __tab.id"""
                    )
                    connection.commit()
        except Error as err:
            print(err)

    def delete(self) -> None:  # Deleting all the QuerySet members
        self.__model.check_table()
        try:  # DELETE command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        f"""DELETE FROM {self.__model.table_name} WHERE {
                        self.__model.table_name}.id IN (SELECT {
                        self.__model.table_name}00.id FROM ({
                        qr.assemble_query(
                            model=self.__model,
                            query=self.__query
                        )}) AS {self.__model.table_name}00)"""
                    )
                    connection.commit()
        except Error as err:
            print(err)

    def exists(self):  # Checking will the QuerySet be empty or not
        if not self.__executed:
            self.__model.check_table()
            try:  # SELECT EXISTS command
                with connect(**db_data) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"""SELECT EXISTS({
                            qr.assemble_query(
                                model=self.__model,
                                query=self.__query
                            )})"""
                        )
                        results = cursor.fetchall()
                        return bool(results[0][0])
            except Error as err:
                print(err)
        else:
            return bool(self.__container)

    def execute(self):  # Direct execution demand (mainly used by QuerySetSlice)
        if not self.__executed:
            self.__exec()

    def __bool__(self):
        return self.exists()

    def __add__(self, other):  # Queries concatenation aka UNION
        if self.__model.__name__ != other.__model.__name__:
            raise TypeError('QuerySet models must be the same to perform UNION operation')
        else:
            self.__executed = False
            self.__container = ()
            self.__union.append(other.__query)
            return self

    def __radd__(self, other):
        return self.__add__(other)

    def __or__(self, other):
        if self.__model.__name__ != other.__model.__name__:
            raise TypeError('QuerySet models must be the same to perform OR operation')
        else:
            self.__executed = False
            self.__container = ()
            return QuerySet(
                self.__model,
                qr.Q.Or(
                    qr.Q.And(*(self.__query['args'] + tuple(
                        qr.Q(**{name: value})
                        for name, value in self.__query['kwargs'].items()
                    ))),
                    qr.Q.And(*(other.__query['args'] + tuple(
                        qr.Q(**{name: value})
                        for name, value in other.__query['kwargs'].items()
                    )))
                )
            )

    def __ror__(self, other):
        return self.__or__(other)

    def __and__(self, other):
        if self.__model.__name__ != other.__model.__name__:
            raise TypeError('QuerySet models must be the same to perform AND operation')
        else:
            self.__executed = False
            self.__container = ()
            self.__query['kwargs'].update(other.__query['kwargs'])
            return QuerySet(
                self.__model,
                *(self.__query['args'] + other.__query['args']),
                **self.__query['kwargs']
            )

    def __rand__(self, other):
        return self.__and__(other)


# Raw SQL-query wrapper.
# Allows to directly execute SQL requests.
class RawQuerySet:
    def __init__(self, model, query: str):
        self.__model = model
        self.__query = query  # Raw SQL statement
        self.__executed = False  # Execution indicator
        self.__container = []  # Raw data selected storage
        self.__validate_query()  # Validating query given using regular expression

    def __validate_query(self):  # Validating query SQL syntax
        if not re.fullmatch(
            '^SELECT(( ALL| DISTINCT| DISTINCTROW)? [\w*().]+( AS \w+)?,?)+ '
            f'FROM ({self.__model.table_name}( AS \w+)?)'
            '(( LEFT| RIGHT| INNER| CROSS) JOIN \w+( AS \w+)? ON \w+\.\w+ = \w+\.\w+)*'
            '( WHERE(( NOT)? \S+(\.\S+)? = [\S\']+(\.[\S\']+)?( AND| OR)?)+)?'
            '( GROUP BY (\w+(\.\w+)?(, )?)+)?'
            '( HAVING(( NOT)? \S+(\.\S+)? = [\S\']+(\.[\S\']+)?( AND| OR)?)+)?'
            '( ORDER BY (\S+(\.\S+)?( ASC| DESC)?(, )?)+)?'
            '( LIMIT \d+)?'
            '( OFFSET \d+)?'
            '( FOR (UPDATE|SHARE))?$',
            self.__query.strip()
        ):
            raise ValueError(
                'Query given does not match the '
                'format required by RawQuerySet.'
            )

    def __exec(self):  # Executing query given
        self.__model.check_table()  # Check if necessary table exists
        try:  # SELECT command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(self.__query)
                    self.__container = cursor.fetchall()  # Saving raw data fetched to container
                    self.__executed = True
        except Error as err:
            print(err)

    # RawQuerySet commands require execution before direct data access
    def __getitem__(self, key: int | slice):
        if not self.__executed:
            self.__exec()
        return self.__container[key]

    def __iter__(self):
        if not self.__executed:
            self.__exec()
        yield from self.__container

    def __len__(self, item):
        if not self.__executed:
            self.__exec()
        return len(self.__container)

    def __bool__(self):
        if not self.__executed:
            self.__exec()
        return bool(self.__container)
