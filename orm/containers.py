from mysql.connector import connect, Error
from settings import db_data
from . import fields as fld, model as mdl, query as qr
from .aggregate import BasicAggregate


class QuerySet:
    class __QuerySetIterator:
        def __init__(self, container):
            self.__container = container
            self.__index = -1

        def __iter__(self):
            return self

        def __next__(self):
            self.__index += 1
            try:
                return self.__container[self.__index]
            except IndexError:
                raise StopIteration

    class __QuerySetSlice:  # QuerySet wrapper object to hide methods that cannot be converted into SQL
        def __init__(self, query_set):
            self.__query_set = query_set

        def __iter__(self):
            return self.__query_set.__iter__()

        def __getitem__(self, key: int | slice):
            if isinstance(key, (int, slice)):
                if not self.__query_set._QuerySet__executed:
                    self.__query_set._QuerySet__exec()
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

        def update(self, **kwargs) -> None:
            self.__query_set.update(**kwargs)

        def delete(self) -> None:
            self.__query_set.delete()

    def __init__(self, model, query):
        self.__model = model
        self.__query = query
        self.__union = []
        self.__executed = False
        self.__container = tuple()

    def __exec(self) -> None:  # Lazy query execution
        self.__model.check_table()
        try:  # SELECT command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        ' UNION '.join(
                            qr.assemble_query(self.__model, q)
                            for q in [self.__query] + self.__union
                        )
                    )
                    results = cursor.fetchall()
                    self.__container = tuple(
                        mdl.ModelInstance(self.__model, **res) for res in results
                    )
                    self.__executed = True
        except Error as err:
            print(err)

    def __iter__(self) -> __QuerySetIterator:
        if not self.__executed:
            self.__exec()
        return QuerySet.__QuerySetIterator(self.__container)

    def __getitem__(self, key: int | slice):
        if isinstance(key, int):
            if key < 0:
                if not self.__executed:
                    self.__exec()
                return self.__container[key]
            else:
                self.__query['offset'] = key
                self.__query['limit'] = 1
                self.__exec()
                return self.__container[0]
        elif isinstance(key, slice):
            if not self.__executed:
                if all((key.start, key.stop, key.start >= 0, key.stop > key.start)):
                    match key.start, key.stop:
                        case start, None:
                            self.__query['offset'] = start
                        case None, stop:
                            self.__query['limit'] = stop
                        case start, stop:
                            self.__query['offset'] = start
                            self.__query['limit'] = stop - start
                    return QuerySet.__QuerySetSlice(self)
                else:
                    self.__exec()
                    return self.__container[key]
            else:
                return self.__container[key]
        else:
            raise TypeError(
                'QuerySet __getitem__() method supports '
                'only int and slice argument types.'
            )

    def __contains__(self, item):
        if not issubclass(type(item), mdl.ModelInstance):
            raise TypeError('QuerySet object can only store model instances.')
        elif item.model.__name__ != self.__model.__name__:
            raise TypeError(
                f'''Wrong model for this QuerySet: expected "{
                self.__model.__name__}" but got "{item.model.__name__}".'''
            )
        else:
            if not self.__executed:
                self.__model.check_table()
                try:  # SELECT EXISTS command with INNER JOIN
                    with connect(**db_data) as connection:
                        with connection.cursor() as cursor:
                            assembled = qr.assemble_query(
                                self.__model, self.__query
                            )
                            index = assembled.find('WHERE') + 6
                            cursor.execute(
                                f"""SELECT EXISTS(SELECT * FROM {
                                self.__model.table_name} AS {self.__model.table_name
                                }0 INNER JOIN {self.__model.table_name} AS intersect ON {
                                self.__model.table_name}0.id = intersect.id{
                                assembled[:index] +
                                f'intersect.id = {item.id} AND ' +
                                assembled[index:]
                                if index != 5 else f' WHERE intersect.id = {item.id}'
                                })"""
                            )
                            cursor.execute(
                                f"""SELECT EXISTS(SELECT * FROM {
                                self.__model.table_name} AS {self.__model.table_name
                                }0 INNER JOIN {self.__model.table_name} AS intersect ON {
                                self.__model.table_name}0.id = intersect.id{
                                assembled[:index] +
                                f'intersect.id = {item.id} AND ' +
                                assembled[index:]
                                if index != 5 else f' WHERE intersect.id = {item.id}'
                                })"""
                            )
                            results = cursor.fetchall()
                            return bool(results[0][0])
                except Error as err:
                    print(err)
            else:
                return item in self.__container

    def __str__(self) -> str:
        if not self.__executed:
            return '<QuerySet(unexecuted)>'
        else:
            return f'<QuerySet{self.__container}>'

    def __len__(self):
        if not self.__executed:
            self.__model.check_table()
            try:  # SELECT COUNT command
                with connect(**db_data) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            qr.assemble_query(
                                model=self.__model,
                                query=self.__query,
                                fields=('COUNT(*)',),
                                validate_fields=False
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
        self.__query['args'] += args
        self.__query['kwargs'].update(kwargs)
        return self

    def get(self, *args, **kwargs): # SELECT WHERE ... LIMIT 1
        try:  # Returns model instance matching query...
            return self.filter(*args, **kwargs)[0]
        except IndexError:  # ... or None if nothing was found
            return None

    def exclude(self, *args, **kwargs):  # SELECT WHERE NOT ...
        self.__executed = False
        self.__query['args'] += (~qr.Q.And(
            *(args + tuple(qr.Q(**{name: val}) for name, val in kwargs.items()))
        ),)
        return self

    def aggregate(self, *args):  # SELECT Aggr(...), ... command
        if len(args) > 0:  # Check if is enough arguments
            if not all(map(lambda arg: issubclass(type(arg), BasicAggregate), args)):
                raise TypeError(  # Check if arguments specified are Aggregate wrappers
                    'Expected BasicAggregate subclass '
                    'instances as args values.'
                )
            self.__executed = False
            try:  # Check if key is specified
                self.__query['aggregate_fields'] += args
            except KeyError:
                self.__query['aggregate_fields'] = args
        else:
            raise ValueError(
                'At least one argument required'
            )
        return self

    def update(self, **kwargs) -> None:  # Updating all the QuerySet members according to the kwargs given
        self.__model.check_table()
        update_set = []
        for name, val in kwargs.items():
            if not name in self.__model.fields:  # Checking if all fields specified right
                raise Exception('Wrong fields specified in update method')
            else:
                update_set.append(f'{self.__model.table_name}.{name} = {self.__model.fields[name].to_sql(val)}')
                for mi in self.__container:
                    setattr(mi, name, val)
        try:  # UPDATE command
            with connect(**db_data) as connection:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        f"""UPDATE {self.__model.table_name}, ({
                        qr.assemble_query(self.__model, self.__query, ('id',))
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
                        self.__model.table_name}0.id FROM ({
                        qr.assemble_query(
                            model=self.__model,
                            query=self.__query,
                            fields=('*',),
                            validate_fields=False
                        )})"""
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
                                query=self.__query,
                                fields=('*',),
                                validate_fields=False
                            )})"""
                        )
                        results = cursor.fetchall()
                        return bool(results[0][0])
            except Error as err:
                print(err)
        else:
            return bool(self.__container)

    def order_by(self, *args):  # *args format: '(-)<field>__<subfield>__...__<subfield>'
        for arg in args:
            if isinstance(arg, str):
                subfs = arg.split('__')
                if subfs[0].replace('-', '') in self.__model.fields:
                    pass
                else:  # Check if model has field listed
                    raise AttributeError(
                        f'Model {self.__model.__name__} does'
                        f' not have field named {subfs[0]}'
                    )
            else:  # Check if all order_by parameters are strings
                raise TypeError(
                    f'Wrong argument type for order_by method:'
                    f' expected str but got {type(arg).__name__}'
                )
        self.__query['order_by'] = args
        return self

    def __bool__(self):
        return self.exists()

    def __add__(self, other):  # Queries concatenation aka UNION
        if self.__model.__name__ != other.__model.__name__:
            raise TypeError('QuerySet models must be the same to perform UNION operation')
        else:
            self.__executed = False
            self.__union.append(other.__query)
            return self

    def __radd__(self, other):
        return self.__add__(other)

    def __or__(self, other):
        if self.__model.__name__ != other.__model.__name__:
            raise TypeError('QuerySet models must be the same to perform OR operation')
        else:
            self.__executed = False
            return QuerySet(self.__model, {
                'args': qr.Q.Or(
                    qr.Q.And(*(self.__query['args'] + tuple(
                        qr.Q(**{name: value})
                        for name, value in self.__query['kwargs'].items()
                    ))),
                    qr.Q.And(*(other.__query['args'] + tuple(
                        qr.Q(**{name: value})
                        for name, value in other.__query['kwargs'].items()
                    )))
                ),
                'kwargs': {}
            })

    def __ror__(self, other):
        return self.__or__(other)

    def __and__(self, other):
        if self.__model.__name__ != other.__model.__name__:
            raise TypeError('QuerySet models must be the same to perform AND operation')
        else:
            self.__executed = False
            self.__query['kwargs'].update(other.__query['kwargs'])
            return QuerySet(self.__model, {
                'args': self.__query['args'] + other.__query['args'],
                'kwargs': self.__query['kwargs']
            })

    def __rand__(self, other):
        return self.__and__(other)
