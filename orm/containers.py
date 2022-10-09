from mysql.connector import connect, Error
from settings import db_data
from . import fields as fld, model as mdl, query as qr


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
                raise TypeError('QuerySetSlice __getitem__() method supports only int and slice argument types')

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
                            f"""SELECT {', '.join(
                                f'{self.__model.table_name}0.{fname}'
                                for fname, fval in
                                self.__model.fields.items()
                                if not isinstance(fval, fld.ManyToManyField)
                            )} FROM {self.__model.table_name} AS {
                            self.__model.table_name}0{qr.assemble_query(
                                self.__model,
                                *q['args'],
                                **q['kwargs']
                            )}{f' LIMIT {self.__query["limit"]}'
                            if self.__query.get("limit", None) else ''}{
                            f' OFFSET {self.__query["offset"]}'
                            if self.__query.get("offset", None) else ''}"""
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
            raise TypeError('QuerySet __getitem__() method supports only int and slice argument types.')

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
                            cursor.execute(
                                f"""SELECT EXISTS(SELECT * FROM {
                                self.__model.table_name} AS {self.__model.table_name
                                }0 INNER JOIN {self.__model.table_name} AS intersect ON {
                                self.__model.table_name}0.id = intersect.id{
                                qr.assemble_query(
                                    self.__model,
                                    *self.__query['args'],
                                    **self.__query['kwargs']
                                )} AND intersect.id = {item.id}{f' LIMIT {self.__query["limit"]}'
                                if self.__query.get("limit", None) else ''}{
                                f' OFFSET {self.__query["offset"]}'
                                if self.__query.get("offset", None) else ''})"""
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
                            f"""SELECT COUNT(*) FROM {
                            self.__model.table_name} AS {self.__model.table_name
                            }0{qr.assemble_query(
                                self.__model,
                                *self.__query['args'],
                                **self.__query['kwargs']
                            )}{f' LIMIT {self.__query["limit"]}'
                            if self.__query.get("limit", None) else ''}{
                            f' OFFSET {self.__query["offset"]}'
                            if self.__query.get("offset", None) else ''}"""
                        )
                        results = cursor.fetchall()
                        return results[0][0]
            except Error as err:
                print(err)
        else:
            return len(self.__container)

    def filter(self, *args, **kwargs):
        self.__executed = False
        self.__query['args'] += args
        self.__query['kwargs'].update(kwargs)
        return self

    def get(self, *args, **kwargs):
        try:  # Returns model instance matching query...
            return self.filter(*args, **kwargs)[0]
        except IndexError:  # ... or None if nothing was found
            return None

    def exclude(self, *args, **kwargs):
        self.__executed = False
        self.__query['args'] += (~qr.Q.And(
            *(args + [qr.Q(**{name: val}) for name, val in kwargs.items()])
        ),)
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
                        f"""UPDATE {self.__model.table_name}, (SELECT {
                        self.__model.table_name}0.id FROM {
                        self.__model.table_name} AS {self.__model.table_name}0{
                        qr.assemble_query(
                            self.__model,
                            *self.__query['args'],
                            **self.__query['kwargs']
                        )}{f' LIMIT {self.__query["limit"]}'
                        if self.__query.get("limit", None) else ''}{
                        f' OFFSET {self.__query["offset"]}'
                        if self.__query.get("offset", None) else ''
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
                        self.__model.table_name}0.id FROM (SELECT * FROM {
                        self.__model.table_name}) AS {self.__model.table_name}0{
                        qr.assemble_query(
                            self.__model,
                            *self.__query['args'],
                            **self.__query['kwargs']
                        )}{f' LIMIT {self.__query["limit"]}'
                        if self.__query.get("limit", None) else ''}{
                        f' OFFSET {self.__query["offset"]}'
                        if self.__query.get("offset", None) else ''})"""
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
                            f"""SELECT EXISTS(SELECT * FROM {
                            self.__model.table_name} AS {self.__model.table_name
                            }0{qr.assemble_query(
                                self.__model,
                                *self.__query['args'],
                                **self.__query['kwargs']
                            )}{f' LIMIT {self.__query["limit"]}'
                            if self.__query.get("limit", None) else ''}{
                            f' OFFSET {self.__query["offset"]}'
                            if self.__query.get("offset", None) else ''})"""
                        )
                        results = cursor.fetchall()
                        return bool(results[0][0])
            except Error as err:
                print(err)
        else:
            return bool(self.__container)

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
