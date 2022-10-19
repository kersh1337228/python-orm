from . import fields as fld, aggregate as aggr
from abc import ABC, abstractmethod


class BaseOperation(ABC):  # Database operation interface
    @abstractmethod  # Assembling inner Q, Not, And, Or objects into SQL string
    def assemble_query(self, model):
        pass

    @abstractmethod  # Logical OR operation
    def __or__(self, other):
        pass

    @abstractmethod
    def __ror__(self, other):
        pass

    @abstractmethod  # Logical AND operation
    def __and__(self, other):
        pass

    @abstractmethod
    def __rand__(self, other):
        pass

    @abstractmethod  # Logical NOT operation
    def __invert__(self):
        pass


ops = {  # SQL operations used in WHERE constraints
    '': lambda name, val: f'''{name} = {val}''',
    'gt': lambda name, val: f'''{name} > {val}''',
    'gte': lambda name, val: f'''{name} >= {val}''',
    'lt': lambda name, val: f'''{name} < {val}''',
    'lte': lambda name, val: f'''{name} <= {val}''',
    'startswith': lambda name, val: f"""{name} LIKE BINARY '{val.replace("'", "")}%'""",
    'istartswith': lambda name, val: f"""LOWER({name}) LIKE '{val.replace("'", "").lower()}%'""",
    'endswith': lambda name, val: f"""{name} LIKE BINARY '%{val.replace("'", "")}'""",
    'iendswith': lambda name, val: f"""LOWER({name}) LIKE '%{val.replace("'", "").lower()}'""",
    'contains': lambda name, val: f"""{name} LIKE BINARY '%{val.replace("'", "")}%'""",
    'icontains': lambda name, val: f"""LOWER({name}) LIKE '%{val.replace("'", "").lower()}%'""",
    'range': lambda name, val: f"{name} BETWEEN {val[0]} AND {val[1]}",
    'year': lambda name, val: f"year({name}) = {val}",
    'month': lambda name, val: f"month({name}) = {val}",
    'day': lambda name, val: f"day({name}) = {val}",
    'hour': lambda name, val: f"hour({name}) = {val}",
    'minute': lambda name, val: f"minute({name}) = {val}",
    'second': lambda name, val: f"second({name}) = {val}",
    'isnull': lambda name, val: f"{name} IS {'NOT' if not val else ''} NULL",
    'regex': lambda name, val: f"{name} LIKE {val}",
    'in': lambda name, val: f"{name} IN {val}"
}


class Q(BaseOperation):  # Query class to add more complex constraints like AND, OR, NOT
    # Query logical operations wrappers
    class And(BaseOperation):  # Logical AND wrapper
        def __init__(self, *args):
            self.subset = args

        def __or__(self, other):
            return Q.Or(self, other)

        def __ror__(self, other):
            return Q.Or(other, self)

        def __and__(self, other):
            self.subset.extend(other.subset)
            return self

        def __rand__(self, other):
            other.subset.extend(self.subset)
            return other

        def __invert__(self):
            return Q.Or([~q for q in self.subset])

        def assemble_query(self, model):
            assembled = {'joins': [], 'constraints': []}
            for q in self.subset:
                ass = q.assemble_query(model)
                assembled['joins'].extend(ass['joins'])
                assembled['constraints'].append(f"({ass['constraints']})")
            assembled['constraints'] = ' AND '.join(assembled['constraints'])
            return assembled

    class Or(BaseOperation):  # Logical OR wrapper
        def __init__(self, *args):
            self.subset = args

        def __or__(self, other):
            self.subset.extend(other.subset)
            return self

        def __ror__(self, other):
            other.subset.extend(self.subset)
            return other

        def __and__(self, other):
            return Q.And(self, other)

        def __rand__(self, other):
            return Q.And(other, self)

        def __invert__(self):
            return Q.And([~q for q in self.subset])

        def assemble_query(self, model):
            assembled = {'joins': [], 'constraints': []}
            for q in self.subset:
                ass = q.assemble_query(model)
                assembled['joins'].extend(ass['joins'])
                assembled['constraints'].append(f"({ass['constraints']})")
            assembled['constraints'] = ' OR '.join(assembled['constraints'])
            return assembled

    class Not(BaseOperation):  # Logical NOT wrapper
        def __init__(self, q):
            self.query = q

        def __or__(self, other):
            return Q.Or(self, other)

        def __ror__(self, other):
            return Q.Or(other, self)

        def __and__(self, other):
            return Q.And(self, other)

        def __rand__(self, other):
            return Q.And(other, self)

        def __invert__(self):
            return Q(**self.query)

        def assemble_query(self, model):
            assembled = Q.make_query(model, **self.query)
            assembled['constraints'] = 'NOT (' + assembled['constraints'] + ')'
            return assembled

    join_index = 1

    @staticmethod
    def make_query(model, **kwargs):  # Create SQL-query string from keyword one
        joins, constraints = [], []
        for query, value in kwargs.items():
            parts = query.split('__')
            opname = parts[-1] if parts[-1] in ops else ''
            fnames = parts[:-1] if opname else parts
            # Getting table names nested structure
            current_model = model
            for field in fnames:  # Using joins to specify subfield constraints
                if not field in current_model.fields:
                    raise AttributeError(
                        f'Wrong field "{field}" specified in '
                        f'query for model {current_model.__name__}'
                    )
                else:
                    try:  # Adding joins for ForeignKey and ManyToManyField
                        attr = getattr(current_model, field)
                        joins.extend(attr.get_joins(
                            f'{current_model.table_name}'
                            f'{Q.join_index - 1 if current_model != model else 0}',
                            field, Q.join_index
                        ))
                        current_model = attr.ref
                        Q.join_index += 1
                    except AttributeError:
                        break
            if fnames[-1] in current_model.fields:  # Reformatting value given into SQL-friendly one
                fval = current_model.fields.get(fnames[-1]).to_sql(value)
            else:
                raise AttributeError(
                    f'Wrong operation or model field '
                    f'name specified: "{fnames[-1]}"'
                )
            constraints.append(ops[opname](
                f'''{joins[-1]["alias"] if joins
                else f"{model.table_name}0"}.{fnames[-1]}''', fval
            ))
        return {
            'joins': joins,
            'constraints': ' AND '.join(constraints)
        }

    @staticmethod
    def make_order_by(model, *args):  # Create list of fields for ORDER BY command
        joins, fields = [], []
        for query in args:
            fnames = query.replace('-', '').split('__')  # "-" -> DESC / "" -> ASC
            # Getting table names nested structure
            current_model = model
            for field in fnames:  # Using joins to specify subfields
                if not field in current_model.fields:
                    raise AttributeError(
                        f'Wrong field "{field}" specified in '
                        f'query for model {current_model.__name__}'
                    )
                else:
                    try:  # Adding joins for ForeignKey and ManyToManyField
                        attr = getattr(current_model, field)
                        joins.extend(attr.get_joins(
                            f'{current_model.table_name}'
                            f'{Q.join_index - 1 if current_model != model else 0}',
                            field, Q.join_index
                        ))
                        current_model = attr.ref
                        Q.join_index += 1
                    except AttributeError:
                        break
            fields.append(
                f'''{joins[-1]["alias"] if joins
                else f"{model.table_name}0"}.{fnames[-1]}'''
            )
        return {
            'joins': joins,
            'fields':fields
        }

    @staticmethod
    def make_aggregate(model, *args: tuple[aggr.BasicAggregate]):
        joins, fields = [], []
        for arg in args:
            assembled = arg.assemble(model)
            joins.extend(assembled['joins'])
            fields.append(assembled['fields'])
        return {
            'joins': Q.make_joins_unique(joins),
            'fields': fields
        }

    @staticmethod
    def make_joins_unique(joins: iter):
        joins_unique, uniques, repeat = [], [], False
        for join in joins:  # Making sure joins do not duplicate
            if not join['field'] in uniques:
                if repeat:  # Correcting on alias in first unique join after repeats
                    alias = join['on'].split('.')[0]
                    join['on'] = join['on'].replace(alias, next(filter(
                        lambda j: j['table'] == alias[:-1],
                        joins_unique
                    ))['alias'])
                joins_unique.append(join)
                uniques.append(join['field'])
                repeat = False
            else:  # Repeating joins sequence indicator
                repeat = True
        return joins_unique

    def __init__(self, **kwargs):
        if len(kwargs) > 1:
            raise ValueError(
                'Q class constructor argument must be a single kwarg'
            )
        self.query = kwargs

    def __or__(self, other):
        return Q.Or(self, other)

    def __ror__(self, other):
        return Q.Or(other, self)

    def __and__(self, other):
        return Q.And(self, other)

    def __rand__(self, other):
        return Q.And(other, self)

    def __invert__(self):
        return Q.Not(self.query)

    def assemble_query(self, model):
        return Q.make_query(model, **self.query)


def assemble_query(  # Making SQL query-string for given model with given parameters
        model,  # Allows to gain access to model resources
        query: dict,  # Dictionary storing query parameters
        fields: tuple[str]=None,  # Fields list to select (optional)
        validate_fields: bool=True  # Works only if fields argument specified
):
    # Initialising storages for JOIN, WHERE and ORDER BY
    joins, constraints, order_by = [], [], []
    # Assembling WHERE query
    for arg in query['args']:  # Q-class queries (Q, Q.Not, Q.Or, Q.And)
        ass = arg.assemble_query(model)
        joins.extend(ass['joins'])
        constraints.append(f"({ass['constraints']})")
    if query['kwargs']:  # Keyword queries (<field>__<subfield>__...(__<op>)=<value>)
        ass = Q.make_query(model, **query['kwargs'])
        joins.extend(ass['joins'])
        constraints.append(f"({ass['constraints']})")
    # Assembling ORDER BY query
    if query.get('order_by', None):  # Specifying ORDER BY fields if listed
        order_by = Q.make_order_by(model, *query['order_by'])
        joins.extend(order_by['joins'])
    Q.join_index = 0
    # What fields to select
    if query.get('aggregate_fields', None):
        aggregate = Q.make_aggregate(*query.get('aggregate_fields'))
        joins.extend(aggregate['joins'])
        flist = ', '.join(aggregate['fields'])
    elif fields:  # No validation - can result in SQL error!!!
        if validate_fields:  # If true allows only primary model fields
            if not all(map(lambda f: f in model.fields, fields)):
                raise AttributeError(
                    'Only primary model fields could be '
                    'specified in assemble_query() method.'
                )
            flist = ', '.join(f'{model.table_name}0.{f}' for f in fields)
        else:  # No validation - SQL errors could be raised
            flist = ', '.join(fields)
    else:
        flist = ', '.join(
            f'{model.table_name}0.{fname}'
            for fname, fval in
            model.fields.items()
            if not isinstance(fval, fld.ManyToManyField)
        )
    return f"""SELECT {flist} FROM {model.table_name} AS {
    model.table_name}0{''.join(
        f" {j['type']} JOIN {j['table']} AS {j['alias']} ON {j['on']}"
        for j in Q.make_joins_unique(joins)
    ) + (' WHERE ' + ' AND '.join(
        constraints
    ) if constraints else '') + (
        f''' ORDER BY {", ".join(  
           f"{ob} {'ASC' if ob[0] != '-' else 'DESC'}" for ob in order_by['fields']
        )}''' if query.get('order_by', None) else ''
    ) + (
        f' LIMIT {query["limit"]}' if query.get('limit', None) else ''
    ) + (
        f' OFFSET {query["offset"]}' if query.get('offset', None) else ''
    )}"""
