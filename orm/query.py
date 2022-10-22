from . import fields as fld, aggregate as aggr
from abc import ABC, abstractmethod


class BaseOperation(ABC):  # Database operation interface
    @abstractmethod  # Assembling inner Q, Not, And, Or objects into an SQL string
    def assemble_query(
            self,
            model: object,
            primary_join_index: int,
            annotate_join_index: int
    ) -> tuple[list, str, int]:
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


ops = {  # SQL operations used in WHERE statement
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

        def assemble_query(
                self,
                model: object,
                primary_join_index: int,
                annotate_join_index: int
        ) -> tuple[list, str, int]:
            joins, constraints = [], {'where': [], 'having': []}
            for q in self.subset:
                ajoins, aconstraints, primary_join_index  = q.assemble_query(
                    model, primary_join_index, annotate_join_index
                )
                joins.extend(ajoins)
                constraints['where'].append(f"({aconstraints['where']})")
                constraints['having'].append(f"({aconstraints['having']})")
            return (
                joins,
                {
                    'where': ' AND '.join(constraints['where']),
                    'having': ' AND '.join(constraints['having']),
                },
                primary_join_index
            )

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

        def assemble_query(
                self,
                model: object,
                primary_join_index: int,
                annotate_join_index: int
        ) -> tuple[list, str, int]:
            joins, constraints = [], {'where': [], 'having': []}
            for q in self.subset:
                ajoins, aconstraints = q.assemble_query(
                    model, primary_join_index, annotate_join_index
                )
                joins.extend(ajoins)
                constraints['where'].append(f"({aconstraints['where']})")
                constraints['having'].append(f"({aconstraints['having']})")
            return (
                joins,
                {
                    'where': ' OR '.join(constraints['where']),
                    'having': ' OR '.join(constraints['having']),
                },
                primary_join_index
            )

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

        def assemble_query(
                self,
                model: object,
                primary_join_index: int,
                annotate_join_index: int
        ) -> tuple[list, str, int]:
            joins, constraints, primary_join_index = Q.make_query(
                model, primary_join_index, annotate_join_index  **self.query
            )
            return (
                joins,
                {
                    'where': f"NOT ({constraints['where']})",
                    'having': f"NOT ({constraints['having']})",
                },
                primary_join_index
            )

    @staticmethod  # Create SQL-query string from keyword one
    def make_query(
            model,
            primary_join_index: int,
            annotate_join_index: int,
            **kwargs
    ):
        joins, constraints = [], {'where': [], 'having': []}
        for query, value in kwargs.items():
            parts = query.split('__')
            opname = parts[-1] if parts[-1] in ops else ''
            fnames = parts[:-1] if opname else parts
            joins_extend, current_model, primary_join_index = Q.make_joins(
                model, fnames, primary_join_index, annotate_join_index
            )
            joins.extend(joins_extend)  # Extending joins for nested fields
            if fnames[-1] in current_model.fields:
                # Reformatting value given into an SQL-friendly
                # one if model field name is given
                constraints['where'].append(ops[opname](
                    f'''{joins[-1]["alias"] + '.' if joins
                    else f"{model.table_name}00."
                    if fnames[-1] in model.fields else ''}{fnames[-1]}''',
                    current_model.fields.get(fnames[-1]).to_sql(value)
                ))
            else:
                # Last subfield specified in query was either not
                # in operations list or not in model fields list
                constraints['having'].append(ops[opname](
                    f'''{joins[-1]["alias"] + '.' if joins
                    else f"{model.table_name}00."
                    if fnames[-1] in model.fields else ''}{fnames[-1]}''',
                    value
                ))
        return (
            joins,
            {
                'where': ' AND '.join(constraints['where']),
                'having': ' AND '.join(constraints['having'])
            },
            primary_join_index
        )

    @staticmethod  # Create list of fields for ORDER BY command
    def make_order_by(
            model,
            primary_join_index: int,
            annotate_join_index: int,
            *args
    ) -> tuple[list, list, int]:
        joins, fields = [], []
        for query in args:
            fnames = query.replace('-', '').split('__')  # "-" -> DESC / "" -> ASC
            ajoins, _, primary_join_index = Q.make_joins(
                model, fnames, primary_join_index, annotate_join_index
            )
            joins.extend(ajoins)  # Extending joins for nested fields
            fields.append(
                f'''{joins[-1]["alias"] + '.' if ajoins
                else f"{model.table_name}00." 
                if fnames[-1] in model.fields else ''}{fnames[-1]} {
                "DESC" if query[0] == "-" else "ASC"}'''
            )
        return joins, fields, primary_join_index

    @staticmethod  # Assembling query with aggregate functions in fields list
    def make_aggregate(
            model: object,
            primary_join_index: int,
            annotate_join_index: int,
            *args: tuple[aggr.BaseAggregate | aggr.AggregateOperationWrapper],
            **kwargs: dict[str, aggr.BaseAggregate | aggr.AggregateOperationWrapper]
    ) -> tuple[list, list, list, int, int]:
        joins, fields, aliases = [], [], []
        for aggregate in args:
            annotate_join_index += 1
            ajoins, afields, aalias, primary_join_index = aggregate(
                model, primary_join_index, annotate_join_index
            )  # BasicAggregate or AggregateOperationWrapper class __call__() method
            joins.extend(ajoins)
            fields.append(afields)
            aliases.append(aalias)  # Using automatically generated aliases
        for alias, aggregate in kwargs.items():
            annotate_join_index += 1
            ajoins, afields, aalias, primary_join_index = aggregate(
                model, primary_join_index, annotate_join_index
            )  # BasicAggregate or AggregateOperationWrapper class __call__() method
            joins.extend(ajoins)
            fields.append(afields)
            aliases.append(alias)  # Using aliases given
        return joins, fields, aliases, primary_join_index, annotate_join_index

    @staticmethod
    def make_related_fields(
            model: object,
            primary_join_index: int,
            annotate_join_index: int,
            *args: tuple[str],
    ) -> tuple[list, list, int]:
        joins, fields = [], []
        for field in args:
            fnames = field.split('__')
            ajoins, current_model, primary_join_index = Q.make_joins(
                model, fnames, primary_join_index, annotate_join_index
            )
            joins.extend(ajoins)  # Extending joins for nested fields
            fields.append(', '.join(
                f'{joins[-1]["alias"]}.{fname} AS {field}__{fname}'
                for fname, fval in
                current_model.fields.items()
                if not isinstance(fval, fld.ManyToManyField)
            ))
        return joins, fields, primary_join_index

    @staticmethod  # Table joins for nested fields
    def make_joins(
            model: object,
            fnames: list[str],
            primary_join_index: int,
            annotate_join_index: int
    ) -> tuple[tuple, object, int]:
        current_model, joins = model, ()
        for field in fnames:  # Using joins to specify subfield constraints
            try:  # Adding joins for ForeignKey and ManyToManyField
                attr = getattr(current_model, field)
                joins += (attr.get_joins(
                    f'{current_model.table_name}'
                    f'{primary_join_index - 1 if current_model != model else 0}'
                    f'{annotate_join_index}',
                    field,
                    primary_join_index,
                    annotate_join_index
                ))
                current_model = attr.ref
                primary_join_index += 1
            except AttributeError:
                break
        return joins, current_model, primary_join_index

    @staticmethod  # Eliminates joins duplication (DEPRECATED)
    def make_joins_unique(joins: iter) -> iter:
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

    def assemble_query(
            self,
            model: object,
            primary_join_index: int,
            annotate_join_index: int
    ) -> tuple[list, str, int]:
        return Q.make_query(
            model, primary_join_index, annotate_join_index, **self.query
        )


def assemble_query(  # Making SQL query-string for given model with given parameters
        model,  # Allows to gain access to model resources
        query: dict,  # Dictionary storing query parameters
        aggregate_fields: dict[str, tuple | dict]=None,  # Aggregate fields list to select (optional)
) -> str:
    # Initialising storages for JOIN, WHERE and ORDER BY
    joins, constraints, order_by = [], {'where': [], 'having': []}, ''
    primary_join_index, annotate_join_index = 1, 0
    # Assembling WHERE query
    for arg in query['args']:  # Q-class queries (Q, Q.Not, Q.Or, Q.And)
        ajoins, aconstraints, primary_join_index = arg.assemble_query(
            model, primary_join_index, annotate_join_index
        )
        joins.extend(ajoins)
        constraints['where'].append(f"({aconstraints['where']})")
        constraints['having'].append(f"({aconstraints['having']})")
    if query['kwargs']:  # Keyword queries (<field>__<subfield>__...(__<op>)=<value>)
        ajoins, aconstraints, primary_join_index = Q.make_query(
            model, primary_join_index, annotate_join_index, **query['kwargs']
        )
        joins.extend(ajoins)
        constraints['where'].append(f"({aconstraints['where']})")
        constraints['having'].append(f"({aconstraints['having']})")
    # Assembling field list to select from database
    # Related models fields
    related_flist = ''  # Doing variable assign not to get error if no related fields were specified
    if query['select_related']:  # Appending related fields
        ajoins, afields, primary_join_index = Q.make_related_fields(
            model, primary_join_index,
            annotate_join_index, *query['select_related']
        )
        joins.extend(ajoins)
        related_flist = ', '.join(afields)
    # Annotated fields
    annotated_flist = ''  # Doing variable assign not to get error if no annotate was specified
    if query['annotate']['args'] or query['annotate']['kwargs']:  # Appending annotated fields
        ajoins, afields, aaliases, primary_join_index, annotate_join_index = Q.make_aggregate(
            model, primary_join_index, annotate_join_index,
            *query['annotate']['args'], **query['annotate']['kwargs']
        )
        annotated_flist = ', '.join(  # Making SELECT subquery for each annotated field
            f"""(SELECT {fdef} FROM {model.table_name} AS {model.table_name}0{annotate_join_index}{''.join(
                f" {j['type']} JOIN {j['table']} AS {j['alias']} ON {j['on']}"
                for j in ajoins
            )} WHERE {model.table_name}0{annotate_join_index}.id = {model.table_name}00.id) AS {falias}"""
            for fdef, falias in zip(
                afields, aaliases
            )
        )
    # Primary model fields
    flist = ', '.join(  # Primary model fields
        f'{model.table_name}00.{fname}'
        for fname, fval in
        model.fields.items()
        if not isinstance(fval, fld.ManyToManyField)
    ) + (  # Related fields
        f', {related_flist}' if related_flist else ''
    ) + (  # Annotated fields
        f', {annotated_flist}' if annotated_flist else ''
    )
    if aggregate_fields and (aggregate_fields['args'] or aggregate_fields['kwargs']):  # Fields wrapped in aggregate functions
        ajoins, afields, aaliases, primary_join_index, annotate_join_index = Q.make_aggregate(
            model, 0, -1, *aggregate_fields['args'], **aggregate_fields['kwargs']
        )
        # joins.extend(ajoins)
        aflist = ', '.join(
            f'{fdef} AS {falias}'
            for fdef, falias in zip(
                afields, aaliases
            )
        )
        return f"""SELECT {aflist} FROM (SELECT {
            flist} FROM {model.table_name} AS {model.table_name}00{''.join(
                f" {j['type']} JOIN {j['table']} AS {j['alias']} ON {j['on']}"
                for j in joins
            ) + (
                ' WHERE ' + ' AND '.join(constraints['where']) if constraints['where'] != ['()'] else ''
            ) + (
                ' HAVING ' + ' AND '.join(constraints['having']) if constraints['having'] != ['()'] else ''
            ) + order_by + (
                f' LIMIT {query["limit"]}' if query.get('limit', None) else ''
            ) + (
                f' OFFSET {query["offset"]}' if query.get('offset', None) else ''
        )}) AS {model.table_name}00{''.join(
            f" {j['type']} JOIN {j['table']} AS {j['alias']} ON {j['on']}"
            for j in ajoins
        )}"""
    # Assembling ORDER BY query
    if query.get('order_by', None):  # Specifying ORDER BY fields if listed
        ajoins, afields, primary_join_index = Q.make_order_by(
            model, primary_join_index, annotate_join_index, *query['order_by']
        )
        joins.extend(ajoins)
        order_by = f''' ORDER BY {", ".join(afields)}'''
    # Assembling SQL query
    return f"""SELECT {flist} FROM {model.table_name} AS {
    model.table_name}00{''.join(
        f" {j['type']} JOIN {j['table']} AS {j['alias']} ON {j['on']}"
        for j in joins
    ) + (
        ' WHERE ' + ' AND '.join(constraints['where']) if constraints['where'] != ['()'] else ''
    ) + (
        ' HAVING ' + ' AND '.join(constraints['having']) if constraints['having'] != ['()'] else ''
    ) + order_by + (
        f' LIMIT {query["limit"]}' if query.get('limit', None) else ''
    ) + (
        f' OFFSET {query["offset"]}' if query.get('offset', None) else ''
    )}"""
