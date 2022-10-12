import inspect
import re
from . import model as mod, fields as fld
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
            assembled = {'joins': [], 'constraints': ''}
            for q in self.subset:
                ass = q.assemble_query(model)
                assembled['joins'].extend(ass['joins'])
                assembled['constraints'] += f"({ass['constraints']}) OR "
            assembled['constraints'] = assembled['constraints'][:-4]
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

    join_index = 0

    @staticmethod
    def make_query(
            model,                # Allows to gain access to model resources
            **kwargs              # Regular keyword queries
    ):
        fields = model.fields  # Getting model template aka fields list
        constraints, joins = [], []  # Initializing query concatenate list
        ops = (  # Query operations available
            'gt', 'gte', 'lt', 'lte', 'startswith', 'istartswith', 'endswith',
            'iendswith', 'contains', 'icontains', 'range', 'year', 'month',
            'day', 'hour', 'minute', 'second', 'isnull', 'regex', 'in'
        )
        lops = {'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<='}  # Logical operations and their aliases
        for name in kwargs.keys():  # Query format "<field>__<subfield_1>__...__<subfield_n>(__<operation>)=<value>"
            subq = name.split('__')
            if not subq[0] in fields:  # Checking if all fields specified right
                raise Exception(f'Wrong field "{subq[0]}" specified in query')
            if len(subq) == 1:  # <name> = <value> simple constraint
                constraints.append(
                    f'''{model.table_name}0.{name} {
                    "LIKE" if hasattr(model, name) and isinstance(
                        getattr(model, name), fld.JSONField
                    ) else "="} {fields[name].to_sql(kwargs[name])}'''
                )
                continue
            else:
                opname = subq[-1] if subq[-1] in ops else None
                fnames = subq[:-1] if opname else subq
                # Getting table names nested structure
                tabs_n_als = [(
                    model.table_name,
                    f'{model.table_name}0'
                )]
                m = model
                for fn in fnames:  # Using joins to specify subfields constraints
                    if hasattr(m, fn):
                        attr = getattr(m, fn)
                        if isinstance(attr, fld.ForeignKey):
                            m = getattr(m, fn).ref
                            Q.join_index += 1
                            tabs_n_als.append((
                                m.table_name,
                                f'{m.table_name}{Q.join_index}'
                            ))
                            joins.append(
                                f""" LEFT JOIN {tabs_n_als[-1][0]} AS {
                                tabs_n_als[-1][1]} ON {
                                tabs_n_als[-2][1]}.{fn} = {
                                tabs_n_als[-1][1]}.id"""
                            )
                        elif isinstance(attr, fld.ManyToManyField):
                            m = getattr(m, fn).m2
                            Q.join_index += 1
                            tabs_n_als.append((
                                m.table_name,
                                f'{m.table_name}{Q.join_index}'
                            ))
                            joins.extend((
                                f""" RIGHT JOIN {tabs_n_als[-2][0][:-1]
                                }_{tabs_n_als[-1][0][:-1]
                                } AS joint_table{Q.join_index + 1
                                } ON {tabs_n_als[-2][1]}.id = joint_table{
                                Q.join_index + 1}.{tabs_n_als[-2][0][:-1].lower()}_id""",
                                f""" LEFT JOIN {tabs_n_als[-1][0]} AS {
                                tabs_n_als[-1][1]} ON joint_table{
                                Q.join_index + 1}.{tabs_n_als[-1][0][:-1].lower()}_id = {
                                tabs_n_als[-1][1]}.id"""
                            ))
                            Q.join_index += 1
                        else:
                            break
                # Transforming value specified into SQL form if not field equals id
                fval, fname = kwargs[name], fnames[-1]
                if hasattr(m, fname):  # Check if model has the field given
                    fval = getattr(m, fname).to_sql(fval)
                elif fname == 'id':  # Exception is id which is reserved by default
                    pass
                else:  # If no such field then it can be either field or operation listed incorrectly
                    raise AttributeError(f'Wrong operation or model field name specified: "{fname}"')
                full_fname = f'{tabs_n_als[-1][1]}.{fname}'
                # Appending constraint
                match opname:
                    case None:  # Exact match aka = (LIKE for JSON)
                        constraints.append(
                            f'''{full_fname} {
                            "LIKE" if hasattr(m, fname) and isinstance(
                                getattr(m, fname), fld.JSONField
                            ) else "="} {fval}'''
                        )
                    case 'gt' | 'gte' | 'lt' | 'lte':  # Logical statement aka > \ >= \ < \ <=
                        constraints.append(f'{full_fname} {lops[opname]} {fval}')
                    case 'startswith':  # String starts with substring
                        constraints.append(f"""{full_fname} LIKE BINARY '{fval.replace("'", "")}%'""")
                    case 'istartswith':  # String starts with substring case-insensitive
                        constraints.append(f"""LOWER({full_fname}) LIKE '{fval.replace("'", "").lower()}%'""")
                    case 'endswith':  # String ends with substring
                        constraints.append(f"""{full_fname} LIKE BINARY '%{fval.replace("'", "")}'""")
                    case 'iendswith':  # String ends with substring case-insensitive
                        constraints.append(f"""LOWER({full_fname}) LIKE '%{fval.replace("'", "").lower()}'""")
                    case 'contains':  # String contains substring
                        constraints.append(f"""{full_fname} LIKE BINARY '%{fval.replace("'", "")}%'""")
                    case 'icontains':  # String contains substring case-insensitive
                        constraints.append(f"""LOWER({full_fname}) LIKE '%{fval.replace("'", "").lower()}%'""")
                    case 'range':  # Value lies in range from <a> to <b> aka BETWEEN
                        constraints.append(f"{full_fname} BETWEEN {fval[0]} AND {fval[1]}")
                    case 'year' | 'month' | 'day' | 'hour' | 'minute' | 'second':  # Comparing date\time\datetime parts
                        constraints.append(f"{opname}({full_fname}) = {fval}")
                    case 'isnull':  # Value is SQL NULL
                        constraints.append(f"{opname}({full_fname}) IS {'NOT' if not fval else ''} NULL")
                    case 'regex':  # Regular expression string comparison aka LIKE with regex
                        constraints.append(f"{full_fname} LIKE {fval}")
                    case 'in':  # Value belongs to tuple of values
                        constraints.append(f"{full_fname} IN {fval}")
        return {  # Assembling query
            'joins': joins,
            'constraints': ' AND '.join(constraints)
        }

    @staticmethod
    def make_order_by(
        model,  # Allows to gain access to model resources
        *args   # Field names and direction for ORDER BY command
    ):
        pass

    def __init__(self, **kwargs):
        if len(kwargs) > 1:
            raise Exception('Q class constructor argument must be a single kwarg')
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
        model,       # Allows to gain access to model resources
        query: dict  # Dictionary storing query parameters
):
    assembled = {'joins': [], 'constraints': [], 'orders': []}
    for arg in query['args']:
        ass = arg.assemble_query(model)
        assembled['joins'].extend(ass['joins'])
        assembled['constraints'].append(f"({ass['constraints']})")
    if query['kwargs']:
        ass = Q.make_query(model, **query['kwargs'])
        assembled['joins'].extend(ass['joins'])
        assembled['constraints'].append(f"({ass['constraints']})")
    Q.join_index = 0
    result =  ''.join(
        assembled['joins']
    ) + ' WHERE id > 0' + ' AND '.join(
        assembled['constraints']
    ) + (
        f'ORDER BY {1}' if query.get('order_by', None) else ''
    ) + (
        f' LIMIT {query["limit"]}' if query.get('limit', None) else ''
    ) + (
        f' OFFSET {query["offset"]}' if query.get('offset', None) else ''
    )
