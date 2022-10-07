import inspect
import re
from . import model as mod


class Q:
    # Query logical operations wrappers
    class And:
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
            assembled = {'joins': [], 'constraints': ''}
            for q in self.subset:
                ass = q.assemble_query(model)
                assembled['joins'].extend(ass['joins'])
                assembled['constraints'] += f"({ass['constraints']}) AND "
            assembled['constraints'] = assembled['constraints'][:-5]
            return assembled

    class Or:
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

    class Not:
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
                constraints.append(f'{model.table_name}0.{name} = {fields[name].to_sql(kwargs[name])}')
                continue
            else:
                opname = subq[-1] if subq[-1] in ops else None
                fnames = subq[:-1] if opname else subq
                # Getting table names nested structure
                tabs, aliases = [], []
                m = model
                for fn in fnames:
                    tabs.append(m.table_name)
                    aliases.append(f'{m.table_name}{Q.join_index}')
                    try:
                        m = getattr(m, fn).ref
                        Q.join_index += 1
                    except AttributeError:
                        break
                # Listing joins based on field and table names
                for i in range(len(tabs) - 1):
                    joins.append(
                        f" LEFT JOIN {tabs[i + 1]} AS {aliases[i + 1]} ON {aliases[i] if tabs[i] != model.table_name else model.table_name + '0'}.{fnames[i]} = {aliases[i + 1]}.id"
                    )
                # Appending constraint
                if hasattr(m, fnames[-1]):
                    value = getattr(m, fnames[-1]).to_sql(kwargs[name])
                    match opname:  # Adding constraint based on operation name
                        case None:
                            constraints.append(f'{aliases[-1]}.{fnames[-1]} = {value}')
                        case 'gt' | 'gte' | 'lt' | 'lte':
                            constraints.append(f'{aliases[-1]}.{fnames[-1]} {lops[opname]} {value}')
                        case 'startswith':
                            value = value.replace("'", '')
                            constraints.append(f"{aliases[-1]}.{fnames[-1]} LIKE BINARY '{value}%'")
                        case 'istartswith':
                            value = value.replace("'", '')
                            constraints.append(f"LOWER({aliases[-1]}.{fnames[-1]}) LIKE '{value.lower()}%'")
                        case 'endswith':
                            value = value.replace("'", '')
                            constraints.append(f"{aliases[-1]}.{fnames[-1]} LIKE BINARY '%{value}'")
                        case 'iendswith':
                            value = value.replace("'", '')
                            constraints.append(f"LOWER({aliases[-1]}.{fnames[-1]}) LIKE '%{value.lower()}'")
                        case 'contains':
                            value = value.replace("'", '')
                            constraints.append(f"{aliases[-1]}.{fnames[-1]} LIKE BINARY '%{value}%'")
                        case 'icontains':
                            value = value.replace("'", '')
                            constraints.append(f"LOWER({aliases[-1]}.{fnames[-1]}) LIKE '%{value.lower()}%'")
                        case 'range':
                            constraints.append(f"{aliases[-1]}.{fnames[-1]} BETWEEN {value[0]} AND {value[1]}")
                        case 'year' | 'month' | 'day' | 'hour' | 'minute' | 'second':
                            constraints.append(f"{opname}({aliases[-1]}.{fnames[-1]}) = {value}")
                        case 'isnull':
                            constraints.append(f"{opname}({aliases[-1]}.{fnames[-1]}) IS {'NOT' if not value else ''} NULL")
                        case 'regex':
                            constraints.append(f"{aliases[-1]}.{fnames[-1]} LIKE {value}")
                        case 'in':
                            constraints.append(f"{aliases[-1]}.{fnames[-1]} IN {value}")
                else:
                    raise AttributeError(f'Wrong operation name specified: "{fnames[-1]}"')
        # Assembling query
        return {
            'joins': joins,
            'constraints': ' AND '.join(constraints)
        }

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


def assemble_query(
        model,  # Allows to gain access to model resources
        *args,  # Q class queries
        **kwargs  # Regular keyword queries
):
    assembled = {'joins': [], 'constraints': []}
    for arg in args:
        ass = arg.assemble_query(model)
        assembled['joins'].extend(ass['joins'])
        assembled['constraints'].append(f"({ass['constraints']})")
    if kwargs:
        ass = Q.make_query(model, **kwargs)
        assembled['joins'].extend(ass['joins'])
        assembled['constraints'].append(f"({ass['constraints']})")
    Q.join_index = 0
    return ''.join(
        assembled['joins']
    ) + ' WHERE ' + ' AND '.join(
        assembled['constraints']
    ) if assembled['constraints'] else ''
