import inspect
import re
from . import model as mod


class Q:
    @staticmethod
    def make_query(model, raw: dict):
        fields = model.get_fields(model)  # Getting model template aka fields list
        id = fields.pop('id', None)  # Extracting id not to get error
        constraints, joins = [], []  # Initializing query concatenate list
        ops = (  # Query operations available
            'gt', 'gte', 'lt', 'lte', 'startswith', 'istartswith', 'endswith',
            'iendswith', 'contains', 'icontains', 'range', 'year', 'month',
            'day', 'hour', 'minute', 'second', 'isnull', 'regex', 'in'
        )
        lops = {'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<='}  # Logical operations and their aliases
        for name in raw.keys():  # Query format "<field>__<subfield_1>__...__<subfield_n>(__<operation>)=<value>"
            subq = name.split('__')
            if not subq[0] in fields:  # Checking if all fields specified right
                raise Exception(f'Wrong field "{subq[0]}" specified in query')
            if len(subq) == 1:  # <name> = <value> simple constraint
                constraints.append(f'{name} = {raw[name]}')
                continue
            else:
                opname = subq[-1] if subq[-1] in ops else None
                fnames = subq[:-1] if opname else subq
                # Getting table names nested structure
                tabs = []
                m = model
                for fn in fnames:
                    tabs.append(m.table_name())
                    try:
                        m = getattr(m, fn).ref
                    except AttributeError:
                        break
                # Listing joins based on field and table names
                [joins.append(
                    f" LEFT JOIN {tabs[i + 1]} ON {tabs[i]}.{fnames[i]} = {tabs[i + 1]}.id"
                ) for i in range(len(tabs) - 1)]
                # Appending constraint
                value = getattr(m, fnames[-1]).to_sql(raw[name])
                match opname:  # Adding constraint based on operation name
                    case None:
                        constraints.append(f'{tabs[-1]}.{fnames[-1]} = {value}')
                    case 'gt' | 'gte' | 'lt' | 'lte':
                        constraints.append(f'{tabs[-1]}.{fnames[-1]} {lops[opname]} {value}')
                    case 'startswith':
                        value = value.replace("'", '')
                        constraints.append(f"{tabs[-1]}.{fnames[-1]} LIKE '{value}%'")
                    case 'istartswith':
                        value = value.replace("'", '')
                        constraints.append(f"LOWER({tabs[-1]}.{fnames[-1]}) LIKE '{value.lower()}%'")
                    case 'endswith':
                        value = value.replace("'", '')
                        constraints.append(f"{tabs[-1]}.{fnames[-1]} LIKE '%{value}'")
                    case 'iendswith':
                        value = value.replace("'", '')
                        constraints.append(f"LOWER({tabs[-1]}.{fnames[-1]}) LIKE '%{value.lower()}'")
                    case 'contains':
                        value = value.replace("'", '')
                        constraints.append(f"{tabs[-1]}.{fnames[-1]} LIKE '%{value}%'")
                    case 'icontains':
                        value = value.replace("'", '')
                        constraints.append(f"LOWER({tabs[-1]}.{fnames[-1]}) LIKE '%{value.lower()}%'")
                    case 'range':
                        constraints.append(f"{tabs[-1]}.{fnames[-1]} BETWEEN {value[0]} AND {value[1]}")
                    case 'year' | 'month' | 'day' | 'hour' | 'minute' | 'second':
                        constraints.append(f"{opname}({tabs[-1]}.{fnames[-1]}) = {value}")
                    case 'isnull':
                        constraints.append(f"{opname}({tabs[-1]}.{fnames[-1]}) IS {'NOT' if not value else ''} NULL")
                    case 'regex':
                        constraints.append(f"{tabs[-1]}.{fnames[-1]} LIKE {value}")
                    case 'in':
                        constraints.append(f"{tabs[-1]}.{fnames[-1]} IN {value}")
        # Assembling query
        if id: constraints.append(f'{model.table_name()}.id = {id}')
        result = ''.join(joins) + ' WHERE ' + ' AND '.join(constraints)
        return result

    def __init__(self, **kwargs):
        if len(kwargs) != 1:
            raise Exception('Q class query must be a single keyword argument')
        try:
            fr = inspect.getouterframes(inspect.currentframe(), 2)[1]
            mname = re.findall(r'([A-Z]{1}[a-z]*)\.', ''.join(fr.code_context))[0]
            model = fr.frame.f_locals[mname]
            if not issubclass(model, mod.Model):
                raise IndexError
        except IndexError:
            raise Exception('Q class must be initialized inside of model method only')
        self.joins, self.query = Q.make_query(model, kwargs).split('WHERE')
        print()

    def __or__(self, other):
        self.query += ' OR ' + other.query
        return self

    def __ror__(self, other):
        self.query += ' OR ' + other.query
        return self

    def __and__(self, other):
        self.query += ' AND ' + other.query
        return self

    def __rand__(self, other):
        self.query += ' AND ' + other.query
        return self

    def __invert__(self):
        self.query = 'NOT ' + self.query
        return self