from . import query as qr, fields as fld
from abc import ABC, abstractmethod


# Interface for aggregate functions and operation
# wrappers to apply binary operations on them.
class BaseBinaryOperableAggregate(ABC):
    @abstractmethod
    def __call__(
            self,
            model: object,
            primary_join_index: int,
            annotate_join_index: int
    ) -> tuple[list, str, str, int]:
        pass

    # Binary operations overload.
    def __add__(self, other):
        return AggregateOperationWrapper('+', 'add', self, other)

    def __radd__(self, other):
        return AggregateOperationWrapper('+', 'add', other, self)

    def __sub__(self, other):
        return AggregateOperationWrapper('-', 'sub', self, other)

    def __rsub__(self, other):
        return AggregateOperationWrapper('-', 'sub', other, self)

    def __mul__(self, other):
        return AggregateOperationWrapper('*', 'mul', self, other)

    def __rmul__(self, other):
        return AggregateOperationWrapper('*', 'mul', other, self)

    def __floordiv__(self, other):
        return AggregateOperationWrapper('div', 'floor_div', self, other)

    def __rfloordiv__(self, other):
        return AggregateOperationWrapper('div', 'floor_div', other, self)

    def __truediv__(self, other):
        return AggregateOperationWrapper('/', 'div', self, other)

    def __rtruediv__(self, other):
        return AggregateOperationWrapper('/', 'div', other, self)

    def __eq__(self, other):
        return AggregateOperationWrapper('=', 'equals', self, other)

    def __ne__(self, other):
        return AggregateOperationWrapper('!=', 'not_equals', self, other)

    def __gt__(self, other):
        return AggregateOperationWrapper('>', 'greater_than', self, other)

    def __ge__(self, other):
        return AggregateOperationWrapper('>=', 'greater_than_or_equals', self, other)

    def __lt__(self, other):
        return AggregateOperationWrapper('<', 'less_than', self, other)

    def __le__(self, other):
        return AggregateOperationWrapper('<=', 'less_than_or_equals', self, other)


class AggregateOperationWrapper(BaseBinaryOperableAggregate):
    def __init__(self, operation: str, operation_alias: str, *args):
        self.__operation = operation
        self.__operation_alias = operation_alias
        self.__subset = args

    def __call__(
            self,
            model: object,
            primary_join_index: int,
            annotate_join_index: int
    ):
        joins, fields, alias = [], [], []
        for aggregate in self.__subset:
            ajoins, afields, aalias, primary_join_index = aggregate(
                model, primary_join_index, annotate_join_index
            )
            joins.extend(ajoins)
            fields.append(afields)
            alias.append(aalias)
        fields = f' {self.__operation} '.join(fields)
        alias = f'___{self.__operation_alias}___'.join(alias)
        return joins, fields, alias, primary_join_index


class BaseAggregate(BaseBinaryOperableAggregate):  # Base wrapper for SQL aggregate functions
    functions = ('MAX', 'MIN', 'AVG', 'COUNT', 'SUM')  # MySQL aggregate functions list

    def __init__(self, field_name: str, function: str):
        if not function in BaseAggregate.functions:  # Check if function specified right
            raise ValueError('Wrong aggregate function specified')
        else:
            self._field_name = field_name
            self._function = function

    def __call__(
            self,
            model: object,
            primary_join_index: int,
            annotate_join_index: int
    ) -> tuple[list, str, str, int]:  # Converting aggregate function into SQL-friendly format
        fnames = self._field_name.replace('-', '').split('__')  # Divide name given into subfields sequence
        joins, current_model, primary_join_index = qr.Q.make_joins(
            model, fnames, primary_join_index, annotate_join_index
        )  # Adding joins for nested models
        return (
            joins,
            f'''{self._function}({joins[-1]["alias"] if joins
            else f"{model.table_name}00"}.{fnames[-1]})''',
            f'{self._field_name}__{self._function.lower()}',
            primary_join_index
        )

class Max(BaseAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'MAX')


class Min(BaseAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'MIN')


class Avg(BaseAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'AVG')


class Count(BaseAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'COUNT')

    def __call__(
            self,
            model: object,
            primary_join_index: int,
            annotate_join_index: int
    ) -> tuple[list, str, str, int]:  # BasicAggregate method override to work properly with ManyToManyField
        fnames = self._field_name.replace('-', '').split('__')
        joins, current_model, primary_join_index = qr.Q.make_joins(
            model, fnames, primary_join_index, annotate_join_index
        )   # Aggregate alias format: <field>__<function>
        return (
            joins,
            'COUNT(*)',
            f'{self._field_name}__{self._function.lower()}',
            primary_join_index
        )


class Sum(BaseAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'SUM')
