from . import query as qr, fields as fld


class BasicAggregate:  # Base wrapper for SQL aggregate functions
    functions = ('MAX', 'MIN', 'AVG', 'COUNT', 'SUM')  # MySQL aggregate functions list

    def __init__(self, field_name: str, function: str):
        if not function in BasicAggregate.functions:  # Check if function specified right
            raise ValueError('Wrong aggregate function specified')
        else:
            self.field_name = field_name
            self.function = function

    def __call__(self, model):  # Adding joins and SQL
        fnames = self.field_name.replace('-', '').split('__')
        joins, current_model = qr.Q.make_joins(model, fnames)
        return {
            'joins': joins,
            'fields': f'''{self.function}({joins[-1]["alias"] if joins
            else f"{model.table_name}0"}.{fnames[-1]}) AS {
            self.field_name}__{self.function.lower()}'''
        }


class Max(BasicAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'MAX')


class Min(BasicAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'MIN')


class Avg(BasicAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'AVG')


class Count(BasicAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'COUNT')

    def __call__(self, model):  # BasicAggregate method override to work properly with ManyToManyField
        fnames = self.field_name.replace('-', '').split('__')
        joins, current_model = qr.Q.make_joins(model, fnames)
        return {
            'joins': joins,
            'fields': f'''COUNT(*) AS {
            self.field_name}__{self.function.lower()}'''
        }



class Sum(BasicAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'SUM')
