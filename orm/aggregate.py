from . import query as qr


class BasicAggregate:  # Base wrapper for SQL aggregate functions
    functions = ('MAX', 'MIN', 'AVG', 'COUNT', 'SUM')  # MySQL aggregate functions list

    def __init__(self, field_name: str, function: str):
        if not function in BasicAggregate.functions:  # Check if function specified right
            raise ValueError('Wrong aggregate function specified')
        else:
            self.field_name = field_name
            self.function = function

    def assemble(self, model):  # Adding joins and SQL
        joins = []
        fnames = self.field_name.replace('-', '').split('__')
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
                        f'{qr.Q.join_index - 1 if current_model != model else 0}',
                        field, qr.Q.join_index
                    ))
                    current_model = attr.ref
                    qr.Q.join_index += 1
                except AttributeError:
                    break
        return {
            'joins': joins,
            'fields': f'''{self.function}({joins[-1]["alias"] if joins
            else f"{model.table_name}0"}.{fnames[-1]})'''
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


class Sum(BasicAggregate):
    def __init__(self, field_name: str):
        super().__init__(field_name, 'SUM')
