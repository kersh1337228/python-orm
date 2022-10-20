import datetime, time
from applications.booking.models import *
from applications.user.models import *
from applications.airline.models import *
from orm.query import Q
from orm.aggregate import *


if __name__ == '__main__':
    s = time.perf_counter()
    a = list(Order.filter(
        ticket__flight__routes__plane__name__istartswith='airbus'
    ).annotate(
        price_formula=Max('ticket__flight__economy_price') //
                      Min('ticket__flight__economy_price') -
                      Avg('ticket__flight__economy_price') *
                      Sum('ticket__flight__economy_price')
    ).order_by(
        '-price_formula'
    ))
    print(time.perf_counter() - s)
