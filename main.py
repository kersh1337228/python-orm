import datetime, time
from applications.booking.models import *
from applications.user.models import *
from applications.airline.models import *
from orm.query import Q
from orm.aggregate import *


if __name__ == '__main__':
    # s = time.perf_counter()
    # o = list(Order.prefetch_related('ticket__flight__routes'))
    o = Order.prefetch_related('ticket__flight__routes__plane').get(id=7)
    t = o.ticket.flight.routes[0]
    pass
    # print(time.perf_counter() - s)
