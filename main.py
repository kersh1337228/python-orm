import datetime
from applications.booking.models import *
from applications.user.models import *
from applications.airline.models import *
from orm.query import Q
from orm.aggregate import Max


if __name__ == '__main__':
    o = Order.get(id=3)
    pred = o in Order.aggregate(Max('ticket__flight__economy_price'))
    pass
