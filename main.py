import datetime
from applications.booking.models import *
from applications.user.models import *
from applications.airline.models import *
from orm.query import Q


if __name__ == '__main__':
    list(Order.filter())
    pass