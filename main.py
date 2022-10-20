import datetime, time
from applications.booking.models import *
from applications.user.models import *
from applications.airline.models import *
from orm.query import Q
from orm.aggregate import *


if __name__ == '__main__':
    rs = Flight.filter(id=7).aggregate(Count('routes'))
    pass
