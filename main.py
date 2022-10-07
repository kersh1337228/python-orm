import datetime
from applications.user.models import Role, User
from applications.airline.models import *
from orm.query import Q


if __name__ == '__main__':
    rs = Airport.filter(Q(capacity__lte=900), city__startswith='M') * Airport.filter(Q(capacity__lte=500) | Q(capacity__gte=50))
    pass
