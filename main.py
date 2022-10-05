import datetime
from applications.user.models import Role, User
from applications.airline.models import *
from orm.query import Q

class Test:
    @classmethod
    def foo(cls, q):
        print(q)

if __name__ == '__main__':
    # r = Route.filter(
    #     plane__airline__country__in=('Russia', 'Belarus'),
    #     arrival_point__city='Kyiv'
    # )
    Route.filter(
        Q(departure_point__capacity__gte=100) |
        Q(plane__airline__country__in=('Russia', 'Belarus')) &
        ~Q(arrival_point__city='Kyiv')
    )
    # nq = Q(departure_point__capacity__gte=100) |\
    #      Q(plane__airline__country__in=('Russia', 'Belarus')) &\
    #      ~Q(arrival_point__city='Kyiv')
