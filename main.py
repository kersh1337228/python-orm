import datetime
from applications.user.models import Role, User
from applications.airline.models import *
from orm.query import Q

class Test1:
    def __or__(self, other):
        print(self, other)
    def __ror__(self, other):
        print(other, self)
class Test2:
    def __or__(self, other):
        print(self, other)
    def __ror__(self, other):
        print(other, self)

if __name__ == '__main__':
    # r = Route.filter(
    #     plane__airline__country__in=('Russia', 'Belarus'),
    #     arrival_point__city='Kyiv'
    # )
    r = Route.filter(
        Q(departure_point__capacity__gte=100) |
        Q(plane__airline__country__in=('Russia', 'Belarus')) &
        ~Q(arrival_point__city='Kyiv'), Q(departure_point__capacity__lte=900),
        arrival_point__capacity__lte=900
    )
    print()
    # nq = Q(departure_point__capacity__gte=100) |\
    #      Q(plane__airline__country__in=('Russia', 'Belarus')) &\
    #      ~Q(arrival_point__city='Kyiv')
