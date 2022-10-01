import datetime
from applications.user.models import Role, User
from applications.airline.models import *


if __name__ == '__main__':
    r = Route.filter(
        plane__airline__country__in=('Russia', 'Belarus'),
        arrival_point__city='Kyiv'
    )
    print()
