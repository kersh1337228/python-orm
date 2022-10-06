import datetime
from applications.user.models import Role, User
from applications.airline.models import *
from orm.query import Q


if __name__ == '__main__':
    rs = Route.filter(
        Q(plane__airline__country__in=('Belarus', 'Austria'))
    ).filter(arrival_point__capacity__lte=900)
    rs.update(plane=Plane.get(id=5))
