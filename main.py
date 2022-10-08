import datetime
from applications.user.models import Role, User
from applications.airline.models import *
from orm.query import Q


if __name__ == '__main__':
    roles = Role.filter()
    User.filter(username__startswith='test').delete()
    users = User.bulk_create(
        {'username': 'test1', 'password': '123', 'role': roles[0]},
        {'username': 'test2', 'password': '234', 'role': roles[1]},
        {'username': 'test3', 'password': '456', 'role': roles[0]},
    )
    # u = User.create()
    pass
