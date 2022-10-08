from orm import fields, model
from ..airline import models as alms
from ..user import models as ums
import datetime


class Ticket(model.Model):
    flight = fields.ForeignKey(ref=alms.Flight, null=False, on_delete=fields.CASCADE)
    type = fields.CharField(default='economy', null=False)
    baggage = fields.BooleanField(default=False, null=False)


class Order(model.Model):
    ticket = fields.ForeignKey(ref=Ticket, null=False, unique=True, on_delete=fields.CASCADE)
    user = fields.ForeignKey(ref=ums.User, null=False, on_delete=fields.CASCADE)
    change_time = fields.DateTimeField(default=datetime.datetime.now())
    state = fields.CharField(default='Created', null=False, choices=('created', 'confirmed', 'changed', 'closed'))
