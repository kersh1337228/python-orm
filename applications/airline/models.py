from orm import fields, model


class Airport(model.Model):
    name = fields.CharField(null=False, unique=True)
    code = fields.CharField(null=False, unique=True)
    city = fields.CharField(null=False)
    country = fields.CharField(null=False)
    capacity = fields.IntField(null=False)


class Airline(model.Model):
    name = fields.CharField(null=False, unique=True)
    country = fields.CharField(null=False)


class Plane(model.Model):
    name = fields.CharField(null=False, unique=True)
    capacity = fields.JSONField(null=False)
    airline = fields.ForeignKey(ref=Airline, null=False)


class Route(model.Model):
    flight_time = fields.DurationField(null=False)
    departure_time = fields.DateTimeField(null=False)
    departure_point = fields.ForeignKey(ref=Airport, null=False)
    arrival_time = fields.DateTimeField(null=False)
    arrival_point = fields.ForeignKey(ref=Airport, null=False)
    plane = fields.ForeignKey(ref=Plane, null=False)


class Flight(model.Model):
    routes = fields.ManyToManyField(ref=Route)
    costs = fields.JSONField(null=False)
