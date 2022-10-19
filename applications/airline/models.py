from orm import fields, model


class Airport(model.Model):
    name = fields.CharField(null=False, unique=True)
    code = fields.CharField(null=False, unique=True)
    city = fields.CharField(null=False)
    country = fields.CharField(null=False)


class Plane(model.Model):
    name = fields.CharField(null=False, unique=True)
    economy_capacity = fields.UnsignedIntField(null=False)
    business_capacity = fields.UnsignedIntField()


class Airline(model.Model):
    name = fields.CharField(null=False, unique=True)
    country = fields.CharField(null=False)
    planes = fields.ManyToManyField(ref=Plane)


class Route(model.Model):
    departure_time = fields.DateTimeField(null=False)
    departure_point = fields.ForeignKey(ref=Airport, null=False)
    arrival_time = fields.DateTimeField(null=False)
    arrival_point = fields.ForeignKey(ref=Airport, null=False)
    plane = fields.ForeignKey(ref=Plane, null=False)


class Flight(model.Model):
    routes = fields.ManyToManyField(ref=Route)
    economy_price = fields.FloatField()
    business_price = fields.FloatField()
    currency = fields.CharField(size=3, default='USD', null=False)
    airline = fields.ForeignKey(ref=Airline, null=False, on_delete=fields.CASCADE)
