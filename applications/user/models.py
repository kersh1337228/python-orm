from orm import fields, model


class Role(model.Model):
    name = fields.CharField(size=255, null=False, unique=True)


class User(model.Model):
    username = fields.CharField(size=255, null=False, unique=True)
    password = fields.CharField(size=255, null=False)
    role = fields.ForeignKey(ref=Role, null=False)

