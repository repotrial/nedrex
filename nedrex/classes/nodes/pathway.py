from mongoengine import (
    StringField as _StringField,
    ListField as _ListField,
    Document as _Document,
)


class Pathway(_Document):
    meta = {
        "indexes" : [
            "primaryDomainId",
            "domainIds"
        ]
    }

    primaryDomainId = _StringField()
    domainIds = _ListField(_StringField(), default=[])
    displayName = _StringField(default="")
    species = _StringField(default="")
    type = _StringField(default="Pathway")
