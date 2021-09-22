from mongoengine import (
    Document as _Document,
    StringField as _StringField,
    ListField as _ListField,
)


class DrugHasIndication(_Document):
    meta = {
        "indexes" : [
            "sourceDomainId",
            "targetDomainId"
        ]
    }

    sourceDomainId = _StringField()
    targetDomainId = _StringField()
    databases = _ListField(_StringField(), default=[])
    type = _StringField(default="DrugHasIndication")
