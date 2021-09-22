from mongoengine import (
    Document as _Document,
    StringField as _StringField,
)


class DrugHasContraindication(_Document):
    meta = {
        "indexes" : [
            "sourceDomainId",
            "targetDomainId"
        ]
    }

    sourceDomainId = _StringField()
    targetDomainId = _StringField()
    type = _StringField(default="DrugHasContraindication")
