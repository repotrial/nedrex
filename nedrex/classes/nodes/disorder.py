from mongoengine import (
    Document as _Document,
    StringField as _StringField,
    ListField as _ListField,
)


class Disorder(_Document):
    meta = {
        "indexes" : [
            "primaryDomainId",
            "domainIds"
        ]
    }
    primaryDomainId = _StringField(required=True)
    domainIds = _ListField(_StringField(), default=[])
    displayName = _StringField()
    synonyms = _ListField(_StringField(), default=[])
    icd10 = _ListField(_StringField(), default=[])
    description = _StringField(default="")
    type = _StringField(default="Disorder")
