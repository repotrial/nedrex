from mongoengine import (
    Document as _Document,
    StringField as _StringField,
    ListField as _ListField,
    IntField as _IntField,
)


class Protein(_Document):
    meta = {
        "indexes" : [
            "primaryDomainId",
            "domainIds"
        ]
    }

    primaryDomainId = _StringField(unique=True)
    domainIds = _ListField(_StringField(), default=[])
    sequence = _StringField(default="")
    displayName = _StringField(default="")
    synonyms = _ListField(_StringField(), default=[])
    comments = _StringField(default="")
    geneName = _StringField(default="")
    taxid = _IntField(default=-1)  # -1 indicates no taxonomy assigned.
    type = _StringField(default="Protein")
