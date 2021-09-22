from mongoengine import (
    Document as _Document,
    StringField as _StringField,
    ListField as _ListField,
)


class Gene(_Document):
    meta = {
        "indexes" : [
            "primaryDomainId",
            "domainIds"
        ]
    }
    primaryDomainId = _StringField(unique=True)
    domainIds = _ListField(_StringField(), default=[])
    displayName = _StringField()
    synonyms = _ListField(_StringField(), default=[])
    approvedSymbol = _StringField(required=False)
    symbols = _ListField(_StringField(), default=[])
    description = _StringField()
    chromosome = _StringField()
    mapLocation = _StringField()
    geneType = _StringField(default=None)

    type = _StringField(default="Gene")
