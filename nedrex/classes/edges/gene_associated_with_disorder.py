from mongoengine import Document as _Document, StringField as _StringField, ListField as _ListField, FloatField as _FloatField


class GeneAssociatedWithDisorder(_Document):
    meta = {
        "indexes" : [
            "sourceDomainId",
            "targetDomainId"
        ]
    }
    sourceDomainId = _StringField()
    targetDomainId = _StringField(unique_with="sourceDomainId")
    assertedBy = _ListField(_StringField(), default=[])
    score = _FloatField(required=False)
    type = _StringField(default="GeneAssociatedWithDisorder")

