from mongoengine import (
    Document as _Document,
    EmbeddedDocument as _EmbeddedDocument,
    EmbeddedDocumentField as _EmbeddedDocumentField,
    StringField as _StringField,
    FloatField as _FloatField,
    IntField as _IntField,
)


class BlastHit(_EmbeddedDocument):
    query = _StringField()
    hit = _StringField()
    bitscore = _FloatField()
    evalue = _FloatField()
    queryStart = _IntField()
    queryEnd = _IntField()
    hitStart = _IntField()
    hitEnd = _IntField()
    identity = _FloatField()
    mismatches = _IntField()
    gaps = _IntField()
    queryCover = _FloatField()
    hitCover = _FloatField()
    type = _StringField(default="BlastHit")


class ProteinSimilarityProtein(_Document):
    meta = {
        "indexes" : [
            "memberOne",
            "memberTwo"
        ]
    }

    memberOne = _StringField()
    memberTwo = _StringField(unique_with="memberOne")
    blast12 = _EmbeddedDocumentField(BlastHit)
    blast21 = _EmbeddedDocumentField(BlastHit)
    type = _StringField(default="ProteinSimilarityProtein")
