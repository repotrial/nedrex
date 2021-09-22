from mongoengine import (
    Document as _Document,
    StringField as _StringField,
    FloatField as _FloatField,
)


class MoleculeSimilarityMolecule(_Document):
    meta = {
        "indexes" : [
            "memberOne",
            "memberTwo"
        ]
    }
    memberOne = _StringField()
    memberTwo = _StringField()
    morganR1 = _FloatField()
    morganR2 = _FloatField()
    morganR3 = _FloatField()
    morganR4 = _FloatField()
    maccs = _FloatField()
    type = _StringField(default="MoleculeSimilarityMolecule")
