from mongoengine import (
    Document as _Document,
    StringField as _StringField,
    FloatField as _FloatField,
)


class DisorderComorbidWithDisorder(_Document):
    meta = {
        "indexes" : [
            "memberOne",
            "memberTwo"
        ]
    }
    memberOne = _StringField()
    memberTwo = _StringField()
    phiCor = _FloatField()
    rr12 = _FloatField()
    rr21 = _FloatField()
    rrGeoMean = _FloatField()

    type = _StringField(default="DisorderComorbidWithDisorder")
