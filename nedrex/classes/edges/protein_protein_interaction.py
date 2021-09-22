from mongoengine import (
    Document as _Document,
    StringField as _StringField,
    ListField as _ListField,
)


class ProteinInteractsWithProtein(_Document):
    meta = {
        "indexes" : [
            "memberOne",
            "memberTwo"
        ]
    }
    memberOne = _StringField()
    memberTwo = _StringField(unique_with="memberOne")

    methods = _ListField(_StringField(), default=[])
    databases = _ListField(_StringField(), default=[])
    evidenceTypes = _ListField(_StringField(), default=[])
    type = _StringField(default="ProteinInteractsWithProtein")
