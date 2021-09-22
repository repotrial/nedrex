from mongoengine import (
    Document as _Document,
    StringField as _StringField,
    ListField as _ListField,
)


class Drug(_Document):
    meta = {
        "indexes" : [
            "primaryDomainId",
            "domainIds"
        ]
    }

    primaryDomainId = _StringField(unique=True)
    domainIds = _ListField(_StringField(), default=[])
    primaryDataset = _StringField()
    allDatasets = _ListField(_StringField(), default=[])

    synonyms = _ListField(_StringField(), default=[])
    drugCategories = _ListField(_StringField(), default=[])
    drugGroups = _ListField(_StringField(), default=[])

    displayName = _StringField()
    description = _StringField(default="")
    casNumber = _StringField(default="")
    indication = _StringField(default="")

    meta = {"allow_inheritance": True}


class BiotechDrug(Drug):
    sequences = _ListField(_StringField())
    type = _StringField(default="BiotechDrug")


class SmallMoleculeDrug(Drug):
    iupacName = _StringField()
    smiles = _StringField()
    inchi = _StringField()
    molecularFormula = _StringField()
    type = _StringField(default="SmallMoleculeDrug")
