from fastapi import FastAPI

from .routers import (
    general,
    disorder,
    ppi,
    graph,
    static,
    closeness,
    trustrank,
    bicon,
    diamond,
    relations,
    must
)


app = FastAPI(
    title="NeDRexDB",
    description="""
An API for accessing the NeDRex database. For details about the edge and node types in the database, please consult this [Google Doc](https://docs.google.com/document/d/1ji9_vZJa5XoLXQspKkb3eJ1fn4Mr7CPghCQRavmi1Ac/edit?usp=sharing)

For a tutorial on using the API, please consult [this Google doc](https://docs.google.com/document/d/1_3juAFAYl2bXaJEsPwKTxazcv2TwtST-QM8PXj5c2II/edit?usp=sharing).
""",
    version="1.0.0",
    docs_url=None,
    redoc_url="/",
)

app.include_router(general.router, tags=["General"])
app.include_router(disorder.router, prefix="/disorder", tags=["Disorder routes"])
app.include_router(ppi.router, tags=["PPI routes"])
app.include_router(graph.router, tags=["Graph routes"])
app.include_router(static.router, prefix="/static", tags=["Static"])
app.include_router(closeness.router, tags=["Ranking"])
app.include_router(trustrank.router, tags=["Ranking"])
app.include_router(bicon.router, prefix="/bicon", tags=["BiCoN"])
app.include_router(diamond.router, prefix="/diamond", tags=["DIAMOnD"])
app.include_router(relations.router, prefix="/relations", tags=["Relations"])
app.include_router(must.router, prefix="/must", tags=["MuST"])
