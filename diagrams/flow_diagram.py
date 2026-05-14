"""
AWS Data Flow Diagram
Energy Market Data Lake

Generates: flow_diagram.png
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage import S3
from diagrams.aws.compute import Lambda
from diagrams.aws.analytics import Glue, Athena, GlueCrawlers, GlueDataCatalog
from diagrams.aws.integration import Eventbridge
from diagrams.onprem.client import User
from diagrams.onprem.network import Internet

graph_attr = {
    "fontsize": "20",
    "bgcolor": "white",
    "pad": "0.6",
    "splines": "ortho",
    "nodesep": "1.0",
    "ranksep": "1.4",
    "fontname": "Arial",
    "label": "",
}

node_attr = {
    "fontsize": "13",
    "fontname": "Arial",
}

with Diagram(
    "Energy Market Data Lake – Data Flow",
    filename="diagrams/flow_diagram",
    outformat="png",
    show=False,
    graph_attr=graph_attr,
    node_attr=node_attr,
    direction="LR",
):

    # ── Step 1: Trigger ────────────────────────────────────────────────────
    with Cluster("① Schedule"):
        scheduler = Eventbridge("EventBridge\nSchedules\ncreated but disabled")

    # ── Step 2: Ingest ─────────────────────────────────────────────────────
    with Cluster("② Ingest"):
        ingest = Lambda("Lambda\nIngest\n(Python)")

    # ── Step 3: External APIs ──────────────────────────────────────────────
    with Cluster("External APIs"):
        elexon = Internet("Elexon\nUK Demand\n& Imbalance")
        entsoe = Internet("ENTSO-E\nEU Load\n& DA Prices")
        entsog = Internet("ENTSOG\nEU Gas\nFlows")

    # ── Step 4: Raw Zone ───────────────────────────────────────────────────
    with Cluster("③ Raw Zone  s3://.../raw/"):
        raw_elexon = S3("source=elexon\ndataset=atl\ndate=YYYY-MM-DD")
        raw_prices = S3("source=elexon\ndataset=system_prices\ndate=YYYY-MM-DD")
        raw_entsoe = S3("source=entsoe\ndataset=load|prices\ndate=YYYY-MM-DD")
        raw_entsog = S3("source=entsog\ndataset=gas_flow|gas_demand\ndate=YYYY-MM-DD")

    # ── Step 5: Cataloguing ────────────────────────────────────────────────
    with Cluster("④ Catalogue"):
        crawler = GlueCrawlers("Glue\nCrawler")
        catalog = GlueDataCatalog("Glue Data\nCatalog")

    # ── Step 6: Transform ─────────────────────────────────────────────────
    with Cluster("⑤ Transform"):
        etl = Glue("Glue ETL\n(PySpark)\nJSON/XML → Parquet\nSchemas enforced")

    # ── Step 7: Curated Zone ───────────────────────────────────────────────
    with Cluster("⑥ Curated Zone  s3://.../curated/"):
        curated_power = S3("dataset=electricity\nsource=elexon|entsoe\nregion=gb|fr|de|nl\ndate=YYYY-MM-DD")
        curated_gas = S3("dataset=gas\nsource=entsog\nregion=eu\ndate=YYYY-MM-DD")

    # ── Step 8: Query ──────────────────────────────────────────────────────
    with Cluster("⑦ Query"):
        athena = Athena("Athena\nServerless SQL\nPartition pruning")

    # ── Step 9: Consume ────────────────────────────────────────────────────
    with Cluster("⑧ Consume"):
        analyst = User("Analyst\nAd-hoc SQL")
        dashboard = User("React\nDashboard")

    # ── Flow Edges ─────────────────────────────────────────────────────────
    # Trigger
    scheduler >> Edge(label="manual or disabled\nschedule path") >> ingest

    # APIs → Lambda
    elexon >> Edge(color="darkblue", label="demand ATL\nsystem prices") >> ingest
    entsoe >> Edge(color="darkblue", label="actual load\nDA prices (XML)") >> ingest
    entsog >> Edge(color="darkblue", label="gas flows") >> ingest

    # Lambda → Raw
    ingest >> Edge(color="darkorange", label="PUT JSON") >> raw_elexon
    ingest >> Edge(color="darkorange", label="PUT JSON") >> raw_prices
    ingest >> Edge(color="darkorange", label="PUT XML") >> raw_entsoe
    ingest >> Edge(color="darkorange", label="PUT JSON") >> raw_entsog

    # Raw → Crawler → Catalog
    raw_elexon >> Edge(color="gray", style="dashed") >> crawler
    raw_prices >> Edge(color="gray", style="dashed") >> crawler
    raw_entsoe >> Edge(color="gray", style="dashed") >> crawler
    raw_entsog >> Edge(color="gray", style="dashed") >> crawler
    crawler >> Edge(label="register\nschemas") >> catalog

    # Raw → ETL
    raw_elexon >> Edge(color="purple", label="read") >> etl
    raw_prices >> Edge(color="purple") >> etl
    raw_entsoe >> Edge(color="purple") >> etl
    raw_entsog >> Edge(color="purple") >> etl
    catalog >> Edge(color="gray", style="dashed", label="schema") >> etl

    # ETL → Curated
    etl >> Edge(color="green", label="write\nParquet") >> curated_power
    etl >> Edge(color="green", label="write\nParquet") >> curated_gas

    # Curated → Athena
    curated_power >> Edge(label="scan\npartitions") >> athena
    curated_gas >> Edge(label="scan\npartitions") >> athena
    catalog >> Edge(color="gray", style="dashed", label="table\nmetadata") >> athena

    # Athena → Consumers
    athena >> Edge(label="query\nresults") >> analyst
    athena >> Edge(label="JSON\nexport") >> dashboard
