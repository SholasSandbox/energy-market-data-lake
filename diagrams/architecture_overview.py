"""
AWS Architecture Overview Diagram
Energy Market Data Lake

Generates: architecture_overview.png
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage import S3
from diagrams.aws.compute import Lambda
from diagrams.aws.analytics import Glue, Athena, GlueCrawlers, GlueDataCatalog
from diagrams.aws.integration import Eventbridge
from diagrams.aws.management import Cloudwatch
from diagrams.aws.security import IAMRole
from diagrams.onprem.client import User
from diagrams.onprem.network import Internet

graph_attr = {
    "fontsize": "20",
    "bgcolor": "white",
    "pad": "0.5",
    "splines": "ortho",
    "nodesep": "0.8",
    "ranksep": "1.2",
    "fontname": "Arial",
}

node_attr = {
    "fontsize": "13",
    "fontname": "Arial",
}

with Diagram(
    "Energy Market Data Lake – Architecture Overview",
    filename="diagrams/architecture_overview",
    outformat="png",
    show=False,
    graph_attr=graph_attr,
    node_attr=node_attr,
    direction="TB",
):

    # ── External Data Sources ──────────────────────────────────────────────
    with Cluster("External Data Sources (Public APIs)"):
        elexon = Internet("Elexon API\n(UK Power)")
        entsoe = Internet("ENTSO-E API\n(EU Power)")
        entsog = Internet("ENTSOG API\n(EU Gas)")

    # ── Orchestration ──────────────────────────────────────────────────────
    with Cluster("Orchestration"):
        scheduler = Eventbridge("EventBridge\nSchedule\n(Daily 02:00 UTC)")
        alarm = Cloudwatch("CloudWatch\nLogs & Alarms")

    # ── Ingestion Compute ──────────────────────────────────────────────────
    with Cluster("Ingestion Layer"):
        ingest_lambda = Lambda("Lambda\nIngest Function\n(Python 3.x)")
        lambda_role = IAMRole("Lambda\nExecution Role")

    # ── Storage ────────────────────────────────────────────────────────────
    with Cluster("Amazon S3 – Data Lake"):
        s3_raw = S3("S3 Raw Zone\nJSON / XML\nPartitioned by\nsource / dataset / date")
        s3_curated = S3("S3 Curated Zone\nParquet (Snappy)\nPartitioned by\nsource / region / date")

    # ── Transformation ─────────────────────────────────────────────────────
    with Cluster("Transformation & Cataloguing"):
        crawler = GlueCrawlers("Glue Crawler\n(Schema Discovery)")
        catalog = GlueDataCatalog("Glue Data Catalog\n(Metastore)")
        etl_job = Glue("Glue ETL Job\n(PySpark)\nRaw → Parquet")
        glue_role = IAMRole("Glue\nExecution Role")

    # ── Query & Analytics ──────────────────────────────────────────────────
    with Cluster("Query & Analytics"):
        athena = Athena("Amazon Athena\n(Serverless SQL)")
        s3_results = S3("S3 Query\nResults Bucket")

    # ── Consumption ────────────────────────────────────────────────────────
    with Cluster("Consumption"):
        analyst = User("Energy Market\nAnalyst")
        dashboard = User("React Dashboard\n(GitHub Pages)")

    # ── Edges ─────────────────────────────────────────────────────────────
    # Scheduling → Lambda
    scheduler >> Edge(label="triggers daily") >> ingest_lambda
    lambda_role >> Edge(style="dashed", color="gray") >> ingest_lambda

    # External APIs → Lambda
    elexon >> Edge(label="HTTPS pull") >> ingest_lambda
    entsoe >> Edge(label="HTTPS pull") >> ingest_lambda
    entsog >> Edge(label="HTTPS pull") >> ingest_lambda

    # Lambda → S3 Raw
    ingest_lambda >> Edge(label="writes JSON/XML") >> s3_raw

    # Lambda → CloudWatch
    ingest_lambda >> Edge(style="dashed", color="orange", label="logs") >> alarm

    # S3 Raw → Glue Crawler → Catalog
    s3_raw >> Edge(label="crawls schema") >> crawler
    crawler >> Edge(label="registers tables") >> catalog

    # S3 Raw → Glue ETL → S3 Curated
    s3_raw >> Edge(label="reads raw") >> etl_job
    etl_job >> Edge(label="writes Parquet") >> s3_curated
    catalog >> Edge(style="dashed", color="gray") >> etl_job
    glue_role >> Edge(style="dashed", color="gray") >> etl_job

    # S3 Curated → Athena
    s3_curated >> Edge(label="scans partitions") >> athena
    catalog >> Edge(style="dashed", color="gray", label="table metadata") >> athena
    athena >> Edge(style="dashed", color="gray") >> s3_results

    # Athena → Consumers
    athena >> Edge(label="SQL results") >> analyst
    athena >> Edge(label="JSON export") >> dashboard
