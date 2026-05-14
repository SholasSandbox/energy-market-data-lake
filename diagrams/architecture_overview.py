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
    "nodesep": "1.0",
    "ranksep": "1.4",
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
    direction="LR",
):

    # ── External Data Sources ──────────────────────────────────────────────
    with Cluster("External Data Sources (Public APIs)"):
        elexon = Internet("Elexon API\n(UK Power)")
        entsoe = Internet("ENTSO-E API\n(EU Power)")
        entsog = Internet("ENTSOG API\n(EU Gas)")

    # ── Orchestration ──────────────────────────────────────────────────────
    with Cluster("Orchestration"):
        scheduler = Eventbridge("EventBridge\nSchedules\n(Disabled)")
        manual_ai = Eventbridge("Manual\nStep Functions\nAI Insight Run")

    # ── Compute ────────────────────────────────────────────────────────────
    with Cluster("Compute"):
        ingest_lambda = Lambda("Lambda\nIngest Function\n(Python 3.x)")
        ai_lambda = Lambda("Lambda\nnews_ai_orchestration\n(Workflow actions)")
        lambda_role = IAMRole("Lambda\nExecution Role")
        logs = Cloudwatch("CloudWatch\nLogs")

    # ── Storage ────────────────────────────────────────────────────────────
    with Cluster("Private S3 Data Lake"):
        s3_raw = S3("raw/\nPower, gas,\nnews payloads")
        s3_curated = S3("curated/\nElectricity, gas,\nnews, AI insight")
        s3_controls = S3("audit/ + failed/\nPublish records and\nquarantined failures")

    # ── Transformation ─────────────────────────────────────────────────────
    with Cluster("Transformation & Cataloguing"):
        crawler = GlueCrawlers("Glue Crawler\n(Schema Discovery)")
        catalog = GlueDataCatalog("Glue Data Catalog\n(Metastore)")
        etl_job = Glue("Glue ETL Job\n(PySpark)\nRaw → Parquet")
        glue_role = IAMRole("Glue\nExecution Role")

    # ── Query & Analytics ──────────────────────────────────────────────────
    with Cluster("Query And Analytics"):
        athena = Athena("Amazon Athena\n(Serverless SQL)")
        s3_results = S3("S3 Query\nResults Bucket")

    with Cluster("Public Dashboard Boundary"):
        dashboard_snapshot = S3("Approved\nDashboard JSON\nPublic fields only")

    # ── Consumption ────────────────────────────────────────────────────────
    with Cluster("Consumption"):
        analyst = User("Energy Market\nAnalyst")
        dashboard = User("React Dashboard\nPhase 10 Overview")

    # ── Edges ─────────────────────────────────────────────────────────────
    # Scheduling → Lambda
    scheduler >> Edge(style="dashed", label="not auto-running") >> ingest_lambda
    manual_ai >> Edge(label="manual execution") >> ai_lambda
    lambda_role >> Edge(style="dashed", color="gray") >> ingest_lambda
    lambda_role >> Edge(style="dashed", color="gray") >> ai_lambda

    # External APIs → Lambda
    elexon >> Edge(label="HTTPS pull") >> ingest_lambda
    entsoe >> Edge(label="HTTPS pull") >> ingest_lambda
    entsog >> Edge(label="HTTPS pull") >> ingest_lambda

    # Lambda → S3 Raw
    ingest_lambda >> Edge(label="writes JSON/XML") >> s3_raw

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
    athena >> Edge(label="energy input") >> ai_lambda

    # AI orchestration and publish
    ai_lambda >> Edge(label="curated evidence") >> s3_curated
    ai_lambda >> Edge(label="audit or failure") >> s3_controls
    ai_lambda >> Edge(label="validated snapshot") >> dashboard_snapshot
    ingest_lambda >> Edge(style="dashed", color="orange", label="logs") >> logs
    ai_lambda >> Edge(style="dashed", color="orange", label="logs") >> logs

    # Athena → Consumers
    athena >> Edge(label="SQL results") >> analyst
    dashboard >> Edge(label="fetches approved\npublic JSON") >> dashboard_snapshot
