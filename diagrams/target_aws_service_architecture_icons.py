"""
Target AWS service architecture diagram with AWS symbols.

Generates: diagrams/target_aws_service_architecture_icons.png
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Athena, Glue, GlueCrawlers, GlueDataCatalog
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import EventbridgeScheduler, StepFunctions
from diagrams.aws.ml import Bedrock
from diagrams.aws.network import CloudFront
from diagrams.aws.storage import S3
from diagrams.onprem.client import User
from diagrams.onprem.network import Internet

graph_attr = {
    "fontsize": "20",
    "bgcolor": "white",
    "pad": "0.6",
    "splines": "ortho",
    "nodesep": "0.7",
    "ranksep": "1.0",
    "fontname": "Arial",
}

node_attr = {
    "fontsize": "13",
    "fontname": "Arial",
}

edge_attr = {
    "fontsize": "11",
    "fontname": "Arial",
}

with Diagram(
    "Target AWS Service Architecture",
    filename="diagrams/target_aws_service_architecture_icons",
    outformat="png",
    show=False,
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
    direction="LR",
):
    with Cluster("External Sources"):
        energy_apis = Internet("Energy APIs\nElexon / ENTSO-E /\nENTSOG")
        news = Internet("RSS and\nmarket news")

    with Cluster("Private AWS Data And Insight Boundary"):
        scheduler = EventbridgeScheduler("EventBridge\nScheduler")
        ingest = Lambda("Lambda\ningestion")
        raw = S3("S3 raw zone\nsource payloads")

        with Cluster("Catalog And Transform"):
            crawler = GlueCrawlers("Glue\nCrawlers")
            catalog = GlueDataCatalog("Glue Data\nCatalog")
            etl = Glue("Glue ETL\nParquet")

        curated = S3("S3 curated zone\nvalidated products")
        athena = Athena("Athena\nportfolio queries")

        with Cluster("AI Insight Orchestration"):
            sfn = StepFunctions("Step Functions\nrun orchestration")
            ai_lambda = Lambda("Lambda\nAI assembly")
            bedrock = Bedrock("Bedrock or\nOpenClaw target")
            validation = Lambda("Schema\nvalidation gate")

        audit = S3("S3 audit + failed\nrun evidence")
        dashboard_json = S3("dashboard JSON\napproved public fields")

    with Cluster("Public Decision Surface"):
        public_bucket = S3("S3 public\norigin")
        cloudfront = CloudFront("CloudFront\ncache + delivery")
        dashboard = User("React dashboard\nfilters + export")

    energy_apis >> scheduler
    news >> Edge(label="market context", constraint="false") >> ai_lambda
    scheduler >> ingest

    ingest >> Edge(label="writes source payloads") >> raw
    raw >> crawler >> catalog
    raw >> Edge(label="reads") >> etl
    catalog >> Edge(style="dashed", label="metadata") >> etl
    etl >> Edge(label="writes parquet") >> curated
    curated >> athena

    athena >> Edge(label="evidence") >> sfn
    sfn >> ai_lambda
    ai_lambda >> Edge(label="managed AI call") >> bedrock
    bedrock >> validation
    validation >> Edge(color="darkgreen", label="valid publish") >> dashboard_json
    validation >> Edge(color="darkred", label="invalid quarantine") >> audit
    sfn >> Edge(style="dashed", color="gray", label="run audit", constraint="false") >> audit

    dashboard_json >> public_bucket >> cloudfront >> dashboard
