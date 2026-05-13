"""Tower Hamlets Air Quality Forecasting & Alerting - Reference Architecture."""

from diagrams import Cluster, Diagram, Edge
from diagrams.azure.compute import FunctionApps
from diagrams.azure.identity import ActiveDirectory
from diagrams.azure.integration import APIForFhir
from diagrams.azure.iot import IotHub
from diagrams.azure.storage import DataLakeStorage
from diagrams.azure.web import APIConnections
from diagrams.custom import Custom
from diagrams.generic.device import Mobile

ICONS = (
    "/Users/toz.ozturk/.claude/plugins/cache/fe-vibe/fe-workflows/"
    "1.5.7/skills/fe-architecture-diagram/resources/icons"
)

DBX_ORANGE = "#FF3621"
DBX_DARK = "#1B3139"
AZURE_BLUE = "#0078D4"
GOV_BORDER = "#92400E"
MUTE = "#6B7280"

graph_attr = {
    "splines": "ortho",
    "nodesep": "0.5",
    "ranksep": "1.0",
    "pad": "0.6",
    "fontsize": "18",
    "fontname": "Helvetica",
    "bgcolor": "white",
    "dpi": "180",
    "rankdir": "LR",
    "compound": "true",
    "labelloc": "t",
}

node_attr = {"fontsize": "11", "fontname": "Helvetica"}
edge_attr = {"fontsize": "10", "fontname": "Helvetica", "color": MUTE}

OUTFILE = (
    "/Users/toz.ozturk/AI-Tools/feos/demos/tower-hamlets-hub/"
    "air-quality-forecasting/architecture/reference-architecture"
)


def build(outformat: str) -> None:
    with Diagram(
        "Tower Hamlets · Air Quality Forecasting & Alerting",
        show=False,
        filename=OUTFILE,
        outformat=outformat,
        direction="LR",
        graph_attr=graph_attr,
        node_attr=node_attr,
        edge_attr=edge_attr,
    ):

        # ---------------- SOURCES ----------------
        with Cluster(
            "Sources · LBTH (Azure)",
            graph_attr={
                "bgcolor": "#EFF6FF",
                "style": "rounded",
                "color": AZURE_BLUE,
                "fontcolor": AZURE_BLUE,
                "fontsize": "13",
                "penwidth": "1.5",
            },
        ):
            adls = DataLakeStorage("ADLS Gen2\nDaily sensor CSV")
            residents = APIForFhir("Resident register\n(nightly extract)")

        # ---------------- DATABRICKS WORKSPACE ----------------
        with Cluster(
            "Azure Databricks · workspace fevm-mbcl",
            graph_attr={
                "bgcolor": "#FFF7F5",
                "style": "rounded",
                "color": DBX_ORANGE,
                "fontcolor": DBX_DARK,
                "fontsize": "15",
                "penwidth": "2.5",
                "labeljust": "l",
            },
        ):
            autoloader = Custom(
                "Auto Loader\nTrigger.AvailableNow",
                f"{ICONS}/databricks/workspace.png",
            )

            with Cluster(
                "Lakeflow Spark Declarative Pipeline · Medallion",
                graph_attr={
                    "bgcolor": "#FDECEA",
                    "style": "rounded",
                    "color": DBX_ORANGE,
                    "fontcolor": DBX_DARK,
                    "fontsize": "12",
                },
            ):
                bronze = Custom("Bronze\nraw_readings", f"{ICONS}/databricks/delta_lake.png")
                silver = Custom(
                    "Silver\nreadings_typed\n+ DQ expectations",
                    f"{ICONS}/databricks/delta_lake.png",
                )
                gold = Custom(
                    "Gold\nward_daily_ts\nresidents · subscriptions",
                    f"{ICONS}/databricks/delta_lake.png",
                )
                bronze >> Edge(color=DBX_ORANGE, penwidth="2.2") >> silver
                silver >> Edge(color=DBX_ORANGE, penwidth="2.2") >> gold

            with Cluster(
                "Multi-model bake-off (parallel)",
                graph_attr={
                    "bgcolor": "#FEF3C7",
                    "style": "rounded",
                    "color": "#B45309",
                    "fontcolor": "#B45309",
                    "fontsize": "12",
                },
            ):
                m_naive = Custom("SeasonalNaive", f"{ICONS}/databricks/model_serving.png")
                m_prophet = Custom("Prophet", f"{ICONS}/databricks/model_serving.png")
                m_arima = Custom(
                    "StatsForecast\nAutoARIMA",
                    f"{ICONS}/databricks/model_serving.png",
                )
                m_ets = Custom(
                    "StatsForecast\nAutoETS",
                    f"{ICONS}/databricks/model_serving.png",
                )

            with Cluster(
                "Train · register · score",
                graph_attr={
                    "bgcolor": "#FDECEA",
                    "style": "rounded",
                    "color": DBX_ORANGE,
                    "fontcolor": DBX_DARK,
                    "fontsize": "12",
                },
            ):
                mlflow = Custom("MLflow\nshared experiment", f"{ICONS}/databricks/workspace.png")
                registry = Custom(
                    "UC Model Registry\nward_champion",
                    f"{ICONS}/databricks/unity_catalog.png",
                )
                score = Custom(
                    "Score job\nchampion-per-ward",
                    f"{ICONS}/databricks/workspace.png",
                )
                alerts = Custom("gold_alerts\n(Delta)", f"{ICONS}/databricks/delta_lake.png")
                mlflow >> Edge(color=DBX_ORANGE, penwidth="2.2") >> registry
                registry >> Edge(color=DBX_ORANGE, penwidth="2.2") >> score
                score >> Edge(color=DBX_ORANGE, penwidth="2.2") >> alerts

            # Pipeline → bake-off → MLflow
            autoloader >> Edge(color=DBX_ORANGE, penwidth="2.2") >> bronze
            for m in (m_naive, m_prophet, m_arima, m_ets):
                gold >> Edge(color=MUTE, style="dashed") >> m
                m >> Edge(color="#B45309") >> mlflow
            gold >> Edge(color=DBX_ORANGE, penwidth="2.2", style="bold", xlabel="features") >> score

        # ---------------- ALERT EGRESS ----------------
        with Cluster(
            "Alert egress · Azure",
            graph_attr={
                "bgcolor": "#EFF6FF",
                "style": "rounded",
                "color": AZURE_BLUE,
                "fontcolor": AZURE_BLUE,
                "fontsize": "13",
                "penwidth": "1.5",
            },
        ):
            webhook = APIConnections("Webhook\nHTTPS + basic auth")
            func = FunctionApps("Azure Function\nsign · dedupe · MI")
            acs = IotHub("Azure Communication\nServices · SMS")
            webhook >> Edge(color=AZURE_BLUE, penwidth="2.2") >> func
            func >> Edge(color=AZURE_BLUE, penwidth="2.2") >> acs

        # ---------------- DESTINATION ----------------
        resident = Mobile("Resident mobile")

        # ---------------- GOVERNANCE BAND ----------------
        with Cluster(
            "Unity Catalog · governance · lineage · column masks · tags · audit",
            graph_attr={
                "bgcolor": "#FDE68A",
                "style": "rounded",
                "color": GOV_BORDER,
                "fontcolor": GOV_BORDER,
                "fontsize": "13",
                "penwidth": "1.5",
            },
        ):
            uc = Custom(
                "Unity Catalog",
                f"{ICONS}/databricks/unity_catalog.png",
            )

        # Cross-cluster wires
        adls >> Edge(color=AZURE_BLUE, penwidth="2.2", xlabel="daily upload") >> autoloader
        residents >> Edge(color=AZURE_BLUE, style="dashed", xlabel="nightly") >> gold
        alerts >> Edge(color=DBX_ORANGE, penwidth="2.2") >> webhook
        acs >> Edge(color=AZURE_BLUE, penwidth="2.2", xlabel="SMS") >> resident

        # Governance dotted overlays (drawn last so they don't influence main layout much)
        for tgt in (silver, gold, registry, alerts):
            uc >> Edge(style="dotted", color=GOV_BORDER, arrowhead="none", constraint="false") >> tgt


if __name__ == "__main__":
    for fmt in ("png", "svg"):
        build(fmt)
    print("done")
