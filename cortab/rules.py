"""rdfdf rules for corpusTable transformations."""

from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF
from shortuuid import uuid


# namespaces
crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
crmcls = Namespace("https://clscor.io/ontologies/CRMcls/")


def name_rule():
    """rdfdf rule for corpusName."""

    base_ns = Namespace(f"https://{__subject__.lower()}.clscor.io/entity/")
    name_appellation = base_ns[f"appellation/{uuid()}"]
    literal_name = Literal(__object__.strip())
    full_title = base_ns["type/appellation_type/full_title"]

    __store__.update(
        {
            "base_ns": base_ns,
            "literal_name": literal_name
        }
    )

    triples = [
        (
            base_ns["corpus"],
            RDF.type,
            crmcls["X1_Corpus"]
        ),
        (
            base_ns["corpus"],
            crm["P1_is_identified_by"],
            name_appellation
        ),
        (
            name_appellation,
            RDF.type,
            crm["E41_Appellation"]
        ),
        (
            name_appellation,
            crm["crm:P2_has_type"],
            full_title
        ),
        (
            name_appellation,
            RDF.value,
            literal_name
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def acronym_rule():
    """rdfdf rule for corpusAcronym."""

    base_ns = __store__["base_ns"]
    acronym_appellation = base_ns[f"appellation/{uuid()}"]

    triples = [
        (
            base_ns["corpus"],
            crm["P1_is_identified_by"],
            acronym_appellation
        ),
        (
            acronym_appellation,
            RDF.type,
            crm["E41_Appellation"]
        ),
        (
            acronym_appellation,
            crm["P2_has_type"],
            base_ns["type/appellation_type/acronym"]
        ),
        (
            acronym_appellation,
            RDF.value,
            Literal(__object__.strip())
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def link_rule():
    """rdfdf rule for corpusLink."""

    base_ns = __store__["base_ns"]
    link = URIRef(__object__)

    triples = [
        (
            base_ns["corpus"],
            crm["P1_is_identified_by"],
            link
        ),
        (
            link,
            RDF.type,
            crm["E42_Identifier"]
        ),
        (
            link,
            crm["P2_has_type"],
            base_ns["type/link_type/project-website"]
        ),
        (
            link,
            RDF.value,
            Literal(f"Link to the {__store__['literal_name']} website.")
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph



rules = {
    "corpusName": name_rule,
    "corpusAcronym": acronym_rule,
    "corpusLink": link_rule
}
