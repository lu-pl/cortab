"""rdfdf rules for corpusTable transformations."""

from collections.abc import Iterable, MutableMapping

import langcodes

from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS
from shortuuid import uuid

from cortab.helpers.lang_utils import construct_lang_uris

# namespaces
crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
crmcls = Namespace("https://clscor.io/ontologies/CRMcls/")


def name_rule() -> Graph:
    """Rule for corpusName."""

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


def acronym_rule() -> Graph:
    """Rule for corpusAcronym."""

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


def link_rule() -> Graph:
    """Rule for corpusLink."""

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


def language_rule() -> Graph:
    """Rule for corpusLanguage."""
    language_values = map(str.strip, __object__.split(","))

    # base_ns = __store__["base_ns"]
    base_ns = Namespace(f"https://{__subject__.lower()}.clscor.io/entity/")

    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = base_ns[f"descevent/{uuid()}"]
    protodoc_uri = base_ns[f"protodoc/{uuid()}"]

    iso_language_ns = Namespace("https://vocabs.acdh.oeaw.ac.at/iso6391/")

    __store__["langs"] = {}

    # language triples
    def generate_lang_uris():
        """Generate languages triples."""
        for language_value in language_values:
            language_value = language_value.strip() # todo: properly sanitize

            try:
                lang_uri = __store__["langs"][language_value]
            except KeyError:
                try:
                    _lang_iso = langcodes.find(language_value).to_tag()
                    _lang_uri = iso_language_ns[_lang_iso]
                except LookupError:
                    _lang_uri = base_ns[f"language/{uuid()}"]

            lang_uri = URIRef(_lang_uri)

            # store the language uri and yield language triples
            __store__["langs"][language_value] = lang_uri

            yield from [
                (lang_uri, RDF.type, crm["E56_Language"]),
                (lang_uri, RDFS.label, Literal(language_value)),
                (attrassign_uri, crm["P141_assigned"], lang_uri)
            ]

    # E13 triples
    triples = [
        *generate_lang_uris(),
        (
            attrassign_uri,
            RDF.type,
            crm["E13_Attribute_Assignment"]
        ),
        (
            attrassign_uri,
            crm["P134_continued"],
            descevent_uri
        ),
        (
            attrassign_uri,
            crm["P140_assigned_attribute_to"],
            protodoc_uri
        ),
        (
            attrassign_uri,
            crm["P177_assigned_property_of_type"],
            crm["P72_has_Language"]
        ),
        (
            attrassign_uri,
            crm["P2_has_type"],
            URIRef("https://core.clscor.io/entity/type/eval_type/uncertain")
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


rules = {
    # "corpusName": name_rule,
    # "corpusAcronym": acronym_rule,
    # "corpusLink": link_rule,

    "corpusLanguage": language_rule,
    ## without instance example yet
    # "corpusTextCount": text_count_rule,


}
