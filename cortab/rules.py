"""rdfdf rules for corpusTable transformations."""

from collections.abc import Iterable, MutableMapping

import langcodes

from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD
from shortuuid import uuid

# namespaces
crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
crmcls = Namespace("https://clscor.io/ontologies/CRMcls/")
clst = Namespace("https://core.clscor.io/entity/")


def name_rule() -> Graph:
    """Rule for corpusName field + corpus identifier/descevent."""
    base_ns = Namespace(f"https://{__subject__.lower()}.clscor.io/entity/")
    name_appellation = base_ns[f"appellation/{uuid()}"]
    literal_name = Literal(__object__.strip())
    full_title = base_ns["type/appellation_type/full_title"]

    descevent_uri = base_ns[f"descevent/{uuid()}"]

    __store__.update(
        {
            "base_ns": base_ns,
            "literal_name": literal_name,
            "descevent": descevent_uri
        }
    )

    triples = [
        # corpus identifier
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
            crm["P2_has_type"],
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
    """Rule for corpusAcronym field."""

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
    """Rule for corpusLink field."""

    base_ns = __store__["base_ns"]
    link_uri = URIRef(__object__)
    link_literal = Literal(__object__, datatype=XSD.anyURI)
    descevent_uri = __store__["descevent"]
    descevent_timespan_uri = clst[f"timespan/{uuid()}"]
    vera = clst["person/vera-charvat"]

    link_triples = [
        (
            base_ns["corpus"],
            crm["P1_is_identified_by"],
            link_uri
         ),
        (
            link_uri,
            RDF.type,
            crm["E42_Identifier"]
        ),
        (
            link_uri,
            crm["P2_has_type"],
            base_ns["type/link_type/project-website"]
        ),
        (
            link_uri,
            RDF.value,
            Literal(f"Link to the {__store__['literal_name']} website.")
        )
    ]

    description_triples = [
        (
            descevent_uri,
            RDF.type,
            crmcls["X9_Corpus_Description"]
        ),
        (
            descevent_uri,
            crm["P16_used_specific_object"],
            link_literal
        ),
        (
            descevent_uri,
            crm["P14_carried_out_by"],
            vera
        ),
        (
            vera,
            RDF.type,
            crm["E39_Actor"]
        ),
        (
            descevent_uri,
            crm["P4_has_time-span"],
            descevent_timespan_uri
        ),
        (
            descevent_timespan_uri,
            RDF.type,
            crm["E52_Time-Span"]
        ),
        # timespan cidoc outdated?
        (
            descevent_timespan_uri,
            crm["P81a_end_of_the_begin"],
            Literal("2022-04-14", datatype=XSD.date)
        ),
        (
            descevent_timespan_uri,
            crm["P81b_begin_of_the_end"],
            Literal("2022-06-15", datatype=XSD.date)
        )
    ]

    graph = Graph()

    for triple in [*link_triples, *description_triples]:
        graph.add(triple)

    return graph


def language_rule() -> Graph:
    """Rule for corpusLanguage field."""
    language_values = map(str.strip, __object__.split(","))

    # base_ns = __store__["base_ns"]
    base_ns = Namespace(f"https://{__subject__.lower()}.clscor.io/entity/")

    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = __store__["descevent"]
    protodoc_uri = base_ns[f"protodoc/{uuid()}"]

    iso_language_ns = Namespace("https://vocabs.acdh.oeaw.ac.at/iso6391/")

    __store__["langs"] = {}

    # language triples
    def generate_lang_uris() -> Graph:
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
            clst["uncertain"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def textcount_rule() -> Graph:
    "Rule for corpusTextCount field."
    # base_ns = __store__["base_ns"]
    base_ns = Namespace(f"https://{__subject__.lower()}.clscor.io/entity/")

    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    dimension_uri = base_ns[f"dim/{uuid()}"]


    triples = [
        (
            attrassign_uri,
            RDF.type,
            crm["E13_Attibute_Assignment"]
        ),
        (
            attrassign_uri,
            crm["P134_continued"],
            __store__["descevent"]
        ),
        (
            attrassign_uri,
            crm["P140_assigned_attribute_to"],
            base_ns["corpus"]
        ),
        (
            attrassign_uri,
            crm["P141_assigned"],
            dimension_uri
        ),
        (
            dimension_uri,
            RDF.type,
            crm["E54_Dimension"]
        ),
        (
            dimension_uri,
            crm["P90_has_value"],
            Literal(f"{int(__object__)}", datatype=XSD.integer)
        ),
        (
            dimension_uri,
            crm["P91_has_unit"],
            clst["type/dimension_type/texts"]
        ),
        (
            clst["type/dimension_type/texts"],
            RDF.type,
            crm["E58_Measurement_Unit"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def wordcount_rule() -> Graph:
    "Rule for corpusWordCount field."
    # base_ns = __store__["base_ns"]
    base_ns = Namespace(f"https://{__subject__.lower()}.clscor.io/entity/")

    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    corpus_uri = base_ns["corpus"]
    dimension_uri = base_ns[f"dim/{uuid()}"]


    triples = [
        (
            attrassign_uri,
            RDF.type,
            crm["E13_Attribute_assignment"]
        ),
        (
            attrassign_uri,
            crm["P134_continued"],
            __store__["descevent"]
        ),
        (
            attrassign_uri,
            crm["P140_assigned_attribute_to"],
            corpus_uri
        ),
        (
            attrassign_uri,
            crm["P141_assigned"],
            dimension_uri
        ),
        (
            dimension_uri,
            RDF.type,
            crm["E54_Dimension"]
        ),
        (
            dimension_uri,
            crm["P90_has_value"],
            Literal(f"{int(__object__)}", datatype=XSD.integer)
        ),
        (
            dimension_uri,
            crm["P91_has_unit"],
            clst["type/dimension_type/words"]
        ),
        (
            clst["type/dimension_type/words"],
            RDF.type,
            crmcls["X3_Feature"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


rules = {
    "corpusName": name_rule,
    # "corpusAcronym": acronym_rule,
    # "corpusLink": link_rule,
    # "corpusLanguage": language_rule,
    # "corpusTextCount": textcount_rule,
    # "corpusWordCount": wordcount_rule,

    # "corpusTimespan": timespan_rule,
    # "corpusFormat": format_rule,
    # "corpusAnnotation": annotation_rule,
    # "corpusType": type_rule,
}
