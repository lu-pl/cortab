"""rdfdf rules for corpusTable transformations."""

import functools
import math

from collections.abc import Iterable, MutableMapping

import langcodes

from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, OWL, SKOS
from shortuuid import uuid

from clsns import crm, crmcls, clst

from lodkit import importer
from vocabs.corpusType import corpusType_skos
from vocabs.literaryGenre import literaryGenre_skos


def name_rule() -> Graph:
    """Rule for corpusName field conversion.

    Also basic setup for corpus identifier/descevent/protodoc.
    """
    base_ns = Namespace(f"https://{__subject__.lower()}.clscor.io/entity/")
    name_appellation = base_ns[f"appellation/{uuid()}"]
    literal_name = Literal(__object__.strip())
    full_title = base_ns["type/appellation_type/full_title"]
    protodoc_uri =  base_ns[f"protodoc/{uuid()}"]

    descevent_uri = base_ns[f"descevent/{uuid()}"]

    __store__.update(
        {
            "base_ns": base_ns,
            "literal_name": literal_name,
            "descevent": descevent_uri,
            "protodoc_uri": protodoc_uri
        }
    )

    triples = [
        # corpus identifier
        (
            base_ns["corpus"],
            RDF.type,
            crmcls["X1_Corpus"]
        ),
        # corpus protodoc
        (
            protodoc_uri,
            RDF.type,
            crmcls["X11_Prototypical_Document"]
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
    """Rule for corpusAcronym field conversion."""

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
    """Rule for corpusLink field conversion."""

    base_ns = __store__["base_ns"]
    link_uri = URIRef(__object__)
    link_literal = Literal(__object__, datatype=XSD.anyURI)
    descevent_uri = __store__["descevent"]
    descevent_timespan_uri = clst[f"timespan/{uuid()}"]
    __store__["descevent_timespan_uri"] = descevent_timespan_uri
    protodoc_uri = __store__["protodoc_uri"]

    # persons
    vera = clst["person/vera-charvat"]
    michal = clst["person/michal-mrugalski"]
    susanne = clst["person/susanne-zhanial"]

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
            crm["P135_created_type"],
            protodoc_uri
        ),
        (
            descevent_uri,
            crm["P14_carried_out_by"],
            vera
        ),
        (
            descevent_uri,
            crm["P14_carried_out_by"],
            michal
        ),
        (
            descevent_uri,
            crm["P14_carried_out_by"],
            susanne
        ),
        (
            vera,
            RDF.type,
            crm["E39_Actor"]
        ),
        (
            michal,
            RDF.type,
            crm["E39_Actor"]
        ),
        (
            susanne,
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
    """Rule for corpusLanguage field conversion."""
    language_values = map(str.strip, __object__.split(","))

    # base_ns = __store__["base_ns"]
    base_ns = Namespace(f"https://{__subject__.lower()}.clscor.io/entity/")

    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = __store__["descevent"]
    protodoc_uri = __store__["protodoc_uri"]

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
            clst["type/eval_type/uncertain"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def textcount_rule() -> Graph:
    "Rule for corpusTextCount field conversion."
    base_ns = __store__["base_ns"]

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
            attrassign_uri,
            crm["P2_has_type"],
            clst["type/eval_type/uncertain"]
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
    """Rule for corpusWordCount field conversion."""

    # TODO: make this a decorator
    if math.isnan(__object__):
        return None

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

def timespan_rule():
    """Rule for corpusTimespan field conversion."""
    base_ns = __store__["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = __store__["descevent"]
    descevent_timespan_uri = __store__["descevent_timespan_uri"]

    triples = [
        (
            attrassign_uri,
            RDF.type,
            crm["E13_Attribute_assignment"]
        ),
        (
            attrassign_uri,
            crm["P134_continued"],
            descevent_uri
        ),
        (
            attrassign_uri,
            crm["P140_assigned_attribute_to"],
            base_ns["corpus"]
        ),
        (
            attrassign_uri,
            crm["P177_assigned_property_of_type"],
            crm["P4_has_time-span"]
        ),
        (
            attrassign_uri,
            crm["P141_assigned"],
            descevent_timespan_uri
        ),
        (
            attrassign_uri,
            crm["P2_has_type"],
            clst["type/eval_type/uncertain"]
        ),
        (
            descevent_timespan_uri,
            RDF.type,
            crm["E52_Time-Span"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


## TODO: logic for actual formats (not just TEI, see corpusTable)
def format_rule():
    """Rule for corpusFormat field conversion."""
    base_ns = __store__["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri  = __store__["descevent"]
    protodoc_uri = __store__["protodoc_uri"]
    tei_type_uri = clst["type/format/tei"]
    # divergent from SweDracor example
    tei_standard_uri = URIRef("https://vocabs.sshopencloud.eu/vocabularies/standard/tei")

    triples = [
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
            crmcls["Z4_has_format"]
        ),
        (
            attrassign_uri,
            crm["P141_assigned"],
            tei_type_uri
        ),
        (
            attrassign_uri,
            crm["P2_has_type"],
            clst["type/eval_type/uncertain"]
        ),
        # general TODO: bulk-convert types/vocabs
        (
            tei_type_uri,
            RDF.type,
            crmcls["X7_Format"]
        ),
        (
            tei_type_uri,
            RDFS.label,
            Literal("TEI", lang="en")
        ),
        (
            tei_type_uri,
            OWL.sameAs,
            tei_standard_uri
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


## unclear mapping
# def annotation_rule():
#     """Rule for corpusAnnotation field."""
#     base_ns = __store__["base_ns"]
#     attrassign_uri = base_ns[f"attrassign/{uuid()}"]
#     protodoc_uri = __store__["protodoc_uri"]

#     triples = [
#         (
#             attrassign_uri,
#             RDF.type,
#             crm["E13_Attribute_Assignment"]
#         ),
#         (
#             attrassign_uri,
#             crm["P140_assigned_attribute_to"],
#             protodoc_uri
#         ),
#         (
#             attrassign_uri,
#             crm["P177_assigned_property_of_type"],
#             crmcls["Z3_document_exhibits_feature"]
#         ),
#         (
#             attrassign_uri,
#             crm["P141_assigned"],
#             ...
#         )
#     ]

#     graph = Graph()

#     for triple for triples:
#         graph.add(triple)

#     return graph


def type_rule():
    """Rule for corpusType/corpusType_consolidatedVocab field conversion."""
    base_ns = __store__["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = __store__["descevent"]

    corpus_type_uri = next(
        corpusType_skos.subjects(
            SKOS.prefLabel,
            Literal(__object__, lang="en")
        )
    )

    triples = [
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
            base_ns["corpus"]
        ),
        (
            attrassign_uri,
            crm["P177_assigned_property_of_type"],
            crmcls["Z8_corpus_has_corpus_type"]
        ),
        (
            attrassign_uri,
            crm["P141_assigned"],
            corpus_type_uri
        ),
        (
            attrassign_uri,
            crm["P2_has_type"],
            clst["type/eval_type/uncertain"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def genre_rule():
    """Rule for corpusLiteraryGenre field conversion."""
    base_ns = __store__["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = __store__["descevent"]

    def genre_triples():
        """Lookup genre URIs in vocabs and yield P141 statement."""
        for genre in __object__.split(", "):
            genre = genre.strip()

            corpus_genre_uri = next(
                literaryGenre_skos.subjects(
                    SKOS.prefLabel,
                    Literal(genre, lang="en")
                )
            )

            yield (
                attrassign_uri,
                crm["P141_assigned"],
                corpus_genre_uri
            )

    triples = [
        *genre_triples(),
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
            base_ns["corpus"]
        ),
        (
            attrassign_uri,
            crm["P177_assigned_property_of_type"],
            crm["Z6_document_has_literary_genre"]
        ),
        (
            attrassign_uri,
            crm["P2_has_type"],
            base_ns["type/eval_type/uncertain"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def api_rule():
    """Rule for corpusAPI field conversion."""
    base_ns = __store__["base_ns"]
    api_uri = URIRef(__object__)

    triples = [
        (
            base_ns["corpus"],
            crm["P1_is_identified_by"],
            api_uri
        ),
        (
            api_uri,
            RDF.type,
            crm["E42_Identifier"]
        ),
        (
            api_uri,
            crm["P2_has_type"],
            clst["type/link_type/api"]
        ),
        (
            api_uri,
            RDF.value,
            Literal(f"API for the {__store__['literal_name']} website.")
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def licence_rule():
    """Rule for corpusLicence field conversion."""
    base_ns = __store__["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = __store__["descevent"]
    protodoc_uri = __store__["protodoc_uri"]
    licence_uri = URIRef(__object__.strip())

    triples = [
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
            crm["Z7_license_type"]
        ),
        (
            attrassign_uri,
            crm["P141_assigned"],
            licence_uri
        ),
        (
            attrassign_uri,
            crm["P2_has_type"],
            clst["type/eval_type/uncertain"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def addlink_rule():
    """Rule for additionalLink field conversion."""
    base_ns = __store__["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    addlink_uri = URIRef(__object__.strip())

    descevent_uri = __store__["descevent"]
    protodoc_uri = __store__["protodoc_uri"]


    triples = [
        (
            attrassign_uri,
            crm["P1_is_identified_by"],
            addlink_uri
        ),
        (
            addlink_uri,
            RDF.type,
            crm["E42_Identifier"]
        ),
        (
            addlink_uri,
            crm["P2_has_type"],
            clst["type/link_type/data-repository"]
        ),
        (
            addlink_uri,
            RDF.value,
            Literal(f"Additional link for the {__store__['literal_name']} resource.")
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def addinfo_rule():
    """Rule for corpusInfo field conversion."""
    base_ns = __store__["base_ns"]
    info = Literal(__object__.strip())

    triples = [
        (
            base_ns["corpus"],
            crm["P3_has_note"],
            info
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


rules = {
    "corpusName": name_rule,
    "corpusAcronym": acronym_rule,
    "corpusLink": link_rule,
    "corpusLanguage": language_rule,
    "corpusTextCount": textcount_rule,
    "corpusWordCount": wordcount_rule,
    "corpusTimespan": timespan_rule,
    "corpusFormat/Schema": format_rule,
    # TODO:
    # "corpusAnnotation": annotation_rule,
    # corpusType
    "corpusType_consolidatedVocab": type_rule,
    # corpusLiteraryGenre
    "corpusLiteraryGenre_consolidatedVocab": genre_rule,
    "corpusAPI": api_rule,
    # corpusLicence
    "corpusLicence_consolidatedVocab": licence_rule,
    "additionalLink": addlink_rule,
    # additionalInfo
    "additionalInfo / commentary": addinfo_rule,
}
