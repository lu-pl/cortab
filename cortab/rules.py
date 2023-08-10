"""rdfdf rules for corpusTable transformations."""

import langcodes
import toolz

from collections.abc import Generator

from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, SKOS
from shortuuid import uuid

from clisn import crm, crmcls, clst

from helpers.cortab_utils import skip_nan

# graph imports
from lodkit import importer
# from cortab.vocabs import (

# )

from lodkit.types import _Triple


def name_rule(subject_field, object_field, store) -> Graph:
    """Rule for corpusName field conversion.

    Also basic setup for corpus identifier/descevent/protodoc.
    """
    base_ns = Namespace(f"https://{subject_field.lower()}.clscor.io/entity/")
    name_appellation = base_ns[f"appellation/{uuid()}"]
    literal_name = Literal(object_field.strip())
    full_title = base_ns["type/appellation_type/full_title"]
    protodoc_uri = base_ns[f"protodoc/{uuid()}"]

    descevent_uri = base_ns[f"descevent/{uuid()}"]

    store.update(
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


def acronym_rule(subject_field, object_field, store) -> Graph:
    """Rule for corpusAcronym field conversion."""
    base_ns = store["base_ns"]
    acronym_appellation = base_ns[f"appellation/{uuid()}"]
    store["acronym_appellation"] = acronym_appellation

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
            Literal(object_field)
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def acronym_comment_rule(subject_field, object_field, store):
    """Rule for corpusAcronym_comments."""
    acronym_appellation = store["acronym_appellation"]

    graph = Graph()

    graph.add(
        (
            acronym_appellation,
            RDFS.comment,
            Literal(object_field)
        )
    )

    return graph


def link_rule(subject_field, object_field, store) -> Graph:
    """Rule for corpusLink field conversion."""
    base_ns = store["base_ns"]
    link_uri = URIRef(object_field)
    link_literal = Literal(object_field.strip(), datatype=XSD.anyURI)
    descevent_uri = store["descevent"]
    descevent_timespan_uri = clst[f"timespan/{uuid()}"]
    store["descevent_timespan_uri"] = descevent_timespan_uri
    protodoc_uri = store["protodoc_uri"]

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
            Literal(f"Link to the {store['literal_name']} website.")
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


def language_rule(subject_field, object_field, store) -> Graph:
    """Rule for corpusLanguage field conversion."""
    language_values = map(str.strip, object_field.split(","))

    base_ns = store["base_ns"]

    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = store["descevent"]
    protodoc_uri = store["protodoc_uri"]

    iso_language_ns = Namespace("https://vocabs.acdh.oeaw.ac.at/iso6391/")

    store["langs"] = {}

    # language triples
    def generate_lang_uris() -> Generator[_Triple, None, None]:
        """Generate language triples."""
        for language_value in language_values:
            language_value = language_value.strip()  # todo: properly sanitize

            try:
                lang_uri = store["langs"][language_value]
            except KeyError:
                try:
                    _lang_iso = langcodes.find(language_value).to_tag()
                    _lang_uri = iso_language_ns[_lang_iso]
                except LookupError:
                    _lang_uri = base_ns[f"language/{uuid()}"]

            lang_uri = URIRef(_lang_uri)

            # store the language uri and yield language triples
            store["langs"][language_value] = lang_uri

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


def textcount_rule(subject_field, object_field, store) -> Graph:
    """Rule for corpusTextCount field conversion."""
    base_ns = store["base_ns"]

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
            store["descevent"]
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
            Literal(f"{int(object_field)}", datatype=XSD.integer)
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


def wordcount_rule(subject_field, object_field, store) -> Graph:
    """Rule for corpusWordCount field conversion."""
    base_ns = store["base_ns"]

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
            store["descevent"]
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
            Literal(f"{int(object_field)}", datatype=XSD.integer)
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


def timespan_rule(subject_field, object_field, store):
    """Rule for corpusTimespan field conversion."""
    base_ns = store["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = store["descevent"]
    descevent_timespan_uri = store["descevent_timespan_uri"]

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
        ),
        (
            descevent_timespan_uri,
            RDFS.label,
            Literal(object_field)
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def format_rule(subject_field, object_field, store):
    """Rule for corpusFormat field conversion."""
    base_ns = store["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = store["descevent"]
    protodoc_uri = store["protodoc_uri"]
    tei_type_uri = clst["type/format/tei"]

    format_values = map(str.strip, object_field.split(","))

    def format_triples():
        for format_value in format_values:
            format_value_uri = next(
                formats_crmcls.subjects(
                    RDFS.label,
                    Literal(format_value, lang="en")
                )
            )

            yield (
                attrassign_uri,
                crm["P141_assigned"],
                format_value_uri
            )

    triples = [
        *format_triples(),
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
            crm["P2_has_type"],
            clst["type/eval_type/uncertain"]
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


# unclear mapping
# def annotation_rule():
#     """Rule for corpusAnnotation field."""
#     base_ns = store["base_ns"]
#     attrassign_uri = base_ns[f"attrassign/{uuid()}"]
#     protodoc_uri = store["protodoc_uri"]

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


def type_rule(subject_field, object_field, store):
    """Rule for corpusType/corpusType_consolidatedVocab field conversion."""
    base_ns = store["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = store["descevent"]

    corpus_type_uri = next(
        corpusType_skos.subjects(
            SKOS.prefLabel,
            Literal(object_field, lang="en")
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


def genre_rule(subject_field, object_field, store):
    """Rule for corpusLiteraryGenre field conversion."""
    base_ns = store["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = store["descevent"]

    def genre_triples():
        """Lookup genre URIs in vocabs and yield P141 statement."""
        for genre in object_field.split(","):
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


def api_rule(subject_field, object_field, store):
    """Rule for corpusAPI field conversion."""
    base_ns = store["base_ns"]
    api_uri = URIRef(object_field.strip())

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
            Literal(f"API for the {store['literal_name']} website.")
        )
    ]

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def licence_rule(subject_field, object_field, store):
    """Rule for corpusLicence field conversion."""
    base_ns = store["base_ns"]
    attrassign_uri = base_ns[f"attrassign/{uuid()}"]
    descevent_uri = store["descevent"]
    protodoc_uri = store["protodoc_uri"]
    licence_uri = URIRef(object_field.strip())

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


def addlink_rule(subject_field, object_field, store):
    """Rule for additionalLink field conversion."""

    def addlink_triples():
        """..."""
        for addlink in map(str.strip, object_field.split(",")):
            base_ns = store["base_ns"]
            attrassign_uri = base_ns[f"attrassign/{uuid()}"]
            addlink_uri = URIRef(addlink)

            yield from [
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
                    Literal(
                        f"Additional link for the {store['literal_name']} resource.")
                )
            ]

    graph = Graph()

    for triple in addlink_triples():
        graph.add(triple)

    return graph


def addinfo_rule(subject_field, object_field, store):
    """Rule for corpusInfo field conversion."""
    base_ns = store["base_ns"]
    # info = Literal(object_field.strip())
    info = Literal(object_field)

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


_rules = {
    "corpusName": name_rule,
    "corpusAcronym": acronym_rule,
    "corpusAcronym_comments": acronym_comment_rule,
    "corpusLink": link_rule,
    "corpusLanguage": language_rule,
    "corpusTextCount": textcount_rule,
    "corpusWordCount": wordcount_rule,
    "corpusTimespan": timespan_rule,
    # "corpusFormat/Schema_consolidatedVocab": format_rule,
    "corpusFormat/Schema": format_rule,
    # "corpusAnnotation": annotation_rule,
    # corpusType
    "corpusType_consolidatedVocab": type_rule,
    # corpusLiteraryGenre
    "corpusLiteraryGenre_consolidatedVocab": genre_rule,
    "corpusAPI": api_rule,
    # corpusLicence
    # "corpusLicence_link": licence_rule,
    "additionalLink": addlink_rule,
    # additionalInfo
    "additionalInfo / commentary": addinfo_rule,
}

rules = toolz.dicttoolz.valmap(skip_nan, _rules)
