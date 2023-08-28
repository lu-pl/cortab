"""Defines a row_rule callable for a tabulardf.RowGraphConverter."""

import itertools
import functools
import pandas as pd

from typing import Generator, Mapping

from helpers.cortab_utils import (  # noqa: F401
    genhash,
    vocabs_lookup,
    remove_nan,
    nan_handler
)

from clisn import (
    clscore,
    crm,
    crmcls,
    corpus_base,
    CLSInfraNamespaceManager
)

import langcodes

from lodkit.utils import plist
from lodkit.types import _Triple

from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import RDF, RDFS, XSD

from tabulardf import RowGraphConverter

from lodkit import importer
from vocabs import (
    appellation_type,
    corpus_type,
    feature,
    format as format_vocab,
    licenses,
    link_type,
    literary_genre,
    method
)


TripleGenerator = Generator[_Triple, None, None]


def corpustable_row_rule(row_data: Mapping) -> Graph:
    """row_rule for RowGraphConverter."""
    # URIs
    base_ns = corpus_base(row_data["corpusAcronym"])
    corpus_uri = base_ns["corpus"]
    corpus_link_uri = URIRef(row_data["corpusLink"])
    descevent_uri = base_ns["descevent/1"]
    protodoc_uri = base_ns["protodoc/1"]

    timespan_uri_1 = base_ns["timespan/1"]

    corpus_appellation_uri_1 = base_ns["appellation/1"]
    corpus_appellation_uri_2 = base_ns["appellation/2"]

    attribute_assignment_uri_1 = base_ns["attrassign/1"]
    attribute_assignment_uri_2 = base_ns["attrassign/2"]
    attribute_assignment_uri_3 = base_ns["attrassign/3"]
    attribute_assignment_uri_4 = base_ns["attrassign/4"]
    attribute_assignment_uri_5 = base_ns["attrassign/5"]
    attribute_assignment_uri_6 = base_ns["attrassign/6"]
    attribute_assignment_uri_7 = base_ns["attrassign/7"]

    dimension_uri_1 = base_ns["dimension/1"]

    # triples
    def person_triples() -> TripleGenerator:
        person_names = map(
            str.strip,
            row_data["editor/contributor"].split(",")
        )

        for person_name in person_names:
            person_uri = clscore[f"person/{genhash(person_name)}"]
            yield from (
                (person_uri, RDF.type, crm["E39_Actor"]),
                (person_uri, RDFS.label, Literal(person_name)),
                (descevent_uri, crm["P14_carried_out_by"], person_uri)
            )

    def descevent_triples() -> TripleGenerator:
        yield from [
            (corpus_uri, RDF.type, crmcls["X1_Corpus"]),
            (protodoc_uri, RDF.type, crmcls["X11_Prototypical_Document"]),
            # descevent
            *plist(
                descevent_uri,
                (RDF.type, crmcls["X9_Corpus_Description"]),
                # (crm["P16_used_specific_object"], Literal(row_data["corpusLink"])),
                (crm["P135_created_type"], protodoc_uri),
                (crm["P4_has_time-span"], clscore["timespan/1"]),
                (crm["P3_has_note"], Literal(row_data["additionalInfo / commentary"]))
            ),
            # timespan
            *plist(
                timespan_uri_1,
                (RDF.type, crm["E52_Time-Span"]),
                (
                    crm["81a_end_of_the_begin"],
                    Literal(row_data["descEvent_start"], datatype=XSD.date)
                ),
                (
                    crm["P81b_begin_of_the_end"],
                    Literal(row_data["descEvent_end"], datatype=XSD.date)
                )
            )
        ]

    def corpus_name_triples() -> TripleGenerator:
        yield from [
            (corpus_uri, crm["P1_is_identified_by"], corpus_appellation_uri_1),
            *plist(
                corpus_appellation_uri_1,
                (RDF.type, crm["E41_Appellation"]),
                (crm["P2_has_type"], vocabs_lookup(appellation_type, "full title")),
                (RDF.value, Literal(row_data["corpusName"]))
            )
        ]

    def corpus_acronym_triples() -> TripleGenerator:
        yield from [
            (corpus_uri, crm["P1_is_identified_by"], corpus_appellation_uri_2),
            *plist(
                corpus_appellation_uri_2,
                (RDF.type, crm["E41_Appellation"]),
                (crm["P2_has_type"], vocabs_lookup(appellation_type, "acronym")),
                (RDF.value, Literal(row_data["corpusAcronym"]))
            )
        ]

    def corpus_link_triples() -> TripleGenerator:
        yield from [
            (corpus_uri, crm["P1_is_identified_by"], corpus_link_uri),
            *plist(
                corpus_link_uri,
                (RDF.type, crm["E42_Identifier"]),
                (crm["P2_has_type"], vocabs_lookup(link_type, "project website")),
                (RDF.value, (Literal(f"Link to the {row_data['corpusName']} website.")))
            )
        ]

    @nan_handler
    def corpus_language_triples(
            language_field=row_data["corpusLanguage"]) -> TripleGenerator:

        iso_language_ns = Namespace("https://vocabs.acdh.oeaw.ac.at/iso6391/")
        language_values = map(str.strip, language_field.split(","))

        for language_value in language_values:
            try:
                _lang_iso = langcodes.find(language_value).to_tag()
                _lang_uri = iso_language_ns[_lang_iso]
            except LookupError:
                _lang_uri = clscore[f"language/{genhash(language_value)}"]

            lang_uri = URIRef(_lang_uri)

            yield from [
                (lang_uri, RDF.type, crm["E56_Language"]),
                (lang_uri, RDFS.label, Literal(language_value)),
                *plist(
                    attribute_assignment_uri_1,
                    (RDF.type, crm["E13_Attribute_Assignment"]),
                    (crm["P134_continued"], descevent_uri),
                    (crm["P140_assigned_attribute_to"], protodoc_uri),
                    (crm["P177_assigned_property_of_type"], crm["P72_has_Language"]),
                    (crm["P141_assigned"], lang_uri)
                )
            ]

    @nan_handler
    def corpus_text_count_triples(text_count=row_data["corpusTextCount"]) -> TripleGenerator:
        yield from [
            *plist(
                attribute_assignment_uri_2,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], descevent_uri),
                (crm["P140_assigned_attribute_to"], corpus_uri),
                (crm["P141_assigned"], dimension_uri_1)
            ),
            *plist(
                dimension_uri_1,
                (RDF.type, crm["E54_Dimension"]),
                (
                    crm["P90_has_value"],
                    Literal(text_count, datatype=XSD.integer)
                ),
                (crm["P91_has_unit"], clscore["type/feature/document"])
            )
        ]

    def corpus_timespan_triples() -> TripleGenerator:
        yield from [
            *plist(
                attribute_assignment_uri_3,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], descevent_uri),
                (crm["P140_assigned_attribute_to"], corpus_uri),
                (crm["P177_assigned_property_of_type"], crm["P4_has_time-span"]),
                (crm["P141_assigned"], timespan_uri_1)
            ),
            *plist(
                timespan_uri_1,
                (RDF.type, crm["E52_Time-Span"]),
                (RDFS.label, Literal(row_data["corpusTimespan"]))
            )
        ]

    @nan_handler
    def corpus_format_schema_triples(
            schema=row_data["corpusFormat/Schema_consolidatedVocab"]) -> TripleGenerator:

        format_values = map(
            str.strip,
            schema.split(",")
        )

        format_uris = map(
            functools.partial(vocabs_lookup, format_vocab),
            format_values
        )

        for format_uri in format_uris:
            yield from plist(
                attribute_assignment_uri_4,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], descevent_uri),
                (crm["P140_assigned_attribute_to"], protodoc_uri),
                (crm["P177_assigned_property_of_type"], crmcls["Y2_has_format"]),
                (crm["P141_assigned"], format_uri)
            )

    @nan_handler
    def corpus_literary_genre_triples(
            genre=row_data["corpusLiteraryGenre_consolidatedVocab"]) -> TripleGenerator:

        genre_values = map(
            str.strip,
            genre.split(",")
        )

        genre_uris = map(
            functools.partial(vocabs_lookup, literary_genre),
            genre_values
        )

        for genre_uri in genre_uris:
            yield from plist(
                attribute_assignment_uri_5,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], descevent_uri),
                (crm["P140_assigned_attribute_to"], corpus_uri),
                (
                    crm["P177_assigned_property_of_type"],
                    crmcls["Y4_document_has_literary_genre"]
                ),
                (crm["P141_assigned"], genre_uri)
            )

    @nan_handler
    def corpus_type_triples(
            corpus_type_value=row_data["corpusType_consolidatedVocab"]) -> TripleGenerator:

        yield from plist(
            attribute_assignment_uri_6,
            (RDF.type, crm["E13_Attribute_Assignment"]),
            (crm["P134_continued"], descevent_uri),
            (crm["P140_assigned_attribute_to"], corpus_uri),
            (
                crm["P177_assigned_property_of_type"],
                crmcls["Y6_corpus_has_corpus_type"]
            ),
            (crm["P141_assigned"], vocabs_lookup(corpus_type, corpus_type_value))
        )

    @nan_handler
    def corpus_license_triples(
            license_field=row_data["corpusLicence_consolidatedVocab"]) -> TripleGenerator:

        license_field_values = map(
            str.strip,
            license_field.split(",")
        )

        for license_value in license_field_values:
            yield from plist(
                attribute_assignment_uri_7,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], descevent_uri),
                (crm["P140_assigned_attribute_to"], protodoc_uri),
                (
                    crm["P177_assigned_property_of_type"],
                    crmcls["Y5_license_type"]
                ),
                (crm["P141_assigned"], vocabs_lookup(licenses, license_value))
            )

    triples = itertools.chain(
        descevent_triples(),
        person_triples(),
        corpus_name_triples(),
        corpus_acronym_triples(),
        corpus_link_triples(),
        corpus_language_triples(),
        corpus_text_count_triples(),
        corpus_timespan_triples(),
        corpus_format_schema_triples(),
        corpus_literary_genre_triples(),
        corpus_type_triples(),
        corpus_license_triples()
    )

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def additional_link_row_rule(row_data):
    """..."""
    base_ns = corpus_base(row_data["corpusAcronym"])
    corpus_uri = base_ns["corpus"]

    link_uri = URIRef(row_data["links"])
    descevent_uri = base_ns["descevent/1"]

    def link_triple():
        return (
            corpus_uri,
            crm["P1_is_identified_by"],
            link_uri
        )

    @nan_handler
    def link_type_triple(link_type_value=row_data["link_type_vocab"]):
        return (
            link_uri,
            crm["P2_has_type"],
            vocabs_lookup(link_type, link_type_value)
        )

    @nan_handler
    def descevent_triples(used=row_data["used_in_descEvent"]):
        return (
            descevent_uri,
            crm["P16_used_specific_object"],
            link_uri
        )

    @nan_handler
    def link_comment_triples(link_comment=row_data["link_comment"]):
        return (
            link_uri,
            crm["P3_has_note"],
            Literal(link_comment)
        )

    triples = [
        link_triple(),
        link_type_triple(),
        descevent_triples(),
        link_comment_triples()
    ]

    graph = Graph()

    for triple in triples:
        if triple:
            graph.add(triple)

    return graph



##################################################
import operator
from table_partitions import (
    corpus_table,
    greekdracor_partition,
    rem_partition,
    additional_link_table
)

graph = Graph()
CLSInfraNamespaceManager(graph)

corpustable_converter = RowGraphConverter(
    dataframe=corpus_table,
    row_rule=corpustable_row_rule,
    graph=graph
)

corpustable_graph = remove_nan(corpustable_converter.to_graph())
print(corpustable_converter.serialize())




# additional_link_converter = RowGraphConverter(
#     dataframe=additional_link_table,
#     row_rule=additional_link_row_rule,
#     graph=graph
# )

# print(additional_link_converter.serialize())





# merged_graph = operator.add(
#     corpustable_converter.to_graph(),
#     additional_link_converter.to_graph()
# )

# print(merged_graph.serialize())





# import math

# cnt = 0

# for value in corpus_table.loc[:, "corpusLicence_consolidatedVocab"]:
#     cnt+=1
#     try:
#         if math.isnan(value):
#             continue
#     except TypeError:
#         print(cnt)
#         print(value)
#         print(vocabs_lookup(licenses, value))
