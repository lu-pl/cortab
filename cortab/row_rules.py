"""Defines a row_rule callable for a tabulardf.RowGraphConverter."""

import itertools
import functools
import math

import pandas as pd

from typing import Generator, Mapping

from helpers.cortab_utils import (  # noqa: F401
    genhash,
    vocabs_lookup,
    remove_nan,
    nan_handler,
    mkuri,
    uri_ns
)

from table_partitions import (
    corpus_table,
    additional_link_table,
    disco_partition,
    disco_additional_link_table
)

from clisn import (
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
    appellation,
    corpus,
    feature,
    format as format_vocab,
    licenses,
    link,
    literary_genre,
    method
)


TripleGenerator = Generator[_Triple, None, None]


def corpustable_row_rule(row_data: Mapping) -> Graph:
    """row_rule for RowGraphConverter."""
    # URIs
    corpus_uri = mkuri(row_data["corpusAcronym"])
    corpus_link_uri = URIRef(row_data["corpusLink"])

    uris = uri_ns(
        "descevent_uri",
        ("protodoc_uri", f"{row_data['corpusAcronym']} [X11]"),

        ("descevent_timespan_uri_1", "desc_timespan/1"),
        "corpus_timespan_uri_1",

        "corpus_appellation_uri_1",
        "corpus_appellation_uri_2",

        "attribute_assignment_uri_1",
        "attribute_assignment_uri_2",
        "attribute_assignment_uri_3",
        "attribute_assignment_uri_4",
        "attribute_assignment_uri_5",
        "attribute_assignment_uri_6",
        "attribute_assignment_uri_7",
        "attribute_assignment_uri_8",
        "attribute_assignment_uri_9",

        "dimension_uri_1",
        "dimension_uri_2"
    )

    # triples
    def person_triples() -> TripleGenerator:
        person_names = map(
            str.strip,
            row_data["editor/contributor"].split(",")
        )

        for person_name in person_names:
            person_uri = mkuri(f"person/{person_name}")
            yield from (
                (person_uri, RDF.type, crm["E39_Actor"]),
                (person_uri, RDFS.label, Literal(person_name)),
                (uris.descevent_uri, crm["P14_carried_out_by"], person_uri)
            )

    def descevent_triples() -> TripleGenerator:
        yield from [
            (corpus_uri, RDF.type, crmcls["X1_Corpus"]),

            # X1 -> P148 -> X11
            (corpus_uri, crm.P148_has_component, uris.protodoc_uri),

            (uris.protodoc_uri, RDF.type, crmcls["X11_Prototypical_Document"]),
            # descevent
            *plist(
                uris.descevent_uri,
                (RDF.type, crmcls["X9_Corpus_Description"]),
                # (crm["P16_used_specific_object"], Literal(row_data["corpusLink"])),
                (crm["P135_created_type"], uris.protodoc_uri),
                (crm["P4_has_time-span"], uris.descevent_timespan_uri_1),
                (crm["P3_has_note"], Literal(
                    row_data["additionalInfo / commentary"]))
            ),
            # timespan
            *plist(
                uris.descevent_timespan_uri_1,
                (RDF.type, crm["E52_Time-Span"]),
                (
                    crm["P81a_end_of_the_begin"],
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
            (corpus_uri, crm["P1_is_identified_by"], uris.corpus_appellation_uri_1),
            *plist(
                uris.corpus_appellation_uri_1,
                (RDF.type, crm["E41_Appellation"]),
                (crm["P2_has_type"], vocabs_lookup(appellation, "full title")),
                (RDF.value, Literal(row_data["corpusName"]))
            )
        ]

    def corpus_acronym_triples() -> TripleGenerator:
        yield from [
            (corpus_uri, crm["P1_is_identified_by"], uris.corpus_appellation_uri_2),
            *plist(
                uris.corpus_appellation_uri_2,
                (RDF.type, crm["E41_Appellation"]),
                (crm["P2_has_type"], vocabs_lookup(appellation, "acronym")),
                (RDF.value, Literal(row_data["corpusAcronym"]))
            )
        ]

        comment = row_data["corpusAcronym_comments"]
        if isinstance(comment, str):
            yield (
                uris.corpus_appellation_uri_2,
                crm["P3_has_note"],
                Literal(comment)
            )

    def corpus_link_triples() -> TripleGenerator:
        yield from [
            (corpus_uri, crm["P1_is_identified_by"], corpus_link_uri),
            *plist(
                corpus_link_uri,
                (RDF.type, crm["E42_Identifier"]),
                (crm["P2_has_type"], vocabs_lookup(link, "project website")),
                (RDF.value,
                 (Literal(f"Link to the {row_data['corpusName']} website.")))
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
                _lang_uri = mkuri(f"language/{language_value}")

            lang_uri = URIRef(_lang_uri)

            yield from [
                (lang_uri, RDF.type, crm["E56_Language"]),
                (lang_uri, RDFS.label, Literal(language_value)),
                *plist(
                    uris.attribute_assignment_uri_1,
                    (RDF.type, crm["E13_Attribute_Assignment"]),
                    (crm["P134_continued"], uris.descevent_uri),
                    (crm["P140_assigned_attribute_to"], uris.protodoc_uri),
                    (crm["P177_assigned_property_of_type"],
                     crm["P72_has_Language"]),
                    (crm["P141_assigned"], lang_uri)
                )
            ]

        comment = row_data["corpusLanguage_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_1,
                crm["P3_has_note"],
                Literal(comment)
            )

    @nan_handler
    def corpus_text_count_triples(text_count=row_data["corpusTextCount"]) -> TripleGenerator:
        yield from [
            *plist(
                uris.attribute_assignment_uri_2,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], uris.descevent_uri),
                (crm["P140_assigned_attribute_to"], corpus_uri),
                (crm["P141_assigned"], uris.dimension_uri_1)
            ),
            *plist(
                uris.dimension_uri_1,
                (RDF.type, crm["E54_Dimension"]),
                (
                    crm["P90_has_value"],
                    Literal(int(text_count), datatype=XSD.integer)
                ),
                (crm["P91_has_unit"], mkuri("type/feature/document"))
            )
        ]

        comment = row_data["corpusTextCount_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_2,
                crm["P3_has_note"],
                Literal(comment)
            )

    @nan_handler
    def corpus_word_count_triples(word_count=row_data["corpusWordCount"]) -> TripleGenerator:
        yield from [
            *plist(
                uris.attribute_assignment_uri_8,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], uris.descevent_uri),
                (crm["P140_assigned_attribute_to"], corpus_uri),
                (crm["P141_assigned"], uris.dimension_uri_2)
            ),
            *plist(
                uris.dimension_uri_2,
                (RDF.type, crm["E54_Dimension"]),
                (
                    crm["P90_has_value"],
                    Literal(int(word_count), datatype=XSD.integer)
                ),
                (crm["P91_has_unit"], mkuri("type/feature/word"))
            )
        ]

        comment = row_data["corpusWordCount_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_8,
                crm["P3_has_note"],
                Literal(comment)
            )

    def corpus_timespan_triples() -> TripleGenerator:
        yield from [
            *plist(
                uris.attribute_assignment_uri_3,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], uris.descevent_uri),
                (crm["P140_assigned_attribute_to"], corpus_uri),
                (crm["P177_assigned_property_of_type"], crm["P4_has_time-span"]),
                (crm["P141_assigned"], uris.corpus_timespan_uri_1)
            ),
            *plist(
                uris.corpus_timespan_uri_1,
                (RDF.type, crm["E52_Time-Span"]),
                (RDFS.label, Literal(row_data["corpusTimespan"]))
            )
        ]

        comment = row_data["corpusTimespan_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_3,
                crm["P3_has_note"],
                Literal(comment)
            )

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
                uris.attribute_assignment_uri_4,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], uris.descevent_uri),
                (crm["P140_assigned_attribute_to"], uris.protodoc_uri),
                (crm["P177_assigned_property_of_type"], crmcls["Y2_has_format"]),
                (crm["P141_assigned"], format_uri)
            )

        comment = row_data["corpusFormat/Schema_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_4,
                crm["P3_has_note"],
                Literal(comment)
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
                uris.attribute_assignment_uri_5,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], uris.descevent_uri),
                (crm["P140_assigned_attribute_to"], uris.protodoc_uri),
                (
                    crm["P177_assigned_property_of_type"],
                    crm["P2_has_type"]
                ),
                (crm["P141_assigned"], genre_uri)
            )

        comment = row_data["corpusLiteraryGenre_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_5,
                crm["P3_has_note"],
                Literal(comment)
            )

    @nan_handler
    def corpus_type_triples(
            corpus_type_value=row_data["corpusType_consolidatedVocab"]) -> TripleGenerator:

        yield from plist(
            uris.attribute_assignment_uri_6,
            (RDF.type, crm["E13_Attribute_Assignment"]),
            (crm["P134_continued"], uris.descevent_uri),
            (crm["P140_assigned_attribute_to"], corpus_uri),
            (
                crm["P177_assigned_property_of_type"],
                crm["P2_has_type"]
            ),
            (crm["P141_assigned"], vocabs_lookup(corpus, corpus_type_value))
        )

        comment = row_data["corpusType_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_6,
                crm["P3_has_note"],
                Literal(comment)
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
                uris.attribute_assignment_uri_7,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], uris.descevent_uri),
                (crm["P140_assigned_attribute_to"], uris.protodoc_uri),
                (
                    crm["P177_assigned_property_of_type"],
                    crm["P2_has_type"]
                ),
                (crm["P141_assigned"], vocabs_lookup(licenses, license_value))
            )

        comment = row_data["corpusLicence_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_7,
                crm["P3_has_note"],
                Literal(comment)
            )

    @nan_handler
    def corpus_annotation_triples(
            annotation_field=row_data["corpusAnnotation_consolidatedVocab"]) -> TripleGenerator:

        annotation_field_values = map(
            str.strip,
            annotation_field.split(",")
        )

        annotation_uris = map(
            functools.partial(vocabs_lookup, feature),
            annotation_field_values
        )

        for annotation_uri in annotation_uris:
            yield from plist(
                uris.attribute_assignment_uri_9,
                (RDF.type, crm["E13_Attribute_Assignment"]),
                (crm["P134_continued"], uris.descevent_uri),
                (crm["P140_assigned_attribute_to"], uris.protodoc_uri),
                (
                    crm["P177_assigned_property_of_type"],
                    crmcls["Y1_exhibits_feature"]
                ),
                (crm["P141_assigned"], annotation_uri)
            )

        comment = row_data["corpusAnnotation_comments"]
        if isinstance(comment, str):
            yield (
                uris.attribute_assignment_uri_9,
                crm["P3_has_note"],
                Literal(comment)
            )

    triples = itertools.chain(
        descevent_triples(),
        person_triples(),
        corpus_name_triples(),
        corpus_acronym_triples(),
        corpus_link_triples(),
        corpus_language_triples(),
        corpus_text_count_triples(),
        corpus_word_count_triples(),
        corpus_timespan_triples(),
        corpus_format_schema_triples(),
        corpus_literary_genre_triples(),
        corpus_type_triples(),
        corpus_license_triples(),
        corpus_annotation_triples()
    )

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


def additional_link_row_rule(row_data):
    """Additional link row."""
    corpus_uri = mkuri(row_data["corpusAcronym"])

    link_uri = URIRef(row_data["links"])
    descevent_uri = mkuri("descevent/1")

    def link_triple():
        return (
            corpus_uri,
            crm["P1_is_identified_by"],
            link_uri
        )

    @nan_handler
    def link_type_triple(link_type_value=row_data["link_type_vocab"]):
        yield from plist(
            link_uri,
            (RDF.type, crm["E42_Identifier"]),
            (crm["P2_has_type"], vocabs_lookup(link, link_type_value))
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

    triples = filter(
        bool,
        [
            link_triple(),
            *link_type_triple(),
            descevent_triples(),
            link_comment_triples()
        ]
    )

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


graph = Graph()
CLSInfraNamespaceManager(graph)

corpustable_converter = RowGraphConverter(
    dataframe=corpus_table,
    # dataframe=disco_partition,
    row_rule=corpustable_row_rule,
    graph=graph
)

# corpustable_graph = corpustable_converter.to_graph()
corpustable_graph = remove_nan(corpustable_converter.to_graph())

additional_link_converter = RowGraphConverter(
    dataframe=additional_link_table,
    # dataframe=disco_additional_link_table,
    row_rule=additional_link_row_rule,
    graph=graph
)

additional_link_graph = additional_link_converter.to_graph()
print(additional_link_graph.serialize())
