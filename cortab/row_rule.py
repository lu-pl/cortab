"""Defines a row_rule callable for a tabulardf.RowGraphConverter."""

import itertools
import pandas as pd

from typing import Generator, Mapping

from helpers.cortab_utils import (  # noqa: F401
    genhash,
    vocabs_lookup,
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
    format,
    licenses,
    link_type,
    literary_genre,
    method
)


def row_rule(row_data: Mapping) -> Graph:
    """row_rule for RowGraphConverter."""
    # URIs
    base_ns = corpus_base(row_data["corpusAcronym"])
    corpus_uri = base_ns["corpus"]
    corpus_link_uri = URIRef(row_data["corpusLink"])
    descevent_uri = base_ns["descevent/1"]
    protodoc_uri = base_ns["protodoc/1"]

    timespan_uri_1 = base_ns["timespan/1"]
    timespan_uri_2 = base_ns["timespan/2"]

    corpus_appellation_uri_1 = base_ns["appellation/1"]
    corpus_appellation_uri_2 = base_ns["appellation/2"]

    attribute_assignment_uri_1 = base_ns["attrassign/1"]
    attribute_assignment_uri_2 = base_ns["attrassign/2"]
    attribute_assignment_uri_3 = base_ns["attrassign/3"]

    dimension_uri_1 = base_ns["dimension/1"]

    # triples
    def person_triples() -> Generator[_Triple, None, None]:
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

    descevent_triples = [
        (corpus_uri, RDF.type, crmcls["X1_Corpus"]),
        (protodoc_uri, RDF.type, crmcls["X11_Prototypical_Document"]),
        # descevent
        *plist(
            descevent_uri,
            (RDF.type, crmcls["X9_Corpus_Description"]),
            (crm["P16_used_specific_object"], Literal(row_data["corpusLink"])),
            (crm["P135_created_type"], protodoc_uri),
            (crm["P4_has_time-span"], timespan_uri_1),
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

    corpus_name_triples = [
        (corpus_uri, crm["P1_is_identified_by"], corpus_appellation_uri_1),
        *plist(
            corpus_appellation_uri_1,
            (RDF.type, crm["E41_Appellation"]),
            (crm["P2_has_type"], vocabs_lookup(appellation_type, "full title")),
            (RDF.value, Literal(row_data["corpusName"]))
        )
    ]

    corpus_acronym_triples = [
        (corpus_uri, crm["P1_is_identified_by"], corpus_appellation_uri_2),
        *plist(
            corpus_appellation_uri_2,
            (RDF.type, crm["E41_Appellation"]),
            (crm["P2_has_type"], vocabs_lookup(appellation_type, "acronym")),
            (RDF.value, Literal(row_data["corpusAcronym"]))
        )
    ]

    corpus_link_triples = [
        (corpus_uri, crm["P1_is_identified_by"], corpus_link_uri),
        *plist(
            corpus_link_uri,
            (RDF.type, crm["E42_Identifier"]),
            (crm["P2_has_type"], vocabs_lookup(link_type, "project website")),
            (RDF.value, (Literal(f"Link to the {row_data['corpusName']} website.")))
        )
    ]

    def corpus_language_triples() -> Generator[_Triple, None, None]:
        iso_language_ns = Namespace("https://vocabs.acdh.oeaw.ac.at/iso6391/")
        language_values = map(str.strip, row_data["corpusLanguage"].split(","))

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

    corpus_text_count_triples = [
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
                Literal(row_data["corpusTextCount"], datatype=XSD.integer)
            ),
            (crm["P91_has_unit"], clscore["type/feature/document"])
        )
    ]

    # warning: NaN
    corpus_timespan_triples = [
        *plist(
            attribute_assignment_uri_3,
            (RDF.type, crm["E13_Attribute_Assignment"]),
            (crm["P134_continued"], descevent_uri),
            (crm["P140_assigned_attribute_to"], corpus_uri),
            (crm["P177_assigned_property_of_type"], crm["P4_has_time-span"]),
            (crm["P141_assigned"], timespan_uri_2)
        ),
        *plist(
            timespan_uri_2,
            (RDF.type, crm["E52_Time-Span"]),
            (RDFS.label, Literal(row_data["corpusTimespan"]))
        )
    ]

    triples = itertools.chain(
        descevent_triples,
        person_triples(),
        corpus_name_triples,
        corpus_acronym_triples,
        corpus_link_triples,
        corpus_language_triples(),
        corpus_text_count_triples,
        corpus_timespan_triples
    )

    graph = Graph()

    for triple in triples:
        graph.add(triple)

    return graph


##################################################
from table_partitions import rem_partition

dataframe = pd.read_csv("./corpusTable.csv")
graph = Graph()
CLSInfraNamespaceManager(graph)

converter = RowGraphConverter(
    # dataframe=dataframe,
    dataframe=rem_partition,
    row_rule=row_rule,
    graph=graph
)

print(converter.serialize())
