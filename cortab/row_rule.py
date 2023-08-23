"""Defines a row_rule callable for a tabulardf.RowGraphConverter."""

import itertools
import pandas as pd

from typing import Mapping

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

from lodkit.utils import plist

from rdflib import Graph, Literal, URIRef
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
    timespan_uri = base_ns["timespan/1"]
    corpus_appellation_uri_1 = base_ns["appellation/1"]
    corpus_appellation_uri_2 = base_ns["appellation/2"]

    # triples
    def person_triples():
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
            (crm["P4_has_time-span"], timespan_uri),
            (crm["P3_has_note"], Literal(row_data["additionalInfo / commentary"]))
        ),
        # timespan
        *plist(
            timespan_uri,
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


    triples = itertools.chain(
        descevent_triples,
        person_triples(),
        corpus_name_triples,
        corpus_acronym_triples,
        corpus_link_triples,
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
