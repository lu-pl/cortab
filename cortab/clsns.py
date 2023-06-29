"""Namespaces and custom NamespaceManager for CLSInfra."""

from collections.abc import MutableMapping

from rdflib import Graph, Namespace
from rdflib.namespace import NamespaceManager
from rdflib._type_checking import _NamespaceSetString


# namespaces
crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
crmcls = Namespace("https://clscor.io/ontologies/CRMcls/")
clst = Namespace("https://core.clscor.io/entity/")

# namespace mappings
_NAMESPACE_PREFIXES_CLSINFRA: MutableMapping[str, Namespace] = {
    "crm": crm,
    "crmcls": crmcls,
    "clst": clst
}


class CLSInfraNamespaceManager(NamespaceManager):
    """Custom NamespaceManager for CLSInfra."""

    def __init__(self,
                 graph: Graph,
                 bind_namespaces: "_NamespaceSetString" = "rdflib"):
        """Call init.super and add CLSInfra namespaces."""
        super().__init__(graph=graph, bind_namespaces=bind_namespaces)

        for prefix, ns in _NAMESPACE_PREFIXES_CLSINFRA.items():
            graph.bind(prefix, ns)
