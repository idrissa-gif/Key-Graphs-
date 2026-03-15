#!/usr/bin/env python3
"""
Graph Keys Discovery Tool

This tool extends classical keys to graph keys as defined in the paper:
"Keys for graphs" (https://www.researchgate.net/publication/283189709_Keys_for_graphs)

The approach:
1. Discover n-almost keys using SAKey-like algorithm
2. Identify object properties in discovered keys
3. For each object property, find its range class
4. Discover keys for the range class
5. Extend the original key by following object properties recursively

Usage:
    python graph_keys.py --abox1 Abox1.nt --abox2 Abox2.nt --tbox Tbox1.nt --refalign refalign.rdf
"""

import re
import argparse
from collections import defaultdict
from typing import Dict, Set, List, Tuple, Optional, FrozenSet
from dataclasses import dataclass, field
import xml.etree.ElementTree as ET


# ============================================================================
# N-Triples Parser
# ============================================================================

def parse_ntriples(filepath: str) -> List[Tuple[str, str, str]]:
    """Parse N-Triples file and return list of (subject, predicate, object) tuples."""
    triples = []
    uri_pattern = re.compile(r'<([^>]+)>')
    literal_pattern = re.compile(r'"([^"]*)"(?:\^\^<[^>]+>|@[a-z]+)?')

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Extract URIs and literals
            uris = uri_pattern.findall(line)

            if len(uris) >= 2:
                subject = uris[0]
                predicate = uris[1]

                # Check if object is a URI or literal
                if len(uris) >= 3:
                    obj = uris[2]
                else:
                    # Try to extract literal
                    literal_match = literal_pattern.search(line)
                    if literal_match:
                        obj = literal_match.group(1)
                    else:
                        continue

                triples.append((subject, predicate, obj))

    return triples


# ============================================================================
# RDF Knowledge Base
# ============================================================================

@dataclass
class KnowledgeBase:
    """Represents an RDF knowledge base with indexing for efficient access."""

    # Main triple store
    triples: List[Tuple[str, str, str]] = field(default_factory=list)

    # Indexes
    subjects: Dict[str, List[Tuple[str, str]]] = field(default_factory=lambda: defaultdict(list))
    predicates: Dict[str, List[Tuple[str, str]]] = field(default_factory=lambda: defaultdict(list))
    instances_by_class: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    property_values: Dict[Tuple[str, str], Set[str]] = field(default_factory=lambda: defaultdict(set))

    def add_triple(self, s: str, p: str, o: str):
        """Add a triple to the knowledge base."""
        self.triples.append((s, p, o))
        self.subjects[s].append((p, o))
        self.predicates[p].append((s, o))
        self.property_values[(s, p)].add(o)

        # Track class instances
        if p == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
            self.instances_by_class[o].add(s)

    def get_property_value(self, subject: str, predicate: str) -> Set[str]:
        """Get all values for a subject-predicate pair."""
        return self.property_values.get((subject, predicate), set())

    def get_instances(self, class_uri: str) -> Set[str]:
        """Get all instances of a class."""
        return self.instances_by_class.get(class_uri, set())

    def get_all_properties(self, subject: str) -> List[Tuple[str, str]]:
        """Get all property-value pairs for a subject."""
        return self.subjects.get(subject, [])


@dataclass
class Ontology:
    """Represents the ontology schema (TBox)."""

    # Property definitions
    property_domain: Dict[str, str] = field(default_factory=dict)
    property_range: Dict[str, str] = field(default_factory=dict)
    object_properties: Set[str] = field(default_factory=set)
    datatype_properties: Set[str] = field(default_factory=set)

    # Class hierarchy
    subclass_of: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    all_classes: Set[str] = field(default_factory=set)

    @classmethod
    def from_triples(cls, triples: List[Tuple[str, str, str]]) -> 'Ontology':
        """Build ontology from N-Triples."""
        onto = cls()

        RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
        RDFS_DOMAIN = 'http://www.w3.org/2000/01/rdf-schema#domain'
        RDFS_RANGE = 'http://www.w3.org/2000/01/rdf-schema#range'
        RDFS_SUBCLASS = 'http://www.w3.org/2000/01/rdf-schema#subClassOf'
        OWL_OBJECT_PROP = 'http://www.w3.org/2002/07/owl#ObjectProperty'
        OWL_DATATYPE_PROP = 'http://www.w3.org/2002/07/owl#DatatypeProperty'
        OWL_CLASS = 'http://www.w3.org/2002/07/owl#Class'

        for s, p, o in triples:
            if p == RDF_TYPE:
                if o == OWL_OBJECT_PROP:
                    onto.object_properties.add(s)
                elif o == OWL_DATATYPE_PROP:
                    onto.datatype_properties.add(s)
                elif o == OWL_CLASS:
                    onto.all_classes.add(s)
            elif p == RDFS_DOMAIN:
                onto.property_domain[s] = o
            elif p == RDFS_RANGE:
                onto.property_range[s] = o
            elif p == RDFS_SUBCLASS:
                onto.subclass_of[s].add(o)
                onto.all_classes.add(s)
                onto.all_classes.add(o)

        return onto

    def is_object_property(self, prop: str) -> bool:
        """Check if property is an object property."""
        return prop in self.object_properties

    def get_range(self, prop: str) -> Optional[str]:
        """Get the range class of a property."""
        return self.property_range.get(prop)

    def get_domain(self, prop: str) -> Optional[str]:
        """Get the domain class of a property."""
        return self.property_domain.get(prop)

    def get_superclasses(self, class_uri: str) -> Set[str]:
        """Get all superclasses of a class (transitive)."""
        result = set()
        to_visit = list(self.subclass_of.get(class_uri, set()))
        while to_visit:
            cls = to_visit.pop()
            if cls not in result:
                result.add(cls)
                to_visit.extend(self.subclass_of.get(cls, set()))
        return result

    def get_subclasses(self, class_uri: str) -> Set[str]:
        """Get all subclasses of a class (transitive)."""
        result = set()
        # Build reverse mapping
        subclasses_of = defaultdict(set)
        for sub, supers in self.subclass_of.items():
            for sup in supers:
                subclasses_of[sup].add(sub)

        to_visit = list(subclasses_of.get(class_uri, set()))
        while to_visit:
            cls = to_visit.pop()
            if cls not in result:
                result.add(cls)
                to_visit.extend(subclasses_of.get(cls, set()))
        return result

    def get_equivalent_classes(self, class_uri: str) -> Set[str]:
        """Get class and all its super/subclasses for matching."""
        result = {class_uri}
        result |= self.get_superclasses(class_uri)
        result |= self.get_subclasses(class_uri)
        return result


# ============================================================================
# Key Discovery (SAKey-like Algorithm)
# ============================================================================

@dataclass
class Key:
    """Represents a key (set of properties)."""
    properties: FrozenSet[str]
    support: int = 0
    exceptions: int = 0
    target_class: Optional[str] = None

    def __hash__(self):
        return hash(self.properties)

    def __eq__(self, other):
        return self.properties == other.properties

    def __str__(self):
        props = [p.split('/')[-1] for p in self.properties]
        return f"Key({', '.join(sorted(props))})"


@dataclass
class GraphKey:
    """Represents a graph key (key with extensions via object properties)."""
    base_key: Key
    extensions: Dict[str, 'GraphKey'] = field(default_factory=dict)  # property -> GraphKey for range
    depth: int = 0

    def __str__(self):
        result = str(self.base_key)
        if self.extensions:
            ext_strs = []
            for prop, gk in self.extensions.items():
                prop_name = prop.split('/')[-1]
                ext_strs.append(f"{prop_name}->{gk}")
            result += f" + [{', '.join(ext_strs)}]"
        return result


class SAKeyAlgorithm:
    """
    Simplified SAKey algorithm for discovering n-almost keys.

    An n-almost key is a set of properties that uniquely identifies
    all but n instances of a class.
    """

    def __init__(self, kb: KnowledgeBase, ontology: Ontology, n: int = 0):
        self.kb = kb
        self.ontology = ontology
        self.n = n  # Maximum number of exceptions allowed

    def discover_keys(self, target_class: str, max_key_size: int = 3) -> List[Key]:
        """
        Discover n-almost keys for a target class.

        Args:
            target_class: URI of the class to find keys for
            max_key_size: Maximum number of properties in a key

        Returns:
            List of discovered keys sorted by quality
        """
        instances = self.kb.get_instances(target_class)
        if not instances:
            return []

        # Collect all properties used by instances of this class
        properties = self._get_class_properties(instances)
        if not properties:
            return []

        # Find keys of increasing size
        keys = []

        # Single property keys
        for prop in properties:
            key = self._evaluate_key(frozenset([prop]), instances, target_class)
            if key and key.exceptions <= self.n:
                keys.append(key)

        # Multi-property keys (if needed)
        if max_key_size > 1:
            prop_list = list(properties)
            for size in range(2, min(max_key_size + 1, len(prop_list) + 1)):
                for i, prop1 in enumerate(prop_list):
                    for prop2 in prop_list[i+1:]:
                        if size == 2:
                            key = self._evaluate_key(
                                frozenset([prop1, prop2]), instances, target_class
                            )
                            if key and key.exceptions <= self.n:
                                keys.append(key)

        # Remove non-minimal keys
        keys = self._filter_minimal_keys(keys)

        # Sort by quality (fewer exceptions, then smaller size)
        keys.sort(key=lambda k: (k.exceptions, len(k.properties)))

        return keys

    def _get_class_properties(self, instances: Set[str]) -> Set[str]:
        """Get all properties used by instances of a class."""
        properties = set()
        for instance in instances:
            for prop, _ in self.kb.get_all_properties(instance):
                # Skip rdf:type
                if prop != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                    properties.add(prop)
        return properties

    def _evaluate_key(self, properties: FrozenSet[str],
                      instances: Set[str], target_class: str) -> Optional[Key]:
        """
        Evaluate if a set of properties forms a valid key.

        A key is valid if distinct instances have distinct property value combinations.
        """
        # Build property value signatures for each instance
        signatures = defaultdict(list)

        for instance in instances:
            # Get values for each property in the key
            sig_parts = []
            has_all_props = True

            for prop in sorted(properties):
                values = self.kb.get_property_value(instance, prop)
                if not values:
                    has_all_props = False
                    break
                # Sort values for consistent signatures
                sig_parts.append(tuple(sorted(values)))

            if has_all_props:
                sig = tuple(sig_parts)
                signatures[sig].append(instance)

        # Count collisions (instances sharing the same signature)
        exceptions = 0
        for sig, insts in signatures.items():
            if len(insts) > 1:
                exceptions += len(insts) - 1

        # Also count instances without complete signatures as exceptions
        instances_with_sig = sum(len(insts) for insts in signatures.values())
        exceptions += len(instances) - instances_with_sig

        support = len(signatures)

        return Key(
            properties=properties,
            support=support,
            exceptions=exceptions,
            target_class=target_class
        )

    def _filter_minimal_keys(self, keys: List[Key]) -> List[Key]:
        """Remove non-minimal keys (keys that are supersets of other keys)."""
        minimal = []

        for key in keys:
            is_minimal = True
            for other in keys:
                if (other.properties < key.properties and
                    other.exceptions <= key.exceptions):
                    is_minimal = False
                    break
            if is_minimal:
                minimal.append(key)

        return minimal


# ============================================================================
# Graph Key Extension
# ============================================================================

class GraphKeyBuilder:
    """
    Extends classical keys to graph keys by following object properties.

    For each object property in a key, we find keys for the range class
    and use them to extend the original key recursively.
    """

    def __init__(self, kb: KnowledgeBase, ontology: Ontology,
                 sakey: SAKeyAlgorithm, max_depth: int = 2):
        self.kb = kb
        self.ontology = ontology
        self.sakey = sakey
        self.max_depth = max_depth
        self.key_cache: Dict[str, List[Key]] = {}
        self.actual_range_classes: Dict[str, Set[str]] = {}  # prop -> actual classes

    def extend_key(self, key: Key, depth: int = 0) -> GraphKey:
        """
        Extend a classical key to a graph key.

        Args:
            key: The base key to extend
            depth: Current recursion depth

        Returns:
            GraphKey with extensions via object properties
        """
        graph_key = GraphKey(base_key=key, depth=depth)

        if depth >= self.max_depth:
            return graph_key

        # Find object properties in the key
        for prop in key.properties:
            if self.ontology.is_object_property(prop):
                # Find actual range classes from the data
                actual_classes = self._find_actual_range_classes(prop, key.target_class)

                for range_class in actual_classes:
                    # Find keys for the range class
                    range_keys = self._get_keys_for_class(range_class)

                    if range_keys:
                        # Use the best key and extend it recursively
                        best_range_key = range_keys[0]
                        extension = self.extend_key(best_range_key, depth + 1)
                        graph_key.extensions[prop] = extension
                        break  # Use first class that has keys

        return graph_key

    def _find_actual_range_classes(self, prop: str, domain_class: str) -> List[str]:
        """Find actual classes of entities linked via object property in the data."""
        cache_key = f"{prop}|{domain_class}"
        if cache_key in self.actual_range_classes:
            return list(self.actual_range_classes[cache_key])

        range_classes = defaultdict(int)
        domain_instances = self.kb.get_instances(domain_class)

        for instance in domain_instances:
            linked_entities = self.kb.get_property_value(instance, prop)
            for linked in linked_entities:
                # Find the class of this linked entity
                entity_classes = self.kb.get_property_value(
                    linked, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
                )
                for cls in entity_classes:
                    range_classes[cls] += 1

        # Sort by frequency and return classes with sufficient instances
        sorted_classes = sorted(range_classes.keys(), key=lambda c: -range_classes[c])
        result = [c for c in sorted_classes if range_classes[c] >= 5]
        self.actual_range_classes[cache_key] = set(result)
        return result

    def _get_keys_for_class(self, class_uri: str) -> List[Key]:
        """Get cached keys for a class or discover them."""
        if class_uri not in self.key_cache:
            self.key_cache[class_uri] = self.sakey.discover_keys(class_uri)
        return self.key_cache[class_uri]

    def build_all_graph_keys(self, target_class: str) -> List[GraphKey]:
        """Build graph keys for all keys of a target class."""
        base_keys = self._get_keys_for_class(target_class)
        return [self.extend_key(key) for key in base_keys]


# ============================================================================
# Entity Linking
# ============================================================================

class EntityLinker:
    """
    Links entities between two knowledge bases using graph keys.
    """

    def __init__(self, kb1: KnowledgeBase, kb2: KnowledgeBase,
                 ontology: Ontology):
        self.kb1 = kb1
        self.kb2 = kb2
        self.ontology = ontology

    def compute_signature(self, entity: str, graph_key: GraphKey,
                         kb: KnowledgeBase) -> Optional[Tuple]:
        """
        Compute the signature of an entity based on a graph key.

        Returns None if the entity doesn't have all required properties.
        """
        sig_parts = []

        # Base key signature
        for prop in sorted(graph_key.base_key.properties):
            values = kb.get_property_value(entity, prop)
            if not values:
                return None

            # For object properties with extensions, compute nested signatures
            if prop in graph_key.extensions:
                nested_sigs = []
                for val in values:
                    nested_sig = self.compute_signature(
                        val, graph_key.extensions[prop], kb
                    )
                    if nested_sig:
                        nested_sigs.append(nested_sig)
                if nested_sigs:
                    sig_parts.append(('nested', prop, tuple(sorted(nested_sigs))))
                else:
                    # No valid nested signatures - skip this entity for this graph key
                    return None
            else:
                sig_parts.append(('direct', prop, tuple(sorted(values))))

        return tuple(sig_parts)

    def link_entities(self, target_class: str,
                     graph_keys: List[GraphKey]) -> Dict[str, str]:
        """
        Find entity links between kb1 and kb2 using graph keys.

        Returns:
            Dictionary mapping entities from kb1 to kb2
        """
        links = {}

        instances1 = self.kb1.get_instances(target_class)
        instances2 = self.kb2.get_instances(target_class)

        # Build signature index for kb2
        for graph_key in graph_keys:
            kb2_signatures: Dict[Tuple, str] = {}

            for entity2 in instances2:
                sig = self.compute_signature(entity2, graph_key, self.kb2)
                if sig:
                    kb2_signatures[sig] = entity2

            # Match entities from kb1
            for entity1 in instances1:
                if entity1 in links:
                    continue

                sig = self.compute_signature(entity1, graph_key, self.kb1)
                if sig and sig in kb2_signatures:
                    links[entity1] = kb2_signatures[sig]

        return links

    def link_with_simple_keys(self, target_class: str,
                              keys: List[Key]) -> Dict[str, str]:
        """
        Find entity links using simple (non-graph) keys.
        """
        links = {}

        instances1 = self.kb1.get_instances(target_class)
        instances2 = self.kb2.get_instances(target_class)

        for key in keys:
            # Build signature index for kb2
            kb2_signatures: Dict[Tuple, str] = {}

            for entity2 in instances2:
                sig_parts = []
                has_all = True
                for prop in sorted(key.properties):
                    values = self.kb2.get_property_value(entity2, prop)
                    if not values:
                        has_all = False
                        break
                    sig_parts.append(tuple(sorted(values)))

                if has_all:
                    kb2_signatures[tuple(sig_parts)] = entity2

            # Match entities from kb1
            for entity1 in instances1:
                if entity1 in links:
                    continue

                sig_parts = []
                has_all = True
                for prop in sorted(key.properties):
                    values = self.kb1.get_property_value(entity1, prop)
                    if not values:
                        has_all = False
                        break
                    sig_parts.append(tuple(sorted(values)))

                if has_all:
                    sig = tuple(sig_parts)
                    if sig in kb2_signatures:
                        links[entity1] = kb2_signatures[sig]

        return links


# ============================================================================
# Evaluation
# ============================================================================

def parse_reference_alignment(filepath: str) -> Set[Tuple[str, str]]:
    """Parse the reference alignment RDF file using regex for robustness."""
    alignments = set()

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Use regex to find Cell blocks
    cell_pattern = re.compile(r'<Cell>(.*?)</Cell>', re.DOTALL)
    entity1_pattern = re.compile(r'<entity1[^>]*rdf:resource="([^"]+)"')
    entity2_pattern = re.compile(r'<entity2[^>]*rdf:resource="([^"]+)"')

    for cell_match in cell_pattern.finditer(content):
        cell_content = cell_match.group(1)

        entity1_match = entity1_pattern.search(cell_content)
        entity2_match = entity2_pattern.search(cell_content)

        if entity1_match and entity2_match:
            entity1 = entity1_match.group(1)
            entity2 = entity2_match.group(1)
            alignments.add((entity1, entity2))

    return alignments


def evaluate_links(predicted: Dict[str, str],
                  reference: Set[Tuple[str, str]]) -> Dict[str, float]:
    """
    Evaluate predicted links against reference alignment.

    Returns:
        Dictionary with precision, recall, and F-measure
    """
    predicted_set = set(predicted.items())

    true_positives = len(predicted_set & reference)
    false_positives = len(predicted_set - reference)
    false_negatives = len(reference - predicted_set)

    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
    f_measure = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    return {
        'precision': precision,
        'recall': recall,
        'f_measure': f_measure,
        'true_positives': true_positives,
        'false_positives': false_positives,
        'false_negatives': false_negatives,
        'total_predicted': len(predicted_set),
        'total_reference': len(reference)
    }


# ============================================================================
# Main Pipeline
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Graph Keys Discovery and Entity Linking Tool'
    )
    parser.add_argument('--abox1', required=True, help='Path to first ABox (N-Triples)')
    parser.add_argument('--abox2', required=True, help='Path to second ABox (N-Triples)')
    parser.add_argument('--tbox', required=True, help='Path to TBox (N-Triples)')
    parser.add_argument('--refalign', required=True, help='Path to reference alignment (RDF)')
    parser.add_argument('--n', type=int, default=0, help='Maximum exceptions for n-almost keys')
    parser.add_argument('--max-depth', type=int, default=2, help='Maximum depth for graph keys')
    parser.add_argument('--target-class', default=None,
                       help='Target class URI (default: auto-detect)')

    args = parser.parse_args()

    print("=" * 70)
    print("GRAPH KEYS DISCOVERY AND ENTITY LINKING TOOL")
    print("=" * 70)

    # Load data
    print("\n[1] Loading data...")
    print(f"    - ABox 1: {args.abox1}")
    abox1_triples = parse_ntriples(args.abox1)
    print(f"      Loaded {len(abox1_triples)} triples")

    print(f"    - ABox 2: {args.abox2}")
    abox2_triples = parse_ntriples(args.abox2)
    print(f"      Loaded {len(abox2_triples)} triples")

    print(f"    - TBox: {args.tbox}")
    tbox_triples = parse_ntriples(args.tbox)
    print(f"      Loaded {len(tbox_triples)} triples")

    # Build knowledge bases
    print("\n[2] Building knowledge bases...")
    kb1 = KnowledgeBase()
    for s, p, o in abox1_triples:
        kb1.add_triple(s, p, o)

    kb2 = KnowledgeBase()
    for s, p, o in abox2_triples:
        kb2.add_triple(s, p, o)

    # Build ontology
    ontology = Ontology.from_triples(tbox_triples)
    print(f"    - Found {len(ontology.object_properties)} object properties")
    print(f"    - Found {len(ontology.datatype_properties)} datatype properties")
    print(f"    - Found {len(ontology.all_classes)} classes")

    # Determine target class
    if args.target_class:
        target_classes = [args.target_class]
    else:
        # Find classes with instances in both KBs
        common_classes = set(kb1.instances_by_class.keys()) & set(kb2.instances_by_class.keys())
        # Filter to substantial classes
        target_classes = [
            c for c in common_classes
            if len(kb1.get_instances(c)) > 5 and len(kb2.get_instances(c)) > 5
        ]

    print(f"\n[3] Target classes for key discovery:")
    for tc in target_classes[:5]:  # Show first 5
        tc_name = tc.split('/')[-1]
        n1 = len(kb1.get_instances(tc))
        n2 = len(kb2.get_instances(tc))
        print(f"    - {tc_name}: {n1} instances in KB1, {n2} in KB2")

    # Load reference alignment
    print(f"\n[4] Loading reference alignment...")
    reference = parse_reference_alignment(args.refalign)
    print(f"    - {len(reference)} reference links")

    # Key discovery and linking for each class
    all_simple_links = {}
    all_graph_links = {}

    for target_class in target_classes:
        tc_name = target_class.split('/')[-1]
        print(f"\n[5] Processing class: {tc_name}")

        # Discover simple keys
        print("    [5.1] Discovering simple keys (SAKey)...")
        sakey = SAKeyAlgorithm(kb1, ontology, n=args.n)
        simple_keys = sakey.discover_keys(target_class, max_key_size=3)

        if simple_keys:
            print(f"          Found {len(simple_keys)} simple keys:")
            for key in simple_keys[:5]:
                print(f"          - {key} (exceptions: {key.exceptions})")
        else:
            print("          No simple keys found")
            continue

        # Build graph keys
        print("    [5.2] Extending to graph keys...")
        builder = GraphKeyBuilder(kb1, ontology, sakey, max_depth=args.max_depth)
        graph_keys = builder.build_all_graph_keys(target_class)

        # Count keys with extensions
        keys_with_ext = sum(1 for gk in graph_keys if gk.extensions)
        print(f"          Built {len(graph_keys)} graph keys ({keys_with_ext} with extensions)")

        for gk in graph_keys[:3]:
            print(f"          - {gk}")

        # Show details of extensions
        for gk in graph_keys:
            if gk.extensions:
                base_name = str(gk.base_key)
                for prop, ext in gk.extensions.items():
                    prop_name = prop.split('/')[-1]
                    print(f"          >> Extension: {base_name} via {prop_name} -> {ext.base_key}")

        # Entity linking
        print("    [5.3] Performing entity linking...")
        linker = EntityLinker(kb1, kb2, ontology)

        # Link with simple keys
        simple_links = linker.link_with_simple_keys(target_class, simple_keys)
        all_simple_links.update(simple_links)
        print(f"          Simple keys: {len(simple_links)} links")

        # Link with graph keys
        graph_links = linker.link_entities(target_class, graph_keys)
        all_graph_links.update(graph_links)
        print(f"          Graph keys: {len(graph_links)} links")

    # Evaluation
    print("\n" + "=" * 70)
    print("EVALUATION RESULTS")
    print("=" * 70)

    print("\n[6] Evaluating simple keys (SAKey-like):")
    simple_eval = evaluate_links(all_simple_links, reference)
    print(f"    - Precision: {simple_eval['precision']:.4f}")
    print(f"    - Recall: {simple_eval['recall']:.4f}")
    print(f"    - F-measure: {simple_eval['f_measure']:.4f}")
    print(f"    - True Positives: {simple_eval['true_positives']}")
    print(f"    - False Positives: {simple_eval['false_positives']}")
    print(f"    - False Negatives: {simple_eval['false_negatives']}")

    print("\n[7] Evaluating graph keys:")
    graph_eval = evaluate_links(all_graph_links, reference)
    print(f"    - Precision: {graph_eval['precision']:.4f}")
    print(f"    - Recall: {graph_eval['recall']:.4f}")
    print(f"    - F-measure: {graph_eval['f_measure']:.4f}")
    print(f"    - True Positives: {graph_eval['true_positives']}")
    print(f"    - False Positives: {graph_eval['false_positives']}")
    print(f"    - False Negatives: {graph_eval['false_negatives']}")

    # Improvement analysis
    print("\n[8] Improvement Analysis:")
    if simple_eval['f_measure'] > 0:
        improvement = ((graph_eval['f_measure'] - simple_eval['f_measure']) /
                      simple_eval['f_measure'] * 100)
        print(f"    - F-measure improvement: {improvement:+.2f}%")

    additional_links = len(all_graph_links) - len(all_simple_links)
    print(f"    - Additional links from graph keys: {additional_links}")

    # Overlap analysis
    simple_set = set(all_simple_links.items())
    graph_set = set(all_graph_links.items())
    only_graph = graph_set - simple_set
    print(f"    - Links unique to graph keys: {len(only_graph)}")

    print("\n" + "=" * 70)
    print("Done!")


if __name__ == '__main__':
    main()
