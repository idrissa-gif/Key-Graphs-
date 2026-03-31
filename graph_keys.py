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
                      instances: Set[str], target_class: str,
                      kb: 'KnowledgeBase' = None) -> Optional[Key]:
        """
        Evaluate if a set of properties forms a valid key.

        A key is valid if distinct instances have distinct property value combinations.
        Accepts an optional kb parameter to evaluate against a different knowledge base.
        """
        if kb is None:
            kb = self.kb

        # Build property value signatures for each instance
        signatures = defaultdict(list)

        for instance in instances:
            # Get values for each property in the key
            sig_parts = []
            has_all_props = True

            for prop in sorted(properties):
                values = kb.get_property_value(instance, prop)
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

    def validate_key_in_kb(self, key: Key, kb: 'KnowledgeBase') -> Optional[Key]:
        """Evaluate a key discovered on KB1 against KB2 to check cross-KB validity."""
        instances = kb.get_instances(key.target_class)
        if not instances:
            return None
        return self._evaluate_key(key.properties, instances, key.target_class, kb)

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
        graph_keys = [self.extend_key(key) for key in base_keys]

        # Also discover path-only graph keys (keys that work via object properties)
        path_keys = self._discover_path_keys(target_class)
        graph_keys.extend(path_keys)

        return graph_keys

    def _discover_path_keys(self, target_class: str) -> List[GraphKey]:
        """
        Discover path-only graph keys that work by following object properties.

        These are keys where the object property value itself isn't unique,
        but properties of the linked entities provide uniqueness.
        """
        path_keys = []
        instances = self.kb.get_instances(target_class)

        if len(instances) < 5:
            return path_keys

        # Find object properties used by instances of this class
        obj_props_used = defaultdict(int)
        for instance in instances:
            for prop in self.ontology.object_properties:
                values = self.kb.get_property_value(instance, prop)
                if values:
                    obj_props_used[prop] += 1

        # Filter to properties used by most instances
        candidate_props = [p for p, count in obj_props_used.items()
                         if count >= len(instances) * 0.5]

        for prop in candidate_props:
            # Find range classes
            range_classes = self._find_actual_range_classes(prop, target_class)

            for range_class in range_classes:
                # Get keys for range class
                range_keys = self._get_keys_for_class(range_class)

                if not range_keys:
                    continue

                # Try to build a path key: prop -> range_key
                for range_key in range_keys[:3]:  # Try top 3 keys
                    # Check if this path produces unique signatures
                    signatures = defaultdict(list)

                    for instance in instances:
                        prop_values = self.kb.get_property_value(instance, prop)
                        if not prop_values:
                            continue

                        # Compute signature via path
                        nested_sigs = []
                        for val in prop_values:
                            sig_parts = []
                            has_all = True
                            for rk_prop in sorted(range_key.properties):
                                rk_values = self.kb.get_property_value(val, rk_prop)
                                if not rk_values:
                                    has_all = False
                                    break
                                sig_parts.append(tuple(sorted(rk_values)))
                            if has_all:
                                nested_sigs.append(tuple(sig_parts))

                        if nested_sigs:
                            sig = tuple(sorted(nested_sigs))
                            signatures[sig].append(instance)

                    # Check if this is a valid key (unique or n-almost)
                    exceptions = sum(1 for sig, insts in signatures.items() if len(insts) > 1)
                    total_sigs = len(signatures)

                    if total_sigs > 0 and exceptions <= self.sakey.n:
                        # Create a path-only graph key
                        base_key = Key(
                            properties=frozenset([prop]),
                            support=total_sigs,
                            exceptions=exceptions,
                            target_class=target_class
                        )
                        graph_key = GraphKey(base_key=base_key, depth=0)
                        extension_key = GraphKey(base_key=range_key, depth=1)
                        graph_key.extensions[prop] = extension_key
                        path_keys.append(graph_key)
                        break  # Found a working key for this property

        return path_keys


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
                normalized = tuple(sorted(self._normalize_value(v) for v in values))
                sig_parts.append(('direct', prop, normalized))

        return tuple(sig_parts)

    def link_entities(self, target_class: str,
                     graph_keys: List[GraphKey],
                     simple_links: Dict[str, str] = None) -> Dict[str, str]:
        """
        Find entity links between kb1 and kb2 using graph keys.

        Graph keys ADD to simple links by finding matches via extensions
        that simple keys couldn't find.  Ambiguity filtering is applied:
        a signature must be unique in both KBs for a link to be created.

        Returns:
            Dictionary mapping entities from kb1 to kb2
        """
        # Start with simple links if provided
        links = dict(simple_links) if simple_links else {}

        instances1 = self.kb1.get_instances(target_class)
        instances2 = self.kb2.get_instances(target_class)

        # Get entities already linked by simple keys
        already_linked = set(links.keys())
        targets_used = set(links.values())

        # Process ONLY graph keys with extensions (these provide additional value)
        keys_with_extensions = [gk for gk in graph_keys if gk.extensions]

        for graph_key in keys_with_extensions:
            # Build signature → entity list for KB2 (detect ambiguity)
            kb2_sig_to_entities: Dict[Tuple, List[str]] = defaultdict(list)
            for entity2 in instances2:
                if entity2 in targets_used:
                    continue
                sig = self.compute_signature(entity2, graph_key, self.kb2)
                if sig:
                    kb2_sig_to_entities[sig].append(entity2)

            # Keep only unambiguous KB2 signatures
            unambiguous_kb2 = {
                sig: ents[0]
                for sig, ents in kb2_sig_to_entities.items()
                if len(ents) == 1
            }

            # Count KB1 signatures for ambiguity detection
            kb1_sig_counts: Dict[Tuple, int] = defaultdict(int)
            for entity1 in instances1:
                if entity1 in already_linked:
                    continue
                sig = self.compute_signature(entity1, graph_key, self.kb1)
                if sig:
                    kb1_sig_counts[sig] += 1

            # Match unlinked entities — only when signature is unique in both KBs
            for entity1 in instances1:
                if entity1 in already_linked:
                    continue
                sig = self.compute_signature(entity1, graph_key, self.kb1)
                if sig is None:
                    continue
                if (kb1_sig_counts[sig] == 1
                        and sig in unambiguous_kb2
                        and unambiguous_kb2[sig] not in targets_used):
                    target = unambiguous_kb2[sig]
                    links[entity1] = target
                    already_linked.add(entity1)
                    targets_used.add(target)

        return links

    @staticmethod
    def _normalize_value(v: str) -> str:
        """Normalize a value for cross-KB comparison (string literals only)."""
        if v.startswith('http://') or v.startswith('https://'):
            return v
        return v.strip().lower()

    def _get_extended_instances(self, target_class: str, kb: KnowledgeBase,
                                excluded: Set[str] = None) -> Set[str]:
        """
        Get instances of target_class plus any direct superclass (covers type
        migration transformations where SPIMBENCH promotes entities to their
        parent class in the second ABox).
        """
        result: Set[str] = set(kb.get_instances(target_class))
        for parent in self.ontology.subclass_of.get(target_class, set()):
            result |= kb.get_instances(parent)
        if excluded:
            result -= excluded
        return result

    def _build_sig(self, entity: str, key: Key, kb: 'KnowledgeBase') -> Optional[Tuple]:
        """Build a normalized signature for an entity given a key and KB."""
        sig_parts = []
        for prop in sorted(key.properties):
            values = kb.get_property_value(entity, prop)
            if not values:
                return None
            normalized = tuple(sorted(self._normalize_value(v) for v in values))
            sig_parts.append(normalized)
        return tuple(sig_parts)

    def link_with_simple_keys(self, target_class: str,
                              keys: List[Key],
                              excluded_kb2: Set[str] = None) -> Dict[str, str]:
        """
        Find entity links using simple (non-graph) keys.

        Ambiguity filtering: a signature must be unique in both KB1 and KB2.
        If multiple entities share the same signature in either KB, the match is
        considered unreliable and skipped to avoid false positives.

        excluded_kb2: KB2 entities already linked in a previous class pass.
        """
        links: Dict[str, str] = {}
        used_kb2: Set[str] = set(excluded_kb2) if excluded_kb2 else set()

        instances1 = self.kb1.get_instances(target_class)
        # Also include superclass instances in KB2 to handle type-migration
        instances2 = self._get_extended_instances(target_class, self.kb2, excluded_kb2)

        for key in keys:
            # Build signature → entity list for KB2 (detect ambiguity)
            kb2_sig_to_entities: Dict[Tuple, List[str]] = defaultdict(list)
            for entity2 in instances2:
                sig = self._build_sig(entity2, key, self.kb2)
                if sig is not None:
                    kb2_sig_to_entities[sig].append(entity2)

            # Keep only unambiguous KB2 signatures
            unambiguous_kb2: Dict[Tuple, str] = {
                sig: ents[0]
                for sig, ents in kb2_sig_to_entities.items()
                if len(ents) == 1
            }

            # Count KB1 signatures to detect source-side ambiguity
            kb1_sig_counts: Dict[Tuple, int] = defaultdict(int)
            for entity1 in instances1:
                sig = self._build_sig(entity1, key, self.kb1)
                if sig is not None:
                    kb1_sig_counts[sig] += 1

            # Match entities — only when signature is unique in both KBs
            for entity1 in instances1:
                if entity1 in links:
                    continue
                sig = self._build_sig(entity1, key, self.kb1)
                if sig is None:
                    continue
                if (kb1_sig_counts[sig] == 1
                        and sig in unambiguous_kb2
                        and unambiguous_kb2[sig] not in used_kb2):
                    target = unambiguous_kb2[sig]
                    links[entity1] = target
                    used_kb2.add(target)

        return links

    def link_with_votes(self, target_class: str, keys: List[Key],
                        graph_keys: List['GraphKey'],
                        existing_links: Dict[str, str],
                        min_votes: int = 2) -> Dict[str, str]:
        """
        Recover unmatched entities using multi-source voting.

        Each simple key and each graph-key-with-extension is an independent
        evidence source.  For each unlinked KB1 entity we collect candidate KB2
        entities from every source (including ambiguous sources), count votes,
        and link when exactly one candidate has both the maximum vote count AND
        at least *min_votes* supporting sources.

        This handles entities that are ambiguous under any individual key but
        uniquely identified by the combination of multiple keys.
        """
        links = dict(existing_links)
        used_kb2: Set[str] = set(existing_links.values())

        instances1 = self.kb1.get_instances(target_class)
        instances2 = self.kb2.get_instances(target_class)

        # Build per-source (sig → set of KB2 entities) indexes
        # Each source is either a simple key or a graph key with extensions
        source_kb2_maps: List[Dict[Tuple, Set[str]]] = []

        for key in keys:
            sig_to_ents: Dict[Tuple, Set[str]] = defaultdict(set)
            for entity2 in instances2:
                if entity2 in used_kb2:
                    continue
                sig = self._build_sig(entity2, key, self.kb2)
                if sig is not None:
                    sig_to_ents[sig].add(entity2)
            source_kb2_maps.append(dict(sig_to_ents))

        for gk in graph_keys:
            if not gk.extensions:
                continue
            sig_to_ents: Dict[Tuple, Set[str]] = defaultdict(set)
            for entity2 in instances2:
                if entity2 in used_kb2:
                    continue
                sig = self.compute_signature(entity2, gk, self.kb2)
                if sig is not None:
                    sig_to_ents[sig].add(entity2)
            source_kb2_maps.append(dict(sig_to_ents))

        # Compute KB1 signatures per source
        def get_kb1_sig(source_idx: int, entity1: str) -> Optional[Tuple]:
            if source_idx < len(keys):
                return self._build_sig(entity1, keys[source_idx], self.kb1)
            gk_idx = source_idx - len(keys)
            ext_gks = [g for g in graph_keys if g.extensions]
            if gk_idx < len(ext_gks):
                return self.compute_signature(entity1, ext_gks[gk_idx], self.kb1)
            return None

        n_sources = len(source_kb2_maps)

        # Vote for each unlinked KB1 entity
        for entity1 in instances1:
            if entity1 in links:
                continue

            vote_counter: Dict[str, int] = defaultdict(int)
            for src_idx in range(n_sources):
                sig = get_kb1_sig(src_idx, entity1)
                if sig is None:
                    continue
                candidates = source_kb2_maps[src_idx].get(sig, set()) - used_kb2
                for c in candidates:
                    vote_counter[c] += 1

            if not vote_counter:
                continue

            max_votes = max(vote_counter.values())
            if max_votes < min_votes:
                continue

            top = [c for c, v in vote_counter.items() if v == max_votes]
            if len(top) == 1 and top[0] not in used_kb2:
                target = top[0]
                links[entity1] = target
                used_kb2.add(target)

        return links

    def link_with_fuzzy(self, target_class: str, keys: List[Key],
                        existing_links: Dict[str, str],
                        min_similarity: float = 0.7,
                        min_margin: float = 0.10) -> Dict[str, str]:
        """
        Last-resort fuzzy matching for entities not matched by exact keys.

        Computes token-Jaccard similarity over all string-valued key properties
        and links when the best KB2 candidate is clearly ahead of the runner-up.

        Args:
            min_similarity: Minimum Jaccard score for the best candidate.
            min_margin:     Required gap between best and second-best scores
                            (relative to best score) to avoid ambiguous matches.
        """
        links = dict(existing_links)
        used_kb2: Set[str] = set(existing_links.values())

        instances1 = self.kb1.get_instances(target_class)
        instances2 = self.kb2.get_instances(target_class)

        unlinked1 = [e for e in instances1 if e not in links]
        unlinked2 = [e for e in instances2 if e not in used_kb2]

        if not unlinked1 or not unlinked2:
            return links

        def token_set(entity: str, kb: KnowledgeBase) -> Set[str]:
            tokens: Set[str] = set()
            for key in keys:
                for prop in key.properties:
                    for v in kb.get_property_value(entity, prop):
                        if not (v.startswith('http://') or v.startswith('https://')):
                            tokens.update(v.lower().split())
            return tokens

        # Pre-compute token sets for unlinked KB2 entities
        kb2_tokens: Dict[str, Set[str]] = {
            e2: token_set(e2, self.kb2) for e2 in unlinked2
        }

        for entity1 in unlinked1:
            toks1 = token_set(entity1, self.kb1)
            if not toks1:
                continue

            best_sim = 0.0
            second_sim = 0.0
            best_match: Optional[str] = None

            for entity2, toks2 in kb2_tokens.items():
                if entity2 in used_kb2 or not toks2:
                    continue
                union_size = len(toks1 | toks2)
                if union_size == 0:
                    continue
                sim = len(toks1 & toks2) / union_size
                if sim > best_sim:
                    second_sim = best_sim
                    best_sim = sim
                    best_match = entity2
                elif sim > second_sim:
                    second_sim = sim

            if (best_match is not None
                    and best_match not in used_kb2
                    and best_sim >= min_similarity
                    and (second_sim == 0
                         or (best_sim - second_sim) / best_sim >= min_margin)):
                links[entity1] = best_match
                used_kb2.add(best_match)
                kb2_tokens.pop(best_match, None)

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


def count_reference_overlap(reference: Set[Tuple[str, str]],
                            kb1: KnowledgeBase,
                            kb2: KnowledgeBase) -> Tuple[int, int, int]:
    """Count how many reference pairs actually point to entities present in both KBs."""
    kb1_subjects = set(kb1.subjects.keys())
    kb2_subjects = set(kb2.subjects.keys())

    present_pairs = sum(1 for left, right in reference if left in kb1_subjects and right in kb2_subjects)
    missing_left = sum(1 for left, _ in reference if left not in kb1_subjects)
    missing_right = sum(1 for _, right in reference if right not in kb2_subjects)

    return present_pairs, missing_left, missing_right


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
    parser.add_argument('--refalign', default=None, help='Path to reference alignment (RDF)')
    parser.add_argument('--n', type=int, default=0, help='Maximum exceptions for n-almost keys')
    parser.add_argument('--max-depth', type=int, default=2, help='Maximum depth for graph keys')
    parser.add_argument('--no-eval', action='store_true',
                       help='Skip evaluation and only report discovered links')
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

    print("\n[3] Target classes for key discovery:")
    for tc in target_classes[:5]:  # Show first 5
        tc_name = tc.split('/')[-1]
        n1 = len(kb1.get_instances(tc))
        n2 = len(kb2.get_instances(tc))
        print(f"    - {tc_name}: {n1} instances in KB1, {n2} in KB2")

    # Load reference alignment
    reference = set()
    evaluation_enabled = not args.no_eval

    if evaluation_enabled:
        if not args.refalign:
            print("\n[4] No reference alignment provided; evaluation disabled.")
            evaluation_enabled = False
        else:
            print("\n[4] Loading reference alignment...")
            reference = parse_reference_alignment(args.refalign)
            print(f"    - {len(reference)} reference links")

            present_pairs, missing_left, missing_right = count_reference_overlap(reference, kb1, kb2)
            if len(reference) == 0:
                print("    - Warning: no <Cell> alignments found in the reference file.")
                print("      Evaluation will be skipped.")
                evaluation_enabled = False
            elif present_pairs == 0:
                print("    - Warning: reference entities do not occur in the loaded ABoxes.")
                print("      This usually means the alignment file belongs to a different dataset.")
                print("      Evaluation will be skipped.")
                evaluation_enabled = False
            elif present_pairs < len(reference):
                print(f"    - Warning: only {present_pairs}/{len(reference)} reference pairs match entities in both KBs")
                print(f"      Missing on left: {missing_left}, missing on right: {missing_right}")
                print("      Precision/recall numbers will be incomplete.")
    else:
        print("\n[4] Evaluation disabled (--no-eval)")

    # Filter target classes to only those whose instances appear in the reference alignment.
    # Processing classes with no reference coverage generates pure false positives.
    if reference:
        ref_left = {e1 for e1, e2 in reference}
        ref_right = {e2 for e1, e2 in reference}
        covered = []
        skipped = []
        for tc in target_classes:
            kb1_inst = kb1.get_instances(tc)
            kb2_inst = kb2.get_instances(tc)
            if (ref_left & kb1_inst) or (ref_right & kb2_inst):
                covered.append(tc)
            else:
                skipped.append(tc.split('/')[-1])
        if skipped:
            print(f"\n    Skipping {len(skipped)} class(es) not in reference: {', '.join(skipped)}")
        target_classes = covered

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

        # Cross-KB diagnostics: report KB2 exception counts (informational only)
        print("    [5.1b] Cross-KB key quality:")
        for key in simple_keys[:5]:
            kb2_key = sakey.validate_key_in_kb(key, kb2)
            exc = kb2_key.exceptions if kb2_key else 'N/A'
            print(f"           {key}: KB2 exceptions={exc}")

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

        # Link with simple keys (unambiguous signatures only)
        simple_links = linker.link_with_simple_keys(target_class, simple_keys)
        all_simple_links.update(simple_links)
        print(f"          Simple keys: {len(simple_links)} links")

        # Link with graph keys — adds new matches via object-property extensions
        graph_links = linker.link_entities(target_class, graph_keys, simple_links)
        new_from_graph = len(graph_links) - len(simple_links)
        print(f"          Graph keys: {len(graph_links)} links (+{new_from_graph} from extensions)")

        # Voting recovery — resolve ambiguous entities by combining evidence from
        # all simple keys and graph key extensions; link when ≥2 sources agree
        vote_links = linker.link_with_votes(
            target_class, simple_keys, graph_keys, graph_links
        )
        new_from_votes = len(vote_links) - len(graph_links)
        print(f"          Voting: {len(vote_links)} links (+{new_from_votes} recovered)")

        # Fuzzy matching — last resort for entities whose property values were
        # transformed by SPIMBENCH; uses token-Jaccard similarity
        fuzzy_links = linker.link_with_fuzzy(target_class, simple_keys, vote_links)
        new_from_fuzzy = len(fuzzy_links) - len(vote_links)
        all_graph_links.update(fuzzy_links)
        print(f"          Fuzzy:  {len(fuzzy_links)} links (+{new_from_fuzzy} recovered)")

    if evaluation_enabled:
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

        print("\n[8] Improvement Analysis:")
        if simple_eval['f_measure'] > 0:
            improvement = ((graph_eval['f_measure'] - simple_eval['f_measure']) /
                          simple_eval['f_measure'] * 100)
            print(f"    - F-measure improvement: {improvement:+.2f}%")
    else:
        print("\n" + "=" * 70)
        print("LINKING SUMMARY")
        print("=" * 70)
        print("\n[6] Evaluation skipped.")
        print("    - Use a dataset-specific alignment file to compute precision/recall/F-measure.")

    additional_links = len(all_graph_links) - len(all_simple_links)
    print("\n[9] Link Analysis:")
    print(f"    - Simple key links: {len(all_simple_links)}")
    print(f"    - Graph key links: {len(all_graph_links)}")
    print(f"    - Additional links from graph keys: {additional_links}")

    simple_set = set(all_simple_links.items())
    graph_set = set(all_graph_links.items())
    only_graph = graph_set - simple_set
    print(f"    - Links unique to graph keys: {len(only_graph)}")

    print("\n" + "=" * 70)
    print("Done!")


if __name__ == '__main__':
    main()
