#!/usr/bin/env python3
"""Debug why graph key extensions aren't being found."""

import sys
sys.path.insert(0, '/Users/idrissamahamoudoudicko/M2/Knowedle_Discovery/Project')

from graph_keys import parse_ntriples, KnowledgeBase, Ontology, SAKeyAlgorithm

# Load data
print("Loading TBox...")
tbox_triples = list(parse_ntriples('SPIMBENCH_small/Tbox1.nt'))
ontology = Ontology.from_triples(tbox_triples)

print(f"\nObject properties: {len(ontology.object_properties)}")
print("Sample object properties:")
for prop in list(ontology.object_properties)[:10]:
    print(f"  {prop}")
    if ontology.get_range(prop):
        print(f"    range: {ontology.get_range(prop)}")
    if ontology.get_domain(prop):
        print(f"    domain: {ontology.get_domain(prop)}")

# Load KB1
print("\nLoading ABox1...")
abox1_triples = list(parse_ntriples('SPIMBENCH_small/Abox1.nt'))
kb1 = KnowledgeBase()
for s, p, o in abox1_triples:
    kb1.add_triple(s, p, o)

# Find NewsItem keys
print("\n--- NewsItem analysis ---")
newsitem_uri = None
for c in kb1.instances_by_class.keys():
    if 'NewsItem' in c:
        newsitem_uri = c
        break

if newsitem_uri:
    print(f"NewsItem class URI: {newsitem_uri}")

    sakey = SAKeyAlgorithm(kb1, ontology, n=2)
    keys = sakey.discover_keys(newsitem_uri)

    print(f"\nDiscovered {len(keys)} keys:")
    for key in keys[:5]:
        for prop in key.properties:
            print(f"  Property: {prop}")
            print(f"    Is object prop? {ontology.is_object_property(prop)}")
            if ontology.is_object_property(prop):
                range_c = ontology.get_range(prop)
                print(f"    Range: {range_c}")

# Check about property specifically
print("\n--- Looking for 'about' in object properties ---")
about_props = [p for p in ontology.object_properties if 'about' in p.lower()]
print(f"Found {len(about_props)} object properties with 'about':")
for p in about_props:
    print(f"  {p}")
    print(f"    domain: {ontology.get_domain(p)}")
    print(f"    range: {ontology.get_range(p)}")

# Check instances of Thing and related classes
print("\n--- Checking instances of range classes ---")
thing_uri = 'http://www.bbc.co.uk/ontologies/coreconcepts/Thing'
thing_instances = kb1.get_instances(thing_uri)
print(f"Instances of Thing: {len(thing_instances)}")

# Check what classes exist in KB1
print("\n--- Classes with instances in KB1 ---")
for c, insts in kb1.instances_by_class.items():
    if len(insts) > 5:
        print(f"  {c}: {len(insts)} instances")

# Check subclasses of Thing in ontology
print("\n--- Subclasses of Thing in ontology ---")
for cls, parents in ontology.subclass_of.items():
    if thing_uri in parents:
        print(f"  {cls} rdfs:subClassOf Thing")
        instances = kb1.get_instances(cls)
        print(f"    Instances: {len(instances)}")
