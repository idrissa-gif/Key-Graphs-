#!/usr/bin/env python3
"""Debug signatures for graph key matching."""

import sys
sys.path.insert(0, '/Users/idrissamahamoudoudicko/M2/Knowedle_Discovery/Project')

from graph_keys import (parse_ntriples, KnowledgeBase, Ontology,
                       SAKeyAlgorithm, GraphKeyBuilder, EntityLinker)

# Load data
print("Loading data...")
abox1_triples = list(parse_ntriples('SPIMBENCH_small/Abox1.nt'))
abox2_triples = list(parse_ntriples('SPIMBENCH_small/Abox2.nt'))
tbox_triples = list(parse_ntriples('SPIMBENCH_small/Tbox1.nt'))

kb1 = KnowledgeBase()
for s, p, o in abox1_triples:
    kb1.add_triple(s, p, o)

kb2 = KnowledgeBase()
for s, p, o in abox2_triples:
    kb2.add_triple(s, p, o)

ontology = Ontology.from_triples(tbox_triples)

# Find NewsItem class
newsitem_uri = None
for c in kb1.instances_by_class.keys():
    if 'NewsItem' in c:
        newsitem_uri = c
        break

print(f"NewsItem URI: {newsitem_uri}")

# Build keys
sakey = SAKeyAlgorithm(kb1, ontology, n=2)
simple_keys = sakey.discover_keys(newsitem_uri)

print(f"\nSimple keys discovered:")
for key in simple_keys:
    props = [p.split('/')[-1] for p in key.properties]
    print(f"  {props}: {key.exceptions} exceptions")

# Build graph keys
builder = GraphKeyBuilder(kb1, ontology, sakey, max_depth=2)
graph_keys = builder.build_all_graph_keys(newsitem_uri)

print(f"\nGraph keys with extensions:")
for gk in graph_keys:
    if gk.extensions:
        print(f"  {gk}")

# Pick one entity from each KB
instances1 = list(kb1.get_instances(newsitem_uri))[:3]
instances2 = list(kb2.get_instances(newsitem_uri))[:3]

linker = EntityLinker(kb1, kb2, ontology)

# For the graph key with extension (about)
about_gk = None
for gk in graph_keys:
    if gk.extensions:
        about_gk = gk
        break

if about_gk:
    print(f"\nAnalyzing graph key: {about_gk}")

    # Check a sample entity
    for e1 in instances1[:2]:
        print(f"\n  Entity from KB1: {e1.split('/')[-1]}")

        # Get the about values
        about_prop = [p for p in about_gk.base_key.properties
                     if p in about_gk.extensions][0]
        about_vals = kb1.get_property_value(e1, about_prop)
        print(f"  'about' links to: {[v.split('/')[-1] for v in about_vals]}")

        # Check what class the linked entities are
        for val in about_vals:
            val_class = kb1.get_property_value(val, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
            print(f"    -> class: {[c.split('/')[-1] for c in val_class]}")
            # Check if extension key property exists
            ext_key = about_gk.extensions[about_prop]
            for prop in ext_key.base_key.properties:
                vals = kb1.get_property_value(val, prop)
                print(f"    -> {prop.split('/')[-1]}: {vals}")

        # Compute signature
        sig = linker.compute_signature(e1, about_gk, kb1)
        print(f"  Signature: {sig}")

# Check how many signatures match
print("\n\nComparing signatures across KBs:")
if about_gk:
    # Build all signatures for KB1 and KB2
    sigs1 = {}
    for e1 in instances1:
        sig = linker.compute_signature(e1, about_gk, kb1)
        if sig:
            sigs1[sig] = e1

    sigs2 = {}
    for e2 in kb2.get_instances(newsitem_uri):
        sig = linker.compute_signature(e2, about_gk, kb2)
        if sig:
            sigs2[sig] = e2

    matching = set(sigs1.keys()) & set(sigs2.keys())
    print(f"  KB1 signatures: {len(sigs1)}")
    print(f"  KB2 signatures: {len(sigs2)}")
    print(f"  Matching signatures: {len(matching)}")
