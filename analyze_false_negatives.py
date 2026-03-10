#!/usr/bin/env python3
"""Analyze why some entities couldn't be matched."""

import sys
sys.path.insert(0, '/Users/idrissamahamoudoudicko/M2/Knowedle_Discovery/Project')

from graph_keys import (parse_ntriples, parse_reference_alignment, KnowledgeBase,
                       Ontology, SAKeyAlgorithm, GraphKeyBuilder, EntityLinker)

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

# Load reference alignment
reference = parse_reference_alignment('SPIMBENCH_small/refalign.rdf')
print(f"Reference alignments: {len(reference)}")

# Find all simple key links
all_links = {}
target_classes = set(kb1.instances_by_class.keys()) & set(kb2.instances_by_class.keys())

for tc in target_classes:
    if len(kb1.get_instances(tc)) > 5 and len(kb2.get_instances(tc)) > 5:
        sakey = SAKeyAlgorithm(kb1, ontology, n=2)
        simple_keys = sakey.discover_keys(tc)
        if simple_keys:
            linker = EntityLinker(kb1, kb2, ontology)
            links = linker.link_with_simple_keys(tc, simple_keys)
            all_links.update(links)

# Analyze false negatives (reference pairs not in links)
false_negatives = []
for e1, e2 in reference:
    if e1 not in all_links or all_links.get(e1) != e2:
        false_negatives.append((e1, e2))

print(f"\nFalse negatives: {len(false_negatives)}")

# Analyze why each false negative failed
if false_negatives:
    print("\nAnalyzing sample false negatives:")
    for e1, e2 in false_negatives[:5]:
        print(f"\n  KB1: {e1.split('/')[-1]}")
        print(f"  KB2: {e2.split('/')[-1]}")

        # Get class
        e1_classes = kb1.get_property_value(e1, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
        e2_classes = kb2.get_property_value(e2, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
        print(f"  Classes KB1: {[c.split('/')[-1] for c in e1_classes]}")
        print(f"  Classes KB2: {[c.split('/')[-1] for c in e2_classes]}")

        # Compare a key property (e.g., rdfs:label)
        for prop_suffix in ['label', 'comment', 'title', 'name', 'description']:
            prop1_vals = None
            prop2_vals = None
            for prop, vals in kb1.subjects.get(e1, []):
                if prop_suffix in prop:
                    prop1_vals = vals
                    break
            for prop, vals in kb2.subjects.get(e2, []):
                if prop_suffix in prop:
                    prop2_vals = vals
                    break
            if prop1_vals or prop2_vals:
                match_indicator = "✓" if prop1_vals == prop2_vals else "✗"
                # Get the actual values
                p1_val = None
                p2_val = None
                for prop, val in kb1.subjects.get(e1, []):
                    if prop_suffix in prop:
                        p1_val = val[:50] if len(val) > 50 else val
                        break
                for prop, val in kb2.subjects.get(e2, []):
                    if prop_suffix in prop:
                        p2_val = val[:50] if len(val) > 50 else val
                        break
                print(f"  {prop_suffix}: {match_indicator}")
                if p1_val != p2_val:
                    print(f"    KB1: {p1_val}")
                    print(f"    KB2: {p2_val}")
