#!/usr/bin/env python3
import re
from typing import Set, Tuple

def parse_reference_alignment(filepath: str) -> Set[Tuple[str, str]]:
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

ref = parse_reference_alignment('SPIMBENCH_small/refalign.rdf')
print(f'Found {len(ref)} alignments')
if ref:
    for i, (e1, e2) in enumerate(list(ref)[:5]):
        print(f'{e1} -> {e2}')
