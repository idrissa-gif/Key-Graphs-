# Graph Keys: Extending Classical Keys for Entity Linking

A tool for discovering and extending classical keys to graph keys for RDF knowledge base entity linking, based on the SAKey algorithm and the concept of "Keys for Graphs".

## Overview

This project implements:
- **SAKey-like key discovery**: Finds n-almost keys (sets of properties that uniquely identify entities with at most n exceptions)
- **Graph key extension**: Extends classical keys by following object properties to discover nested keys for linked entities
- **Entity linking**: Uses discovered keys to match entities across two knowledge bases
- **Evaluation**: Computes precision, recall, and F-measure against reference alignments

## Features

- Parse N-Triples (.nt) RDF files
- Build ontology schema from TBox (domain/range, class hierarchy)
- Data-driven range class detection (follows actual property links)
- Subclass reasoning for flexible class matching
- Configurable n-almost key threshold

## Usage

```bash
python graph_keys.py \
    --abox1 SPIMBENCH_small/Abox1.nt \
    --abox2 SPIMBENCH_small/Abox2.nt \
    --tbox SPIMBENCH_small/Tbox1.nt \
    --refalign SPIMBENCH_small/refalign.rdf \
    --n 2
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `--abox1` | Path to first knowledge base (N-Triples) |
| `--abox2` | Path to second knowledge base (N-Triples) |
| `--tbox` | Path to ontology schema (N-Triples) |
| `--refalign` | Path to reference alignment (RDF/XML) |
| `--n` | Maximum exceptions allowed for n-almost keys (default: 0) |
| `--max-depth` | Maximum depth for graph key extensions (default: 2) |
| `--target-class` | Specific class to process (optional) |

## Dataset

Uses SPIMBENCH benchmark dataset with:
- `Abox1.nt` / `Abox2.nt`: Instance data (~10K triples each)
- `Tbox1.nt`: Ontology schema (~11K triples)
- `refalign.rdf`: Reference entity alignments (299 pairs)

## Results (SPIMBENCH_small)

| Metric | Simple Keys | Graph Keys |
|--------|-------------|------------|
| Precision | 44.75% | 44.75% |
| Recall | 76.92% | 76.92% |
| F-measure | 56.58% | 56.58% |

## Architecture

```
graph_keys.py
├── parse_ntriples()          # N-Triples parser
├── KnowledgeBase             # RDF data storage with indexing
├── Ontology                  # Schema with domain/range/hierarchy
├── SAKeyAlgorithm            # Key discovery
├── GraphKeyBuilder           # Extends keys via object properties
├── EntityLinker              # Links entities using keys
└── evaluate_links()          # Precision/Recall/F-measure
```

## References

- SAKey: Scalable Almost Key Discovery in RDF Data
- Keys for Graphs (PODS 2014)

## Author

Idrissa Mahamoudou Dicko - M2 Knowledge Discovery Project
