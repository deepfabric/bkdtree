# Roadmap

This document defines the roadmap for BKD tree development.

#### KD tree (memory)
- [D] build - Kdtree in mem
- [D] intersect - Kdtree in mem
- [D] insert - Kdtree in mem
- [D] erase - Kdtree in mem

##### BKD tree (memory + file)
- [D] build
- [D] insert 
- [D] erase
- [D] intersect
- [D] compatible file format to allow multiple versions
- [ ] performance optimization - mmap, splice, point encoding/decoding etc.
- [ ] disaster recovery - insert/erase binlog
- [ ] disaster recovery - rebuild
- [ ] concurrent access - singel writer, multiple reader
- [ ] concurrent access - background compact
