#pragma once

#include "avl.h"
#include "hashtable.h"

struct ZSet
{
    AVLNode *root; // Index by (score, name)
    HMap hmap; // Index by (name)
} ;

struct ZNode
{
    AVLNode tree;
    HNode hmap;
    double score=0;
    size_t len=0;
    char name[0]; // Flexible array
} ;

bool zset_insert(ZSet *zset, double score, const char *name, size_t len);
ZNode * zset_lookup(ZSet *zset, const char *name, size_t len);
void zset_delete(ZSet *zset, ZNode *znode);
ZNode * zset_seekge(ZSet *zset, double score, const char *name, size_t len);
void zset_clear(ZSet *zset);
ZNode * znode_offset(ZNode *node, int64_t offset);
