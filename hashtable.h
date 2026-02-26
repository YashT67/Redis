#pragma once

#include <stddef.h>
#include <stdint.h>

// Hashtable node
struct HNode
{
    HNode *next = NULL;
    uint64_t hcode = 0;
};

// A simple fixed size hashtable
struct HTab
{
    HNode **tab = NULL;
    size_t mask = 0; // 2^n-1
    size_t size = 0; // Number of keys
};

//  The real hashtable interface that uses 2 hashtbales for progressive rehashing
struct HMap
{
    HTab newer;
    HTab older;
    size_t migrate_pos = 0;
};

// Function signatures
HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_insert(HMap *hmap, HNode *node);
HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_clear(HMap *hmap);
size_t hm_size(HMap *hmap);
void hm_foreach(HMap *hmap, bool (*f)(HNode *, void *), void *args); // Invoke the callback on each node until it returns false