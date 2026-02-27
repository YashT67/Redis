#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
// Proj
#include "common.h"
#include "zset.h"

static ZNode *znode_new(double score, const char *name, size_t len)
{
    ZNode *node = (ZNode *)malloc(sizeof(ZNode) + len);
    if (!node)
        return NULL;
    avl_init(&node->tree);
    node->hmap.next = NULL;
    node->hmap.hcode = str_hash((uint8_t *)name, len);
    node->len = len;
    node->score = score;
    memcpy(&node->name[0], name, len);
    return node;
}

static void znode_del(ZNode *node) { free(node); }

static size_t min(size_t lhs, size_t rhs) { return lhs < rhs ? lhs : rhs; }

// Compare lhs node with (score, name) tuple and returns true if lhs < (score, name)
static bool zless(AVLNode *lhs, double score, const char *name, size_t len)
{
    ZNode *zl = container_of(lhs, ZNode, tree);
    if (zl->score != score)
        return zl->score < score;
    int rv = memcmp(zl->name, name, min(len, zl->len));
    if (rv)
        return rv < 0;
    return zl->len < len;
}

// Compare lhs node with rhs node and returns true if lhs < rhs
static bool zless(AVLNode *lhs, AVLNode *rhs)
{
    ZNode *zr = container_of(rhs, ZNode, tree);
    return zless(lhs, zr->score, zr->name, zr->len);
}

// Insert into the avl tree
static void tree_insert(ZSet *zset, ZNode *node)
{
    AVLNode *parent = NULL;
    AVLNode **from = &zset->root;
    while (*from)
    {
        parent = *from;
        from = zless(&node->tree, parent) ? &parent->left : &parent->right;
    }
    *from = &node->tree;
    node->tree.parent = parent;
    zset->root = avl_fix(&node->tree);
}

// Update the score of an existing node
static void zset_update(ZSet *zset, ZNode *node, double score)
{
    if (node->score == score)
        return;
    zset->root = avl_del(&node->tree); // Detach the current node
    avl_init(&node->tree);
    node->score = score;
    tree_insert(zset, node); // Reinsert the node
}

// A helper function for the hashtable lookup
struct HKey
{
    HNode node;
    const char *name = NULL;
    size_t len = 0;
};

// Compare two hnodes by names
static bool hcmp(HNode *node, HNode *key)
{
    ZNode *znode = container_of(node, ZNode, hmap);
    HKey *hkey = container_of(key, HKey, node);
    if (znode->len != hkey->len)
        return false;
    return memcmp(znode->name, hkey->name, hkey->len) == 0;
}

// Lookup by name
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len)
{
    if (!zset->root)
        return NULL;
    HKey key;
    key.node.hcode = str_hash((uint8_t *)name, len);
    key.len = len;
    key.name = name;
    HNode *found = hm_lookup(&zset->hmap, &key.node, hcmp);
    return found ? container_of(found, ZNode, hmap) : NULL;
}

// Add a new (score, name) tuple or update the score of existing one
bool zset_insert(ZSet *zset, double score, const char *name, size_t len)
{
    ZNode *node = zset_lookup(zset, name, len);
    if (node) // Found, update
    {
        zset_update(zset, node, score);
        return false;
    }
    else // Add a new one
    {
        node = znode_new(score, name, len);
        if (!node) // Malloc error: Out of Memory
        {
            fprintf(stderr, "Warning: Failed to insert item due to OOM.\n");
            return false;
        }
        hm_insert(&zset->hmap, &node->hmap);
        tree_insert(zset, node);
        return true;
    }
}

// Delete a node
void zset_delete(ZSet *zset, ZNode *node)
{
    HKey key;
    key.node.hcode = node->hmap.hcode;
    key.len = node->len;
    key.name = node->name;
    HNode *found = hm_delete(&zset->hmap, &key.node, &hcmp); // Remove from the hashtable
    assert(found);
    zset->root = avl_del(&node->tree); // Remove from the tree
    znode_del(node);                   // Deallocate the node
}

// Seek (score, name) greater than equal to key
ZNode *zset_seekge(ZSet *zset, double score, const char *name, size_t len)
{
    AVLNode *found = NULL;
    for (AVLNode *node = zset->root; node;)
    {
        if (zless(node, score, name, len))
            node = node->right; // Node < key
        else
        {
            found = node; // Candidate
            node = node->left;
        }
    }
    return found ? container_of(found, ZNode, tree) : NULL;
}

// Offset into the succeding or the precceding node
ZNode *znode_offset(ZNode *node, int64_t offset)
{
    AVLNode *tnode = node ? avl_offset(&node->tree, offset) : NULL;
    return tnode ? container_of(tnode, ZNode, tree) : NULL;
}

static void tree_dispose(AVLNode *node)
{
    if (!node)
        return;
    tree_dispose(node->right);
    tree_dispose(node->left);
    znode_del(container_of(node, ZNode, tree));
}

// Destroy the zset
void zset_clear(ZSet *zset)
{
    hm_clear(&zset->hmap);
    tree_dispose(zset->root);
    zset->root = NULL;
}