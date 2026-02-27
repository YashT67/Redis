#pragma once

#include <stddef.h>
#include <stdint.h>

struct AVLNode {
    struct AVLNode *left=NULL;
    struct AVLNode *right=NULL;
    struct AVLNode *parent =NULL;
    uint32_t height=0; // Max height/depth
    uint32_t count=0; // Subtree size
} ;

inline void avl_init(AVLNode *node)
{
    node->left=node->right=node->parent=NULL;
    node->height=1;
    node->count=1;
}

// Helpers
inline uint32_t avl_height(AVLNode *node) { return node? node->height:0;}
inline uint32_t avl_count(AVLNode *node) { return node? node->count:0;}

// Apis
AVLNode * avl_fix(AVLNode *node);
AVLNode * avl_del(AVLNode *node);
AVLNode * avl_offset(AVLNode *node, int64_t offset);