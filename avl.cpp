#include <assert.h>
#include "avl.h"

static uint32_t max(uint32_t lhs, uint32_t rhs) { return lhs < rhs ? rhs : lhs; }

// Maintain the height and count field
static void avl_update(AVLNode *node)
{
    node->height = 1 + max(avl_height(node->left), avl_height(node->right));
    node->count = 1 + avl_count(node->left) + avl_count(node->right);
}

static AVLNode *rot_left(AVLNode *node)
{
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->right;
    AVLNode *inner = new_node->left;

    // Inner <-> Node
    node->right = inner;
    if (inner)
        inner->parent = node;

    // New_node -> Parent
    new_node->parent = parent;

    // New_node <-> Node
    node->parent = new_node;
    new_node->left = node;

    // Auxillary data
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *rot_right(AVLNode *node)
{
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->left;
    AVLNode *inner = new_node->right;

    // Inner <-> Node
    node->left = inner;
    if (inner)
        inner->parent = node;

    // New node -> parent
    new_node->parent = parent;

    // New_node <-> Node
    node->parent = new_node;
    new_node->right = node;

    // Auxillary data
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

// The left subtree is taller by 2
static AVLNode *avl_fix_left(AVLNode *node)
{
    if (avl_height(node->left->left) < avl_height(node->left->right))
        node->left = rot_left(node->left);
    return rot_right(node);
}

// The right subtree is taller by 2
static AVLNode *avl_fix_right(AVLNode *node)
{
    if (avl_height(node->right->right) < avl_height(node->right->left))
        node->right = rot_right(node->right);
    return rot_left(node);
}

// Fix imbalanced nodes and maintain invariants until root is reached
AVLNode *avl_fix(AVLNode *node)
{
    while (true) // Returns the root pointer
    {
        AVLNode **from = &node; // Save the fixed subtree here
        AVLNode *parent = node->parent;
        if (parent) // Attached the fixed subtree parent
        {
            from = parent->left == node ? &parent->left : &parent->right;
        } // Else: save to the local variable node
        avl_update(node); // Auxillary data

        // Fix the the height difference
        uint32_t l = avl_height(node->left);
        uint32_t r = avl_height(node->right);
        if (l == r + 2)
            *from = avl_fix_left(node);
        else if (r == l + 2)
            *from = avl_fix_right(node);

        if (!parent)
            return *from; // Root node, stop
        node = parent;    // Continue to the parent node
    }
}

// Detach a node when one of its children is empty
static AVLNode *avl_del_easy(AVLNode *node)
{
    assert(!node->left || !node->right);                    // Atmost 1 child
    AVLNode *child = node->left ? node->left : node->right; // Can be NULL
    AVLNode *parent = node->parent;
    if (child)
        child->parent = parent;
    if (!parent) // Root node
        return child;
    AVLNode **from = parent->left == node ? &parent->left : &parent->right;
    *from = child;
    return avl_fix(parent); // Rebalance the updated tree and returns the root poitner
}

AVLNode *avl_del(AVLNode *node)
{
    if (!node->left || !node->right) // Easy case of 0 or 1 child
        return avl_del_easy(node);

    // Find the successor
    AVLNode *victim = node->right;
    while (victim->left)
        victim = victim->left;

    // Detach the successor and swap with node
    AVLNode *root = avl_del_easy(victim);
    *victim = *node; // Left, right, parent
    if (victim->left)
        victim->left->parent = victim;
    if (victim->right)
        victim->right->parent = victim;

    // Attach the successor to the parent or update the root pointer
    AVLNode **from = &root;
    AVLNode *parent = node->parent;
    if (parent)
        from = parent->left == node ? &parent->left : &parent->right;
    *from = victim;
    return root;
}

// Offset into the succeding or preceding node
AVLNode *avl_offset(AVLNode *node, int64_t offset)
{
    int64_t pos = 0; // Rank difference from the starting node
    while (offset != pos)
    {
        if (pos < offset && pos + avl_count(node->right) >= offset) // Target is in the right subtree
        {
            node = node->right;
            pos += (avl_count(node->left) + 1);
        }
        else if (pos > offset && pos - avl_count(node->left) <= offset) // Target is in the left subtree
        {
            node = node->left;
            pos -= (avl_count(node->right) + 1);
        }
        else // Go to the parent
        {
            AVLNode *parent = node->parent;
            if (!parent)
                return NULL;
            else if (parent->left == node)
                pos += (avl_count(node->right) + 1);
            else
                pos -= (avl_count(node->left) + 1);
            node = parent;
        }
    }
    return node;
}