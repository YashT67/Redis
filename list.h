#pragma once

#include <stddef.h>

struct DList
{
    DList *next=NULL;
    DList *prev=NULL;
} ;

inline void  dlist_init(DList *node) { node->next=node->prev=node; }

inline bool dlist_empty(DList *node) { return node->next==node; }

inline void dlist_detach(DList *node)
{
    DList *prev=node->prev;
    DList *next=node->next;
    prev->next=next;
    next->prev=prev;
}

inline void dlist_insert_before(DList *target, DList *node)
{
    DList *prev=target->prev;
    node->prev=prev;
    node->next=target;
    prev->next=node;
    target->prev=node;
}