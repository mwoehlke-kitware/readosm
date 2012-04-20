/* 
/ readosm.c
/
/ ReadOSM implementation
/
/ version  1.0, 2012 April 10
/
/ Author: Sandro Furieri a.furieri@lqt.it
/
/ ------------------------------------------------------------------------------
/ 
/ Version: MPL 1.1/GPL 2.0/LGPL 2.1
/ 
/ The contents of this file are subject to the Mozilla Public License Version
/ 1.1 (the "License"); you may not use this file except in compliance with
/ the License. You may obtain a copy of the License at
/ http://www.mozilla.org/MPL/
/ 
/ Software distributed under the License is distributed on an "AS IS" basis,
/ WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
/ for the specific language governing rights and limitations under the
/ License.
/
/ The Original Code is the ReadOSM library
/
/ The Initial Developer of the Original Code is Alessandro Furieri
/ 
/ Portions created by the Initial Developer are Copyright (C) 2012
/ the Initial Developer. All Rights Reserved.
/ 
/ Contributor(s):
/ 
/ Alternatively, the contents of this file may be used under the terms of
/ either the GNU General Public License Version 2 or later (the "GPL"), or
/ the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
/ in which case the provisions of the GPL or the LGPL are applicable instead
/ of those above. If you wish to allow use of your version of this file only
/ under the terms of either the GPL or the LGPL, and not to allow others to
/ use your version of this file under the terms of the MPL, indicate your
/ decision by deleting the provisions above and replace them with the notice
/ and other provisions required by the GPL or the LGPL. If you do not delete
/ the provisions above, a recipient may use your version of this file under
/ the terms of any one of the MPL, the GPL or the LGPL.
/ 
*/

#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <time.h>

#include <expat.h>
#include <zlib.h>

#include "readosm.h"
#include "readosm_internals.h"

#if defined(_WIN32) && !defined(__MINGW32__)
#define strcasecmp	_stricmp
#endif /* not WIN32 */

#if defined(_WIN32)
#define atol_64		_atoi64
#else
#define atol_64		atoll
#endif

#define BUFFSIZE	8192

struct xml_params
{
/* an helper struct supporting XML parsing */
    int current_tag;
    const void *user_data;
    readosm_node_callback node_callback;
    readosm_way_callback way_callback;
    readosm_relation_callback relation_callback;
    readosm_internal_node node;
    readosm_internal_way way;
    readosm_internal_relation relation;
    int stop;
};

struct pbf_params
{
/* an helper struct supporting PBF parsing */
    const void *user_data;
    readosm_node_callback node_callback;
    readosm_way_callback way_callback;
    readosm_relation_callback relation_callback;
    int stop;
};

static readosm_internal_tag *
alloc_internal_tag (void)
{
/* allocating an empty internal TAG object */
    readosm_internal_tag *tag = malloc (sizeof (readosm_internal_tag));
    tag->key = NULL;
    tag->value = NULL;
    tag->next = NULL;
    return tag;
}

static void
destroy_internal_tag (readosm_internal_tag * tag)
{
/* destroying an internal TAG object */
    if (tag == NULL)
	return;
    if (tag->key)
	free (tag->key);
    if (tag->value)
	free (tag->value);
    free (tag);
}

static void
init_export_tag (readosm_export_tag * tag)
{
/* initializing an empty export TAG object */
    if (tag == NULL)
	return;
    tag->key = NULL;
    tag->value = NULL;
}

static void
reset_export_tag (readosm_export_tag * tag)
{
/* resetting an export TAG object to initial empty state */
    if (tag == NULL)
	return;
    if (tag->key)
	free (tag->key);
    if (tag->value)
	free (tag->value);
    init_export_tag (tag);
}

static void
init_internal_node (readosm_internal_node * node)
{
/* allocating an empty internal NODE object */
    node->id = READOSM_UNDEFINED;
    node->latitude = READOSM_UNDEFINED;
    node->longitude = READOSM_UNDEFINED;
    node->version = READOSM_UNDEFINED;
    node->changeset = READOSM_UNDEFINED;
    node->user = NULL;
    node->uid = READOSM_UNDEFINED;
    node->timestamp = NULL;
    node->tag_count = 0;
    node->first_tag = NULL;
    node->last_tag = NULL;
}

static void
append_tag_to_node (readosm_internal_node * node, const char *key,
		    const char *value)
{
/* appending a TAG to a Node object */
    int len;
    readosm_internal_tag *tag = alloc_internal_tag ();
    len = strlen (key);
    tag->key = malloc (len + 1);
    strcpy (tag->key, key);
    len = strlen (value);
    tag->value = malloc (len + 1);
    strcpy (tag->value, value);
    if (node->first_tag == NULL)
	node->first_tag = tag;
    if (node->last_tag != NULL)
	node->last_tag->next = tag;
    node->last_tag = tag;
}

static void
destroy_internal_node (readosm_internal_node * node)
{
/* destroying an internal NODE object */
    readosm_internal_tag *tag;
    readosm_internal_tag *tag_n;
    if (node == NULL)
	return;
    if (node->user)
	free (node->user);
    if (node->timestamp)
	free (node->timestamp);
    tag = node->first_tag;
    while (tag)
      {
	  tag_n = tag->next;
	  destroy_internal_tag (tag);
	  tag = tag_n;
      }
}

static void
init_export_node (readosm_export_node * node)
{
/* initializing an empty export NODE object */
    if (node == NULL)
	return;
    node->id = READOSM_UNDEFINED;
    node->latitude = READOSM_UNDEFINED;
    node->longitude = READOSM_UNDEFINED;
    node->version = READOSM_UNDEFINED;
    node->changeset = READOSM_UNDEFINED;
    node->user = NULL;
    node->uid = READOSM_UNDEFINED;
    node->timestamp = NULL;
    node->tag_count = 0;
    node->tags = NULL;
}

static void
reset_export_node (readosm_export_node * node)
{
/* resetting an export NODE object to initial empty state */
    int i;
    if (node == NULL)
	return;
    if (node->user)
	free (node->user);
    if (node->timestamp)
	free (node->timestamp);
    for (i = 0; i < node->tag_count; i++)
      {
	  readosm_export_tag *tag = node->tags + i;
	  reset_export_tag (tag);
      }
    if (node->tags)
	free (node->tags);
    init_export_node (node);
}

static readosm_internal_ref *
alloc_internal_ref (void)
{
/* allocating an empty internal NODE-REF object */
    readosm_internal_ref *ref = malloc (sizeof (readosm_internal_ref));
    ref->node_ref = 0;
    ref->next = NULL;
    return ref;
}

static void
destroy_internal_ref (readosm_internal_ref * ref)
{
/* destroying an internal NODE-REF object */
    if (ref == NULL)
	return;
    free (ref);
}

static readosm_internal_way *
alloc_internal_way (void)
{
/* allocating an empty internal WAY object */
    readosm_internal_way *way = malloc (sizeof (readosm_internal_way));
    way->id = 0;
    way->version = 0;
    way->changeset = 0;
    way->user = NULL;
    way->uid = 0;
    way->timestamp = NULL;
    way->ref_count = 0;
    way->first_ref = NULL;
    way->last_ref = NULL;
    way->tag_count = 0;
    way->first_tag = NULL;
    way->last_tag = NULL;
    return way;
}

static void
append_reference_to_way (readosm_internal_way * way, long long node_ref)
{
/* appending a NODE-REF to a WAY object */
    readosm_internal_ref *ref = alloc_internal_ref ();
    ref->node_ref = node_ref;
    if (way->first_ref == NULL)
	way->first_ref = ref;
    if (way->last_ref != NULL)
	way->last_ref->next = ref;
    way->last_ref = ref;
}

static void
append_tag_to_way (readosm_internal_way * way, const char *key,
		   const char *value)
{
/* appending a TAG to a WAY object */
    int len;
    readosm_internal_tag *tag = alloc_internal_tag ();
    len = strlen (key);
    tag->key = malloc (len + 1);
    strcpy (tag->key, key);
    len = strlen (value);
    tag->value = malloc (len + 1);
    strcpy (tag->value, value);
    if (way->first_tag == NULL)
	way->first_tag = tag;
    if (way->last_tag != NULL)
	way->last_tag->next = tag;
    way->last_tag = tag;
}

static void
destroy_internal_way (readosm_internal_way * way)
{
/* destroying an internal WAY object */
    readosm_internal_ref *ref;
    readosm_internal_ref *ref_n;
    readosm_internal_tag *tag;
    readosm_internal_tag *tag_n;
    if (way == NULL)
	return;
    if (way->user)
	free (way->user);
    if (way->timestamp)
	free (way->timestamp);
    ref = way->first_ref;
    while (ref)
      {
	  ref_n = ref->next;
	  destroy_internal_ref (ref);
	  ref = ref_n;
      }
    tag = way->first_tag;
    while (tag)
      {
	  tag_n = tag->next;
	  destroy_internal_tag (tag);
	  tag = tag_n;
      }
    free (way);
}

static void
init_export_way (readosm_export_way * way)
{
/* initializing an empty export WAY object */
    if (way == NULL)
	return;
    way->id = 0;
    way->version = 0;
    way->changeset = 0;
    way->user = NULL;
    way->uid = 0;
    way->timestamp = NULL;
    way->node_ref_count = 0;
    way->node_refs = NULL;
    way->tag_count = 0;
    way->tags = NULL;
}

static void
reset_export_way (readosm_export_way * way)
{
/* resetting an export WAY object to initial empty state */
    int i;
    if (way == NULL)
	return;
    if (way->user)
	free (way->user);
    if (way->timestamp)
	free (way->timestamp);
    if (way->node_refs)
	free (way->node_refs);
    for (i = 0; i < way->tag_count; i++)
      {
	  readosm_export_tag *tag = way->tags + i;
	  reset_export_tag (tag);
      }
    if (way->tags)
	free (way->tags);
    init_export_way (way);
}

static readosm_internal_member *
alloc_internal_member (void)
{
/* allocating an empty internal RELATION-MEMBER object */
    readosm_internal_member *member = malloc (sizeof (readosm_internal_member));
    member->member_type = READOSM_UNDEFINED;
    member->id = 0;
    member->role = NULL;
    member->next = NULL;
    return member;
}

static void
destroy_internal_member (readosm_internal_member * member)
{
/* destroying an internal RELATION-MEMBER object */
    if (member == NULL)
	return;
    if (member->role)
	free (member->role);
    free (member);
}

static void
init_export_member (readosm_export_member * member)
{
/* initializing an empty export RELATION-MEMBER object */
    if (member == NULL)
	return;
    member->member_type = READOSM_UNDEFINED;
    member->id = 0;
    member->role = NULL;
}

static void
reset_export_member (readosm_export_member * member)
{
/* resetting an export RELATION-MEMBER object to initial empty state */
    if (member == NULL)
	return;
    if (member->role)
	free (member->role);
    init_export_member (member);
}

static readosm_internal_relation *
alloc_internal_relation (void)
{
/* allocating an empty internal RELATION object */
    readosm_internal_relation *rel =
	malloc (sizeof (readosm_internal_relation));
    rel->id = 0;
    rel->version = 0;
    rel->changeset = 0;
    rel->user = NULL;
    rel->uid = 0;
    rel->timestamp = NULL;
    rel->member_count = 0;
    rel->first_member = NULL;
    rel->last_member = NULL;
    rel->tag_count = 0;
    rel->first_tag = NULL;
    rel->last_tag = NULL;
    return rel;
}

static void
append_member_to_relation (readosm_internal_relation * relation, int type,
			   long long id, const char *role)
{
/* appending a RELATION-MEMBER to a RELATION object */
    int len;
    readosm_internal_member *member = alloc_internal_member ();
    switch (type)
      {
      case 0:
	  member->member_type = READOSM_MEMBER_NODE;
	  break;
      case 1:
	  member->member_type = READOSM_MEMBER_WAY;
	  break;
      case 2:
	  member->member_type = READOSM_MEMBER_RELATION;
	  break;
      };
    member->id = id;
    len = strlen (role);
    member->role = malloc (len + 1);
    strcpy (member->role, role);
    if (relation->first_member == NULL)
	relation->first_member = member;
    if (relation->last_member != NULL)
	relation->last_member->next = member;
    relation->last_member = member;
}

static void
append_tag_to_relation (readosm_internal_relation * relation, const char *key,
			const char *value)
{
/* appending a TAG to a RELATION object */
    int len;
    readosm_internal_tag *tag = alloc_internal_tag ();
    len = strlen (key);
    tag->key = malloc (len + 1);
    strcpy (tag->key, key);
    len = strlen (value);
    tag->value = malloc (len + 1);
    strcpy (tag->value, value);
    if (relation->first_tag == NULL)
	relation->first_tag = tag;
    if (relation->last_tag != NULL)
	relation->last_tag->next = tag;
    relation->last_tag = tag;
}

static void
destroy_internal_relation (readosm_internal_relation * relation)
{
/* destroing an internal RELATION object */
    readosm_internal_member *member;
    readosm_internal_member *member_n;
    readosm_internal_tag *tag;
    readosm_internal_tag *tag_n;
    if (relation == NULL)
	return;
    if (relation->user)
	free (relation->user);
    if (relation->timestamp)
	free (relation->timestamp);
    member = relation->first_member;
    while (member)
      {
	  member_n = member->next;
	  destroy_internal_member (member);
	  member = member_n;
      }
    tag = relation->first_tag;
    while (tag)
      {
	  tag_n = tag->next;
	  destroy_internal_tag (tag);
	  tag = tag_n;
      }
    free (relation);
}

static void
init_export_relation (readosm_export_relation * relation)
{
/* initializing an empty export RELATION object */
    if (relation == NULL)
	return;
    relation->id = 0;
    relation->version = 0;
    relation->changeset = 0;
    relation->user = NULL;
    relation->uid = 0;
    relation->timestamp = NULL;
    relation->member_count = 0;
    relation->members = NULL;
    relation->tag_count = 0;
    relation->tags = NULL;
}

static void
reset_export_relation (readosm_export_relation * relation)
{
/* resetting an export RELATION object to initial empty state */
    int i;
    if (relation == NULL)
	return;
    if (relation->user)
	free (relation->user);
    if (relation->timestamp)
	free (relation->timestamp);
    for (i = 0; i < relation->member_count; i++)
      {
	  readosm_export_member *member = relation->members + i;
	  reset_export_member (member);
      }
    if (relation->members)
	free (relation->members);
    for (i = 0; i < relation->tag_count; i++)
      {
	  readosm_export_tag *tag = relation->tags + i;
	  reset_export_tag (tag);
      }
    if (relation->tags)
	free (relation->tags);
    init_export_relation (relation);
}

static void
xml_init_params (struct xml_params *params, const void *user_data,
		 readosm_node_callback node_fnct, readosm_way_callback way_fnct,
		 readosm_relation_callback relation_fnct, int stop)
{
/* initializing an empty XML helper structure */
    params->current_tag = READOSM_CURRENT_TAG_UNKNOWN;
    params->user_data = user_data;
    params->node_callback = node_fnct;
    params->way_callback = way_fnct;
    params->relation_callback = relation_fnct;

    params->node.id = READOSM_UNDEFINED;
    params->node.latitude = READOSM_UNDEFINED;
    params->node.longitude = READOSM_UNDEFINED;
    params->node.version = READOSM_UNDEFINED;
    params->node.changeset = READOSM_UNDEFINED;
    params->node.user = NULL;
    params->node.uid = READOSM_UNDEFINED;
    params->node.timestamp = NULL;
    params->node.tag_count = 0;
    params->node.first_tag = NULL;
    params->node.last_tag = NULL;

    params->way.id = READOSM_UNDEFINED;
    params->way.version = READOSM_UNDEFINED;
    params->way.changeset = READOSM_UNDEFINED;
    params->way.user = NULL;
    params->way.uid = READOSM_UNDEFINED;
    params->way.timestamp = NULL;
    params->way.ref_count = 0;
    params->way.first_ref = NULL;
    params->way.last_ref = NULL;
    params->way.tag_count = 0;
    params->way.first_tag = NULL;
    params->way.last_tag = NULL;

    params->relation.id = READOSM_UNDEFINED;
    params->relation.version = READOSM_UNDEFINED;
    params->relation.changeset = READOSM_UNDEFINED;
    params->relation.user = NULL;
    params->relation.uid = READOSM_UNDEFINED;
    params->relation.timestamp = NULL;
    params->relation.member_count = 0;
    params->relation.first_member = NULL;
    params->relation.last_member = NULL;
    params->relation.tag_count = 0;
    params->relation.first_tag = NULL;
    params->relation.last_tag = NULL;

    params->stop = stop;
}

static void
xml_reset_params (struct xml_params *params)
{
/* resetting the XML helper structure to initial empty state */
    readosm_internal_tag *tag;
    readosm_internal_tag *tag_n;
    readosm_internal_ref *ref;
    readosm_internal_ref *ref_n;
    readosm_internal_member *member;
    readosm_internal_member *member_n;

    if (params->node.user)
	free (params->node.user);
    if (params->node.timestamp)
	free (params->node.timestamp);
    tag = params->node.first_tag;
    while (tag)
      {
	  tag_n = tag->next;
	  destroy_internal_tag (tag);
	  tag = tag_n;
      }

    if (params->way.user)
	free (params->way.user);
    if (params->way.timestamp)
	free (params->way.timestamp);
    ref = params->way.first_ref;
    while (ref)
      {
	  ref_n = ref->next;
	  destroy_internal_ref (ref);
	  ref = ref_n;
      }
    tag = params->way.first_tag;
    while (tag)
      {
	  tag_n = tag->next;
	  destroy_internal_tag (tag);
	  tag = tag_n;
      }

    if (params->relation.user)
	free (params->relation.user);
    if (params->relation.timestamp)
	free (params->relation.timestamp);
    member = params->relation.first_member;
    while (member)
      {
	  member_n = member->next;
	  destroy_internal_member (member);
	  member = member_n;
      }
    tag = params->relation.first_tag;
    while (tag)
      {
	  tag_n = tag->next;
	  destroy_internal_tag (tag);
	  tag = tag_n;
      }

    xml_init_params (params, params->user_data, params->node_callback,
		     params->way_callback, params->relation_callback,
		     params->stop);
}

static int
test_endianness ()
{
/* checks the current CPU endianness */
    readosm_endian4 endian4;
    endian4.bytes[0] = 0x01;
    endian4.bytes[1] = 0x00;
    endian4.bytes[2] = 0x00;
    endian4.bytes[3] = 0x00;
    if (endian4.uint32_value == 1)
	return READOSM_LITTLE_ENDIAN;
    return READOSM_BIG_ENDIAN;
}

static int
call_node_callback (readosm_node_callback node_callback,
		    const void *user_data, readosm_internal_node * node)
{
/* calling the Node-handling callback function */
    int ret;
    int len;
    readosm_internal_tag *tag;
    readosm_export_node exp_node;

/* 
 / please note: READONLY-NODE simply is the same as export 
 / NODE inteded to disabale any possible awful user action
*/
    readosm_node *readonly_node = (readosm_node *) & exp_node;

/*initialing an empty export NODE object */
    init_export_node (&exp_node);

/* setting up the export NODE object */
    exp_node.id = node->id;
    exp_node.latitude = node->latitude;
    exp_node.longitude = node->longitude;
    exp_node.version = node->version;
    exp_node.changeset = node->changeset;
    if (node->user != NULL)
      {
	  len = strlen (node->user);
	  exp_node.user = malloc (len + 1);
	  strcpy (exp_node.user, node->user);
      }
    exp_node.uid = node->uid;
    if (node->timestamp != NULL)
      {
	  len = strlen (node->timestamp);
	  exp_node.timestamp = malloc (len + 1);
	  strcpy (exp_node.timestamp, node->timestamp);
      }

/* setting up the NODE-TAGs array */
    tag = node->first_tag;
    while (tag)
      {
	  exp_node.tag_count++;
	  tag = tag->next;
      }
    if (exp_node.tag_count > 0)
      {
	  int i;
	  readosm_export_tag *p_tag;
	  exp_node.tags =
	      malloc (sizeof (readosm_export_tag) * exp_node.tag_count);
	  for (i = 0; i < exp_node.tag_count; i++)
	    {
		p_tag = exp_node.tags + i;
		init_export_tag (p_tag);
	    }
	  i = 0;
	  tag = node->first_tag;
	  while (tag)
	    {
		p_tag = exp_node.tags + i;
		if (tag->key != NULL)
		  {
		      len = strlen (tag->key);
		      p_tag->key = malloc (len + 1);
		      strcpy (p_tag->key, tag->key);
		  }
		if (tag->value != NULL)
		  {
		      len = strlen (tag->value);
		      p_tag->value = malloc (len + 1);
		      strcpy (p_tag->value, tag->value);
		  }
		i++;
		tag = tag->next;
	    }
      }

/* calling the user-defined NODE handling callback function */
    ret = (*node_callback) (user_data, readonly_node);

/* resetting the export WAY object */
    reset_export_node (&exp_node);
    return ret;
}

static int
call_way_callback (readosm_way_callback way_callback,
		   const void *user_data, readosm_internal_way * way)
{
/* calling the Way-handling callback function */
    int ret;
    int len;
    int i;
    readosm_internal_ref *ref;
    readosm_internal_tag *tag;
    readosm_export_way exp_way;

/* 
 / please note: READONLY-WAY simply is the same as export 
 / WAY inteded to disabale any possible awful user action
*/
    readosm_way *readonly_way = (readosm_way *) & exp_way;

/*initialing an empty export WAY object */
    init_export_way (&exp_way);

    exp_way.id = way->id;
    exp_way.version = way->version;
    exp_way.changeset = way->changeset;
    if (way->user != NULL)
      {
	  len = strlen (way->user);
	  exp_way.user = malloc (len + 1);
	  strcpy (exp_way.user, way->user);
      }
    exp_way.uid = way->uid;
    if (way->timestamp != NULL)
      {
	  len = strlen (way->timestamp);
	  exp_way.timestamp = malloc (len + 1);
	  strcpy (exp_way.timestamp, way->timestamp);
      }

    ref = way->first_ref;
    while (ref)
      {
	  exp_way.node_ref_count++;
	  ref = ref->next;
      }

/* setting up the NODE-REFs array */
    if (exp_way.node_ref_count > 0)
      {
	  exp_way.node_refs =
	      malloc (sizeof (long long) * exp_way.node_ref_count);
	  i = 0;
	  ref = way->first_ref;
	  while (ref)
	    {
		*(exp_way.node_refs + i) = ref->node_ref;
		i++;
		ref = ref->next;
	    }
      }

/* setting up the WAY-TAGs array */
    tag = way->first_tag;
    while (tag)
      {
	  exp_way.tag_count++;
	  tag = tag->next;
      }
    if (exp_way.tag_count > 0)
      {
	  readosm_export_tag *p_tag;
	  exp_way.tags =
	      malloc (sizeof (readosm_export_tag) * exp_way.tag_count);
	  for (i = 0; i < exp_way.tag_count; i++)
	    {
		p_tag = exp_way.tags + i;
		init_export_tag (p_tag);
	    }
	  i = 0;
	  tag = way->first_tag;
	  while (tag)
	    {
		p_tag = exp_way.tags + i;
		if (tag->key != NULL)
		  {
		      len = strlen (tag->key);
		      p_tag->key = malloc (len + 1);
		      strcpy (p_tag->key, tag->key);
		  }
		if (tag->value != NULL)
		  {
		      len = strlen (tag->value);
		      p_tag->value = malloc (len + 1);
		      strcpy (p_tag->value, tag->value);
		  }
		i++;
		tag = tag->next;
	    }
      }

/* calling the user-defined WAY handling callback function */
    ret = (*way_callback) (user_data, readonly_way);

/* resetting the export WAY object */
    reset_export_way (&exp_way);
    return ret;
}

static int
call_relation_callback (readosm_relation_callback relation_callback,
			const void *user_data,
			readosm_internal_relation * relation)
{
/* calling the Relation-handling callback function */
    int ret;
    int len;
    int i;
    readosm_internal_member *member;
    readosm_internal_tag *tag;
    readosm_export_relation exp_relation;

/* 
 / please note: READONLY-RELATION simply is the same as export 
 / RELATION inteded to disabale any possible awful user action
*/
    readosm_relation *readonly_relation = (readosm_relation *) & exp_relation;

/*initialing an empty export RELATION object */
    init_export_relation (&exp_relation);

    exp_relation.id = relation->id;
    exp_relation.version = relation->version;
    exp_relation.changeset = relation->changeset;
    if (relation->user != NULL)
      {
	  len = strlen (relation->user);
	  exp_relation.user = malloc (len + 1);
	  strcpy (exp_relation.user, relation->user);
      }
    exp_relation.uid = relation->uid;
    if (relation->timestamp != NULL)
      {
	  len = strlen (relation->timestamp);
	  exp_relation.timestamp = malloc (len + 1);
	  strcpy (exp_relation.timestamp, relation->timestamp);
      }

/* setting up the RELATION-MEMBERs array */
    member = relation->first_member;
    while (member)
      {
	  exp_relation.member_count++;
	  member = member->next;
      }
    if (exp_relation.member_count > 0)
      {
	  readosm_export_member *p_member;
	  exp_relation.members =
	      malloc (sizeof (readosm_export_member) *
		      exp_relation.member_count);
	  for (i = 0; i < exp_relation.member_count; i++)
	    {
		p_member = exp_relation.members + i;
		init_export_member (p_member);
	    }
	  i = 0;
	  member = relation->first_member;
	  while (member)
	    {
		p_member = exp_relation.members + i;
		p_member->member_type = member->member_type;
		p_member->id = member->id;
		if (member->role != NULL)
		  {
		      len = strlen (member->role);
		      p_member->role = malloc (len + 1);
		      strcpy (p_member->role, member->role);
		  }
		i++;
		member = member->next;
	    }
      }

/* setting up the RELATION-TAGs array */
    tag = relation->first_tag;
    while (tag)
      {
	  exp_relation.tag_count++;
	  tag = tag->next;
      }
    if (exp_relation.tag_count > 0)
      {
	  readosm_export_tag *p_tag;
	  exp_relation.tags =
	      malloc (sizeof (readosm_export_tag) * exp_relation.tag_count);
	  for (i = 0; i < exp_relation.tag_count; i++)
	    {
		p_tag = exp_relation.tags + i;
		init_export_tag (p_tag);
	    }
	  i = 0;
	  tag = relation->first_tag;
	  while (tag)
	    {
		p_tag = exp_relation.tags + i;
		if (tag->key != NULL)
		  {
		      len = strlen (tag->key);
		      p_tag->key = malloc (len + 1);
		      strcpy (p_tag->key, tag->key);
		  }
		if (tag->value != NULL)
		  {
		      len = strlen (tag->value);
		      p_tag->value = malloc (len + 1);
		      strcpy (p_tag->value, tag->value);
		  }
		i++;
		tag = tag->next;
	    }
      }

/* calling the user-defined RELATION handling callback function */
    ret = (*relation_callback) (user_data, readonly_relation);

/* resetting the export RELATION object */
    reset_export_relation (&exp_relation);
    return ret;
}

static readosm_file *
alloc_osm_file (int little_endian_cpu, int format)
{
/* allocating and initializing the OSM input file struct */
    readosm_file *input = malloc (sizeof (readosm_file));
    if (!input)
	return NULL;
    input->magic1 = READOSM_MAGIC_START;
    input->file_format = format;
    input->little_endian_cpu = little_endian_cpu;
    input->magic2 = READOSM_MAGIC_END;
    input->in = NULL;
    return input;
}

static void
destroy_osm_file (readosm_file * input)
{
/* destroyng the OSM input file struct */
    if (input)
      {
	  if (input->in)
	      fclose (input->in);
	  free (input);
      }
}

static void
xml_start_node (struct xml_params *params, const char **attr)
{
/* an XML Node starts here */
    int i;
    int len;
    xml_reset_params (params);
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      params->node.id = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "lat") == 0)
	      params->node.latitude = atof (attr[i + 1]);
	  if (strcmp (attr[i], "lon") == 0)
	      params->node.longitude = atof (attr[i + 1]);
	  if (strcmp (attr[i], "version") == 0)
	      params->node.version = atoi (attr[i + 1]);
	  if (strcmp (attr[i], "changeset") == 0)
	      params->node.changeset = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "user") == 0)
	    {
		len = strlen (attr[i + 1]);
		params->node.user = malloc (len + 1);
		strcpy (params->node.user, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "uid") == 0)
	      params->node.uid = atoi (attr[i + 1]);
	  if (strcmp (attr[i], "timestamp") == 0)
	    {
		len = strlen (attr[i + 1]);
		params->node.timestamp = malloc (len + 1);
		strcpy (params->node.timestamp, attr[i + 1]);
	    }
      }
    params->current_tag = READOSM_CURRENT_TAG_IS_NODE;
}

static void
xml_end_node (struct xml_params *params)
{
/* an XML Node ends here */
    if (params->node_callback != NULL && params->stop == 0)
      {
	  int ret =
	      call_node_callback (params->node_callback, params->user_data,
				  &(params->node));
	  if (ret != READOSM_OK)
	      params->stop = 1;
      }
    xml_reset_params (params);
}

static void
xml_start_way (struct xml_params *params, const char **attr)
{
/* an XML Way starts here */
    int i;
    int len;
    xml_reset_params (params);
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      params->way.id = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "version") == 0)
	      params->way.version = atoi (attr[i + 1]);
	  if (strcmp (attr[i], "changeset") == 0)
	      params->way.changeset = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "user") == 0)
	    {
		len = strlen (attr[i + 1]);
		params->way.user = malloc (len + 1);
		strcpy (params->way.user, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "uid") == 0)
	      params->way.uid = atoi (attr[i + 1]);
	  if (strcmp (attr[i], "timestamp") == 0)
	    {
		len = strlen (attr[i + 1]);
		params->way.timestamp = malloc (len + 1);
		strcpy (params->way.timestamp, attr[i + 1]);
	    }
      }
    params->current_tag = READOSM_CURRENT_TAG_IS_WAY;
}

static void
xml_end_way (struct xml_params *params)
{
/* an XML Way ends here */
    if (params->way_callback != NULL && params->stop == 0)
      {
	  int ret = call_way_callback (params->way_callback, params->user_data,
				       &(params->way));
	  if (ret != READOSM_OK)
	      params->stop = 1;
      }
    xml_reset_params (params);
}

static void
xml_start_relation (struct xml_params *params, const char **attr)
{
/* an XML Relation starts here */
    int i;
    int len;
    xml_reset_params (params);
    for (i = 0; attr[i]; i += 2)
      {
	  if (strcmp (attr[i], "id") == 0)
	      params->relation.id = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "version") == 0)
	      params->relation.version = atoi (attr[i + 1]);
	  if (strcmp (attr[i], "changeset") == 0)
	      params->relation.changeset = atol_64 (attr[i + 1]);
	  if (strcmp (attr[i], "user") == 0)
	    {
		len = strlen (attr[i + 1]);
		params->relation.user = malloc (len + 1);
		strcpy (params->relation.user, attr[i + 1]);
	    }
	  if (strcmp (attr[i], "uid") == 0)
	      params->relation.uid = atoi (attr[i + 1]);
	  if (strcmp (attr[i], "timestamp") == 0)
	    {
		len = strlen (attr[i + 1]);
		params->relation.timestamp = malloc (len + 1);
		strcpy (params->relation.timestamp, attr[i + 1]);
	    }
      }
    params->current_tag = READOSM_CURRENT_TAG_IS_RELATION;
}

static void
xml_end_relation (struct xml_params *params)
{
/* an XML Relation ends here */
    if (params->relation_callback != NULL && params->stop == 0)
      {
	  int ret = call_relation_callback (params->relation_callback,
					    params->user_data,
					    &(params->relation));
	  if (ret != READOSM_OK)
	      params->stop = 1;
      }
    xml_reset_params (params);
}

static void
xml_start_xtag (struct xml_params *params, const char **attr)
{
/* an XML Tag starts here */
    readosm_internal_tag *tag;
    int i;
    int len;

    if (params->current_tag == READOSM_CURRENT_TAG_IS_NODE
	|| params->current_tag == READOSM_CURRENT_TAG_IS_WAY
	|| params->current_tag == READOSM_CURRENT_TAG_IS_RELATION)
      {
	  tag = alloc_internal_tag ();
	  for (i = 0; attr[i]; i += 2)
	    {
		if (strcmp (attr[i], "k") == 0)
		  {
		      len = strlen (attr[i + 1]);
		      tag->key = malloc (len + 1);
		      strcpy (tag->key, attr[i + 1]);
		  }
		if (strcmp (attr[i], "v") == 0)
		  {
		      len = strlen (attr[i + 1]);
		      tag->value = malloc (len + 1);
		      strcpy (tag->value, attr[i + 1]);
		  }
	    }
	  if (params->current_tag == READOSM_CURRENT_TAG_IS_NODE)
	    {
		if (params->node.first_tag == NULL)
		    params->node.first_tag = tag;
		if (params->node.last_tag != NULL)
		    params->node.last_tag->next = tag;
		params->node.last_tag = tag;
	    }
	  if (params->current_tag == READOSM_CURRENT_TAG_IS_WAY)
	    {
		if (params->way.first_tag == NULL)
		    params->way.first_tag = tag;
		if (params->way.last_tag != NULL)
		    params->way.last_tag->next = tag;
		params->way.last_tag = tag;
	    }
	  if (params->current_tag == READOSM_CURRENT_TAG_IS_RELATION)
	    {
		if (params->relation.first_tag == NULL)
		    params->relation.first_tag = tag;
		if (params->relation.last_tag != NULL)
		    params->relation.last_tag->next = tag;
		params->relation.last_tag = tag;
	    }
      }
}

static void
xml_start_nd (struct xml_params *params, const char **attr)
{
/* an XML Nd starts here */
    readosm_internal_ref *ref;
    int i;

    if (params->current_tag == READOSM_CURRENT_TAG_IS_WAY)
      {
	  ref = alloc_internal_ref ();
	  for (i = 0; attr[i]; i += 2)
	    {
		if (strcmp (attr[i], "ref") == 0)
		    ref->node_ref = atol_64 (attr[i + 1]);
	    }
	  if (params->way.first_ref == NULL)
	      params->way.first_ref = ref;
	  if (params->way.last_ref != NULL)
	      params->way.last_ref->next = ref;
	  params->way.last_ref = ref;
      }
}

static void
xml_start_member (struct xml_params *params, const char **attr)
{
/* an XML Member starts here */
    readosm_internal_member *member;
    int i;
    int len;

    if (params->current_tag == READOSM_CURRENT_TAG_IS_RELATION)
      {
	  member = alloc_internal_member ();
	  for (i = 0; attr[i]; i += 2)
	    {
		if (strcmp (attr[i], "ref") == 0)
		    member->id = atol_64 (attr[i + 1]);
		if (strcmp (attr[i], "type") == 0)
		  {
		      if (strcmp (attr[i + 1], "node") == 0)
			  member->member_type = READOSM_MEMBER_NODE;
		      if (strcmp (attr[i + 1], "way") == 0)
			  member->member_type = READOSM_MEMBER_WAY;
		      if (strcmp (attr[i + 1], "relation") == 0)
			  member->member_type = READOSM_MEMBER_RELATION;
		  }
		if (strcmp (attr[i], "role") == 0)
		  {
		      len = strlen (attr[i + 1]);
		      member->role = malloc (len + 1);
		      strcpy (member->role, attr[i + 1]);
		  }
	    }
	  if (params->relation.first_member == NULL)
	      params->relation.first_member = member;
	  if (params->relation.last_member != NULL)
	      params->relation.last_member->next = member;
	  params->relation.last_member = member;
      }
}

static void
xml_start_tag (void *data, const char *el, const char **attr)
{
/* some generic XML tag starts here */
    struct xml_params *params = (struct xml_params *) data;
    if (strcmp (el, "node") == 0)
	xml_start_node (params, attr);
    if (strcmp (el, "tag") == 0)
	xml_start_xtag (params, attr);
    if (strcmp (el, "way") == 0)
	xml_start_way (params, attr);
    if (strcmp (el, "nd") == 0)
	xml_start_nd (params, attr);
    if (strcmp (el, "relation") == 0)
	xml_start_relation (params, attr);
    if (strcmp (el, "member") == 0)
	xml_start_member (params, attr);
}

static void
xml_end_tag (void *data, const char *el)
{
/* some generic XML tag ends here */
    struct xml_params *params = (struct xml_params *) data;
    if (strcmp (el, "node") == 0)
	xml_end_node (params);
    if (strcmp (el, "way") == 0)
	xml_end_way (params);
    if (strcmp (el, "relation") == 0)
	xml_end_relation (params);
}

static int
parse_osm_xml (readosm_file * input, const void *user_data,
	       readosm_node_callback node_fnct, readosm_way_callback way_fnct,
	       readosm_relation_callback relation_fnct)
{
/* parsing the input file [OSM XML format] */
    XML_Parser parser;
    char xml_buff[BUFFSIZE];
    int done = 0;
    int len;
    struct xml_params params;

    xml_init_params (&params, user_data, node_fnct, way_fnct, relation_fnct, 0);

    parser = XML_ParserCreate (NULL);
    if (!parser)
	return READOSM_CREATE_XML_PARSER_ERROR;

    XML_SetUserData (parser, &params);
    XML_SetElementHandler (parser, xml_start_tag, xml_end_tag);
    while (!done)
      {
	  len = fread (xml_buff, 1, BUFFSIZE, input->in);
	  if (ferror (input->in))
	      return READOSM_READ_ERROR;
	  done = feof (input->in);
	  if (!XML_Parse (parser, xml_buff, len, done))
	      return READOSM_XML_ERROR;
	  if (params.stop)
	      return READOSM_ABORT;
      }
    XML_ParserFree (parser);

    return READOSM_OK;
}

static void
init_variant (readosm_variant * variant, int little_endian_cpu)
{
/* initializing an empty PBF Variant object */
    variant->little_endian_cpu = little_endian_cpu;
    variant->type = READOSM_VAR_UNDEFINED;
    variant->field_id = 0;
    variant->length = 0;
    variant->pointer = NULL;
    variant->valid = 0;
    variant->first = NULL;
    variant->last = NULL;
}

static void
reset_variant (readosm_variant * variant)
{
/* resetting a PBF Variant object to its initial empty state */
    variant->type = READOSM_VAR_UNDEFINED;
    variant->field_id = 0;
    variant->length = 0;
    variant->pointer = NULL;
    variant->valid = 0;
}

static void
add_variant_hints (readosm_variant * variant, unsigned char type,
		   unsigned char field_id)
{
/* adding a field hint to a PBF Variant object */
    readosm_variant_hint *hint = malloc (sizeof (readosm_variant_hint));
    hint->type = type;
    hint->field_id = field_id;
    hint->next = NULL;
    if (variant->first == NULL)
	variant->first = hint;
    if (variant->last != NULL)
	variant->last->next = hint;
    variant->last = hint;
}

static int
find_type_hint (readosm_variant * variant, unsigned char field_id,
		unsigned char type, unsigned char *type_hint)
{
/* attempting to find the type hint for some PBF Variant field */
    readosm_variant_hint *hint = variant->first;
    while (hint)
      {
	  if (hint->field_id == field_id)
	    {
		switch (type)
		  {
		  case 0:
		      switch (hint->type)
			{
			case READOSM_VAR_INT32:
			case READOSM_VAR_INT64:
			case READOSM_VAR_UINT32:
			case READOSM_VAR_UINT64:
			case READOSM_VAR_SINT32:
			case READOSM_VAR_SINT64:
			case READOSM_VAR_BOOL:
			case READOSM_VAR_ENUM:
			    *type_hint = hint->type;
			    return 1;
			}
		      break;
		  case 2:
		      if (hint->type == READOSM_LEN_BYTES)
			{
			    *type_hint = hint->type;
			    return 1;
			}
		      break;
		  };
	    }
	  hint = hint->next;
      }
    return 0;
}

static void
finalize_variant (readosm_variant * variant)
{
/* cleaning any memory allocation for a PBF Variant object */
    readosm_variant_hint *hint;
    readosm_variant_hint *hint_n;
    hint = variant->first;
    while (hint)
      {
	  hint_n = hint->next;
	  free (hint);
	  hint = hint_n;
      }
    variant->first = NULL;
    variant->last = NULL;
}

static void
init_string_table (readosm_string_table * string_table)
{
/* initializing an empty PBF StringTable object */
    string_table->first = NULL;
    string_table->last = NULL;
    string_table->count = 0;
    string_table->strings = NULL;
}

static void
append_string_to_table (readosm_string_table * string_table,
			readosm_variant * variant)
{
/* appending a string to a PBF StringTable object */
    readosm_string *string = malloc (sizeof (readosm_string));
    string->string = malloc (variant->length + 1);
    memcpy (string->string, variant->pointer, variant->length);
    *(string->string + variant->length) = '\0';
    string->next = NULL;
    if (string_table->first == NULL)
	string_table->first = string;
    if (string_table->last != NULL)
	string_table->last->next = string;
    string_table->last = string;
}

static void
array_from_string_table (readosm_string_table * string_table)
{
/* creating a pointer array supporting a StringTable object */
    int i;
    readosm_string *string = string_table->first;
    while (string != NULL)
      {
	  /* counting how many strings are into the table */
	  string_table->count++;
	  string = string->next;
      }
    if (string_table->count <= 0)
	return;

/* allocating the pointer array */
    string_table->strings =
	malloc (sizeof (readosm_string *) * string_table->count);
    i = 0;
    string = string_table->first;
    while (string != NULL)
      {
	  /* setting up pointers to strings */
	  *(string_table->strings + i) = string;
	  i++;
	  string = string->next;
      }
}

static void
finalize_string_table (readosm_string_table * string_table)
{
/* cleaning any memory allocation for a StringTable object */
    readosm_string *string;
    readosm_string *string_n;
    string = string_table->first;
    while (string)
      {
	  string_n = string->next;
	  if (string->string)
	      free (string->string);
	  free (string);
	  string = string_n;
      }
    if (string_table->strings)
	free (string_table->strings);
}

static void
init_uint32_packed (readosm_uint32_packed * packed)
{
/* initialing an empty PBF uint32 packed object */
    packed->first = NULL;
    packed->last = NULL;
    packed->count = 0;
    packed->values = NULL;
}

static void
append_uint32_packed (readosm_uint32_packed * packed, unsigned int val)
{
/* appending an uint32 value to a PBF packed object */
    readosm_uint32 *value = malloc (sizeof (readosm_uint32));
    value->value = val;
    value->next = NULL;
    if (packed->first == NULL)
	packed->first = value;
    if (packed->last != NULL)
	packed->last->next = value;
    packed->last = value;
}

static void
array_from_uint32_packed (readosm_uint32_packed * packed)
{
/* creating an array supporting an uint32 packed object */
    int i;
    readosm_uint32 *value = packed->first;
    while (value != NULL)
      {
	  /* counting how many values are into the packed list */
	  packed->count++;
	  value = value->next;
      }
    if (packed->count <= 0)
	return;

/* allocating the array */
    packed->values = malloc (sizeof (unsigned int) * packed->count);
    i = 0;
    value = packed->first;
    while (value != NULL)
      {
	  /* setting up array values */
	  *(packed->values + i) = value->value;
	  i++;
	  value = value->next;
      }
}

static void
finalize_uint32_packed (readosm_uint32_packed * packed)
{
/* cleaning any memory allocation for an uint32 packed object */
    readosm_uint32 *value;
    readosm_uint32 *value_n;
    value = packed->first;
    while (value)
      {
	  value_n = value->next;
	  free (value);
	  value = value_n;
      }
    if (packed->values)
	free (packed->values);

}

static void
reset_uint32_packed (readosm_uint32_packed * packed)
{
/* resetting an uint32 packed object to empty initial state */
    finalize_uint32_packed (packed);
    packed->first = NULL;
    packed->last = NULL;
    packed->count = 0;
    packed->values = NULL;
}

static void
init_int32_packed (readosm_int32_packed * packed)
{
/* initialing an empty PBF int32 packed object */
    packed->first = NULL;
    packed->last = NULL;
    packed->count = 0;
    packed->values = NULL;
}

static void
append_int32_packed (readosm_int32_packed * packed, int val)
{
/* appending an int32 value to a PBF packed object */
    readosm_int32 *value = malloc (sizeof (readosm_int32));
    value->value = val;
    value->next = NULL;
    if (packed->first == NULL)
	packed->first = value;
    if (packed->last != NULL)
	packed->last->next = value;
    packed->last = value;
}

static void
finalize_int32_packed (readosm_int32_packed * packed)
{
/* cleaning any memory allocation for an int32 packed object */
    readosm_int32 *value;
    readosm_int32 *value_n;
    value = packed->first;
    while (value)
      {
	  value_n = value->next;
	  free (value);
	  value = value_n;
      }
    if (packed->values)
	free (packed->values);

}

static void
reset_int32_packed (readosm_int32_packed * packed)
{
/* resetting an int32 packed object to empty initial state */
    finalize_int32_packed (packed);
    packed->first = NULL;
    packed->last = NULL;
    packed->count = 0;
    packed->values = NULL;
}

static void
init_int64_packed (readosm_int64_packed * packed)
{
/* initialing an empty PBF int64 packed object */
    packed->first = NULL;
    packed->last = NULL;
    packed->count = 0;
    packed->values = NULL;
}

static void
append_int64_packed (readosm_int64_packed * packed, long long val)
{
/* appending an int64 value to a PBF packed object */
    readosm_int64 *value = malloc (sizeof (readosm_int64));
    value->value = val;
    value->next = NULL;
    if (packed->first == NULL)
	packed->first = value;
    if (packed->last != NULL)
	packed->last->next = value;
    packed->last = value;
}

static void
array_from_int64_packed (readosm_int64_packed * packed)
{
/* creating an array supporting an int64 packed object */
    int i;
    readosm_int64 *value = packed->first;
    while (value != NULL)
      {
	  /* counting how many values are into the packed list */
	  packed->count++;
	  value = value->next;
      }
    if (packed->count <= 0)
	return;

/* allocating the array */
    packed->values = malloc (sizeof (long long) * packed->count);
    i = 0;
    value = packed->first;
    while (value != NULL)
      {
	  /* setting up array values */
	  *(packed->values + i) = value->value;
	  i++;
	  value = value->next;
      }
}

static void
finalize_int64_packed (readosm_int64_packed * packed)
{
/* cleaning any memory allocation for an int64 packed object */
    readosm_int64 *value;
    readosm_int64 *value_n;
    value = packed->first;
    while (value)
      {
	  value_n = value->next;
	  free (value);
	  value = value_n;
      }
    if (packed->values)
	free (packed->values);
}

static void
reset_int64_packed (readosm_int64_packed * packed)
{
/* resetting an int64 packed object to empty initial state */
    finalize_int64_packed (packed);
    packed->first = NULL;
    packed->last = NULL;
    packed->count = 0;
    packed->values = NULL;
}

static void
init_packed_infos (readosm_packed_infos * packed)
{
/* initialing an empty PBF  packed Infos object */
    packed->ver_count = 0;
    packed->versions = NULL;
    packed->tim_count = 0;
    packed->timestamps = NULL;
    packed->cng_count = 0;
    packed->changesets = NULL;
    packed->uid_count = 0;
    packed->uids = NULL;
    packed->usr_count = 0;
    packed->users = NULL;
}

static void
finalize_packed_infos (readosm_packed_infos * packed)
{
/* cleaning any memory allocation for a packed Infos object */
    if (packed->versions)
	free (packed->versions);
    if (packed->timestamps)
	free (packed->timestamps);
    if (packed->changesets)
	free (packed->changesets);
    if (packed->uids)
	free (packed->uids);
    if (packed->users)
	free (packed->users);
}

static unsigned char *
read_var (unsigned char *start, unsigned char *stop, readosm_variant * variant)
{
/* 
 / attempting to read a variable lenght base128 int 
 /
 / PBF integers are encoded as base128, i.e. using 7 bits
 / for each byte: if the most significant bit is 1, then
 / a further byte is required to get the int value, and so
 / on, until a byte having a 0 most significat bit is found.
 /
 / using this encoding little values simply require few bytes:
 / as a worst case 5 bytes are required to encode int32, and
 / 10 bytes to encode int64
 /
 / there is a further complication: negative value will always 
 / require 5 or 10 bytes: thus SINT32 and SINT64 values are
 / encoded using a "ZigZag" schema.
 /
 / for more details please see:
 / https://developers.google.com/protocol-buffers/docs/encoding
*/
    unsigned char *ptr = start;
    unsigned char c;
    unsigned int v32;
    unsigned long long v64;
    unsigned int value32 = 0x00000000;
    unsigned long long value64 = 0x0000000000000000;
    readosm_endian4 endian4;
    readosm_endian8 endian8;
    int next;
    int count = 0;
    int neg;

    while (1)
      {
	  if (ptr > stop)
	      return NULL;
	  c = *ptr++;
	  if ((c & 0x80) == 0x80)
	      next = 1;
	  else
	      next = 0;
	  c &= 0x7f;
	  switch (variant->type)
	    {
	    case READOSM_VAR_INT32:
	    case READOSM_VAR_UINT32:
	    case READOSM_VAR_SINT32:
		switch (count)
		  {
		  case 0:
		      memset (endian4.bytes, 0x00, 4);
		      if (variant->little_endian_cpu)
			  endian4.bytes[0] = c;
		      else
			  endian4.bytes[3] = c;
		      v32 = endian4.uint32_value;
		      v32 &= READOSM_MASK32_1;
		      value32 |= v32;
		      break;
		  case 1:
		      memset (endian4.bytes, 0x00, 4);
		      if (variant->little_endian_cpu)
			  endian4.bytes[0] = c;
		      else
			  endian4.bytes[3] = c;
		      v32 = endian4.uint32_value << 7;
		      v32 &= READOSM_MASK32_2;
		      value32 |= v32;
		      break;
		  case 2:
		      memset (endian4.bytes, 0x00, 4);
		      if (variant->little_endian_cpu)
			  endian4.bytes[0] = c;
		      else
			  endian4.bytes[3] = c;
		      v32 = endian4.uint32_value << 14;
		      v32 &= READOSM_MASK32_3;
		      value32 |= v32;
		      break;
		  case 3:
		      memset (endian4.bytes, 0x00, 4);
		      if (variant->little_endian_cpu)
			  endian4.bytes[0] = c;
		      else
			  endian4.bytes[3] = c;
		      v32 = endian4.uint32_value << 21;
		      v32 &= READOSM_MASK32_4;
		      value32 |= v32;
		      break;
		  case 4:
		      memset (endian4.bytes, 0x00, 4);
		      if (variant->little_endian_cpu)
			  endian4.bytes[0] = c;
		      else
			  endian4.bytes[3] = c;
		      v32 = endian4.uint32_value << 28;
		      v32 &= READOSM_MASK32_5;
		      value32 |= v32;
		      break;
		  default:
		      return NULL;
		  };
		break;
	    case READOSM_VAR_INT64:
	    case READOSM_VAR_UINT64:
	    case READOSM_VAR_SINT64:
		switch (count)
		  {
		  case 0:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value;
		      v64 &= READOSM_MASK64_1;
		      value64 |= v64;
		      break;
		  case 1:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 7;
		      v64 &= READOSM_MASK64_2;
		      value64 |= v64;
		      break;
		  case 2:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 14;
		      v64 &= READOSM_MASK64_3;
		      value64 |= v64;
		      break;
		  case 3:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 21;
		      v64 &= READOSM_MASK64_4;
		      value64 |= v64;
		      break;
		  case 4:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 28;
		      v64 &= READOSM_MASK64_5;
		      value64 |= v64;
		      break;
		  case 5:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 35;
		      v64 &= READOSM_MASK64_6;
		      value64 |= v64;
		      break;
		  case 6:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 42;
		      v64 &= READOSM_MASK64_7;
		      value64 |= v64;
		      break;
		  case 7:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 49;
		      v64 &= READOSM_MASK64_8;
		      value64 |= v64;
		      break;
		  case 8:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 56;
		      v64 &= READOSM_MASK64_9;
		      value64 |= v64;
		      break;
		  case 9:
		      memset (endian8.bytes, 0x00, 8);
		      if (variant->little_endian_cpu)
			  endian8.bytes[0] = c;
		      else
			  endian8.bytes[7] = c;
		      v64 = endian8.uint64_value << 63;
		      v64 &= READOSM_MASK64_A;
		      value64 |= v64;
		      break;
		  default:
		      return NULL;
		  };
		break;
	    };
	  count++;
	  if (!next)
	      break;
      }

    switch (variant->type)
      {
      case READOSM_VAR_INT32:
	  variant->value.int32_value = (int) value32;
	  variant->valid = 1;
	  return ptr;
      case READOSM_VAR_UINT32:
	  variant->value.uint32_value = value32;
	  variant->valid = 1;
	  return ptr;
      case READOSM_VAR_SINT32:
	  if ((value32 & 0x00000001) == 0)
	      neg = 1;
	  else
	      neg = -1;
	  v32 = (value32 + 1) / 2;
	  variant->value.int32_value = v32 * neg;
	  variant->valid = 1;
	  return ptr;
      case READOSM_VAR_INT64:
	  variant->value.int64_value = (int) value64;
	  variant->valid = 1;
	  return ptr;
      case READOSM_VAR_UINT64:
	  variant->value.uint64_value = value64;
	  variant->valid = 1;
	  return ptr;
      case READOSM_VAR_SINT64:
	  if ((value64 & 0x0000000000000001) == 0)
	      neg = 1;
	  else
	      neg = -1;
	  v64 = (value64 + 1) / 2;
	  variant->value.int64_value = v64 * neg;
	  variant->valid = 1;
	  return ptr;
      };
    return NULL;
}

static unsigned char *
read_bytes (unsigned char *start, unsigned char *stop,
	    readosm_variant * variant)
{
/* 
 / attempting to read some bytes from PBF
 / Strings and alike are encoded in PBF using a two steps approach:
 / - an INT32 field declares the expected length
 / - then the string (no terminating NULL char) follows
*/
    unsigned char *ptr = start;
    readosm_variant varlen;
    unsigned int len;

/* initializing an empty variant field (lenght) */
    init_variant (&varlen, variant->little_endian_cpu);
    varlen.type = READOSM_VAR_UINT32;

    ptr = read_var (ptr, stop, &varlen);
    if (varlen.valid)
      {
	  len = varlen.value.uint32_value;
	  if ((ptr + len - 1) > stop)
	      return NULL;
	  variant->pointer = ptr;
	  variant->length = len;
	  variant->valid = 1;
	  return ptr + len;
      }
    return NULL;
}

static int
parse_uint32_packed (readosm_uint32_packed * packed, unsigned char *start,
		     unsigned char *stop, char little_endian_cpu)
{
/* parsing an uint32 packed object */
    unsigned char *ptr = start;
    readosm_variant variant;

/* initializing an empty variant field (lenght) */
    init_variant (&variant, little_endian_cpu);
    variant.type = READOSM_VAR_UINT32;

    while (1)
      {
	  ptr = read_var (start, stop, &variant);
	  if (variant.valid)
	    {
		append_uint32_packed (packed, variant.value.uint32_value);
		if (ptr > stop)
		    break;
		start = ptr;
		continue;
	    }
	  return 0;
      }
    return 1;
}

static int
parse_sint32_packed (readosm_int32_packed * packed, unsigned char *start,
		     unsigned char *stop, char little_endian_cpu)
{
/* parsing an int32 packed object */
    unsigned char *ptr = start;
    readosm_variant variant;

/* initializing an empty variant field (lenght) */
    init_variant (&variant, little_endian_cpu);
    variant.type = READOSM_VAR_SINT32;

    while (1)
      {
	  ptr = read_var (start, stop, &variant);
	  if (variant.valid)
	    {
		append_int32_packed (packed, variant.value.int32_value);
		if (ptr > stop)
		    break;
		start = ptr;
		continue;
	    }
	  return 0;
      }
    return 1;
}

static int
parse_sint64_packed (readosm_int64_packed * packed, unsigned char *start,
		     unsigned char *stop, char little_endian_cpu)
{
/* parsing a sint64 packed object */
    unsigned char *ptr = start;
    readosm_variant variant;

/* initializing an empty variant field (lenght) */
    init_variant (&variant, little_endian_cpu);
    variant.type = READOSM_VAR_SINT64;

    while (1)
      {
	  ptr = read_var (start, stop, &variant);
	  if (variant.valid)
	    {
		append_int64_packed (packed, variant.value.int64_value);
		if (ptr > stop)
		    break;
		start = ptr;
		continue;
	    }
	  return 0;
      }
    return 1;
}

static unsigned int
get_header_size (unsigned char *buf, int little_endian_cpu)
{
/* 
 / retrieving the current header size 
 / please note: header sizes in PBF always are 4 bytes
 / BIG endian encoded
*/
    readosm_endian4 endian4;
    if (little_endian_cpu)
      {
	  endian4.bytes[0] = *(buf + 3);
	  endian4.bytes[1] = *(buf + 2);
	  endian4.bytes[2] = *(buf + 1);
	  endian4.bytes[3] = *(buf + 0);
      }
    else
      {
	  endian4.bytes[0] = *(buf + 0);
	  endian4.bytes[1] = *(buf + 1);
	  endian4.bytes[2] = *(buf + 2);
	  endian4.bytes[3] = *(buf + 3);
      }
    return endian4.uint32_value;
}

static unsigned char *
parse_field (unsigned char *start, unsigned char *stop,
	     readosm_variant * variant)
{
/* attempting to parse a variant field */
    unsigned char *ptr = start;
    unsigned char type;
    unsigned char field_id;
    unsigned char type_hint;

    if (ptr > stop)
	return NULL;

/*
 / any PBF field is prefixed by a single byte
 / a bitwise mask is used so to store both the
 / field-id and the field-type on a single byte
*/
    type = *ptr & 0x07;
    field_id = (*ptr & 0xf8) >> 3;

/* attempting to indentify the field accordingly to declared hints */
    if (!find_type_hint (variant, field_id, type, &type_hint))
	return NULL;

    variant->type = type_hint;
    variant->field_id = field_id;
    ptr++;

/* parsing the field value */
    switch (variant->type)
      {
      case READOSM_VAR_INT32:
      case READOSM_VAR_INT64:
      case READOSM_VAR_UINT32:
      case READOSM_VAR_UINT64:
      case READOSM_VAR_SINT32:
      case READOSM_VAR_SINT64:
	  return read_var (ptr, stop, variant);
      case READOSM_LEN_BYTES:
	  return read_bytes (ptr, stop, variant);
      };
    return NULL;
}

static int
skip_osm_header (readosm_file * input, unsigned int sz)
{
/*
 / expecting to retrieve a valid OSMHeader header 
 / there is nothing really interesting here, so we'll
 / simply discard the whole block, simply advancing
 / the read file-pointer as appropriate
*/
    int ok_header = 0;
    int hdsz = 0;
    size_t rd;
    unsigned char *buf = malloc (sz);
    unsigned char *base = buf;
    unsigned char *start = buf;
    unsigned char *stop = buf + sz - 1;
    readosm_variant variant;
    if (buf == NULL)
	goto error;

/* initializing an empty variant field */
    init_variant (&variant, input->little_endian_cpu);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 1);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 2);
    add_variant_hints (&variant, READOSM_VAR_INT32, 3);

    rd = fread (buf, 1, sz, input->in);
    if (rd != sz)
	goto error;

/* reading the OSMHeader header */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_LEN_BYTES
	      && variant.length == 9)
	    {
		if (memcmp (variant.pointer, "OSMHeader", 9) == 0)
		    ok_header = 1;
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_VAR_INT32)
	      hdsz = variant.value.int32_value;
	  if (base > stop)
	      break;
      }
    free (buf);
    buf = NULL;
    if (!ok_header || !hdsz)
	goto error;

    buf = malloc (hdsz);
    base = buf;
    start = buf;
    stop = buf + hdsz - 1;
    rd = fread (buf, 1, hdsz, input->in);
    if ((int) rd != hdsz)
	goto error;

    if (buf != NULL)
	free (buf);
    finalize_variant (&variant);
    return 1;

  error:
    if (buf != NULL)
	free (buf);
    finalize_variant (&variant);
    return 0;
}

static int
unzip_compressed_block (unsigned char *zip_ptr, unsigned int zip_sz,
			unsigned char *raw_ptr, unsigned int raw_sz)
{
/* 
 / decompressing a zip compressed block 
 / please note: PBF data blocks are internally stored as
 / ZIP compessed blocks
 /
 / both the compressed and uncompressed sizes are declared
 / for each PBF ZIPped block
*/
    uLongf size = raw_sz;
    int ret = uncompress (raw_ptr, &size, zip_ptr, zip_sz);
    if (ret != Z_OK || size != raw_sz)
	return 0;
    return 1;
}

static int
parse_string_table (readosm_string_table * string_table,
		    unsigned char *start, unsigned char *stop,
		    char little_endian_cpu)
{
/* 
 / attempting to parse a StringTable 
 / 
 / Remark: each PBF compressed block includes a StringTable
 / i.e. a centralized table where any string value used within
 / the compressed block itself appears only one time.
 / This is obviously intended so to minimize storage requirements.
 /
 / Individual objects within the PBF file will never directly
 / encode string values; they'll use instead the corresponding
 / index referencing the appropriate string within the StringTable.
*/
    readosm_variant variant;
    unsigned char *base = start;

/* initializing an empty variant field */
    init_variant (&variant, little_endian_cpu);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 1);

/* reading the StringTable */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_LEN_BYTES)
	      append_string_to_table (string_table, &variant);
	  if (base > stop)
	      break;
      }

    finalize_variant (&variant);
    return 1;

  error:
    finalize_variant (&variant);
    return 0;
}

static int
parse_pbf_node_infos (readosm_packed_infos * packed_infos,
		      unsigned char *start, unsigned char *stop,
		      char little_endian_cpu)
{
/* 
 / attempting to parse a valid PBF DenseInfos
 /
 / Remark: PBF DenseNodes blocks require a DenseInfos block
 / this actually consists in six strings:
 / - versions
 / - timestamps
 / - changesets
 / - uids
 / - user-names (expressed as index to StringTable entries)
 /
 / each "string" in turn contains an array of INT values;
 / and individual values are usually encoded as DELTAs,
 / i.e. differences respect the immedialy preceding value.
*/
    readosm_variant variant;
    unsigned char *base = start;
    readosm_uint32_packed packed_u32;
    readosm_uint32 *pu32;
    readosm_int32_packed packed_32;
    readosm_int32 *p32;
    readosm_int64_packed packed_64;
    readosm_int64 *p64;
    int count;

/* initializing empty packed objects */
    init_uint32_packed (&packed_u32);
    init_int32_packed (&packed_32);
    init_int64_packed (&packed_64);

/* initializing an empty variant field */
    init_variant (&variant, little_endian_cpu);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 1);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 2);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 3);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 4);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 5);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 6);

/* reading the DenseInfo block */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_LEN_BYTES)
	    {
		/* versions: *not* delta encoded */
		if (!parse_uint32_packed
		    (&packed_u32, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		count = 0;
		pu32 = packed_u32.first;
		while (pu32)
		  {
		      count++;
		      pu32 = pu32->next;
		  }
		packed_infos->ver_count = count;
		if (packed_infos->versions != NULL)
		  {
		      free (packed_infos->versions);
		      packed_infos->versions = NULL;
		  }
		if (count > 0)
		  {
		      packed_infos->versions = malloc (sizeof (int) * count);
		      count = 0;
		      pu32 = packed_u32.first;
		      while (pu32)
			{
			    *(packed_infos->versions + count) = pu32->value;
			    count++;
			    pu32 = pu32->next;
			}
		  }
		reset_uint32_packed (&packed_u32);
	    }
	  if (variant.field_id == 2 && variant.type == READOSM_LEN_BYTES)
	    {
		/* timestamps: delta encoded */
		int delta = 0;
		if (!parse_sint32_packed
		    (&packed_32, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		count = 0;
		p32 = packed_32.first;
		while (p32)
		  {
		      count++;
		      p32 = p32->next;
		  }
		packed_infos->tim_count = count;
		if (packed_infos->timestamps != NULL)
		  {
		      free (packed_infos->timestamps);
		      packed_infos->timestamps = NULL;
		  }
		if (count > 0)
		  {
		      packed_infos->timestamps = malloc (sizeof (int) * count);
		      count = 0;
		      p32 = packed_32.first;
		      while (p32)
			{
			    delta += p32->value;
			    *(packed_infos->timestamps + count) = delta;
			    count++;
			    p32 = p32->next;
			}
		  }
		reset_int32_packed (&packed_32);
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_LEN_BYTES)
	    {
		/* changesets: delta encoded */
		long long delta = 0;
		if (!parse_sint64_packed
		    (&packed_64, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		count = 0;
		p64 = packed_64.first;
		while (p64)
		  {
		      count++;
		      p64 = p64->next;
		  }
		packed_infos->cng_count = count;
		if (packed_infos->changesets != NULL)
		  {
		      free (packed_infos->changesets);
		      packed_infos->changesets = NULL;
		  }
		if (count > 0)
		  {
		      packed_infos->changesets =
			  malloc (sizeof (long long) * count);
		      count = 0;
		      p64 = packed_64.first;
		      while (p64)
			{
			    delta += p64->value;
			    *(packed_infos->changesets + count) = delta;
			    count++;
			    p64 = p64->next;
			}
		  }
		reset_int64_packed (&packed_64);
	    }
	  if (variant.field_id == 4 && variant.type == READOSM_LEN_BYTES)
	    {
		/* uids: delta encoded */
		int delta = 0;
		if (!parse_sint32_packed
		    (&packed_32, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		count = 0;
		p32 = packed_32.first;
		while (p32)
		  {
		      count++;
		      p32 = p32->next;
		  }
		packed_infos->uid_count = count;
		if (packed_infos->uids != NULL)
		  {
		      free (packed_infos->uids);
		      packed_infos->uids = NULL;
		  }
		if (count > 0)
		  {
		      packed_infos->uids = malloc (sizeof (int) * count);
		      count = 0;
		      p32 = packed_32.first;
		      while (p32)
			{
			    delta += p32->value;
			    *(packed_infos->uids + count) = delta;
			    count++;
			    p32 = p32->next;
			}
		  }
		reset_int32_packed (&packed_32);
	    }
	  if (variant.field_id == 5 && variant.type == READOSM_LEN_BYTES)
	    {
		/* user-names: delta encoded (index to StringTable) */
		int delta = 0;
		if (!parse_sint32_packed
		    (&packed_32, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		count = 0;
		p32 = packed_32.first;
		while (p32)
		  {
		      count++;
		      p32 = p32->next;
		  }
		packed_infos->usr_count = count;
		if (packed_infos->users != NULL)
		  {
		      free (packed_infos->users);
		      packed_infos->users = NULL;
		  }
		if (count > 0)
		  {
		      packed_infos->users = malloc (sizeof (int) * count);
		      count = 0;
		      p32 = packed_32.first;
		      while (p32)
			{
			    delta += p32->value;
			    *(packed_infos->users + count) = delta;
			    count++;
			    p32 = p32->next;
			}
		  }
		reset_int32_packed (&packed_32);
	    }
	  if (base > stop)
	      break;
      }
    finalize_uint32_packed (&packed_u32);
    finalize_int32_packed (&packed_32);
    finalize_int64_packed (&packed_64);
    finalize_variant (&variant);
    return 1;

  error:
    finalize_variant (&variant);
    finalize_uint32_packed (&packed_u32);
    finalize_int32_packed (&packed_32);
    finalize_int64_packed (&packed_64);
    return 0;
}

static int
parse_pbf_nodes (readosm_string_table * strings,
		 unsigned char *start, unsigned char *stop,
		 char little_endian_cpu, struct pbf_params *params)
{
/* 
 / attempting to parse a valid PBF DenseNodes 
 /
 / Remark: a PBF DenseNodes block consists in five strings:
 / - ids
 / - DenseInfos
 / - longitudes
 / - latitudes
 / - packed-keys (*)
 /
 / each "string" in turn contains an array of INT values;
 / and individual values are usually encoded as DELTAs,
 / i.e. differences respect the immedialy preceding value.
 /
 / (*) packed keys actually are encoded as arrays of index
 / to StringTable entries.
 / alternatively we have a key-index and then a value-index;
 / any 0 value means that the current Node stops: next index
 / will be a key-index for the next Node item
*/
    readosm_variant variant;
    unsigned char *base = start;
    readosm_uint32_packed packed_keys;
    readosm_int64_packed packed_ids;
    readosm_int64_packed packed_lats;
    readosm_int64_packed packed_lons;
    readosm_packed_infos packed_infos;
    readosm_internal_node *nodes = NULL;
    int nd_count = 0;

/* initializing empty packed objects */
    init_uint32_packed (&packed_keys);
    init_int64_packed (&packed_ids);
    init_int64_packed (&packed_lats);
    init_int64_packed (&packed_lons);
    init_packed_infos (&packed_infos);

/* initializing an empty variant field */
    init_variant (&variant, little_endian_cpu);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 1);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 5);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 8);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 9);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 10);

/* reading the Node */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_LEN_BYTES)
	    {
		/* NODE IDs */
		if (!parse_sint64_packed
		    (&packed_ids, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_int64_packed (&packed_ids);
	    }
	  if (variant.field_id == 5 && variant.type == READOSM_LEN_BYTES)
	    {
		/* DenseInfos */
		if (!parse_pbf_node_infos (&packed_infos,
					   variant.pointer,
					   variant.pointer + variant.length - 1,
					   variant.little_endian_cpu))
		    goto error;
	    }
	  if (variant.field_id == 8 && variant.type == READOSM_LEN_BYTES)
	    {
		/* latitudes */
		if (!parse_sint64_packed
		    (&packed_lats, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_int64_packed (&packed_lats);
	    }
	  if (variant.field_id == 9 && variant.type == READOSM_LEN_BYTES)
	    {
		/* longitudes */
		if (!parse_sint64_packed
		    (&packed_lons, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_int64_packed (&packed_lons);
	    }
	  if (variant.field_id == 10 && variant.type == READOSM_LEN_BYTES)
	    {
		/* packes-keys */
		if (!parse_uint32_packed
		    (&packed_keys, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_uint32_packed (&packed_keys);
	    }
	  if (base > stop)
	      break;
      }
    if (packed_ids.count == packed_lats.count
	&& packed_ids.count == packed_lons.count
	&& packed_ids.count == packed_infos.ver_count
	&& packed_ids.count == packed_infos.tim_count
	&& packed_ids.count == packed_infos.cng_count
	&& packed_ids.count == packed_infos.uid_count
	&& packed_ids.count == packed_infos.usr_count)
      {
	  /* 
	     / all right, we now have the same item count anywhere
	     / we can now go further away attempting to reassemble
	     / individual Nodes 
	   */
	  readosm_internal_node *nd;
	  int i;
	  int i_keys = 0;
	  long long delta_id = 0;
	  long long delta_lat = 0;
	  long long delta_lon = 0;
	  nd_count = packed_ids.count;
	  nodes = malloc (sizeof (readosm_internal_node) * nd_count);
	  for (i = 0; i < nd_count; i++)
	    {
		/* initializing an array of empty internal Nodes */
		nd = nodes + i;
		init_internal_node (nd);
	    }
	  for (i = 0; i < nd_count; i++)
	    {
		/* reassembling internal Nodes */
		const char *key = NULL;
		const char *value = NULL;
		time_t xtime;
		struct tm *times;
		int s_id;
		nd = nodes + i;
		delta_id += *(packed_ids.values + i);
		delta_lat += *(packed_lats.values + i);
		delta_lon += *(packed_lons.values + i);
		nd->id = delta_id;
		/* latitudes and longitudes require to be rescaled as DOUBLEs */
		nd->latitude = delta_lat / 10000000.0;
		nd->longitude = delta_lon / 10000000.0;
		nd->version = *(packed_infos.versions + i);
		xtime = *(packed_infos.timestamps + i);
		times = gmtime (&xtime);
		if (times)
		  {
		      /* formatting Timestamps */
		      char buf[64];
		      int len;
		      sprintf (buf, "%04d-%02d-%02dT%02d:%02d:%02dZ",
			       times->tm_year + 1900, times->tm_mon + 1,
			       times->tm_mday, times->tm_hour, times->tm_min,
			       times->tm_sec);
		      if (nd->timestamp)
			  free (nd->timestamp);
		      len = strlen (buf);
		      nd->timestamp = malloc (len + 1);
		      strcpy (nd->timestamp, buf);
		  }
		nd->changeset = *(packed_infos.changesets + i);
		if (*(packed_infos.uids + i) >= 0)
		    nd->uid = *(packed_infos.uids + i);
		s_id = *(packed_infos.users + i);
		if (s_id > 0)
		  {
		      /* retrieving user-names as strings (by index) */
		      readosm_string *s_ptr = *(strings->strings + s_id);
		      int len = strlen (s_ptr->string);
		      if (nd->user != NULL)
			  free (nd->user);
		      if (len > 0)
			{
			    nd->user = malloc (len + 1);
			    strcpy (nd->user, s_ptr->string);
			}
		  }
		for (; i_keys < packed_keys.count; i_keys++)
		  {
		      /* decoding packed-keys */
		      int is = *(packed_keys.values + i_keys);
		      if (is == 0)
			{
			    /* next Node */
			    i_keys++;
			    break;
			}
		      if (key == NULL)
			{
			    readosm_string *s_ptr = *(strings->strings + is);
			    key = s_ptr->string;
			}
		      else
			{
			    readosm_string *s_ptr = *(strings->strings + is);
			    value = s_ptr->string;
			    append_tag_to_node (nd, key, value);
			    key = NULL;
			    value = NULL;
			}
		  }
	    }
      }

/* memory cleanup */
    finalize_uint32_packed (&packed_keys);
    finalize_int64_packed (&packed_ids);
    finalize_int64_packed (&packed_lats);
    finalize_int64_packed (&packed_lons);
    finalize_packed_infos (&packed_infos);
    finalize_variant (&variant);

/* processing each Node in the block */
    if (params->node_callback != NULL && params->stop == 0)
      {
	  int ret;
	  readosm_internal_node *nd;
	  int i;
	  for (i = 0; i < nd_count; i++)
	    {
		nd = nodes + i;
		ret =
		    call_node_callback (params->node_callback,
					params->user_data, nd);
		if (ret != READOSM_OK)
		  {
		      params->stop = 1;
		      break;
		  }
	    }
      }

/* memory cleanup: destroying Nodes */
    if (nodes != NULL)
      {
	  readosm_internal_node *nd;
	  int i;
	  for (i = 0; i < nd_count; i++)
	    {
		nd = nodes + i;
		destroy_internal_node (nd);
	    }
	  free (nodes);
      }
    return 1;

  error:
    finalize_uint32_packed (&packed_keys);
    finalize_int64_packed (&packed_ids);
    finalize_int64_packed (&packed_lats);
    finalize_int64_packed (&packed_lons);
    finalize_packed_infos (&packed_infos);
    finalize_variant (&variant);
    if (nodes != NULL)
      {
	  readosm_internal_node *nd;
	  int i;
	  for (i = 0; i < nd_count; i++)
	    {
		nd = nodes + i;
		destroy_internal_node (nd);
	    }
	  free (nodes);
      }
    return 0;
}

static int
parse_pbf_way_info (readosm_internal_way * way, readosm_string_table * strings,
		    unsigned char *start, unsigned char *stop,
		    char little_endian_cpu)
{
/* attempting to parse a valid PBF Way-Info */
    readosm_variant variant;
    unsigned char *base = start;

/* initializing an empty variant field */
    init_variant (&variant, little_endian_cpu);
    add_variant_hints (&variant, READOSM_VAR_INT32, 1);
    add_variant_hints (&variant, READOSM_VAR_INT32, 2);
    add_variant_hints (&variant, READOSM_VAR_INT64, 3);
    add_variant_hints (&variant, READOSM_VAR_INT32, 4);
    add_variant_hints (&variant, READOSM_VAR_INT32, 5);
    add_variant_hints (&variant, READOSM_VAR_INT32, 6);

/* reading the WayInfo */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_VAR_INT32)
	    {
		/* version */
		way->version = variant.value.int32_value;
	    }
	  if (variant.field_id == 2 && variant.type == READOSM_VAR_INT32)
	    {
		/* timestamp */
		const time_t xtime = variant.value.int32_value;
		struct tm *times = gmtime (&xtime);
		if (times)
		  {
		      char buf[64];
		      int len;
		      sprintf (buf, "%04d-%02d-%02dT%02d:%02d:%02dZ",
			       times->tm_year + 1900, times->tm_mon + 1,
			       times->tm_mday, times->tm_hour, times->tm_min,
			       times->tm_sec);
		      if (way->timestamp)
			  free (way->timestamp);
		      len = strlen (buf);
		      way->timestamp = malloc (len + 1);
		      strcpy (way->timestamp, buf);
		  }
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_VAR_INT64)
	    {
		/* changeset */
		way->changeset = variant.value.int64_value;
	    }
	  if (variant.field_id == 4 && variant.type == READOSM_VAR_INT32)
	    {
		/* uid */
		way->uid = variant.value.int32_value;
	    }
	  if (variant.field_id == 5 && variant.type == READOSM_VAR_INT32)
	    {
		/* user-name: index to StringTable entry */
		int userid;
		if (way->user)
		    free (way->user);
		way->user = NULL;
		userid = variant.value.int32_value;
		if (userid > 0 && userid < strings->count)
		  {
		      readosm_string *string = *(strings->strings + userid);
		      int len = strlen (string->string);
		      way->user = malloc (len + 1);
		      strcpy (way->user, string->string);
		  }
	    }
	  if (base > stop)
	      break;
      }
    finalize_variant (&variant);
    return 1;

  error:
    finalize_variant (&variant);
    return 0;
}

		/* user-name: index to StringTable entry */

static int
parse_pbf_way (readosm_string_table * strings,
	       unsigned char *start, unsigned char *stop,
	       char little_endian_cpu, struct pbf_params *params)
{
/* attempting to parse a valid PBF Way */
    readosm_variant variant;
    unsigned char *base = start;
    readosm_uint32_packed packed_keys;
    readosm_uint32_packed packed_values;
    readosm_int64_packed packed_refs;
    readosm_internal_way *way = alloc_internal_way ();

/* initializing empty packed objects */
    init_uint32_packed (&packed_keys);
    init_uint32_packed (&packed_values);
    init_int64_packed (&packed_refs);

/* initializing an empty variant field */
    init_variant (&variant, little_endian_cpu);
    add_variant_hints (&variant, READOSM_VAR_INT64, 1);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 2);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 3);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 4);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 8);

/* reading the Way */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_VAR_INT64)
	    {
		/* WAY ID */
		way->id = variant.value.int64_value;
	    }
	  if (variant.field_id == 2 && variant.type == READOSM_LEN_BYTES)
	    {
		/* KEYs are encoded as an array of StringTable index */
		if (!parse_uint32_packed
		    (&packed_keys, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_uint32_packed (&packed_keys);
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_LEN_BYTES)
	    {
		/* VALUEs are encoded as an array of StringTable index  */
		if (!parse_uint32_packed
		    (&packed_values, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_uint32_packed (&packed_values);
	    }
	  if (variant.field_id == 4 && variant.type == READOSM_LEN_BYTES)
	    {
		/* WAY-INFO block */
		if (!parse_pbf_way_info
		    (way, strings, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
	    }
	  if (variant.field_id == 8 && variant.type == READOSM_LEN_BYTES)
	    {
		/* NODE-REFs */
		long long delta = 0;
		readosm_int64 *value;
		/* KEYs are encoded as an array of StringTable index */
		if (!parse_sint64_packed
		    (&packed_refs, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		value = packed_refs.first;
		while (value != NULL)
		  {
		      /* appending Node references to Way */
		      delta += value->value;
		      append_reference_to_way (way, delta);
		      value = value->next;
		  }
	    }
	  if (base > stop)
	      break;
      }

/* reassembling a WAY object */
    if (packed_keys.count == packed_values.count)
      {
	  int i;
	  for (i = 0; i < packed_keys.count; i++)
	    {
		int i_key = *(packed_keys.values + i);
		int i_val = *(packed_values.values + i);
		readosm_string *s_key = *(strings->strings + i_key);
		readosm_string *s_value = *(strings->strings + i_val);
		append_tag_to_way (way, s_key
				   /* user-name: index to StringTable entry */
				   ->string, s_value->string);
	    }
      }
    else
	goto error;

    finalize_uint32_packed (&packed_keys);
    finalize_uint32_packed (&packed_values);
    finalize_int64_packed (&packed_refs);
    finalize_variant (&variant);

/* processing the WAY */
    if (params->way_callback != NULL && params->stop == 0)
      {
	  int ret =
	      call_way_callback (params->way_callback, params->user_data, way);
	  if (ret != READOSM_OK)
	      params->stop = 1;
      }
    destroy_internal_way (way);
    return 1;

  error:
    finalize_uint32_packed (&packed_keys);
    finalize_uint32_packed (&packed_values);
    finalize_int64_packed (&packed_refs);
    finalize_variant (&variant);
    destroy_internal_way (way);
    return 0;
}

static int
parse_pbf_relation_info (readosm_internal_relation * relation,
			 readosm_string_table * strings, unsigned char *start,
			 unsigned char *stop, char little_endian_cpu)
{
/* attempting to parse a valid PBF RelationInfo */
    readosm_variant variant;
    unsigned char *base = start;

/* initializing an empty variant field */
    init_variant (&variant, little_endian_cpu);
    add_variant_hints (&variant, READOSM_VAR_INT32, 1);
    add_variant_hints (&variant, READOSM_VAR_INT32, 2);
    add_variant_hints (&variant, READOSM_VAR_INT64, 3);
    add_variant_hints (&variant, READOSM_VAR_INT32, 4);
    add_variant_hints (&variant, READOSM_VAR_INT32, 5);
    add_variant_hints (&variant, READOSM_VAR_INT32, 6);

/* reading the RelationInfo */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_VAR_INT32)
	    {
		/* version */
		relation->version = variant.value.int32_value;
	    }
	  if (variant.field_id == 2 && variant.type == READOSM_VAR_INT32)
	    {
		/* timestamp */
		const time_t xtime = variant.value.int32_value;
		struct tm *times = gmtime (&xtime);
		if (times)
		  {
		      char buf[64];
		      int len;
		      sprintf (buf, "%04d-%02d-%02dT%02d:%02d:%02dZ",
			       times->tm_year + 1900, times->tm_mon + 1,
			       times->tm_mday, times->tm_hour, times->tm_min,
			       times->tm_sec);
		      if (relation->timestamp)
			  free (relation->timestamp);
		      len = strlen (buf);
		      relation->timestamp = malloc (len + 1);
		      strcpy (relation->timestamp, buf);
		  }
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_VAR_INT64)
	    {
		/* changeset */
		relation->changeset = variant.value.int64_value;
	    }
	  if (variant.field_id == 4 && variant.type == READOSM_VAR_INT32)
	    {
		/* uid */
		relation->uid = variant.value.int32_value;
	    }
	  if (variant.field_id == 5 && variant.type == READOSM_VAR_INT32)
	    {
		/* user-name: index to StringTable entry */
		int userid;
		if (relation->user)
		    free (relation->user);
		relation->user = NULL;
		userid = variant.value.int32_value;
		if (userid > 0 && userid < strings->count)
		  {
		      readosm_string *string = *(strings->strings + userid);
		      int len = strlen (string->string);
		      relation->user = malloc (len + 1);
		      strcpy (relation->user, string->string);
		  }
	    }
	  if (base > stop)
	      break;
      }
    finalize_variant (&variant);
    return 1;

  error:
    finalize_variant (&variant);
    return 0;
}

static int
parse_pbf_relation (readosm_string_table * strings,
		    unsigned char *start, unsigned char *stop,
		    char little_endian_cpu, struct pbf_params *params)
{
/* attempting to parse a valid PBF Relation */
    readosm_variant variant;
    unsigned char *base = start;
    readosm_uint32_packed packed_keys;
    readosm_uint32_packed packed_values;
    readosm_uint32_packed packed_roles;
    readosm_uint32_packed packed_types;
    readosm_int64_packed packed_refs;
    readosm_internal_relation *relation = alloc_internal_relation ();

/* initializing empty packed objects */
    init_uint32_packed (&packed_keys);
    init_uint32_packed (&packed_values);
    init_uint32_packed (&packed_roles);
    init_uint32_packed (&packed_types);
    init_int64_packed (&packed_refs);

/* initializing an empty variant field */
    init_variant (&variant, little_endian_cpu);
    add_variant_hints (&variant, READOSM_VAR_INT64, 1);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 2);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 3);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 4);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 8);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 9);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 10);

/* reading the Relation */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_VAR_INT64)
	    {
		/* RELATION ID */
		relation->id = variant.value.int64_value;
	    }
	  if (variant.field_id == 2 && variant.type == READOSM_LEN_BYTES)
	    {
		/* KEYs are encoded as an array of StringTable index */
		if (!parse_uint32_packed
		    (&packed_keys, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_uint32_packed (&packed_keys);
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_LEN_BYTES)
	    {
		/* VALUEs are encoded as an array of StringTable index */
		if (!parse_uint32_packed
		    (&packed_values, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_uint32_packed (&packed_values);
	    }
	  if (variant.field_id == 4 && variant.type == READOSM_LEN_BYTES)
	    {
		/* RELATION-INFO block */
		if (!parse_pbf_relation_info
		    (relation, strings, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
	    }
	  if (variant.field_id == 8 && variant.type == READOSM_LEN_BYTES)
	    {
		/* MEMBER-ROLEs are encoded as an array of StringTable index */
		if (!parse_uint32_packed
		    (&packed_roles, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_uint32_packed (&packed_roles);
	    }
	  if (variant.field_id == 9 && variant.type == READOSM_LEN_BYTES)
	    {
		/* MEMBER-REFs are encoded as an array */
		if (!parse_sint64_packed
		    (&packed_refs, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_int64_packed (&packed_refs);
	    }
	  if (variant.field_id == 10 && variant.type == READOSM_LEN_BYTES)
	    {
		/* MEMBER-TYPEs are encoded as an array */
		if (!parse_uint32_packed
		    (&packed_types, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_uint32_packed (&packed_types);
	    }
	  if (base > stop)
	      break;
      }

/* reassembling a RELATION object */
    if (packed_keys.count == packed_values.count)
      {
	  int i;
	  for (i = 0; i < packed_keys.count; i++)
	    {
		int i_key = *(packed_keys.values + i);
		int i_val = *(packed_values.values + i);
		readosm_string *s_key = *(strings->strings + i_key);
		readosm_string *s_value = *(strings->strings + i_val);
		append_tag_to_relation (relation, s_key->string,
					s_value->string);
	    }
      }
    else
	goto error;
    if (packed_roles.count == packed_refs.count
	&& packed_roles.count == packed_types.count)
      {
	  int i;
	  long long delta = 0;
	  for (i = 0; i < packed_roles.count; i++)
	    {
		int i_role = *(packed_roles.values + i);
		readosm_string *s_role = *(strings->strings + i_role);
		int type = *(packed_types.values + i);
		delta += *(packed_refs.values + i);
		append_member_to_relation (relation, type, delta,
					   s_role->string);
	    }
      }
    else
	goto error;

    finalize_uint32_packed (&packed_keys);
    finalize_uint32_packed (&packed_values);
    finalize_uint32_packed (&packed_roles);
    finalize_uint32_packed (&packed_types);
    finalize_int64_packed (&packed_refs);
    finalize_variant (&variant);

/* processing the RELATION */
    if (params->relation_callback != NULL && params->stop == 0)
      {
	  int ret = call_relation_callback (params->relation_callback,
					    params->user_data, relation);
	  if (ret != READOSM_OK)
	      params->stop = 1;
      }
    destroy_internal_relation (relation);
    return 1;

  error:
    finalize_uint32_packed (&packed_keys);
    finalize_uint32_packed (&packed_values);
    finalize_uint32_packed (&packed_roles);
    finalize_uint32_packed (&packed_types);
    finalize_int64_packed (&packed_refs);
    finalize_variant (&variant);
    destroy_internal_relation (relation);
    return 0;
}

static int
parse_primitive_group (readosm_string_table * strings,
		       unsigned char *start, unsigned char *stop,
		       char little_endian_cpu, struct pbf_params *params)
{
/* 
 / attempting to parse a valid Primitive Group 
 /
 / each PBF PrimitiveGroup can store only one type:
 / - NODEs
 / - WAYs
 / - RELATIONs
*/
    readosm_variant variant;
    unsigned char *base = start;

/* initializing an empty variant field */
    init_variant (&variant, little_endian_cpu);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 1);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 2);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 3);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 4);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 5);

/* reading the Primitive Group */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 2 && variant.type == READOSM_LEN_BYTES)
	    {
		/* DenseNodes */
		if (!parse_pbf_nodes
		    (strings, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu, params))
		    goto error;
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_LEN_BYTES)
	    {
		/* Way */
		if (!parse_pbf_way
		    (strings, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu, params))
		    goto error;
	    }
	  if (variant.field_id == 4 && variant.type == READOSM_LEN_BYTES)
	    {
		/* Relation */
		if (!parse_pbf_relation
		    (strings, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu, params))
		    goto error;
	    }
	  if (base > stop)
	      break;
      }
    finalize_variant (&variant);
    return 1;

  error:
    finalize_variant (&variant);
    return 0;
}

static int
parse_osm_data (readosm_file * input, unsigned int sz,
		struct pbf_params *params)
{
/* expecting to retrieve a valid OSMData header */
    int ok_header = 0;
    int hdsz = 0;
    size_t rd;
    unsigned char *buf = malloc (sz);
    unsigned char *base = buf;
    unsigned char *start = buf;
    unsigned char *stop = buf + sz - 1;
    unsigned char *zip_ptr = NULL;
    int zip_sz = 0;
    unsigned char *raw_ptr = NULL;
    int raw_sz = 0;
    readosm_variant variant;
    readosm_string_table string_table;
    if (buf == NULL)
	goto error;

/* initializing an empty string list */
    init_string_table (&string_table);

/* initializing an empty variant field */
    init_variant (&variant, input->little_endian_cpu);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 1);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 2);
    add_variant_hints (&variant, READOSM_VAR_INT32, 3);

    rd = fread (buf, 1, sz, input->in);
    if (rd != sz)
	goto error;

/* reading the OSMData header */
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_LEN_BYTES
	      && variant.length == 7)
	    {
		if (memcmp (variant.pointer, "OSMData", 7) == 0)
		    ok_header = 1;
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_VAR_INT32)
	      hdsz = variant.value.int32_value;
	  if (base > stop)
	      break;
      }
    free (buf);
    buf = NULL;
    if (!ok_header || !hdsz)
	goto error;

    buf = malloc (hdsz);
    base = buf;
    start = buf;
    stop = buf + hdsz - 1;
    rd = fread (buf, 1, hdsz, input->in);
    if ((int) rd != hdsz)
	goto error;

/* uncompressing the OSMData zipped */
    finalize_variant (&variant);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 1);
    add_variant_hints (&variant, READOSM_VAR_INT32, 2);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 3);
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_LEN_BYTES)
	    {
		/* found an uncompressed block */
		raw_sz = variant.length;
		raw_ptr = malloc (raw_sz);
		memcpy (raw_ptr, variant.pointer, raw_sz);
	    }
	  if (variant.field_id == 2 && variant.type == READOSM_VAR_INT32)
	    {
		/* expected size of unZipped block */
		raw_sz = variant.value.int32_value;
	    }
	  if (variant.field_id == 3 && variant.type == READOSM_LEN_BYTES)
	    {
		/* found a ZIP-compressed block */
		zip_ptr = variant.pointer;
		zip_sz = variant.length;
	    }
	  if (base > stop)
	      break;
      }
    if (zip_ptr != NULL && zip_sz != 0 && raw_sz != 0)
      {
	  /* unZipping a compressed block */
	  raw_ptr = malloc (raw_sz);
	  if (!unzip_compressed_block (zip_ptr, zip_sz, raw_ptr, raw_sz))
	      goto error;
      }
    free (buf);
    buf = NULL;
    if (raw_ptr == NULL || raw_sz == 0)
	goto error;

/* parsing the PrimitiveBlock */
    base = raw_ptr;
    start = raw_ptr;
    stop = raw_ptr + raw_sz - 1;
    finalize_variant (&variant);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 1);
    add_variant_hints (&variant, READOSM_LEN_BYTES, 2);
    add_variant_hints (&variant, READOSM_VAR_INT32, 17);
    add_variant_hints (&variant, READOSM_VAR_INT32, 18);
    add_variant_hints (&variant, READOSM_VAR_INT64, 19);
    add_variant_hints (&variant, READOSM_VAR_INT64, 20);
    while (1)
      {
	  /* resetting an empty variant field */
	  reset_variant (&variant);

	  base = parse_field (start, stop, &variant);
	  if (base == NULL && variant.valid == 0)
	      goto error;
	  start = base;
	  if (variant.field_id == 1 && variant.type == READOSM_LEN_BYTES)
	    {
		/* the StringTable */
		if (!parse_string_table
		    (&string_table, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu))
		    goto error;
		array_from_string_table (&string_table);
	    }
	  if (variant.field_id == 2 && variant.type == READOSM_LEN_BYTES)
	    {
		/* the PrimitiveGroup to be parsed */
		if (!parse_primitive_group
		    (&string_table, variant.pointer,
		     variant.pointer + variant.length - 1,
		     variant.little_endian_cpu, params))
		    goto error;
	    }
	  if (variant.field_id == 17 && variant.type == READOSM_VAR_INT32)
	    {
		/* assumed to be a termination marker (???) */
		break;
	    }
	  if (base > stop)
	      break;
      }

    if (buf != NULL)
	free (buf);
    if (raw_ptr != NULL)
	free (raw_ptr);
    finalize_variant (&variant);
    finalize_string_table (&string_table);
    return 1;

  error:
    if (buf != NULL)
	free (buf);
    if (raw_ptr != NULL)
	free (raw_ptr);
    finalize_variant (&variant);
    finalize_string_table (&string_table);
    return 0;
}

static int
parse_osm_pbf (readosm_file * input, const void *user_data,
	       readosm_node_callback node_fnct, readosm_way_callback way_fnct,
	       readosm_relation_callback relation_fnct)
{
/* parsing the input file [OSM PBF format] */
    size_t rd;
    unsigned char buf[8];
    unsigned int hdsz;
    struct pbf_params params;

/* initializing the PBF helper structure */
    params.user_data = user_data;
    params.node_callback = node_fnct;
    params.way_callback = way_fnct;
    params.relation_callback = relation_fnct;
    params.stop = 0;

/* reading BlobHeader size: OSMHeader */
    rd = fread (buf, 1, 4, input->in);
    if (rd != 4)
	return READOSM_INVALID_PBF_HEADER;
    hdsz = get_header_size (buf, input->little_endian_cpu);

/* testing OSMHeader */
    if (!skip_osm_header (input, hdsz))
	return READOSM_INVALID_PBF_HEADER;

/* 
 / the PBF file is internally organized as a collection
 / of many subsequent OSMData blocks 
*/
    while (1)
      {
	  /* reading BlobHeader size: OSMData */
	  if (params.stop)
	      return READOSM_ABORT;
	  rd = fread (buf, 1, 4, input->in);
	  if (rd == 0 && feof (input->in))
	      break;
	  if (rd != 4)
	      return READOSM_INVALID_PBF_HEADER;
	  hdsz = get_header_size (buf, input->little_endian_cpu);

	  /* parsing OSMData */
	  if (!parse_osm_data (input, hdsz, &params))
	      return READOSM_INVALID_PBF_HEADER;
      }
    return READOSM_OK;
}

READOSM_DECLARE int
readosm_open (const char *path, const void **osm_handle)
{
/* opening and initializing the OSM input file */
    readosm_file *input;
    int len;
    int format;
    int little_endian_cpu = test_endianness ();

    *osm_handle = NULL;
    if (path == NULL || osm_handle == NULL)
	return READOSM_NULL_HANDLE;

    len = strlen (path);
    if (len > 4 && strcasecmp (path + len - 4, ".osm") == 0)
	format = READOSM_OSM_FORMAT;
    else if (len > 8 && strcasecmp (path + len - 8, ".osm.pbf") == 0)
	format = READOSM_PBF_FORMAT;
    else
	return READOSM_INVALID_SUFFIX;

/* allocating the OSM input file struct */
    input = alloc_osm_file (little_endian_cpu, format);
    if (!input)
	return READOSM_INSUFFICIENT_MEMORY;
    *osm_handle = input;

    input->in = fopen (path, "rb");
    if (input->in == NULL)
	return READOSM_FILE_NOT_FOUND;

    return READOSM_OK;
}

READOSM_DECLARE int
readosm_close (const void *osm_handle)
{
/* attempting to destroy the OSM input file */
    readosm_file *input = (readosm_file *) osm_handle;
    if (!input)
	return READOSM_NULL_HANDLE;
    if ((input->magic1 == READOSM_MAGIC_START)
	&& input->magic2 == READOSM_MAGIC_END)
	;
    else
	return READOSM_INVALID_HANDLE;

/* destroying the workbook */
    destroy_osm_file (input);

    return READOSM_OK;
}

READOSM_DECLARE int
readosm_parse (const void *osm_handle, const void *user_data,
	       readosm_node_callback node_fnct, readosm_way_callback way_fnct,
	       readosm_relation_callback relation_fnct)
{
/* attempting to parse the OSM input file */
    int ret;
    readosm_file *input = (readosm_file *) osm_handle;
    if (!input)
	return READOSM_NULL_HANDLE;
    if ((input->magic1 == READOSM_MAGIC_START)
	&& input->magic2 == READOSM_MAGIC_END)
	;
    else
	return READOSM_INVALID_HANDLE;

    if (input->file_format == READOSM_OSM_FORMAT)
	ret =
	    parse_osm_xml (input, user_data, node_fnct, way_fnct,
			   relation_fnct);
    else if (input->file_format == READOSM_PBF_FORMAT)
	ret =
	    parse_osm_pbf (input, user_data, node_fnct, way_fnct,
			   relation_fnct);
    else
	return READOSM_INVALID_HANDLE;

    return ret;
}
