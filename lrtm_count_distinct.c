
#include <assert.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <limits.h>

#include "postgres.h"
#include "utils/datum.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "nodes/execnodes.h"
#include "access/tupmacs.h"
#include "utils/pg_crc.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* if set to 1, the table resize will be profiled */
#define DEBUG_PROFILE       0

#define GET_AGG_CONTEXT(fname, fcinfo, aggcontext)  \
    if (! AggCheckCallContext(fcinfo, &aggcontext)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
    }

#define CHECK_AGG_CONTEXT(fname, fcinfo)  \
    if (! AggCheckCallContext(fcinfo, NULL)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
    }

#define ARRAY_INIT_SIZE     32      /* initial size of the array (in bytes) */
#define ARRAY_FREE_FRACT    0.2     /* we want >= 20% free space after compaction */

/* A hash table - a collection of buckets. */
typedef struct element_set_t {

    uint32  item_size;  /* length of the value (depends on the actual data type) */
    uint32  nsorted;    /* number of items in the sorted part (distinct) */
    uint32  nall;       /* number of all items (unsorted part may contain duplicates) */
    uint32  nbytes;     /* number of bytes in the data array */

    /* used for arrays only (cache for get_typlenbyvalalign results) */
    char    typalign;

    /* aggregation memory context (reference, so we don't need to do lookups repeatedly) */
    MemoryContext aggctx;

    /* elements */
    char *  data;       /* nsorted items first, then (nall - nsorted) unsorted items */

} element_set_t;


/*
 * prototypes
 */

/* transition functions */
PG_FUNCTION_INFO_V1(lrtm_count_distinct_append);
PG_FUNCTION_INFO_V1(lrtm_count_distinct_elements_append);

/* parallel aggregation support functions */
PG_FUNCTION_INFO_V1(lrtm_count_distinct_serial);
PG_FUNCTION_INFO_V1(lrtm_count_distinct_deserial);
PG_FUNCTION_INFO_V1(lrtm_count_distinct_combine);

/* final functions */
PG_FUNCTION_INFO_V1(lrtm_count_distinct);
PG_FUNCTION_INFO_V1(lrtm_array_agg_distinct_type_by_element);
PG_FUNCTION_INFO_V1(lrtm_array_agg_distinct_type_by_array);

/* supplementary subroutines */
static void add_element(element_set_t * eset, char * value);
static element_set_t *init_set(int item_size, char typalign, MemoryContext ctx);
static int compare_items(const void * a, const void * b, void * size);
static void compact_set(element_set_t * eset, bool need_space);
static Datum build_array(element_set_t * eset, Oid input_type);

#if DEBUG_PROFILE
static void print_set_stats(element_set_t * eset);
#endif


Datum
lrtm_count_distinct_append(PG_FUNCTION_ARGS)
{
    element_set_t  *eset;

    /* info for anyelement */
    Oid         element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
    Datum       element = PG_GETARG_DATUM(1);

    /* memory contexts */
    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /*
     * If the new value is NULL, we simply return the current aggregate state
     * (it might be NULL, so check it).
     */
    if (PG_ARGISNULL(1) && PG_ARGISNULL(0))
        PG_RETURN_NULL();
    else if (PG_ARGISNULL(1))
        PG_RETURN_DATUM(PG_GETARG_DATUM(0));

    /* from now on we know the new value is not NULL */

    /* switch to the per-group hash-table memory context */
    GET_AGG_CONTEXT("lrtm_count_distinct_append", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    /* init the hash table, if needed */
    if (PG_ARGISNULL(0))
    {
        int16       typlen;
        bool        typbyval;
        char        typalign;

        /* get type information for the second parameter (anyelement item) */
        get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

        /* we can't handle varlena types yet or values passed by reference */
        if ((typlen < 0) || (! typbyval))
            elog(ERROR, "lrtm_count_distinct handles only fixed-length types passed by value");

        eset = init_set(typlen, typalign, aggcontext);
    } else
        eset = (element_set_t *)PG_GETARG_POINTER(0);

    /* add the value into the set */
    add_element(eset, (char*)&element);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(eset);
}

Datum
lrtm_count_distinct_elements_append(PG_FUNCTION_ARGS)
{
    int             i;
    element_set_t  *eset = NULL;

    /* info for anyarray */
    Oid input_type;
    Oid element_type;

    /* array data */
    ArrayType  *input;
    int         ndims;
    int        *dims;
    int         nitems;
    bits8      *null_bitmap;
    char       *arr_ptr;
    Datum       element;

    /* memory contexts */
    MemoryContext oldcontext;
    MemoryContext aggcontext;

    if (PG_ARGISNULL(1) && PG_ARGISNULL(0))
        PG_RETURN_NULL();
    else if (PG_ARGISNULL(1))
        PG_RETURN_DATUM(PG_GETARG_DATUM(0));

    /* from now on we know the new value is not NULL */

    /* get the type of array elements */
    input_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
    element_type = get_element_type(input_type);

    /*
     * parse the array contents (we know we got non-NULL value)
     */
    input = PG_GETARG_ARRAYTYPE_P(1);
    ndims = ARR_NDIM(input);
    dims = ARR_DIMS(input);
    nitems = ArrayGetNItems(ndims, dims);
    null_bitmap = ARR_NULLBITMAP(input);
    arr_ptr = ARR_DATA_PTR(input);

    /* make sure we're running as part of aggregate function */
    GET_AGG_CONTEXT("lrtm_count_distinct_elements_append", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    /* add all array elements to the set */
    for (i = 0; i < nitems; i++)
    {
        /* ignore nulls */
        if (null_bitmap && !(null_bitmap[i / 8] & (1 << (i % 8))))
            continue;

        /* init the hash table, if needed */
        if (eset == NULL)
        {
            if (PG_ARGISNULL(0))
            {
                int16       typlen;
                bool        typbyval;
                char        typalign;

                /* get type information for the second parameter (anyelement item) */
                get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

                /* we can't handle varlena types yet or values passed by reference */
                if ((typlen < 0) || (! typbyval))
                    elog(ERROR, "lrtm_count_distinct_elements handles only arrays of fixed-length types passed by value");

                eset = init_set(typlen, typalign, aggcontext);
            }
            else
                eset = (element_set_t *)PG_GETARG_POINTER(0);
        }

        element = fetch_att(arr_ptr, true, eset->item_size);

        add_element(eset, (char*)&element);

        /* advance array pointer */
        arr_ptr = att_addlength_pointer(arr_ptr, eset->item_size, arr_ptr);
        arr_ptr = (char *) att_align_nominal(arr_ptr, eset->typalign);
    }

    MemoryContextSwitchTo(oldcontext);

    if (eset == NULL)
        PG_RETURN_NULL();
    else
        PG_RETURN_POINTER(eset);
}

Datum
lrtm_count_distinct_serial(PG_FUNCTION_ARGS)
{
    element_set_t * eset = (element_set_t *)PG_GETARG_POINTER(0);
    Size    hlen = offsetof(element_set_t, data);   /* header */
    Size    dlen;                                   /* elements */
    bytea  *out;                                    /* output */
    char   *ptr;

    Assert(eset != NULL);

    CHECK_AGG_CONTEXT("lrtm_count_distinct_serial", fcinfo);

    compact_set(eset, false);

    Assert(eset->nall > 0);
    Assert(eset->nall == eset->nsorted);

    dlen = eset->nall * eset->item_size;

    out = (bytea *)palloc(VARHDRSZ + dlen + hlen);

    SET_VARSIZE(out, VARHDRSZ + dlen + hlen);
    ptr = VARDATA(out);

    memcpy(ptr, eset, hlen);
    ptr += hlen;

    memcpy(ptr, eset->data, dlen);

    PG_RETURN_BYTEA_P(out);
}

Datum
lrtm_count_distinct_deserial(PG_FUNCTION_ARGS)
{
    element_set_t *eset = (element_set_t *)palloc(sizeof(element_set_t));
    bytea  *state = (bytea *)PG_GETARG_POINTER(0);
#ifdef USE_ASSERT_CHECKING
    Size	len = VARSIZE_ANY_EXHDR(state);
#endif
    char   *ptr = VARDATA_ANY(state);

    CHECK_AGG_CONTEXT("lrtm_count_distinct_deserial", fcinfo);

    Assert(len > 0);
    Assert((len - offsetof(element_set_t, data)) > 0);

    /* copy the header */
    memcpy(eset, ptr, offsetof(element_set_t, data));
    ptr += offsetof(element_set_t, data);

    Assert((eset->nall > 0) && (eset->nall == eset->nsorted));
    Assert(len == offsetof(element_set_t, data) + eset->nall * eset->item_size);

    /* we only allocate the necessary space */
    eset->data = palloc(eset->nall * eset->item_size);
    eset->nbytes = eset->nall * eset->item_size;

    memcpy((void *)eset->data, ptr, eset->nall * eset->item_size);

    PG_RETURN_POINTER(eset);
}

Datum
lrtm_count_distinct_combine(PG_FUNCTION_ARGS)
{
    int i;
    char *data, *tmp, *ptr1, *ptr2, *prev;
    element_set_t *eset1;
    element_set_t *eset2;
    MemoryContext agg_context;
    MemoryContext old_context;

    GET_AGG_CONTEXT("lrtm_count_distinct_combine", fcinfo, agg_context);

    eset1 = PG_ARGISNULL(0) ? NULL : (element_set_t *) PG_GETARG_POINTER(0);
    eset2 = PG_ARGISNULL(1) ? NULL : (element_set_t *) PG_GETARG_POINTER(1);

    if (eset2 == NULL)
        PG_RETURN_POINTER(eset1);

    if (eset1 == NULL)
    {
        old_context = MemoryContextSwitchTo(agg_context);

        eset1 = (element_set_t *)palloc(sizeof(element_set_t));
        eset1->item_size = eset2->item_size;
        eset1->nsorted = eset2->nsorted;
        eset1->nall = eset2->nall;
        eset1->nbytes = eset2->nbytes;

        eset1->data = palloc(eset1->nbytes);

        memcpy(eset1->data, eset2->data, eset1->nbytes);

        MemoryContextSwitchTo(old_context);

        PG_RETURN_POINTER(eset1);
    }

    Assert((eset1 != NULL) && (eset2 != NULL));
    Assert((eset1->item_size > 0) && (eset1->item_size == eset2->item_size));

    /* make sure both states are sorted */
    compact_set(eset1, false);
    compact_set(eset2, false);

    data = MemoryContextAlloc(agg_context, (eset1->nbytes + eset2->nbytes));
    tmp = data;

    /* merge the two arrays */
    ptr1 = eset1->data;
    ptr2 = eset2->data;
    prev = NULL;

    for (i = 0; i < eset1->nall + eset2->nall; i++)
    {
        char *element;

        Assert(ptr1 <= (eset1->data + eset1->nbytes));
        Assert(ptr2 <= (eset2->data + eset2->nbytes));

        if ((ptr1 < (eset1->data + eset1->nbytes)) &&
            (ptr2 < (eset2->data + eset2->nbytes)))
        {
            if (memcmp(ptr1, ptr2, eset1->item_size) <= 0)
            {
                element = ptr1;
                ptr1 += eset1->item_size;
            }
            else
            {
                element = ptr2;
                ptr2 += eset1->item_size;
            }
        }
        else if (ptr1 < (eset1->data + eset1->nbytes))
        {
            element = ptr1;
            ptr1 += eset1->item_size;
        }
        else if (ptr2 < (eset2->data + eset2->nbytes))
        {
            element = ptr2;
            ptr2 += eset2->item_size;
        }
        else
            elog(ERROR, "unexpected");
		if (tmp == data)
        {
            /* first value, so just copy */
            memcpy(tmp, element, eset1->item_size);
            prev = tmp;
            tmp += eset1->item_size;
        }
        else if (memcmp(prev, element, eset1->item_size) != 0)
        {
            /* not equal to the last one, so should be greater */
            Assert(memcmp(prev, element, eset1->item_size) < 0);

            /* first value, so just copy */
            memcpy(tmp, element, eset1->item_size);
            prev = tmp;
            tmp += eset1->item_size;
        }
    }

    /* we must have processed the input arrays completely */
    Assert(ptr1 == (eset1->data + (eset1->nall * eset1->item_size)));
    Assert(ptr2 == (eset2->data + (eset2->nall * eset2->item_size)));

    /* we might have eliminated some duplicate elements */
    Assert((tmp - data) <= ((eset1->nall + eset2->nall) * eset1->item_size));

    pfree(eset1->data);
    eset1->data = data;

    /* and finally compute the current number of elements */
    eset1->nbytes = tmp - data;
    eset1->nall = eset1->nbytes / eset1->item_size;
    eset1->nsorted = eset1->nall;

    PG_RETURN_POINTER(eset1);
}

Datum
lrtm_count_distinct(PG_FUNCTION_ARGS)
{
    element_set_t * eset;

    CHECK_AGG_CONTEXT("lrtm_count_distinct", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    eset = (element_set_t *)PG_GETARG_POINTER(0);

    /* do the compaction */
    compact_set(eset, false);

#if DEBUG_PROFILE
    print_set_stats(eset);
#endif

    PG_RETURN_INT64(eset->nall);
}

Datum
array_agg_distinct_type_by_element(PG_FUNCTION_ARGS)
{
    /* get element type for the dummy second parameter (anynonarray item) */
    Oid element_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

    CHECK_AGG_CONTEXT("lrtm_count_distinct", fcinfo);

    /* return empty array if the state was not initialized */
    if (PG_ARGISNULL(0))
        PG_RETURN_DATUM(PointerGetDatum(construct_empty_array(element_type)));

    return build_array((element_set_t *)PG_GETARG_POINTER(0), element_type);
}

Datum
array_agg_distinct_type_by_array(PG_FUNCTION_ARGS)
{
    /* get element type for the dummy second parameter (anyarray item) */
    Oid input_type = get_fn_expr_argtype(fcinfo->flinfo, 1),
        element_type = get_element_type(input_type);

    CHECK_AGG_CONTEXT("lrtm_count_distinct", fcinfo);

    /* return empty array if the state was not initialized */
    if (PG_ARGISNULL(0))
        PG_RETURN_DATUM(PointerGetDatum(construct_empty_array(element_type)));

    return build_array((element_set_t *)PG_GETARG_POINTER(0), element_type);
}

static Datum
build_array(element_set_t * eset, Oid element_type)
{
    Datum * array_of_datums;
    int i;

    int16       typlen;
    bool        typbyval;
    char        typalign;

    /* do the compaction */
    compact_set(eset, false);

#if DEBUG_PROFILE
    print_set_stats(eset);
#endif
    
    /* get detailed type information on the element type */
    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

    /* Copy data from compact array to array of Datums
     * A bit suboptimal way, spends excessive memory
     */
    array_of_datums = palloc0(eset->nsorted * sizeof(Datum));
    for (i = 0; i < eset->nsorted; i++)
        memcpy(array_of_datums + i, eset->data + (eset->item_size * i), eset->item_size);

    /* build and return the array */
    PG_RETURN_DATUM(PointerGetDatum(construct_array(
        array_of_datums, eset->nsorted, element_type, typlen, typbyval, typalign
    )));
}
static void
compact_set(element_set_t * eset, bool need_space)
{
    char   *base = eset->data + (eset->nsorted * eset->item_size);
    char   *last = base;
    char   *curr;
    int        i;
    int        cnt = 1;
    double    free_fract;

    Assert(eset->nall > 0);
    Assert(eset->data != NULL);
    Assert(eset->nsorted <= eset->nall);
    Assert(eset->nall * eset->item_size <= eset->nbytes);

    /* if there are no new (unsorted) items, we don't need to sort */
    if (eset->nall > eset->nsorted)
    {
        qsort_arg(eset->data + eset->nsorted * eset->item_size,
                  eset->nall - eset->nsorted, eset->item_size,
                  compare_items, &eset->item_size);
        for (i = 1; i < eset->nall - eset->nsorted; i++)
        {
            curr = base + (i * eset->item_size);

            /* items differ (keep the item) */
            if (memcmp(last, curr, eset->item_size) != 0)
            {
                last += eset->item_size;
                cnt  += 1;

                /* only copy if really needed */
                if (last != curr)
                    memcpy(last, curr, eset->item_size);
            }
        }

        /* duplicities removed -> update the number of items in this part */
        eset->nall = eset->nsorted + cnt;
        if (eset->nsorted == 0)
            eset->nsorted = eset->nall;
        if (eset->nsorted < eset->nall)
        {
            MemoryContext oldctx = MemoryContextSwitchTo(eset->aggctx);
            char * data = palloc(eset->nbytes);
            char * ptr = data;
            char * a = eset->data;
            char * a_max = eset->data + eset->nsorted * eset->item_size;
            char * b = eset->data + (eset->nsorted * eset->item_size);
            char * b_max = eset->data + eset->nall * eset->item_size;

            MemoryContextSwitchTo(oldctx);

            while (true)
            {
                int r = memcmp(a, b, eset->item_size);
                if (r == 0)
                {
                    memcpy(ptr, a, eset->item_size);
                    a += eset->item_size;
                    b += eset->item_size;
                }
                else if (r < 0)
                {
                    memcpy(ptr, a, eset->item_size);
                    a += eset->item_size;
                }
                else
                {
                    memcpy(ptr, b, eset->item_size);
                    b += eset->item_size;
                }

                ptr += eset->item_size;
                if ((a == a_max) || (b == b_max))
                {
                    if (a != a_max)        
                    {
                        memcpy(ptr, a, a_max - a);
                        ptr += (a_max - a);
                    }
                    else if (b != b_max)    
                    {
                        memcpy(ptr, b, b_max - b);
                        ptr += (b_max - b);
                    }

                    break;
                }
            }

            Assert((ptr - data) <= (eset->nall * eset->item_size));

            eset->nsorted = (ptr - data) / eset->item_size;
            eset->nall = eset->nsorted;
            pfree(eset->data);
            eset->data = data;
        }
    }

    Assert(eset->nall == eset->nsorted);

    free_fract
        = (eset->nbytes - eset->nall * eset->item_size) * 1.0 / eset->nbytes;

    if (need_space && (free_fract < ARRAY_FREE_FRACT))
    {
        if ((eset->nbytes / 0.8) < ALLOCSET_SEPARATE_THRESHOLD)
            eset->nbytes *= 2;
        else
            eset->nbytes /= 0.8;

        eset->data = repalloc(eset->data, eset->nbytes);
    }
}

static void
add_element(element_set_t * eset, char * value)
{
    if (eset->item_size * (eset->nall + 1) > eset->nbytes)
        compact_set(eset, true);
    Assert(eset->nbytes >= eset->item_size * (eset->nall + 1));

    memcpy(eset->data + (eset->item_size * eset->nall), value, eset->item_size);
    eset->nall += 1;
}
static element_set_t *
init_set(int item_size, char typalign, MemoryContext ctx)
{
    element_set_t * eset = (element_set_t *)palloc(sizeof(element_set_t));

    eset->item_size = item_size;
    eset->typalign = typalign;
    eset->nsorted = 0;
    eset->nall = 0;
    eset->nbytes = ARRAY_INIT_SIZE;
    eset->aggctx = ctx;

    eset->data = palloc(eset->nbytes);

    return eset;
}

#if DEBUG_PROFILE
static void
print_set_stats(element_set_t * eset)
{
    elog(WARNING, "bytes=%d item=%d all=%d sorted=%d",
                  eset->nbytes, eset->item_size, eset->nall, eset->nsorted);
}
#endif
static int
compare_items(const void * a, const void * b, void * size)
{
    return memcmp(a, b, *(int*)size);
}
