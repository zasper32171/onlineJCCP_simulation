extern "C"
{
#include "simlib.h"
}

#include <assert.h>
#include <float.h>

#define MAX_NODES          16
#define MAX_VIDEOS       1024
#define MAX_VARIANTS        4

#define EV_START            1
#define EV_ARRVL            2
#define EV_DEPRT            3
#define EV_END              4

#define STRM_ARRVL          1
#define STRM_VIDEO          2
#define STRM_VARIANT        3
#define STRM_DELAY          4
#define STRM_SHUFFLE        5

#define VAR_CACHE_HIT       1
#define VAR_DELAY           2
#define VAR_EXT_TRAFFIC     3

#define TRANSITION     300000
#define MAX_REQUESTS  1000000
#define MAX_SIM_TIME    1.E30

struct List {
    int          video;
    int          variant;
    struct List* prev;
    struct List* next;
};

typedef struct List List;

bool   trans_stat;

bool   cached       [MAX_NODES + 1][MAX_VIDEOS + 1][MAX_VARIANTS + 1];
double storage      [MAX_NODES + 1];
double workload     [MAX_NODES + 1];
int    popular      [MAX_NODES + 1][MAX_VIDEOS + 1];
List*  cache_list   [MAX_NODES + 1];
bool   neighbor     [MAX_NODES + 1][MAX_NODES + 1];
double delay        [MAX_NODES + 1][MAX_NODES + 1];

double relat_stor_cap;
double stor_capacity;
double proc_capacity;
double arrvl_rate;

double vid_len;
double bitrate      [MAX_VARIANTS + 1];
double vid_size     [MAX_VARIANTS + 1];
double lib_size;
double alpha;

int    num_node, num_vid, num_var;

int    source, transcode, target;
int    video, variant;

int    transcodable [MAX_NODES + 1];

bool   cache_hit;
double total_delay, ext_traffic;

int    num_req;

FILE*  outfile;

void run(void);
void init(void);
void deinit(void);
void start(void);
void arrive(void);
void depart(void);
void report(void);
void onlineJCCB(void);
void offlineOptimal(void);
void LRUupdate(int node);
void cachePushFront(int node);
void cacheRemoveEntry (int node);
void cachePopBack (int node);

int main()
{
    outfile = fopen("log.out", "w");

    proc_capacity = 10;

    for (relat_stor_cap = 0.00; relat_stor_cap <= 1.00 + EPSILON; relat_stor_cap += 0.05)
        run();

    printf("\n");

    relat_stor_cap = 0.2;

    for (proc_capacity = 0.0; proc_capacity <= 50.0 + EPSILON; proc_capacity += 2.5)
        run();

    fclose(outfile);

    return 0;
}

void run()
{
    init();

    event_schedule(0, EV_START);
    event_schedule(MAX_SIM_TIME, EV_END);

    do
    {
        timing();

        switch (next_event_type)
        {
            case EV_START:
                start();
                break;
            case EV_ARRVL:
                arrive();
                break;
            case EV_DEPRT:
                depart();
                break;
            case EV_END:
                report();
                break;
        }

        if (trans_stat && num_req >= TRANSITION)
        {
            trans_stat = false;
            num_req    = 0;
        }
    } while (next_event_type != EV_END);

    deinit();
}

void init()
{
    init_simlib();

    trans_stat = true;

    num_node = 3;
    num_vid  = 1000;
    num_var  = 4;

    vid_len  = 10;

    bitrate[0] = 2;
    bitrate[1] = bitrate[0] * 0.82;
    bitrate[2] = bitrate[0] * 0.76;
    bitrate[3] = bitrate[0] * 0.55;
    bitrate[4] = bitrate[0] * 0.45;

    for (int i = 1; i <= num_var; i++)
        vid_size[i] = vid_len * bitrate[i] * 60;

    lib_size = 0;

    for (int i = 1; i <= num_var; i++)
        lib_size += vid_size[i] * num_vid;

    alpha = 0.8;

    for (int i = 1; i <= num_node; i++)
    {
        for (int j = 1; j <= num_node; j++)
            neighbor[i][j] = (i == j)? 0: 1;

        for (int j = 1; j <= num_vid; j++)
            for (int k = 1; k <= num_var; k++)
                cached[i][j][k] = false;

        storage[i] = workload[i] = 0;

        for (int j = 1; j <= num_vid; j++)
            popular[i][j] = j;

        for (int j = 1; j <= num_vid; j++)
        {
            int temp = popular[i][j];
            int k = uniform_integer(j, num_vid, STRM_SHUFFLE);
            popular[i][j] = popular[i][k];
            popular[i][k] = temp;
        }

        cache_list[i] = NULL;
    }

//  relat_stor_cap  = 0.8;
    stor_capacity   = relat_stor_cap * lib_size;
//  proc_capacity   = 10;
    arrvl_rate      = 8;

    num_req  = 0;
}

void deinit()
{
    deinit_simlib();

    for (int i = 0; i < MAX_NODES; i++)
    {
        List* it, *next;

        for (it = cache_list[i]; it != NULL; it = next)
        {
            next = it->next;
            free(it);
        }
    }
}

void start()
{
    /* generate first arrivals for all nodes */

    for (int i = 1; i <= num_node; i++)
    {
        transfer[3] = i;
        transfer[4] = popular[i][zipf(alpha, num_vid, STRM_VIDEO)];
        transfer[5] = uniform_integer(1, num_var, STRM_VARIANT);
        event_schedule(sim_time + expon(1 / arrvl_rate, STRM_ARRVL), EV_ARRVL);
    }
}

void arrive()
{
    target      = transfer[3];
    video       = transfer[4];
    variant     = transfer[5];

    /* schedule next arrival */

    transfer[3] = target;
    transfer[4] = popular[target][zipf(alpha, num_vid, STRM_VIDEO)];
    transfer[5] = uniform_integer(1, num_var, STRM_VARIANT);
    event_schedule(sim_time + expon(1 / arrvl_rate, STRM_ARRVL), EV_ARRVL);

    /* determine delays upon arrival */

    for (int i = 1; i <= num_node; i++)
    {
        delay[i][i] = uniform(5, 10, STRM_DELAY);

        for (int j = 1; j < i; j++)
            delay[i][j] = delay[j][i]= (neighbor[i][j])?
                                            uniform(20, 25, STRM_DELAY): INFINITY;

        delay[i][0] = delay[0][i] = uniform(100, 200, STRM_DELAY);
    }

    /* determine source & transcode nodes */

    onlineJCCB();

    /* collect statistics */

    if (!trans_stat)
    {
//      cache_hit   = (source == target);
        cache_hit   = (source != 0);
        total_delay = delay[target][source];
        ext_traffic = (source == 0)? vid_size[variant] :0;

        sampst(cache_hit,   VAR_CACHE_HIT);
        sampst(total_delay, VAR_DELAY);
        sampst(ext_traffic, VAR_EXT_TRAFFIC);
    }

    /* update storage & workload */

    LRUupdate(target);

    if (transcode != 0)
    {
        workload[transcode] += bitrate[variant];

        assert(workload[transcode] >= 0 &&
               workload[transcode] <= proc_capacity);
    }

    /* schedule departure */

    transfer[3] = source;
    transfer[4] = transcode;
    transfer[5] = target;
    transfer[6] = video;
    transfer[7] = variant;
    event_schedule(sim_time + vid_len, EV_DEPRT);

    /* count requests, check end of simulation */

    num_req++;

    if (!trans_stat && num_req >= MAX_REQUESTS)
        event_schedule(sim_time, EV_END);
}

void depart()
{
    source    = transfer[3];
    transcode = transfer[4];
    target    = transfer[5];
    video     = transfer[6];
    variant   = transfer[7];

    /* determine delays upon departure (for offline-optimal) */

    for (int i = 1; i <= num_node; i++)
    {
        delay[i][i] = uniform(5, 10, STRM_DELAY);

        for (int j = 1; j < i; j++)
            delay[i][j] = delay[j][i]= (neighbor[target][i])?
                                            uniform(20, 25, STRM_DELAY): INFINITY;

        delay[i][0] = delay[0][i] = uniform(100, 200, STRM_DELAY);
    }

    /* resource released */

    if (transcode != 0) {
        workload[transcode] -= bitrate[variant];

        /* correct floating point error */
        if (workload[transcode] < 0 && workload[transcode] >= -EPSILON)
            workload[transcode] = 0;

        assert(workload[transcode] >= 0 &&
               workload[transcode] <= proc_capacity);
    }
}

void report()
{
    static bool print_title = true;

    double cache_hit_ratio = sampst(0.00, -VAR_CACHE_HIT);
    double avg_delay       = sampst(0.00, -VAR_DELAY);

    sampst(0.00, -VAR_EXT_TRAFFIC);
    double ext_traffic = (double)transfer[1] * transfer[2] / 1.E6;

    if (print_title)
    {
        printf(" Relat. Storage   Processing       Cache           Avg            Ext   \n");
        printf("    Capacity       Capacity      Hit Ratio        Delay         Traffic \n");
        printf("________________________________________________________________________\n\n");

        print_title = false;
    }

    printf("    %6.2f         %6.1f         %6.3f         %6.2f         %6.3f  \n",
            relat_stor_cap, proc_capacity, cache_hit_ratio, avg_delay, ext_traffic );

    fprintf(outfile, "%f %f %f %f %f\n",
            relat_stor_cap, proc_capacity, cache_hit_ratio, avg_delay, ext_traffic );
}

void onlineJCCB()
{
    /* get closet transcodables */

    for (int i = 1; i <= num_node; i++) {
        transcodable[i] = 0;

        if (!(target == i || neighbor[target][i])) continue;

        for (int j = i + 1; j <= num_var; j++)
        {
            if (cached[i][video][j])
            {
                transcodable[i] = j;
                break;
            }
        }
    }

    /* determine source & transcode nodes */

    if (cached[target][video][variant])
    {
        source    = target;
        transcode = 0;
        return;
    }

    int    candidate = 0;
    double min_load  = DBL_MAX;
    double min_delay = DBL_MAX;

    for (int i = 1; i <= num_node; i++)
    {
        if (cached[i][video][variant]
                && delay[target][i] < min_delay)
        {
            candidate = i;
            min_delay = delay[target][i];
        }
    }

    if (candidate != 0)
    {
        source    = candidate;
        transcode = 0;
        return;
    }

    if (transcodable[target] != 0 &&
             workload[target] + bitrate[variant] < proc_capacity)
    {
        source    = target;
        transcode = target;
        return;
    }

    for (int i = 1; i <= num_node; i++)
    {
        if (neighbor[target][i] && transcodable[i] != 0
                && workload[i] < min_load
                && workload[i] + bitrate[variant] < proc_capacity)
        {
            candidate = i;
            min_load  = workload[i];
        }
    }

    if (candidate != 0)
    {
        source    = candidate;
        transcode = candidate;
        return;
    }

    if (workload[target] + bitrate[variant] < proc_capacity)
    {
        for (int i = 1; i <= num_node; i++)
        {
            if (neighbor[target][i] && transcodable[i] != 0
                && delay[target][i] < min_delay)
            {
                candidate = i;
                min_delay = delay[target][i];
            }
        }

        if (candidate != 0)
        {
            source    = candidate;
            transcode = target;
            return;
        }
    }

    source    = 0;
    transcode = 0;
}

void offlineOptimal()
{

}

void LRUupdate(int node)
{
    if (cached[node][video][variant])
        cacheRemoveEntry(node);

    cachePushFront(node);

    while (storage[node] > stor_capacity)
        cachePopBack(node);

    assert(storage[node] >= 0 &&
           storage[node] <= stor_capacity);
}

void cachePushFront(int node)
{
    List* entry      = (List*) malloc(sizeof(List));
    entry->video     = video;
    entry->variant   = variant;
    entry->prev      = NULL;
    entry->next      = cache_list[node];

    if (entry->next != NULL) entry->next->prev  = entry;

    cache_list[node] = entry;

    cached[node][video][variant] = true;
    storage[node] += vid_size[variant];
}

void cacheRemoveEntry (int node)
{
    for (List* it = cache_list[node]; it != NULL; it = it->next)
    {
        if (it->video == video && it->variant == variant)
        {
            if (it == cache_list[node])
                cache_list[node] = it->next;

            if(it->prev != NULL) it->prev->next = it->next;
            if(it->next != NULL) it->next->prev = it->prev;
            free(it);

            cached[node][video][variant] = false;
            storage[node] -= vid_size[variant];

            break;
        }
    }
}

void cachePopBack (int node)
{
    if (cache_list[node] == NULL) return;

    List* tail = cache_list[node];
    while (tail->next != NULL) tail = tail->next;

    if (tail == cache_list[node])
        cache_list[node] = NULL;

    int vid = tail->video;
    int var = tail->variant;

    cached[node][vid][var] = false;
    storage[node] -= vid_size[var];

    if(tail->prev != NULL) tail->prev->next = NULL;

    free(tail);
}
