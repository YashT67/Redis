// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h> // To check if is Not a Number / isnan()
// system
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <signal.h>
// C++
#include <string>
#include <vector>
using namespace std;
// Proj
#include "hashtable.h"
#include "zset.h"
#include "common.h"

#define container_of(ptr, T, member) \
    ((T *)((char *)ptr - offsetof(T, member)))

typedef vector<uint8_t> Buffer;

static void msg(const char *message) { fprintf(stderr, "%s\n", message); }

static void msg_errno(const char *message) { fprintf(stderr, "[errno: %d] %s", errno, message); }

static void die(const char *msg)
{
    fprintf(stderr, "[%d] %s", errno, msg);
    abort();
}

static void fd_set_nb(int fd)
{
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0); // Get flags
    if (errno)
    {
        die("fcntl error");
    }

    flags |= O_NONBLOCK;
    (void)fcntl(fd, F_SETFL, flags); // Set flags
    if (errno)
    {
        die("fcntl error");
    }
}

const size_t max_msg = 32 << 20;

struct Conn
{
    int fd = -1;

    // Application's intention for the event loop
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;

    // Buffered input and output
    Buffer incoming;
    Buffer outgoing;
};

// Append to the back
static void buf_append(Buffer &buf, const uint8_t *data, size_t len)
{
    buf.insert(buf.end(), data, data + len);
}

// Remove from the front
static void buf_consume(Buffer &buf, size_t n)
{
    buf.erase(buf.begin(), buf.begin() + n);
}

// Application callback when the listening sockte is ready
static Conn *handle_accept(int fd)
{
    // Accept
    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
    if (connfd < 0)
    {
        msg_errno("accept error");
        return NULL;
    }

    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "New client from: %u.%u.%u.%u.%u\n",
            ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, (ip >> 24), ntohs(client_addr.sin_port));
    fd_set_nb(connfd); // Set new connection to non blocking mode

    // Create a struct Conn
    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    return conn;
}

const size_t max_args = 128 * 1024;

// Read number/len of strings
static bool read_u32(const uint8_t *&curr, const uint8_t *end, uint32_t &out)
{
    if (curr + 4 > end)
        return false;
    memcpy(&out, curr, 4);
    curr += 4;
    return true;
}

// Read string
static bool read_str(const uint8_t *&curr, const uint8_t *end, size_t n, string &out)
{
    if (curr + n > end)
        return false;
    out.assign(curr, curr + n);
    curr += n;
    return true;
}

// +------+-----+------+-----+------+-----+-----+------+
// | nstr | len | str1 | len | str2 | ... | len | strn |
// +------+-----+------+-----+------+-----+-----+------+

static int32_t parse_req(const uint8_t *data, size_t n, vector<string> &out)
{
    const uint8_t *end = data + n;
    uint32_t nstr;
    // Get number of requests
    if (!read_u32(data, end, nstr))
        return -1;
    if (nstr > max_args)
        return -1;

    // Add the requests
    while (out.size() < nstr)
    {
        uint32_t len;
        if (!read_u32(data, end, len))
            return -1;
        out.push_back(string());
        if (!read_str(data, end, len, out.back()))
            return -1;
    }
    if (data != end)
        return -1; // Trailing garbage
    return 0;
}

// Data types for serialized data
enum
{
    TAG_NIL = 0, // NIL
    TAG_STR = 1, // String
    TAG_INT = 2, // Int64
    TAG_DBL = 3, // Double
    TAG_ARR = 4, // Array
    TAG_ERR = 5, // Error + msg
};

// Error code for TAG_ERR
enum
{
    ERR_UNKNOWN = 1, // Unknown error
    ERR_TOO_BIG = 2, // Response too big
    ERR_BAD_TYP = 3, // Unexpexted value type
    ERR_BAD_ARG = 4, // Bad arguments
};

// Helper functions for the serialization
static void buf_append_u8(Buffer &buf, uint8_t data) { buf.push_back(data); }
static void buf_append_u32(Buffer &buf, uint32_t data) { buf_append(buf, (const uint8_t *)&data, 4); }
static void buf_append_i64(Buffer &buf, int64_t data) { buf_append(buf, (const uint8_t *)&data, 8); }
static void buf_append_dbl(Buffer &buf, double data) { buf_append(buf, (const uint8_t *)&data, 8); }

// Append serialized data to the back
static void out_nil(Buffer &buf)
{
    buf_append_u8(buf, TAG_NIL);
}
static void out_str(Buffer &out, const char *s, size_t size)
{
    buf_append_u8(out, TAG_STR);
    buf_append_u32(out, (uint32_t)size);
    buf_append(out, (const uint8_t *)s, size);
}
static void out_int(Buffer &out, int64_t val)
{
    buf_append_u8(out, TAG_INT);
    buf_append_i64(out, val);
}
static void out_dbl(Buffer &out, double val)
{
    buf_append_u8(out, TAG_DBL);
    buf_append_dbl(out, val);
}
static void out_arr(Buffer &out, uint32_t n)
{
    buf_append_u8(out, TAG_ARR);
    buf_append_u32(out, n);
}
static void out_err(Buffer &out, uint32_t code, const string &msg)
{
    buf_append_u8(out, TAG_ERR);
    buf_append_u8(out, code);
    buf_append_u32(out, (uint32_t)msg.size());
    buf_append(out, (const uint8_t *)msg.data(), msg.size());
}
static size_t out_begin_arr(Buffer &out)
{
    out.push_back(TAG_ARR);
    buf_append_u32(out, 32); // Size of array
    return out.size() - 4;   // The 'ctx' arg
}
static void out_end_arr(Buffer &out, size_t ctx, uint32_t n)
{
    assert(out[ctx - 1] == TAG_ARR);
    memcpy(&out[ctx], &n, 4);
}

// Global states
static struct
{
    HMap db; // Top-level hashtable
} g_data;

// Value types
enum
{
    T_INIT = 0,
    T_STR = 1,  // String
    T_ZSET = 2, // Sorted set
};

// KV pair for hashtable
struct Entry
{
    struct HNode node;
    string key;
    uint32_t type = 0;
    string str;
    ZSet zset;
};

static Entry *entry_new(uint32_t type)
{
    Entry *ent = new Entry();
    ent->type = type;
    return ent;
}

static void entry_del(Entry *ent)
{
    if (ent->type == T_ZSET)
        zset_clear(&ent->zset);
    delete ent;
}

// Helper struct for lookup
struct LookupKey
{
    struct HNode node;
    string key;
};

// Equality comparison for struct Entry
static bool entry_eq(HNode *lhs, HNode *rhs)
{
    struct Entry *le = container_of(lhs, struct Entry, node);
    struct LookupKey *re = container_of(rhs, struct LookupKey, node);
    return le->key == re->key;
}

// FNV hash
static uint64_t str_hash(const uint8_t *data, size_t len)
{
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++)
    {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}

static void do_get(vector<string> &cmd, Buffer &out)
{
    LookupKey dummy; // A dummy entry just for lookup
    dummy.key.swap(cmd[1]);
    dummy.node.hcode = str_hash((uint8_t *)dummy.key.data(), dummy.key.size());

    // Hashtable lookup
    HNode *node = hm_lookup(&g_data.db, &dummy.node, &entry_eq);
    if (!node)
        return out_nil(out);

    // Copy the value
    Entry *ent = container_of(node, Entry, node);
    if (ent->type != T_STR)
        return out_err(out, ERR_BAD_TYP, "not a string value\n");
    return out_str(out, ent->str.data(), ent->str.size());
}

static void do_set(vector<string> &cmd, Buffer &out)
{
    LookupKey dummy; // A dummy entry just for lookup
    dummy.key.swap(cmd[1]);
    dummy.node.hcode = str_hash((uint8_t *)dummy.key.data(), dummy.key.size());

    // Hashtable lookup
    HNode *node = hm_lookup(&g_data.db, &dummy.node, &entry_eq);
    if (node) // If found then update the value
    {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != T_STR)
            return out_err(out, ERR_BAD_TYP, "not a string value\n");
        ent->str.swap(cmd[2]);
    }
    else // Not found then allocate and insert
    {
        Entry *ent = entry_new(T_STR);
        ent->key.swap(dummy.key);
        ent->node.hcode = dummy.node.hcode;
        ent->str.swap(cmd[2]);
        hm_insert(&g_data.db, &ent->node);
    }
    return out_nil(out);
}

static void do_del(vector<string> &cmd, Buffer &out)
{
    LookupKey dummy; // A dummy entry just for lookup & delete
    dummy.key.swap(cmd[1]);
    dummy.node.hcode = str_hash((uint8_t *)dummy.key.data(), dummy.key.size());

    // Hashtable delete
    HNode *node = hm_delete(&g_data.db, &dummy.node, &entry_eq);
    if (node) // If found then deallocate memory
        entry_del(container_of(node, Entry, node));
    return out_int(out, node ? 1 : 0);
}

// Appends the callback key to the buffer
static bool cb_keys(HNode *node, void *arg)
{
    Buffer &out = *(Buffer *)arg;
    const string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

static void do_keys(Buffer &out)
{
    out_arr(out, hm_size(&g_data.db));
    hm_foreach(&g_data.db, &cb_keys, (void *)&out);
}

// Helper functions to convert from string to double or int64 and return when properly converted
static bool str2dbl(const string &s, double &out)
{
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}
static bool str2int(const string &s, int64_t &out)
{
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

// Add the tuple (score, name) to the Zset==zset
static void do_zadd(vector<string> &cmd, Buffer &out)
{
    double score = 0;
    if (!str2dbl(cmd[2], score))
        return out_err(out, ERR_BAD_TYP, "expect float");

    // Lookup or create a new zset
    LookupKey dummy;
    dummy.key.swap(cmd[1]);
    dummy.node.hcode = str_hash((uint8_t *)dummy.key.data(), dummy.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &dummy.node, &entry_eq);

    Entry *ent = NULL;
    if (!hnode)
    {
        ent = entry_new(T_ZSET);
        ent->key.swap(dummy.key);
        ent->node.hcode = dummy.node.hcode;
        hm_insert(&g_data.db, &ent->node);
    }
    else
    {
        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET)
            return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    // Add or update the tuple
    const string &name = cmd[3];
    bool added = zset_insert(&ent->zset, score, name.data(), name.size());
    return out_int(out, (int64_t)added);
}

static const ZSet empty_zset;

static ZSet *expect_zset(string &s)
{
    LookupKey dummy;
    dummy.key.swap(s);
    dummy.node.hcode = str_hash((uint8_t *)dummy.key.data(), dummy.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &dummy.node, &entry_eq);
    if (!hnode)
        return (ZSet *)&empty_zset;
    Entry *ent = container_of(hnode, Entry, node);
    return ent->type == T_ZSET ? &ent->zset : NULL;
}

// Remove the tuple with Name==name in Zset==zset
static void do_zrem(vector<string> &cmd, Buffer &out)
{
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
        return out_err(out, ERR_BAD_ARG, "expect zset");

    const string &name = cmd[2];
    ZNode *znode = zset_lookup(zset, name.data(), name.size());
    if (znode)
        zset_delete(zset, znode);
    return out_int(out, znode ? 1 : 0);
}

// Output the score with Name==name in ZSet==zset
static void do_zscore(vector<string> &cmd, Buffer &out)
{
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
        return out_err(out, ERR_BAD_ARG, "expect zset");

    const string &name = cmd[2];
    ZNode *znode = zset_lookup(zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// Output the tuples >= (score, name) + offset till limit in Zset==zset
static void do_zquery(vector<string> &cmd, Buffer &out)
{
    // Parse args
    double score = 0;
    if (!str2dbl(cmd[2], score))
        return out_err(out, ERR_BAD_ARG, "expect float as score");
    const string &name = cmd[3];
    int64_t offset = 0, limit = 0;
    if (!str2int(cmd[4], offset) || !str2int(cmd[5], limit))
        return out_err(out, ERR_BAD_ARG, "expect int as offset and limit");

    // Get the zset
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
        return out_err(out, ERR_BAD_TYP, "expect zset");

    // Seek to the key
    if (limit <= 0)
        return out_arr(out, 0);
    ZNode *znode = zset_seekge(zset, score, name.data(), name.size());
    znode = znode_offset(znode, offset);

    // Output
    size_t ctx = out_begin_arr(out);
    uint32_t n = 0;
    while (znode && n < limit)
    {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = znode_offset(znode, +1);
        n++;
    }
    return out_end_arr(out, ctx, n);
}

static void do_request(vector<string> &cmd, Buffer &out)
{
    if (cmd.size() == 2 && cmd[0] == "get") // get value
        return do_get(cmd, out);
    else if (cmd.size() == 3 && cmd[0] == "set") // set value
        return do_set(cmd, out);
    else if (cmd.size() == 2 && cmd[0] == "del") // del value
        return do_del(cmd, out);
    else if (cmd.size() == 1 && cmd[0] == "keys") // keys
        return do_keys(out);
    else if (cmd.size() == 4 && cmd[0] == "zadd") // zadd zset score name
        return do_zadd(cmd, out);
    else if (cmd.size() == 3 && cmd[0] == "zrem") // zrem zset name
        return do_zrem(cmd, out);
    else if (cmd.size() == 3 && cmd[0] == "zscore") // zscore zset name
        return do_zscore(cmd, out);
    else if (cmd.size() == 6 && cmd[0] == "zquery") // zquery zset score name offset limit
        return do_zquery(cmd, out);
    else // Unrecognised command
        return out_err(out, ERR_UNKNOWN, "unknown command.");
}

// Check size of response and append it in the buffer
static void response_begin(Buffer &out, size_t *header)
{
    *header = out.size();   // Message header position
    buf_append_u32(out, 0); // Reserve space
}
static uint32_t response_size(Buffer &out, size_t header) { return out.size() - header - 4; }
static void response_end(Buffer &out, size_t header)
{
    size_t msg_size = response_size(out, header);
    if (msg_size > max_msg)
    {
        out.resize(header + 4);
        out_err(out, ERR_TOO_BIG, "response is too big.");
        msg_size = response_size(out, header);
    }
    uint32_t len = (uint32_t)msg_size;
    memcpy(&out[header], &len, 4);
}

// Process 1 request if there is enough data
static bool try_one_request(Conn *conn)
{
    if (conn->incoming.size() < 4)
        return false; // Want read
    uint32_t len;

    memcpy(&len, conn->incoming.data(), 4);
    if (len > max_msg) // Message too long
    {
        msg("message too long");
        conn->want_close = true;
        return false;
    }
    if (conn->incoming.size() < 4 + len)
        return false; // Want read
    const uint8_t *request = &conn->incoming[4];

    // Do some application logic
    vector<string> cmd;
    if (parse_req(request, len, cmd) < 0)
    {
        msg("bad request");
        conn->want_close = true;
        return false;
    }

    // Do request and generate response
    size_t header_pos = 0;
    response_begin(conn->outgoing, &header_pos);
    do_request(cmd, conn->outgoing);
    response_end(conn->outgoing, header_pos);

    buf_consume(conn->incoming, 4 + len); // Remove the message
    return true;
}

// Application callback when the socket is writable
static void handle_write(Conn *conn)
{
    assert(conn->outgoing.size() > 0);
    int rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN)
        return;      // Not ready
    else if (rv < 0) // Error handling
    {
        msg_errno("write error");
        conn->want_close = true;
        return;
    }

    buf_consume(conn->outgoing, (size_t)rv); // Remove written data
    // Update readiness intention
    if (conn->outgoing.size() == 0) // All data written
    {
        conn->want_read = true;
        conn->want_write = false;
    } // else write again
}

// Application callback when the socket is readable
static void handle_read(Conn *conn)
{
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN) // Actually not read
        return;
    if (rv <= 0)
    {
        if (rv < 0) // Handle IO error
            msg_errno("read error");
        else if (conn->incoming.size() == 0) // Handle EOF
            msg("Client closed");
        else
            msg("Unexpected EOF");
        conn->want_close = true;
        return;
    }

    buf_append(conn->incoming, buf, (size_t)rv); // Input the new data
    while (try_one_request(conn)) // Parse request and generate response
    {
    }

    // Update the readiness condition
    if (conn->outgoing.size() > 0) // Has a response
    {
        conn->want_read = false;
        conn->want_write = true;
        handle_write(conn);
    } // else read again
}

int main()
{
    // Ignore SIGPIPE so that writing to a closed socket doesn't crash the server
    signal(SIGPIPE, SIG_IGN);

    // The listening socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        die("socket error");
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // Bind
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(0);
    int rv = bind(fd, (const sockaddr *)&addr, sizeof(addr));
    if (rv)
        die("bind error");

    fd_set_nb(fd);              // Set socket to non blocking mode
    rv = listen(fd, SOMAXCONN); // Listen
    if (rv < 0)
        die("listen error");

    vector<Conn *> fd2conn; // Map of all connections, keyed by fd
    vector<struct pollfd> poll_args;

    // The event loop
    while (true)
    {
        poll_args.clear(); // Prepare the arguments of poll
        // Put the listening socket in 1st pos
        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);
        // Rest are connection sockets
        for (Conn *conn : fd2conn)
        {
            if (!conn)
                continue;
            struct pollfd pfd = {conn->fd, POLLERR, 0}; // Always poll for error
            if (conn->want_read)
                pfd.events |= POLLIN;
            if (conn->want_write)
                pfd.events |= POLLOUT;
            poll_args.push_back(pfd);
        }

        // Wait for readiness
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), -1);
        if (rv < 0 && errno == EINTR)
            continue;
        else if (rv < 0)
            die("poll error");

        // Handle the listening socket
        if (poll_args[0].revents)
        {
            if (Conn *conn = handle_accept(fd))
            {
                if (fd2conn.size() <= (size_t)conn->fd)
                    fd2conn.resize(conn->fd + 1);
                assert(!fd2conn[conn->fd]);
                fd2conn[conn->fd] = conn;
            }
        }

        // Handle connection sockets
        for (int i = 1; i < poll_args.size(); ++i) // Note: skip the 1st
        {
            uint32_t ready = poll_args[i].revents;
            if (!ready)
                continue;

            Conn *conn = fd2conn[poll_args[i].fd];
            if (ready & POLLIN)
            {
                assert(conn->want_read);
                handle_read(conn);
            }
            if (ready & POLLOUT)
            {
                assert(conn->want_write);
                handle_write(conn);
            }
            if ((ready & POLLERR) || conn->want_close)
            {
                (void)close(conn->fd);
                fd2conn[conn->fd] = NULL;
                delete conn;
            }
        }
    } // The event loop
    return 0;
}