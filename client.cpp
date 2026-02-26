#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>
using namespace std;

static void msg(const char *message) { fprintf(stderr, "%s\n", message); }

static void die(const char *msg)
{
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static int32_t read_full(int fd, char *buf, size_t n)
{
    while (n > 0)
    {
        ssize_t rv = read(fd, buf, n);
        if (rv < 0)
        {
            if (errno == EINTR)
                continue; // Interrupted by signal, read again
            return -1;    // Real error
        }
        else if (rv == 0)
            return -1; // Unexpected EOF
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t write_full(int fd, const char *buf, size_t n)
{
    while (n > 0)
    {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0)
            return -1; // Error
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

const size_t max_msg = 4096;

static uint32_t send_req(int fd, vector<string> &cmd)
{
    uint32_t len = 4;
    for (const string &s : cmd)
    {
        len += 4 + s.size();
    }
    if (len > max_msg)
        return -1;

    char wbuf[4 + max_msg]; // 4 Bytes len
    memcpy(&wbuf[0], &len, 4);
    uint32_t n = (uint32_t)cmd.size();
    memcpy(&wbuf[4], &n, 4);

    size_t curr = 8;
    for (const string &s : cmd)
    {
        uint32_t size = s.size();
        memcpy(&wbuf[curr], &size, 4);
        memcpy(&wbuf[curr + 4], s.data(), size);
        curr += 4 + s.size();
    }
    return write_full(fd, wbuf, len + 4);
}

// Tags
enum
{
    TAG_NIL = 0, // NIL
    TAG_STR = 1, // String
    TAG_INT = 2, // Int64
    TAG_DBL = 3, // Double
    TAG_ARR = 4, // Array
    TAG_ERR = 5, // Error code + msg
};

static uint32_t print_response(const uint8_t *data, size_t size)
{
    if (size < 1)
    {
        msg("bad response");
        return -1;
    }

    switch (data[0])
    {
    case TAG_NIL:
        printf("(nil)\n");
        return 1;
    case TAG_STR:
        if (size < 1 + 4)
        {
            msg("bad response");
            return -1;
        }
        {
            uint32_t len = 0;
            memcpy(&len, &data[1], 4);
            if (size < 1 + 4 + len)
            {
                msg("bad response");
                return -1;
            }
            printf("(str) %.*s\n", len, &data[1 + 4]);
            return 1 + 4 + len;
        }
    case TAG_INT:
        if (size < 1 + 8)
        {
            msg("bad response");
            return -1;
        }
        {
            int64_t val = 0;
            memcpy(&val, &data[1], 8);
            printf("(int) %ld\n", val);
            return 1 + 8;
        }
    case TAG_DBL:
        if (size < 1 + 8)
        {
            msg("bad response");
            return -1;
        }
        {
            double val = 0;
            memcpy(&val, &data[1], 8);
            printf("(dbl) %g\n", val);
            return 1 + 8;
        }
    case TAG_ARR:
        if (size < 1 + 4)
        {
            msg("bad response");
            return -1;
        }
        {
            uint32_t len = 0;
            memcpy(&len, &data[1], 4);
            printf("(arr) len = %d\n", len);
            size_t arr_bytes = 1 + 4;
            for (uint32_t i = 0; i < len; i++)
            {
                int rv = print_response(&data[arr_bytes], size - arr_bytes);
                if (rv < 0)
                    return rv;
                arr_bytes += (size_t)rv;
            }
            printf("(arr) end\n");
            return arr_bytes;
        }
    case TAG_ERR:
        if (size < 1 + 8)
        {
            msg("bad response");
            return -1;
        }
        {
            int32_t code = 0;
            uint32_t len = 0;
            memcpy(&code, &data[1], 4);
            memcpy(&len, &data[1 + 4], 4);
            if (size < 1 + 8 + len)
            {
                msg("bad response");
                return -1;
            }
            printf("(err) %d %.*s", code, len, &data[1 + 8]);
            return 1 + 8 + len;
        }
    default:
        msg("bad response");
        return -1;
    }
}

static int32_t read_res(int fd)
{
    char rbuf[4 + max_msg]; // 4 bytes for length
    errno = 0;
    int err = read_full(fd, rbuf, 4);
    if (err)
    {
        if (errno == 0)
            msg("EOF"); // Unexpected EOF
        else
            msg("read error"); // Some other read error
        return err;
    }

    uint32_t len = 0;
    memcpy(&len, &rbuf[0], 4);
    if (len > max_msg)
    {
        msg("too long");
        return -1;
    }

    // Reply body
    err = read_full(fd, &rbuf[4], len);
    if (err)
    {
        msg("read error");
        return err;
    }

    // Print the response
    int32_t rv = print_response((uint8_t *)&rbuf[4], len);
    if (rv > 0 && (uint32_t)rv != len)
    {
        msg("bad response");
        rv = -1;
    }
    return rv;
}

int main(int argc, char **argv)
{
    // Define socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        die("socket()");
    }

    // Connect
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); // 127.0.0.1
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv)
    {
        die("connect");
    }

    vector<string> cmd;
    for (int i = 1; i < argc; i++)
    {
        cmd.push_back(argv[i]);
    }

    int32_t err = send_req(fd, cmd);
    if (err)
        goto L_DONE;
    err = read_res(fd);
    if (err)
        goto L_DONE;

L_DONE:
    close(fd);
    return 0;
}