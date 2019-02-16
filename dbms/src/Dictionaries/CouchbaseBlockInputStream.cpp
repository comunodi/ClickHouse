#include <Common/config.h>

#if USE_COUCHBASE

#include <libcouchbase/couchbase++.h>
#include <libcouchbase/couchbase++/views.h>
#include <libcouchbase/couchbase++/query.h>
#include <libcouchbase/couchbase++/endure.h>
#include <libcouchbase/couchbase++/logging.h>
#include <IO/WriteHelpers.h>
#include "CouchbaseBlockInputStream.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int N1QL_EXECUTION_ERROR;
}


CouchbaseBlockInputStream::CouchbaseBlockInputStream(
        Couchbase::Client & client,
        const std::string & query,
        const Block & sample_block,
        const size_t max_block_size)
        : client(client), max_block_size{max_block_size}
{
    auto m = Couchbase::Query::execute(client, query);
    if (!m.status()) {
        throw Exception{"Couldn't issue N1QL query with status: "
        + toString(m.status()), ErrorCodes::N1QL_EXECUTION_ERROR};
    }
    result = m.body();
    description.init(sample_block);
}

CouchbaseBlockInputStream::~CouchbaseBlockInputStream() = default;

// TODO: parse response from JSON
Block CouchbaseBlockInputStream::readImpl() {
    return Block();
}

}



#endif
