#include <Common/config.h>

#if USE_COUCHBASE

#include <libcouchbase/couchbase++.h>
#include <libcouchbase/couchbase++/views.h>
#include <libcouchbase/couchbase++/query.h>
#include <libcouchbase/couchbase++/endure.h>
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
        const std::string & statement,
        const Block & sample_block,
        const size_t max_block_size)
        : status{}, query_command{statement}, query{client, query_command, status}, max_block_size{max_block_size}
{
    if (!query.status()) {
        std::stringstream ss;
        ss << "Couldn't issue N1QL query with status: " << query.status().description();
        throw Exception{ss.str(), ErrorCodes::N1QL_EXECUTION_ERROR};
    }
    description.init(sample_block);
}

CouchbaseBlockInputStream::~CouchbaseBlockInputStream() = default;

// TODO: parse response from JSON
Block CouchbaseBlockInputStream::readImpl() {
    return Block();
}

}



#endif
