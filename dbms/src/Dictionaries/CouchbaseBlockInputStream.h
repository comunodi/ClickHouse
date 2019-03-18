#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include "ExternalResultDescription.h"

namespace Couchbase
{
class Client;
class Status;
class Query;
class QueryCommand;
}

namespace DB
{
class CouchbaseBlockInputStream final : public IBlockInputStream
{
public:
    CouchbaseBlockInputStream(
            Couchbase::Client & client,
            const std::string & statement,
            const Block & sample_block,
            const size_t max_block_size);

    ~CouchbaseBlockInputStream() override;

    String getName() const override { return "Couchbase"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    Block readImpl() override;

    Couchbase::Status status;
    Couchbase::QueryCommand query_command;
    Couchbase::Query query;
    const size_t max_block_size;
    ExternalResultDescription description;
};

}