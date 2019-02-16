#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include "ExternalResultDescription.h"

namespace Couchbase
{
class Client;
}

namespace DB
{
class CouchbaseBlockInputStream final : public IBlockInputStream
{
public:
    CouchbaseBlockInputStream(
            Couchbase::Client & client,
            const std::string & query,
            const Block & sample_block,
            const size_t max_block_size);

    ~CouchbaseBlockInputStream() override;

    String getName() const override { return "Couchbase"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    Block readImpl() override;

    Couchbase::Client & client;
    Couchbase::Buffer result; // JSON response
    const size_t max_block_size;
    ExternalResultDescription description;
};

}