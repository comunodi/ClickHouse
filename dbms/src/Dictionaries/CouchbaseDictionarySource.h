#pragma once

#include <Common/config.h>

#if USE_COUCHBASE

#    include "DictionaryStructure.h"
#    include "ExternalQueryBuilder.h"
#    include "IDictionarySource.h"

namespace Couchbase
{
class Client;
}

namespace DB
{
/// Allows loading dictionaries from a Couchbase database
class CouchbaseDictionarySource final : public IDictionarySource
{
public:
    CouchbaseDictionarySource(
            const DictionaryStructure & dict_struct_,
            const std::string & scheme,
            const std::string & hosts,
            const std::string & bucket,
            const std::string & password,
            const std::string & user,
            const std::string & table,
            const Block & sample_block);

    CouchbaseDictionarySource(
            const DictionaryStructure & dict_struct_,
            const Poco::Util::AbstractConfiguration & config,
            const std::string & config_prefix,
            const Block & sample_block);

    /// copy-constructor is provided in order to support cloneability
    CouchbaseDictionarySource(const CouchbaseDictionarySource & other);

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{"Method loadUpdatedAll is unsupported for CouchbaseDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    }

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    static std::string toConnectionString(const std::string & scheme, const std::string & hosts, const std::string & bucket);

    Poco::Logger * log;

    const DictionaryStructure dict_struct;

    const std::string scheme;
    const std::string hosts;
    const std::string bucket;
    const std::string password;
    const std::string user;
    const std::string table;

    Block sample_block;

    ExternalQueryBuilder query_builder;
    const std::string load_all_query;

    std::unique_ptr<Couchbase::Client> client;
};

}

#endif
