#include "CouchbaseDictionarySource.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/config.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

void registerDictionarySourceCouchbase(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & /* context */) -> DictionarySourcePtr {
#if USE_COUCHBASE
        return std::make_unique<CouchbaseDictionarySource>(dict_struct, config, config_prefix + ".couchbase", sample_block);
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        throw Exception{"Dictionary source of type `couchbase` is disabled because ClickHouse was built without couchbase support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    };
    factory.registerSource("couchbase", createTableSource);
}

}


#if USE_COUCHBASE
#    include <libcouchbase/couchbase++.h>
#    include <libcouchbase/couchbase++/views.h>
#    include <libcouchbase/couchbase++/query.h>

#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <common/LocalDateTime.h>

#    include <common/logger_useful.h>

#    include "CouchbaseBlockInputStream.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_CONNECTION_STRING;
    extern const int N1QL_EXECUTION_ERROR;
}

static const UInt64 max_block_size = 8192;


CouchbaseDictionarySource::CouchbaseDictionarySource(
        const DictionaryStructure & dict_struct_,
        const std::string & scheme,
        const std::string & hosts,
        const std::string & bucket,
        const std::string & password,
        const std::string & user,
        const std::string & table,
        const Block & sample_block)
        : log(&Logger::get("CouchbaseDictionarySource"))
        , dict_struct(dict_struct_)
        , scheme(scheme)
        , hosts(hosts)
        , bucket(bucket)
        , password(password)
        , user(user)
        , table(table)
        , sample_block(sample_block)
        , query_builder{dict_struct, bucket, table, "", IdentifierQuotingStyle::Backticks}
        , load_all_query{query_builder.composeLoadAllQuery()}
        , client{std::make_unique<Couchbase::Client>(toConnectionString(scheme, hosts, bucket), password, user)}
{
    Couchbase::Status status = client->connect();
    if (!status) {
        std::stringstream ss;
        ss << "Cannot connect to Couchbase: " << status.description();
        throw Exception{ss.str(), ErrorCodes::BAD_CONNECTION_STRING};
    }
}


CouchbaseDictionarySource::CouchbaseDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const Block & sample_block)
        : CouchbaseDictionarySource(
        dict_struct_,
        config.getString(config_prefix + ".scheme", "couchbase"),
        config.getString(config_prefix + ".hosts"),
        config.getString(config_prefix + ".bucket", "default"),
        config.getString(config_prefix + ".password", ""),
        config.getString(config_prefix + ".user", ""),
        config.getString(config_prefix + ".table"),
        sample_block)
{
}


/// copy-constructor is provided in order to support cloneability
CouchbaseDictionarySource::CouchbaseDictionarySource(const CouchbaseDictionarySource & other)
        : CouchbaseDictionarySource{other.dict_struct,
                                    other.scheme,
                                    other.hosts,
                                    other.bucket,
                                    other.password,
                                    other.user,
                                    other.table,
                                    other.sample_block}
{
}


BlockInputStreamPtr CouchbaseDictionarySource::loadAll()
{
    LOG_TRACE(log, load_all_query);
    return std::make_shared<CouchbaseBlockInputStream>(*client, load_all_query, sample_block, max_block_size);
}

BlockInputStreamPtr CouchbaseDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.

    const auto query = query_builder.composeLoadIdsQuery(ids);
    return std::make_shared<CouchbaseBlockInputStream>(*client, query, sample_block, max_block_size);
}

BlockInputStreamPtr CouchbaseDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.

    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return std::make_shared<CouchbaseBlockInputStream>(*client, query, sample_block, max_block_size);
}

bool CouchbaseDictionarySource::isModified() const
{
    return true;
}

bool CouchbaseDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool CouchbaseDictionarySource::hasUpdateField() const
{
    return false;
}

DictionarySourcePtr CouchbaseDictionarySource::clone() const
{
    return std::make_unique<CouchbaseDictionarySource>(*this);
}

std::string CouchbaseDictionarySource::toString() const
{
    return "Couchbase: " + toConnectionString(scheme, hosts, bucket);
}

std::string CouchbaseDictionarySource::toConnectionString(
        const std::string & scheme,
        const std::string & hosts,
        const std::string & bucket)
{
    return scheme + "://" + hosts + "/" + bucket);
}

}

#endif
