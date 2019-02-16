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
#    include <libcouchbase/couchbase++/endure.h>
#    include <libcouchbase/couchbase++/logging.h>

#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <common/LocalDateTime.h>
#    include <common/logger_useful.h>
#    include "CouchbaseBlockInputStream.h"


namespace DB
{
static const UInt64 max_block_size = 8192;


CouchbaseDictionarySource::CouchbaseDictionarySource(
        const DictionaryStructure & dict_struct_,
        const std::string & host,
        UInt16 port,
        const std::string & password,
        const std::string & user,
        const Block & sample_block)
        : log(&Logger::get("CouchbaseDictionarySource"))
        , dict_struct(dict_struct_)
        , host(host)
        , port(port)
        , password(password)
        , user(user)
        , sample_block(sample_block)
        , query_builder{dict_struct, "", "", "", IdentifierQuotingStyle::Backticks}
        , load_all_query{query_builder.composeLoadAllQuery()}
        , client{std::make_unique<Couchbase::Client>(toConnectionString(host, port), password, user)}
{
}


CouchbaseDictionarySource::CouchbaseDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const Block & sample_block)
        : CouchbaseDictionarySource(
        dict_struct_,
        config.getString(config_prefix + ".host"),
        config.getUInt(config_prefix + ".port", 0),
        config.getString(config_prefix + ".password", ""),
        config.getString(config_prefix + ".user", ""),
        sample_block)
{
}


/// copy-constructor is provided in order to support cloneability
CouchbaseDictionarySource::CouchbaseDictionarySource(const CouchbaseDictionarySource & other)
        : CouchbaseDictionarySource{other.dict_struct,
                                    other.host,
                                    other.port,
                                    other.password,
                                    other.user,
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
    return "Couchbase: " + toConnectionString(host, port);
}

std::string CouchbaseDictionarySource::toConnectionString(const std::string & host, const UInt16 port)
{
    return host + (port != 0 ? ":" + DB::toString(port) : "");
}

}

#endif
