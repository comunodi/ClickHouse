#pragma once

#include <Common/config.h>

#if USE_COUCHBASE

#    include "DictionaryStructure.h"
#    include "IDictionarySource.h"

#    include <libcouchbase/couchbase++.h>
#    include <libcouchbase/couchbase++/views.h>
#    include <libcouchbase/couchbase++/query.h>
#    include <libcouchbase/couchbase++/endure.h>
#    include <libcouchbase/couchbase++/logging.h>

namespace DB
{

}

#endif
