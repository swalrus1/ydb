LIBRARY()

SRCS(
    manager.cpp
    GLOBAL object.cpp
    checker.cpp
    GLOBAL behaviour.cpp
    GLOBAL update.cpp
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/services/metadata/secret
    ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
