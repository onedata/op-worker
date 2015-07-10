%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Defines common macros and records that are used within whole datastore module.
%%%      This header shall not be included directly by any erl file.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_COMMON_INTERNAL_HRL).
-define(DATASTORE_COMMON_INTERNAL_HRL, 1).

%% Levels
-define(DISK_ONLY_LEVEL, disk_only).
-define(GLOBAL_ONLY_LEVEL, global_only).
-define(LOCAL_ONLY_LEVEL, local_only).
-define(GLOBALLY_CACHED_LEVEL, globally_cached).
-define(LOCALLY_CACHED_LEVEL, locally_cached).

-define(DEFAULT_STORE_LEVEL, ?GLOBALLY_CACHED_LEVEL).

%% This record shall not be used outside datastore engine and shall not be instantiated
%% directly. Use MODEL_CONFIG macro instead.
-record(model_config, {
    name :: model_behaviour:model_type(),
    size = 0 :: non_neg_integer(),
    fields = [],
    defaults = {},
    hooks = [] :: [{model_behaviour:model_type(), model_behaviour:model_action()}],
    bucket :: datastore:bucket(),
    store_level = ?DEFAULT_STORE_LEVEL :: datastore:store_level(),
    link_store_level = ?DEFAULT_STORE_LEVEL :: datastore:store_level(),
    transactional_global_cache = true :: boolean()
}).

%% Helper macro for instantiating #model_config record.
%% Bucket           :: see #model_config.bucket
%% Hooks            :: see #model_config.hooks
%% StoreLevel       :: see #model_config.store_level (optional)
%% LinkStoreLevel   :: see #model_config.link_store_level (optional)
-define(MODEL_CONFIG(Bucket, Hooks), ?MODEL_CONFIG(Bucket, Hooks, ?DEFAULT_STORE_LEVEL, ?DEFAULT_STORE_LEVEL)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel), ?MODEL_CONFIG(Bucket, Hooks, StoreLevel, StoreLevel)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel),
    ?MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, true)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions), #model_config{name = ?MODULE,
    size = record_info(size, ?MODULE),
    fields = record_info(fields, ?MODULE),
    defaults = #?MODULE{},
    bucket = Bucket,
    hooks = Hooks,
    store_level = StoreLevel,
    link_store_level = LinkStoreLevel,
    transactional_global_cache = Transactions
}).

-endif.
