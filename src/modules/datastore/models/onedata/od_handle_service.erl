%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model serves as cache for od_handle_service records
%%% synchronized via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(od_handle_service).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

-type id() :: binary().
-type record() :: #od_handle_service{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type name() :: binary().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([name/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%% API
-export([save_to_cache/1, get_from_cache/1, invalidate_cache/1, list/0]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0]).
-export([get_record_struct/1, upgrade_record/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec save_to_cache(doc()) -> {ok, id()} | {error, term()}.
save_to_cache(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).


-spec get_from_cache(id()) -> {ok, doc()} | {error, term()}.
get_from_cache(Key) ->
    datastore_model:get(?CTX, Key).


-spec invalidate_cache(id()) -> ok | {error, term()}.
invalidate_cache(Key) ->
    datastore_model:delete(?CTX, Key).


-spec list() -> {ok, [id()]} | {error, term()}.
list() ->
    datastore_model:fold_keys(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, string},
        {proxy_endpoint, string},
        {service_properties, [term]},

        {users, [{string, [atom]}]},
        {groups, [{string, [atom]}]},

        {eff_users, [{string, [atom]}]},
        {eff_groups, [{string, [atom]}]},

        {revision_history, [term]}
    ]};
get_record_struct(2) ->
    {record, [
        {name, string},

        {eff_users, #{string => [atom]}},
        {eff_groups, #{string => [atom]}},

        {cache_state, #{atom => term}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, HandleService) ->
    {
        od_handle_service,
        Name,
        _ProxyEndpoint,
        _ServiceProperties,

        _Users,
        _Groups,

        _EffUsers,
        _EffGroups,

        _RevisionHistory
    } = HandleService,
    {2, #od_handle_service{
        name = Name,

        eff_users = #{},
        eff_groups = #{},

        cache_state = #{}
    }}.
