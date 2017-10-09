%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model server as cache for od_handle records
%%% synchronized via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(od_handle).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type record() :: #od_handle{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type resource_type() :: binary().
-type resource_id() :: binary().
-type public_handle() :: binary().
-type metadata() :: binary().
-type timestamp() :: calendar:datetime().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([resource_type/0, resource_id/0, public_handle/0, metadata/0,
    timestamp/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%% API
-export([save/1, get/1, delete/1, list/0]).
-export([actual_timestamp/0]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0]).
-export([get_record_struct/1, upgrade_record/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv universaltime().
%%--------------------------------------------------------------------
-spec actual_timestamp() -> timestamp().
actual_timestamp() ->
    erlang:universaltime().

%%--------------------------------------------------------------------
%% @doc
%% Saves handle.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns handle.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes handle.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
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
        {public_handle, string},
        {resource_type, string},
        {resource_id, string},
        {metadata, string},
        {timestamp, {{integer, integer, integer}, {integer, integer, integer}}},

        {handle_service, string},

        {users, [{string, [atom]}]},
        {groups, [{string, [atom]}]},

        {eff_users, [{string, [atom]}]},
        {eff_groups, [{string, [atom]}]},

        {revision_history, [term]}
    ]};
get_record_struct(2) ->
    {record, [
        {public_handle, string},
        {resource_type, string},
        {resource_id, string},
        {metadata, string},
        {timestamp, {{integer, integer, integer}, {integer, integer, integer}}},

        {handle_service, string},

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
upgrade_record(1, Handle) ->
    {
        od_handle,
        PublicHandle,
        ResourceType,
        ResourceId,
        Metadata,
        Timestamp,

        HandleServiceId,

        _Users,
        _Groups,

        _EffUsers,
        _EffGroups,

        _RevisionHistory
    } = Handle,
    {2, #od_handle{
        public_handle = PublicHandle,
        resource_type = ResourceType,
        metadata = Metadata,
        timestamp = Timestamp,

        resource_id = ResourceId,
        handle_service = HandleServiceId,

        eff_users = #{},
        eff_groups = #{},

        cache_state = #{}
    }}.
