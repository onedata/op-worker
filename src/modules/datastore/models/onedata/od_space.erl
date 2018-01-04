%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model server as cache for od_space records
%%% synchronized via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(od_space).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type record() :: #od_space{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type login() :: binary().
-type name() :: binary().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([login/0, name/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%% API
-export([save/1, get/1, delete/1, list/0, run_after/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0]).
-export([get_posthooks/0]).
-export([get_record_struct/1, upgrade_record/2]).

%%%===================================================================
%%% API
%%%===================================================================

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

%%--------------------------------------------------------------------
%% @doc
%% Space create/update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(create, _, {ok, SpaceDoc = #document{key = SpaceId}}) ->
    space_strategies:create(space_strategies:new(SpaceId)),
    ok = permissions_cache:invalidate(),
    emit_monitoring_event(SpaceDoc);
run_after(update, [_, _, _, _], {ok, SpaceDoc = #document{key = SpaceId}}) ->
    space_strategies:create(space_strategies:new(SpaceId)),
    ok = permissions_cache:invalidate(),
    emit_monitoring_event(SpaceDoc);
run_after(save, _, {ok, SpaceDoc = #document{key = SpaceId}}) ->
    space_strategies:create(space_strategies:new(SpaceId)),
    ok = permissions_cache:invalidate(),
    emit_monitoring_event(SpaceDoc);
run_after(update, _, {ok, SpaceDoc = #document{}}) ->
    ok = permissions_cache:invalidate(),
    emit_monitoring_event(SpaceDoc);
run_after(_Function, _Args, Result) ->
    Result.

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
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun run_after/3].

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

        {providers_supports, [{string, integer}]},
        {providers, [string]},
        {users, [{string, [atom]}]},
        {groups, [{string, [atom]}]},
        {shares, [string]},

        {eff_users, [{string, [atom]}]},
        {eff_groups, [{string, [atom]}]},
        {revision_history, [term]}
    ]};
get_record_struct(2) ->
    {record, [
        {name, string},

        {direct_users, #{string => [atom]}},
        {eff_users, #{string => [atom]}},

        {direct_groups, #{string => [atom]}},
        {eff_groups, #{string => [atom]}},

        {providers, #{string => integer}},
        {shares, [string]},

        {cache_state, #{atom => term}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, Space) ->
    {
        od_space,
        Name,

        _ProviderSupports,
        _Providers,
        _Users,
        _Groups,
        _Shares,

        _EffUsers,
        _EffGroups,

        _RevisionHistory
    } = Space,
    {2, #od_space{
        name = Name,

        direct_users = #{},
        eff_users = #{},

        direct_groups = #{},
        eff_groups = #{},

        providers = #{},
        shares = [],

        cache_state = #{}
    }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends event informing about od_space update if provider supports the space.
%% @end
%%--------------------------------------------------------------------
-spec emit_monitoring_event(doc()) -> no_return().
emit_monitoring_event(SpaceDoc = #document{key = SpaceId}) ->
    case space_logic:is_supported(SpaceDoc, oneprovider:get_id(fail_with_undefined)) of
        true -> monitoring_event:emit_od_space_updated(SpaceId);
        false -> ok
    end,
    {ok, SpaceId}.

