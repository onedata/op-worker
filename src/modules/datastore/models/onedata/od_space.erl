%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model serves as cache for od_space records
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

-type alias() :: binary().
-type name() :: binary().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([alias/0, name/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all,
    disc_driver => undefined
}).

%% API
-export([update_cache/3, get_from_cache/1, invalidate_cache/1, list/0, run_after/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0]).
-export([get_posthooks/0]).
-export([get_record_struct/1, upgrade_record/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec update_cache(id(), diff(), doc()) -> {ok, doc()} | {error, term()}.
update_cache(Id, Diff, Default) ->
    datastore_model:update(?CTX, Id, Diff, Default).


-spec get_from_cache(id()) -> {ok, doc()} | {error, term()}.
get_from_cache(Key) ->
    datastore_model:get(?CTX, Key).


-spec invalidate_cache(id()) -> ok | {error, term()}.
invalidate_cache(Key) ->
    datastore_model:delete(?CTX, Key).


-spec list() -> {ok, [id()]} | {error, term()}.
list() ->
    datastore_model:fold_keys(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%--------------------------------------------------------------------
%% @doc
%% Space update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(create, _, {ok, Doc}) ->
    run_after(Doc);
run_after(update, _, {ok, Doc}) ->
    run_after(Doc);
run_after(_Function, _Args, Result) ->
    Result.

-spec run_after(doc()) -> {ok, doc()}.
run_after(Doc = #document{key = SpaceId}) ->
    space_strategies:create(space_strategies:new(SpaceId)),
    ok = permissions_cache:invalidate(),
    ok = fslogic_worker:init_cannonical_paths_cache(SpaceId),
    emit_monitoring_event(Doc),
    maybe_revise_space_harvesters(Doc),
    {ok, Doc}.

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
    4.

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
    ]};
get_record_struct(3) ->
    {record, [
        {name, string},

        {direct_users, #{string => [atom]}},
        {eff_users, #{string => [atom]}},

        {direct_groups, #{string => [atom]}},
        {eff_groups, #{string => [atom]}},

        {providers, #{string => integer}},
        {shares, [string]},
        {harvesters, [string]}, % new field

        {cache_state, #{atom => term}}
    ]};
get_record_struct(4) ->
    {record, [
        {name, string},

        {direct_users, #{string => [atom]}},
        {eff_users, #{string => [atom]}},

        {direct_groups, #{string => [atom]}},
        {eff_groups, #{string => [atom]}},

        {storages, #{string => integer}}, % new field

        {providers, #{string => integer}},
        {shares, [string]},
        {harvesters, [string]},

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
    {2, {od_space,
        Name,

        #{},
        #{},

        #{},
        #{},

        #{},
        [],

        #{}
    }};
upgrade_record(2, Space) ->
    {
        od_space,
        Name,

        DirectUsers,
        EffUsers,

        DirectGroups,
        EffGroups,

        Providers,
        Shares,

        CacheState
    } = Space,
    {3, {od_space,
        Name,

        DirectUsers,
        EffUsers,

        DirectGroups,
        EffGroups,

        Providers,
        Shares,
        [],

        CacheState
    }};
upgrade_record(3, Space) ->
    {
        od_space,
        Name,

        DirectUsers,
        EffUsers,

        DirectGroups,
        EffGroups,

        Providers,
        Shares,
        Harvesters,

        CacheState
    } = Space,
    {4, #od_space{
        name = Name,

        direct_users = DirectUsers,
        eff_users = EffUsers,

        direct_groups = DirectGroups,
        eff_groups = EffGroups,

        storages = #{},
        providers = Providers,
        shares = Shares,
        harvesters = Harvesters,

        cache_state = CacheState
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
-spec emit_monitoring_event(doc()) -> {ok, id()}.
emit_monitoring_event(SpaceDoc = #document{key = SpaceId}) ->
    case space_logic:is_supported(SpaceDoc, oneprovider:get_id_or_undefined()) of
        true -> monitoring_event_emitter:emit_od_space_updated(SpaceId);
        false -> ok
    end,
    {ok, SpaceId}.

-spec maybe_revise_space_harvesters(doc()) -> ok.
maybe_revise_space_harvesters(#document{
    key = SpaceId,
    value = #od_space{harvesters = Harvesters}
}) ->
    case provider_logic:supports_space(SpaceId) of
        true ->
            spawn(fun() ->
                main_harvesting_stream:revise_space_harvesters(SpaceId, Harvesters)
            end);
        false ->
            ok
    end.
