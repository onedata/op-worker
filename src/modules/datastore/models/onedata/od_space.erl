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
-include_lib("ctool/include/errors.hrl").

-type id() :: binary().
-type record() :: #od_space{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type alias() :: binary().
-type name() :: binary().
-type support_size() :: pos_integer().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([alias/0, name/0, support_size/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all,
    disc_driver => undefined
}).

%% API
-export([update_cache/3, get_from_cache/1, invalidate_cache/1, list/0, run_after/3]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_posthooks/0]).

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
run_after(Doc = #document{key = SpaceId, value = Space = #od_space{harvesters = Harvesters}}) ->
    ProviderId = oneprovider:get_id(),

    case space_logic:is_supported(Space, ProviderId) of
        false ->
            ok;
        true ->
            ok = permissions_cache:invalidate(),
            ok = qos_bounded_cache:ensure_exists_on_all_nodes(SpaceId),
            ok = fslogic_worker:init_effective_caches(SpaceId),
            monitoring_event_emitter:emit_od_space_updated(SpaceId),
            % run asynchronously as this requires the space record, which will be cached
            % only after run_after finishes (running synchronously could cause an infinite loop)
            spawn(main_harvesting_stream, revise_space_harvesters, [SpaceId, Harvesters]),
            ok = dbsync_worker:start_streams([SpaceId]),

            % Guard against various races when toggling support parameters in oz % (datastore
            % posthooks can be executed in different order than datastore functions which
            % trigger them)
            critical_section:run({handle_space_support_parameters_change, SpaceId}, fun() ->
                case space_logic:get(?ROOT_SESS_ID, SpaceId) of
                    {ok, CurrentDoc} ->
                        ok = handle_space_support_parameters_change(ProviderId, CurrentDoc);
                    ?ERROR_NOT_FOUND ->
                        ok
                end
            end)
    end,
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
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun run_after/3].


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec handle_space_support_parameters_change(oneprovider:id(), doc()) ->
    ok | no_return().
handle_space_support_parameters_change(ProviderId, #document{key = SpaceId, value = #od_space{
    support_parameters_registry = SupportParametersRegistry
}}) ->
    SupportParameters = try
        support_parameters_registry:get_entry(ProviderId, SupportParametersRegistry)
    catch _:_ ->
        % possible race when revoking space in oz when support parameters were already
        % removed but provider is still visible as supporting this space
        #support_parameters{
            accounting_enabled = false,
            dir_stats_service_enabled = false,
            dir_stats_service_status = disabled
        }
    end,
    dir_stats_service_state:handle_space_support_parameters_change(SpaceId, SupportParameters).
