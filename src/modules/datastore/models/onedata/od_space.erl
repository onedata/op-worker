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
-export([handle_space_name_appeared/5, handle_space_name_disappeared/3]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_posthooks/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec update_cache(id(), diff(), doc()) -> {ok, doc()} | {error, term()}.
update_cache(Id, Diff, Default) ->
    run_in_critical_section(Id, fun() ->
        PrevVal = case get_from_cache(Id) of
            {ok, #document{value = V}} -> V;
            {error, not_found} -> #od_space{}
        end,
        case datastore_model:update(?CTX, Id, Diff, Default) of
            {ok, #document{value = NewVal}} = Res ->
                handle_new_supports(Id, PrevVal, NewVal),
                handle_name_change(Id, PrevVal, NewVal),
                Res;
            {error, _} = Error ->
                Error
        end
    end).


-spec get_from_cache(id()) -> {ok, doc()} | {error, term()}.
get_from_cache(Id) ->
    datastore_model:get(?CTX, Id).


-spec invalidate_cache(id()) -> ok | {error, term()}.
invalidate_cache(Id) ->
    run_in_critical_section(Id, fun() ->
        case get_from_cache(Id) of
            {ok, #document{value = #od_space{name = Name, eff_users = UsersMap}}} ->
                apply_for_all_user_spaces(fun(ParentGuid, SpacesByName) ->
                    handle_space_name_disappeared(Name, ParentGuid, SpacesByName)
                end, maps:keys(UsersMap)),
                datastore_model:delete(?CTX, Id);
            {error, not_found} ->
                ok
        end
    end).


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

            % Guard against various races when toggling support parameters in oz % (datastore
            % posthooks can be executed in different order than datastore functions which
            % trigger them)
            ok = critical_section:run({handle_space_support_parameters_change, SpaceId}, fun() ->
                case space_logic:get(?ROOT_SESS_ID, SpaceId) of
                    {ok, CurrentDoc} ->
                        ok = handle_space_support_parameters_change(ProviderId, CurrentDoc);
                    ?ERROR_NOT_FOUND ->
                        ok
                end
            end),
    
            ok = dbsync_worker:start_streams([SpaceId])
    end,
    {ok, Doc}.


-spec handle_space_name_appeared(id(), name(), file_id:file_guid(), #{od_space:name() => [od_space:id()]},
    create | {rename, name()}) -> ok.
handle_space_name_appeared(SpaceId, Name, ParentGuid, SpacesByName, Operation) ->
    FinalNewName = case maps:get(Name, SpacesByName) of
        [SpaceId] ->
            Name;
        [_S1, _S2] = L ->
            [OtherSpaceId] = L -- [SpaceId],
            emit_renamed_event(OtherSpaceId, ParentGuid, space_logic:disambiguate_space_name(Name, OtherSpaceId), Name),
            space_logic:disambiguate_space_name(Name, SpaceId);
        _ ->
            space_logic:disambiguate_space_name(Name, SpaceId)
    end,
    case Operation of
        create -> ok;
        {rename, PrevName} -> emit_renamed_event(SpaceId, ParentGuid, FinalNewName, PrevName)
    end.


-spec handle_space_name_disappeared(name(), file_id:file_guid(), #{od_space:name() => [od_space:id()]}) -> ok.
handle_space_name_disappeared(SpaceName, ParentGuid, SpacesByName) ->
    case maps:get(SpaceName, SpacesByName, []) of
        [S] ->
            emit_renamed_event(S, ParentGuid, SpaceName, space_logic:disambiguate_space_name(SpaceName, S));
        _ ->
            ok
    end.


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
-spec run_in_critical_section(id(), fun (() -> Result)) -> Result.
run_in_critical_section(SpaceId, Fun) ->
    critical_section:run({od_space, SpaceId}, Fun).


%% @private
-spec handle_new_supports(id(), PrevVal :: record(), NewVal :: record()) -> ok.
handle_new_supports(SpaceId, #od_space{providers = PrevProviders}, #od_space{providers = NewProviders}) ->
    case maps:keys(NewProviders) -- maps:keys(PrevProviders) of
        [_] -> % emit event only on first support
            file_meta:emit_space_dir_created(fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId), SpaceId);
        _ ->
            ok
    end.


%% @private
-spec handle_name_change(id(), PrevVal :: record(), NewVal :: record()) -> ok.
handle_name_change(SpaceId, #od_space{name = undefined}, #od_space{name = Name, eff_users = UsersMap}) ->
    % first appearance of this space in provider
    apply_for_all_user_spaces(fun(ParentGuid, SpacesByName) ->
        handle_space_name_appeared(SpaceId, Name, ParentGuid, SpacesByName, create)
    end, maps:keys(UsersMap));
handle_name_change(SpaceId, #od_space{name = PrevName}, #od_space{name = NewName, eff_users = UsersMap}) when PrevName =/= NewName ->
    apply_for_all_user_spaces(fun(ParentGuid, SpacesByName) ->
        handle_space_name_disappeared(PrevName, ParentGuid, SpacesByName),
        handle_space_name_appeared(SpaceId, NewName, ParentGuid, SpacesByName, {rename, PrevName})
    end, maps:keys(UsersMap));
handle_name_change(_, _, _) ->
    ok.


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


%% @private
-spec emit_renamed_event(id(), file_id:file_guid(), name(), name()) -> ok.
emit_renamed_event(SpaceId, ParentGuid, NewName, PrevName) ->
    FileCtx = file_ctx:new_by_guid(fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)),
    fslogic_event_emitter:emit_file_renamed_no_exclude(FileCtx, ParentGuid, ParentGuid, NewName, PrevName).


%% @private
-spec apply_for_all_user_spaces(fun((file_id:file_guid(), #{od_space:name() => [od_space:id()]}) -> ok), [od_user:id()]) -> ok.
apply_for_all_user_spaces(Fun, UsersList) ->
    % Apply only to cached spaces, as spaces listed in oneclient must have been already fetched.
    {ok, Spaces} = list(),
    SpacesWithSupport = lists:filter(fun(S) ->
        case get_from_cache(S) of
            {ok, #document{value = #od_space{providers = P}}} ->
                maps:size(P) > 0;
            _ ->
                false
        end
    end, Spaces),
    maps:fold(fun(UserId, [SessId | _], _) ->
        ParentGuid = fslogic_file_id:user_root_dir_guid(UserId),
        SpacesByName = space_logic:group_spaces_by_name(SessId, SpacesWithSupport),
        Fun(ParentGuid, SpacesByName)
    end, ok, maps:with(UsersList, map_sessions_to_users())).


%% @private
-spec map_sessions_to_users() -> #{od_user:id() => [session:id()]}.
map_sessions_to_users() ->
    {ok, SessList} = session:list(),
    lists:foldl(fun
        (#document{key = SessId, value = #session{type = fuse, identity = ?SUB(user, UserId)}}, Acc) ->
            Acc#{UserId => [SessId | maps:get(UserId, Acc, [])]};
        (_, Acc) ->
            Acc
    end, #{}, SessList).
    