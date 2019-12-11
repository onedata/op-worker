%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_storage records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_storage records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_logic).
-author("Michal Stanisz").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

-export([create/1, get/1]).
-export([safe_delete/1]).
-export([support_space/3]).
-export([update_space_support_size/3]).
-export([revoke_support/2]).
-export([set_qos_parameters/2, get_qos_parameters/2]).
-export([describe/1]).
-export([supports_any_space/1]).
-export([support_critical_section/2]).

-export([migrate_to_zone/0]).

% for test purpose
-export([create_in_zone/2, delete_in_zone/1, upgrade_legacy_support/2]).

-compile({no_auto_import, [get/1]}).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(storage_config:doc()) -> {ok, od_storage:id()} | errors:error().
create(StorageConfig) ->
    StorageName = storage_config:get_name(StorageConfig),
    case create_in_zone(StorageName, undefined) of
        {ok, StorageId} ->
            case storage_config:save_doc(StorageConfig#document{key = StorageId}) of
                {ok, StorageId} ->
                    ok = storage_config:on_storage_created(StorageId),
                    {ok, StorageId};
                Error ->
                    case delete_in_zone(StorageId) of
                        ok -> ok;
                        {error, _} = Error1 ->
                            ?warning("Could not revert storage creation in Onezone: ~p", [Error1])
                    end,
                    Error
            end;
        Other ->
            Other
    end.


%% @private
-spec create_in_zone(storage_config:name(), od_storage:id() | undefined) ->
    {ok, od_storage:id()} | errors:error().
create_in_zone(StorageName, StorageId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"name">> => StorageName}
    }),
    ?CREATE_RETURN_ID(?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end)).


-spec get(od_storage:id()) -> {ok, od_storage:doc()} | errors:error().
get(StorageId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves storage data shared between providers through given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get_shared_data(od_storage:id(), od_space:id()) -> {ok, od_storage:doc()} | errors:error().
get_shared_data(StorageId, SpaceId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance, scope = shared},
        subscribe = true,
        auth_hint = ?THROUGH_SPACE(SpaceId)
    }).


%%--------------------------------------------------------------------
%% @doc
%% Removes storage. Fails with an error if the storage supports
%% any space.
%% @end
%%--------------------------------------------------------------------
-spec safe_delete(od_storage:id()) -> ?ERROR_STORAGE_IN_USE | {error, term()}.
safe_delete(StorageId) ->
    support_critical_section(StorageId, fun() ->
        case supports_any_space(StorageId) of
            true ->
                ?ERROR_STORAGE_IN_USE;
            false ->
                % TODO VFS-5124 Remove from rtransfer
                delete(StorageId)
        end
    end).


%% @private
-spec delete(od_storage:id()) -> ok | {error, term()}.
delete(StorageId) ->
    case delete_in_zone(StorageId) of
        ok -> storage_config:delete(StorageId);
        Error -> Error
    end.


%% @private
-spec delete_in_zone(od_storage:id()) -> ok | errors:error().
delete_in_zone(StorageId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id()),
        % only storage not supporting any space can be deleted so no need to invalidate od_space cache
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


-spec support_space(od_storage:id(), tokens:serialized(), SupportSize :: integer()) ->
    {ok, od_space:id()} | errors:error().
support_space(StorageId, SerializedToken, SupportSize) ->
    support_critical_section(StorageId, fun() ->
        support_space_insecure(StorageId, SerializedToken, SupportSize)
    end).


%% @private
-spec support_space_insecure(od_storage:id(), tokens:serialized(), SupportSize :: integer()) ->
    {ok, od_space:id()} | errors:error().
support_space_insecure(StorageId, SerializedToken, SupportSize) ->
%% @TODO VFS-5497 This check will not be needed when multisupport is implemented (will be checked in zone)
    case check_support_token(SerializedToken) of
        {ok, SpaceId} ->
            case storage_config:is_imported_storage(StorageId) andalso supports_any_space(StorageId) of
                true -> ?ERROR_STORAGE_IN_USE;
                false ->
                    Data = #{<<"token">> => SerializedToken, <<"size">> => SupportSize},
                    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
                        operation = create,
                        gri = #gri{type = od_storage, id = StorageId, aspect = support},
                        data = Data
                    }),

                    ?CREATE_RETURN_ID(?ON_SUCCESS(Result, fun(_) ->
                        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id()),
                        gs_client_worker:invalidate_cache(od_space, SpaceId),
                        gs_client_worker:invalidate_cache(od_storage, StorageId)
                    end))
            end;
        ?ERROR_RELATION_ALREADY_EXISTS(_, _, _, _) = Error ->
            Error;
        TokenError ->
            ?ERROR_BAD_VALUE_TOKEN(<<"token">>, TokenError)
    end.


%% @private
-spec check_support_token(tokens:serialized()) -> {ok, od_space:id()} | errors:error().
check_support_token(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, #token{type = ?INVITE_TOKEN(?SUPPORT_SPACE, SpaceId)}} ->
            case provider_logic:supports_space(SpaceId) of
                true ->
                    ?ERROR_RELATION_ALREADY_EXISTS(
                        od_space, SpaceId, od_provider, oneprovider:get_id()
                    );
                false ->
                    {ok, SpaceId}
            end;
        {ok, #token{type = ReceivedType}} ->
            ?ERROR_NOT_AN_INVITE_TOKEN(?SUPPORT_SPACE, ReceivedType);
        {error, _} = Error ->
            Error
    end.


-spec update_space_support_size(od_storage:id(), od_space:id(), NewSupportSize :: integer()) ->
    ok | errors:error().
update_space_support_size(StorageId, SpaceId, NewSupportSize) ->
    OccupiedSize = space_quota:current_size(SpaceId),
    update_space_support_size(StorageId, SpaceId, NewSupportSize, OccupiedSize).


%% @private
-spec update_space_support_size(od_storage:id(), od_space:id(), NewSupportSize :: integer(),
    CurrentOccupiedSize :: non_neg_integer()) ->
    ok | errors:error().
update_space_support_size(_StorageId, _SpaceId, NewSupportSize, CurrentOccupiedSize)
    when NewSupportSize < CurrentOccupiedSize ->
    ?ERROR_BAD_VALUE_TOO_LOW(<<"size">>, CurrentOccupiedSize);

update_space_support_size(StorageId, SpaceId, NewSupportSize, _CurrentOccupiedSize) ->
    Data = #{<<"size">> => NewSupportSize},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_storage, id = StorageId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_storage, StorageId),
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


-spec revoke_support(od_storage:id(), od_space:id()) -> ok | errors:error().
revoke_support(StorageId, SpaceId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_storage, id = StorageId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        on_space_unsupported(SpaceId, StorageId),
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_storage, StorageId),
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


-spec set_qos_parameters(od_storage:id(), od_storage:qos_parameters()) -> ok | errors:error().
set_qos_parameters(StorageId, QosParameters) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"qos_parameters">> => QosParameters}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Get own storage QoS parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_parameters(od_storage:id()) -> {ok, od_storage:qos_parameters()} | errors:error().
get_qos_parameters(StorageId) ->
    case get(StorageId) of
        {ok, #document{value = #od_storage{qos_parameters = QosParameters}}} -> {ok, QosParameters};
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get QoS parameters of storage supporting given space.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_parameters(od_storage:id(), od_space:id()) ->
    {ok, od_storage:qos_parameters()} | errors:error().
get_qos_parameters(StorageId, SpaceId) ->
    case get_shared_data(StorageId, SpaceId) of
        {ok, #document{value = #od_storage{qos_parameters = QosParameters}}} -> {ok, QosParameters};
        Error -> Error
    end.


-spec get_spaces(od_storage:id()) -> {ok, [od_space:id()]} | errors:error().
get_spaces(StorageId) ->
    case get(StorageId) of
        {ok, #document{value = #od_storage{spaces = Spaces}}} ->
            {ok, Spaces};
        Error -> Error
    end.


-spec supports_any_space(od_storage:id()) -> boolean().
supports_any_space(StorageId) ->
    case get_spaces(StorageId) of
        {ok, []} -> false;
        {ok, _Spaces} -> true;
        {error, _} = Error -> Error
    end.


-spec describe(od_storage:id()) ->
    {ok, #{binary() := binary() | boolean() | undefined}} | {error, term()}.
describe(StorageId) ->
    case storage_config:describe(StorageId) of
        {ok, Map} ->
            {ok, QosParameters} = get_qos_parameters(StorageId),
            {ok, Map#{
                <<"qosParameters">> => QosParameters
            }};
        {error, _} = Error -> Error
    end.


%% @private
-spec on_space_unsupported(od_space:id(), od_storage:id()) -> ok.
on_space_unsupported(SpaceId, StorageId) ->
    autocleaning_api:disable(SpaceId),
    autocleaning_api:delete_config(SpaceId),
    file_popularity_api:disable(SpaceId),
    file_popularity_api:delete_config(SpaceId),
    storage_sync:space_unsupported(SpaceId, StorageId),
    main_harvesting_stream:space_unsupported(SpaceId).


-spec support_critical_section(od_storage:id(), fun(() -> Result :: term())) ->
    Result :: term().
support_critical_section(StorageId, Fun) ->
    critical_section:run({storage_support, StorageId}, Fun).

%%%===================================================================
%%% Upgrade from 19.02.*
%%%===================================================================

-define(ZONE_CONNECTION_RETRIES, 180).

%%--------------------------------------------------------------------
%% @doc
%% Migrates storages and spaces support data to Onezone.
%% Removes obsolete space_storage and storage documents.
%% Dedicated for upgrading Oneprovider from 19.02.* to the next major release.
%% @end
%%--------------------------------------------------------------------
-spec migrate_to_zone() -> ok.
migrate_to_zone() ->
    ?info("Checking connection to Onezone..."),
    migrate_to_zone(oneprovider:is_connected_to_oz(), ?ZONE_CONNECTION_RETRIES).

-spec migrate_to_zone(IsConnectedToZone :: boolean(), Retries :: integer()) -> ok.
migrate_to_zone(false, 0) ->
    ?critical("Could not establish connection to Onezone. Aborting upgrade procedure."),
    throw(?ERROR_NO_CONNECTION_TO_ONEZONE);
migrate_to_zone(false, Retries) ->
    ?warning("There is no connection to Onezone. Next retry in 10 seconds"),
    timer:sleep(timer:seconds(10)),
    migrate_to_zone(oneprovider:is_connected_to_oz(), Retries - 1);
migrate_to_zone(true, _) ->
    ?info("Starting storage migration procedure..."),
    {ok, StorageDocs} = storage:list(),
    lists:foreach(fun migrate_storage_docs/1, StorageDocs),

    {ok, Spaces} = provider_logic:get_spaces(),
    lists:foreach(fun migrate_space_support/1, Spaces),

    % Remove virtual storage (with id equal to that of provider) in Onezone
    % call using ?MODULE macro for mocking in tests
    case ?MODULE:delete_in_zone(oneprovider:get_id()) of
        ok -> ok;
        ?ERROR_NOT_FOUND -> ok;
        Error -> throw(Error)
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Renames old `storage` record to `storage_config` and creates
%% appropriate storages in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec migrate_storage_docs(storage_config:doc()) -> ok.
migrate_storage_docs(#document{key = StorageId, value = Storage}) ->
    #storage{
        name = Name,
        helpers = Helpers,
        readonly = Readonly,
        luma_config = LumaConfig
    } = Storage,
    StorageConfig = #storage_config{
        name = Name,
        helpers = Helpers,
        readonly = Readonly,
        luma_config = LumaConfig
    },
    case storage_config:save_doc(#document{key = StorageId, value = StorageConfig}) of
        {ok, _} -> ok;
        {error, already_exists} -> ok;
        Error -> throw(Error)
    end,
    case provider_logic:has_storage(StorageId) of
        true -> ok;
        false ->
            % call using ?MODULE macro for mocking in tests
            {ok, StorageId} = ?MODULE:create_in_zone(Name, StorageId),
            ?notice("Storage ~p created in Onezone", [StorageId])
    end,
    ok = storage:delete(StorageId).


%% @private
-spec migrate_space_support(od_space:id()) -> ok.
migrate_space_support(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, SpaceStorage} ->
            % so far space could have been supported by only one storage in given provider
            [StorageId] = space_storage:get_storage_ids(SpaceStorage),
            MiR = space_storage:get_mounted_in_root(SpaceStorage),

            case space_logic:is_supported_by_storage(SpaceId, StorageId) of
                true -> ok;
                false ->
                    % call using ?MODULE macro for mocking in tests
                    case ?MODULE:upgrade_legacy_support(StorageId, SpaceId) of
                        ok -> ?notice("Support of space ~p by storage ~p upgraded in Onezone",
                            [SpaceId, StorageId]);
                        Error1 -> throw(Error1)
                    end
            end,
            case lists:member(StorageId, MiR) of
                true -> ok = storage_config:set_imported_storage_insecure(StorageId, true);
                false -> ok
            end,
            case space_storage:delete(SpaceId) of
                ok -> ok;
                ?ERROR_NOT_FOUND -> ok;
                Error2 -> throw(Error2)
            end;
        ?ERROR_NOT_FOUND ->
            ok
    end,
    ?notice("Support of space: ~p successfully migrated", [SpaceId]).


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Upgrades legacy space support in Onezone to model with new storages.
%% Dedicated for upgrading Oneprovider from 19.02.* to the next major release.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_legacy_support(od_storage:id(), od_space:id()) -> ok | errors:error().
upgrade_legacy_support(StorageId, SpaceId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_storage, id = StorageId, aspect = {upgrade_legacy_support, SpaceId}}
    }).
