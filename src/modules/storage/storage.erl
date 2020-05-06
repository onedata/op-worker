%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for all storage model related operations.
%%%
%%% This module is an overlay to `storage_config` which manages private
%%% local storage configuration and `storage_logic` that is responsible
%%% for management of storage details shared via GraphSync.
%%% Functions from those two modules should not be called directly.
%%%
%%% This module contains datastore functions for a deprecated model 'storage'.
%%% The model has been renamed to `storage_config` and those functions are
%%% needed to properly perform upgrade procedure.
%%% @end
%%%-------------------------------------------------------------------
-module(storage).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/6, get/1, describe/1, exists/1, delete/1, clear_storages/0]).

%%% Functions to retrieve storage details
-export([get_id/1, get_helper/1, get_type/1, get_luma_config/1]).
-export([fetch_name/1, fetch_qos_parameters_of_local_storage/1,
    fetch_qos_parameters_of_remote_storage/2]).
-export([is_readonly/1, is_luma_enabled/1, is_imported_storage/1, is_posix_compatible/1]).
-export([is_local/1]).

%%% Functions to modify storage details
-export([update_name/2, update_luma_config/2]).
-export([set_readonly/2, set_imported_storage/2, set_qos_parameters/2]).
-export([set_helper_insecure/2, update_helper_args/2, update_helper_admin_ctx/2,
    update_helper/2]).

%%% Support related functions
-export([support_space/3, update_space_support_size/3, revoke_space_support/2]).
-export([supports_any_space/1]).

%%% Upgrade from 19.02.*
-export([migrate_to_zone/0]).

%% Legacy datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

% exported for initializer
-export([on_storage_created/1]).


-type id() :: od_storage:id().
-opaque data() :: storage_config:doc().
-type name() :: od_storage:name().
-type qos_parameters() :: od_storage:qos_parameters().

-export_type([id/0, data/0, name/0, qos_parameters/0]).

-compile({no_auto_import, [get/1]}).

-define(throw_on_error(Res), case Res of
    {error, _} = Error -> throw(Error);
    _ -> Res
end).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(name(), helpers:helper(), boolean(), luma_config:config(),
    boolean(), qos_parameters()) -> {ok, id()} | {error, term()}.
create(Name, Helper, Readonly, LumaConfig, ImportedStorage, QosParameters) ->
    lock_on_storage_by_name(Name, fun() ->
        case is_name_occupied(Name) of
            true ->
                ?ERROR_ALREADY_EXISTS;
            false ->
                create_insecure(Name, Helper, Readonly, LumaConfig, ImportedStorage, QosParameters)
        end
    end).


%% @private
-spec create_insecure(name(), helpers:helper(), boolean(), luma_config:config(),
    boolean(), qos_parameters()) -> {ok, id()} | {error, term()}.
create_insecure(Name, Helper, Readonly, LumaConfig, ImportedStorage, QosParameters) ->
    case storage_logic:create_in_zone(Name, QosParameters) of
        {ok, Id} ->
            case storage_config:create(Id, Helper, Readonly, LumaConfig, ImportedStorage) of
                {ok, Id} ->
                    on_storage_created(Id),
                    {ok, Id};
                StorageConfigError ->
                    case storage_logic:delete_in_zone(Id) of
                        ok -> ok;
                        {error, _} = Error ->
                            ?error("Could not revert creation of storage ~p in Onezone: ~p",
                                [Id, Error])
                    end,
                    StorageConfigError
            end;
        StorageLogicError ->
            StorageLogicError
    end.


-spec get(id() | data()) -> {ok, data()} | {error, term()}.
get(StorageId) when is_binary(StorageId) ->
    storage_config:get(StorageId);
get(StorageData) ->
    {ok, StorageData}.


%%-------------------------------------------------------------------
%% @doc
%% Returns map describing the storage. The data is redacted to
%% remove sensitive information.
%% @end
%%-------------------------------------------------------------------
-spec describe(id() | data()) -> {ok, json_utils:json_term()} | {error, term()}.
describe(StorageId) when is_binary(StorageId) ->
    case get(StorageId) of
        {ok, StorageData} -> describe(StorageData);
        {error, _} = Error -> Error
    end;
describe(StorageData) ->
    StorageId = get_id(StorageData),
    Helper = get_helper(StorageData),
    LumaConfig = get_luma_config(StorageData),
    LumaUrl = case LumaConfig of
        undefined -> undefined;
        _ -> luma_config:get_url(LumaConfig)
    end,
    AdminCtx = helper:get_redacted_admin_ctx(Helper),
    HelperArgs = helper:get_args(Helper),
    Base = maps:merge(HelperArgs, AdminCtx),
    {ok, Base#{
        <<"id">> => StorageId,
        <<"name">> => fetch_name(StorageId),
        <<"type">> => helper:get_name(Helper),
        <<"readonly">> => is_readonly(StorageData),
        <<"insecure">> => helper:is_insecure(Helper),
        <<"storagePathType">> => helper:get_storage_path_type(Helper),
        <<"lumaEnabled">> => is_luma_enabled(StorageData),
        <<"lumaUrl">> => LumaUrl,
        <<"importedStorage">> => is_imported_storage(StorageData),
        <<"qosParameters">> => fetch_qos_parameters_of_local_storage(StorageId)
    }}.


-spec exists(id()) -> boolean().
exists(StorageId) ->
    storage_config:exists(StorageId).


%%--------------------------------------------------------------------
%% @doc
%% Removes storage. Fails with an error if the storage supports
%% any space.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | ?ERROR_STORAGE_IN_USE | {error, term()}.
delete(StorageId) ->
    lock_on_storage_by_id(StorageId, fun() ->
        case supports_any_space(StorageId) of
            true ->
                ?ERROR_STORAGE_IN_USE;
            false ->
                % TODO VFS-5124 Remove from rtransfer
                delete_insecure(StorageId)
        end
    end).


%% @private
-spec delete_insecure(id()) -> ok | {error, term()}.
delete_insecure(StorageId) ->
    case storage_logic:delete_in_zone(StorageId) of
        ok ->
            ok = storage_config:delete(StorageId),
            luma:invalidate(StorageId);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes all storages after provider have been deregistered.
%% @end
%%--------------------------------------------------------------------
-spec clear_storages() -> ok.
clear_storages() ->
    % all storages should have been deleted by Onezone after
    % provider was deregistered, so clear only local data
    storage_config:delete_all().


%%%===================================================================
%%% Functions to retrieve storage details
%%%===================================================================

-spec get_id(data()) -> id().
get_id(StorageData) ->
    storage_config:get_id(StorageData).


-spec get_helper(data() | id()) -> helpers:helper().
get_helper(StorageDataOrId)  ->
    storage_config:get_helper(StorageDataOrId).


-spec get_luma_config(data()) -> luma_config:config() | undefined.
get_luma_config(StorageData) ->
    storage_config:get_luma_config(StorageData).


-spec get_type(data() | id()) -> helper:type().
get_type(StorageDataOrId) ->
    Helper = get_helper(StorageDataOrId),
    helper:get_type(Helper).


-spec fetch_name(id()) -> name().
fetch_name(StorageId) when is_binary(StorageId) ->
    {ok, Name} = ?throw_on_error(storage_logic:get_name(StorageId)),
    Name.


-spec fetch_qos_parameters_of_local_storage(id()) -> qos_parameters().
fetch_qos_parameters_of_local_storage(StorageId) when is_binary(StorageId) ->
    {ok, QosParameters} =
        ?throw_on_error(storage_logic:get_qos_parameters_of_local_storage(StorageId)),
    QosParameters.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves QoS parameters from storage details shared between providers
%% through given space.
%% @end
%%--------------------------------------------------------------------
-spec fetch_qos_parameters_of_remote_storage(id(), od_space:id()) -> qos_parameters().
fetch_qos_parameters_of_remote_storage(StorageId, SpaceId) when is_binary(StorageId) ->
    {ok, QosParameters} =
        ?throw_on_error(storage_logic:get_qos_parameters_of_remote_storage(StorageId, SpaceId)),
    QosParameters.


-spec is_readonly(data() | id()) -> boolean().
is_readonly(StorageDataOrId) ->
    storage_config:is_readonly(StorageDataOrId).


-spec is_imported_storage(data() | id()) -> boolean().
is_imported_storage(StorageDataOrId) ->
    storage_config:is_imported_storage(StorageDataOrId).


-spec is_luma_enabled(data()) -> boolean().
is_luma_enabled(Storage) ->
    get_luma_config(Storage) =/= undefined.


-spec is_local(id()) -> boolean().
is_local(StorageId) ->
    case storage_logic:get_provider(StorageId) of
        ?ERROR_FORBIDDEN -> false;
        {error, _} = Error -> throw(Error);
        {ok, ProviderId} -> oneprovider:is_self(ProviderId)
    end.

-spec is_posix_compatible(id() | data()) -> boolean().
is_posix_compatible(StorageDataOrId) ->
    Helper = get_helper(StorageDataOrId),
    helper:is_posix_compatible(Helper).

%%%===================================================================
%%% Functions to modify storage details
%%%===================================================================

-spec update_name(id(), NewName :: name()) -> ok.
update_name(StorageId, NewName) ->
    storage_logic:update_name(StorageId, NewName).


-spec update_luma_config(id(), ChangesOrNewConfig) -> ok | {error, term()}
    when ChangesOrNewConfig :: #{url => luma_config:url(), api_key => luma_config:api_key()}
                               | luma_config:config() | undefined.
update_luma_config(StorageId, DiffOrNewConfig) ->
    UpdateFun = case is_map(DiffOrNewConfig) of
        false ->
            % New config is given explicitly. May enable/disable luma.
            fun (_) -> {ok, DiffOrNewConfig} end;
        _ ->
            % Changes to existing luma config are given. Only eligible if luma enabled.
            fun (undefined) -> {error, luma_disabled};
                (PreviousConfig) ->
                    {ok, luma_config:new(
                        maps:get(url, DiffOrNewConfig, luma_config:get_url(PreviousConfig)),
                        maps:get(api_key, DiffOrNewConfig, luma_config:get_api_key(PreviousConfig))
                    )}
            end
    end,
    storage_config:update_luma_config(StorageId, UpdateFun).


-spec set_readonly(id(), boolean()) -> ok | {error, term()}.
set_readonly(StorageId, Readonly) ->
    storage_config:set_readonly(StorageId, Readonly).


-spec set_imported_storage(id(), boolean()) -> ok | {error, term()}.
set_imported_storage(StorageId, ImportedStorage) ->
    lock_on_storage_by_id(StorageId, fun() ->
        case supports_any_space(StorageId) of
            true ->
                ?ERROR_STORAGE_IN_USE;
            false ->
                storage_config:set_imported_storage(StorageId, ImportedStorage)
        end
    end).


-spec set_qos_parameters(id(), qos_parameters()) -> ok | errors:error().
set_qos_parameters(StorageId, QosParameters) ->
    case storage_logic:set_qos_parameters(StorageId, QosParameters) of
        ok ->
            {ok, Spaces} = storage_logic:get_spaces(StorageId),
            lists:foreach(fun(SpaceId) ->
                ok = qos_hooks:reevaluate_all_impossible_qos_in_space(SpaceId)
            end, Spaces);
        Error -> Error
    end.


-spec update_helper_args(id(), helper:args()) -> ok | {error, term()}.
update_helper_args(StorageId, Changes) when is_map(Changes) ->
    UpdateFun = fun(Helper) -> helper:update_args(Helper, Changes) end,
    update_helper(StorageId, UpdateFun).


-spec update_helper_admin_ctx(id(), helper:user_ctx()) -> ok | {error, term()}.
update_helper_admin_ctx(StorageId, Changes) ->
    UpdateFun = fun(Helper) -> helper:update_admin_ctx(Helper, Changes) end,
    update_helper(StorageId, UpdateFun).


-spec set_helper_insecure(id(), Insecure :: boolean()) -> ok | {error, term()}.
set_helper_insecure(StorageId, Insecure) when is_boolean(Insecure) ->
    UpdateFun = fun(Helper) -> helper:update_insecure(Helper, Insecure) end,
    update_helper(StorageId, UpdateFun).


-spec update_helper(id(), fun((helpers:helper()) -> helpers:helper())) ->
    ok | {error, term()}.
update_helper(StorageId, UpdateFun) ->
    case storage_config:update_helper(StorageId, UpdateFun) of
        ok -> on_helper_changed(StorageId);
        {error, no_changes} -> ok;
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% Support related functions
%%%===================================================================

-spec support_space(id(), tokens:serialized(), od_space:support_size()) ->
    {ok, od_space:id()} | errors:error().
support_space(StorageId, SerializedToken, SupportSize) ->
    lock_on_storage_by_id(StorageId, fun() ->
        support_space_insecure(StorageId, SerializedToken, SupportSize)
    end).


%% @private
-spec support_space_insecure(id(), tokens:serialized(), od_space:support_size()) ->
    {ok, od_space:id()} | errors:error().
support_space_insecure(StorageId, SpaceSupportToken, SupportSize) ->
    case validate_support_request(SpaceSupportToken) of
        ok ->
            case is_imported_storage(StorageId) andalso supports_any_space(StorageId) of
                true ->
                    ?ERROR_STORAGE_IN_USE;
                false ->
                    case storage_logic:support_space(StorageId, SpaceSupportToken, SupportSize) of
                        {ok, SpaceId} ->
                            on_space_supported(SpaceId, StorageId),
                            {ok, SpaceId};
                        {error, _} = Error ->
                            Error
                    end
            end;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if given token is valid support token and whether provider
%% does not already support this space.
%% @TODO VFS-5497 This check will not be needed when multisupport is implemented
%% @end
%%--------------------------------------------------------------------
-spec validate_support_request(tokens:serialized()) -> ok | errors:error().
validate_support_request(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, #token{type = ?INVITE_TOKEN(?SUPPORT_SPACE, SpaceId)}} ->
            case provider_logic:supports_space(SpaceId) of
                true ->
                    ?ERROR_RELATION_ALREADY_EXISTS(
                        od_space, SpaceId, od_provider, oneprovider:get_id()
                    );
                false -> ok
            end;
        {ok, #token{type = ReceivedType}} ->
           ?ERROR_BAD_VALUE_TOKEN(<<"token">>,
               ?ERROR_NOT_AN_INVITE_TOKEN(?SUPPORT_SPACE, ReceivedType));
        {error, _} = Error ->
            ?ERROR_BAD_VALUE_TOKEN(<<"token">>, Error)
    end.


-spec update_space_support_size(id(), od_space:id(), NewSupportSize :: integer()) ->
    ok | errors:error().
update_space_support_size(StorageId, SpaceId, NewSupportSize) ->
    CurrentOccupiedSize = space_quota:current_size(SpaceId),
    case NewSupportSize < CurrentOccupiedSize of
        true -> ?ERROR_BAD_VALUE_TOO_LOW(<<"size">>, CurrentOccupiedSize);
        false -> storage_logic:update_space_support_size(StorageId, SpaceId, NewSupportSize)
    end.


-spec revoke_space_support(id(), od_space:id()) -> ok | errors:error().
revoke_space_support(StorageId, SpaceId) ->
    %% @TODO VFS-6208 Cancel sync and auto-cleaning traverse and clean up ended tasks when unsupporting
    case storage_logic:revoke_space_support(StorageId, SpaceId) of
        ok -> on_space_unsupported(SpaceId, StorageId);
        Error -> Error
    end.


-spec supports_any_space(id()) -> boolean() | errors:error().
supports_any_space(StorageId) ->
    case storage_logic:get_spaces(StorageId) of
        {ok, []} -> false;
        {ok, _Spaces} -> true;
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec on_storage_created(id()) -> ok.
on_storage_created(StorageId) ->
    rtransfer_config:add_storage(StorageId).


%% @private
-spec on_space_supported(od_space:id(), id()) -> ok.
on_space_supported(SpaceId, StorageId) ->
    % remove possible remnants of previous support 
    % (when space was unsupported in Onezone without provider knowledge)
    delete_associated_documents(SpaceId, StorageId),
    ok = qos_hooks:reevaluate_all_impossible_qos_in_space(SpaceId).


%% @private
-spec on_space_unsupported(od_space:id(), id()) -> ok.
on_space_unsupported(SpaceId, StorageId) ->
    delete_associated_documents(SpaceId, StorageId),
    main_harvesting_stream:space_unsupported(SpaceId),
    luma:invalidate(StorageId, SpaceId).


%% @private
-spec on_helper_changed(StorageId :: id()) -> ok.
on_helper_changed(StorageId) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    fslogic_event_emitter:emit_helper_params_changed(StorageId),
    rtransfer_config:add_storage(StorageId),
    rpc:multicall(Nodes, rtransfer_config, restart_link, []),
    helpers_reload:refresh_helpers_by_storage(StorageId).


%% @private
-spec delete_associated_documents(od_space:id(), id()) -> ok.
delete_associated_documents(SpaceId, StorageId) ->
    file_popularity_api:disable(SpaceId),
    file_popularity_api:delete_config(SpaceId),
    autocleaning_api:disable(SpaceId),
    autocleaning_api:delete_config(SpaceId),
    storage_sync:clean_up(SpaceId, StorageId).


%% @private
-spec is_name_occupied(name()) -> boolean().
is_name_occupied(Name) ->
    {ok, StorageIds} = provider_logic:get_storage_ids(),
    lists:member(Name, lists:map(fun storage_logic:get_name/1, StorageIds)).


%% @private
-spec lock_on_storage_by_id(id(), fun(() -> Result)) -> Result.
lock_on_storage_by_id(Identifier, Fun) ->
    critical_section:run({storage_id, Identifier}, Fun).


%% @private
-spec lock_on_storage_by_name(name(), fun(() -> Result)) -> Result.
lock_on_storage_by_name(Identifier, Fun) ->
    critical_section:run({storage_name, Identifier}, Fun).


%%%===================================================================
%%% Upgrade from 19.02.*
%%%===================================================================

-define(ZONE_CONNECTION_RETRIES, 180).

%%--------------------------------------------------------------------
%% @doc
%% Migrates storages and spaces support data to Onezone.
%% Removes obsolete space_storage and storage documents.
%% Dedicated for upgrading Oneprovider from 19.02.* to 19.09.*.
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
    {ok, StorageDocs} = list_deprecated(),
    lists:foreach(fun migrate_storage_docs/1, StorageDocs),

    {ok, Spaces} = provider_logic:get_spaces(),
    lists:foreach(fun migrate_space_support/1, Spaces),

    % Remove virtual storage (with id equal to that of provider) in Onezone
    case storage_logic:delete_in_zone(oneprovider:get_id()) of
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
        helpers = [Helper],
        readonly = Readonly,
        luma_config = LumaConfig
    } = Storage,
    case storage_config:create(StorageId, Helper, Readonly, LumaConfig, false) of
        {ok, _} -> ok;
        {error, already_exists} -> ok;
        Error -> throw(Error)
    end,
    case provider_logic:has_storage(StorageId) of
        true -> ok;
        false ->
            {ok, StorageId} = storage_logic:create_in_zone(Name, #{}, StorageId),
            ?notice("Storage ~p created in Onezone", [StorageId])
    end,
    ok = delete_deprecated(StorageId).


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
                    case storage_logic:upgrade_legacy_support(StorageId, SpaceId) of
                        ok -> ?notice("Support of space ~p by storage ~p upgraded in Onezone",
                            [SpaceId, StorageId]);
                        Error1 -> throw(Error1)
                    end
            end,
            case lists:member(StorageId, MiR) of
                true -> ok = storage_config:set_imported_storage(StorageId, true);
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


%% @TODO VFS-5856 deprecated, included for upgrade procedure. Remove in 19.09.*.
%%%===================================================================
%% Deprecated API and datastore_model callbacks
%%
%% All functions below are deprecated. Previous `storage` model have
%% been renamed to `storage_config` and this functions are needed to
%% properly perform upgrade procedure.
%%%===================================================================

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all
}).


%%--------------------------------------------------------------------
%% @doc
%% Deletes storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_deprecated(id()) -> ok | {error, term()}.
delete_deprecated(StorageId) ->
    datastore_model:delete(?CTX, StorageId).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list_deprecated() -> {ok, [datastore_doc:doc(#storage{})]} | {error, term()}.
list_deprecated() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).


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
    5.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, binary},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}}
        ]}]}
    ]};
get_record_struct(2) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean}
        ]}]},
        {readonly, boolean}
    ]};
get_record_struct(3) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {cache_timeout, integer},
            {api_key, string}
        ]}}
    ]};
get_record_struct(4) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean},
            {extended_direct_io, boolean}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {cache_timeout, integer},
            {api_key, string}
        ]}}
    ]};
get_record_struct(5) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean},
            {extended_direct_io, boolean},
            {storage_path_type, string}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {api_key, string}
        ]}}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {storage, Name, Helpers}) ->
    {2, {storage,
        Name,
        [{
            helper,
            helper:translate_name(HelperName),
            maps:fold(fun(K, V, Args) ->
                maps:put(helper:translate_arg_name(K), V, Args)
            end, #{}, HelperArgs),
            #{},
            false
        } || {_, HelperName, HelperArgs} <- Helpers],
        false
    }};
upgrade_record(2, {_, Name, Helpers, Readonly}) ->
    {3, {storage,
        Name,
        Helpers,
        Readonly,
        undefined
    }};
upgrade_record(3, {_, Name, Helpers, Readonly, LumaConfig}) ->
    {4, {storage,
        Name,
        [{
            helper,
            HelperName,
            HelperArgs,
            AdminCtx,
            Insecure,
            false
        } || {_, HelperName, HelperArgs, AdminCtx, Insecure} <- Helpers],
        Readonly,
        LumaConfig
    }};
upgrade_record(4, {_, Name, Helpers, Readonly, LumaConfig}) ->
    {5, {storage,
        Name,
        [
            #helper{
                name = HelperName,
                args = HelperArgs,
                admin_ctx = AdminCtx,
                insecure = Insecure,
                extended_direct_io = ExtendedDirectIO,
                storage_path_type = ?CANONICAL_STORAGE_PATH
            } || {_, HelperName, HelperArgs, AdminCtx, Insecure,
            ExtendedDirectIO} <- Helpers
        ],
        Readonly,
        LumaConfig
    }}.
