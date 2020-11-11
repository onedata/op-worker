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
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/6, get/1, exists/1, delete/1, clear_storages/0]).

%% Functions to retrieve storage details in Onepanel compatible format
-export([describe/1, describe_luma_config/1]).

%%% Functions to retrieve storage details
-export([
    get_id/1, get_block_size/1, get_helper/1, get_helper_name/1,
    get_luma_feed/1, get_luma_config/1
]).
-export([fetch_name/1, fetch_qos_parameters_of_local_storage/1,
    fetch_qos_parameters_of_remote_storage/2]).
-export([should_skip_storage_detection/1, is_imported/1, is_posix_compatible/1, is_local_storage_readonly/1, is_storage_readonly/2]).
-export([has_non_auto_luma_feed/1]).
-export([is_local/1]).
-export([verify_configuration/3]).

%%% Functions to modify storage details
-export([update_name/2, update_luma_config/2]).
-export([set_qos_parameters/2, update_readonly_and_imported/3]).
-export([update_helper_args/2, update_helper_admin_ctx/2,
    update_helper/2]).

%%% Support related functions
-export([support_space/3, update_space_support_size/3, revoke_space_support/2]).
-export([supports_any_space/1]).

%%% Upgrade from 19.02.*
-export([migrate_to_zone/0]).

%%% Upgrade from 20.02.0-beta3
-export([migrate_imported_storages_to_zone/0]).

%% Legacy datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

% exported for initializer and env_up escripts
-export([on_storage_created/1]).


-type id() :: od_storage:id().
-opaque data() :: storage_config:doc().
-type name() :: od_storage:name().
-type qos_parameters() :: od_storage:qos_parameters().
-type luma_feed() :: luma:feed().
-type luma_config() :: luma_config:config().
-type access_type() :: ?READONLY | ?READWRITE.
-type imported() :: boolean().
-type readonly() :: boolean().

%% @formatter:off
-type config() :: #{
    readonly => readonly(),
    importedStorage => imported(),
    skipStorageDetection => boolean()
}.
%% @formatter:on

-export_type([id/0, data/0, name/0, qos_parameters/0, luma_config/0, luma_feed/0, access_type/0,
    imported/0, readonly/0, config/0]).

-compile({no_auto_import, [get/1]}).

-define(throw_on_error(Res), case Res of
    {error, _} = Error -> throw(Error);
    _ -> Res
end).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(name(), helpers:helper(), luma_config(),
    imported(), readonly(), qos_parameters()) -> {ok, id()} | {error, term()}.
create(Name, Helper, LumaConfig, ImportedStorage, Readonly, QosParameters) ->
    lock_on_storage_by_name(Name, fun() ->
        case is_name_occupied(Name) of
            true ->
                ?ERROR_ALREADY_EXISTS;
            false ->
                create_insecure(Name, Helper, LumaConfig, ImportedStorage, Readonly, QosParameters)
        end
    end).


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
    AdminCtx = helper:get_redacted_admin_ctx(Helper),
    HelperArgs = helper:get_args(Helper),
    Base = maps:merge(HelperArgs, AdminCtx),
    {ok, LumaConfigDescription} = describe_luma_config(StorageData),
    BaseWithLuma = maps:merge(Base, LumaConfigDescription),
    {ok, BaseWithLuma#{
        <<"id">> => StorageId,
        <<"name">> => fetch_name(StorageId),
        <<"type">> => helper:get_name(Helper),
        <<"importedStorage">> => is_imported(StorageId),
        <<"readonly">> => is_local_storage_readonly(StorageId),
        <<"qosParameters">> => fetch_qos_parameters_of_local_storage(StorageId)
    }}.


-spec describe_luma_config(id() | data()) -> {ok, json_utils:json_map()}.
describe_luma_config(StorageId) when is_binary(StorageId) ->
    case get(StorageId) of
        {ok, StorageData} -> describe(StorageData);
        {error, _} = Error -> Error
    end;
describe_luma_config(StorageData) ->
    LumaConfig = get_luma_config(StorageData),
    {ok, luma_config:describe(LumaConfig)}.


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
            luma:clear_db(StorageId);
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


-spec verify_configuration(id() | name(), config(), helpers:helper()) -> ok | {error, term()}.
verify_configuration(IdOrName, Config, Helper) ->
    try
        sanitize_readonly_option(IdOrName, Config),
        check_helper_against_readonly_option(Config, Helper),
        check_helper_against_imported_option(Config, Helper)
    catch
        throw:Error -> Error
    end.

%%%===================================================================
%%% Functions to retrieve storage details
%%%===================================================================

-spec get_id(id() | data()) -> id().
get_id(StorageId) when is_binary(StorageId) ->
    StorageId;
get_id(StorageData) ->
    storage_config:get_id(StorageData).


%%--------------------------------------------------------------------
%% @doc
%% Returns size of block used by underlying object storage.
%% For posix-compatible ones 'undefined' is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_block_size(id()) -> non_neg_integer() | undefined.
get_block_size(StorageId) ->
    helper:get_block_size(get_helper(StorageId)).


-spec get_helper(data() | id()) -> helpers:helper().
get_helper(StorageDataOrId) ->
    storage_config:get_helper(StorageDataOrId).

-spec get_helper_name(data() | id()) -> helper:name().
get_helper_name(StorageDataOrId) ->
    Helper = storage_config:get_helper(StorageDataOrId),
    helper:get_name(Helper).

-spec get_luma_feed(id() | data()) -> luma_feed().
get_luma_feed(Storage) ->
    storage_config:get_luma_feed(Storage).

-spec get_luma_config(id() | data()) -> luma_config().
get_luma_config(StorageData) ->
    storage_config:get_luma_config(StorageData).


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


-spec should_skip_storage_detection(data() | id()) -> boolean().
should_skip_storage_detection(StorageDataOrId) ->
    storage_config:should_skip_storage_detection(StorageDataOrId).


-spec is_imported(id() | data()) -> boolean().
is_imported(StorageId) when is_binary(StorageId) ->
    {ok, Imported} = ?throw_on_error(storage_logic:is_imported(StorageId)),
    Imported;
is_imported(StorageData) ->
    is_imported(storage:get_id(StorageData)).

-spec is_local_storage_readonly(id()) -> boolean().
is_local_storage_readonly(StorageId) when is_binary(StorageId) ->
    {ok, Readonly} = ?throw_on_error(storage_logic:is_local_storage_readonly(StorageId)),
    Readonly.

-spec is_storage_readonly(id() | data(), od_space:id()) -> boolean().
is_storage_readonly(StorageId, SpaceId) when is_binary(StorageId) ->
    {ok, Readonly} = ?throw_on_error(storage_logic:is_storage_readonly(StorageId, SpaceId)),
    Readonly;
is_storage_readonly(StorageData, SpaceId) ->
    is_storage_readonly(storage:get_id(StorageData), SpaceId).


-spec has_non_auto_luma_feed(data()) -> boolean().
has_non_auto_luma_feed(Storage) ->
    get_luma_feed(Storage) =/= ?AUTO_FEED.


-spec is_local(id()) -> boolean().
is_local(StorageId) ->
    provider_logic:has_storage(StorageId).


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


-spec update_luma_config(id(), Diff :: luma_config:diff()) ->
    ok | {error, term()}.
update_luma_config(StorageId, Diff) ->
    UpdateFun = fun(LumaConfig) ->
        luma_config:update(LumaConfig, Diff)
    end,
    case storage_config:update_luma_config(StorageId, UpdateFun) of
        ok ->
            luma:clear_db(StorageId);
        {error, no_update} ->
            ok;
        {error, _} = Error ->
            Error
    end.


-spec update_readonly_and_imported(id(), readonly(), imported()) -> ok | {error, term()}.
update_readonly_and_imported(StorageId, Readonly, Imported) ->
    storage_logic:update_readonly_and_imported(StorageId, Readonly, Imported).

-spec set_qos_parameters(id(), qos_parameters()) -> ok | errors:error().
set_qos_parameters(StorageId, QosParameters) ->
    set_qos_parameters(StorageId, oneprovider:get_id(), QosParameters).


-spec set_qos_parameters(id(), oneprovider:id(), qos_parameters()) -> ok | errors:error().
set_qos_parameters(_StorageId, ProviderId, #{<<"providerId">> := OtherProvider}) when ProviderId =/= OtherProvider ->
    ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"qosParameters.providerId">>, [ProviderId]);
set_qos_parameters(StorageId, _ProviderId, #{<<"storageId">> := OtherStorage}) when StorageId =/= OtherStorage ->
    ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"qosParameters.storageId">>, [StorageId]);
set_qos_parameters(StorageId, ProviderId, QosParameters) ->
    ExtendedQosParameters = QosParameters#{
        <<"storageId">> => StorageId,
        <<"providerId">> => ProviderId
    },
    case storage_logic:set_qos_parameters(StorageId, ExtendedQosParameters) of
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
    case validate_support_request(SerializedToken) of
        ok ->
            case storage_logic:support_space(StorageId, SerializedToken, SupportSize) of
                {ok, SpaceId} ->
                    on_space_supported(SpaceId, StorageId),
                    {ok, SpaceId};
                {error, _} = Error ->
                    Error
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
    %% @TODO VFS-6132 Use space_unsupport when it is implemented
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

-spec on_storage_created(id()) -> ok.
on_storage_created(StorageId) ->
    rtransfer_config:add_storage(StorageId).


%% @private
-spec on_storage_created(id(), qos_parameters()) -> ok.
on_storage_created(StorageId, QosParameters) ->
    ok = set_qos_parameters(StorageId, QosParameters),
    on_storage_created(StorageId).


%% @private
-spec on_space_supported(od_space:id(), id()) -> ok.
on_space_supported(SpaceId, StorageId) ->
    % remove possible remnants of previous support 
    % (when space was unsupported in Onezone without provider knowledge)
    space_unsupport:cleanup_local_documents(SpaceId, StorageId),
    space_logic:on_space_supported(SpaceId).


%% @private
-spec on_space_unsupported(od_space:id(), id()) -> ok.
on_space_unsupported(SpaceId, StorageId) ->
    space_unsupport:cleanup_local_documents(SpaceId, StorageId),
    auto_storage_import_worker:notify_space_unsupported(SpaceId),
    main_harvesting_stream:space_unsupported(SpaceId).


%% @private
-spec on_helper_changed(StorageId :: id()) -> ok.
on_helper_changed(StorageId) ->
    fslogic_event_emitter:emit_helper_params_changed(StorageId),
    rtransfer_config:add_storage(StorageId),
    rpc:multicall(consistent_hashing:get_all_nodes(), rtransfer_config, restart_link, []),
    helpers_reload:refresh_helpers_by_storage(StorageId).


%% @private
-spec is_name_occupied(name()) -> boolean().
is_name_occupied(Name) ->
    {ok, StorageIds} = provider_logic:get_storage_ids(),
    lists:member(Name, lists:map(fun(StorageId) ->
        {ok, OccupiedName} = storage_logic:get_name(StorageId),
        OccupiedName
    end, StorageIds)).


%% @private
-spec lock_on_storage_by_id(id(), fun(() -> Result)) -> Result.
lock_on_storage_by_id(Identifier, Fun) ->
    critical_section:run({storage_id, Identifier}, Fun).


%% @private
-spec lock_on_storage_by_name(name(), fun(() -> Result)) -> Result.
lock_on_storage_by_name(Identifier, Fun) ->
    critical_section:run({storage_name, Identifier}, Fun).


%% @private
-spec create_insecure(name(), helpers:helper(), luma_config(),
    imported(), readonly(), qos_parameters()) -> {ok, id()} | {error, term()}.
create_insecure(Name, Helper, LumaConfig, ImportedStorage, Readonly, QosParameters) ->
    case storage_logic:create_in_zone(Name, ImportedStorage, Readonly) of
        {ok, Id} ->
            case storage_config:create(Id, Helper, LumaConfig) of
                {ok, Id} ->
                    on_storage_created(Id, QosParameters),
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


%% @private
-spec check_helper_against_readonly_option(config(), helpers:helper()) -> ok.
check_helper_against_readonly_option(#{readonly := true}, _Helper) ->
    ok;
check_helper_against_readonly_option(#{readonly := false}, Helper) ->
    case helper:supports_storage_access_type(Helper, ?READWRITE) of
        false ->
            HelperName = helper:get_name(Helper),
            throw(?ERROR_REQUIRES_READONLY_STORAGE(HelperName));
        true ->
            ok
    end.

%% @private
-spec check_helper_against_imported_option(config(), helpers:helper()) -> ok.
check_helper_against_imported_option(#{importedStorage := false}, _Helper) ->
    ok;
check_helper_against_imported_option(#{importedStorage := true}, Helper) ->
    case helper:is_import_supported(Helper) of
        false ->
            HelperName = helper:get_name(Helper),
            throw(?ERROR_STORAGE_IMPORT_NOT_SUPPORTED(HelperName, ?OBJECT_HELPERS));
        true ->
            ok
    end.


%% @private
-spec sanitize_readonly_option(id() | name(), config()) -> ok.
sanitize_readonly_option(IdOrName, #{
    skipStorageDetection := SkipStorageDetection,
    readonly := Readonly,
    importedStorage := Imported
}) ->

    case {ensure_boolean(Readonly), ensure_boolean(SkipStorageDetection), ensure_boolean(Imported)} of
        {false, _, _} -> ok;
        {true, false, _} -> throw(?ERROR_BAD_VALUE_NOT_ALLOWED(skipStorageDetection, [true]));
        {true, true, false} -> throw(?ERROR_REQUIRES_IMPORTED_STORAGE(IdOrName));
        {true, true, true} -> ok
    end.



-spec ensure_boolean(binary() | boolean()) -> boolean().
ensure_boolean(<<"true">>) -> true;
ensure_boolean(<<"false">>) -> false;
ensure_boolean(Boolean) when is_boolean(Boolean) -> Boolean.


%%%===================================================================
%%% Upgrade from 19.02.*
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Migrates storages and spaces support data to Onezone.
%% Removes obsolete space_storage and storage documents.
%% Dedicated for upgrading Oneprovider from 19.02.* to 19.09.*.
%% @end
%%--------------------------------------------------------------------
-spec migrate_to_zone() -> ok.
migrate_to_zone() ->
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
migrate_storage_docs(#document{key = StorageId, value = Storage = #storage{name = Name}}) ->
    StorageConfigV1 = storage_config:migrate_to_storage_config_v1(Storage),
    {_, StorageConfig} = datastore_versions:upgrade_record(1, storage_config, StorageConfigV1),
    case storage_config:create(StorageId, StorageConfig) of
        {ok, _} -> ok;
        {error, already_exists} -> ok;
        Error -> throw(Error)
    end,
    case provider_logic:has_storage(StorageId) of
        true -> ok;
        false ->
            {ok, StorageId} = storage_logic:create_in_zone(Name, unknown, false, StorageId),
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
            ok = storage_logic:set_imported(StorageId, lists:member(StorageId, MiR)),
            case space_storage:delete(SpaceId) of
                ok -> ok;
                ?ERROR_NOT_FOUND -> ok;
                Error2 -> throw(Error2)
            end;
        ?ERROR_NOT_FOUND ->
            ok
    end,
    ?notice("Support of space: ~p successfully migrated", [SpaceId]).


%%%===================================================================
%%% Upgrade from 20.02.0-beta3
%%%===================================================================

-spec migrate_imported_storages_to_zone() -> ok.
migrate_imported_storages_to_zone() ->
    ?info("Starting imported storages migration procedure..."),
    {ok, StorageIds} = provider_logic:get_storage_ids(),
    lists:foreach(fun(StorageId) ->
        ImportedStorage = storage_config:is_imported_storage(StorageId),
        storage_logic:set_imported(StorageId, ImportedStorage)
    end, StorageIds),
    ?notice("Imported storages migration procedure finished succesfully").


%% @TODO VFS-5856 deprecated, included for upgrade procedure. Remove in next major release after 20.02.*.
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
upgrade_record(4, {_, Name, Helpers, Readonly, {luma_config, Url, _CacheTimeout, ApiKey}}) ->
    {5, {storage,
        Name,
        [
            {helper,
                HelperName,
                HelperArgs,
                AdminCtx,
                Insecure,
                ExtendedDirectIO,
                ?CANONICAL_STORAGE_PATH
            } || {_, HelperName, HelperArgs, AdminCtx, Insecure,
            ExtendedDirectIO} <- Helpers
        ],
        Readonly,
        {luma_config, Url, ApiKey}
    }}.