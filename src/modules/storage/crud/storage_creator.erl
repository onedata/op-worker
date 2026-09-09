%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles storage creation.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_creator).
-feature(maybe_expr, enable).
-compile({feature, maybe_expr, enable}).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/logging.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").

%% API
-export([create/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(onedata_storage:create_spec()) -> {ok, storage:id()} | {error, term()}.
create(StorageCreateSpec) ->
    StorageName = StorageCreateSpec#storage_create_spec.name,
    StorageType = StorageCreateSpec#storage_create_spec.type,

    ?info("Gathered storage configuration for '~ts' (~ts) - parameters: ~n~ts", [
        StorageName, StorageType, storage_crud_utils:pretty_print_spec(StorageCreateSpec)
    ]),

    try do_create(StorageCreateSpec) of
        {ok, StorageId} = Result ->
            ?notice("Successfully added storage '~ts' (~ts) with Id: '~ts'", [
                StorageName, StorageType, StorageId
            ]),
            Result;
        {error, _} = Error ->
            ?error(?autoformat_with_msg("Failed to add storage '~ts' (~ts)", [StorageName, StorageType], Error)),
            Error
    catch Class:Reason:Stacktrace ->
        ?examine_exception(
            "Unexpected error when adding storage '~ts' (~ts)",
            [StorageName, StorageType],
            Class, Reason, Stacktrace
        )
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec do_create(onedata_storage:create_spec()) -> {ok, storage:id()} | {error, term()}.
do_create(StorageCreateSpec = #storage_create_spec{
    name = Name,
    type = Type,
    readonly = Readonly,
    imported = Imported,
    luma = LumaSpec,
    qos_parameters = QosParameters
}) ->
    HelperSpec = helper_spec:build(StorageCreateSpec),
    storage_crud_utils:verify_storage_path_type(HelperSpec),
    storage_crud_utils:verify_configuration(Name, Readonly, Imported, HelperSpec),

    LumaConfig = build_luma_config(LumaSpec),
    run_diagnostics(HelperSpec, LumaConfig, StorageCreateSpec),

    ?info("Adding storage: '~ts' (~ts)", [Name, Type]),
    maybe
        {ok, Id} ?= storage_logic:create_in_zone(Name, Imported, Readonly, QosParameters),

        case storage_config:create(Id, HelperSpec, LumaConfig) of
            {ok, Id} ->
                storage:on_storage_created(Id),
                {ok, Id};
            StorageConfigError ->
                revert_creation_in_zone(Id),
                StorageConfigError
        end
    end.


%% @private
-spec build_luma_config(onedata_storage:luma_spec()) -> luma_config:config().
build_luma_config(#luma_spec{feed = external, url = Url, api_key = ApiKey}) ->
    luma_config:new_with_external_feed(Url, ApiKey);
build_luma_config(#luma_spec{feed = Feed}) ->
    luma_config:new(Feed).


%% @private
-spec run_diagnostics(helper_spec:t(), luma_config:config(), onedata_storage:create_spec()) ->
    ok.
run_diagnostics(HelperSpec, LumaConfig, #storage_create_spec{
    name = Name,
    type = Type,
    readonly = Readonly
}) ->
    LumaFeed = luma_config:get_feed(LumaConfig),
    % the storage supports no spaces yet, so being readonly is the only reason
    % not to write a test file on it
    DiagnosticsMode = case Readonly of
        true -> access_only;
        false -> access_and_read_write
    end,

    ?info("Verifying storage access: '~ts' (~ts)", [Name, Type]),
    storage_crud_utils:run_diagnostics(HelperSpec, LumaFeed, DiagnosticsMode).


%% @private
-spec revert_creation_in_zone(storage:id()) -> ok.
revert_creation_in_zone(StorageId) ->
    case storage_logic:delete_in_zone(StorageId) of
        ok ->
            ok;
        {error, _} = DeleteError ->
            ?error("Could not revert creation of storage '~ts' in Onezone: ~tp", [
                StorageId, DeleteError
            ])
    end.
