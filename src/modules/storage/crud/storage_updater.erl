%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles storage update.
%%% NOTE: DO NOT CALL this module directly; always use storage.erl module
%%%       as it wraps update in critical section
%%% @end
%%%-------------------------------------------------------------------
-module(storage_updater).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("op_panel_contracts/include/storage/common.hrl").

%% API
-export([
    update/2,
    update_helper_config/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec update(storage:id(), onedata_storage:update_spec()) -> ok | errors:error().
update(StorageId, UpdateSpec) ->
    ?info("Gathered storage (id: '~ts') update diff: ~n~ts", [
        StorageId, storage_crud_utils:pretty_print_spec(UpdateSpec)
    ]),

    try do_update(StorageId, UpdateSpec) of
        ok ->
            ?info("Successfully updated storage '~ts'", [StorageId]),
            ok;
        {error, _} = Error ->
            ?error(?autoformat_with_msg("Failed to update storage '~ts'", [StorageId], Error)),
            Error
    catch Class:Reason:Stacktrace ->
        ?examine_exception(
            "Unexpected error when updating storage '~ts'", [StorageId],
            Class, Reason, Stacktrace
        )
    end.


-spec update_helper_config(storage:id(), fun((helper_config:t()) -> {ok, helper_config:t()} | {error, term()})) ->
    ok | {error, term()}.
update_helper_config(StorageId, UpdateFun) ->
    case storage_config:update_helper_config(StorageId, UpdateFun) of
        ok -> on_helper_changed(StorageId);
        {error, no_changes} -> ok;
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec do_update(storage:id(), onedata_storage:update_spec()) -> ok | errors:error().
do_update(StorageId, UpdateSpec) ->
    CurrentOpStorageData = get_op_storage_data(StorageId),
    assert_valid_type(CurrentOpStorageData, UpdateSpec),

    CurrentOzStorageData = get_oz_storage_data(StorageId),

    {HelperConfigChanged, LumaChanged, NewStorageConfig} = prepare_new_storage_config(
        StorageId, UpdateSpec, CurrentOpStorageData, CurrentOzStorageData
    ),

    MaybeQosParams = UpdateSpec#storage_update_spec.qos_parameters,

    % TODO fld with rollback and compensation ?
    lists:foreach(fun
        ({true, UpdateFun}) ->
            case UpdateFun() of
                ok -> ok;
                {error, _} = Error -> throw(Error)
            end;
        (_) ->
            ok
    end, [
        {HelperConfigChanged orelse LumaChanged, fun() ->
            case storage_config:update(StorageId, fun(_) -> {ok, NewStorageConfig} end) of
                {ok, _} -> ok;
                {error, _} = Error -> Error
            end
        end},
        {HelperConfigChanged, fun() -> on_helper_changed(StorageId) end},
        {true, fun() -> storage_logic:update_in_zone(StorageId, UpdateSpec) end},
        {MaybeQosParams /= undefined, fun() -> on_qos_change(StorageId) end},
        {LumaChanged, fun() -> luma_crud_api:clear_db(CurrentOpStorageData) end}
    ]).


%% @private
-spec get_oz_storage_data(storage:id()) -> od_storage:record() | no_return().
get_oz_storage_data(StorageId) ->
    case storage_logic:get(StorageId) of
        {ok, #document{value = StorageData}} -> StorageData;
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec get_op_storage_data(storage:id()) -> storage:data() | no_return().
get_op_storage_data(StorageId) ->
    case storage:get(StorageId) of
        {ok, StorageData} -> StorageData;
        {error, not_found} -> throw(?ERROR_NOT_FOUND)
    end.


%% @private
-spec assert_valid_type(storage:data(), onedata_storage:update_spec()) -> ok | no_return().
assert_valid_type(OpStorageData, UpdateSpec) ->
    HelperConfig = storage:get_helper_config(OpStorageData),
    StorageType = helper_config:get_name(HelperConfig),

    case StorageType == UpdateSpec#storage_update_spec.type of
        true -> ok;
        false -> throw(?ERR_BAD_VALUE_NOT_ALLOWED(?err_ctx(), <<"type">>, [StorageType]))
    end.


%% @private
-spec prepare_new_storage_config(
    storage:id(),
    onedata_storage:update_spec(),
    storage:data(),
    od_storage:record()
) ->
    {boolean(), boolean(), storage_config:record()} | no_return().
prepare_new_storage_config(StorageId, UpdateSpec, CurrentOpStorageData, CurrentOzStorageData) ->
    CurrentHelperConfig = storage:get_helper_config(CurrentOpStorageData),

    {ReadonlyChanged, NewReadonly} = infer_new_value(
        UpdateSpec#storage_update_spec.readonly,
        CurrentOzStorageData#od_storage.readonly
    ),
    {ImportedChanged, NewImported} = infer_new_value(
        UpdateSpec#storage_update_spec.imported,
        CurrentOzStorageData#od_storage.imported
    ),

    {HelperConfigChanged, NewHelperConfig} = case helper_config:update(CurrentHelperConfig, UpdateSpec) of
        {ok, UpdatedHelperConfig} ->
            storage_crud_utils:verify_configuration(
                StorageId, NewReadonly, NewImported, UpdatedHelperConfig
            ),
            {true, UpdatedHelperConfig};
        {error, no_change} when ReadonlyChanged orelse ImportedChanged ->
            storage_crud_utils:verify_configuration(
                StorageId, NewReadonly, NewImported, CurrentHelperConfig
            ),
            {false, CurrentHelperConfig};
        {error, no_change} ->
            {false, CurrentHelperConfig}
    end,

    CurrentLumaConfig = storage:get_luma_config(CurrentOpStorageData),
    CurrentLumaGeneration = storage:get_luma_generation(CurrentOpStorageData),

    {LumaChanged, NewLumaConfig} = case UpdateSpec#storage_update_spec.luma of
        undefined ->
            {false, CurrentLumaConfig};
        LumaSpec ->
            LumaDiff = build_luma_diff(LumaSpec),
            case luma_config:update(CurrentLumaConfig, LumaDiff) of
                {ok, UpdatedLumaConfig} ->
                    {true, UpdatedLumaConfig};
                {error, no_update} ->
                    {false, CurrentLumaConfig};
                {error, _} = Error ->
                    throw(Error)
            end
    end,

    NewLumaFeed = luma_config:get_feed(NewLumaConfig),
    IgnoreReadWriteTest = NewReadonly orelse
        (NewImported andalso storage:supports_any_space(StorageId)),
    storage_crud_utils:run_diagnostics(NewHelperConfig, NewLumaFeed, not IgnoreReadWriteTest),

    {HelperConfigChanged, LumaChanged, #storage_config{
        helper_config = NewHelperConfig,
        luma_config = NewLumaConfig,
        luma_generation = case LumaChanged of
            true -> CurrentLumaGeneration + 1;
            false -> CurrentLumaGeneration
        end
    }}.


%% @private
-spec infer_new_value(undefined | term(), term()) -> {boolean(), term()}.
infer_new_value(undefined, CurrentValue) -> {false, CurrentValue};
infer_new_value(NewValue, CurrentValue) -> {NewValue /= CurrentValue, CurrentValue}.


%% @private
-spec build_luma_diff(onedata_storage:luma_spec()) -> luma_config:diff().
build_luma_diff(#luma_spec{feed = Feed, url = Url, api_key = ApiKey}) ->
    maps_utils:remove_undefined(#{
        feed => Feed,
        url => Url,
        api_key => ApiKey
    }).


%% @private
-spec on_helper_changed(storage:id()) -> ok.
on_helper_changed(StorageId) ->
    fslogic_event_emitter:emit_helper_params_changed(StorageId),
    % TODO VFS-11947 consider error handling here and error propagation / rollback
    rtransfer_config:add_storage(StorageId),
    helpers_reload:refresh_helpers_by_storage(StorageId).


%% @private
-spec on_qos_change(storage:id()) -> ok.
on_qos_change(StorageId) ->
    {ok, Spaces} = storage_logic:get_spaces(StorageId),

    lists:foreach(fun(SpaceId) ->
        ok = qos_logic:reevaluate_all_impossible_qos_in_space(SpaceId)
    end, Spaces).
