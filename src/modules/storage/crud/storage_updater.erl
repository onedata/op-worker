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


-record(saga_step, {
    name :: binary(),
    should_run :: boolean(),
    action :: fun(() -> ok | {error, term()}),
    compensation :: fun(() -> ok | {error, term()})
}).
-type saga_step() :: #saga_step{}.

-type named_compensation() :: {Name :: binary(), fun(() -> ok | {error, term()})}.


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
    PrevStorageConfigDoc = get_op_storage_config_doc(StorageId),
    PrevStorageConfig = PrevStorageConfigDoc#document.value,

    assert_valid_type(PrevStorageConfig, UpdateSpec),

    PrevOzStorageData = get_oz_storage_data(StorageId),

    {HelperConfigChanged, LumaChanged, NewStorageConfig} = prepare_new_storage_config(
        StorageId, UpdateSpec, PrevStorageConfig, PrevOzStorageData
    ),

    Result = execute_saga([
        #saga_step{
            name = <<"update OP storage config">>,
            should_run = HelperConfigChanged orelse LumaChanged,
            action = fun() ->
                update_in_op(StorageId, NewStorageConfig, HelperConfigChanged)
            end,
            compensation = fun() ->
                update_in_op(StorageId, PrevStorageConfig, HelperConfigChanged)
            end
        },
        #saga_step{
            name = <<"update storage data in Onezone">>,
            should_run = true,
            action = fun() ->
                storage_logic:update_in_zone(StorageId, UpdateSpec)
            end,
            compensation = fun() ->
                RollbackUpdateSpec = build_rollback_oz_spec(UpdateSpec, PrevOzStorageData),
                storage_logic:update_in_zone(StorageId, RollbackUpdateSpec)
            end
        },
        #saga_step{
            name = <<"reevaluate QoS entries">>,
            should_run = UpdateSpec#storage_update_spec.qos_parameters /= undefined,
            action = fun() -> on_qos_change(StorageId) end,
            compensation = fun() -> ok end
        }
    ]),

    LumaChanged andalso best_effort_clear_luma(StorageId, case Result of
        ok -> PrevStorageConfig;
        {error, _} -> NewStorageConfig
    end),

    Result.


%% @private
-spec get_oz_storage_data(storage:id()) -> od_storage:record() | no_return().
get_oz_storage_data(StorageId) ->
    case storage_logic:get(StorageId) of
        {ok, #document{value = StorageData}} -> StorageData;
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec get_op_storage_config_doc(storage:id()) -> storage_config:doc() | no_return().
get_op_storage_config_doc(StorageId) ->
    case storage_config:get(StorageId) of
        {ok, StorageConfigDoc} -> StorageConfigDoc;
        {error, not_found} -> throw(?ERROR_NOT_FOUND)
    end.


%% @private
-spec assert_valid_type(storage_config:record(), onedata_storage:update_spec()) -> ok | no_return().
assert_valid_type(StorageConfig, UpdateSpec) ->
    HelperConfig = StorageConfig#storage_config.helper_config,
    StorageType = helper_config:get_name(HelperConfig),

    case StorageType == UpdateSpec#storage_update_spec.type of
        true -> ok;
        false -> throw(?ERR_BAD_VALUE_NOT_ALLOWED(?err_ctx(), <<"type">>, [StorageType]))
    end.


%% @private
-spec prepare_new_storage_config(
    storage:id(),
    onedata_storage:update_spec(),
    storage_config:record(),
    od_storage:record()
) ->
    {boolean(), boolean(), storage_config:record()} | no_return().
prepare_new_storage_config(StorageId, UpdateSpec, PrevStorageConfig, PrevOzStorageData) ->
    PrevHelperConfig = PrevStorageConfig#storage_config.helper_config,

    {ReadonlyChanged, NewReadonly} = infer_new_value(
        UpdateSpec#storage_update_spec.readonly,
        PrevOzStorageData#od_storage.readonly
    ),
    {ImportedChanged, NewImported} = infer_new_value(
        UpdateSpec#storage_update_spec.imported,
        PrevOzStorageData#od_storage.imported
    ),

    {HelperConfigChanged, NewHelperConfig} = case helper_config:update(PrevHelperConfig, UpdateSpec) of
        {ok, UpdatedHelperConfig} ->
            storage_crud_utils:verify_configuration(
                StorageId, NewReadonly, NewImported, UpdatedHelperConfig
            ),
            {true, UpdatedHelperConfig};
        {error, no_change} when ReadonlyChanged orelse ImportedChanged ->
            storage_crud_utils:verify_configuration(
                StorageId, NewReadonly, NewImported, PrevHelperConfig
            ),
            {false, PrevHelperConfig};
        {error, no_change} ->
            {false, PrevHelperConfig}
    end,

    PrevLumaConfig = PrevStorageConfig#storage_config.luma_config,
    PrevLumaGeneration = PrevStorageConfig#storage_config.luma_generation,

    {LumaChanged, NewLumaConfig} = case UpdateSpec#storage_update_spec.luma of
        undefined ->
            {false, PrevLumaConfig};
        LumaSpec ->
            LumaDiff = build_luma_diff(LumaSpec),
            case luma_config:update(PrevLumaConfig, LumaDiff) of
                {ok, UpdatedLumaConfig} ->
                    {true, UpdatedLumaConfig};
                {error, no_update} ->
                    {false, PrevLumaConfig};
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
            true -> PrevLumaGeneration + 1;
            false -> PrevLumaGeneration
        end
    }}.


%% @private
-spec infer_new_value(undefined | term(), term()) -> {boolean(), term()}.
infer_new_value(undefined, PrevValue) -> {false, PrevValue};
infer_new_value(NewValue, PrevValue) -> {NewValue /= PrevValue, NewValue}.


%% @private
-spec build_luma_diff(onedata_storage:luma_spec()) -> luma_config:diff().
build_luma_diff(#luma_spec{feed = Feed, url = Url, api_key = ApiKey}) ->
    maps_utils:remove_undefined(#{
        feed => Feed,
        url => Url,
        api_key => ApiKey
    }).


%% @private
-spec update_in_op(storage:id(), storage_config:record(), boolean()) -> ok | {error, term()}.
update_in_op(StorageId, StorageConfig, HelperConfigChanged) ->
    case storage_config:update(StorageId, fun(_) -> {ok, StorageConfig} end) of
        {ok, _} when HelperConfigChanged ->
            on_helper_changed(StorageId);
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec on_helper_changed(storage:id()) -> ok.
on_helper_changed(StorageId) ->
    fslogic_event_emitter:emit_helper_params_changed(StorageId),
    % TODO VFS-12677 consider error handling here and error propagation / rollback
    rtransfer_config:add_storage(StorageId),
    helpers_reload:refresh_helpers_by_storage(StorageId).


%% @private
-spec build_rollback_oz_spec(onedata_storage:update_spec(), od_storage:record()) ->
    onedata_storage:update_spec().
build_rollback_oz_spec(UpdateSpec, PrevOzData) ->
    #storage_update_spec{
        type = UpdateSpec#storage_update_spec.type,
        name = rollback_value(
            UpdateSpec#storage_update_spec.name,
            PrevOzData#od_storage.name
        ),
        readonly = rollback_value(
            UpdateSpec#storage_update_spec.readonly,
            PrevOzData#od_storage.readonly
        ),
        imported = rollback_value(
            UpdateSpec#storage_update_spec.imported,
            PrevOzData#od_storage.imported
        ),
        qos_parameters = rollback_value(
            UpdateSpec#storage_update_spec.qos_parameters,
            PrevOzData#od_storage.qos_parameters
        )
    }.


%% @private
-spec rollback_value(undefined | term(), term()) -> undefined | term().
rollback_value(undefined, _OldValue) -> undefined;
rollback_value(_NewValue, OldValue) -> OldValue.


%% @private
-spec on_qos_change(storage:id()) -> ok.
on_qos_change(StorageId) ->
    {ok, Spaces} = storage_logic:get_spaces(StorageId),

    lists:foreach(fun(SpaceId) ->
        ok = qos_logic:reevaluate_all_impossible_qos_in_space(SpaceId)
    end, Spaces).


%% @private
-spec best_effort_clear_luma(storage:id(), storage_config:record()) -> ok.
best_effort_clear_luma(StorageId, StorageConfig) ->
    LumaGeneration = StorageConfig#storage_config.luma_generation,

    try
        luma_crud_api:clear_db(#document{
            key = StorageId,
            value = StorageConfig
        })
    catch Class:Reason:Stacktrace ->
        ?examine_exception(
            "Failed to clear LUMA DB for generation ~B of storage '~ts' - "
            "stale LUMA entries may remain in the database and require manual cleanup",
            [LumaGeneration, StorageId],
            Class, Reason, Stacktrace
        )
    end,

    ok.


%%%===================================================================
%%% Saga execution
%%%===================================================================


%% @private
-spec execute_saga([saga_step()]) -> ok | {error, term()}.
execute_saga(Steps) ->
    execute_saga(Steps, []).


%% @private
-spec execute_saga([saga_step()], [named_compensation()]) -> ok | {error, term()}.
execute_saga([], _Compensations) ->
    ok;

execute_saga([#saga_step{should_run = false} | Rest], Compensations) ->
    execute_saga(Rest, Compensations);

execute_saga([#saga_step{should_run = true, name = Name} = Step | Rest], Compensations) ->
    ?info("Storage update saga - executing step: ~ts", [Name]),
    case run_saga_action(Name, Step#saga_step.action) of
        ok ->
            NewCompensations = [{Name, Step#saga_step.compensation} | Compensations],
            execute_saga(Rest, NewCompensations);
        {error, _} = Error ->
            run_compensations(Compensations),
            Error
    end.


%% @private
-spec run_saga_action(binary(), fun(() -> ok | {error, term()})) -> ok | {error, term()}.
run_saga_action(Name, Action) ->
    try
        Action()
    catch Class:Reason:Stacktrace ->
        ?examine_exception(
            "Storage update saga step '~ts' failed", [Name],
            Class, Reason, Stacktrace
        )
    end.


%% @private
-spec run_compensations([named_compensation()]) -> ok.
run_compensations([]) ->
    ok;
run_compensations([{Name, Compensation} | Rest]) ->
    ?warning("Storage update saga - compensating step: ~ts", [Name]),
    try
        Compensation()
    catch Class:Reason:Stacktrace ->
        ?examine_exception(
            "Storage update saga - FAILED to compensate step; "
            "manual intervention may be required to restore consistent state",
            Class, Reason, Stacktrace
        )
    end,
    run_compensations(Rest).
