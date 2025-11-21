%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% High-level unified RPC API for storage management used by Onepanel.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_rpc_api).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/storage/common.hrl").

%% API
-export([
    update/2,
    describe/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec update(storage:id(), onedata_storage:update_spec()) -> ok | errors:error().
update(StorageId, UpdateSpec) ->
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


-spec describe(storage:id()) -> {ok, onedata_storage:description()} | errors:error().
describe(StorageId) ->
    try
        do_describe(StorageId)
    catch Class:Reason:Stacktrace ->
        ?examine_exception(
            "Unexpected error when describing storage '~ts'", [StorageId],
            Class, Reason, Stacktrace
        )
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec do_describe(storage:id()) ->
    {ok, onedata_storage:description()} | errors:error().
do_describe(StorageId) ->
    case storage:get(StorageId) of
        {ok, StorageData} ->
            HelperConfig = storage:get_helper_config(StorageData),
            HelperConfigDescription = helper_config:describe(HelperConfig),
            LumaConfig = storage:get_luma_config(StorageData),

            {ok, #storage_description{
                id = StorageId,
                name = storage:fetch_name_of_local_storage(StorageId),
                type = HelperConfigDescription#helper_config_description.type,
                timeout = HelperConfigDescription#helper_config_description.timeout,
                readonly = storage:is_local_storage_readonly(StorageId),
                imported = storage:is_imported(StorageId),
                archive = HelperConfigDescription#helper_config_description.archive,
                luma = #luma_spec{
                    feed = luma_config:get_feed(LumaConfig),
                    url = luma_config:get_url(LumaConfig),
                    api_key = luma_config:get_api_key(LumaConfig)
                },
                qos_parameters = storage:fetch_qos_parameters_of_local_storage(StorageId),
                credentials = HelperConfigDescription#helper_config_description.credentials,
                configuration = HelperConfigDescription#helper_config_description.configuration
            }};

        {error, not_found} ->
            ?ERROR_NOT_FOUND
    end.


% TODO use critical section for update?
%% @private
-spec do_update(storage:id(), onedata_storage:update_spec()) -> ok | errors:error().
do_update(StorageId, UpdateSpec = #storage_update_spec{
    name = MaybeName,
    readonly = MaybeNewReadonly,
    imported = MaybeNewImported,
    luma = MaybeLumaSpec,
    qos_parameters = MaybeQosParams
}) ->
    case storage:get(StorageId) of
        {error, not_found} ->
            ?ERROR_NOT_FOUND;
        {ok, CurrentStorageData} ->
            CurrentHelperConfig = storage:get_helper_config(CurrentStorageData),
            assert_valid_type(CurrentHelperConfig, UpdateSpec),

            CurrentReadonly = storage:is_local_storage_readonly(StorageId),
            NewReadonly = utils:ensure_defined(MaybeNewReadonly, CurrentReadonly),

            CurrentImported = storage:is_imported(StorageId),
            NewImported = utils:ensure_defined(MaybeNewImported, CurrentImported),

            ReadonlyOrImportedChanged = 
                (NewReadonly =/= CurrentReadonly) orelse (NewImported =/= CurrentImported),

            {HelperConfigChanged, NewHelperConfig} = case helper_config:update(CurrentHelperConfig, UpdateSpec) of
                {ok, UpdatedHelperConfig} ->
                    verify_storage_configuration(StorageId, NewReadonly, NewImported, UpdatedHelperConfig),
                    {true, UpdatedHelperConfig};
                {error, no_change} when ReadonlyOrImportedChanged ->
                    verify_storage_configuration(StorageId, NewReadonly, NewImported, CurrentHelperConfig),
                    {false, CurrentHelperConfig};
                {error, no_change} ->
                    {false, CurrentHelperConfig}
            end,

            NewLumaFeed = get_luma_feed(MaybeLumaSpec, CurrentStorageData),
            IgnoreReadWriteTest = NewReadonly orelse
                (NewImported andalso storage:supports_any_space(StorageId)),
            run_diagnostics(NewHelperConfig, NewLumaFeed, not IgnoreReadWriteTest),

            % @TODO VFS-5513 Modify everything in a single datastore operation
            % TODO VFS-6951 refactor storage configuration API
            lists:foreach(fun
                ({true, UpdateFun}) ->
                    case UpdateFun() of
                        ok -> ok;
                        {error, no_changes} -> ok;
                        {error, _} = Error -> throw(Error)
                    end;
                (_) ->
                    ok
            end, [
                % TODO do all those calls need to be independent? Can't there be only 2 calls: to oz and sotrage_config?
                {MaybeQosParams =/= undefined, fun() ->
                    storage:set_qos_parameters(StorageId, MaybeQosParams)
                end},
                {MaybeName =/= undefined, fun() ->
                    storage:update_name(StorageId, MaybeName)
                end},
                {HelperConfigChanged, fun() ->
                    storage:update_helper_config(StorageId, fun(_) ->
                        {ok, NewHelperConfig}
                    end)
                end},
                {MaybeLumaSpec =/= undefined, fun() ->
                    LumaDiff = build_luma_diff(MaybeLumaSpec),
                    storage:update_luma_config(StorageId, LumaDiff)
                end},
                {ReadonlyOrImportedChanged, fun() ->
                    storage:update_readonly_and_imported(StorageId, NewReadonly, NewImported)
                end}
            ])
    end.


%% @private
-spec assert_valid_type(helper_config:t(), onedata_storage:update_spec()) -> ok | no_return().
assert_valid_type(HelperConfig, UpdateSpec) ->
    StorageType = helper_config:get_name(HelperConfig),

    case StorageType == UpdateSpec#storage_update_spec.type of
        true -> ok;
        false -> throw(?ERR_BAD_VALUE_NOT_ALLOWED(?err_ctx(), <<"type">>, [StorageType]))
    end.


%% @private
-spec get_luma_feed(undefined | onedata_storage:luma_spec(), storage:data()) -> luma:feed().
get_luma_feed(undefined, CurrentStorageData) ->
    LumaConfig = storage:get_luma_config(CurrentStorageData),
    luma_config:get_feed(LumaConfig);
get_luma_feed(#luma_spec{feed = Feed}, _CurrentStorageData) ->
    Feed.


%% @private
-spec build_luma_diff(onedata_storage:luma_spec()) -> luma_config:diff().
build_luma_diff(#luma_spec{feed = Feed, url = Url, api_key = ApiKey}) ->
    maps_utils:remove_undefined(#{
        feed => Feed,
        url => Url,
        api_key => ApiKey
    }).


%% @private
-spec verify_storage_configuration(storage:id() | storage:name(), boolean(), boolean(), helper_config:t()) ->
    ok | no_return().
verify_storage_configuration(IdOrName, Readonly, Imported, HelperConfig) ->
    Config = #{
        readonly => Readonly,
        importedStorage => Imported
    },
    case storage:verify_configuration(IdOrName, Config, HelperConfig) of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec run_diagnostics(helper_config:t(), luma:feed(), boolean()) -> ok | no_return().
run_diagnostics(HelperConfig, LumaFeed, PerformReadWriteTest) ->
    Opts = #{read_write_test => PerformReadWriteTest},

    case storage_detector:run_diagnostics(all_nodes, HelperConfig, LumaFeed, Opts) of
        ok ->
            ok;
        {{error, _} = Error, Details} ->
            ?error("Storage diagnostics failed: ~tp, details: ~tp", [Error, Details]),
            throw(Error)
    end.
