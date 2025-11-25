%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles storage update.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_updater).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/storage/common.hrl").

%% API
-export([
    update/2
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
                    storage_crud_utils:verify_configuration(StorageId, NewReadonly, NewImported, UpdatedHelperConfig),
                    {true, UpdatedHelperConfig};
                {error, no_change} when ReadonlyOrImportedChanged ->
                    storage_crud_utils:verify_configuration(StorageId, NewReadonly, NewImported, CurrentHelperConfig),
                    {false, CurrentHelperConfig};
                {error, no_change} ->
                    {false, CurrentHelperConfig}
            end,

            NewLumaFeed = get_luma_feed(MaybeLumaSpec, CurrentStorageData),
            IgnoreReadWriteTest = NewReadonly orelse
                (NewImported andalso storage:supports_any_space(StorageId)),
            storage_crud_utils:run_diagnostics(NewHelperConfig, NewLumaFeed, not IgnoreReadWriteTest),

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
                    update_name(StorageId, MaybeName)
                end},
                {HelperConfigChanged, fun() ->
                    storage:update_helper_config(StorageId, fun(_) ->
                        {ok, NewHelperConfig}
                    end)
                end},
                {MaybeLumaSpec =/= undefined, fun() ->
                    LumaDiff = build_luma_diff(MaybeLumaSpec),
                    update_luma_config(StorageId, LumaDiff)
                end},
                {ReadonlyOrImportedChanged, fun() ->
                    update_readonly_and_imported(StorageId, NewReadonly, NewImported)
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
-spec update_name(storage:id(), NewName :: storage:name()) -> ok.
update_name(StorageId, NewName) ->
    storage_logic:update_name(StorageId, NewName).


%% @private
-spec update_luma_config(storage:id(), Diff :: luma_config:diff()) ->
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


%% @private
-spec update_readonly_and_imported(storage:id(), storage:readonly(), storage:imported()) -> ok | {error, term()}.
update_readonly_and_imported(StorageId, Readonly, Imported) ->
    storage_logic:update_readonly_and_imported(StorageId, Readonly, Imported).
