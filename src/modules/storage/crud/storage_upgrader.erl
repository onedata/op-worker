%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles storage model upgrade.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_upgrader).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([upgrade_after_swift_version_update_to_v3/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec upgrade_after_swift_version_update_to_v3() -> ok.
upgrade_after_swift_version_update_to_v3() ->
    {ok, StorageList} = storage:get_all(),

    lists:foreach(fun(StorageData) ->
        case storage:get_helper_name(StorageData) of
            ?SWIFT_HELPER_NAME ->
                StorageId = storage:get_id(StorageData),
                StorageName = storage:fetch_name_of_local_storage(StorageId),

                ?info("Upgrading swift storage '~ts' (~ts)...", [StorageName, StorageId]),

                ok = upgrade_swift_helper_after_swift_version_update_to_v3(StorageId),
                % Existing luma entries will not work as they lack necessary projectName
                ok = luma_crud_api:clear_db(StorageId),

                ?info("Successfully upgraded swift storage '~ts' (~ts)", [StorageName, StorageId]);
            _ ->
                ok
        end
    end, StorageList).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates helper to reflect changes made in feature/VFS-12688-try-to-update-swift-to-v3
%% (tenantName is moved from helper args to admin ctx as projectName)
%% @end
%%--------------------------------------------------------------------
-spec upgrade_swift_helper_after_swift_version_update_to_v3(storage:id()) ->
    ok | {error, term()}.
upgrade_swift_helper_after_swift_version_update_to_v3(StorageId) ->
    storage:update_helper_config(StorageId, fun(HelperConfig = #helper_config{
        args = Args,
        admin_ctx = AdminCtx
    }) ->
        case maps:take(<<"tenantName">>, Args) of
            {ProjectName, NewArgs} ->
                {ok, HelperConfig#helper_config{
                    args = NewArgs,
                    admin_ctx = AdminCtx#{<<"projectName">> => ProjectName}
                }};
            error ->
                % ensure update is idempotent
                {ok, HelperConfig}
        end
    end).
