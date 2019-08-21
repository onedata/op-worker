%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Main API for storage_sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").

% todo etsy -> drzewa
% todo persystencja
% todo dodać canelownaie

%% API
-export([
    enable_import/2, enable_import/3,
    configure_storage_update/3, configure_storage_update/4,
    disable_storage_update/1,
    get_import_details/2, get_update_details/2, disable_storage_import/2]).

-spec get_import_details(od_space:id(), storage:id()) -> ok.
get_import_details(SpaceId, StorageId) ->
    space_strategies:get_import_details(SpaceId, StorageId).

-spec enable_import(od_space:id(), storage_sync_traverse:arguments()) ->
    ok | {error, term()}.
enable_import(SpaceId, Args) ->
    StorageId = get_supporting_storage(SpaceId),
    enable_import(SpaceId, StorageId, Args).

-spec enable_import(od_space:id(), storage:id(), storage_sync_traverse:arguments()) ->
    ok | {error, term()}.
enable_import(SpaceId, StorageId, Config) ->
    file_meta:make_space_exist(SpaceId),
    space_strategies:enable_import(SpaceId, StorageId, Config).

disable_storage_import(SpaceId, StorageId) ->
    space_strategies:disable_import(SpaceId, StorageId).

-spec get_update_details(od_space:id(), storage:id()) -> ok.
get_update_details(SpaceId, StorageId) ->
    space_strategies:get_update_details(SpaceId, StorageId).

-spec configure_storage_update(od_space:id(), boolean(),
    storage_sync_traverse:arguments()) -> ok | {error, term()}.
configure_storage_update(SpaceId, Enabled, Config) ->
    StorageId = get_supporting_storage(SpaceId),
    configure_storage_update(SpaceId, StorageId, Enabled, Config).

-spec configure_storage_update(od_space:id(), storage:id(), boolean(),
    storage_sync_traverse:arguments()) -> ok | {error, term()}.
configure_storage_update(SpaceId, StorageId, Enabled, Config) ->
    file_meta:make_space_exist(SpaceId),
    {ImportEnabled, _} = get_import_details(SpaceId, StorageId),
    case {Enabled, ImportEnabled} of
        {true, false} ->
            {error, import_disabled};
        _ ->
            case maps:get(delete_enable, Config, undefined) of
                true ->
                    Helper = storage:get_helper(StorageId),
                    HelperName = helper:get_name(Helper),
                    case HelperName =:= ?S3_HELPER_NAME of
                        true ->
                            {error, 'Detecting deletions not implemented on s3'};
                        false ->
                            space_strategies:configure_update(SpaceId, StorageId, Enabled, Config)
                    end;
                _ ->
                    space_strategies:configure_update(SpaceId, StorageId, Enabled, Config)
            end
    end.

-spec disable_storage_update(od_space:id()) -> ok.
disable_storage_update(SpaceId) ->
    StorageId = get_supporting_storage(SpaceId),
    disable_storage_update(SpaceId, StorageId).

-spec disable_storage_update(od_space:id(), storage:id()) -> ok.
disable_storage_update(SpaceId, StorageId) ->
    space_strategies:disable_update(SpaceId, StorageId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_supporting_storage(od_space:id()) -> storage:id().
get_supporting_storage(SpaceId) ->
    {ok, #document{value=#space_storage{storage_ids=StorageIds}}} =
        space_storage:get(SpaceId),
    hd(StorageIds).
