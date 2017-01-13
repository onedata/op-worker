%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module is responsible for selection of storage and helper
%%% used during filesystem operations.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_storage).
-author("Rafal Slota").

%% API
-export([select_storage/1, select_helper/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns first configured storage for given space.
%% @end
%%--------------------------------------------------------------------
-spec select_storage(od_space:id()) ->
    {ok, storage:doc()} | {error, Reason :: term()}.
select_storage(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, Doc} ->
            case space_storage:get_storage_ids(Doc) of
                [] -> {error, {no_storage_avaliable, SpaceId}};
                [StorageId | _] -> storage:get(StorageId)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns first configured helper for given storage.
%% @end
%%--------------------------------------------------------------------
-spec select_helper(storage:doc()) ->
    {ok, storage:helper()} | {error, Reason :: term()}.
select_helper(StorageDoc) ->
    case storage:get_helpers(StorageDoc) of
        [] -> {error, {no_helper_available, storage:get_id(StorageDoc)}};
        [Helper | _] -> {ok, Helper}
    end.
