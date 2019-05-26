%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Filename mapping utils
%%% @end
%%%-------------------------------------------------------------------
-module(filename_mapping).
-author("Jakub Kudzia").

%%%===================================================================
%%% Exports
%%%===================================================================

%% API
-export([to_storage_path/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Convert given logical path to storage path
%% @end
%%--------------------------------------------------------------------
-spec to_storage_path(od_space:id(), storage:id(), file_meta:path()) ->
    file_meta:path().
to_storage_path(SpaceId, StorageId, FilePath) ->
    MountedInRoot = space_storage:get_mounted_in_root(SpaceId),
    case lists:member(StorageId, MountedInRoot) of
        true ->
            filter_space_id(SpaceId, FilePath);
        false ->
            FilePath
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if space is mounted in root on given storage.
%% @end
%%--------------------------------------------------------------------
-spec filter_space_id(od_space:id(), file_meta:path()) -> file_meta:path().
filter_space_id(SpaceId, FilePath) ->
    case fslogic_path:split(FilePath) of
        [Sep, SpaceId | Path] ->
            fslogic_path:join([Sep | Path]);
        _ ->
            FilePath
    end.
