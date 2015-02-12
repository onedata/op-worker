%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs file-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files).

-include("types.hrl").
-include("errors.hrl").

%% API
%% Functions operating on directories or files
-export([exists/1, mv/2, cp/2, rm/1]).
%% Functions operating on files
-export([create/1, open/2, write/3, read/3, truncate/2, get_block_map/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file or directory exists.
%%
%% @end
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) -> {ok, boolean()} | error_reply().
exists(FileKey) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
mv(FileKeyFrom, PathTo) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%%
%% @end
%%--------------------------------------------------------------------
-spec cp(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
cp(PathFrom, PathTo) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec rm(FileKey :: file_key()) -> ok | error_reply().
rm(FileKey) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(Path :: file_path()) -> {ok, file_id()} | error_reply().
create(Path) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%%
%% @end
%%--------------------------------------------------------------------
-spec open(FileKey :: file_id_or_path(), OpenType :: open_type()) -> {ok, file_handle()} | error_reply().
open(FileKey, OpenType) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) -> {ok, integer()} | error_reply().
write(FileHandle, Offset, Buffer) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) -> {ok, binary()} | error_reply().
read(FileHandle, Offset, MaxSize) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(FileKey :: file_key(), Size :: integer()) -> ok | error_reply().
truncate(FileKey, Size) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_block_map(FileKey :: file_key()) -> {ok, [block_range()]} | error_reply().
get_block_map(FileKey) ->
    error(not_implemented).
