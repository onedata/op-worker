%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs directory-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_dirs).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([mkdir/2, mkdir/3, ls/4, get_children_count/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec mkdir(SessId :: session:id(), Path :: file_meta:path()) ->
    {ok, DirUUID :: file_meta:uuid()} | logical_file_manager:error_reply().
mkdir(SessId, Path) ->
    {ok, Mode} = application:get_env(?APP_NAME, default_dir_mode),
    mkdir(SessId, Path, Mode).

-spec mkdir(SessId :: session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions()) ->
    {ok, DirUUID :: file_meta:uuid()} | logical_file_manager:error_reply().
mkdir(SessId, Path, Mode) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    lfm_utils:call_fslogic(SessId, #get_file_attr{entry = {path, ParentPath}}, fun
        (#file_attr{uuid = ParentGUID}) ->
            lfm_utils:call_fslogic(SessId,
                #create_dir{parent_uuid = ParentGUID, name = Name, mode = Mode},
                fun(#dir{uuid = DirUUID}) ->
                    {ok, DirUUID}
                end)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%%
%% @end
%%--------------------------------------------------------------------
-spec ls(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(), Limit :: integer()) ->
    {ok, [{file_meta:uuid(), file_meta:name()}]} | logical_file_manager:error_reply().
ls(SessId, FileKey, Offset, Limit) ->
    CTX = fslogic_context:new(SessId),
    {guid, FileGUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId,
        #get_file_children{uuid = FileGUID, offset = Offset, size = Limit},
        fun({file_children, List}) ->
            {ok, [{UUID_, FileName} || {_, UUID_, FileName} <- List]}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns number of children of a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(session:id(), FileKey :: fslogic_worker:file_guid_or_path())
        -> {ok, integer()} | logical_file_manager:error_reply().
get_children_count(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {guid, FileGUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    case count_children(SessId, FileGUID, 0) of
        {error, Err} -> {error, Err};
        ChildrenNum -> {ok, ChildrenNum}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Counts all children of a directory, by listing them in chunks as long
%% as possible
%% @end
%%--------------------------------------------------------------------
-spec count_children(SessId :: session:id(), FileGUID :: fslogic_worker:file_guid(),
    Acc :: non_neg_integer()) ->
    non_neg_integer() | logical_file_manager:error_reply().
count_children(SessId, FileGUID, Acc) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case ls(SessId, {guid, FileGUID}, Acc, Chunk) of
        {ok, List} -> case length(List) of
                          Chunk -> count_children(SessId, FileGUID, Acc + Chunk);
                          N -> Acc + N
                      end;
        {error, Err} -> {error, Err}
    end.

