%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs directory-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_dirs).

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([mkdir/2, mkdir/3, mkdir/4, ls/4, get_child_attr/3, get_children_count/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(SessId :: session:id(), Path :: file_meta:path()) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mkdir(SessId, Path) ->
    mkdir(SessId, Path, undefined).

-spec mkdir(SessId :: session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mkdir(SessId, Path, Mode) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    lfm_utils:call_fslogic(SessId, fuse_request, #resolve_guid{path = ParentPath},
        fun(#uuid{uuid = ParentGuid}) ->
            mkdir(SessId, ParentGuid, Name, Mode)
        end).

-spec mkdir(SessId :: session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mkdir(SessId, ParentGuid, Name, undefined) ->
    {ok, Mode} = application:get_env(?APP_NAME, default_dir_mode),
    mkdir(SessId, ParentGuid, Name, Mode);
mkdir(SessId, ParentGuid, Name, Mode) ->
    lfm_utils:call_fslogic(SessId, file_request, ParentGuid,
        #create_dir{name = Name, mode = Mode},
        fun(#dir{uuid = DirGuid}) ->
            {ok, DirGuid}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%% @end
%%--------------------------------------------------------------------
-spec ls(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(), Limit :: integer()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}]} | logical_file_manager:error_reply().
ls(SessId, FileKey, Offset, Limit) ->
    {guid, FileGuid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    lfm_utils:call_fslogic(SessId, file_request, FileGuid,
        #get_file_children{offset = Offset, size = Limit},
        fun(#file_children{child_links = List}) ->
            {ok, [{Guid_, FileName} || #child_link{uuid = Guid_, name = FileName} <- List]}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Gets attribute of a child with given name.
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    ChildName :: file_meta:name()) ->
    {ok, #file_attr{}} | logical_file_manager:error_reply().
get_child_attr(SessId, ParentGuid, ChildName)  ->
    lfm_utils:call_fslogic(SessId, file_request, ParentGuid,
        #get_child_attr{name = ChildName},
        fun(Attrs) ->
            {ok, Attrs}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Returns number of children of a directory.
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(session:id(), FileKey :: fslogic_worker:file_guid_or_path())
        -> {ok, integer()} | logical_file_manager:error_reply().
get_children_count(SessId, FileKey) ->
    {guid, FileGuid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    case count_children(SessId, FileGuid, 0) of
        {error, Err} -> {error, Err};
        ChildrenNum -> {ok, ChildrenNum}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Counts all children of a directory, by listing them in chunks as long
%% as possible
%% @end
%%--------------------------------------------------------------------
-spec count_children(SessId :: session:id(), FileGuid :: fslogic_worker:file_guid(),
    Acc :: non_neg_integer()) ->
    non_neg_integer() | logical_file_manager:error_reply().
count_children(SessId, FileGuid, Acc) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case ls(SessId, {guid, FileGuid}, Acc, Chunk) of
        {ok, List} -> case length(List) of
                          Chunk -> count_children(SessId, FileGuid, Acc + Chunk);
                          N -> Acc + N
                      end;
        {error, Err} -> {error, Err}
    end.

