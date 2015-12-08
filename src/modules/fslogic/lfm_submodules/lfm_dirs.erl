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

-include("types.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([mkdir/3, ls/4, get_children_count/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec mkdir(fslogic_worker:ctx(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
    ok | error_reply().
mkdir(#fslogic_ctx{session_id = SessId} = _CTX, Path, Mode) ->
    {Name, ParentPath} = fslogic_path:basename_and_parent(Path),
    case file_meta:resolve_path(ParentPath) of
    {ok, {#document{key = ParentUUID}, _}} ->
        lfm_utils:call_fslogic(SessId, #create_dir{
            parent_uuid = ParentUUID, name = Name, mode = Mode
            }, fun(_) -> ok end
        );
    {error, Error} -> {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%%
%% @end
%%--------------------------------------------------------------------
-spec ls(SessId :: session:id(), FileKey :: {uuid, file_uuid()}, Limit :: integer(), Offset :: integer()) ->
    {ok, [{file_uuid(), file_name()}]} | error_reply().
ls(SessId, {uuid, UUID}, Limit, Offset) ->
    lfm_utils:call_fslogic(SessId,
        #get_file_children{uuid=UUID, offset=Offset, size=Limit},
        fun({file_children, List}) ->
            {ok, [{UUID, FileName} || {_, UUID, FileName} <- List]}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns number of children of a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(SessId :: session:id(), FileKey :: {uuid, file_uuid()})
        -> {ok, integer()} | error_reply().
get_children_count(SessId, {uuid, UUID}) ->
    case count_children(SessId, UUID, 0) of
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
-spec count_children(SessId :: session:id(), UUID :: file_uuid(),
    Acc :: non_neg_integer()) -> non_neg_integer() | error_reply().
count_children(SessId, UUID, Acc) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case ls(SessId, {uuid, UUID}, Chunk, Acc) of
        {ok, List} -> case length(List) of
                          Chunk -> count_children(SessId, UUID, Acc + Chunk);
                          N -> Acc + N
                      end;
        {error, Err} -> {error, Err}
    end.

