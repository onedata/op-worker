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
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([mkdir/3, ls/4, get_children_count/1]).

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
    {ok, {#document{key = ParentUUID}, _}} = file_meta:resolve_path(ParentPath),
    lfm_utils:call_fslogic(SessId, #create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode},
        fun(_) -> ok end).


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
-spec get_children_count(FileKey :: file_id_or_path()) -> {ok, integer()} | error_reply().
get_children_count(_FileKey) ->
    {ok, 0}.