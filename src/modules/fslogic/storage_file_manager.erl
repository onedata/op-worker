%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module provides high level file system operations that
%%% operates directly on storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_manager).

-include("types.hrl").
-include("modules/datastore/datastore.hrl").
-include_lib("storage_file_manager_errors.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% File handle used by the module
-record(sfm_handle, {
    helper_handle :: helpers:handle(),
    file :: helpers:file(),
    session_id = session:id(),
    space_uuid = file_meta:uuid(),
    storage :: datastore:document()
}).

-export([new_handle/4]).
-export([mkdir/2, mkdir/3, mv/2, chmod/2, chown/3, link/2]).
-export([stat/1, read/3, write/3, create/2, create/3, open/2, truncate/2, unlink/1]).

-type handle() :: #sfm_handle{}.

-export_type([handle/0]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% @end
%%--------------------------------------------------------------------
-spec new_handle(SessionId :: session:id(), SpaceUUID :: file_meta:uuid(), Storage :: datastore:document(), FileId :: helpers:file()) ->
    handle().
new_handle(SessionId, SpaceUUID, Storage, FileId) ->
    #sfm_handle{
        session_id = SessionId,
        space_uuid = SpaceUUID,
        file = FileId,
        storage = Storage
    }.


%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% @end
%%--------------------------------------------------------------------
-spec open(handle(), OpenMode :: helpers:open_mode()) ->
    {ok, handle()} | error_reply().
open(#sfm_handle{storage = Storage, file = FileId, session_id = SessionId, space_uuid = SpaceUUID} = SFMHandle, OpenMode) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    case helpers:open(HelperHandle, FileId, OpenMode) of
        {ok, _} ->
            {ok, SFMHandle#sfm_handle{helper_handle = HelperHandle}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), Mode :: non_neg_integer()) ->
    ok | error_reply().
mkdir(Handle, Mode) ->
    mkdir(Handle, Mode, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage. Recursive states whether parent directories shall be also created.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | error_reply().
mkdir(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId} = SFMHandle, Mode, Recursive) ->
    Noop = fun(_) -> ok end,
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    case helpers:mkdir(HelperHandle, FileId, Mode) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(FileId),
            case Tokens of
                [_] -> ok;
                [_ | _]  ->
                    LeafLess = fslogic_path:dirname(Tokens),
                    ok = mkdir(SFMHandle#sfm_handle{file = LeafLess}, ?AUTO_CREATED_PARENT_DIR_MODE, true)
            end,
            R = case mkdir(SFMHandle, Mode, false) of
                ok ->
                    chmod(SFMHandle, Mode); %% @todo: find out why umask(0) in helpers_nif.cc doesn't work
                E -> E
            end,
            Noop(HelperHandle), %% @todo: check why NIF crashes when this term is destroyed before recursive call
            R;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileHandleFrom :: handle(), PathOnStorageTo :: file_path()) -> ok | error_reply().
mv(_FileHandleFrom, _PathOnStorageTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), NewMode :: perms_octal()) -> ok | error_reply().
chmod(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}, Mode) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    helpers:chmod(HelperHandle, FileId, Mode).


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: handle(), User :: user_id(), Group :: group_id()) -> ok | error_reply().
chown(_FileHandle, _User, _Group) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec link(Path :: binary(), TargetFileHandle :: handle()) -> {ok, file_uuid()} | error_reply().
link(_Path, _TargetFileHandle) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: handle()) -> {ok, undefined} | error_reply().
stat(_FileHandle) ->
    {ok, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file on storage. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: handle(), Offset :: non_neg_integer(), Buffer :: binary()) -> {ok, non_neg_integer()} | error_reply().
write(#sfm_handle{helper_handle = HelperHandle, file = File}, Offset, Buffer) ->
    helpers:write(HelperHandle, File, Offset, Buffer).


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: handle(), Offset :: non_neg_integer(), MaxSize :: non_neg_integer()) -> {ok, binary()} | error_reply().
read(#sfm_handle{helper_handle = HelperHandle, file = File}, Offset, MaxSize) ->
    helpers:read(HelperHandle, File, Offset, MaxSize).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(handle(), Mode :: non_neg_integer()) ->
    ok | error_reply().
create(Handle, Mode) ->
    create(Handle, Mode, false).

-spec create(handle(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | error_reply().
create(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId} = SFMHandle, Mode, Recursive) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    case helpers:mknod(HelperHandle, FileId, Mode, reg) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(FileId),
            LeafLess = fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1)),
            ok =
                case mkdir(SFMHandle#sfm_handle{file = LeafLess}, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
                    ok -> ok;
                    {error, eexist} -> ok;
                    E0 -> E0
                end,
            create(SFMHandle, Mode, false);
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(handle(), Size :: integer()) -> ok | error_reply().
truncate(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}, Size) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    helpers:truncate(HelperHandle, FileId, Size).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle()) -> ok | error_reply().
unlink(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    helpers:unlink(HelperHandle, FileId).