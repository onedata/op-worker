%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic generic (both for regular and special files) request handlers.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_generic).
-author("Rafal Slota").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod/3, get_file_attr/2, delete_file/2, rename_file/3, update_times/5]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Changes file's access times.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec update_times(fslogic_worker:ctx(), File :: fslogic_worker:file(),
                   ATime :: file_meta:time(), MTime :: file_meta:time(), CTime :: file_meta:time()) -> #fuse_response{} | no_return().
-check_permissions({none, 2}).
update_times(#fslogic_ctx{session_id = SessId}, FileEntry, ATime, MTime, CTime) ->
    UpdateMap = #{atime => ATime, mtime => MTime, ctime => CTime},
    UpdateMap1 = maps:from_list([{Key, Value} || {Key, Value} <- maps:to_list(UpdateMap), is_integer(Value)]),
    {ok, _} = file_meta:update(FileEntry, UpdateMap1),

    %% @todo: replace with events
    spawn(fun() -> fslogic_notify:attributes(FileEntry, [SessId]) end),

    #fuse_response{status = #status{code = ?OK}}.



%%--------------------------------------------------------------------
%% @doc Changes file permissions.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec chmod(fslogic_worker:ctx(), File :: fslogic_worker:file(), Perms :: fslogic_worker:posix_permissions()) ->
                   #fuse_response{} | no_return().
-check_permissions({owner, 2}).
chmod(_CTX, FileEntry, Mode) ->

    case file_meta:get(FileEntry) of
        {ok, #document{value = #file_meta{type = ?REGULAR_FILE_TYPE}}} ->
            Results = lists:map(
                        fun({SID, FID} = Loc) ->
                                {ok, Storage} = storage:get(SID),
                                {Loc, storage_file_manager:chmod(Storage, FID, Mode)}
                        end, fslogic_utils:get_local_storage_file_locations(FileEntry)),

            case [{Loc, Error} || {Loc, {error, _} = Error} <- Results] of
                [] -> ok;
                Errors ->
                    [?error("Unable to chmod [FileId: ~p] [StoragId: ~p] to mode ~p due to: ~p", [FID, SID, Mode, Reason])
                     || {{SID, FID}, {error, Reason}} <- Errors],
                    throw(?EAGAIN)
            end;
        _ -> ok
    end,

    {ok, _} = file_meta:update(FileEntry, #{mode => Mode}),

    %% @todo: replace with events
    spawn(fun() -> fslogic_notify:attributes(FileEntry, []) end),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Changes file owner.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec chown(fslogic_worker:ctx(), File :: fslogic_worker:file(), UserId :: onedata_user:id()) ->
                   #fuse_response{} | no_return().
-check_permissions(root).
chown(_, _File, _UserId) ->
    #fuse_response{status = #status{code = ?ENOTSUP}}.


%%--------------------------------------------------------------------
%% @doc Gets file's attributes.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(Ctx :: fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
                           FuseResponse :: #fuse_response{} | no_return().
-check_permissions({none, 2}).
get_file_attr(#fslogic_ctx{session_id = SessId}, File) ->
    ?info("Get attr for file entry: ~p", [File]),
    case file_meta:get(File) of
        {ok, #document{key = UUID, value = #file_meta{
                                              type = Type, mode = Mode, atime = ATime, mtime = MTime,
                                              ctime = CTime, uid = UID, name = Name}}} ->
            Size = fslogic_blocks:get_file_size(File),

            ok = file_watcher:insert_attr_watcher(UUID, SessId),

            #fuse_response{status = #status{code = ?OK}, fuse_response =
                               #file_attr{
                                  uuid = UUID, type = Type, mode = Mode, atime = ATime, mtime = MTime,
                                  ctime = CTime, uid = fslogic_utils:gen_storage_uid(UID), size = Size, name = Name
                                 }};
        {error, {not_found, _}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.


%%--------------------------------------------------------------------
%% @doc Deletes file.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec delete_file(fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
                         FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{write, {parent, 2}}, {owner_if_parent_sticky, 2}]).
delete_file(_, File) ->
    {ok, #document{value = #file_meta{type = Type}} = FileDoc} = file_meta:get(File),
    {ok, FileChildren} =
        case Type of
            ?DIRECTORY_TYPE ->
                file_meta:list_children(FileDoc, 0, 1);
            ?REGULAR_FILE_TYPE ->
                #document{value = #file_location{} = Location} = fslogic_utils:get_local_file_location(File),
                ToDelete = fslogic_utils:get_local_storage_file_locations(Location),
                Results =
                    lists:map( %% @todo: run this via task manager
                      fun({StorageId, FileId}) ->
                              case storage:get(StorageId) of
                                  {ok, Storage} ->
                                      case storage_file_manager:unlink(Storage, FileId) of
                                          ok -> ok;
                                          {error, Reason1} ->
                                              {{StorageId, FileId}, {error, Reason1}}
                                      end ;
                                  {error, Reason2} ->
                                      {{StorageId, FileId}, {error, Reason2}}
                              end
                      end, ToDelete),
                case Results -- [ok] of
                    [] -> ok;
                    Errors ->
                        lists:foreach(
                          fun({{SID0, FID0}, {error, Reason0}}) ->
                                  ?error("Cannot unlink file ~p from storage ~p due to: ~p", [FID0, SID0, Reason0])
                          end, Errors)
                end,
                {ok, []};
            _ ->
                {ok, []}
        end,
    case length(FileChildren) of
        0 ->
            ok = file_meta:delete(FileDoc),
            {ok, ParentDoc} = file_meta:get_parent(FileDoc),
            {ok, _} = file_meta:update(ParentDoc, #{mtime => utils:time()}),
            #fuse_response{status = #status{code = ?OK}};
        _ ->
            #fuse_response{status = #status{code = ?ENOTEMPTY}}
    end.


%%--------------------------------------------------------------------
%% @doc Renames file.
%% For best performance use following arg types: path -> uuid -> document
%% @end
%%--------------------------------------------------------------------
-spec rename_file(fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(), TargetPath :: file_meta:path()) ->
                         #fuse_response{} | no_return().
-check_permissions([{write, {parent, 2}}, {write, 2}, {write, {parent, {path, 3}}}]).
rename_file(_CTX, SourceEntry, TargetPath) ->
    ?debug("Renaming file ~p to ~p...", [SourceEntry, TargetPath]),
    case file_meta:exists({path, TargetPath}) of
        true ->
            #fuse_response{status = #status{code = ?EEXIST}};
        false ->
            ok = file_meta:rename(SourceEntry, {path, TargetPath}),
            #fuse_response{status = #status{code = ?OK}}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------