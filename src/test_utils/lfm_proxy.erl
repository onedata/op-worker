%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Proxy for logical files manager operations
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_proxy).
-author("Tomasz Lichon").

-include_lib("common_test/include/ct.hrl").
-include("modules/fslogic/lfm_internal.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([init/1, teardown/1, stat/3, truncate/4, create/4, unlink/3, open/4, close/2,
    read/4, write/4, mkdir/3, mkdir/4, mv/4, ls/5, set_perms/4, update_times/6,
    get_xattr/4, set_xattr/4, remove_xattr/4, list_xattr/3, get_acl/3, set_acl/4,
    write_and_check/4, get_transfer_encoding/3, set_transfer_encoding/4,
    get_cdmi_completion_status/3, set_cdmi_completion_status/4, get_mimetype/3,
    set_mimetype/4, fsync/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(Config :: list()) -> list().
init(Config) ->
    Host = self(),
    Servers = lists:map(
        fun(W) ->
            spawn_link(W,
                fun() ->
                    lfm_handles = ets:new(lfm_handles, [public, set, named_table]),
                    Host ! {self(), done},
                    receive
                        exit -> ok
                    end
                end)
        end, lists:usort(?config(op_worker_nodes, Config))),

    lists:foreach(
        fun(Server) ->
            receive
                {Server, done} -> ok
            after timer:seconds(5) ->
                error("Cannot setup lfm_handles ETS")
            end
        end, Servers),

    [{servers, Servers} | Config].

-spec teardown(Config :: list()) -> ok.
teardown(Config) ->
    lists:foreach(
        fun(Pid) ->
            Pid ! exit
        end, ?config(servers, Config)).

-spec stat(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid()) ->
    {ok, lfm_attrs:file_attributes()} | logical_file_manager:error_reply().
stat(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:stat(SessId, uuid_to_guid(Worker, FileKey)),
            Host ! {self(), Result}
        end).

-spec truncate(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid(), non_neg_integer()) ->
    term().
truncate(Worker, SessId, FileKey, Size) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:truncate(SessId, uuid_to_guid(Worker, FileKey), Size),
            Host ! {self(), Result}
        end).

-spec create(node(), session:id(), file_meta:path(), file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
create(Worker, SessId, FilePath, Mode) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:create(SessId, FilePath, Mode),
            Host ! {self(), Result}
        end).

-spec unlink(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    ok | logical_file_manager:error_reply().
unlink(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:unlink(SessId, uuid_to_guid(Worker, FileKey)),
            Host ! {self(), Result}
        end).

-spec open(node(), session:id(), FileKey :: fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(),
    OpenType :: helpers:open_mode()) ->
    {ok, logical_file_manager:handle()} | logical_file_manager:error_reply().
open(Worker, SessId, FileKey, OpenMode) ->
    exec(Worker,
        fun(Host) ->
            Result =
                case logical_file_manager:open(SessId, uuid_to_guid(Worker, FileKey), OpenMode) of
                    {ok, Handle} ->
                        TestHandle = crypto:rand_bytes(10),
                        ets:insert(lfm_handles, {TestHandle, Handle}),
                        {ok, TestHandle};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

-spec close(node(), logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
close(Worker, TestHandle) ->
    exec(Worker,
        fun(Host) ->
            ets:delete(lfm_handles, TestHandle),
            Host ! {self(), ok}
        end).

-spec read(node(), logical_file_manager:handle(), integer(), integer()) ->
    {ok, binary()} | logical_file_manager:error_reply().
read(Worker, TestHandle, Offset, Size) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Result =
                case logical_file_manager:read(Handle, Offset, Size) of
                    {ok, NewHandle, Res}  ->
                        ets:insert(lfm_handles, {TestHandle, NewHandle}),
                        {ok, Res};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

-spec write(node(), logical_file_manager:handle(), integer(), binary()) ->
    {ok, integer()} | logical_file_manager:error_reply().
write(Worker, TestHandle, Offset, Bytes) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Result =
                case logical_file_manager:write(Handle, Offset, Bytes) of
                    {ok, NewHandle, Res}  ->
                        ets:insert(lfm_handles, {TestHandle, NewHandle}),
                        {ok, Res};
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

-spec write_and_check(node(), logical_file_manager:handle(), integer(), binary()) ->
    {ok, integer(), StatAns} | logical_file_manager:error_reply() when
    StatAns :: {ok, lfm_attrs:file_attributes()} | logical_file_manager:error_reply().
write_and_check(Worker, TestHandle, Offset, Bytes) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            #lfm_handle{file_guid = GUID,
                fslogic_ctx = #fslogic_ctx{session_id = SessId}} = Handle,
            Result =
                case logical_file_manager:write(Handle, Offset, Bytes) of
                    {ok, NewHandle, Res}  ->
                        ets:insert(lfm_handles, {TestHandle, NewHandle}),
                        case logical_file_manager:fsync(NewHandle) of
                            ok ->
                                {ok, Res, logical_file_manager:stat(SessId, {guid, GUID})};
                            Other2 ->
                                Other2
                        end;
                    Other -> Other
                end,
            Host ! {self(), Result}
        end).

-spec mkdir(node(), session:id(), binary()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mkdir(Worker, SessId, Path) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:mkdir(SessId, Path),
            Host ! {self(), Result}
        end).

-spec mkdir(node(), session:id(), binary(), file_meta:posix_permissions()) ->
    {ok, DirUUID :: file_meta:uuid()} | logical_file_manager:error_reply().
mkdir(Worker, SessId, Path, Mode) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:mkdir(SessId, Path, Mode),
            Host ! {self(), Result}
        end).

-spec ls(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), integer(), integer()) ->
    {ok, [{fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), file_meta:name()}]} | logical_file_manager:error_reply().
ls(Worker, SessId, FileKey, Offset, Limit) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:ls(SessId, uuid_to_guid(Worker, FileKey), Offset, Limit),
            Host ! {self(), Result}
        end).

-spec mv(node(), session:id(), fslogic_worker:file_guid_or_path(), file_meta:path()) ->
    ok | logical_file_manager:error_reply().
mv(Worker, SessId, FileKeyFrom, PathTo) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:mv(SessId, FileKeyFrom, PathTo),
            Host ! {self(), Result}
        end).

-spec set_perms(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid(), file_meta:posix_permissions()) ->
    ok | logical_file_manager:error_reply().
set_perms(Worker, SessId, FileKey, NewPerms) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:set_perms(SessId, uuid_to_guid(Worker, FileKey), NewPerms),
            Host ! {self(), Result}
        end).

-spec update_times(node(), session:id(), logical_file_manager:file_key(),
    file_meta:time(), file_meta:time(), file_meta:time()) ->
    ok | logical_file_manager:error_reply().
update_times(Worker, SessId, FileKey, ATime, MTime, CTime) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:update_times(SessId, FileKey, ATime, MTime, CTime),
            Host ! {self(), Result}
        end).

-spec get_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), xattr:name()) ->
    {ok, #xattr{}} | logical_file_manager:error_reply().
get_xattr(Worker, SessId, FileKey, XattrKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:get_xattr(SessId, uuid_to_guid(Worker, FileKey), XattrKey),
            Host ! {self(), Result}
        end).

-spec set_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), #xattr{}) ->
    ok | logical_file_manager:error_reply().
set_xattr(Worker, SessId, FileKey, Xattr) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:set_xattr(SessId, uuid_to_guid(Worker, FileKey), Xattr),
            Host ! {self(), Result}
        end).

-spec remove_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), xattr:name()) ->
    ok | logical_file_manager:error_reply().
remove_xattr(Worker, SessId, FileKey, XattrKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:remove_xattr(SessId, uuid_to_guid(Worker, FileKey), XattrKey),
            Host ! {self(), Result}
        end).

-spec list_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    {ok, [xattr:name()]} | logical_file_manager:error_reply().
list_xattr(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:list_xattr(SessId, uuid_to_guid(Worker, FileKey)),
            Host ! {self(), Result}
        end).

-spec get_acl(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    {ok, [lfm_perms:access_control_entity()]} | logical_file_manager:error_reply().
get_acl(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:get_acl(SessId, uuid_to_guid(Worker, FileKey)),
            Host ! {self(), Result}
        end).

-spec set_acl(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), [lfm_perms:access_control_entity()]) ->
    ok | logical_file_manager:error_reply().
set_acl(Worker, SessId, FileKey, EntityList) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:set_acl(SessId, uuid_to_guid(Worker, FileKey), EntityList),
            Host ! {self(), Result}
        end).

-spec get_transfer_encoding(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid()) ->
    {ok, xattr:transfer_encoding()} | logical_file_manager:error_reply().
get_transfer_encoding(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:get_transfer_encoding(SessId, uuid_to_guid(Worker, FileKey)),
            Host ! {self(), Result}
        end).

-spec set_transfer_encoding(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid(), xattr:transfer_encoding()) ->
    ok | logical_file_manager:error_reply().
set_transfer_encoding(Worker, SessId, FileKey, Encoding) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:set_transfer_encoding(SessId, uuid_to_guid(Worker, FileKey), Encoding),
            Host ! {self(), Result}
        end).

-spec get_cdmi_completion_status(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid()) ->
    {ok, xattr:cdmi_completion_status()} | logical_file_manager:error_reply().
get_cdmi_completion_status(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:get_cdmi_completion_status(SessId, uuid_to_guid(Worker, FileKey)),
            Host ! {self(), Result}
        end).

-spec set_cdmi_completion_status(node(), session:id(),
    logical_file_manager:file_key() | file_meta:uuid(), xattr:cdmi_completion_status()) ->
    ok | logical_file_manager:error_reply().
set_cdmi_completion_status(Worker, SessId, FileKey, CompletionStatus) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:set_cdmi_completion_status(SessId, uuid_to_guid(Worker, FileKey), CompletionStatus),
            Host ! {self(), Result}
        end).

-spec get_mimetype(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid()) ->
    {ok, xattr:mimetype()} | logical_file_manager:error_reply().
get_mimetype(Worker, SessId, FileKey) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:get_mimetype(SessId, uuid_to_guid(Worker, FileKey)),
            Host ! {self(), Result}
        end).

-spec set_mimetype(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid(), xattr:mimetype()) ->
ok | logical_file_manager:error_reply().
set_mimetype(Worker, SessId, FileKey, Mimetype) ->
    exec(Worker,
        fun(Host) ->
            Result =
                logical_file_manager:set_mimetype(SessId, uuid_to_guid(Worker, FileKey), Mimetype),
            Host ! {self(), Result}
        end).

-spec fsync(node(), logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
fsync(Worker, TestHandle) ->
    exec(Worker,
        fun(Host) ->
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Result = logical_file_manager:fsync(Handle),
            Host ! {self(), Result}
        end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

exec(Worker, Fun) ->
    Host = self(),
    Pid = spawn_link(Worker,
        fun() ->
            try
                Fun(Host)
            catch
                _:Reason ->
                    Host ! {self(), {error, {test_exec, Reason, erlang:get_stacktrace()}}}
            end
        end),
    receive
        {Pid, Result} -> Result
    after timer:seconds(10) ->
        {error, test_timeout}
    end.


uuid_to_guid(W, {uuid, UUID}) ->
    {guid, uuid_to_guid(W, UUID)};
uuid_to_guid(W, UUID) when is_binary(UUID) ->
    rpc:call(W, fslogic_uuid, to_file_guid, [UUID]);
uuid_to_guid(_, Other) ->
    Other.