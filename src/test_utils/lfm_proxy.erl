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
-include("proto/oneclient/fuse_messages.hrl").


%% API
-export([init/1, teardown/1, stat/3, truncate/4, create/4, create/5, unlink/3, open/4, close/2, close_all/1,
    read/4, write/4, mkdir/3, mkdir/4, mkdir/5, mv/4, ls/5, read_dir_plus/5, set_perms/4, update_times/6,
    get_xattr/4, get_xattr/5, set_xattr/4, set_xattr/6, remove_xattr/4, list_xattr/5, get_acl/3, set_acl/4,
    write_and_check/4, get_transfer_encoding/3, set_transfer_encoding/4,
    get_cdmi_completion_status/3, set_cdmi_completion_status/4, get_mimetype/3,
    set_mimetype/4, fsync/2, fsync/4, rm_recursive/3, get_metadata/6, set_metadata/6,
    has_custom_metadata/3, remove_metadata/4, check_perms/4, create_share/4,
    remove_share/3, remove_share_by_guid/3, resolve_guid/3, invalidate_file_replica/5,
    get_file_distribution/3]).

-define(EXEC(Worker, Function),
    exec(Worker,
        fun(Host) ->
            Result = Function,
            Host ! {self(), Result}
        end)).

-define(EXEC_TIMEOUT(Worker, Function, Timeout),
    exec(Worker,
        fun(Host) ->
            Result = Function,
            Host ! {self(), Result}
        end, Timeout)).

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
    ?EXEC(Worker, logical_file_manager:stat(SessId, uuid_to_guid(Worker, FileKey))).

-spec truncate(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid(), non_neg_integer()) ->
    term().
truncate(Worker, SessId, FileKey, Size) ->
    ?EXEC(Worker, logical_file_manager:truncate(SessId, uuid_to_guid(Worker, FileKey), Size)).

-spec create(node(), session:id(), file_meta:path(), file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
create(Worker, SessId, FilePath, Mode) ->
    ?EXEC(Worker, logical_file_manager:create(SessId, FilePath, Mode)).

-spec create(node(), session:id(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
create(Worker, SessId, ParentGuid, Name, Mode) ->
    ?EXEC(Worker, logical_file_manager:create(SessId, ParentGuid, Name, Mode)).

-spec unlink(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    ok | logical_file_manager:error_reply().
unlink(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:unlink(SessId, uuid_to_guid(Worker, FileKey), false)).

-spec open(node(), session:id(), FileKey :: fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(),
    OpenType :: helpers:open_flag()) ->
    {ok, logical_file_manager:handle()} | logical_file_manager:error_reply().
open(Worker, SessId, FileKey, OpenFlag) ->
    ?EXEC(Worker,
        case logical_file_manager:open(SessId, uuid_to_guid(Worker, FileKey), OpenFlag) of
            {ok, Handle} ->
                TestHandle = crypto:strong_rand_bytes(10),
                ets:insert(lfm_handles, {TestHandle, Handle}),
                {ok, TestHandle};
            Other -> Other
        end).

-spec close(node(), logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
close(Worker, TestHandle) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            logical_file_manager:fsync(Handle),
            logical_file_manager:release(Handle),
            ets:delete(lfm_handles, TestHandle),
            ok
        end).

-spec close_all(node()) -> ok.
close_all(Worker) ->
    ?EXEC_TIMEOUT(Worker,
        begin
            lists:foreach(fun({_, Handle}) ->
                logical_file_manager:fsync(Handle),
                logical_file_manager:release(Handle)
            end, ets:tab2list(lfm_handles)),
            true = ets:delete_all_objects(lfm_handles),
            ok
        end, timer:minutes(5)).

-spec read(node(), logical_file_manager:handle(), integer(), integer()) ->
    {ok, binary()} | logical_file_manager:error_reply().
read(Worker, TestHandle, Offset, Size) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            case logical_file_manager:read(Handle, Offset, Size) of
                {ok, NewHandle, Res}  ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    {ok, Res};
                Other -> Other
            end
        end).

-spec write(node(), logical_file_manager:handle(), integer(), binary()) ->
    {ok, integer()} | logical_file_manager:error_reply().
write(Worker, TestHandle, Offset, Bytes) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            case logical_file_manager:write(Handle, Offset, Bytes) of
                {ok, NewHandle, Res}  ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    {ok, Res};
                Other -> Other
            end
        end).

-spec write_and_check(node(), logical_file_manager:handle(), integer(), binary()) ->
    {ok, integer(), StatAns} | logical_file_manager:error_reply() when
    StatAns :: {ok, lfm_attrs:file_attributes()} | logical_file_manager:error_reply().
write_and_check(Worker, TestHandle, Offset, Bytes) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Guid = lfm_context:get_guid(Handle),
            SessId = lfm_context:get_session_id(Handle),
            case logical_file_manager:write(Handle, Offset, Bytes) of
                {ok, NewHandle, Res}  ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    case logical_file_manager:fsync(NewHandle) of
                        ok ->
                            {ok, Res, logical_file_manager:stat(SessId, {guid, Guid})};
                        Other2 ->
                            Other2
                    end;
                Other -> Other
            end
        end).

-spec mkdir(node(), session:id(), binary()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mkdir(Worker, SessId, Path) ->
    ?EXEC(Worker, logical_file_manager:mkdir(SessId, Path)).

-spec mkdir(node(), session:id(), binary(), file_meta:posix_permissions()) ->
    {ok, DirUuid :: file_meta:uuid()} | logical_file_manager:error_reply().
mkdir(Worker, SessId, Path, Mode) ->
    ?EXEC(Worker, logical_file_manager:mkdir(SessId, Path, Mode)).

-spec mkdir(node(), session:id(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:posix_permissions()) ->
    {ok, DirUuid :: file_meta:uuid()} | logical_file_manager:error_reply().
mkdir(Worker, SessId, ParentGuid, Name, Mode) ->
    ?EXEC(Worker, logical_file_manager:mkdir(SessId, ParentGuid, Name, Mode)).

-spec ls(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), integer(), integer()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}]} | logical_file_manager:error_reply().
ls(Worker, SessId, FileKey, Offset, Limit) ->
    ?EXEC(Worker, logical_file_manager:ls(SessId, uuid_to_guid(Worker, FileKey), Offset, Limit)).

-spec read_dir_plus(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), integer(), integer()) ->
    {ok, [#file_attr{}]} | logical_file_manager:error_reply().
read_dir_plus(Worker, SessId, FileKey, Offset, Limit) ->
    ?EXEC(Worker, logical_file_manager:read_dir_plus(SessId, uuid_to_guid(Worker, FileKey), Offset, Limit)).

-spec mv(node(), session:id(), fslogic_worker:file_guid_or_path(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | logical_file_manager:error_reply().
mv(Worker, SessId, FileKeyFrom, PathTo) ->
    ?EXEC(Worker, logical_file_manager:mv(SessId, FileKeyFrom, PathTo)).

-spec set_perms(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid(), file_meta:posix_permissions()) ->
    ok | logical_file_manager:error_reply().
set_perms(Worker, SessId, FileKey, NewPerms) ->
    ?EXEC(Worker, logical_file_manager:set_perms(SessId, uuid_to_guid(Worker, FileKey), NewPerms)).

-spec update_times(node(), session:id(), logical_file_manager:file_key(),
    file_meta:time(), file_meta:time(), file_meta:time()) ->
    ok | logical_file_manager:error_reply().
update_times(Worker, SessId, FileKey, ATime, MTime, CTime) ->
    ?EXEC(Worker, logical_file_manager:update_times(SessId, FileKey, ATime, MTime, CTime)).

-spec get_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), xattr:name()) ->
    {ok, #xattr{}} | logical_file_manager:error_reply().
get_xattr(Worker, SessId, FileKey, XattrKey) ->
    get_xattr(Worker, SessId, FileKey, XattrKey, false).

-spec get_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), xattr:name(), boolean()) ->
    {ok, #xattr{}} | logical_file_manager:error_reply().
get_xattr(Worker, SessId, FileKey, XattrKey, Inherited) ->
    ?EXEC(Worker, logical_file_manager:get_xattr(SessId, uuid_to_guid(Worker, FileKey), XattrKey, Inherited)).

-spec set_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), #xattr{}) ->
    ok | logical_file_manager:error_reply().
set_xattr(Worker, SessId, FileKey, Xattr) ->
    set_xattr(Worker, SessId, FileKey, Xattr, false, false).

-spec set_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), #xattr{},
    Create :: boolean(), Replace :: boolean()) ->
    ok | logical_file_manager:error_reply().
set_xattr(Worker, SessId, FileKey, Xattr, Create, Replace) ->
    ?EXEC(Worker, logical_file_manager:set_xattr(SessId, uuid_to_guid(Worker, FileKey), Xattr, Create, Replace)).

-spec remove_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), xattr:name()) ->
    ok | logical_file_manager:error_reply().
remove_xattr(Worker, SessId, FileKey, XattrKey) ->
    ?EXEC(Worker, logical_file_manager:remove_xattr(SessId, uuid_to_guid(Worker, FileKey), XattrKey)).

-spec list_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), boolean(), boolean()) ->
    {ok, [xattr:name()]} | logical_file_manager:error_reply().
list_xattr(Worker, SessId, FileKey, Inherited, ShowInternal) ->
    ?EXEC(Worker, logical_file_manager:list_xattr(SessId, uuid_to_guid(Worker, FileKey), Inherited, ShowInternal)).

-spec get_acl(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    {ok, [lfm_perms:access_control_entity()]} | logical_file_manager:error_reply().
get_acl(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:get_acl(SessId, uuid_to_guid(Worker, FileKey))).

-spec set_acl(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), [lfm_perms:access_control_entity()]) ->
    ok | logical_file_manager:error_reply().
set_acl(Worker, SessId, FileKey, EntityList) ->
    ?EXEC(Worker, logical_file_manager:set_acl(SessId, uuid_to_guid(Worker, FileKey), EntityList)).

-spec get_transfer_encoding(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid()) ->
    {ok, xattr:transfer_encoding()} | logical_file_manager:error_reply().
get_transfer_encoding(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:get_transfer_encoding(SessId, uuid_to_guid(Worker, FileKey))).

-spec set_transfer_encoding(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid(), xattr:transfer_encoding()) ->
    ok | logical_file_manager:error_reply().
set_transfer_encoding(Worker, SessId, FileKey, Encoding) ->
    ?EXEC(Worker, logical_file_manager:set_transfer_encoding(SessId, uuid_to_guid(Worker, FileKey), Encoding)).

-spec get_cdmi_completion_status(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid()) ->
    {ok, xattr:cdmi_completion_status()} | logical_file_manager:error_reply().
get_cdmi_completion_status(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:get_cdmi_completion_status(SessId, uuid_to_guid(Worker, FileKey))).

-spec set_cdmi_completion_status(node(), session:id(),
    logical_file_manager:file_key() | file_meta:uuid(), xattr:cdmi_completion_status()) ->
    ok | logical_file_manager:error_reply().
set_cdmi_completion_status(Worker, SessId, FileKey, CompletionStatus) ->
    ?EXEC(Worker, logical_file_manager:set_cdmi_completion_status(SessId, uuid_to_guid(Worker, FileKey), CompletionStatus)).

-spec get_mimetype(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid()) ->
    {ok, xattr:mimetype()} | logical_file_manager:error_reply().
get_mimetype(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:get_mimetype(SessId, uuid_to_guid(Worker, FileKey))).

-spec set_mimetype(node(), session:id(), logical_file_manager:file_key() | file_meta:uuid(), xattr:mimetype()) ->
ok | logical_file_manager:error_reply().
set_mimetype(Worker, SessId, FileKey, Mimetype) ->
    ?EXEC(Worker, logical_file_manager:set_mimetype(SessId, uuid_to_guid(Worker, FileKey), Mimetype)).

-spec fsync(node(), logical_file_manager:handle()) ->
    ok | logical_file_manager:error_reply().
fsync(Worker, TestHandle) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            logical_file_manager:fsync(Handle)
        end).

-spec fsync(node(), session:id(), logical_file_manager:file_key(), oneprovider:id()) ->
    ok | logical_file_manager:error_reply().
fsync(Worker, SessId, FileKey, OneproviderId) ->
    ?EXEC(Worker, logical_file_manager:fsync(SessId, FileKey, OneproviderId)).

-spec rm_recursive(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    ok | logical_file_manager:error_reply().
rm_recursive(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:rm_recursive(SessId, uuid_to_guid(Worker, FileKey))).

-spec get_metadata(node(), session:id(), logical_file_manager:file_key(),
    custom_metadata:type(), custom_metadata:filter(), boolean()) ->
    {ok, custom_metadata:value()}.
get_metadata(Worker, SessId, FileKey, Type, Names, Inherited) ->
    ?EXEC(Worker, logical_file_manager:get_metadata(SessId, FileKey, Type, Names, Inherited)).

-spec set_metadata(node(), session:id(), logical_file_manager:file_key(),
    custom_metadata:type(), custom_metadata:value(), custom_metadata:filter()) ->
    ok.
set_metadata(Worker, SessId, FileKey, Type, Value, Names) ->
    ?EXEC(Worker, logical_file_manager:set_metadata(SessId, FileKey, Type, Value, Names)).

-spec has_custom_metadata(node(), session:id(), logical_file_manager:file_key()) ->
    {ok, boolean()}.
has_custom_metadata(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:has_custom_metadata(SessId, FileKey)).

-spec remove_metadata(node(), session:id(), logical_file_manager:file_key(),
    custom_metadata:type()) -> ok.
remove_metadata(Worker, SessId, FileKey, Type) ->
    ?EXEC(Worker, logical_file_manager:remove_metadata(SessId, FileKey, Type)).

-spec check_perms(node(), session:id(), logical_file_manager:file_key(), helpers:open_flag()) ->
    {ok, boolean()} | {error, term()}.
check_perms(Worker, SessId, FileKey, OpenFlag) ->
    ?EXEC(Worker, logical_file_manager:check_perms(SessId, FileKey, OpenFlag)).

-spec create_share(node(), session:id(), logical_file_manager:file_key(), od_share:name()) ->
    {ok, {od_share:id(), od_share:share_guid()}} | {error, term()}.
create_share(Worker, SessId, FileKey, Name) ->
    ?EXEC(Worker, logical_file_manager:create_share(SessId, FileKey, Name)).

-spec remove_share(node(), session:id(), od_share:id()) ->
    ok | {error, term()}.
remove_share(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:remove_share(SessId, FileKey)).

-spec remove_share_by_guid(node(), session:id(), od_share:share_guid()) ->
    ok | {error, term()}.
remove_share_by_guid(Worker, SessId, ShareGuid) ->
    ?EXEC(Worker, logical_file_manager:remove_share_by_guid(SessId, ShareGuid)).

-spec resolve_guid(node(), session:id(), file_ctx:path()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
resolve_guid(Worker, SessId, Path) ->
    ?EXEC(Worker, remote_utils:call_fslogic(SessId, fuse_request, #resolve_guid{path = Path},
        fun(#guid{guid = Guid}) ->
            {ok, Guid}
        end)).

-spec invalidate_file_replica(node(), session:id(), logical_file_manager:file_key(),
    ProviderId :: oneprovider:id(), MigrationProviderId :: undefined | oneprovider:id()) -> ok.
invalidate_file_replica(Worker, SessId, FileKey, ProviderId, MigrationProviderId) ->
    ?EXEC(Worker, logical_file_manager:invalidate_file_replica(SessId, FileKey, ProviderId, MigrationProviderId)).

-spec get_file_distribution(node(), session:id(), logical_file_manager:file_key()) -> {ok, list()}.
get_file_distribution(Worker, SessId, FileKey) ->
    ?EXEC(Worker, logical_file_manager:get_file_distribution(SessId, FileKey)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

exec(Worker, Fun) ->
    exec(Worker, Fun, timer:seconds(60)).

exec(Worker, Fun, Timeout) ->
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
    after Timeout ->
        {error, test_timeout}
    end.


uuid_to_guid(W, {uuid, Uuid}) ->
    {guid, uuid_to_guid(W, Uuid)};
uuid_to_guid(W, Uuid) when is_binary(Uuid) ->
    rpc:call(W, fslogic_uuid, uuid_to_guid, [Uuid]);
uuid_to_guid(_, Other) ->
    Other.