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
-export([init/1, teardown/1, stat/3, truncate/4, create/4, create/5,
    create_and_open/4, create_and_open/5, unlink/3, open/4, close/2, close_all/1,
    read/4, silent_read/4, write/4, mkdir/3, mkdir/4, mkdir/5, mv/4, ls/5,
    ls/6, read_dir_plus/5, read_dir_plus/6, set_perms/4,
    update_times/6, get_xattr/4, get_xattr/5, set_xattr/4, set_xattr/6, remove_xattr/4, list_xattr/5,
    get_acl/3, set_acl/4, write_and_check/4, get_transfer_encoding/3, set_transfer_encoding/4,
    get_cdmi_completion_status/3, set_cdmi_completion_status/4, get_mimetype/3,
    set_mimetype/4, fsync/2, fsync/4, rm_recursive/3, get_metadata/6, set_metadata/6,
    has_custom_metadata/3, remove_metadata/4, check_perms/4, create_share/4,
    remove_share/3, remove_share_by_guid/3, resolve_guid/3, schedule_file_replica_eviction/5,
    schedule_file_replication/4, get_file_distribution/3,
    schedule_replication_by_index/6, schedule_replica_eviction_by_index/7,
    get_file_qos/3, add_qos/5, get_qos_details/3, remove_qos/3, check_qos_fulfilled/3]).

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

-spec add_qos(node(), session:id(), logical_file_manager:file_key(), binary(),
    qos_entry:replicas_num()) -> {ok, qos_entry:id()} | logical_file_manager:error_reply().
add_qos(Worker, SessId, FileKey, Expression, ReplicasNum) ->
    ?EXEC(Worker, lfm:add_qos(SessId, FileKey, Expression, ReplicasNum)).

-spec get_file_qos(node(), session:id(), logical_file_manager:file_key()) ->
    {ok, {file_qos:qos_list(), file_qos:target_storages()}} | logical_file_manager:error_reply().
get_file_qos(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_file_qos(SessId, FileKey)).

-spec get_qos_details(node(), session:id(), qos_entry:id()) ->
    {ok, #qos_entry{}} | logical_file_manager:error_reply().
get_qos_details(Worker, SessId, QosId) ->
    ?EXEC(Worker, lfm:get_qos_details(SessId, QosId)).

-spec remove_qos(node(), session:id(), qos_entry:id()) ->
    ok | logical_file_manager:error_reply().
remove_qos(Worker, SessId, QosId) ->
    ?EXEC(Worker, lfm:remove_qos(SessId, QosId)).

-spec check_qos_fulfilled(node(), session:id(), qos_entry:id()) ->
    boolean() | logical_file_manager:error_reply().
check_qos_fulfilled(Worker, SessId, QosId) ->
    ?EXEC(Worker, lfm:check_qos_fulfilled(SessId, QosId)).

-spec stat(node(), session:id(), lfm:file_key() | file_meta:uuid()) ->
    {ok, lfm_attrs:file_attributes()} | lfm:error_reply().
stat(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:stat(SessId, uuid_to_guid(Worker, FileKey))).

-spec truncate(node(), session:id(), lfm:file_key() | file_meta:uuid(), non_neg_integer()) ->
    term().
truncate(Worker, SessId, FileKey, Size) ->
    ?EXEC(Worker, lfm:truncate(SessId, uuid_to_guid(Worker, FileKey), Size)).

-spec create(node(), session:id(), file_meta:path(), file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
create(Worker, SessId, FilePath, Mode) ->
    ?EXEC(Worker, lfm:create(SessId, FilePath, Mode)).

-spec create(node(), session:id(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
create(Worker, SessId, ParentGuid, Name, Mode) ->
    ?EXEC(Worker, lfm:create(SessId, ParentGuid, Name, Mode)).

-spec create_and_open(node(), session:id(), file_meta:path(), file_meta:posix_permissions()) ->
    {ok, {fslogic_worker:file_guid(), lfm:handle()}} |
    lfm:error_reply().
create_and_open(Worker, SessId, Path, Mode) ->
    ?EXEC(Worker,
        case lfm:create_and_open(SessId, Path, Mode, rdwr) of
            {ok, {Guid, Handle}} ->
                TestHandle = crypto:strong_rand_bytes(10),
                ets:insert(lfm_handles, {TestHandle, Handle}),
                {ok, {Guid, TestHandle}};
            Other -> Other
        end).

-spec create_and_open(node(), session:id(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:posix_permissions()) ->
    {ok, {fslogic_worker:file_guid(), lfm:handle()}} |
    lfm:error_reply().
create_and_open(Worker, SessId, ParentGuid, Name, Mode) ->
    ?EXEC(Worker,
        case lfm:create_and_open(SessId, ParentGuid, Name, Mode, rdwr) of
            {ok, {Guid, Handle}} ->
                TestHandle = crypto:strong_rand_bytes(10),
                ets:insert(lfm_handles, {TestHandle, Handle}),
                {ok, {Guid, TestHandle}};
            Other -> Other
        end).

-spec unlink(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    ok | lfm:error_reply().
unlink(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:unlink(SessId, uuid_to_guid(Worker, FileKey), false)).

-spec open(node(), session:id(), FileKey :: fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(),
    OpenType :: helpers:open_flag()) ->
    {ok, lfm:handle()} | lfm:error_reply().
open(Worker, SessId, FileKey, OpenFlag) ->
    ?EXEC(Worker,
        case lfm:open(SessId, uuid_to_guid(Worker, FileKey), OpenFlag) of
            {ok, Handle} ->
                TestHandle = crypto:strong_rand_bytes(10),
                ets:insert(lfm_handles, {TestHandle, Handle}),
                {ok, TestHandle};
            Other -> Other
        end).

-spec close(node(), lfm:handle()) ->
    ok | lfm:error_reply().
close(Worker, TestHandle) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            ok = lfm:fsync(Handle),
            ok = lfm:release(Handle),
            ets:delete(lfm_handles, TestHandle),
            ok
        end).

-spec close_all(node()) -> ok.
close_all(Worker) ->
    ?EXEC_TIMEOUT(Worker,
        begin
            lists:foreach(fun({_, Handle}) ->
                lfm:fsync(Handle),
                lfm:release(Handle)
            end, ets:tab2list(lfm_handles)),
            true = ets:delete_all_objects(lfm_handles),
            ok
        end, timer:minutes(5)).

-spec read(node(), lfm:handle(), integer(), integer()) ->
    {ok, binary()} | lfm:error_reply().
read(Worker, TestHandle, Offset, Size) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            case lfm:read(Handle, Offset, Size) of
                {ok, NewHandle, Res}  ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    {ok, Res};
                Other -> Other
            end
        end).

-spec silent_read(node(), lfm:handle(), integer(), integer()) ->
    {ok, binary()} | lfm:error_reply().
silent_read(Worker, TestHandle, Offset, Size) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            case lfm:silent_read(Handle, Offset, Size) of
                {ok, NewHandle, Res}  ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    {ok, Res};
                Other -> Other
            end
        end).

-spec write(node(), lfm:handle(), integer(), binary()) ->
    {ok, integer()} | lfm:error_reply().
write(Worker, TestHandle, Offset, Bytes) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            case lfm:write(Handle, Offset, Bytes) of
                {ok, NewHandle, Res}  ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    {ok, Res};
                Other -> Other
            end
        end).

-spec write_and_check(node(), lfm:handle(), integer(), binary()) ->
    {ok, integer(), StatAns} | lfm:error_reply() when
    StatAns :: {ok, lfm_attrs:file_attributes()} | lfm:error_reply().
write_and_check(Worker, TestHandle, Offset, Bytes) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            Guid = lfm_context:get_guid(Handle),
            SessId = lfm_context:get_session_id(Handle),
            case lfm:write(Handle, Offset, Bytes) of
                {ok, NewHandle, Res}  ->
                    ets:insert(lfm_handles, {TestHandle, NewHandle}),
                    case lfm:fsync(NewHandle) of
                        ok ->
                            {ok, Res, lfm:stat(SessId, {guid, Guid})};
                        Other2 ->
                            Other2
                    end;
                Other -> Other
            end
        end).

-spec mkdir(node(), session:id(), binary()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
mkdir(Worker, SessId, Path) ->
    ?EXEC(Worker, lfm:mkdir(SessId, Path)).

-spec mkdir(node(), session:id(), binary(), file_meta:posix_permissions()) ->
    {ok, DirUuid :: file_meta:uuid()} | lfm:error_reply().
mkdir(Worker, SessId, Path, Mode) ->
    ?EXEC(Worker, lfm:mkdir(SessId, Path, Mode)).

-spec mkdir(node(), session:id(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:posix_permissions()) ->
    {ok, DirUuid :: file_meta:uuid()} | lfm:error_reply().
mkdir(Worker, SessId, ParentGuid, Name, Mode) ->
    ?EXEC(Worker, lfm:mkdir(SessId, ParentGuid, Name, Mode)).

-spec ls(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), integer(), integer()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}]} | lfm:error_reply().
ls(Worker, SessId, FileKey, Offset, Limit) ->
    ?EXEC(Worker, lfm:ls(SessId, uuid_to_guid(Worker, FileKey), Offset, Limit)).

-spec ls(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(),
    integer(), integer(), undefined | binary()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}], binary(), boolean()} | lfm:error_reply().
ls(Worker, SessId, FileKey, Offset, Limit, Token) ->
    ?EXEC(Worker, lfm:ls(SessId, uuid_to_guid(Worker, FileKey), Offset, Limit, Token)).

-spec read_dir_plus(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), integer(), integer()) ->
    {ok, [#file_attr{}]} | lfm:error_reply().
read_dir_plus(Worker, SessId, FileKey, Offset, Limit) ->
    ?EXEC(Worker, lfm:read_dir_plus(SessId, uuid_to_guid(Worker, FileKey), Offset, Limit)).

-spec read_dir_plus(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(),
    integer(), integer(), undefined | binary()) ->
    {ok, [#file_attr{}],  binary(), boolean()} | lfm:error_reply().
read_dir_plus(Worker, SessId, FileKey, Offset, Limit, Token) ->
    ?EXEC(Worker, lfm:read_dir_plus(SessId, uuid_to_guid(Worker, FileKey), Offset, Limit, Token)).

-spec mv(node(), session:id(), fslogic_worker:file_guid_or_path(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
mv(Worker, SessId, FileKeyFrom, PathTo) ->
    ?EXEC(Worker, lfm:mv(SessId, FileKeyFrom, PathTo)).

-spec set_perms(node(), session:id(), lfm:file_key() | file_meta:uuid(), file_meta:posix_permissions()) ->
    ok | lfm:error_reply().
set_perms(Worker, SessId, FileKey, NewPerms) ->
    ?EXEC(Worker, lfm:set_perms(SessId, uuid_to_guid(Worker, FileKey), NewPerms)).

-spec update_times(node(), session:id(), lfm:file_key(),
    file_meta:time(), file_meta:time(), file_meta:time()) ->
    ok | lfm:error_reply().
update_times(Worker, SessId, FileKey, ATime, MTime, CTime) ->
    ?EXEC(Worker, lfm:update_times(SessId, FileKey, ATime, MTime, CTime)).

-spec get_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), xattr:name()) ->
    {ok, #xattr{}} | lfm:error_reply().
get_xattr(Worker, SessId, FileKey, XattrKey) ->
    get_xattr(Worker, SessId, FileKey, XattrKey, false).

-spec get_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), xattr:name(), boolean()) ->
    {ok, #xattr{}} | lfm:error_reply().
get_xattr(Worker, SessId, FileKey, XattrKey, Inherited) ->
    ?EXEC(Worker, lfm:get_xattr(SessId, uuid_to_guid(Worker, FileKey), XattrKey, Inherited)).

-spec set_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), #xattr{}) ->
    ok | lfm:error_reply().
set_xattr(Worker, SessId, FileKey, Xattr) ->
    set_xattr(Worker, SessId, FileKey, Xattr, false, false).

-spec set_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), #xattr{},
    Create :: boolean(), Replace :: boolean()) ->
    ok | lfm:error_reply().
set_xattr(Worker, SessId, FileKey, Xattr, Create, Replace) ->
    ?EXEC(Worker, lfm:set_xattr(SessId, uuid_to_guid(Worker, FileKey), Xattr, Create, Replace)).

-spec remove_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), xattr:name()) ->
    ok | lfm:error_reply().
remove_xattr(Worker, SessId, FileKey, XattrKey) ->
    ?EXEC(Worker, lfm:remove_xattr(SessId, uuid_to_guid(Worker, FileKey), XattrKey)).

-spec list_xattr(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), boolean(), boolean()) ->
    {ok, [xattr:name()]} | lfm:error_reply().
list_xattr(Worker, SessId, FileKey, Inherited, ShowInternal) ->
    ?EXEC(Worker, lfm:list_xattr(SessId, uuid_to_guid(Worker, FileKey), Inherited, ShowInternal)).

-spec get_acl(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    {ok, [lfm_perms:access_control_entity()]} | lfm:error_reply().
get_acl(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_acl(SessId, uuid_to_guid(Worker, FileKey))).

-spec set_acl(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path(), [lfm_perms:access_control_entity()]) ->
    ok | lfm:error_reply().
set_acl(Worker, SessId, FileKey, EntityList) ->
    ?EXEC(Worker, lfm:set_acl(SessId, uuid_to_guid(Worker, FileKey), EntityList)).

-spec get_transfer_encoding(node(), session:id(), lfm:file_key() | file_meta:uuid()) ->
    {ok, xattr:transfer_encoding()} | lfm:error_reply().
get_transfer_encoding(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_transfer_encoding(SessId, uuid_to_guid(Worker, FileKey))).

-spec set_transfer_encoding(node(), session:id(), lfm:file_key() | file_meta:uuid(), xattr:transfer_encoding()) ->
    ok | lfm:error_reply().
set_transfer_encoding(Worker, SessId, FileKey, Encoding) ->
    ?EXEC(Worker, lfm:set_transfer_encoding(SessId, uuid_to_guid(Worker, FileKey), Encoding)).

-spec get_cdmi_completion_status(node(), session:id(), lfm:file_key() | file_meta:uuid()) ->
    {ok, xattr:cdmi_completion_status()} | lfm:error_reply().
get_cdmi_completion_status(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_cdmi_completion_status(SessId, uuid_to_guid(Worker, FileKey))).

-spec set_cdmi_completion_status(node(), session:id(),
    lfm:file_key() | file_meta:uuid(), xattr:cdmi_completion_status()) ->
    ok | lfm:error_reply().
set_cdmi_completion_status(Worker, SessId, FileKey, CompletionStatus) ->
    ?EXEC(Worker, lfm:set_cdmi_completion_status(SessId, uuid_to_guid(Worker, FileKey), CompletionStatus)).

-spec get_mimetype(node(), session:id(), lfm:file_key() | file_meta:uuid()) ->
    {ok, xattr:mimetype()} | lfm:error_reply().
get_mimetype(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_mimetype(SessId, uuid_to_guid(Worker, FileKey))).

-spec set_mimetype(node(), session:id(), lfm:file_key() | file_meta:uuid(), xattr:mimetype()) ->
ok | lfm:error_reply().
set_mimetype(Worker, SessId, FileKey, Mimetype) ->
    ?EXEC(Worker, lfm:set_mimetype(SessId, uuid_to_guid(Worker, FileKey), Mimetype)).

-spec fsync(node(), lfm:handle()) ->
    ok | lfm:error_reply().
fsync(Worker, TestHandle) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            lfm:fsync(Handle)
        end).

-spec fsync(node(), session:id(), lfm:file_key(), oneprovider:id()) ->
    ok | lfm:error_reply().
fsync(Worker, SessId, FileKey, OneproviderId) ->
    ?EXEC(Worker, lfm:fsync(SessId, FileKey, OneproviderId)).

-spec rm_recursive(node(), session:id(), fslogic_worker:file_guid_or_path() | file_meta:uuid_or_path()) ->
    ok | lfm:error_reply().
rm_recursive(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:rm_recursive(SessId, uuid_to_guid(Worker, FileKey))).

-spec get_metadata(node(), session:id(), lfm:file_key(),
    custom_metadata:type(), custom_metadata:filter(), boolean()) ->
    {ok, custom_metadata:value()}.
get_metadata(Worker, SessId, FileKey, Type, Names, Inherited) ->
    ?EXEC(Worker, lfm:get_metadata(SessId, FileKey, Type, Names, Inherited)).

-spec set_metadata(node(), session:id(), lfm:file_key(),
    custom_metadata:type(), custom_metadata:value(), custom_metadata:filter()) ->
    ok.
set_metadata(Worker, SessId, FileKey, Type, Value, Names) ->
    ?EXEC(Worker, lfm:set_metadata(SessId, FileKey, Type, Value, Names)).

-spec has_custom_metadata(node(), session:id(), lfm:file_key()) ->
    {ok, boolean()}.
has_custom_metadata(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:has_custom_metadata(SessId, FileKey)).

-spec remove_metadata(node(), session:id(), lfm:file_key(),
    custom_metadata:type()) -> ok.
remove_metadata(Worker, SessId, FileKey, Type) ->
    ?EXEC(Worker, lfm:remove_metadata(SessId, FileKey, Type)).

-spec check_perms(node(), session:id(), lfm:file_key(), helpers:open_flag()) ->
    {ok, boolean()} | {error, term()}.
check_perms(Worker, SessId, FileKey, OpenFlag) ->
    ?EXEC(Worker, lfm:check_perms(SessId, FileKey, OpenFlag)).

-spec create_share(node(), session:id(), lfm:file_key(), od_share:name()) ->
    {ok, {od_share:id(), od_share:root_file_guid()}} | {error, term()}.
create_share(Worker, SessId, FileKey, Name) ->
    ?EXEC(Worker, lfm:create_share(SessId, FileKey, Name)).

-spec remove_share(node(), session:id(), od_share:id()) ->
    ok | {error, term()}.
remove_share(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:remove_share(SessId, FileKey)).

-spec remove_share_by_guid(node(), session:id(), od_share:root_file_guid()) ->
    ok | {error, term()}.
remove_share_by_guid(Worker, SessId, ShareGuid) ->
    ?EXEC(Worker, lfm:remove_share_by_guid(SessId, ShareGuid)).

-spec resolve_guid(node(), session:id(), file_ctx:path()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
resolve_guid(Worker, SessId, Path) ->
    ?EXEC(Worker, remote_utils:call_fslogic(SessId, fuse_request, #resolve_guid{path = Path},
        fun(#guid{guid = Guid}) ->
            {ok, Guid}
        end)).

-spec schedule_file_replica_eviction(node(), session:id(), lfm:file_key(),
    ProviderId :: oneprovider:id(), MigrationProviderId :: undefined | oneprovider:id()) ->
    {ok, transfer:id()} | {error, term()}.
schedule_file_replica_eviction(Worker, SessId, FileKey, ProviderId, MigrationProviderId) ->
    ?EXEC(Worker, lfm:schedule_replica_eviction(SessId, FileKey, ProviderId, MigrationProviderId)).

-spec schedule_file_replication(node(), session:id(), lfm:file_key(),
    ProviderId :: oneprovider:id()) -> {ok, transfer:id()} | {error, term()}.
schedule_file_replication(Worker, SessId, FileKey, ProviderId) ->
    ?EXEC(Worker, lfm:schedule_file_replication(SessId, FileKey, ProviderId, undefined)).

-spec schedule_replication_by_index(node(), session:id(),
    ProviderId :: oneprovider:id(), SpaceId :: od_space:id(), IndexName :: transfer:index_name(),
    transfer:query_view_params()) -> {ok, transfer:id()} | {error, term()}.
schedule_replication_by_index(Worker, SessId, ProviderId, SpaceId, IndexName, QueryViewParams) ->
    ?EXEC(Worker, lfm:schedule_replication_by_index(SessId,
        ProviderId, undefined, SpaceId, IndexName, QueryViewParams)).

-spec schedule_replica_eviction_by_index(node(), session:id(), ProviderId :: oneprovider:id(),
    MigrationProviderId :: undefined | oneprovider:id(), od_space:id(),
    transfer:index_name(), transfer:query_view_params()) -> {ok, transfer:id()} | {error, term()}.
schedule_replica_eviction_by_index(Worker, SessId, ProviderId, MigrationProviderId,
    SpaceId, IndexName, QueryViewParams
) ->
    ?EXEC(Worker, lfm:schedule_replica_eviction_by_index(
        SessId, ProviderId, MigrationProviderId, SpaceId, IndexName, QueryViewParams
    )).

-spec get_file_distribution(node(), session:id(), lfm:file_key()) -> {ok, list()}.
get_file_distribution(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_file_distribution(SessId, FileKey)).

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