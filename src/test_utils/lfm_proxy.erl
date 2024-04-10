%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Proxy for lfm (logical files manager) operations.
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_proxy).
-author("Tomasz Lichon").

-include("modules/fslogic/acl.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("common_test/include/ct.hrl").


%% API
-export([init/1, init/2, init/3, teardown/1]).
-export([
    stat/3, stat/4, resolve_symlink/3, get_fs_stats/3,
    get_file_references/3,
    resolve_guid/3, get_file_path/3,
    get_parent/3,
    ensure_dir/5,
    check_perms/4,
    set_perms/4,
    update_times/6,
    unlink/3, rm_recursive/3,
    mv/4, mv/5,
    cp/4, cp/5,
    is_dir/3,

    get_file_location/3,
    create/3, create/4, create/5,
    make_link/4, make_link/5,
    make_symlink/4, make_symlink/5, read_symlink/3,
    create_and_open/3, create_and_open/4, create_and_open/5,
    open/4,
    close/2, close_all/1,
    read/4, silent_read/4, check_size_and_read/4,
    write/4, write_and_check/4,
    fsync/2, fsync/4,
    truncate/4,

    mkdir/3, mkdir/4, mkdir/5,
    create_dir_at_path/4,
    get_children/4, get_children/5,
    get_children_attrs/4, get_children_attrs/5,
    get_child_attr/4,
    get_files_recursively/5,

    get_xattr/4, get_xattr/5,
    set_xattr/4, set_xattr/6,
    remove_xattr/4,
    list_xattr/5,

    get_acl/3, set_acl/4, remove_acl/3,

    has_custom_metadata/3,

    create_multipart_upload/4,
    abort_multipart_upload/3,
    complete_multipart_upload/3,
    list_multipart_uploads/5,
    upload_multipart_part/4,
    list_multipart_parts/5
]).

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
    init(Config, true).


-spec init(Config :: list(), boolean()) -> list().
init(Config, Link) ->
    init(Config, Link, ?config(op_worker_nodes, Config)).


-spec init(Config :: list(), boolean(), [node()]) -> list().
init(Config, Link, Workers) ->
    teardown(Config),
    Host = self(),
    ServerFun = fun() ->
        register(lfm_proxy_server, self()),
        lfm_handles = ets:new(lfm_handles, [public, set, named_table]),
        Host ! {self(), done},
        receive
            exit -> ok
        end
    end,
    Servers = lists:map(fun(W) ->
        case Link of
            true -> spawn_link(W, ServerFun);
            false -> spawn(W, ServerFun)
        end
    end, lists:usort(Workers)),

    lists:foreach(
        fun(Server) ->
            receive
                {Server, done} -> ok
            after timer:seconds(5) ->
                error("Cannot setup lfm_handles ETS")
            end
        end, Servers),

    Config.


-spec teardown(Config :: list()) -> ok.
teardown(Config) ->
    lists:foreach(fun(Worker) ->
        case rpc:call(Worker, erlang, whereis, [lfm_proxy_server]) of
            undefined -> ok;
            Pid -> Pid ! exit
        end
    end, ?config(op_worker_nodes, Config)).


%%%===================================================================
%%% General file related operations
%%%===================================================================


-spec stat(node(), session:id(), lfm:file_key() | file_meta:uuid()) ->
    {ok, lfm_attrs:file_attributes()} | lfm:error_reply().
stat(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:stat(SessId, uuid_to_file_ref(Worker, FileKey))).


-spec stat(node(), session:id(), lfm:file_key() | file_meta:uuid(), [onedata_file:attr_name()]) ->
    {ok, lfm_attrs:file_attributes()} | lfm:error_reply().
stat(Worker, SessId, FileKey, Attributes) ->
    ?EXEC(Worker, lfm:stat(SessId, uuid_to_file_ref(Worker, FileKey), Attributes)).


-spec resolve_symlink(node(), session:id(), lfm:file_key() | file_meta:uuid()) ->
    {ok, file_id:file_guid()} | lfm:error_reply().
resolve_symlink(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:resolve_symlink(SessId, uuid_to_file_ref(Worker, FileKey))).


-spec get_fs_stats(node(), session:id(), lfm:file_key() | file_meta:uuid()) ->
    {ok, lfm_attrs:fs_stats()} | lfm:error_reply().
get_fs_stats(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_fs_stats(SessId, uuid_to_file_ref(Worker, FileKey))).


-spec get_file_references(node(), session:id(), lfm:file_key() | file_meta:uuid()) ->
    {ok, [file_id:file_guid()]} | lfm:error_reply().
get_file_references(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_file_references(SessId, uuid_to_file_ref(Worker, FileKey))).


-spec resolve_guid(node(), session:id(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
resolve_guid(Worker, SessId, Path) ->
    ?EXEC(Worker, remote_utils:call_fslogic(SessId, fuse_request, #resolve_guid{path = Path},
        fun(#guid{guid = Guid}) ->
            {ok, Guid}
        end)).


-spec get_file_path(node(), session:id(), file_id:file_guid()) ->
    {ok, binary()} | lfm:error_reply().
get_file_path(Worker, SessId, Guid) ->
    ?EXEC(Worker, lfm:get_file_path(SessId, Guid)).


-spec get_parent(node(), session:id(), lfm:file_key()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
get_parent(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_parent(SessId, FileKey)).


-spec ensure_dir(node(), session:id(), fslogic_worker:file_guid(), file_meta:path(), file_meta:mode()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
ensure_dir(Worker, SessId, RelativeRootGuid, FilePath, Mode) ->
    ?EXEC(Worker, lfm:ensure_dir(SessId, RelativeRootGuid, FilePath, Mode)).


-spec check_perms(node(), session:id(), lfm:file_key(), helpers:open_flag()) ->
    ok | {error, term()}.
check_perms(Worker, SessId, FileKey, OpenFlag) ->
    ?EXEC(Worker, lfm:check_perms(SessId, FileKey, OpenFlag)).


-spec set_perms(node(), session:id(), lfm:file_key() | file_meta:uuid(), file_meta:posix_permissions()) ->
    ok | lfm:error_reply().
set_perms(Worker, SessId, FileKey, NewPerms) ->
    ?EXEC(Worker, lfm:set_perms(SessId, uuid_to_file_ref(Worker, FileKey), NewPerms)).


-spec update_times(node(), session:id(), lfm:file_key(),
    file_meta:time(), file_meta:time(), file_meta:time()) ->
    ok | lfm:error_reply().
update_times(Worker, SessId, FileKey, ATime, MTime, CTime) ->
    ?EXEC(Worker, lfm:update_times(SessId, FileKey, ATime, MTime, CTime)).


-spec unlink(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path()) ->
    ok | lfm:error_reply().
unlink(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:unlink(SessId, uuid_to_file_ref(Worker, FileKey), false)).


-spec rm_recursive(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path()) ->
    ok | lfm:error_reply().
rm_recursive(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:rm_recursive(SessId, uuid_to_file_ref(Worker, FileKey))).


-spec mv(node(), session:id(), lfm:file_key(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
mv(Worker, SessId, FileKey, TargetPath) ->
    {TargetName, TargetDirPath} = filepath_utils:basename_and_parent_dir(TargetPath),
    mv(Worker, SessId, FileKey, {path, TargetDirPath}, TargetName).


-spec mv(node(), session:id(), lfm:file_key(), lfm:file_key(),
    file_meta:name()) -> {ok, fslogic_worker:file_guid()} | lfm:error_reply().
mv(Worker, SessId, FileKey, TargetParentKey, TargetName) ->
    ?EXEC(Worker, lfm:mv(SessId, FileKey, TargetParentKey, TargetName)).


-spec cp(node(), session:id(), lfm:file_key(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
cp(Worker, SessId, FileKey, TargetPath) ->
    {TargetName, TargetDirPath} = filepath_utils:basename_and_parent_dir(TargetPath),
    cp(Worker, SessId, FileKey, {path, TargetDirPath}, TargetName).


-spec cp(node(), session:id(), lfm:file_key(), lfm:file_key(),
    file_meta:name()) -> {ok, fslogic_worker:file_guid()} | lfm:error_reply().
cp(Worker, SessId, FileKey, TargetParentKey, TargetName) ->
    ?EXEC(Worker, lfm:cp(SessId, FileKey, TargetParentKey, TargetName)).


-spec is_dir(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path()) ->
    boolean() | lfm:error_reply().
is_dir(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:is_dir(SessId, uuid_to_file_ref(Worker, FileKey))).


%%%===================================================================
%%% Regular file specific operations
%%%===================================================================


-spec get_file_location(node(), session:id(), FileKey :: lfm:file_key() | file_meta:uuid_or_path()) ->
    {ok, file_location:record()} | lfm:error_reply().
get_file_location(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_file_location(SessId, uuid_to_file_ref(Worker, FileKey))).


-spec read_symlink(node(), session:id(), FileKey :: lfm:file_key()) ->
    {ok, file_meta_symlinks:symlink()} | lfm:error_reply().
read_symlink(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:read_symlink(SessId, uuid_to_file_ref(Worker, FileKey))).


-spec create(node(), session:id(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
create(Worker, SessId, FilePath) ->
    ?EXEC(Worker, lfm:create(SessId, FilePath)).


-spec create(node(), session:id(), file_meta:path(), file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
create(Worker, SessId, FilePath, Mode) ->
    ?EXEC(Worker, lfm:create(SessId, FilePath, Mode)).


-spec create(node(), session:id(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
create(Worker, SessId, ParentGuid, Name, Mode) ->
    ?EXEC(Worker, lfm:create(SessId, ParentGuid, Name, Mode)).


-spec make_link(node(), session:id(), file_meta:path(), fslogic_worker:file_guid()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_link(Worker, SessId, LinkPath, FileGuid) ->
    {LinkName, TargetParentPath} = filepath_utils:basename_and_parent_dir(LinkPath),
    make_link(Worker, SessId, ?FILE_REF(FileGuid), {path, TargetParentPath}, LinkName).


-spec make_link(node(), session:id(), lfm:file_key(), lfm:file_key(), file_meta:name()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_link(Worker, SessId, FileKey, TargetParentKey, Name) ->
    ?EXEC(Worker, lfm:make_link(SessId, FileKey, TargetParentKey, Name)).


-spec make_symlink(node(), session:id(), file_meta:path(), file_meta_symlinks:symlink()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_symlink(Worker, SessId, SymlinkPath, SymlinkValue) ->
    {Name, TargetParentPath} = filepath_utils:basename_and_parent_dir(SymlinkPath),
    make_symlink(Worker, SessId, {path, TargetParentPath}, Name, SymlinkValue).


-spec make_symlink(node(), session:id(), ParentKey :: lfm:file_key(),
    Name :: file_meta:name(), LinkTarget :: file_meta_symlinks:symlink()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_symlink(Worker, SessId, ParentKey, Name, Link) ->
    ?EXEC(Worker, lfm:make_symlink(SessId, ParentKey, Name, Link)).


-spec create_and_open(node(), session:id(), file_meta:path()) ->
    {ok, {fslogic_worker:file_guid(), lfm:handle()}} |
    lfm:error_reply().
create_and_open(Worker, SessId, Path) ->
    create_and_open(Worker, SessId, Path, undefined).


-spec create_and_open(node(), session:id(), file_meta:path(), undefined | file_meta:posix_permissions()) ->
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


-spec open(node(), session:id(), FileKey :: lfm:file_key() | file_meta:uuid_or_path(),
    OpenType :: helpers:open_flag()) ->
    {ok, lfm:handle()} | lfm:error_reply().
open(Worker, SessId, FileKey, OpenFlag) ->
    ?EXEC(Worker,
        case lfm:open(SessId, uuid_to_file_ref(Worker, FileKey), OpenFlag) of
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


-spec check_size_and_read(node(), lfm:handle(), integer(), integer()) ->
    {ok, binary()} | lfm:error_reply().
check_size_and_read(Worker, TestHandle, Offset, Size) ->
    ?EXEC(Worker,
        begin
            [{_, Handle}] = ets:lookup(lfm_handles, TestHandle),
            case lfm:check_size_and_read(Handle, Offset, Size) of
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
                            {ok, Res, lfm:stat(SessId, ?FILE_REF(Guid))};
                        Other2 ->
                            Other2
                    end;
                Other -> Other
            end
        end).


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


-spec truncate(node(), session:id(), lfm:file_key() | file_meta:uuid(), non_neg_integer()) ->
    term().
truncate(Worker, SessId, FileKey, Size) ->
    ?EXEC(Worker, lfm:truncate(SessId, uuid_to_file_ref(Worker, FileKey), Size)).


%%%===================================================================
%%% Directory specific operations
%%%===================================================================


-spec mkdir(node(), session:id(), binary()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
mkdir(Worker, SessId, Path) ->
    mkdir(Worker, SessId, Path, undefined).


-spec mkdir(node(), session:id(), binary(), undefined | file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | lfm:error_reply().
mkdir(Worker, SessId, Path, Mode) ->
    ?EXEC(Worker, lfm:mkdir(SessId, Path, Mode)).


-spec mkdir(node(), session:id(), fslogic_worker:file_guid(),
    file_meta:name(), file_meta:posix_permissions()) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | lfm:error_reply().
mkdir(Worker, SessId, ParentGuid, Name, Mode) ->
    ?EXEC(Worker, lfm:mkdir(SessId, ParentGuid, Name, Mode)).


-spec create_dir_at_path(node(), session:id(), fslogic_worker:file_guid(), file_meta:path()) ->
    {ok, #file_attr{}} | lfm:error_reply().
create_dir_at_path(Worker, SessId, ParentGuid, Path) ->
    ?EXEC(Worker, lfm:create_dir_at_path(SessId, ParentGuid, Path)).


%% @TODO VFS-7327 change all usages to get_children_attrs
-spec get_children(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(),
    file_listing:options()) -> 
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}], file_listing:pagination_token()} | lfm:error_reply().
get_children(Worker, SessId, FileKey, ListOpts) ->
    case ?EXEC(Worker, lfm:get_children_attrs(SessId, uuid_to_file_ref(Worker, FileKey), ListOpts, [guid, name])) of
        {ok, Children, Token} ->
            {ok, [{Guid, Name} || #file_attr{name = Name, guid = Guid} <- Children], Token};
        Error ->
            Error
    end.


%% @TODO VFS-7327 change all usages to get_children_attrs
-spec get_children(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(),
    file_listing:offset(), file_listing:limit()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}]} | lfm:error_reply().
get_children(Worker, SessId, FileKey, Offset, Limit) ->
    %% @TODO VFS-7327 use get_children/4 function accepting options map everywhere in tests
    case get_children(Worker, SessId, FileKey, #{offset => Offset, limit => Limit, tune_for_large_continuous_listing => false}) of
        {ok, List, _ListingToken} -> {ok, List};
        {error, _} = Error -> Error
    end.


-spec get_children_attrs(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(),
    file_listing:options()) -> {ok, [#file_attr{}], file_listing:pagination_token()} | lfm:error_reply().
get_children_attrs(Worker, SessId, FileKey, ListOpts) ->
    ?EXEC(Worker, lfm:get_children_attrs(SessId, uuid_to_file_ref(Worker, FileKey), ListOpts)).


-spec get_children_attrs(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(),
    file_listing:options(), [onedata_file:attr_name()]) ->
    {ok, [#file_attr{}], file_listing:pagination_token()} | lfm:error_reply().
get_children_attrs(Worker, SessId, FileKey, ListOpts, Attributes) ->
    ?EXEC(Worker, lfm:get_children_attrs(SessId, uuid_to_file_ref(Worker, FileKey), ListOpts, Attributes)).


-spec get_child_attr(node(), session:id(), fslogic_worker:file_guid(), file_meta:name()) ->
    {ok, lfm_attrs:file_attributes()} | lfm:error_reply().
get_child_attr(Worker, SessId, ParentGuid, ChildName) ->
    ?EXEC(Worker, lfm:get_child_attr(SessId, ParentGuid, ChildName)).


-spec get_files_recursively(
    node(),
    session:id(),
    lfm:file_key(),
    dir_req:recursive_listing_opts(),
    [onedata_file:attr_name()]
) ->
    {ok, [file_attr:record()], [file_meta:path()], recursive_listing:pagination_token()} | lfm:error_reply().
get_files_recursively(Worker, SessId, FileKey, Options, Attributes) ->
    ?EXEC(Worker, lfm:get_files_recursively(SessId, uuid_to_file_ref(Worker, FileKey), Options, Attributes)).


%%%===================================================================
%%% Xattrs related operations
%%%===================================================================


-spec get_xattr(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(), onedata_file:xattr_name()) ->
    {ok, #xattr{}} | lfm:error_reply().
get_xattr(Worker, SessId, FileKey, XattrKey) ->
    get_xattr(Worker, SessId, FileKey, XattrKey, false).


-spec get_xattr(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(), onedata_file:xattr_name(), boolean()) ->
    {ok, #xattr{}} | lfm:error_reply().
get_xattr(Worker, SessId, FileKey, XattrKey, Inherited) ->
    ?EXEC(Worker, lfm:get_xattr(SessId, uuid_to_file_ref(Worker, FileKey), XattrKey, Inherited)).


-spec set_xattr(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(), #xattr{}) ->
    ok | lfm:error_reply().
set_xattr(Worker, SessId, FileKey, Xattr) ->
    set_xattr(Worker, SessId, FileKey, Xattr, false, false).


-spec set_xattr(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(), #xattr{},
    Create :: boolean(), Replace :: boolean()) ->
    ok | lfm:error_reply().
set_xattr(Worker, SessId, FileKey, Xattr, Create, Replace) ->
    ?EXEC(Worker, lfm:set_xattr(SessId, uuid_to_file_ref(Worker, FileKey), Xattr, Create, Replace)).


-spec remove_xattr(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(), onedata_file:xattr_name()) ->
    ok | lfm:error_reply().
remove_xattr(Worker, SessId, FileKey, XattrKey) ->
    ?EXEC(Worker, lfm:remove_xattr(SessId, uuid_to_file_ref(Worker, FileKey), XattrKey)).


-spec list_xattr(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(), boolean(), boolean()) ->
    {ok, [onedata_file:xattr_name()]} | lfm:error_reply().
list_xattr(Worker, SessId, FileKey, Inherited, ShowInternal) ->
    ?EXEC(Worker, lfm:list_xattr(SessId, uuid_to_file_ref(Worker, FileKey), Inherited, ShowInternal)).


%%%===================================================================
%%% ACL related operations
%%%===================================================================


-spec get_acl(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path()) ->
    {ok, acl:acl()} | lfm:error_reply().
get_acl(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:get_acl(SessId, uuid_to_file_ref(Worker, FileKey))).


-spec set_acl(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path(), acl:acl()) ->
    ok | lfm:error_reply().
set_acl(Worker, SessId, FileKey, EntityList) ->
    ?EXEC(Worker, lfm:set_acl(SessId, uuid_to_file_ref(Worker, FileKey), EntityList)).


-spec remove_acl(node(), session:id(), lfm:file_key() | file_meta:uuid_or_path()) ->
    ok | lfm:error_reply().
remove_acl(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:remove_acl(SessId, uuid_to_file_ref(Worker, FileKey))).


%%%===================================================================
%%% Metadata related operations
%%%===================================================================


-spec has_custom_metadata(node(), session:id(), lfm:file_key()) ->
    {ok, boolean()}.
has_custom_metadata(Worker, SessId, FileKey) ->
    ?EXEC(Worker, lfm:has_custom_metadata(SessId, FileKey)).


%%%===================================================================
%%% Multipart upload related operations
%%%===================================================================

-spec create_multipart_upload(node(), session:id(), od_space:id(), file_meta:path()) ->
    {ok, multipart_upload:id()} | {error, term()}.
create_multipart_upload(Worker, SessId, SpaceId, Path) ->
    ?EXEC(Worker, lfm:create_multipart_upload(SessId, SpaceId, Path)).


-spec abort_multipart_upload(node(), session:id(), multipart_upload:id()) -> ok | {error, term()}.
abort_multipart_upload(Worker, SessId, UploadId) ->
    ?EXEC(Worker, lfm:abort_multipart_upload(SessId, UploadId)).


-spec complete_multipart_upload(node(), session:id(), multipart_upload:id()) -> ok | {error, term()}.
complete_multipart_upload(Worker, SessId, UploadId) ->
    ?EXEC(Worker, lfm:complete_multipart_upload(SessId, UploadId)).


-spec list_multipart_uploads(node(), session:id(), od_space:id(), non_neg_integer(), multipart_upload:pagination_token() | undefined) ->
    {ok, [multipart_upload:record()], multipart_upload:pagination_token(), boolean()} | {error, term()}.
list_multipart_uploads(Worker, SessId, SpaceId, Limit, Token) ->
    ?EXEC(Worker, lfm:list_multipart_uploads(SessId, SpaceId, Limit, Token)).


-spec upload_multipart_part(node(), session:id(), multipart_upload:id(), multipart_upload_part:record()) ->
    ok | {error, term()}.
upload_multipart_part(Worker, SessId, UploadId, Part) ->
    ?EXEC(Worker, lfm:upload_multipart_part(SessId, UploadId, Part)).


-spec list_multipart_parts(node(), session:id(), multipart_upload:id(), non_neg_integer(), multipart_upload_part:part_number()) ->
    {ok, [multipart_upload_part:record()], boolean()} | {error, term()}.
list_multipart_parts(Worker, SessId, UploadId, Limit, StartAfter) ->
    ?EXEC(Worker, lfm:list_multipart_parts(SessId, UploadId, Limit, StartAfter)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


exec(Worker, Fun) ->
    exec(Worker, Fun, timer:minutes(2)).


exec(Worker, Fun, Timeout) ->
    Host = self(),
    Pid = spawn_link(Worker,
        fun() ->
            try
                Fun(Host)
            catch
                _:Reason:Stacktrace ->
                    Host ! {self(), {error, {test_exec, Reason, Stacktrace}}}
            end
        end),
    receive
        {Pid, Result} -> Result
    after Timeout ->
        {error, test_timeout}
    end.


uuid_to_file_ref(W, {uuid, Uuid}) ->
    ?FILE_REF(uuid_to_file_ref(W, Uuid));
uuid_to_file_ref(W, Uuid) when is_binary(Uuid) ->
    rpc:call(W, fslogic_file_id, uuid_to_guid, [Uuid]);
uuid_to_file_ref(_, Other) ->
    Other.
