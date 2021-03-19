%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for tree traverse during archive download.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_streaming_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([run/3]).
-export([init_pool/0, stop_pool/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2, task_finished/2, task_canceled/2, 
    get_job/1, update_job_progress/5]).

-type id() :: tree_traverse:id().

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

%%%===================================================================
%%% API
%%%===================================================================

-spec run([lfm_attrs:file_attributes()], session:id(), cowboy_req:req()) -> ok | {error, term()}.
run(FileAttrsList, SessionId, CowboyReq) ->
    TaskId = datastore_key:new(),
    case tree_traverse_session:setup_for_task(user_ctx:new(SessionId), TaskId) of
        ok ->
            {ok, UserId} = session:get_user_id(SessionId),
            TarStream = tar_utils:open_archive_stream(),
            {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
            FinalTarStream = stream_many_files(FileAttrsList, TaskId, UserCtx, TarStream, CowboyReq),
            http_streaming_utils:send_data_chunk(tar_utils:close_archive_stream(FinalTarStream), CowboyReq),
            tree_traverse_session:close_for_task(TaskId);
        {error, _} = Error ->
            Error
    end.


-spec init_pool() -> ok  | no_return().
init_pool() ->
    MasterJobsLimit = application:get_env(?APP_NAME, dir_streaming_traverse_master_jobs_limit, 50),
    SlaveJobsLimit = application:get_env(?APP_NAME, dir_streaming_traverse_slave_jobs_limit, 50),
    ParallelismLimit = application:get_env(?APP_NAME, dir_streaming_traverse_parallelism_limit, 50),

    ok = tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec task_finished(id(), traverse:pool()) -> ok.
task_finished(TaskId, PoolName) ->
    {ok, #{ <<"connection_pid">> := EncodedPid }} = traverse_task:get_additional_data(PoolName, TaskId),
    Pid = transfer_utils:decode_pid(EncodedPid),
    Pid ! done,
    ok.

-spec task_canceled(id(), traverse:pool()) -> ok.
task_canceled(_TaskId, _PoolName) ->
    ok.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job() | tree_traverse:slave_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job, MasterJobArgs) ->
    tree_traverse:do_master_job(Job, MasterJobArgs).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{file_ctx = FileCtx, user_id = UserId}, TaskId) ->
    {ok, #{ <<"connection_pid">> := EncodedPid }} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    Pid = transfer_utils:decode_pid(EncodedPid),
    Guid = file_ctx:get_guid_const(FileCtx),
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    {ok, FileAttrs} = lfm:stat(user_ctx:get_session_id(UserCtx), {guid, Guid}),
    Pid ! {file_attrs, FileAttrs, self()},
    case slave_job_loop(Pid) of
        ok -> ok;
        error -> 
            ?debug("Canceling dir streaming traverse ~p due to unexpected exit of connection process ~p.",
                [TaskId, Pid]),
            ok = traverse:cancel(?POOL_NAME, TaskId)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec slave_job_loop(pid()) -> ok | error.
slave_job_loop(Pid) ->
    receive
        done -> ok
    after timer:seconds(5) ->
        case is_process_alive(Pid) of
            true -> slave_job_loop(Pid);
            false -> error
        end
    end.


%% @private
-spec stream_many_files([lfm_attrs:file_attributes()], id(), user_ctx:ctx(), tar_utils:stream(), 
    cowboy_req:req()) -> tar_utils:stream().
stream_many_files([], _TaskId, _UserCtx, TarStream, _CowboyReq) -> 
    TarStream;
stream_many_files(
    [#file_attr{guid = Guid, type = ?DIRECTORY_TYPE} = FileAttrs | Tail],
    TaskId, UserCtx, TarStream, CowboyReq
) ->
    {ok, Path} = get_file_path(Guid),
    PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
    % add starting dir to archive here as traverse do not execute slave job on it
    {Bytes, TarStream1} = new_tar_file_entry(TarStream, FileAttrs, PathPrefix),
    http_streaming_utils:send_data_chunk(Bytes, CowboyReq),
    traverse_task:delete_ended(?POOL_NAME, TaskId), %% @TODO VFS-6212 start traverse with cleanup option
    Options = #{
        task_id => TaskId,
        batch_size => 1,
        children_master_jobs_mode => sync,
        children_dirs_handling_mode => generate_slave_and_master_jobs,
        additional_data => #{<<"connection_pid">> => transfer_utils:encode_pid(self())}
    },
    {ok, _} = tree_traverse:run(
        ?POOL_NAME, file_ctx:new_by_guid(Guid), user_ctx:get_user_id(UserCtx), Options),
    TarStream2 = stream_loop(TarStream1, CowboyReq, user_ctx:get_session_id(UserCtx), PathPrefix),
    stream_many_files(Tail, TaskId, UserCtx, TarStream2, CowboyReq);
stream_many_files(
    [#file_attr{guid = Guid, type = ?REGULAR_FILE_TYPE} = FileAttrs | Tail], 
    TaskId, UserCtx, TarStream, CowboyReq
) ->
    {ok, Path} = get_file_path(Guid),
    PathPrefix = str_utils:ensure_suffix(filename:dirname(Path), <<"/">>),
    TarStream1 = stream_file(TarStream, CowboyReq, user_ctx:get_session_id(UserCtx), FileAttrs, PathPrefix),
    stream_many_files(Tail, TaskId, UserCtx, TarStream1, CowboyReq).


%% @private
-spec stream_file(tar_utils:stream(), cowboy_req:req(), session:id(), lfm_attrs:file_attributes(),
    file_meta:path()) -> tar_utils:stream().
stream_file(TarStream, Req, SessionId, FileAttrs, RootDirPath) ->
    #file_attr{size = FileSize, guid = Guid} = FileAttrs,
    case lfm:monitored_open(SessionId, {guid, Guid}, read) of
        {ok, FileHandle} ->
            {Bytes, TarStream1} = new_tar_file_entry(TarStream, FileAttrs, RootDirPath),
            http_streaming_utils:send_data_chunk(Bytes, Req),
            Range = {0, FileSize - 1},
            ReadBlockSize = http_streaming_utils:get_read_block_size(FileHandle),
            TarStream2 = http_streaming_utils:stream_bytes_range(
                FileHandle, FileSize, Range, Req, fun(D) -> D end, ReadBlockSize, TarStream1),
            lfm:monitored_release(FileHandle),
            TarStream2;
        {error, ?ENOENT} ->
            TarStream;
        {error, ?EACCES} ->
            % ignore files with no access
            TarStream
    end.


%% @private
-spec stream_loop(tar_utils:stream(), cowboy_req:req(), session:id(), file_meta:path()) ->
    tar_utils:stream().
stream_loop(TarStream, Req, SessionId, RootDirPath) ->
    receive
        {file_attrs, #file_attr{type = ?REGULAR_FILE_TYPE} = FileAttrs, Pid} ->
            TarStream2 = stream_file(TarStream, Req, SessionId, FileAttrs, RootDirPath),
            Pid ! done,
            stream_loop(TarStream2, Req, SessionId, RootDirPath);
        {file_attrs, #file_attr{type = ?DIRECTORY_TYPE} = FileAttrs, Pid} ->
            {Bytes, TarStream1} = new_tar_file_entry(TarStream, FileAttrs, RootDirPath),
            http_streaming_utils:send_data_chunk(Bytes, Req),
            Pid ! done,
            stream_loop(TarStream1, Req, SessionId, RootDirPath);
        done -> TarStream
    end.


%% @private
-spec new_tar_file_entry(tar_utils:stream(), lfm_attrs:file_attributes(), file_meta:path()) ->
    {binary(), tar_utils:stream()}.
new_tar_file_entry(TarStream, FileAttrs, StartingDirPath) ->
    #file_attr{mode = Mode, mtime = MTime, type = Type, size = FileSize, guid = Guid} = FileAttrs,
    FinalMode = case {file_id:is_share_guid(Guid), Type} of
        {true, ?REGULAR_FILE_TYPE} -> 8#664;
        {true, ?DIRECTORY_TYPE} -> 8#775;
        {false, _} -> Mode
    end,
    {ok, Path} = get_file_path(Guid),
    Filename = string:prefix(Path, StartingDirPath),
    FileType = case Type of
        ?DIRECTORY_TYPE -> directory;
        ?REGULAR_FILE_TYPE -> regular
    end,
    TarStream1 = tar_utils:new_file_entry(TarStream, Filename, FileSize, FinalMode, MTime, FileType),
    tar_utils:flush(TarStream1).


%% TODO VFS-6057 resolve share path up to share not user root dir
%% @private
-spec get_file_path(fslogic_worker:file_guid()) -> {ok, file_meta:path()} | {error, term()}.
get_file_path(ShareGuid) ->
    {Uuid, SpaceId, _} = file_id:unpack_share_guid(ShareGuid),
    Guid = file_id:pack_guid(Uuid, SpaceId),
    lfm:get_file_path(?ROOT_SESS_ID, Guid).
