%%%%%-------------------------------------------------------------------
%%%%% @author Jakub Kudzia
%%%%% @copyright (C) 2020 ACK CYFRONET AGH
%%%%% This software is released under the MIT license
%%%%% cited in 'LICENSE.txt'.
%%%%% @end
%%%%%-------------------------------------------------------------------
%%%%% @doc
%%%%% WRITEME
%%%%% @end
%%%%%-------------------------------------------------------------------
-module(dir_deletion_traverse).
%%-author("Jakub Kudzia").

%%-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/2]).

%% Traverse behaviour callbacks
-export([
    task_started/2,
    task_finished/2,
    get_job/1,
    update_job_progress/5,
    do_master_job/2,
    do_slave_job/2
]).

% TODO
% TODO * cyz potrzebuje dokument w ktorym bede zapisywal, ze wszystkie batche zostaly juz przetworzne?
% TODO * moze ten sam dokument pozoli mi stiwerdzic, ze traverse usuwajacy ten katalog jest juz w trakcie zeby
% TODO   nie zlecac 2 razy

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    MasterJobsLimit = application:get_env(?APP_NAME, dir_deletion_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, dir_deletion_traverse_slave_jobs_limit, 10),
    ParallelismLimit = application:get_env(?APP_NAME, dir_deletion_traverse_parallelism_limit, 10),

    tree_traverse:init(?POOL_NAME, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    traverse:stop_pool(?POOL_NAME).


%%-spec start(file_ctx:ctx()) -> {ok, id()}.
start(DirCtx, UserCtx) ->
%%    TaskId = file_ctx:get_guid_const(DirCtx), % todo czy nie bedzie problemu jak,
%%    ?alert("TaskId: ~p", [TaskId]),
    Options = #{
        traverse_info => #{
            user_ctx => UserCtx,
            space_id => file_ctx:get_space_id_const(DirCtx)
        }
    },
    % todo trzeba ogarnac rozne runy dla tego samego katalopgu
    tree_traverse:run(?POOL_NAME, DirCtx, Options).

% todo zrobic skic implementacji usuwania katalogu podanego po file_ctx (?)

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

task_started(TaskId, Pool) ->
    ?critical("dir deletion job ~p started", [TaskId]).

task_finished(TaskId, Pool) ->
    ?critical("dir deletion job ~p finished", [TaskId]).

%%-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
%%    {ok, tree_traverse:master_job(), traverse:pool(), id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

%%-spec update_job_progress(undefined | main_job | traverse:job_id(),
%%    tree_traverse:master_job(), traverse:pool(), id(),
%%    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

%%-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
%%    {ok, traverse:master_job_map()}.
do_master_job(#tree_traverse{
    doc = #document{key = FileUuid, value = #file_meta{name = DirName, parent_uuid = ParentUuid}},
    traverse_info = #{
        user_ctx := UserCtx,
        space_id := SpaceId
    }} = Job,
    MasterJobArgs
) ->
    ?alert("MASTER JOB: ~p", [DirName]),
    % todo moze async?
    {ok, MasterJobMap} = tree_traverse:do_master_job(Job, MasterJobArgs),
    IsLast = case maps:get(master_jobs, MasterJobMap, []) of
        [#tree_traverse{doc = #document{key = FileUuid}} | _] -> false;
        _ -> true
    end,



    {ok, MasterJobMap#{
        finish_callback =>  fun(_MasterJobExtendedArgs, SlavesDescription) ->
            % todo trzeba zapewnic, ze to jest robione w ostatnim batchu !!!!!
            case maps:get(slave_jobs_failed, SlavesDescription) of
                0 ->
                    case IsLast of
                        true ->
                            FileCtx = file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)),

                        false ->
                            ok
                    end;
                _ ->
                    ok
            end
        end
    }}.

%%-spec do_slave_job(traverse:job(), id()) -> ok.
do_slave_job({#document{key = FileUuid, value = #file_meta{name = Name}} = Doc, TraverseInfo = #{
    user_ctx := UserCtx,
    space_id := SpaceId
}}, TaskId) ->
    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)),
    Result = ok,
%%    Result = fslogic_delete:delete_file_locally(UserCtx, FileCtx, false),
    Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================

