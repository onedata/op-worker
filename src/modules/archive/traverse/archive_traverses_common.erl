%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module containing common functions used across archivisation 
%%% traverse modules. 
%%% @end
%%%-------------------------------------------------------------------
-module(archive_traverses_common).
-author("Michal Stanisz").

-include("tree_traverse.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([do_master_job/3]).
-export([update_children_count/4, take_children_count/3]).
-export([execute_unsafe_job/6]).
-export([is_cancelled/1]).

-type error_handler(T) :: fun((tree_traverse:id(), tree_traverse:job(), Error :: any(), Stacktrace :: list()) -> T).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args(), 
    error_handler({ok, traverse:master_job_map()})) -> {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse{file_ctx = FileCtx}, MasterJobArgs = #{task_id := TaskId}, ErrorHandler) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    
    {Module, Function} = case IsDir of
        true -> {?MODULE, do_dir_master_job_unsafe};
        false -> {tree_traverse, do_master_job}
    end,
    UpdatedJob = Job#tree_traverse{file_ctx = FileCtx2},
    archive_traverses_common:execute_unsafe_job(
        Module, Function, [MasterJobArgs], UpdatedJob, TaskId, ErrorHandler).


-spec update_children_count(tree_traverse:pool(), tree_traverse:id(), file_meta:uuid(), non_neg_integer()) ->
    ok.
update_children_count(PoolName, TaskId, DirUuid, ChildrenCount) ->
    ?extract_ok(traverse_task:update_additional_data(traverse_task:get_ctx(), PoolName, TaskId,
        fun(AD) ->
            PrevCountMap = get_count_map(AD),
            UpdatedMap = case PrevCountMap of
                #{DirUuid := PrevCountBin} ->
                    PrevCountMap#{
                        DirUuid => integer_to_binary(binary_to_integer(PrevCountBin) + ChildrenCount)
                    };
                _ ->
                    PrevCountMap#{
                        DirUuid => integer_to_binary(ChildrenCount)
                    }
            end,
            {ok, set_count_map(AD, UpdatedMap)}
        end
    )).


-spec take_children_count(tree_traverse:pool(), tree_traverse:id(), file_meta:uuid()) ->
    non_neg_integer().
take_children_count(PoolName, TaskId, DirUuid) ->
    {ok, AdditionalData} = traverse_task:get_additional_data(PoolName, TaskId),
    ChildrenCount = maps:get(DirUuid, get_count_map(AdditionalData)),
    ok = ?extract_ok(traverse_task:update_additional_data(traverse_task:get_ctx(), PoolName, TaskId,
        fun(AD) ->
            {ok, set_count_map(AD, maps:remove(DirUuid, get_count_map(AD)))}
        end
    )),
    binary_to_integer(ChildrenCount).


-spec execute_unsafe_job(module(), atom(), [term()], tree_traverse:job(), tree_traverse:id(), error_handler(T)) ->
    T.
execute_unsafe_job(Module, JobFunctionName, Options, Job, TaskId, ErrorHandler) ->
    try
        erlang:apply(Module, JobFunctionName, [Job | Options])
    catch
        _Class:{badmatch, {error, Reason}}:Stacktrace ->
            ErrorHandler(TaskId, Job, ?ERROR_POSIX(Reason), Stacktrace);
        _Class:Reason:Stacktrace ->
            ErrorHandler(TaskId, Job, Reason, Stacktrace)
    end.


-spec is_cancelled(archivisation_traverse_ctx:ctx() | archive:doc()) -> boolean() | {error, term()}.
is_cancelled(#document{key = ArchiveId}) ->
    case archive:get(ArchiveId) of
        {ok, #document{value = #archive{state = State}}} ->
            State == ?ARCHIVE_CANCELLING;
        {error, _} = Error ->
            Error
    end;
is_cancelled(ArchiveCtx) ->
    case archive_traverse_ctx:get_archive_doc(ArchiveCtx) of
        undefined -> false;
        ArchiveDoc -> is_cancelled(ArchiveDoc)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_count_map(traverse:additional_data()) -> map().
get_count_map(AD) ->
    CountMapBin = maps:get(<<"children_count_map">>, AD, term_to_binary(#{})),
    binary_to_term(CountMapBin).


-spec set_count_map(traverse:additional_data(), map()) -> traverse:additional_data().
set_count_map(AD, CountMap) ->
    AD#{<<"children_count_map">> => term_to_binary(CountMap)}.
