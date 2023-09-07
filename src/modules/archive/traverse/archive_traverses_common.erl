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
-export([do_master_job/4]).
-export([update_children_count/4, take_children_count/3]).
-export([execute_unsafe_job/5]).
-export([is_cancelling/1]).

-type error_handler(T) :: fun((tree_traverse:job(), Error :: any(), stacktrace() | undefined) -> T).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec do_master_job(module(), tree_traverse:master_job(), traverse:master_job_extended_args(), 
    error_handler({ok, traverse:master_job_map()})) -> {ok, traverse:master_job_map()} | {error, term()}.
do_master_job(
    TraverseModule, 
    Job = #tree_traverse{file_ctx = FileCtx}, 
    MasterJobArgs,
    ErrorHandler
) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    
    {Module, Function} = case IsDir of
        true -> {TraverseModule, do_dir_master_job_unsafe};
        false -> {tree_traverse, do_master_job}
    end,
    UpdatedJob = Job#tree_traverse{file_ctx = FileCtx2},
    archive_traverses_common:execute_unsafe_job(
        Module, Function, [MasterJobArgs], UpdatedJob, ErrorHandler).


-spec execute_unsafe_job(module(), atom(), [term()], tree_traverse:job(), error_handler(T)) ->
    T.
execute_unsafe_job(Module, JobFunctionName, Options, Job, ErrorHandler) ->
    try
        erlang:apply(Module, JobFunctionName, [Job | Options])
    catch
        _Class:{badmatch, {error, Reason}}:Stacktrace ->
            ErrorHandler(Job, ?ERROR_POSIX(Reason), Stacktrace);
        _Class:Reason:Stacktrace ->
            ErrorHandler(Job, Reason, Stacktrace)
    end.


-spec is_cancelling(archivisation_traverse_ctx:ctx() | archive:doc() | archive:id()) ->
    boolean() | {error, term()}.
is_cancelling(ArchiveId) when is_binary(ArchiveId) ->
    case archive:get(ArchiveId) of
        {ok, #document{value = #archive{state = ?ARCHIVE_CANCELLING(_)}}} ->
            true;
        {ok, #document{}} ->
            false;
        {error, _} = Error ->
            Error
    end;
is_cancelling(#document{key = ArchiveId}) ->
    is_cancelling(ArchiveId);
is_cancelling(ArchiveCtx) ->
    case archivisation_traverse_ctx:get_archive_doc(ArchiveCtx) of
        undefined -> false;
        ArchiveDoc -> is_cancelling(ArchiveDoc)
    end.


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
