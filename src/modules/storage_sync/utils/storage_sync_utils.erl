%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains helper functions for storage_sync
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_utils).
-author("Jakub Kudzia").

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/errors.hrl").


%% API
-export([take_children_storage_ctxs_for_batch/2, take_hash_for_batch/2, module/1,
    all_children_imported/2]).

%%-------------------------------------------------------------------
%% @doc
%% Takes list of storage_file_ctxs for given batch from job Data.
%% @end
%%-------------------------------------------------------------------
-spec take_children_storage_ctxs_for_batch(non_neg_integer(),
    space_strategy:job_data()) -> {undefined | [storage_file_ctx:ctx()], space_strategy:job_data()}.
take_children_storage_ctxs_for_batch(BatchKey, Data) ->
    recursive_take([children_storage_file_ctxs, BatchKey], Data).

%%-------------------------------------------------------------------
%% @doc
%% Takes hash of file attributes for given batch from job Data.
%% @end
%%-------------------------------------------------------------------
-spec take_hash_for_batch(non_neg_integer(), space_strategy:job_data()) ->
    {binary() | undefined, space_strategy:job_data()}.
take_hash_for_batch(BatchKey, Data) ->
    recursive_take([hashes_map, BatchKey], Data).

%%-------------------------------------------------------------------
%% @doc
%% Returns module responsible for handling given strategy_type job.
%% @end
%%-------------------------------------------------------------------
-spec module(space_strategy:job()) -> atom().
module(#space_strategy_job{strategy_type = Module}) ->
    Module.

%%-------------------------------------------------------------------
%% @doc
%% Check whether first job of list of subjobs matches given file.
%% If true it means that some children haven't been imported yet.
%% @end
%%-------------------------------------------------------------------
-spec all_children_imported([space_strategy:job()], file_meta:uuid()) ->
    boolean().
all_children_imported([], _FileUuid) -> true;
all_children_imported(Jobs, FileUuid) ->
    not job_matches_file(hd(Jobs), FileUuid).

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether job matches file with name FileName and given FileCtx.
%% @end
%%-------------------------------------------------------------------
-spec job_matches_file(space_strategy:job(), file_meta:uuid()) -> boolean().
job_matches_file(#space_strategy_job{data = #{
    file_name := FileName,
    parent_ctx := ParentCtx
}}, FileUuid) ->
    try
        {FileCtx2, _} = file_ctx:get_child(
            ParentCtx, FileName, user_ctx:new(?ROOT_SESS_ID)),
        FileUuid =:= file_ctx:get_uuid_const(FileCtx2)
    catch
        throw:?ENOENT ->
            false
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Extension of maps:take function. Calls take on the lowest nested map,
%% and returns tuple {Value, UpdatedMap}.
%% @end
%%-------------------------------------------------------------------
-spec recursive_take(term(), map()) -> {term(), map()}.
recursive_take([Key], Map) ->
    case maps:take(Key, Map) of
        error ->
            {undefined, Map};
        {Value, Map2} ->
            {Value, Map2}
    end;
recursive_take([Key | Keys], Map) ->
    SubMap = maps:get(Key, Map, #{}),
    {Value, SubMap2} = recursive_take(Keys, SubMap),
    {Value, Map#{Key => SubMap2}};
recursive_take(Key, Map) ->
    recursive_take([Key], Map).
