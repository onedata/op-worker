%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions responsible for traversing view and
%%% transferring regular files.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_view_traverse).
-author("Bartosz Walkowicz").

-behaviour(view_traverse).

-include_lib("ctool/include/logging.hrl").

%% view_traverse callbacks
-export([process_row/3, task_finished/1, task_canceled/1]).


%%%===================================================================
%%% view_traverse callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link view_traverse} callback process_row/3.
%% @end
%%--------------------------------------------------------------------
-spec process_row(json_utils:json_map(), transfer_traverse_worker:traverse_info(), non_neg_integer()) ->
    ok.
process_row(Row, TraverseInfo = #{
    space_id := SpaceId,
    transfer_id := TransferId,
    view_name := ViewName
}, _RowNumber) ->
    FileCtxsOrErrors = lists:filtermap(fun(ObjectId) ->
        case resolve_file(TransferId, SpaceId, ViewName, ObjectId) of
            ignore -> false;
            Result -> {true, Result}
        end
    end, get_object_ids(Row)),

    transfer:increment_files_to_process_counter(TransferId, length(FileCtxsOrErrors)),

    lists:foreach(fun
        ({ok, FileCtx}) ->
            transfer_traverse_worker:process_file(TransferId, TraverseInfo, FileCtx);
        (error) ->
            transfer:increment_files_failed_and_processed_counters(TransferId)
    end, FileCtxsOrErrors).


-spec task_finished(transfer:id()) -> ok.
task_finished(TransferId) ->
    transfer:mark_traverse_finished(TransferId),
    ok.


-spec task_canceled(transfer:id()) -> ok.
task_canceled(TransferId) ->
    task_finished(TransferId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_object_ids(map()) -> [binary()].
get_object_ids(#{<<"value">> := ObjectIds}) when is_list(ObjectIds) ->
    lists:flatten(ObjectIds);
get_object_ids(#{<<"value">> := ObjectId}) ->
    [ObjectId].


%% @private
-spec resolve_file(transfer:id(), od_space:id(), transfer:view_name(), file_id:objectid()) ->
    ignore | error | {ok, file_ctx:ctx()}.
resolve_file(TransferId, SpaceId, ViewName, ObjectId) ->
    try
        {ok, FileGuid} = file_id:objectid_to_guid(ObjectId),
        FileCtx0 = file_ctx:new_by_guid(FileGuid), % TODO VFS-7443 - maybe use referenced guid?

        case file_ctx:file_exists_const(FileCtx0) of
            true ->
                % TODO VFS-6386 Enable and test view transfer with dirs
                case file_ctx:is_dir(FileCtx0) of
                    {true, _} ->
                        ignore;
                    {false, FileCtx1} ->
                        {ok, FileCtx1}
                end;
            false ->
                % TODO VFS-4218 currently we silently omit garbage
                % returned from view (view can return anything)
                ignore
        end
    catch Type:Reason:Stacktrace ->
        ?error_stacktrace(
            "Resolution of file id ~tp returned by querying view ~tp in space ~tp "
            "during transfer ~ts failed due to:~n~tp:~tp",
            [ObjectId, ViewName, SpaceId, TransferId, Type, Reason],
            Stacktrace
        ),
        error
    end.
