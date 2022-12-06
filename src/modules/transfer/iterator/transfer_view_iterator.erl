%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides `transfer_iterator` functionality for db view.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_view_iterator).
-author("Bartosz Walkowicz").

-behaviour(transfer_iterator).

-include_lib("ctool/include/logging.hrl").

%% API
-export([
    build/4,
    get_next_batch/3
]).

-define(DOC_ID_MISSING, doc_id_missing).

-record(transfer_view_iterator, {
    space_id :: od_space:id(),
    transfer_id :: transfer:id(),
    view_name :: transfer:view_name(),
    query_view_params :: transfer:query_view_params(),
    last_doc_id :: undefined | ?DOC_ID_MISSING | file_meta:uuid()
}).
-type record() :: #transfer_view_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(od_space:id(), transfer:id(), transfer:view_name(), transfer:query_view_params()) ->
    record().
build(SpaceId, TransferId, ViewName, QueryViewParams) ->
    #transfer_view_iterator{
        space_id = SpaceId,
        transfer_id = TransferId,
        view_name = ViewName,
        query_view_params = QueryViewParams,
        last_doc_id = undefined
    }.


-spec get_next_batch(user_ctx:ctx(), pos_integer(), record()) ->
    {more | done, [error | {ok, file_ctx:ctx()}], record()} |
    {error, term()}.
get_next_batch(_UserCtx, Limit, Iterator = #transfer_view_iterator{
    space_id = SpaceId,
    transfer_id = TransferId,
    view_name = ViewName,
    query_view_params = QueryViewParams,
    last_doc_id = LastDocId
}) ->
    QueryViewParams2 = case LastDocId of
        undefined ->
            [{skip, 0} | QueryViewParams];
        ?DOC_ID_MISSING ->
            % doc_id is missing when view has reduce function defined
            % in such case we must iterate over results using limit and skip
            [{skip, Limit} | QueryViewParams];
        _ ->
            [{skip, 1}, {startkey_docid, LastDocId} | QueryViewParams]
    end,

    case index:query(SpaceId, ViewName, [{limit, Limit} | QueryViewParams2]) of
        {ok, #{<<"rows">> := Rows}} ->
            ProgressMarker = case length(Rows) < Limit of
                true -> done;
                false -> more
            end,

            {NewLastDocId, Results} = lists:foldl(fun(Row, {_LastDocId, OuterAcc0}) ->
                DocId = maps:get(<<"id">>, Row, ?DOC_ID_MISSING),

                OuterAcc1 = lists:foldl(fun(ObjectId, InnerAcc) ->
                    case resolve_file(SpaceId, ViewName, ObjectId) of
                        ignore -> InnerAcc;
                        Result -> [Result | InnerAcc]
                    end
                end, OuterAcc0, get_object_ids(Row)),

                {DocId, OuterAcc1}
            end, {undefined, []}, Rows),

            NewIterator = Iterator#transfer_view_iterator{
                last_doc_id = NewLastDocId
            },

            {ProgressMarker, lists:reverse(Results), NewIterator};

        {error, Reason} = Error ->
            ?error("Querying view ~p failed due to ~p when processing transfer ~p", [
                ViewName, Reason, TransferId
            ]),
            Error
    end.


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
-spec resolve_file(od_space:id(), transfer:view_name(), file_id:objectid()) ->
    ignore | error | {ok, file_ctx:ctx()}.
resolve_file(SpaceId, ViewName, ObjectId) ->
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
            "Processing result of query view ~p in space ~p failed due to ~p:~p",
            [ViewName, SpaceId, Type, Reason],
            Stacktrace
        ),
        error
    end.
