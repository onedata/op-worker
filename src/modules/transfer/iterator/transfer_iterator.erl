%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module defines iterator functionality to be implemented by any
%%% possible data source in transfer.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_iterator).
-author("Bartosz Walkowicz").

-include("modules/datastore/transfer.hrl").

%% API
-export([
    build/1,
    get_next_batch/3
]).

-type record() ::
    transfer_file_tree_iterator:record() |
    transfer_view_iterator:record().

-export_type([record/0]).



%%%===================================================================
%%% Callbacks
%%%===================================================================


%% NOTE: 'get_next_batch' takes as 2nd argument integer that defines desired number of entries.
%% However, due to inner workings of iterators, there can be less or more returned entries.
-callback get_next_batch(user_ctx:ctx(), ApproxLimit :: pos_integer(), record()) ->
    {more | done, [error | {ok, file_ctx:ctx()}], record()} |
    {error, term()}.


%%%===================================================================
%%% API
%%%===================================================================


-spec build(transfer:doc()) -> record().
build(#document{key = TransferId, value = #transfer{
    file_uuid = FileUuid,
    space_id = SpaceId,
    index_name = undefined
}}) ->
    transfer_file_tree_iterator:build(TransferId, file_ctx:new_by_uuid(FileUuid, SpaceId));

build(#document{key = TransferId, value = #transfer{
    space_id = SpaceId,
    index_name = ViewName,
    query_view_params = QueryViewParams
}}) ->
    transfer_view_iterator:build(SpaceId, TransferId, ViewName, QueryViewParams).


-spec get_next_batch(user_ctx:ctx(), pos_integer(), record()) ->
    {more | done, [{ok, file_ctx:ctx()}], record()} |
    {error, term()}.
get_next_batch(UserCtx, Limit, Iterator) ->
    RecordType = utils:record_type(Iterator),
    RecordType:get_next_batch(UserCtx, Limit, Iterator).
