%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module implementing model for storing multipart upload parts.
%%% @end
%%%-------------------------------------------------------------------
-module(multipart_upload_part).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% functions operating on document using datastore model API
-export([create/2, list/3, cleanup/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE
}).

-type record() :: #multipart_upload_part{}.
-type part() :: non_neg_integer().

-export_type([record/0, part/0]).

-define(DEFAULT_LIST_LIMIT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(multipart_upload:id(), record()) -> ok.
create(UploadId, #multipart_upload_part{number = PartNumber} = MultipartUploadPart) ->
    PartId = get_part_id(UploadId, PartNumber),
    {ok, _} = datastore_model:save(?CTX, #document{
        key = PartId,
        value = MultipartUploadPart}
    ),
    ok = ?extract_ok(?ok_if_exists(datastore_model:add_links(?CTX, UploadId, oneprovider:get_id(), {PartNumber, PartId}))).


-spec list(multipart_upload:id(), non_neg_integer(), non_neg_integer()) -> 
    {ok, [record()], boolean()}.
list(UploadId, Limit, StartAfter) ->
    FoldFun = fun(#link{target = PartId}, Acc) -> 
        {ok, [PartId | Acc]} 
    end,
    {ok, ReversedPartIds} = datastore_model:fold_links(?CTX, UploadId, oneprovider:get_id(), FoldFun, [], #{
        size => Limit,
        prev_link_name  => StartAfter,
        prev_tree_id => oneprovider:get_id()
    }),
    ReversedParts = lists_utils:pmap(fun(PartId) ->
        {ok, #document{value = MultipartPart}} = datastore_model:get(?CTX, PartId),
        MultipartPart
    end, ReversedPartIds),
    {ok, lists:reverse(ReversedParts), length(ReversedParts) < Limit}.


-spec cleanup(multipart_upload:id()) -> ok.
cleanup(UploadId) ->
    {ok, List, IsLast} = list(UploadId, ?DEFAULT_LIST_LIMIT, 0),
    lists:foreach(fun(#multipart_upload_part{number = PartNumber}) ->
        ok = datastore_model:delete(?CTX, get_part_id(UploadId, PartNumber)),
        ok = datastore_model:delete_links(?CTX, UploadId, oneprovider:get_id(), PartNumber)
    end, List),
    case IsLast of
        true -> ok;
        false -> cleanup(UploadId)
    end.

%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec get_part_id(multipart_upload:id(), non_neg_integer()) -> binary().
get_part_id(UploadId, PartNumber) ->
    datastore_key:new_from_digest([UploadId, PartNumber]).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {number, integer},
        {size, integer},
        {etag, binary},
        {last_modified, integer}
    ]}.
