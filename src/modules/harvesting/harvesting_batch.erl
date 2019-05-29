%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module used by modules associated with harvesting metadata.
%%% It implements a simple data structure that is used for storing
%%% metadata changes.
%%% By default, elements in the batch are stored in the map, where
%%% metadata doc is associated with file_id:objectid().
%%% The batch can later be sorted and encoded for sending it to Onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_batch).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/0, size/1, is_empty/1, add_object/2, sort_and_encode/1,
    get_first_seq/1, get_last_seq/1, get_sorted_batch/1, strip_batch/2]).

-record(harvesting_batch, {
    first_seq :: seq(),
    last_seq :: seq(),
    batch = #{} :: batch_map() | batch_list(),
    size = 0
}).

-type seq() :: custom_metadata:seq() | undefined.
-type operation() :: binary(). % ?SUBMIT | ?DELETE
-type file_id() :: file_id:objectid().
-type doc() :: custom_metadata:doc().
-type object() :: #{binary() => non_neg_integer() | binary() | object()}.
-type batch_map() :: #{file_id() => object()}.
-type batch_list() :: [object()].
-type batch() :: #harvesting_batch{}.

-define(SUBMIT, <<"submit">>).
-define(DELETE, <<"delete">>).

-export_type([batch/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init() -> batch().
init() ->
    #harvesting_batch{}.

-spec size(batch()) -> non_neg_integer().
size(#harvesting_batch{size = Size}) ->
    Size.

-spec is_empty(batch()) -> boolean().
is_empty(#harvesting_batch{size = Size}) ->
    Size =:= 0.

-spec add_object(doc(), batch()) -> batch().
add_object(Doc = #document{
    value = #custom_metadata{
        file_objectid = FileId,
        value = Metadata
    },
    deleted = false
}, HB = #harvesting_batch{batch = Batch, size = Size}) when map_size(Metadata) > 0 ->
    HB#harvesting_batch{
        batch = Batch#{FileId => submit_object(Doc)},
        size = Size + 1
    };
add_object(Doc = #document{
    value = #custom_metadata{
        file_objectid = FileId
    }
}, HB = #harvesting_batch{batch = Batch, size = Size}) ->
    % delete entry because one of the following happened:
    %   * #custom_metadata document has_been deleted
    %   * #custom_metadata.value map is empty
    HB#harvesting_batch{
        batch = Batch#{FileId => delete_object(Doc)},
        size = Size + 1
    }.


%%-------------------------------------------------------------------
%% @doc
%% Prepares harvesting_batch for sending to Onezone.
%% Objects stored in batch_map() are encoded, sorted by sequence number
%% and stored in a batch_map().
%% If batch is already prepared, the function does nothing.
%% @end
%%-------------------------------------------------------------------
-spec sort_and_encode(batch()) -> batch().
sort_and_encode(HB = #harvesting_batch{batch = Batch}) when is_list(Batch) ->
    HB;
sort_and_encode(HB = #harvesting_batch{batch = Batch}) when map_size(Batch) =:= 0 ->
    HB#harvesting_batch{
        batch = [],
        first_seq = undefined,
        last_seq = undefined
    };
sort_and_encode(HB = #harvesting_batch{batch = Batch}) when is_map(Batch) ->
    {EncodedBatch, MaxSeq} = maps:fold(fun
        (_FileId, Object, {AccIn, MaxSeqIn}) ->
            {[maybe_encode_object(Object) | AccIn], max(get_seq(Object), MaxSeqIn)}
    end, {[], 0}, Batch),
    SortedBatch  = [First | _] = lists:sort(fun(#{<<"seq">> := Seq1}, #{<<"seq">> := Seq2}) ->
        Seq1 =< Seq2
    end, EncodedBatch),
    HB#harvesting_batch{
        batch = SortedBatch,
        first_seq = get_seq(First),
        last_seq = MaxSeq
    }.

-spec get_first_seq(batch()) -> seq().
get_first_seq(#harvesting_batch{first_seq = FirstSeq}) ->
    FirstSeq.

-spec get_last_seq(batch()) -> seq().
get_last_seq(#harvesting_batch{last_seq = LastSeq}) ->
    LastSeq.

-spec get_sorted_batch(batch()) -> any().
get_sorted_batch(#harvesting_batch{batch = Batch}) when is_list(Batch) ->
    Batch.

%%-------------------------------------------------------------------
%% @doc
%% Strips all batch elements preceding StripAfter sequence number.
%% @end
%%-------------------------------------------------------------------
-spec strip_batch(batch(), seq()) -> batch().
strip_batch(HB = #harvesting_batch{batch = Batch}, StripAfter) when is_list(Batch) ->
    {StrippedBatchReversed, NewFirstSeqIn, NewSize} = lists:foldl(fun
        (Object, AccIn = {[], undefined, 0}) ->
            case get_seq(Object) of
                Seq when Seq < StripAfter -> AccIn;
                Seq -> {[Object], Seq, 1}
            end;
        (Object, {StrippedBatchIn, NewFirstSeqIn, NewSizeIn}) ->
            {[Object | StrippedBatchIn], NewFirstSeqIn, NewSizeIn + 1}
    end, {[], undefined, 0}, Batch),

    HB#harvesting_batch{
        first_seq = NewFirstSeqIn,
        batch = lists:reverse(StrippedBatchReversed),
        size = NewSize
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec submit_object(doc()) -> object().
submit_object(Doc = #document{value = #custom_metadata{value = Metadata}}) ->
    BaseObject = base_object(Doc, <<"submit">>),
    BaseObject#{<<"payload">> => build_payload(Metadata)}.

-spec delete_object(doc()) -> object().
delete_object(Doc) ->
    base_object(Doc, <<"delete">>).

-spec base_object(doc(), operation()) -> object().
base_object(#document{
    value = #custom_metadata{file_objectid = FileId},
    seq = Seq
}, Operation) ->
    #{
        <<"fileId">> => FileId,
        <<"operation">> => Operation,
        <<"seq">> => Seq
    }.

-spec build_payload(object()) -> object().
build_payload(Metadata) ->
    maps:fold(fun
        (<<"onedata_json">>, JSON, Payload) ->
            Payload#{<<"json">> => JSON};
        (<<"onedata_rdf">>, RDF, Payload) ->
            Payload#{<<"rdf">> => RDF};
        (Key, Value, Payload = #{<<"xattrs">> := Xattrs}) ->
            Payload#{<<"xattrs">> => Xattrs#{Key => Value}};
        (Key, Value, Payload) ->
            Payload#{<<"xattrs">> => #{Key => Value}}
    end, #{}, Metadata).

-spec maybe_encode_object(object()) -> object().
maybe_encode_object(Object = #{<<"operation">> := ?DELETE}) ->
    Object;
maybe_encode_object(Object = #{<<"operation">> := ?SUBMIT, <<"payload">> := Payload}) ->
    Object#{<<"payload">> => encode_payload(Payload)}.

-spec encode_payload(object()) -> object().
encode_payload(Payload) ->
    maps:fold(fun
        (<<"json">>, JSON, PayloadIn) ->
            PayloadIn#{<<"json">> => json_utils:encode(JSON)};
        (<<"rdf">>, RDF, PayloadIn) ->
            PayloadIn#{<<"rdf">> => json_utils:encode(RDF)};
        (<<"xattrs">>, _Value, PayloadIn) ->
            PayloadIn
    end, Payload, Payload).

-spec get_seq(object()) -> seq().
get_seq(#{<<"seq">> := Seq}) ->
    Seq.