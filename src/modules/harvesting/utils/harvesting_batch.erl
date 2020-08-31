%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module used by modules associated with harvesting metadata.
%%% It defines a simple data structure that is used for storing
%%% metadata changes, collected from #file_meta{} and #custom_metadata{}
%%% documents.
%%%
%%% BATCH ENTRY
%%% When handling a changed document, received from couchbase_changes_stream,
%%% counterpart document is fetched from the database.
%%% Then, data is extracted from both documents to create a BatchEntry :: batch_entry().
%%% BatchEntry has the following format:
%%% #{
%%%      <<"fileId">> => file_id:objectid(),
%%%      <<"spaceId">> => od_space:id(),
%%%      <<"fileName">> => file_meta:name(),
%%%      <<"operation">> => ?SUBMIT | ?DELETE,
%%%      <<"seq">> => couchbase_changes:seq(),
%%%      <<"payload">> => #{    % optional, makes sense only for ?SUBMIT operation
%%%          <<"json">> => EncodedJSON,
%%%          <<"rdf">> => EncodedRDF,
%%%          <<"xattrs">> => #{binary() => binary()}
%%%      }
%%% }
%%%
%%% ACCUMULATOR
%%% Batch entries should be collected in accumulator() structure. It is a map,
%%% where batch entry is associated with file_id:objectid(), which ensures that
%%% only one and the newest change is associated with given file_id:objectid().
%%%
%%% BATCH
%%% Before sending to Onezone accumulator() must be converted to Batch :: batch()
%%% by calling ?MODULE:prepare_to_send/1 function.
%%% Next, it is necessary to extract BatchEntries :: batch_entries() list
%%% from Batch structure.
%%% BatchEntries has format accepted by graph-sync and can be directly
%%% passed to space_logic:harvest_metadata/5 function.
%%%
%%% NOTE!!!
%%% If you introduce any changes in this module, please ensure that
%%% docs in {@link harvesting_stream} module are up to date.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_batch).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/metadata.hrl").

%% API
-export([new_accumulator/0, size/1, is_empty/1, accumulate/2, prepare_to_send/1,
    get_first_seq/1, get_last_seq/1, get_batch_entries/1, strip/2]).

-record(harvesting_batch, {
    first_seq :: undefined | seq(),
    last_seq :: undefined | seq(),
    entries = [] :: batch_entries(),
    size = 0
}).

-type seq() :: couchbase_changes:seq() | undefined.
-type file_id() :: file_id:objectid().
-type doc() :: custom_metadata:doc() | file_meta:doc().
-type json() :: json_utils:json_term().
-type batch_entry() :: json().
-type accumulator() :: #{file_id() => batch_entry()}.
-type batch_entries() :: [batch_entry()].
-type batch() :: #harvesting_batch{}.

-define(SUBMIT, <<"submit">>).
-define(DELETE, <<"delete">>).

-export_type([batch/0, accumulator/0, batch_entries/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new_accumulator() -> accumulator().
new_accumulator() ->
    #{}.

-spec size(accumulator() | batch()) -> non_neg_integer().
size(Accumulator) when is_map(Accumulator) ->
    map_size(Accumulator);
size(#harvesting_batch{size = Size}) ->
    Size.

-spec is_empty(accumulator() | batch()) -> boolean().
is_empty(Batch) ->
    harvesting_batch:size(Batch) =:= 0.

-spec accumulate(doc(), accumulator()) -> accumulator().
accumulate(Doc = #document{value = #file_meta{}}, Accumulator) ->
    accumulate_file_meta(Doc, Accumulator);
accumulate(Doc = #document{value = #custom_metadata{}}, Accumulator) ->
    accumulate_custom_metadata(Doc, Accumulator).


%%-------------------------------------------------------------------
%% @doc
%% Prepares harvesting_batch for sending to Onezone.
%% accumulator() is unprepared, which means that:
%%     * batch entries are not sorted (they are stored in a map)
%%     * batch entries are not encoded
%% Call prepare_to_send(Accumulator) returns batch() which contains:
%%     * sorted list of encoded batch entries, ready to send to Onezone
%%     * batch entries are sorted by sequence numbers
%% If batch is already prepared, this function does nothing.
%% @end
%%-------------------------------------------------------------------
-spec prepare_to_send(accumulator() | batch()) -> batch().
prepare_to_send(Batch = #harvesting_batch{}) -> Batch;
prepare_to_send(Accumulator) when map_size(Accumulator) =:= 0 -> #harvesting_batch{};
prepare_to_send(Accumulator) when is_map(Accumulator) ->
    {EncodedBatch, MaxSeq} = maps:fold(fun
        (_FileId, Object, {AccIn, MaxSeqIn}) ->
            {[encode_entry(Object) | AccIn], max(get_seq(Object), MaxSeqIn)}
    end, {[], 0}, Accumulator),
    SortedEntries = [First | _] = lists:sort(fun(#{<<"seq">> := Seq1}, #{<<"seq">> := Seq2}) ->
        Seq1 =< Seq2
    end, EncodedBatch),
    #harvesting_batch{
        entries = SortedEntries,
        first_seq = get_seq(First),
        last_seq = MaxSeq,
        size = harvesting_batch:size(Accumulator)
    }.

-spec get_first_seq(batch()) -> seq().
get_first_seq(#harvesting_batch{first_seq = FirstSeq}) -> FirstSeq.

-spec get_last_seq(batch()) -> seq().
get_last_seq(#harvesting_batch{last_seq = LastSeq}) -> LastSeq.

-spec get_batch_entries(batch()) -> batch_entries().
get_batch_entries(#harvesting_batch{entries = Entries}) -> Entries.

%%-------------------------------------------------------------------
%% @doc
%% Strips all batch entries preceding StripAfter sequence number.
%% StripAfter argument is exclusive.
%% @end
%%-------------------------------------------------------------------
-spec strip(batch(), seq()) -> batch().
strip(Batch = #harvesting_batch{entries = Entries}, StripAfter) when is_list(Entries) ->
    {StrippedEntriesReversed, NewFirstSeqIn, NewSize} = lists:foldl(fun
        (Object, AccIn = {[], undefined, 0}) ->
            case get_seq(Object) of
                Seq when Seq =< StripAfter -> AccIn;
                Seq -> {[Object], Seq, 1}
            end;
        (Object, {StrippedEntriesIn, NewFirstSeqIn, NewSizeIn}) ->
            {[Object | StrippedEntriesIn], NewFirstSeqIn, NewSizeIn + 1}
    end, {[], undefined, 0}, Entries),

    Batch#harvesting_batch{
        first_seq = NewFirstSeqIn,
        entries = lists:reverse(StrippedEntriesReversed),
        size = NewSize
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec accumulate_file_meta(file_meta:doc(), accumulator()) -> accumulator().
accumulate_file_meta(FileMetaDoc = #document{
    key = FileUuid,
    value = #file_meta{},
    seq = Seq,
    deleted = false
}, Accumulator) when is_map(Accumulator) ->
    FileId = compute_file_id(FileMetaDoc),
    CustomMetadataDoc = case custom_metadata:get(FileUuid) of
        {ok, Doc} -> Doc;
        {error, not_found} -> undefined
    end,
    % if FileId is already in the accumulator, we can safely overwrite it because
    % we are interested in the newest change only
    Accumulator#{FileId => submission_batch_entry(FileId, Seq, FileMetaDoc, CustomMetadataDoc)};
accumulate_file_meta(FileMetaDoc = #document{
    value = #file_meta{},
    seq = Seq,
    deleted = true
}, Accumulator) when is_map(Accumulator) ->
    % deletion operation is send only when file_meta is deleted
    FileId = compute_file_id(FileMetaDoc),
    Accumulator#{FileId => deletion_batch_entry(FileId, Seq)}.


-spec accumulate_custom_metadata(custom_metadata:doc(), accumulator()) -> accumulator().
accumulate_custom_metadata(CustomMetadataDoc = #document{
    key = FileUuid,
    value = #custom_metadata{file_objectid = FileId},
    seq = Seq,
    deleted = false
}, Accumulator) when is_map(Accumulator) ->
    % if FileId is already in the accumulator, we can safely overwrite it because
    % we are interested in the newest change only
    FileMetaDoc = case file_meta:get_including_deleted(FileUuid) of
        {ok, Doc} -> Doc;
        {error, not_found} -> undefined
    end,
    Accumulator#{FileId => submission_batch_entry(FileId, Seq, FileMetaDoc, CustomMetadataDoc)};
accumulate_custom_metadata(#document{
    value = #custom_metadata{},
    deleted = true
}, Accumulator) when is_map(Accumulator) ->
    % deletion of custom_metadata doc is ignored as deletion of entry in the harvester will be
    % triggered by deletion of file_meta
    Accumulator.


-spec submission_batch_entry(file_id(),  couchbase_changes:seq(), undefined | doc(), undefined | doc()) -> batch_entry().
submission_batch_entry(FileId, Seq,
    #document{value = #file_meta{name = FileName}},
    #document{value = #custom_metadata{value = Metadata, space_id = SpaceId}}
) ->
    #{
        <<"fileId">> => FileId,
        <<"operation">> => ?SUBMIT,
        <<"seq">> => Seq,
        <<"spaceId">> => SpaceId,
        <<"fileName">> => FileName,
        <<"payload">> => Metadata
    };
submission_batch_entry(FileId, Seq,
    #document{value = #file_meta{name = FileName}, scope = SpaceId},
    undefined
) ->
    #{
        <<"fileId">> => FileId,
        <<"operation">> => ?SUBMIT,
        <<"seq">> => Seq,
        <<"spaceId">> => SpaceId,
        <<"fileName">> => FileName,
        <<"payload">> => #{}
    };
submission_batch_entry(FileId, Seq,
    undefined,
    #document{value = #custom_metadata{value = Metadata}, scope = SpaceId}
) ->
    #{
        <<"fileId">> => FileId,
        <<"operation">> => ?SUBMIT,
        <<"seq">> => Seq,
        <<"spaceId">> => SpaceId,
        <<"fileName">> => <<>>,
        <<"payload">> => Metadata
    }.


-spec deletion_batch_entry(file_id(), couchbase_changes:seq()) -> any().
deletion_batch_entry(FileId, Seq) ->
    #{
        <<"fileId">> => FileId,
        <<"operation">> => ?DELETE,
        <<"seq">> => Seq
    }.


-spec encode_entry(batch_entry()) -> batch_entry().
encode_entry(Entry = #{<<"operation">> := ?DELETE}) ->
    Entry;
encode_entry(Entry = #{<<"operation">> := ?SUBMIT, <<"payload">> := Payload}) ->
    Entry#{<<"payload">> => encode_payload(Payload)}.


-spec encode_payload(json()) -> json().
encode_payload(Payload) ->
    maps:fold(fun
        (<<"onedata_json">>, JSON, PayloadIn) ->
            PayloadIn#{<<"json">> => JSON};
        (<<"onedata_rdf">>, RDF, PayloadIn) ->
            PayloadIn#{<<"rdf">> => RDF};
        (Key, Value, PayloadIn) ->
            case is_cdmi_xattr(Key) orelse is_faas_xattr(Key) of
                true ->
                    PayloadIn;
                false ->
                    maps:update_with(<<"xattrs">>, fun(Xattrs) ->
                        Xattrs#{Key => Value}
                    end, #{Key => Value}, PayloadIn)
            end
    end, #{}, Payload).

-spec is_cdmi_xattr(binary()) -> boolean().
is_cdmi_xattr(XattrKey) ->
    KeyLen = byte_size(XattrKey),
    CdmiPrefixLen = byte_size(?CDMI_PREFIX),
    binary:part(XattrKey, 0, min(KeyLen, CdmiPrefixLen)) =:= ?CDMI_PREFIX.

-spec is_faas_xattr(binary()) -> boolean().
is_faas_xattr(<<?FAAS_PREFIX_STR, _/binary>>) -> true;
is_faas_xattr(_)                              -> false.

-spec get_seq(batch_entry()) -> seq().
get_seq(#{<<"seq">> := Seq}) ->
    Seq.


-spec compute_file_id(file_meta:doc()) -> file_id().
compute_file_id(#document{key = FileUuid, scope = SpaceId}) ->
    {ok, FileId} = file_id:guid_to_objectid(file_id:pack_guid(FileUuid, SpaceId)),
    FileId.