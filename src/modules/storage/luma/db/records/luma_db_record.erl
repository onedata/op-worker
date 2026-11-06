%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for luma_db.
%%% It contains functions for encoding/decoding luma_db:db_record()
%%% structures.
%%% It also defines behaviour that must be implemented by modules
%%% defining luma_db:db_record() structures.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_db_record).
-author("Jakub Kudzia").

%% API
-export([encode/1, decode/1, update/2, to_json/1]).

%%%===================================================================
%%% Callback definitions
%%%===================================================================

-callback to_json(luma_db:db_record()) -> json_utils:json_map().

-callback from_json(json_utils:json_map()) -> luma_db:db_record().

-callback update(luma_db:db_record(), luma_db:db_diff()) -> {ok, luma_db:db_record()} | {error, term()}.

-optional_callbacks([update/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec encode(luma_db:db_record()) -> binary().
encode(Record) ->
    Module = utils:record_type(Record),
    RecordJson = to_json(Record),
    json_utils:encode(RecordJson#{<<"recordType">> => atom_to_binary(Module, utf8)}).

-spec decode(binary()) -> json_utils:json_map().
decode(EncodedRecord) ->
    RecordJson = json_utils:decode(EncodedRecord),
    {ModuleBin, RecordJson2} = maps:take(<<"recordType">>, RecordJson),
    Module = binary_to_existing_atom(ModuleBin, utf8),
    Module:from_json(RecordJson2).

-spec update(luma_db:db_record(), luma_db:db_diff()) ->
    {ok, luma_db:db_record()} | {error, term()}.
update(Record, Diff) ->
    Module = utils:record_type(Record),
    Module:update(Record, Diff).

-spec to_json(luma_db:db_record()) -> json_utils:json_map().
to_json(Record) ->
    Module = utils:record_type(Record),
    Module:to_json(Record).