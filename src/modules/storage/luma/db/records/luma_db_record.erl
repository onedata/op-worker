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
-export([encode/1, decode/1]).

%%%===================================================================
%%% Callback definitions
%%%===================================================================

-callback to_json(luma_db:db_record()) -> json_utils:json_map().

-callback from_json(json_utils:json_map()) -> luma_db:db_record().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec encode(luma_db:db_record()) -> binary().
encode(Record) ->
    Module = utils:record_type(Record),
    RecordJson = Module:to_json(Record),
    json_utils:encode(RecordJson#{<<"record_type">> => atom_to_binary(Module, utf8)}).

-spec decode(binary()) -> json_utils:json_map().
decode(EncodedRecord) ->
    RecordJson = json_utils:decode(EncodedRecord),
    Module = binary_to_existing_atom(maps:get(<<"record_type">>, RecordJson), utf8),
    Module:from_json(RecordJson).
