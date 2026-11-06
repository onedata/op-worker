%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `download_args` interface for
%%% #atm_store_dump_download_args{}.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_dump_download_args).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("http/http_download.hrl").

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type record() :: #atm_store_dump_download_args{}.

-export_type([record/0]).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_store_dump_download_args{
    session_id = SessionId,
    store_id = AtmStoreId
}, _NestedRecordEncoder) ->
    #{
        <<"sessionId">> => SessionId,
        <<"atmStoreId">> => AtmStoreId
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"sessionId">> := SessionId,
    <<"atmStoreId">> := AtmStoreId
}, _NestedRecordDecoder) ->
    #atm_store_dump_download_args{
        session_id = SessionId,
        store_id = AtmStoreId
    }.
