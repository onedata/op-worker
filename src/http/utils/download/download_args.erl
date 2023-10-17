%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `download_args` interface - an object which can be
%%% used for storing and retrieving arguments for downloading entity.
%%% @end
%%%-------------------------------------------------------------------
-module(download_args).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type type() ::
    atm_store_dump_download_args |
    file_content_download_args.

-type record() ::
    atm_store_dump_download_args:record() |
    file_content_download_args:record().

-export_type([type/0, record/0]).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(DownloadsArgs, NestedRecordEncoder) ->
    RecordType = utils:record_type(DownloadsArgs),

    maps:merge(
        #{<<"type">> => atom_to_binary(RecordType)},
        NestedRecordEncoder(DownloadsArgs, RecordType)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    #{<<"type">> := DownloadArgsRecordTypeBin} = AtmStoreContainerJson,
    NestedRecordDecoder
) ->
    RecordType = binary_to_existing_atom(DownloadArgsRecordTypeBin),
    NestedRecordDecoder(AtmStoreContainerJson, RecordType).
