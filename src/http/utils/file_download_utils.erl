%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling file download.
%%% @end
%%%--------------------------------------------------------------------
-module(file_download_utils).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_read_block_size/1, stream_range/5]).

-type encoding() :: binary().  % <<"base64">> | <<"utf-8">>

-export_type([encoding/0]).

-define(DEFAULT_READ_BLOCK_SIZE, 1048576). % 1 MB


%%%===================================================================
%%% API
%%%===================================================================


-spec get_read_block_size(lfm_context:ctx()) -> non_neg_integer().
get_read_block_size(FileHandle) ->
    utils:ensure_defined(
        storage:get_block_size(lfm_context:get_storage_id(FileHandle)),
        ?DEFAULT_READ_BLOCK_SIZE
    ).


-spec stream_range(
    lfm:handle(),
    Range :: {From :: non_neg_integer(), To :: non_neg_integer()},
    cowboy_req:req(),
    encoding(),
    ReadBlockSize :: non_neg_integer()
) ->
    ok | no_return().
stream_range(FileHandle, {From, To}, Req, Encoding, ReadBlockSize) ->
    ToRead = min(To - From + 1, ReadBlockSize - From rem ReadBlockSize),
    {ok, NewFileHandle, Data} = ?check(lfm:read(FileHandle, From, ToRead)),

    case byte_size(Data) of
        0 ->
            ok;
        DataSize ->
            ?error("FILE STREAM BIN: ~p", [DataSize]),

            EncodedData = cdmi_encoder:encode(Data, Encoding),
            cowboy_req:stream_body(EncodedData, nofin, Req),

            stream_range(
                NewFileHandle, {From + DataSize, To}, Req,
                Encoding, ReadBlockSize
            )
    end.
