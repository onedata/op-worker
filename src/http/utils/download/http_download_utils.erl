%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains utils for downloading data using http and cowboy.
%%% @end
%%%--------------------------------------------------------------------
-module(http_download_utils).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("http/http_download.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% API
-export([
    allow_onezone_as_frame_ancestor/1,
    set_content_disposition_header/2,

    send_data_chunk/4
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec allow_onezone_as_frame_ancestor(cowboy_req:req()) -> cowboy_req:req().
allow_onezone_as_frame_ancestor(Req) ->
    http_cors:allow_frame_ancestors(oneprovider:get_oz_url(), Req).


-spec set_content_disposition_header(cowboy_req:req(), file_meta:name()) ->
    cowboy_req:req().
set_content_disposition_header(Req, FileName) ->
    NormalizedFileName = normalize_filename(FileName),
    %% @todo VFS-2073 - check if needed
    %% FileNameUrlEncoded = http_utils:url_encode(FileName),
    cowboy_req:set_resp_header(
        ?HDR_CONTENT_DISPOSITION,
        <<"attachment; filename=\"", NormalizedFileName/binary, "\"">>,
        %% @todo VFS-2073 - check if needed
        %% "filename*=UTF-8''", FileNameUrlEncoded/binary>>
        Req
    ).


%%--------------------------------------------------------------------
%% @doc
%% TODO VFS-6597 - update cowboy to at least ver 2.7 to fix streaming big files
%% Cowboy uses separate process to manage socket and all messages, including
%% data, to stream are sent to that process. However because it doesn't enforce
%% any backpressure mechanism it is easy, on slow networks and fast storages,
%% to read to memory entire file while sending process doesn't keep up with
%% sending those data. To avoid this it is necessary to check message_queue_len
%% of sending process and ensure it is not larger than max allowed blocks to
%% read into memory.
%% @end
%%--------------------------------------------------------------------
-spec send_data_chunk(
    Data :: iodata(),
    cowboy_req:req(),
    MaxSentBlocksCount :: non_neg_integer(),
    RetryDelay :: time:millis()
) ->
    {NextRetryDelay :: time:millis(), cowboy_req:req()}.
send_data_chunk(Data, #{pid := ConnPid} = Req, MaxSentBlocksCount, RetryDelay) ->
    {message_queue_len, MsgQueueLen} = process_info(ConnPid, message_queue_len),

    case MsgQueueLen < MaxSentBlocksCount of
        true ->
            cowboy_req:stream_body(Data, nofin, Req),
            {max(RetryDelay div 2, ?MIN_HTTP_SEND_RETRY_DELAY), Req};
        false ->
            timer:sleep(RetryDelay),
            send_data_chunk(
                Data, Req, MaxSentBlocksCount,
                min(2 * RetryDelay, ?MAX_HTTP_SEND_RETRY_DELAY)
            )
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec normalize_filename(file_meta:name()) -> file_meta:name().
normalize_filename(FileName) ->
    case re:run(FileName, <<"^ *$">>, [{capture, none}]) of
        match -> <<"_">>;
        nomatch -> FileName
    end.
