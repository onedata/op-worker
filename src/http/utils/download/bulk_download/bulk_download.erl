%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Bulk download allows downloading whole directories and multiple files 
%%% as a single tarball. It is handled by two types of processes - cowboy 
%%% request handling process (known as `connection process` throughout all 
%%% bulk_download modules) and `bulk download main process` (for details consult 
%%% module with the same name).
%%% During bulk download there can be many connection processes (connection might 
%%% have broke or client simply paused download). As it is highly volatile and it 
%%% is impossible to perform necessary finalization main process is introduced. 
%%% There is only one main process for whole bulk download (one exception being 
%%% starting the same bulk download from the beginning in which case old main 
%%% process is killed and new one is started). 
%%% 
%%% Bulk download implementation is split between several modules: 
%%%     * `bulk_download` - module assumes that its functions are called by 
%%%             connection process. It is responsible for communication with 
%%%             main process and streaming data.
%%%     * `bulk_download_main_process` - module responsible for handling 
%%%             bulk download main process. It is operated by main process.
%%%     * `bulk_download_traverse` - module responsible for tree traverse during 
%%%             bulk download. It is operated by processes from traverse pool.
%%%     * `bulk_download_task` - implements datastore API for storing bulk download 
%%%             related data. 
%%% 
%%% Whole bulk download procedure has following steps:
%%%     1) connection process starts main process 
%%%     2) main process takes first file from requested list of files 
%%%     3) if file is a directory main process starts bulk download traverse, otherwise 5) 
%%%     4) process from traverse pool sequentially reports next files to main process 
%%%        (i.e steps 5-8 are repeated until traverse finishes)
%%%     5) main process reads part of file and sends this chunk to connection process 
%%%     6) connection process forwards received chunk to client and sends confirmation 
%%%        to main process
%%%     7) upon send confirmation main process reads next file chunk 
%%%     8) steps 5-7 are repeated until the end of file 
%%%     9) repeat steps 2-8 until all files from given list are processed 
%%% 
%%% In case of connection failure connection process will die and main process will be 
%%% waiting for confirmation on step 7) in above list. 
%%% To resume bulk download new connection process sends resume request to main process 
%%% with offset and main process responds with missing data and then continues as if 
%%% there was no break.
%%% @end
%%%--------------------------------------------------------------------
-module(bulk_download).
-author("Michal Stanisz").


-include("modules/bulk_download/bulk_download.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([run/5, can_continue/1, is_offset_allowed/2, continue/3]).

-type id() :: binary().

-export_type([id/0]).

-define(MAX_CHUNK_SIZE, 10485760). % 10 MB

%%%===================================================================
%%% API
%%%===================================================================

-spec run(id(), [lfm_attrs:file_attributes()], session:id(), boolean(), cowboy_req:req()) -> 
    ok | {error, term()}.
run(BulkDownloadId, FileAttrsList, SessionId, FollowLinks, CowboyReq) ->
    Conn = self(),
    case bulk_download_task:get_main_pid(BulkDownloadId) of
        {ok, Pid} -> bulk_download_main_process:abort(Pid);
        _ -> ok
    end,
    {ok, MainPid} = bulk_download_main_process:start(
        BulkDownloadId, FileAttrsList, SessionId, Conn, FollowLinks),
    data_streaming_loop(BulkDownloadId, MainPid, CowboyReq).
    

-spec can_continue(id()) -> boolean().
can_continue(BulkDownloadId) ->
    case bulk_download_task:get_main_pid(BulkDownloadId) of
        {ok, _} -> true;
        _ -> false
    end.


-spec is_offset_allowed(id(), non_neg_integer()) -> boolean().
is_offset_allowed(BulkDownloadId, Offset) ->
    case bulk_download_task:get_main_pid(BulkDownloadId) of
        {ok, MainPid} ->
            bulk_download_main_process:is_offset_allowed(MainPid, Offset);
        _ ->
            false
    end.


-spec continue(id(), non_neg_integer(), cowboy_req:req()) -> ok | error.
continue(BulkDownloadId, Offset, CowboyReq) ->
    case bulk_download_task:get_main_pid(BulkDownloadId) of
        {ok, MainPid} ->
            bulk_download_main_process:resume(MainPid, Offset),
            data_streaming_loop(BulkDownloadId, MainPid, CowboyReq);
        _ ->
            error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec data_streaming_loop(id(), pid(), cowboy_req:req()) -> ok | error.
data_streaming_loop(BulkDownloadId, MainPid, CowboyReq) ->
    receive
        ?MSG_DATA_CHUNK(Data, SendRetryDelay) ->
            % TODO VFS-6597 - after cowboy update to at least ver 2.7 send bytes directly from main process
            % when resending unreceived data chunk can be very large, so ensure it is sent in smaller chunks
            NewDelay = send_in_chunks(Data, CowboyReq, SendRetryDelay),
            bulk_download_main_process:report_data_sent(MainPid, NewDelay),
            data_streaming_loop(BulkDownloadId, MainPid, CowboyReq);
        ?MSG_ERROR ->
            error;
        ?MSG_DONE ->
            ok
    after ?LOOP_TIMEOUT ->
        case is_process_alive(MainPid) of
            true -> data_streaming_loop(BulkDownloadId, MainPid, CowboyReq);
            false -> 
                ?error("Process ~p unexpectedly finished. Bulk download ~p will fail.", [MainPid, BulkDownloadId]),
                error(?ERROR_INTERNAL_SERVER_ERROR)
        end
    end.


%% @private
-spec send_in_chunks(binary(), cowboy_req:req(), time:millis()) -> time:millis().
send_in_chunks(Data, CowboyReq, SendRetryDelay) when byte_size(Data) > ?MAX_CHUNK_SIZE ->
    {NewDelay, _} = http_streamer:send_data_chunk(binary:part(Data, 0, ?MAX_CHUNK_SIZE), CowboyReq, 2, SendRetryDelay),
    send_in_chunks(binary:part(Data, ?MAX_CHUNK_SIZE, byte_size(Data) - ?MAX_CHUNK_SIZE), CowboyReq, NewDelay);
send_in_chunks(Data, CowboyReq, SendRetryDelay) ->
    {NewDelay, _} = http_streamer:send_data_chunk(Data, CowboyReq, 2, SendRetryDelay),
    NewDelay.
