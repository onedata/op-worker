%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Bulk download allows for downloading whole directories and multiple files 
%%% as a single TAR archive. It is handled by two types of processes - cowboy 
%%% request handling process (known as `connection process` throughout all 
%%% bulk_download modules) and `bulk download main process` (for details consult 
%%% module with the same name).
%%% During bulk download there can be many connection processes (connection might 
%%% have broke or client simply paused download). As it is highly volatile and it 
%%% is impossible to perform necessary finalization main process is introduced. 
%%% There is only one main process for whole bulk download (one exception being 
%%% starting the same bulk download from the beginning in which case old main 
%%% process is killed and new one is started).
%%% Because some of data sent just before failure might have been lost main process 
%%% buffers last sent bytes. Thanks to this it is possible to catch up lost data.
%%% This module assumes that its functions are called by connection process. It is 
%%% responsible for communication with main process and streaming data.
%%% @end
%%%--------------------------------------------------------------------
-module(bulk_download).
-author("Michal Stanisz").


-include("modules/bulk_download/bulk_download.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([run/4, resume/3, catch_up_data/2]).

-type id() :: binary().

-export_type([id/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec run(id(), [lfm_attrs:file_attributes()], session:id(), cowboy_req:req()) -> 
    ok | {error, term()}.
run(Id, FileAttrsList, SessionId, CowboyReq) ->
    Conn = self(),
    case bulk_download_persistence:get_main_pid(Id) of
        {ok, Pid} -> bulk_download_main_process:finish(Pid);
        _ -> ok
    end,
    {ok, MainPid} = bulk_download_main_process:start(Id, FileAttrsList, SessionId, Conn),
    communication_loop(Id, MainPid, CowboyReq).
    

-spec resume(id(), cowboy_req:req(), time:millis()) -> ok | error.
resume(Id, CowboyReq, NewDelay) ->
    case bulk_download_persistence:get_main_pid(Id) of
        {ok, MainPid} ->
            bulk_download_main_process:report_data_sent(MainPid, NewDelay),
            communication_loop(Id, MainPid, CowboyReq);
        _ ->
            error
    end.


-spec catch_up_data(id(), non_neg_integer()) -> {ok, binary(), time:millis()} | error.
catch_up_data(Id, ResumeOffset) ->
    case bulk_download_persistence:get_main_pid(Id) of
        {ok, MainPid} ->
            bulk_download_main_process:resume(MainPid, ResumeOffset),
            receive 
                ?MSG_DATA_CHUNK(Data, SendRetryDelay) -> 
                    {ok, Data, SendRetryDelay};
                ?MSG_ERROR -> 
                    error
            after ?LOOP_TIMEOUT -> 
                error 
            end;
        _ ->
            error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec communication_loop(id(), pid(), cowboy_req:req()) -> ok | error.
communication_loop(Id, MainPid, CowboyReq) ->
    receive
        ?MSG_DATA_CHUNK(Data, SendRetryDelay) ->
            % TODO VFS-6597 - after cowboy update to at least ver 2.7 send bytes directly from main process
            {NewDelay, _} = http_streamer:send_data_chunk(Data, CowboyReq, SendRetryDelay),
            bulk_download_main_process:report_data_sent(MainPid, NewDelay),
            communication_loop(Id, MainPid, CowboyReq);
        ?MSG_ERROR ->
            error;
        ?MSG_DONE ->
            ok
    after ?LOOP_TIMEOUT ->
        case is_process_alive(MainPid) of
            true -> communication_loop(Id, MainPid, CowboyReq);
            false -> 
                ?error("Proces ~p unexpectedly finished. Bulk download ~p will fail.", [MainPid, Id]),
                error(?ERROR_INTERNAL_SERVER_ERROR)
        end
    end.
