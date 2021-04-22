%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Bulk download allows for downloading whole directories and multiple files as a single TAR archive. 
%%% It is handled by two types of processes - cowboy request handling process (known as 
%%% `connection process` throughout all bulk_download modules) and `bulk download main process` 
%%% (for details consult module with the same name).
%%% During bulk download there can be many connection processes (connection might have broke or 
%%% client simply paused download). As it is highly volatile and it is impossible to perform necessary 
%%% finalization main process is introduced. There is only one main process for whole 
%%% bulk download (one exception being starting the same bulk download from the beginning in which 
%%% case old main process is killed and new one is started).
%%% This module assumes that its functions are called by connection process. It is responsible for 
%%% communication with main process and streaming data.
%%% @end
%%%--------------------------------------------------------------------
-module(bulk_download).
-author("Michal Stanisz").


-include("modules/bulk_download/bulk_download.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([start/4, resume/3, catch_up_data/2]).

-type id() :: bulk_download_main_process:id().

%%%===================================================================
%%% API
%%%===================================================================

-spec start(id(), [lfm_attrs:file_attributes()], session:id(), cowboy_req:req()) -> 
    ok | {error, term()}.
start(Id, FileAttrsList, SessionId, CowboyReq) ->
    Conn = self(),
    case bulk_download_main_process:get_pid(Id) of
        {ok, Pid} -> Pid ! ?MSG_STOP;
        _ -> ok
    end,
    {ok, MainPid} = bulk_download_main_process:run(Id, FileAttrsList, SessionId, Conn),
    communication_loop(Id, MainPid, CowboyReq).
    

-spec resume(id(), cowboy_req:req(), time:millis()) -> ok | error.
resume(Id, CowboyReq, NewDelay) ->
    case bulk_download_main_process:get_pid(Id) of
        {ok, MainPid} ->
            MainPid ! ?MSG_CONTINUE(NewDelay),
            communication_loop(Id, MainPid, CowboyReq);
        _ ->
            error
    end.


-spec catch_up_data(id(), non_neg_integer()) -> {ok, binary(), time:millis()} | error.
catch_up_data(Id, ResumeOffset) ->
    case bulk_download_main_process:get_pid(Id) of
        {ok, MainPid} ->
            MainPid ! ?MSG_RESUMED(self(), ResumeOffset),
            receive 
                ?MSG_DATA_CHUNK(Data, SendRetryDelay) -> {ok, Data, SendRetryDelay};
                ?MSG_ERROR -> error
            after 1000 -> error end;
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
            {NewDelay, _} = http_streamer:send_data_chunk(Data, CowboyReq, SendRetryDelay),
            MainPid ! ?MSG_CONTINUE(NewDelay),
            communication_loop(Id, MainPid, CowboyReq);
        ?MSG_ERROR ->
            error;
        ?MSG_DONE ->
            ok
    after timer:seconds(10) ->
        case is_process_alive(MainPid) of
            true -> communication_loop(Id, MainPid, CowboyReq);
            false -> 
                ?error("Proces ~p unexpectedly finished. Bulk download ~p will fail.", [MainPid, Id]),
                error(?ERROR_INTERNAL_SERVER_ERROR)
        end
    end.
