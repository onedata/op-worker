%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Rtransfer config and start.
%%% @end
%%%--------------------------------------------------------------------
-module(rtransfer_config).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-define(RTRANSFER_PORT, application:get_env(?APP_NAME, rtransfer_port, 6665)).
-define(RTRANSFER_NUM_ACCEPTORS, 10).

%% API
-export([rtransfer_opts/0]).

-define(STREAMS_NUM, application:get_env(?APP_NAME, streams_number, 10)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get default rtransfer config
%% @end
%%--------------------------------------------------------------------
-spec rtransfer_opts() -> list().
rtransfer_opts() ->
    [
        {bind, lists:duplicate(?STREAMS_NUM, {0, 0, 0, 0})},
        {get_nodes_fun,
            fun(ProviderId) ->
                {ok, IPs} = provider_logic:resolve_ips(ProviderId),
                [{IP, ?RTRANSFER_PORT} || IP <- IPs]
            end},
        {open_fun,
            fun(FileGUID, OpenFlag) ->
                lfm_files:open(?ROOT_SESS_ID, {guid, FileGUID}, OpenFlag)
            end},
        {read_fun,
            fun(Handle, Offset, MaxSize) ->
                lfm_files:silent_read(Handle, Offset, MaxSize)
            end},
        {write_fun,
            fun(Handle, Offset, Buffer) ->
                lfm_files:write_without_events(Handle, Offset, Buffer)
            end},
        {close_fun,
            fun(Handle) ->
                case {lfm_context:get_session_id(Handle), lfm_context:get_open_flag(Handle)} of
                    {?ROOT_SESS_ID, write} ->
                        ok = lfm_files:fsync(Handle);
                    _ ->
                        ok
                end,
                lfm_files:release(Handle)
            end},
        {ranch_opts,
            [
                {num_acceptors, ?RTRANSFER_NUM_ACCEPTORS},
                {transport, ranch_tcp},
                {trans_opts, [{port, ?RTRANSFER_PORT}]}
            ]
        }
    ].

%%%===================================================================
%%% Internal functions
%%%===================================================================
