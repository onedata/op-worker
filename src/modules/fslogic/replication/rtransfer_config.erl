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

-include("modules/fslogic/lfm_internal.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/oz/oz_providers.hrl").
-include_lib("ctool/include/logging.hrl").

-define(RTRANSFER_PORT, 6665).
-define(RTRANSFER_NUM_ACCEPTORS, 10).

%% API
-export([start_rtransfer/0]).

%% Test API
-export([rtransfer_opts/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Start rtransfer server
%% @end
%%--------------------------------------------------------------------
-spec start_rtransfer() -> {ok, pid()}.
start_rtransfer() ->
    {ok, _} = rtransfer:start_link(rtransfer_opts()).

%%--------------------------------------------------------------------
%% @doc
%% Get default rtransfer config
%% @end
%%--------------------------------------------------------------------
-spec rtransfer_opts() -> list().
rtransfer_opts() ->
    [
        {get_nodes_fun,
            fun(ProviderId) ->
                {ok, #provider_details{urls = URLs}} = oz_providers:get_details(provider, ProviderId),
                lists:map(
                    fun(URL) ->
                        {ok, Ip} = inet:ip(binary_to_list(URL)),
                        {Ip, ?RTRANSFER_PORT}
                    end, URLs)
            end},
        {open_fun,
            fun(FileGUID, OpenMode) ->
                lfm_files:open(?ROOT_SESS_ID, {guid, FileGUID}, OpenMode)
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
            fun
                (Handle = #lfm_handle{fslogic_ctx = #fslogic_ctx{session_id = ?ROOT_SESS_ID}, open_mode = write}) ->
                    lfm_files:fsync(Handle);
                (_) ->
                    ok
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