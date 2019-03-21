%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This behaviour describes a module that will serve as RPC handler for
%%% calls to server from client side (ember). They are performed using
%%% WebSocket adapter. There are only two RPC backends: private and public,
%%% available to clients with session or no session.
%%% @end
%%%-------------------------------------------------------------------
-module(rpc_backend_behaviour).
-author("Lukasz Opiola").


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Called to handle a callback from client side.
%% FunctionId is used to determine what function should be called.
%% RequestData is JSON received from client and decoded.
%% ResponseData will be encoded into JSON and sent back to the client.
%% When simple ok is returned, it means that the operation succeeded, but no
%% response data should be sent.
%% @end
%%--------------------------------------------------------------------
-callback handle(FunctionId :: binary(), RequestData :: term()) ->
    ok | {ok, ResponseData :: term()} | op_gui_error:error_result().
