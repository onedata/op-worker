%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% gsi_handler definitions.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(GSI_HANDLER_HRL).
-define(GSI_HANDLER, 1).

-record(worker_request, {subject, request, fuse_id, access_token, peer_id}).

-define(GSI_SLAVE_COUNT, 2). %% How many slave nodes that loads GSI NIF has to be started

%% Proxy Certificate Extension ID
-define(PROXY_CERT_EXT, {1,3,6,1,5,5,7,1,14}).

-endif.
