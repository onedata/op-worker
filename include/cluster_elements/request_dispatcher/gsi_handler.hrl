%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: gsi_handler header
%% @end
%% ===================================================================

-ifndef(GSI_HANDLER_HRL).
-define(GSI_HANDLER, 1).

-record(veil_request, {subject, request, fuse_id, original_message}).

-define(GSI_SLAVE_COUNT, 2). %% How many slave nodes that loads GSI NIF has to be started

%% Proxy Certificate Extension ID
-define(PROXY_CERT_EXT, {1,3,6,1,5,5,7,1,14}).

-record(message_reroute, {provider_id = throw(no_provider_id), provider_urls = throw(no_provider_urls),
                            fuse_id = throw(no_fuse_id)}).

-endif.
