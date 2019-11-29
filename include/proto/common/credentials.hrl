%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Credentials that can be exchanged for user identity in identity model
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CREDENTIALS_HRL).
-define(CREDENTIALS_HRL, 1).

% Record containing access token for user authorization in OZ.
-record(token_auth, {
    token :: tokens:serialized(),
    %% @TODO VFS-5718 - currently always undefined, peer IP should be collected
    %% when a client (FUSE, REST, GUI) connects and associated with his
    %% auth / session. It is utilized in gs_client_worker when adding
    %% an auth override to the request.
    peer_ip = undefined :: undefined | ip_utils:ip(),
    interface = undefined :: undefined | cv_interface:interface(),
    data_access_caveats_policy = disallow_data_access_caveats :: data_access_caveats:policy()
}).

-endif.
