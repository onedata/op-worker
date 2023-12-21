%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for http auth.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HTTP_AUTH_HRL).
-define(HTTP_AUTH_HRL, 1).


-record(http_auth_ctx, {
    interface :: cv_interface:interface(),
    data_access_caveats_policy :: data_access_caveats:policy()
}).


-endif.
