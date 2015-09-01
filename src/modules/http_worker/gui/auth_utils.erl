%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2015 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This library is used to authenticate users that have been redirected
%% from global registry.
%% @end
%% ===================================================================
-module(auth_utils).

-include_lib("ctool/include/logging.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
-export([authorize/1]).

%%--------------------------------------------------------------------
%% @doc
%% Authorizes a user via Global Registry. Upon success, returns the root
%% macaroon and discharge macaroons that can be used to perform operations
%% on behalf of the user.
%% @end
%%--------------------------------------------------------------------
-spec authorize(SerializedMacaroon) ->
    {ok, SerializedMacaroon, [SerializedMacaroon]} | {error, term()} when
    SerializedMacaroon :: binary().
authorize(_) -> ok.

