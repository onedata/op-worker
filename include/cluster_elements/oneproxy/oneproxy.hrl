%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% oneproxy definitions.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(ONEPROXY_HRL).
-define(ONEPROXY_HRL, 1).

-record(oneproxy_state, {timeout = timer:minutes(1), endpoint}).

%% oneproxy listeners
-define(ONEPROXY_DISPATCHER, oneproxy_dispatcher).
-define(ONEPROXY_REST, oneproxy_rest).

-endif.