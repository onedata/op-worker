%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header file contains record definitions and macros used internally
%%% in appmock engine.
%%% @end
%%%-------------------------------------------------------------------

-include("appmock.hrl").
-include("client_interface.hrl").

-define(APP_NAME, appmock).

% Records that holds the state of a mapping in ETS
-record(mapping_state, {
    response = #mock_resp{} :: #mock_resp{} | [#mock_resp{}] | function(),
    % Used to remember state between requests on the same stub
    state = [],
    % Counts number of requests to certain endpoint
    counter = 0
}).

