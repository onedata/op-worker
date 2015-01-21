-module(appmock_app).

-behaviour(application).

-include("appmock.hrl").
-include_lib("ctool/include/logging.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    appmock_logic:initialize("/root/appmock/suite_desc.erl"),
    appmock_sup:start_link().

stop(_State) ->
    appmock_logic:terminate().
