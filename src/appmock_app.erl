%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements OTP application behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(appmock_app).
-behaviour(application).

-include("appmock.hrl").
-include_lib("ctool/include/logging.hrl").

%% Application behaviour callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% API
%%%===================================================================

start(_StartType, _StartArgs) ->
    appmock_logic:initialize("/root/appmock/suite_desc.erl"),
    appmock_sup:start_link().

stop(_State) ->
    appmock_logic:terminate().
