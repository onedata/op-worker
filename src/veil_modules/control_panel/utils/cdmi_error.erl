%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides error handling functions for cdmi modules
%% @end
%% ===================================================================
-module(cdmi_error).

-include("veil_modules/control_panel/cdmi.hrl").

%% API
-export([error_reply/5]).

%% error_reply/5
%% ====================================================================
%% @doc Prints error description log and sets cowboy reply to given error code
%% @end
-spec error_reply(req(), #state{}, integer(), string(), list()) -> {halt, req(), #state{}}.
%% ====================================================================
error_reply(Req,State,ErrorCode,ErrorDescription,DescriptionArgs) ->
    ?error_stacktrace(ErrorDescription, DescriptionArgs),
    {ok, Req2} = veil_cowboy_bridge:apply(cowboy_req,reply,[ErrorCode, Req]),
    {halt, Req2, State}.