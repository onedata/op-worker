%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides error handling functions for cdmi modules
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_error).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("modules/http_worker/http_common.hrl").
-include("modules/http_worker/rest/http_status.hrl").

%% API
-export([invalid_range_reply/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Send cowboy reply reporting invalid range of "Range" header value
%% @end
%%--------------------------------------------------------------------
-spec invalid_range_reply(Req :: req(), State :: #{}) -> {term(), req(), #{}}.
invalid_range_reply(Req, State) ->
    ?error_stacktrace("Invalid range header value in CDMI request"),
    {ok, Req2} = cowboy_req:reply(?BAD_REQUEST, [], [], Req),
    {halt, Req2, State}.
