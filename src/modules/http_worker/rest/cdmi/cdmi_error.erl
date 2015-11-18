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
%%% Created : 12. Nov 2015 13:41
%%%-------------------------------------------------------------------
-module(cdmi_error).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("modules/http_worker/http_common.hrl").
-include("modules/http_worker/rest/http_status.hrl").

%% API
-export([invalid_range_reply/2, unlink_failed_reply/3]).

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

%%--------------------------------------------------------------------
%% @doc
%% Send cowboy reply reporting failure when unlinkig file
%% @end
%%--------------------------------------------------------------------
-spec unlink_failed_reply(Req :: req(), State :: #{}, Code :: code()) -> {term(), req(), #{}}.
unlink_failed_reply(Req, State, Code) ->
    ?error_stacktrace("Failed to unlink file. Error code:~p", [Code]),
    {ok, Req2} = cowboy_req:reply(?INTERNAL_SERVER_ERROR, [], [], Req),
    {halt, Req2, State}.