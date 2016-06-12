%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Exception handler for cdmi operations.
%%% It behaves as follows:
%%% * for badmatch or case_clause errors it strips parts "badmatch" and
%%%   "case_clause" from tuples and returns the actual error
%%%   e.g for error: {badmatch, {case_clause, {error, ENOENT}}}
%%%   it will terminate request with http status and message suitable to ENOENT
%%% * for any other exception it calls request_exception_handler:handle()
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_exception_handler).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").

%% API
-export([handle/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% this handler returns appropriate cowboy status on basis of caught exception.
%% @end
%%--------------------------------------------------------------------
-spec handle(cowboy_req:req(), term(), atom(), term()) -> no_return().
handle(Req, State, Type, Error) ->
    request_exception_handler:handle(Req, State, Type, Error).

%%%===================================================================
%%% Internal functions
%%%===================================================================