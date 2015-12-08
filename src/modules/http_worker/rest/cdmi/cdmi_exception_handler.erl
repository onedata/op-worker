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
%% TODO describe handler behaviour, so far it's copied from requeste_exception_handler
%%% * for exception of integer type terminates request with given integer as
%%%   http status
%%% * for exception of {integer, term()} type terminates request with given
%%%   integer as http status, and given term converted to json as body
%%% * for any other exception terminates request with 500 http code and logs
%%%   error with stacktrace
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_exception_handler).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("modules/http_worker/rest/http_status.hrl").

%% Function that translates handler exception to cowboy format
-type exception_handler() ::
fun((Req :: cowboy_req:req(), State :: term(), Type :: atom(), Error :: term()) -> term()).

-export_type([exception_handler/0]).

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
handle(Req, State, error, {case_clause, {error, no_peer_certificate}}) ->
    {ok, Req2} = cowboy_req:reply(?NOT_AUTHORIZED, [], [], Req),
    {halt, Req2, State};
%% TODO wrong error pattern below?
handle(Req, State, _Type, {error, ?ENOENT}) ->
    {ok, Req2} = cowboy_req:reply(?NOT_FOUND, [], [], Req),
    {halt, Req2, State};
handle(Req, State, error, {badmatch,{{error,{not_found,file_meta}},created}}) ->
    BodyBinary = json_utils:encode([{<<"Error when creating file">>, <<"not found file meta">>}]),
    {ok, Req2} = cowboy_req:reply(?NOT_FOUND, [], BodyBinary, Req),
    {halt, Req2, State};
handle(Req, State, Type, Error) ->
    ?error_stacktrace("Unhandled exception in cdmi request ~p:~p", [Type, Error]),
    request_exception_handler:handle(Req, State, Type, Error).

%%%===================================================================
%%% Internal functions
%%%===================================================================