%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Default exception handler for rest operations. You can override it with your
%%% own handler by setting 'exception_handler' in handler_description.
%%% It behaves as follows:
%%% * for exception of integer type terminates request with given integer as
%%%   http status
%%% * for exception of {integer, term()} type terminates request with given
%%%   integer as http status, and given term converted to json as body
%%% * for any other exception terminates request with 500 http code and logs
%%%   error with stacktrace
%%% @end
%%%--------------------------------------------------------------------
-module(request_exception_handler).
-author("Tomasz Lichon").

-include_lib("ctool/include/logging.hrl").
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
handle(Req, State, _Type, Status) when is_integer(Status) ->
    {ok, Req2} = cowboy_req:reply(Status, [], [], Req),
    {halt, Req2, State};
handle(Req, State, _Type, {Status, BodyBinary})
    when is_integer(Status) andalso is_binary(BodyBinary) ->
    {ok, Req2} = cowboy_req:reply(Status, [], BodyBinary, Req),
    {halt, Req2, State};
handle(Req, State, _Type, {Status, Body}) when is_integer(Status) ->
    BodyBinary = json:encode(Body),
    {ok, Req2} = cowboy_req:reply(Status, [], BodyBinary, Req),
    {halt, Req2, State};
handle(Req, State, Type, Error) ->
    ?error_stacktrace("Unhandled exception in rest request ~p:~p", [Type, Error]),
    {ok, Req2} = cowboy_req:reply(?INTERNAL_SERVER_ERROR, [], [], Req),
    {halt, Req2, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================