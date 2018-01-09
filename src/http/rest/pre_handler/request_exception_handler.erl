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
-include("http/rest/http_status.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

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
handle(Req, State, error, {badmatch, Badmatch}) ->
    handle(Req, State, error, Badmatch);
handle(Req, State, error, {case_clause, CaseClause}) ->
    handle(Req, State, error, CaseClause);
handle(Req, State, error, {error, not_found}) ->
    handle(Req, State, error, ?ERROR_NOT_FOUND);
handle(Req, State, error, {error, {<<"not_found">>, _}}) ->
    handle(Req, State, error, ?ERROR_NOT_FOUND);
handle(Req, State, _, {error, {_, invalid_json}}) ->
    handle(Req, State, error, invalid_json);
handle(Req, State, _, invalid_json) ->
    handle(Req, State, error, ?ERROR_INVALID_JSON);
handle(Req, State, error, {error, ?ENOENT}) ->
    handle(Req, State, error, ?ERROR_NOT_FOUND);
handle(Req, State, error, {error, ?ENOATTR}) ->
    handle(Req, State, error, ?ERROR_ATTRIBUTE_NOT_FOUND);
handle(Req, State, error, {error, ?EACCES}) ->
    handle(Req, State, error, ?ERROR_PERMISSION_DENIED);
handle(Req, State, error, {error, ?EPERM}) ->
    handle(Req, State, error, ?ERROR_FORBIDDEN);
handle(Req, State, error, {error, ?EEXIST}) ->
    handle(Req, State, error, ?ERROR_EXISTS);
handle(Req, State, _Type, Status) when is_integer(Status) ->
    {ok, Req2} = cowboy_req:reply(Status, [], [], Req),
    {halt, Req2, State};
handle(Req, State, _Type, {error, {Type, Error}})
    when is_binary(Type), is_binary(Error) ->
    handle(Req, State, error, ?ERROR_REPLY(?INTERNAL_SERVER_ERROR, Type, Error));
handle(Req, State, _Type, {Status, BodyBinary})
    when is_integer(Status) andalso is_binary(BodyBinary) ->
    {ok, Req2} = cowboy_req:reply(Status, [], BodyBinary, Req),
    {halt, Req2, State};
handle(Req, State, _Type, {Status, Body}) when is_integer(Status) ->
    BodyBinary = json_utils:encode_map(Body),
    {ok, Req2} = cowboy_req:reply(Status, [], BodyBinary, Req),
    {halt, Req2, State};
handle(Req, State, Type, Error) ->
    ?error_stacktrace("Unhandled exception in rest request ~p:~p", [Type, Error]),
    {ok, Req2} = cowboy_req:reply(?INTERNAL_SERVER_ERROR, [], [], Req),
    {halt, Req2, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================