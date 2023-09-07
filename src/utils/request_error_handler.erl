%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% Module containing utility functions concerning request errors handling 
%%% in Oneprovider.
%%% @end
%%%-------------------------------------------------------------------
-module(request_error_handler).
-author("Michał Stanisz").

-include("global_definitions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle/5]).

-define(SHOULD_LOG_REQUESTS_ON_ERROR, application:get_env(
    ?CLUSTER_WORKER_APP_NAME, log_requests_on_error, false
)).

%%%===================================================================
%%% API
%%%===================================================================


-spec handle(
    Type :: atom(),
    Reason :: term(),
    Stacktrace :: list(),
    session:id(),
    Request :: term()
) ->
    errors:error().
handle(Class, Reason, Stacktrace, SessionId, RequestTerm) ->
    case infer_error(Reason) of
        {true, Error} when Class =:= throw ->
            % do not log at all when the error was thrown as execution flow control
            Error;
        {true, Error} ->
            % log on debug level when it was possible to infer the error, but it happened
            % as a result of an exception
            ?debug_exception(format_log_message(SessionId, RequestTerm), Class, Reason, Stacktrace),
            Error;
        false ->
            % the error is not an errors:error() - log a full exception with stacktrace
            ?examine_exception(format_log_message(SessionId, RequestTerm), Class, Reason, Stacktrace)
    end.


%%%===================================================================
%%% Internals
%%%===================================================================

%% @private
-spec format_log_message(session:id(), Request :: term()) -> string().
format_log_message(SessionId, RequestTerm) ->
    AutoformattedDetails = case ?SHOULD_LOG_REQUESTS_ON_ERROR of
        true ->
            Request = lager:pr(RequestTerm, ?MODULE),
            ?autoformat([SessionId, Request]);
        false ->
            ?autoformat([SessionId])
    end,
    str_utils:format("Cannot process request~s", [AutoformattedDetails]).


%% @private
-spec infer_error(term()) -> {true, errors:error()} | false.
infer_error({badmatch, Error}) ->
    infer_error(Error);

infer_error({case_clause, Error}) ->
    infer_error(Error);

infer_error({error, Reason} = Error) ->
    case errors:is_known_error(Error) of
        true ->
            {true, Error};
        false ->
            infer_error(Reason)
    end;

infer_error(Error) ->
    case errors:is_posix_code(Error) of
        true ->
            {true, ?ERROR_POSIX(Error)};
        false ->
            false
    end.
