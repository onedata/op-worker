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


%% @private
-spec handle(
    Type :: atom(),
    Reason :: term(),
    Stacktrace :: list(),
    session:id(),
    Request :: term()
) ->
    errors:error().
handle(throw, Reason, _Stacktrace, _SessionId, _Request) ->
    infer_error(Reason);

handle(_Type, Reason, Stacktrace, SessionId, Request) ->
    Error = infer_error(Reason),

    {LogFormat, LogFormatArgs} = case ?SHOULD_LOG_REQUESTS_ON_ERROR of
        true ->
            MF = "Cannot process request:~n~p~nfor session: ~p~ndue to: ~p~ncaused by ~p",
            FA = [lager:pr(Request, ?MODULE), SessionId, Error, Reason],
            {MF, FA};
        false ->
            MF = "Cannot process request for session: ~p~ndue to: ~p~ncaused by ~p",
            FA = [SessionId, Error, Reason],
            {MF, FA}
    end,

    ?debug_stacktrace(LogFormat, LogFormatArgs, Stacktrace),

    Error.


%% @private
-spec infer_error(term()) -> errors:error().
infer_error({badmatch, Error}) ->
    infer_error(Error);

infer_error({error, Reason} = Error) ->
    case ordsets:is_element(Reason, ?ERROR_CODES) of
        true -> ?ERROR_POSIX(Reason);
        false -> Error
    end;

infer_error(Reason) ->
    % wrap in a proper term in hope it's a standardized error;
    % if not, it will finally generate a translation error log and return an internal server error
    infer_error({error, Reason}).
