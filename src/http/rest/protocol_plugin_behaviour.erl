%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Behaviour for rest protocol plugins.
%%% @end
%%%-------------------------------------------------------------------
-module(protocol_plugin_behaviour).
-author("Tomasz Lichon").

-type handler() :: handler_selector() | handler_description().

-type handler_selector() :: fun((cowboy_req:req()) -> {handler_description(), cowboy_req:req()}).

%% Handler description is a map which defines:
%% * handler module itself
%% * handler_initial_opts that should be passed to handler's rest_init function
%% * exception_handler, lambda functions that gets executed whenever handler fails
%%   with exception. It may return approriate error to prevent crash.
%%   Leaving it empty leads to usage of default exception_handler which:
%%   * for exception of integer type terminates request with given integer as http status
%%   * for exception of {integer, term()} type terminates request with given
%%     integer as http status, and given term converted to json as body
%%   * for any other exception terminates request with 500 http code and logs
%%     error with stacktrace
-type handler_description() :: #{
    handler => module(),
    handler_initial_opts => term(),
    exception_handler => exception_handler()
}.

%% Function that translates exception to cowboy callback response format
-type exception_handler() ::
fun((Req :: cowboy_req:req(), State :: term(), Type :: atom(), Error :: term()) -> term()).

-export_type([handler/0, handler_selector/0, handler_description/0, exception_handler/0]).

%%--------------------------------------------------------------------
%% @doc
%% Defines routes, as cowboy_path formatted string, with their handlers (defined
%% as map with its description or lambda that calculates such map on basis on request).
%% @end
%%--------------------------------------------------------------------
-callback routes() -> [{Route :: string(), handler()}].