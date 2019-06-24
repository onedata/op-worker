%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module is used to report errors back to client side with
%%% unified API. It must be used ALWAYS when returning a non-ok value from
%%% backend modules:
%%%     - data backends
%%%     - rpc backends
%%%     - session plugin
%%%     - route plugin
%%%     - etc.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_error).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").

%% Type that must be returned from backend modules when an error occurs.
%% The proplist is encoded to JSON and must contain the fields
%% 'severity' and 'message'.
-type error_result() :: {error_result, proplists:proplist()}.
-export_type([error_result/0]).

%% API
%% Non-severe error -> display notification, e.g. directory creation failed.
-export([report_warning/1]).
%% Unexpected error -> display alert, propose page refresh.
-export([report_error/1]).
%% Severe crash -> display alert, impose page refresh.
-export([report_critical/1]).

%% Predefined errors
-export([internal_server_error/0]).
-export([unauthorized/0]).
-export([no_session/0]).
-export([cannot_decode_message/0]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a term that should be returned when a non-severe error occurs.
%% Warning means that a notification should be displayed to the client and
%% the use of application can be continued.
%% @end
%%--------------------------------------------------------------------
-spec report_warning(Message :: binary()) -> error_result().
report_warning(Message) ->
    error_result(<<"warning">>, Message).


%%--------------------------------------------------------------------
%% @doc
%% Creates a term that should be returned when an unexpected error occurs.
%% Error means that an alert should be displayed to the client that informs
%% him of application error and possibly propose page refresh.
%% @end
%%--------------------------------------------------------------------
-spec report_error(Message :: binary()) -> error_result().
report_error(Message) ->
    error_result(<<"error">>, Message).


%%--------------------------------------------------------------------
%% @doc
%% Creates a term that should be returned when a critical error occurs.
%% Critical error means that an alert should be displayed to the client
%% that informs him of application crash and impose page refresh.
%% @end
%%--------------------------------------------------------------------
-spec report_critical(Message :: binary()) -> error_result().
report_critical(Message) ->
    error_result(<<"critical">>, Message).


%%--------------------------------------------------------------------
%% @doc
%% Predefined error that reports an "Internal Sever Error" on client side.
%% @end
%%--------------------------------------------------------------------
-spec internal_server_error() -> error_result().
internal_server_error() ->
    error_result(<<"error">>, <<"Internal Sever Error">>).


%%--------------------------------------------------------------------
%% @doc
%% Predefined error that reports an "Unauthorized" on client side.
%% @end
%%--------------------------------------------------------------------
-spec unauthorized() -> error_result().
unauthorized() ->
    error_result(<<"error">>, <<"Unauthorized">>).


%%--------------------------------------------------------------------
%% @doc
%% Predefined error that reports that client has no valid session (and hence is
%% not allowed to perform an operation, e.g. private RPC).
%% @end
%%--------------------------------------------------------------------
-spec no_session() -> error_result().
no_session() ->
    error_result(<<"error">>, <<"No valid session">>).


%%--------------------------------------------------------------------
%% @doc
%% Predefined error that reports that received message could not be decoded.
%% @end
%%--------------------------------------------------------------------
-spec cannot_decode_message() -> error_result().
cannot_decode_message() ->
    error_result(<<"error">>, <<"Cannot decode message">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Returns a generic error result.
%% @end
%%--------------------------------------------------------------------
-spec error_result(Severity :: binary(), Message :: binary()) -> error_result().
error_result(Severity, Message) ->
    {error_result, [
        {<<"severity">>, Severity},
        {<<"message">>, Message}
    ]}.
