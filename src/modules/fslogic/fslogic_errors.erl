%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2015, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc
%% This module provides error translators for generic fslogic errors.
%% @end
%% ===================================================================
-module(fslogic_errors).
-author("Rafal Slota").

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("storage_file_manager_errors.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle_error/3, gen_status_message/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handle error caught during processing of fslogic request.
%% @end
%%--------------------------------------------------------------------
-spec handle_error(fslogic_worker:request(), Type :: atom(),  Reason :: term()) ->
    fslogic_worker:response().
handle_error(Request, throw, Reason) ->
    %% Manually thrown error, normal interrupt case.
    report_error(Request, Reason, debug, erlang:get_stacktrace());
handle_error(Request, error, {badmatch, Reason}) ->
    %% Bad Match assertion - something went wrong, but it could be expected (e.g. file not found assertion).
    report_error(Request, Reason, warning, erlang:get_stacktrace());
handle_error(Request, error, {case_clause, Reason}) ->
    %% Case Clause assertion - something went seriously wrong and we should know about it.
    report_error(Request, Reason, error, erlang:get_stacktrace());
handle_error(Request, error, Reason) ->
    %% Something went horribly wrong. This should not happen.
    report_error(Request, Reason, error, erlang:get_stacktrace()).

%%--------------------------------------------------------------------
%% @doc
%% Translates operation error to status messages.
%% This function is intended to be extended when new translation is needed.
%% @end
%%--------------------------------------------------------------------
-spec gen_status_message(Error :: term()) -> #status{}.
gen_status_message({error, Reason}) ->
    gen_status_message(Reason);
gen_status_message({not_a_space, _}) ->
    #status{code = ?ENOENT, description = describe_error(?ENOENT)};
gen_status_message({not_found, file_meta}) ->
    #status{code = ?ENOENT, description = describe_error(?ENOENT)};
gen_status_message(already_exists) ->
    #status{code = ?EEXIST, description = describe_error(?EEXIST)};
gen_status_message({403,<<>>,<<>>}) ->
    #status{code = ?EACCES, description = describe_error(?EACCES)};
gen_status_message(Error) when is_atom(Error) ->
    case ordsets:is_element(Error, ?ERROR_CODES) of
        true -> #status{code = Error};
        false ->
            #status{code = ?EAGAIN, description = describe_error(Error)}
    end;
gen_status_message({ErrorCode, ErrorDescription}) when
    is_atom(ErrorCode) and is_binary(ErrorDescription) ->
    case ordsets:is_element(ErrorCode, ?ERROR_CODES) of
        true -> #status{code = ErrorCode, description = ErrorDescription};
        false -> #status{code = ?EAGAIN, description = ErrorDescription}
    end;
gen_status_message(Reason) ->
    ?error_stacktrace("Unknown error occured: ~p", [Reason]),
    #status{code = ?EAGAIN, description = <<"An unknown error occured.">>}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a FUSE response with translated error description.
%% Logs an error with given log level.
%% @end
%%--------------------------------------------------------------------
-spec report_error(Request :: fslogic_worker:request(), Error :: term(),
    LogLevel :: debug | warning | error, Stacktrace :: term()) ->
    fslogic_worker:response().
report_error(Request, Error, LogLevel, Stacktrace) ->
    Status = #status{code = Code, description = Description} =
        fslogic_errors:gen_status_message(Error),
    MsgFormat =
        "Cannot process request ~p due to error: ~p (code: ~p)~nStacktrace: ~p",
    FormatArgs = [Request, Description, Code, Stacktrace],
    case LogLevel of
        debug -> ?debug_stacktrace(MsgFormat, FormatArgs);
        warning -> ?warning_stacktrace(MsgFormat, FormatArgs);
        error -> ?error_stacktrace(MsgFormat, FormatArgs)
    end,
    error_response(Request, Status).

%%--------------------------------------------------------------------
%% @doc
%% Returns response with given status, matching given request.
%% @end
%%--------------------------------------------------------------------
-spec error_response(fslogic_worker:request(), #status{}) ->
    #fuse_response{} | #provider_response{} | #proxyio_response{}.
error_response(#fuse_request{}, Status) ->
    #fuse_response{status = Status};
error_response(#file_request{}, Status) ->
    #fuse_response{status = Status};
error_response(#provider_request{}, Status) ->
    #provider_response{status = Status};
error_response(#proxyio_request{}, Status) ->
    #proxyio_response{status = Status}.

%%--------------------------------------------------------------------
%% @doc
%% Translates error ID to error description.
%% @end
%%--------------------------------------------------------------------
-spec describe_error(ErrorId :: atom()) -> ErrorDescription :: binary().
describe_error(ErrorId) ->
    case lists:keyfind(ErrorId, 1, ?ERROR_DESCRIPTIONS) of
        {ErrorId, ErrorDescription} -> ErrorDescription;
        false -> atom_to_binary(ErrorId, utf8)
    end.
