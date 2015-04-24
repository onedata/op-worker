%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements worker_plugin_behaviour callbacks.
%%%      Also it decides whether request has to be handled locally or rerouted
%%%      to other priovider.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic).
-behaviour(worker_plugin_behaviour).

-include("modules/fslogic/fslogic_common.hrl").
-include("errors.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").


-export([init/1, handle/2, cleanup/0, fslogic_runner/5, handle_fuse_message/2]).
-export([extract_logical_path/1]).


%%%===================================================================
%%% Types
%%%===================================================================

-type ctx() :: #fslogic_ctx{}.
-type file() :: file_meta:entry(). %% Type alias for better code organization
-type open_flags() :: read | write | rwrd.
-type posix_permissions() :: non_neg_integer().

-export_type([ctx/0, file/0, open_flags/0, posix_permissions/0]).


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: term()} | {error, Reason :: term()}.

init(_Args) ->
    {ok, undefined}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request, State :: term()) -> Result when
    Request :: ping | healthcheck | {spawn_handler, SocketPid :: pid()},
    Result :: nagios_handler:healthcheck_reponse() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping, _) ->
    pong;

handle(healthcheck, _) ->
    ok;

handle({#fslogic_ctx{} = CTX, #fusemessage{input = RequestBody}}, _) ->
    RequestType = element(1, RequestBody),
    fslogic_runner(fun maybe_handle_fuse_message/2, CTX, RequestType, RequestBody);

%% Handle requests that have wrong structure.
handle(_Request, _State) ->
    ?log_bad_request(_Request),
    erlang:error(wrong_request).


%% maybe_handle_fuse_message/1
%%--------------------------------------------------------------------
%% @doc Tries to handle fuse message locally (i.e. handle_fuse_message/1) or delegate request to 'provider_proxy' module.
%% @end
-spec maybe_handle_fuse_message(CTX :: ctx(), RequestBody :: tuple()) -> Result :: term().
%%--------------------------------------------------------------------
maybe_handle_fuse_message(CTX, RequestBody) ->
    %% @todo: get space data for given file
    {ok, #space_info{name = SpaceName, providers = Providers} = SpaceInfo} = {ok, #space_info{providers = [crypto:rand_bytes(10)]}},

    Self = crypto:rand_bytes(10), %% @todo: get proper provider id

    case lists:member(Self, Providers) of
        true ->
            handle_fuse_message(CTX, RequestBody);
        false ->
            PrePostProcessResponse = try
                case fslogic_remote:prerouting(SpaceInfo, RequestBody, Providers) of
                    {ok, {reroute, Self, RequestBody1}} ->  %% Request should be handled locally for some reason
                        {ok, handle_fuse_message(CTX, RequestBody1)};
                    {ok, {reroute, RerouteToProvider, RequestBody1}} ->
                        RemoteResponse = undefined, %% @todo: implement message rerouting
                        {ok, RemoteResponse};
                    %% @todo: uncomment those lines after implementing slogic_remote:prerouting/3
%%                     {ok, {response, Response}} -> %% Do not handle this request and return custom response
%%                         {ok, Response};
                    {error, PreRouteError} ->
                        ?error("Cannot initialize reouting for request ~p due to error in prerouting handler: ~p", [RequestBody, PreRouteError]),
                        throw({unable_to_reroute_message, {prerouting_error, PreRouteError}})
                end
            catch
                Type:Reason ->
                    ?error_stacktrace("Unable to process remote fslogic request due to: ~p", [{Type, Reason}]),
                    {error, {Type, Reason}}
            end,
            %% Remote response may need some tweaking...
            case fslogic_remote:postrouting(SpaceInfo, PrePostProcessResponse, RequestBody) of
                undefined -> throw({unable_to_reroute_message, PrePostProcessResponse});
                LocalResponse -> LocalResponse
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.


%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------


%%--------------------------------------------------------------------
%% @doc Runs Method(RequestBody) while catching errors and translating them with
%%      fslogic_errors module.
%% @end
%%--------------------------------------------------------------------
-spec fslogic_runner(Method :: function(), CTX :: ctx(), RequestType :: atom(), RequestBody :: term()) -> Response :: term().
fslogic_runner(Method, CTX, RequestType, RequestBody) when is_function(Method) ->
    fslogic_runner(Method, CTX, RequestType, RequestBody, fslogic_errors).


%%--------------------------------------------------------------------
%% @doc Runs Method(RequestBody) while catching errors and translating them with
%%      given ErrorHandler module. ErrorHandler module has to export at least gen_error_message/2 (see fslogic_errors:gen_error_message/1).
%% @end
%%--------------------------------------------------------------------
-spec fslogic_runner(Method :: function(), CTX :: ctx(), RequestType :: atom(), RequestBody :: term(), ErrorHandler :: atom()) -> Response :: term().
fslogic_runner(Method, CTX, RequestType, RequestBody, ErrorHandler) when is_function(Method) ->
    try
        ?debug("Processing request (type ~p): ~p", [RequestType, RequestBody]),
        Method(CTX, RequestBody)
    catch
        Reason ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Manually thrown error, normal interrupt case.
            ?debug_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ErrorHandler:gen_error_message(RequestType, ErrorCode);
        error:{badmatch, Reason} ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Bad Match assertion - something went wrong, but it could be expected.
            ?warning_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ErrorHandler:gen_error_message(RequestType, ErrorCode);
        error:{case_clause, Reason} ->
            {ErrorCode, ErrorDetails} = fslogic_errors:gen_error_code(Reason),
            %% Bad Match assertion - something went seriously wrong and we should know about it.
            ?error_stacktrace("Cannot process request ~p due to error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ErrorHandler:gen_error_message(RequestType, ErrorCode);
        error:UnkError ->
            {ErrorCode, ErrorDetails} = {?EREMOTEIO, UnkError},
            %% Bad Match assertion - something went horribly wrong. This should not happen.
            ?error_stacktrace("Cannot process request ~p due to unknown error: ~p (code: ~p)", [RequestBody, ErrorDetails, ErrorCode]),
            ErrorHandler:gen_error_message(RequestType, ErrorCode)
    end.


%%--------------------------------------------------------------------
%% @doc Processes requests from FUSE.
%% @end
%%--------------------------------------------------------------------
-spec handle_fuse_message(CTX :: ctx(), Record :: tuple()) -> no_return().
handle_fuse_message(CTX, _Req = #chmod{uuid = FileUUID, mode = Mode}) ->
    fslogic_req_generic:chmod(CTX, FileUUID, Mode);

handle_fuse_message(CTX, _Req = #getattr{uuid = FileUUID}) ->
    fslogic_req_generic:get_attrs(CTX, FileUUID);

handle_fuse_message(_CTX, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @doc Convinience method that returns logical file path for the operation.
%% @end
%%--------------------------------------------------------------------
-spec extract_logical_path(Record :: tuple()) -> file_meta:path() | undefined.
extract_logical_path(_) ->
    <<"/">>.
