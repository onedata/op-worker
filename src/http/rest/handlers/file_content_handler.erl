%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The module handles streaming and uploading file content via REST API.
%%% @end
%%%-------------------------------------------------------------------
-module(file_content_handler).
-author("Bartosz Walkowicz").

-behaviour(cowboy_rest).

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% cowboy rest handler API
-export([init/2]).

-type scope_policy() :: allow_share_mode | disallow_share_mode.


%%%===================================================================
%%% API
%%%===================================================================


-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{method := <<"GET">>} = Req, State) ->
    {ok, handle_request(Req, allow_share_mode), State};

init(#{method := <<"PATCH">>} = Req, State) ->
    {ok, handle_request(Req, disallow_share_mode), State};

init(Req, State) ->
    NewReq = cowboy_req:reply(
        ?HTTP_405_METHOD_NOT_ALLOWED,
        #{?HDR_ALLOW => str_utils:join_binary([<<"GET">>, <<"PATCH">>], <<", ">>)},
        Req
    ),
    {ok, NewReq, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec handle_request(cowboy_req:req(), scope_policy()) -> cowboy_req:req().
handle_request(Req, ScopePolicy) ->
    try
        FileObjectId = cowboy_req:binding(id, Req),
        FileGuid = middleware_utils:decode_object_id(FileObjectId, id),
        ensure_operation_supported(FileGuid, ScopePolicy),

        Auth = authenticate_client(Req),
        ensure_authorized(Auth, FileGuid, ScopePolicy),

        middleware_utils:assert_file_managed_locally(FileGuid),
        process_request(Auth, FileGuid, Req)
    catch
        throw:Error ->
            http_req:send_error(Error, Req);
        Type:Reason ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Reason
            ]),
            cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req)
    end.


%% @private
-spec ensure_operation_supported(file_id:file_guid(), scope_policy()) ->
    ok | no_return().
ensure_operation_supported(FileGuid, ScopePolicy) ->
    case ScopePolicy of
        allow_share_mode ->
            ok;
        disallow_share_mode ->
            file_id:is_share_guid(FileGuid) andalso throw(?ERROR_NOT_SUPPORTED)
    end.


%% @private
-spec authenticate_client(cowboy_req:req()) -> aai:auth() | no_return().
authenticate_client(Req) ->
    case http_auth:authenticate(Req, rest, allow_data_access_caveats) of
        {ok, Auth} ->
            Auth;
        {error, _} = Error ->
            throw(Error)
    end.


%% @private
-spec ensure_authorized(aai:auth(), file_id:file_guid(), scope_policy()) ->
    true | no_return().
ensure_authorized(?GUEST, _FileGuid, disallow_share_mode) ->
    throw(?ERROR_UNAUTHORIZED);
ensure_authorized(Auth, FileGuid, _ScopePolicy) ->
    IsAuthorized = case file_id:is_share_guid(FileGuid) of
        true ->
            true;
        false ->
            middleware_utils:has_access_to_file(Auth, FileGuid)
    end,

    IsAuthorized orelse throw(?ERROR_FORBIDDEN).


%% @private
-spec process_request(aai:auth(), file_id:file_guid(), cowboy_req:req()) ->
    cowboy_req:req().
process_request(#auth{session_id = SessionId}, FileGuid, #{method := <<"GET">>} = Req) ->
    case lfm:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE} = FileAttrs} ->
            http_download_utils:stream_file(SessionId, FileAttrs, Req);
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            throw(?ERROR_POSIX(?EISDIR));
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end;

process_request(#auth{session_id = SessionId}, FileGuid, #{method := <<"PATCH">>} = Req) ->
    % TODO handle patch
    Req.
