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

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% cowboy rest handler API
-export([handle_request/2]).

-type scope_policy() :: allow_share_mode | disallow_share_mode.


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_request(#op_req{}, cowboy_req:req()) -> cowboy_req:req().
handle_request(#op_req{operation = Operation, auth = OriginalAuth, gri = #gri{
    type = op_file,
    id = FileGuid,
    aspect = Aspect
}, data = RawParams} = OpReq, Req) ->
    try
        RawParams = http_parser:parse_query_string(Req),
        SanitizedParams = sanitize_params(RawParams, Req),
        ScopePolicy = get_scope_policy(Operation, Aspect),

        Auth = ensure_proper_context(OriginalAuth, FileGuid, ScopePolicy),
        ensure_authorized(Auth, FileGuid, ScopePolicy),
        middleware_utils:assert_file_managed_locally(FileGuid),

        process_request(OpReq#op_req{auth = Auth, data = SanitizedParams}, Req)
    catch
        throw:Error ->
            http_req:send_error(Error, Req);
        Type:Reason ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Reason
            ]),
            cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req)
    end.



%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_scope_policy(middleware:operation(), gri:aspect()) -> scope_policy().
get_scope_policy(create, child) -> disallow_share_mode;
get_scope_policy(create, content) -> disallow_share_mode;
get_scope_policy(get, content) -> allow_share_mode.


%% @private
-spec sanitize_params(RawParams :: map(), cowboy_req:req()) -> map() | no_return().
sanitize_params(_RawParams, #{method := <<"GET">>}) ->
    #{};
sanitize_params(RawParams, #{method := <<"PUT">>}) ->
    middleware_sanitizer:sanitize_data(RawParams, #{
        optional => #{<<"offset">> => {integer, {not_lower_than, 0}}}
    });
sanitize_params(RawParams, #{method := <<"POST">>}) ->
    ModeParam = <<"mode">>,

    middleware_sanitizer:sanitize_data(RawParams, #{
        required => #{
            <<"name">> => {binary, non_empty}
        },
        optional => #{
            <<"type">> => {binary, [<<"reg">>, <<"dir">>]},
            ModeParam => {binary, fun(Mode) ->
                try binary_to_integer(Mode, 8) of
                    ValidMode when ValidMode >= 0 andalso ValidMode =< 8#1777 ->
                        {true, ValidMode};
                    _ ->
                        throw(?ERROR_BAD_VALUE_NOT_IN_RANGE(ModeParam, 0, 8#1777))
                catch _:_ ->
                    throw(?ERROR_BAD_VALUE_INTEGER(ModeParam))
                end
            end},
            <<"offset">> => {integer, {not_lower_than, 0}}
        }
    }).


%% @private
-spec ensure_proper_context(aai:auth(), file_id:file_guid(), scope_policy()) ->
    aai:auth() | no_return().
ensure_proper_context(Auth, FileGuid, ScopePolicy) ->
    case {file_id:is_share_guid(FileGuid), ScopePolicy} of
        {true, allow_share_mode} ->
            ?GUEST;
        {true, disallow_share_mode} ->
            throw(?ERROR_NOT_SUPPORTED);
        {false, _} ->
            Auth
    end.


%% @private
-spec ensure_authorized(aai:auth(), file_id:file_guid(), scope_policy()) ->
    true | no_return().
ensure_authorized(?GUEST, _FileGuid, disallow_share_mode) ->
    throw(?ERROR_UNAUTHORIZED);
ensure_authorized(Auth, FileGuid, disallow_share_mode) ->
    middleware_utils:has_access_to_file(Auth, FileGuid) orelse throw(?ERROR_FORBIDDEN);
ensure_authorized(?GUEST, FileGuid, allow_share_mode) ->
    file_id:is_share_guid(FileGuid) orelse throw(?ERROR_UNAUTHORIZED);
ensure_authorized(Auth, FileGuid, allow_share_mode) ->
    case file_id:is_share_guid(FileGuid) of
        true ->
            true;
        false ->
            middleware_utils:has_access_to_file(Auth, FileGuid) orelse throw(?ERROR_FORBIDDEN)
    end.


%% @private
-spec process_request(#op_req{}, cowboy_req:req()) -> cowboy_req:req() | no_return().
process_request(#op_req{
    operation = get,
    auth = #auth{session_id = SessionId},
    gri = #gri{id = FileGuid, aspect = content}
}, Req) ->
    case lfm:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE} = FileAttrs} ->
            http_download_utils:stream_file(SessionId, FileAttrs, Req);
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} ->
            throw(?ERROR_POSIX(?EISDIR));
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end;

process_request(#op_req{
    operation = create,
    auth = #auth{session_id = SessionId},
    gri = #gri{id = FileGuid, aspect = content},
    data = Params
}, Req) ->
    FileKey = {guid, FileGuid},

    Offset = case maps:get(<<"offset">>, Params, undefined) of
        undefined ->
            ?check(lfm:truncate(SessionId, FileKey, 0)),
            0;
        Num ->
            Num
    end,

    Req2 = write_req_body_to_file(SessionId, FileKey, Offset, Req),
    http_req:send_response(?NO_CONTENT_REPLY, Req2);

process_request(#op_req{
    operation = create,
    auth = #auth{session_id = SessionId},
    gri = #gri{id = ParentGuid, aspect = child},
    data = Params
}, Req) ->
    Name = maps:get(<<"name">>, Params),
    Mode = maps:get(<<"mode">>, Params, undefined),

    {Guid, Req3} = case maps:get(<<"type">>, Params, <<"reg">>) of
        <<"reg">> ->
            {ok, FileGuid} = ?check(lfm:create(SessionId, ParentGuid, Name, Mode)),

            case {maps:get(<<"offset">>, Params, 0), cowboy_req:has_body(Req)} of
                {0, false} ->
                    {FileGuid, Req};
                {Offset, _} ->
                    try
                        Req2 = write_req_body_to_file(SessionId, {guid, FileGuid}, Offset, Req),
                        {FileGuid, Req2}
                    catch Type:Reason ->
                        lfm:unlink(SessionId, {guid, FileGuid}, false),
                        erlang:Type(Reason)
                    end
            end;
        <<"dir">> ->
            {ok, DirGuid} = ?check(lfm:mkdir(SessionId, ParentGuid, Name, Mode)),
            {DirGuid, Req}
    end,
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),

    http_req:send_response(
        ?CREATED_REPLY([<<"data">>, ObjectId], #{<<"fileId">> => ObjectId}),
        Req3
    ).


%% @private
-spec write_req_body_to_file(
    session:id(),
    lfm:file_key(),
    Offset :: non_neg_integer(),
    cowboy_req:req()
) ->
    cowboy_req:req() | no_return().
write_req_body_to_file(SessionId, FileKey, Offset, Req) ->
    {ok, FileHandle} = ?check(lfm:monitored_open(SessionId, FileKey, write)),

    {ok, Req2} = file_upload_utils:upload_file(
        FileHandle, Offset, Req,
        fun cowboy_req:read_body/2, #{}
    ),

    ?check(lfm:fsync(FileHandle)),
    ?check(lfm:monitored_release(FileHandle)),

    Req2.
