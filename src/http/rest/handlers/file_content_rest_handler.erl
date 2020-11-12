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
-module(file_content_rest_handler).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([handle_request/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_request(middleware:req(), cowboy_req:req()) -> cowboy_req:req().
handle_request(OpReq0, Req) ->
    OpReq1 = #op_req{
        auth = Auth,
        operation = Operation,
        gri = GRI = #gri{
            type = op_file,
            id = FileGuid,
            aspect = Aspect,
            scope = Scope
        }
    } = middleware_utils:switch_context_if_shared_file_request(OpReq0),

    ensure_operation_supported(Operation, Aspect, Scope),
    OpReq2 = sanitize_params(OpReq1),

    case api_auth:check_authorization(Auth, ?OP_WORKER, Operation, GRI) of
        ok -> ensure_has_access_to_file(OpReq1);
        {error, _} = Error -> throw(Error)
    end,
    middleware_utils:assert_file_managed_locally(FileGuid),

    process_request(OpReq2, Req).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_operation_supported(middleware:operation(), gri:aspect(), gri:scope()) ->
    true | no_return().
ensure_operation_supported(create, child, private) -> true;
ensure_operation_supported(create, content, private) -> true;
ensure_operation_supported(get, content, public) -> true;
ensure_operation_supported(get, content, private) -> true;
ensure_operation_supported(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%% @private
-spec sanitize_params(middleware:req()) -> middleware:req() | no_return().
sanitize_params(#op_req{operation = get, gri = #gri{aspect = content}} = OpReq) ->
    OpReq;
sanitize_params(#op_req{
    operation = create,
    data = RawParams,
    gri = #gri{aspect = content}
} = OpReq) ->
    OpReq#op_req{data = middleware_sanitizer:sanitize_data(RawParams, #{
        optional => #{<<"offset">> => {integer, {not_lower_than, 0}}}
    })};
sanitize_params(#op_req{
    operation = create,
    data = RawParams,
    gri = #gri{aspect = child}
} = OpReq) ->
    ModeParam = <<"mode">>,

    OpReq#op_req{data = middleware_sanitizer:sanitize_data(RawParams, #{
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
    })}.


%% @private
-spec ensure_has_access_to_file(middleware:req()) -> true | no_return().
ensure_has_access_to_file(#op_req{operation = get, auth = ?GUEST, gri = #gri{id = Guid, scope = public}}) ->
    file_id:is_share_guid(Guid) orelse throw(?ERROR_UNAUTHORIZED);
ensure_has_access_to_file(#op_req{auth = ?GUEST}) ->
    throw(?ERROR_UNAUTHORIZED);
ensure_has_access_to_file(#op_req{auth = Auth, gri = #gri{id = Guid}}) ->
    middleware_utils:has_access_to_file(Auth, Guid) orelse throw(?ERROR_FORBIDDEN).


%% @private
-spec process_request(middleware:req(), cowboy_req:req()) ->
    cowboy_req:req() | no_return().
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
            % Overwrite file if no explicit offset was given
            ?check(lfm:truncate(SessionId, FileKey, 0)),
            0;
        Num ->
            % Otherwise leave previous content and start writing from specified offset
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
