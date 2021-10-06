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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([handle_request/2]).


% timeout after which cowboy returns the data read from socket, regardless of its size
% the value was decided upon experimentally
-define(COWBOY_READ_BODY_PERIOD_SECONDS, 15).

-define(DEFAULT_CREATE_PARENTS_FLAG, false).


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
    OpReq2 = sanitize_params(OpReq1, Req),

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
ensure_operation_supported(create, file_at_path, private) -> true;
ensure_operation_supported(get, content, public) -> true;
ensure_operation_supported(get, content, private) -> true;
ensure_operation_supported(get, file_at_path, private) -> true;
ensure_operation_supported(delete, file_at_path, private) -> true;
ensure_operation_supported(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%% @private
-spec sanitize_params(middleware:req(), cowboy_req:req()) -> middleware:req() | no_return().
sanitize_params(#op_req{
    operation = create,
    data = RawParams,
    gri = #gri{aspect = content}
} = OpReq, _Req) ->
    OpReq#op_req{data = middleware_sanitizer:sanitize_data(RawParams, #{
        optional => #{<<"offset">> => {integer, {not_lower_than, 0}}}
    })};
sanitize_params(#op_req{
    operation = create,
    data = RawParams,
    gri = #gri{aspect = Aspect}
} = OpReq, Req) when Aspect == child orelse Aspect == file_at_path
    ->
    {AllRawParams, OptionalParamsDependingOnAspect, RequiredParamsDependingOnAspect} = case Aspect of
        child ->
            {RawParams, #{}, #{<<"name">> => {binary, non_empty}}};
        file_at_path ->
            {RawParams#{path => cowboy_req:path_info(Req)}, #{
                <<"create_parents">> => {boolean, any}
            }, #{
                path => {list_of_binaries, fun(PathTokens) ->
                    case PathTokens == [] of
                        true ->
                            throw(?ERROR_BAD_VALUE_EMPTY(<<"path">>));
                        false -> ok
                    end,
                    JoinedPath = str_utils:join_as_binaries(PathTokens, <<"/">>),
                    case filepath_utils:sanitize(JoinedPath) of
                        {error, _} ->
                            throw(?ERROR_BAD_VALUE_PATH);
                        {ok, _} -> ok
                    end,
                    {true, PathTokens}
                end}
            }}

    end,

    AllRequiredParams = case maps:get(<<"type">>, AllRawParams, undefined) of
        <<"LNK">> ->
            RequiredParamsDependingOnAspect#{<<"target_file_id">> => {binary, fun(ObjectId) ->
                {true, middleware_utils:decode_object_id(ObjectId, <<"target_file_id">>)}
            end}};
        <<"SYMLNK">> ->
            RequiredParamsDependingOnAspect#{<<"target_file_path">> => {binary, non_empty}};
        _ ->
            % Do not do anything - exception will be raised by middleware_sanitizer
            RequiredParamsDependingOnAspect
    end,
    AllOptionalParams = OptionalParamsDependingOnAspect#{
        <<"type">> => {atom, [
            ?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE, ?LINK_TYPE, ?SYMLINK_TYPE
        ]},
        <<"mode">> => {binary, fun(Mode) ->
            try binary_to_integer(Mode, 8) of
                ValidMode when ValidMode >= 0 andalso ValidMode =< 8#1777 ->
                    {true, ValidMode};
                _ ->
                    % TODO VFS-7536 add basis of number system to ?ERROR_BAD_VALUE_NOT_IN_RANGE
                    throw(?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"mode">>, 0, 8#1777))
            catch _:_ ->
                % TODO VFS-7536 add basis of number system to ?ERROR_BAD_VALUE_NOT_IN_RANGE
                throw(?ERROR_BAD_VALUE_INTEGER(<<"mode">>))
            end
        end},
        <<"offset">> => {integer, {not_lower_than, 0}}
    },
    OpReq#op_req{data = middleware_sanitizer:sanitize_data(AllRawParams, #{
        required => AllRequiredParams,
        optional => AllOptionalParams
    })};
sanitize_params(#op_req{operation = get, data = RawParams, gri = #gri{aspect = content}} = OpReq, _Req) ->
    OpReq#op_req{data = middleware_sanitizer:sanitize_data(RawParams, #{
        optional => #{<<"follow_symlinks">> => {boolean, any}}
    })};
sanitize_params(#op_req{
    operation = Operation,
    data = RawParams,
    gri = #gri{aspect = file_at_path}
} = OpReq, Req) when Operation == delete orelse Operation == get ->

    AllRawParams = RawParams#{path => cowboy_req:path_info(Req)},
    AllOptionalParams = #{
        path => {list_of_binaries, fun(PathTokens) ->
            JoinedPath = str_utils:join_as_binaries(PathTokens, <<"/">>),
            case filepath_utils:sanitize(JoinedPath) of
                {error, _} -> throw(?ERROR_BAD_VALUE_PATH);
                {ok, _} -> ok
            end,
            {true, PathTokens}
        end
        },
        <<"follow_symlinks">> => {boolean, any}
    },

    OpReq#op_req{data = middleware_sanitizer:sanitize_data(AllRawParams, #{
        optional => AllOptionalParams
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
    operation = create,
    auth = #auth{session_id = SessionId},
    gri = #gri{id = FileGuid, aspect = content},
    data = Params
}, Req) ->
    FileRef = ?FILE_REF(FileGuid),

    Offset = case maps:get(<<"offset">>, Params, undefined) of
        undefined ->
            % Overwrite file if no explicit offset was given
            ?check(lfm:truncate(SessionId, FileRef, 0)),
            0;
        Num ->
            % Otherwise leave previous content and start writing from specified offset
            Num
    end,

    Req2 = write_req_body_to_file(SessionId, FileRef, Offset, Req),
    http_req:send_response(?NO_CONTENT_REPLY, Req2);
process_request(#op_req{
    operation = create,
    auth = #auth{session_id = SessionId},
    gri = #gri{aspect = Aspect},
    data = Params
} = OpReq, Req) when Aspect == child orelse Aspect == file_at_path ->

    FileGuid = resolve_target_file(OpReq),

    Name = case Aspect of
        content -> maps:get(<<"name">>, Params);
        file_at_path -> lists:last(cowboy_req:path_info(Req))
    end,

    Mode = maps:get(<<"mode">>, Params, undefined),

    {Guid, Req3} = case maps:get(<<"type">>, Params, ?REGULAR_FILE_TYPE) of
        ?DIRECTORY_TYPE ->
            {ok, CreatedDirGuid} = ?check(lfm:mkdir(SessionId, FileGuid, Name, Mode)),
            {CreatedDirGuid, Req};
        ?REGULAR_FILE_TYPE ->
            {ok, CreatedFileGuid} = ?check(lfm:create(SessionId, FileGuid, Name, Mode)),
            FileRef = ?FILE_REF(CreatedFileGuid),

            case {maps:get(<<"offset">>, Params, 0), cowboy_req:has_body(Req)} of
                {0, false} ->
                    {CreatedFileGuid, Req};
                {Offset, _} ->
                    try
                        Req2 = write_req_body_to_file(SessionId, FileRef, Offset, Req),
                        {CreatedFileGuid, Req2}
                    catch Type:Reason ->
                        lfm:unlink(SessionId, FileRef, false),
                        erlang:Type(Reason)
                    end
            end;
        ?LINK_TYPE ->
            TargetFileGuid = maps:get(<<"target_file_id">>, Params),

            {ok, #file_attr{guid = LinkGuid}} = ?check(lfm:make_link(
                SessionId, ?FILE_REF(TargetFileGuid), ?FILE_REF(FileGuid), Name
            )),
            {LinkGuid, Req};
        ?SYMLINK_TYPE ->
            TargetFilePath = maps:get(<<"target_file_path">>, Params),

            {ok, #file_attr{guid = SymlinkGuid}} = ?check(lfm:make_symlink(
                SessionId, ?FILE_REF(FileGuid), Name, TargetFilePath
            )),
            {SymlinkGuid, Req}
    end,
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),

    http_req:send_response(
        ?CREATED_REPLY([<<"data">>, ObjectId], #{<<"fileId">> => ObjectId}),
        Req3
    );
process_request(#op_req{
    operation = get,
    auth = #auth{session_id = SessionId},
    gri = #gri{aspect = Aspect},
    data = Data
} = OpReq, Req) when Aspect == content orelse Aspect == file_at_path ->

    FileGuid = resolve_target_file(OpReq),

    FollowSymlinks = maps:get(<<"follow_symlinks">>, Data, true),
    case ?check(lfm:stat(SessionId, ?FILE_REF(FileGuid, FollowSymlinks))) of
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE} = FileAttrs} ->
            file_download_utils:download_single_file(SessionId, FileAttrs, Req);
        {ok, #file_attr{type = ?SYMLINK_TYPE} = FileAttrs} ->
            file_download_utils:download_single_file(SessionId, FileAttrs, Req);
        {ok, #file_attr{}} ->
            case page_file_download:gen_file_download_url(SessionId, [FileGuid], FollowSymlinks) of
                {ok, Url} ->
                    cowboy_req:reply(?HTTP_302_FOUND, #{?HDR_LOCATION => Url}, Req);
                {error, _} = Error ->
                    http_req:send_error(Error, Req)
            end
    end;
process_request(#op_req{
    operation = delete,
    auth = #auth{session_id = SessionId},
    gri = #gri{aspect = file_at_path}
} = OpReq, _Req) ->
    FileGuid = resolve_target_file(OpReq),
    lfm:rm_recursive(SessionId, ?FILE_REF(FileGuid)).


%% @private
-spec write_req_body_to_file(
    session:id(),
    lfm:file_ref(),
    Offset :: non_neg_integer(),
    cowboy_req:req()
) ->
    cowboy_req:req() | no_return().
write_req_body_to_file(SessionId, FileRef, Offset, Req) ->
    {ok, FileHandle} = ?check(lfm:monitored_open(SessionId, FileRef, write)),

    {ok, Req2} = file_upload_utils:upload_file(
        FileHandle, Offset, Req,
        fun cowboy_req:read_body/2,
        #{period => timer:seconds(?COWBOY_READ_BODY_PERIOD_SECONDS)}
    ),

    ?check(lfm:fsync(FileHandle)),
    ?check(lfm:monitored_release(FileHandle)),

    Req2.


%% @private
-spec resolve_target_file(middleware:req()) -> fslogic_worker:file_guid().
resolve_target_file(#op_req{
    operation = Operation,
    auth = #auth{session_id = SessionId},
    gri = #gri{id = BaseDirGuid, aspect = file_at_path},
    data = Data
}) ->
    PathInfo = maps:get(path, Data),

    CreateDirs = case Operation of
        create -> maps:get(<<"create_parents">>, Data, ?DEFAULT_CREATE_PARENTS_FLAG);
        _ -> false
    end,

    FilePath = case Operation of
        create -> str_utils:join_as_binaries(lists:droplast(PathInfo), <<"/">>);
        _ -> str_utils:join_as_binaries(PathInfo, <<"/">>)
    end,

    Mode = maps:get(<<"mode">>, Data, ?DEFAULT_DIR_MODE),

    case lfm:resolve_guid_by_relative_path(SessionId, BaseDirGuid, FilePath, CreateDirs, Mode) of
        {ok, ResolvedGuid} -> ResolvedGuid;
        {error, ?ENOENT} -> throw(?ERROR_POSIX(?ENOENT));
        {error, ?ENOTDIR} -> throw(?ERROR_POSIX(?ENOTDIR));
        {error, ?EACCES} -> throw(?ERROR_POSIX(?EACCES))
    end;
resolve_target_file(#op_req{gri = #gri{id = TargetFileGuid}}) ->
    TargetFileGuid.