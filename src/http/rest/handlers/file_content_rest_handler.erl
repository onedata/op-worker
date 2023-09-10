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

-type create_fun() :: fun((session:id(), file_id:file_guid(), file_meta:name(), file_meta:mode()) ->
    {ok, file_id:file_guid()} | {error, term()}).

-type create_link_fun() :: fun((session:id(), lfm:file_ref(), file_meta:name(),
file_id:file_guid() | file_meta:path()) -> {ok, file_id:file_guid()} | {error, term()}).

% timeout after which cowboy returns the data read from socket, regardless of its size
% the value was decided upon experimentally
-define(COWBOY_READ_BODY_PERIOD_SECONDS, 15).

-define(DEFAULT_CREATE_PARENTS_FLAG, false).

%% @TODO VFS-8986 - remove update_existing in create endpoint and allow for creation of non existing files in update endpoint
%% @TODO VFS-8986 - make update endpoint to work by path

%%%===================================================================
%%% API
%%%===================================================================


-spec handle_request(middleware:req(), cowboy_req:req()) -> cowboy_req:req().
handle_request(OpReq, Req) ->
    try
        handle_request_unsafe(OpReq, Req)
    catch
        Class:Reason:Stacktrace ->
            Error = case request_error_handler:infer_error(Reason) of
                {true, KnownError} ->
                    KnownError;
                false ->
                    ?examine_exception(Class, Reason, Stacktrace)
            end,
            http_req:send_error(Error, Req)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


handle_request_unsafe(OpReq0, Req) ->
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

    ?check(api_auth:check_authorization(Auth, ?OP_WORKER, Operation, GRI)),
    ensure_has_access_to_file(OpReq1),
    middleware_utils:assert_file_managed_locally(FileGuid),

    process_request(OpReq2, Req).


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
} = OpReq, Req) when
    Aspect == child;
    Aspect == file_at_path
->
    {AllRawParams, OptionalParamsDependingOnAspect, RequiredParamsDependingOnAspect} = case Aspect of
        child ->
            {RawParams, #{}, #{<<"name">> => {binary, non_empty}}};
        file_at_path ->
            {
                RawParams#{path => cowboy_req:path_info(Req)},
                #{<<"create_parents">> => {boolean, any}},
                #{path => {list_of_binaries, fun
                    ([]) ->
                        throw(?ERROR_MISSING_REQUIRED_VALUE(<<"path">>));
                    (PathTokens) ->
                        assert_valid_file_path(PathTokens),
                        {true, PathTokens}
                end
                }}
            }
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
        <<"offset">> => {integer, {not_lower_than, 0}},
        <<"update_existing">> => {boolean, any}
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
        path => {list_of_binaries, fun assert_valid_file_path/1},
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
    middleware_utils:has_access_to_file_space(Auth, Guid) orelse throw(?ERROR_FORBIDDEN).


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
            ?lfm_check(lfm:truncate(SessionId, FileRef, 0)),
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

    ParentGuid = resolve_target_file(OpReq),

    Name = case Aspect of
        child -> maps:get(<<"name">>, Params);
        file_at_path -> lists:last(maps:get(path, Params, <<"">>))
    end,

    UpdateExisting = maps:get(<<"update_existing">>, Params, false),

    {Guid, Req3} = case maps:get(<<"type">>, Params, ?REGULAR_FILE_TYPE) of
        ?DIRECTORY_TYPE ->
            Mode = maps:get(<<"mode">>, Params, ?DEFAULT_DIR_PERMS),
            {DirGuid, _NewFileCreated} = create(fun lfm:mkdir/4, SessionId, ParentGuid, Name, Mode, UpdateExisting),
            {DirGuid, Req};
        ?REGULAR_FILE_TYPE ->
            Mode = maps:get(<<"mode">>, Params, ?DEFAULT_FILE_PERMS),
            {FileGuid, NewFileCreated} = create(fun lfm:create/4, SessionId, ParentGuid, Name, Mode, UpdateExisting),

            case {maps:get(<<"offset">>, Params, 0), cowboy_req:has_body(Req)} of
                {0, false} ->
                    {FileGuid, Req};
                {Offset, _} ->
                    FileRef = ?FILE_REF(FileGuid),

                    try
                        Req2 = write_req_body_to_file(SessionId, FileRef, Offset, Req),
                        {FileGuid, Req2}
                    catch Type:Reason ->
                        NewFileCreated andalso lfm:unlink(SessionId, FileRef, false),
                        erlang:Type(Reason)
                    end
            end;
        ?LINK_TYPE ->
            TargetFileGuid = maps:get(<<"target_file_id">>, Params),
            {create_link(fun lfm:make_link/4, SessionId, ParentGuid, Name, TargetFileGuid, UpdateExisting), Req};
        ?SYMLINK_TYPE ->
            TargetFilePath = maps:get(<<"target_file_path">>, Params),
            {create_link(fun lfm:make_symlink/4, SessionId, ParentGuid, Name, TargetFilePath, UpdateExisting), Req}
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
    case ?lfm_check(lfm:stat(SessionId, ?FILE_REF(FileGuid, FollowSymlinks))) of
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
} = OpReq, Req) ->
    FileGuid = resolve_target_file(OpReq),
    ?check(lfm:rm_recursive(SessionId, ?FILE_REF(FileGuid))),
    http_req:send_response(?NO_CONTENT_REPLY, Req).


%% @private
-spec write_req_body_to_file(
    session:id(),
    lfm:file_ref(),
    Offset :: non_neg_integer(),
    cowboy_req:req()
) ->
    cowboy_req:req() | no_return().
write_req_body_to_file(SessionId, FileRef, Offset, Req) ->
    {ok, FileHandle} = ?lfm_check(lfm:monitored_open(SessionId, FileRef, write)),

    {ok, Req2} = file_upload_utils:upload_file(
        FileHandle, Offset, Req,
        fun cowboy_req:read_body/2,
        #{period => timer:seconds(?COWBOY_READ_BODY_PERIOD_SECONDS)}
    ),

    ?lfm_check(lfm:fsync(FileHandle)),
    ?lfm_check(lfm:monitored_release(FileHandle)),

    Req2.


%% @private
-spec assert_valid_file_path([binary()]) -> true | no_return().
assert_valid_file_path(PathTokens) ->
    case filepath_utils:sanitize(filepath_utils:join(PathTokens)) of
        {error, _} -> throw(?ERROR_BAD_VALUE_FILE_PATH);
        {ok, _} -> true
    end.


%% @private
-spec resolve_target_file(middleware:req()) -> fslogic_worker:file_guid() | no_return().
resolve_target_file(#op_req{
    operation = Operation,
    auth = #auth{session_id = SessionId},
    gri = #gri{id = BaseDirGuid, aspect = file_at_path},
    data = Data
}) ->
    PathInfo = maps:get(path, Data, []),
    Mode = maps:get(<<"mode">>, Data, ?DEFAULT_DIR_MODE),
    CreateDirs = maps:get(<<"create_parents">>, Data, ?DEFAULT_CREATE_PARENTS_FLAG),

    FilePath = case Operation of
        create -> filepath_utils:join(lists:droplast(PathInfo));
        _ -> filepath_utils:join(PathInfo)
    end,

    Result = case {Operation, CreateDirs} of
        {create, true} ->
            lfm:ensure_dir(SessionId, BaseDirGuid, FilePath, Mode);
        _ ->
            lfm:resolve_guid_by_relative_path(SessionId, BaseDirGuid, FilePath)
    end,

    case Result of
        {ok, ResolvedGuid} -> ResolvedGuid;
        {error, Errno} -> throw(?ERROR_POSIX(Errno))
    end;
resolve_target_file(#op_req{gri = #gri{id = TargetFileGuid}}) ->
    TargetFileGuid.


%% @private
-spec create(create_fun(), session:id(), file_id:file_guid(), file_meta:name(), file_meta:mode(),
    UpdateExisting :: boolean()) -> {file_id:file_guid(), NewFileCreated :: boolean()} | no_return().
create(CreateFun, SessionId, ParentGuid, Name, Mode, false) ->
    {ok, Guid} = ?lfm_check(CreateFun(SessionId, ParentGuid, Name, Mode)),
    {Guid, true};
create(CreateFun, SessionId, ParentGuid, Name, Mode, true) ->
    case CreateFun(SessionId, ParentGuid, Name, Mode) of
        {ok, CreatedFileGuid} ->
            {CreatedFileGuid, true};
        {error, ?EEXIST} ->
            {ResolvedGuid, NewFileCreated} = case resolve_guid(SessionId, ParentGuid, Name,
                fun() -> create(CreateFun, SessionId, ParentGuid, Name, Mode, true) end
            ) of
                {G, NFC} ->
                    {G, NFC};
                G ->
                    {G, false}
            end,
            ?lfm_check(lfm:set_perms(SessionId, ?FILE_REF(ResolvedGuid), Mode)),
            {ResolvedGuid, NewFileCreated};
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%% @private
-spec create_link(create_link_fun(), session:id(), file_id:file_guid(), file_meta:name(), file_meta:mode(),
    UpdateExisting :: boolean()) -> file_id:file_guid() | no_return().
create_link(CreateFun, SessionId, ParentGuid, Name, TargetFilePath, false) ->
    {ok, #file_attr{guid = SymlinkGuid}} =
        ?lfm_check(CreateFun(SessionId, ?FILE_REF(ParentGuid), Name, TargetFilePath)),
    SymlinkGuid;
create_link(CreateFun, SessionId, ParentGuid, Name, TargetFilePath, true) ->
    case CreateFun(SessionId, ?FILE_REF(ParentGuid), Name, TargetFilePath) of
        {ok, #file_attr{guid = LinkGuid}} ->
            LinkGuid;
        {error, ?EEXIST} ->
            delete_file(SessionId, ParentGuid, Name),
            create_link(CreateFun, SessionId, ParentGuid, Name, TargetFilePath, true);
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%% @private
-spec delete_file(session:id(), file_id:file_guid(), file_meta:name()) -> ok | no_return().
delete_file(SessionId, ParentGuid, Name) ->
    case resolve_guid(SessionId, ParentGuid, Name, fun() -> ok end) of
        ok -> ok;
        ResolvedGuid ->
            case lfm:unlink(SessionId, ?FILE_REF(ResolvedGuid), false) of
                ok -> ok;
                {error, ?ENOENT} -> ok;
                ?ERROR_NOT_FOUND -> ok;
                {error, Errno} -> throw(?ERROR_POSIX(Errno))
            end
    end.


%% @private
-spec resolve_guid(session:id(), file_id:file_guid(), file_meta:name(), fun(() -> Term)) ->
    file_id:file_guid() | Term.
resolve_guid(SessionId, ParentGuid, Name, Fallback) ->
    case lfm:resolve_guid_by_relative_path(SessionId, ParentGuid, Name) of
        {ok, ResolvedGuid} -> ResolvedGuid;
        {error, ?ENOENT} -> Fallback();
        ?ERROR_NOT_FOUND -> Fallback();
        {error, Errno} -> throw(?ERROR_POSIX(Errno))
    end.
