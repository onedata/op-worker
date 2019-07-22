%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides function to operate on cdmi dataobjects (files).
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_dataobject).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest.hrl").
-include("http/cdmi.hrl").
-include("global_definitions.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").


%% API
-export([
    get_cdmi/2, get_binary/2,
    put_cdmi/2, put_binary/2,
    delete_cdmi/2
]).

%% the default json response for get/put cdmi_object will contain this entities,
%% they can be chosen selectively by appending '?name1;name2' list to the
%% request url
-define(DEFAULT_GET_FILE_OPTS,
    [
        <<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>,
        <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>,
        <<"metadata">>, <<"mimetype">>, <<"valuetransferencoding">>,
        <<"valuerange">>, <<"value">>
    ]
).
-define(DEFAULT_PUT_FILE_OPTS,
    [
        <<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>,
        <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>,
        <<"metadata">>, <<"mimetype">>
    ]
).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Handles GET requests for file, returning file content as response body.
%% @end
%%--------------------------------------------------------------------
-spec get_binary(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {stop, cowboy_req:req(), cdmi_handler:cdmi_req()}.
get_binary(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = FileGuid, size = Size}
} = CdmiReq) ->
    % prepare response
    Ranges = cdmi_parser:parse_range_header(Req, Size),
    MimeType = cdmi_metadata:get_mimetype(SessionId, {guid, FileGuid}),
    Req1 = cowboy_req:set_resp_header(<<"content-type">>, MimeType, Req),
    HttpStatus = case Ranges of
        undefined -> ?HTTP_200_OK;
        _ -> ?HTTP_206_PARTIAL_CONTENT
    end,
    Req2 = cdmi_streamer:stream_binary(HttpStatus, Req1, CdmiReq, Ranges),
    {stop, Req2, CdmiReq}.


%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {term(), cowboy_req:req(), cdmi_handler:cdmi_req()}.
get_cdmi(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = FileGuid},
    options = Options
} = CdmiReq) ->
    NonEmptyOpts = utils:ensure_defined(Options, [], ?DEFAULT_GET_FILE_OPTS),
    FileInfo = get_file_info(NonEmptyOpts, CdmiReq),

    case maps:take(<<"value">>, FileInfo) of
        {{range, Range}, FileInfo2} ->
            Encoding = cdmi_metadata:get_encoding(SessionId, {guid, FileGuid}),
            DataPrefix = case map_size(FileInfo2) of
                0 ->
                    <<"{\"value\":\"">>;
                _ ->
                    EncodedFileInfo1 = json_utils:encode(FileInfo2),
                    % Closing '}' must be removed to append streaming content
                    EncodedFileInfo2 = erlang:binary_part(
                        EncodedFileInfo1,
                        0,
                        byte_size(EncodedFileInfo1) - 1
                    ),
                    <<EncodedFileInfo2/binary, ",\"value\":\"">>
            end,
            DataSuffix = <<"\"}">>,

            Req2 = cdmi_streamer:stream_cdmi(
                Req, CdmiReq, Range, Encoding,
                DataPrefix, DataSuffix
            ),
            {stop, Req2, CdmiReq};
        error ->
            {json_utils:encode(FileInfo), Req, CdmiReq}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Writes given content (request body) to file starting from specified
%% offset (content-range header) or if it is not specified completely
%% overwriting it.
%% If file doesn't exist then creates it first.
%% @end
%%--------------------------------------------------------------------
-spec put_binary(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {true, cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
put_binary(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = Attrs
} = CdmiReq) ->
    CdmiPartialFlag = cowboy_req:header(<<"x-cdmi-partial">>, Req),
    {MimeType, Encoding} = cdmi_parser:parse_content_type_header(Req),
    {Guid, Truncate, Offset} = case Attrs of
        undefined ->
            {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
            {ok, FileGuid} = ?check(lfm:create(SessionId, Path, DefaultMode)),
            cdmi_metadata:update_mimetype(SessionId, {guid, FileGuid}, MimeType),
            cdmi_metadata:update_encoding(SessionId, {guid, FileGuid}, Encoding),
            {FileGuid, false, 0};
        #file_attr{guid = FileGuid, size = Size} ->
            cdmi_metadata:update_mimetype(SessionId, {guid, FileGuid}, MimeType),
            cdmi_metadata:update_encoding(SessionId, {guid, FileGuid}, Encoding),
            Length = cowboy_req:body_length(Req),
            case cdmi_parser:parse_content_range_header(Req, Size) of
                undefined ->
                    {FileGuid, true, 0};
                {{From, To}, _ExpectedSize} when Length =:= undefined; Length =:= To - From + 1 ->
                    {FileGuid, false, From};
                _ ->
                    throw(?ERROR_BAD_DATA(<<"content-range">>))
            end
    end,

    FileKey = {guid, Guid},
    {ok, FileHandle} = ?check(lfm:open(SessionId, FileKey, write)),
    cdmi_metadata:update_cdmi_completion_status(
        SessionId,
        FileKey,
        <<"Processing">>
    ),
    Truncate andalso ?check(lfm:truncate(SessionId, FileKey, 0)),

    {ok, Req2} = write_req_body_to_file(Req, Offset, FileHandle),
    ?check(lfm:fsync(FileHandle)),
    ?check(lfm:release(FileHandle)),
    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
        SessionId,
        FileKey,
        CdmiPartialFlag
    ),
    {true, Req2, CdmiReq}.


%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec put_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {term(), cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
put_cdmi(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = Attrs,
    options = Options
} = CdmiReq) ->
    % parse body
    {ok, Body, Req0} = cdmi_parser:parse_body(Req),

    % prepare necessary data
    CdmiPartialFlag = cowboy_req:header(<<"x-cdmi-partial">>, Req0),
    MimeType = maps:get(<<"mimetype">>, Body, undefined),
    Encoding = maps:get(<<"valuetransferencoding">>, Body, undefined),
    CopyURI = maps:get(<<"copy">>, Body, undefined),
    MoveURI = maps:get(<<"move">>, Body, undefined),
    UserMetadata = maps:get(<<"metadata">>, Body, undefined),
    URIMetadataNames = [MetadataName || {<<"metadata">>, MetadataName} <- Options],

    Value = maps:get(<<"value">>, Body, undefined),
    Range = case lists:keyfind(<<"value">>, 1, Options) of
        {<<"value">>, Begin, End} -> {Begin, End};
        false -> undefined
    end,
    RawValue = cdmi_encoder:decode(Value, Encoding, Range),
    RawValueSize = byte_size(RawValue),

    % create object using create/cp/mv
    {ok, OperationPerformed, Guid} = case {Attrs, CopyURI, MoveURI} of
        {undefined, undefined, undefined} ->
            {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
            {ok, NewGuid} = ?check(lfm:create(SessionId, Path, DefaultMode)),
            {ok, created, NewGuid};
        {#file_attr{guid = NewGuid}, undefined, undefined} ->
            {ok, none, NewGuid};
        {undefined, CopyURI, undefined} ->
            {ok, NewGuid} = ?check(lfm:cp(
                SessionId,
                {path, filepath_utils:ensure_begins_with_slash(CopyURI)},
                Path
            )),
            {ok, copied, NewGuid};
        {undefined, undefined, MoveURI} ->
            {ok, NewGuid} = ?check(lfm:mv(
                SessionId,
                {path, filepath_utils:ensure_begins_with_slash(MoveURI)},
                Path
            )),
            {ok, moved, NewGuid}
    end,

    % update value and metadata depending on creation type
    FileKey = {guid, Guid},
    case OperationPerformed of
        created ->
            {ok, FileHandler} = ?check(lfm:open(SessionId, FileKey, write)),
            cdmi_metadata:update_cdmi_completion_status(
                SessionId,
                FileKey,
                <<"Processing">>
            ),
            {ok, _, RawValueSize} = ?check(lfm:write(FileHandler, 0, RawValue)),
            ?check(lfm:fsync(FileHandler)),
            ?check(lfm:release(FileHandler)),

            % update cdmi metadata
            cdmi_metadata:update_encoding(SessionId, FileKey, utils:ensure_defined(
                Encoding, undefined, <<"utf-8">>
            )),
            cdmi_metadata:update_mimetype(SessionId, FileKey, MimeType),
            cdmi_metadata:update_user_metadata(SessionId, FileKey, UserMetadata),
            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(SessionId, FileKey, CdmiPartialFlag),

            % return response
            {ok, Attrs2} = ?check(lfm:stat(SessionId, FileKey)),
            CdmiReq2 = CdmiReq#cdmi_req{file_attrs = Attrs2},
            FileInfo = get_file_info(?DEFAULT_PUT_FILE_OPTS, CdmiReq2),
            Req2 = cowboy_req:set_resp_body(json_utils:encode(FileInfo), Req0),
            {true, Req2, CdmiReq2};
        CopiedOrMoved when CopiedOrMoved =:= copied orelse CopiedOrMoved =:= moved ->
            % update cdmi metadata
            cdmi_metadata:update_encoding(SessionId, FileKey, Encoding),
            cdmi_metadata:update_mimetype(SessionId, FileKey, MimeType),
            cdmi_metadata:update_user_metadata(SessionId, FileKey, UserMetadata, URIMetadataNames),

            % update cdmi metadata
            {ok, Attrs2} = ?check(lfm:stat(SessionId, FileKey)),
            CdmiReq2 = CdmiReq#cdmi_req{file_attrs = Attrs2},
            FileInfo = get_file_info(?DEFAULT_PUT_FILE_OPTS, CdmiReq2),
            Req2 = cowboy_req:set_resp_body(json_utils:encode(FileInfo), Req0),
            {true, Req2, CdmiReq};
        none ->
            cdmi_metadata:update_encoding(SessionId, FileKey, Encoding),
            cdmi_metadata:update_mimetype(SessionId, FileKey, MimeType),
            cdmi_metadata:update_user_metadata(SessionId, FileKey, UserMetadata, URIMetadataNames),
            case Range of
                {From, To} when is_binary(Value) andalso To - From + 1 == byte_size(RawValue) ->
                    {ok, FileHandler} = ?check(lfm:open(SessionId, FileKey, write)),
                    cdmi_metadata:update_cdmi_completion_status(SessionId, FileKey, <<"Processing">>),
                    {ok, _, RawValueSize} = ?check(lfm:write(FileHandler, From, RawValue)),
                    ?check(lfm:fsync(FileHandler)),
                    ?check(lfm:release(FileHandler)),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(SessionId, FileKey, CdmiPartialFlag),
                    {true, Req0, CdmiReq};
                undefined when is_binary(Value) ->
                    {ok, FileHandler} = ?check(lfm:open(SessionId, FileKey, write)),
                    cdmi_metadata:update_cdmi_completion_status(SessionId, FileKey, <<"Processing">>),
                    ?check(lfm:truncate(SessionId, FileKey, 0)),
                    {ok, _, RawValueSize} = ?check(lfm:write(FileHandler, 0, RawValue)),
                    ?check(lfm:fsync(FileHandler)),
                    ?check(lfm:release(FileHandler)),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(SessionId, FileKey, CdmiPartialFlag),
                    {true, Req0, CdmiReq};
                undefined ->
                    {true, Req0, CdmiReq};
                _MalformedRange ->
                    throw(?ERROR_BAD_DATA(<<"range">>))
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes specified file (dataobject).
%% @end
%%--------------------------------------------------------------------
-spec delete_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {true, cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
delete_cdmi(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = Guid}
} = CdmiReq) ->
    ?check(lfm:unlink(SessionId, {guid, Guid}, false)),
    {true, Req, CdmiReq}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% @private
-spec get_file_info([RequestedInfo :: binary()], cdmi_handler:cdmi_req()) ->
    map() | no_return().
get_file_info(RequestedInfo, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = #file_attr{guid = Guid, size = FileSize} = Attrs
}) ->
    lists:foldl(fun
        (<<"objectType">>, Acc) ->
            Acc#{<<"objectType">> => <<"application/cdmi-object">>};
        (<<"objectID">>, Acc) ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),
            Acc#{<<"objectID">> => ObjectId};
        (<<"objectName">>, Acc) ->
            Acc#{<<"objectName">> => filename:basename(Path)};
        (<<"parentURI">>, Acc) ->
            ParentURI = case Path of
                <<"/">> -> <<>>;
                _ -> filepath_utils:parent_dir(Path)
            end,
            Acc#{<<"parentURI">> => ParentURI};
        (<<"parentID">>, Acc) ->
            case Path of
                <<"/">> ->
                    Acc;
                _ ->
                    {ok, #file_attr{guid = ParentGuid}} = ?check(lfm:stat(
                        SessionId,
                        {path, filepath_utils:parent_dir(Path)}
                    )),
                    {ok, ParentObjectId} = file_id:guid_to_objectid(ParentGuid),
                    Acc#{<<"parentID">> => ParentObjectId}
            end;
        (<<"capabilitiesURI">>, Acc) ->
            Acc#{<<"capabilitiesURI">> => <<?DATAOBJECT_CAPABILITY_PATH>>};
        (<<"completionStatus">>, Acc) ->
            CompletionStatus = cdmi_metadata:get_cdmi_completion_status(
                SessionId, {guid, Guid}
            ),
            Acc#{<<"completionStatus">> => CompletionStatus};
        (<<"mimetype">>, Acc) ->
            MimeType = cdmi_metadata:get_mimetype(SessionId, {guid, Guid}),
            Acc#{<<"mimetype">> => MimeType};
        (<<"metadata">>, Acc) ->
            Metadata = cdmi_metadata:prepare_metadata(
                SessionId, {guid, Guid}, <<>>, Attrs
            ),
            Acc#{<<"metadata">> => Metadata};
        ({<<"metadata">>, Prefix}, Acc) ->
            Metadata = cdmi_metadata:prepare_metadata(
                SessionId, {guid, Guid}, Prefix, Attrs
            ),
            Acc#{<<"metadata">> => Metadata};
        (<<"valuetransferencoding">>, Acc) ->
            Encoding = cdmi_metadata:get_encoding(SessionId, {guid, Guid}),
            Acc#{<<"valuetransferencoding">> => Encoding};
        (<<"value">>, Acc) ->
            Acc#{<<"value">> => {range, default}};
        ({<<"value">>, From, To}, Acc) ->
            Acc#{<<"value">> => {range, {From, To}}};
        (<<"valuerange">>, Acc) ->
            case lists:keyfind(<<"value">>, 1, RequestedInfo) of
                {<<"value">>, From, To} ->
                    Acc#{<<"valuerange">> => iolist_to_binary(
                        [integer_to_binary(From), <<"-">>, integer_to_binary(To)]
                    )};
                _ ->
                    Acc#{<<"valuerange">> => iolist_to_binary(
                        [<<"0-">>, integer_to_binary(FileSize - 1)]
                    )} %todo fix 0--1 when file is empty
            end;
        (_, Acc) ->
            Acc
    end, #{}, RequestedInfo).


%% @private
-spec write_req_body_to_file(cowboy_req:req(), integer(), lfm:handle()) ->
    {ok, cowboy_req:req()}.
write_req_body_to_file(Req0, Offset, FileHandle) ->
    {Status, Chunk, Req1} = cowboy_req:read_body(Req0),
    {ok, _NewHandle, Bytes} = ?check(lfm:write(FileHandle, Offset, Chunk)),
    case Status of
        more -> write_req_body_to_file(Req1, Offset + Bytes, FileHandle);
        ok -> {ok, Req1}
    end.
