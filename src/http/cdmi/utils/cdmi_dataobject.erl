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
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-include_lib("ctool/include/logging.hrl").


%% API
-export([
    get_cdmi/2, get_binary/2,
    put_cdmi/2, put_binary/2,
    delete_cdmi/2
]).

%% the default json response for get/put cdmi_object will contain this entities,
%% they can be choosed selectively by appending '?name1;name2' list to the
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

-define(run(__FunctionCall), check_result(__FunctionCall)).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Handles GET requests for file, returning file content as response body.
%% @end
%%--------------------------------------------------------------------
-spec get_binary(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {term(), cowboy_req:req(), cdmi_handler:cdmi_req()}.
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
    Answer = get_file_info(NonEmptyOpts, CdmiReq),

    case maps:get(<<"value">>, Answer, undefined) of
        {range, Range} ->
            % prepare response
            BodyWithoutValue = maps:remove(<<"value">>, Answer),
            ValueTransferEncoding = cdmi_metadata:get_encoding(SessionId, {guid, FileGuid}),
            JsonBodyWithoutValue = json_utils:encode(BodyWithoutValue),
            JsonBodyPrefix = case BodyWithoutValue of
                #{} = Map when map_size(Map) =:= 0 -> <<"{\"value\":\"">>;
                _ ->
                    <<(erlang:binary_part(JsonBodyWithoutValue, 0, byte_size(JsonBodyWithoutValue) - 1))/binary, ",\"value\":\"">>
            end,
            JsonBodySuffix = <<"\"}">>,

            Req2 = cdmi_streamer:stream_cdmi(
                Req, CdmiReq, Range,
                ValueTransferEncoding, JsonBodyPrefix, JsonBodySuffix
            ),
            {stop, Req2, CdmiReq};
        undefined ->
            {json_utils:encode(Answer), Req, CdmiReq}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Handles PUT without cdmi content-type
%% @end
%%--------------------------------------------------------------------
-spec put_binary(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {term(), cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
put_binary(Req, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = Attrs
} = CdmiReq) ->
    % prepare request data
    Content = cowboy_req:header(<<"content-type">>, Req, <<"application/octet-stream">>),
    CdmiPartialFlag = cowboy_req:header(<<"x-cdmi-partial">>, Req),
    {MimeType, Encoding} = cdmi_parser:parse_content_type_header(Content),
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),
    case Attrs of
        undefined ->
            {ok, FileGuid} = ?run(lfm:create(SessionId, Path, DefaultMode)),
            cdmi_metadata:update_mimetype(SessionId, {guid, FileGuid}, MimeType),
            cdmi_metadata:update_encoding(SessionId, {guid, FileGuid}, Encoding),
            {ok, FileHandle} = ?run(lfm:open(SessionId, {path, Path}, write)),
            cdmi_metadata:update_cdmi_completion_status(SessionId, {guid, FileGuid}, <<"Processing">>),
            {ok, Req1} = write_body_to_file(Req, 0, FileHandle),
            ?run(lfm:fsync(FileHandle)),
            ?run(lfm:release(FileHandle)),
            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                SessionId, {path, Path}, CdmiPartialFlag
            ),
            {true, Req1, CdmiReq};
        #file_attr{guid = FileGuid, size = Size} ->
            cdmi_metadata:update_mimetype(SessionId, {guid, FileGuid}, MimeType),
            cdmi_metadata:update_encoding(SessionId, {guid, FileGuid}, Encoding),
            Length = cowboy_req:body_length(Req),
            case cdmi_parser:parse_content_range_header(Req, Size) of
                undefined ->
                    {ok, FileHandle} = ?run(lfm:open(SessionId, {guid, FileGuid}, write)),
                    cdmi_metadata:update_cdmi_completion_status(SessionId, {guid, FileGuid}, <<"Processing">>),
                    ?run(lfm:truncate(SessionId, {guid, FileGuid}, 0)),
                    {ok, Req2} = write_body_to_file(Req, 0, FileHandle),
                    ?run(lfm:fsync(FileHandle)),
                    ?run(lfm:release(FileHandle)),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                        SessionId, {guid, FileGuid}, CdmiPartialFlag
                    ),
                    {true, Req2, CdmiReq};
                {{From, To}, _ExpectedSize} when Length =:= undefined orelse Length =:= To - From + 1 ->
                    {ok, FileHandle} = ?run(lfm:open(SessionId, {guid, FileGuid}, write)),
                    cdmi_metadata:update_cdmi_completion_status(SessionId, {guid, FileGuid}, <<"Processing">>),
                    {ok, Req3} = write_body_to_file(Req, From, FileHandle),
                    ?run(lfm:fsync(FileHandle)),
                    ?run(lfm:release(FileHandle)),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
                        SessionId, {guid, FileGuid}, CdmiPartialFlag
                    ),
                    {true, Req3, CdmiReq};
                _ ->
                    throw(?ERROR_BAD_DATA(<<"content-range">>))
            end
    end.


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
    RequestedMimeType = maps:get(<<"mimetype">>, Body, undefined),
    RequestedValueTransferEncoding = maps:get(<<"valuetransferencoding">>, Body, undefined),
    RequestedCopyURI = maps:get(<<"copy">>, Body, undefined),
    RequestedMoveURI = maps:get(<<"move">>, Body, undefined),
    RequestedUserMetadata = maps:get(<<"metadata">>, Body, undefined),
    URIMetadataNames = [MetadataName || {OptKey, MetadataName} <- Options, OptKey == <<"metadata">>],
    Value = maps:get(<<"value">>, Body, undefined),
    Range = get_range(Options),
    RawValue = cdmi_encoder:decode(Value, RequestedValueTransferEncoding, Range),
    RawValueSize = byte_size(RawValue),
    {ok, DefaultMode} = application:get_env(?APP_NAME, default_file_mode),

    % create object using create/cp/mv
    {ok, OperationPerformed, Guid} =
        case {Attrs, RequestedCopyURI, RequestedMoveURI} of
            {undefined, undefined, undefined} ->
                {ok, NewGuid} = ?run(lfm:create(SessionId, Path, DefaultMode)),
                {ok, created, NewGuid};
            {#file_attr{guid = NewGuid}, undefined, undefined} ->
                {ok, none, NewGuid};
            {undefined, CopyURI, undefined} ->
                {ok, NewGuid} = ?run(lfm:cp(
                    SessionId,
                    {path, filepath_utils:ensure_begins_with_slash(CopyURI)},
                    Path
                )),
                {ok, copied, NewGuid};
            {undefined, undefined, MoveURI} ->
                {ok, NewGuid} = ?run(lfm:mv(
                    SessionId,
                    {path, filepath_utils:ensure_begins_with_slash(MoveURI)},
                    Path
                )),
                {ok, moved, NewGuid}
        end,

    % update value and metadata depending on creation type
    case OperationPerformed of
        created ->
            {ok, FileHandler} = ?run(lfm:open(SessionId, {guid, Guid}, write)),
            cdmi_metadata:update_cdmi_completion_status(SessionId, {guid, Guid}, <<"Processing">>),
            {ok, _, RawValueSize} = ?run(lfm:write(FileHandler, 0, RawValue)),
            ?run(lfm:fsync(FileHandler)),
            ?run(lfm:release(FileHandler)),

            % update cdmi metadata
            cdmi_metadata:update_encoding(SessionId, {guid, Guid}, utils:ensure_defined(
                RequestedValueTransferEncoding, undefined, <<"utf-8">>
            )),
            cdmi_metadata:update_mimetype(SessionId, {guid, Guid}, RequestedMimeType),
            cdmi_metadata:update_user_metadata(SessionId, {guid, Guid}, RequestedUserMetadata),
            cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(SessionId, {guid, Guid}, CdmiPartialFlag),

            % return response
            {ok, Attrs2} = ?run(lfm:stat(SessionId, {guid, Guid})),
            CdmiReq2 = CdmiReq#cdmi_req{file_attrs = Attrs2},
            Answer = get_file_info(?DEFAULT_PUT_FILE_OPTS, CdmiReq2),
            Req2 = cowboy_req:set_resp_body(json_utils:encode(Answer), Req0),
            {true, Req2, CdmiReq2};
        CopiedOrMoved when CopiedOrMoved =:= copied orelse CopiedOrMoved =:= moved ->
            % update cdmi metadata
            cdmi_metadata:update_encoding(SessionId, {guid, Guid}, RequestedValueTransferEncoding),
            cdmi_metadata:update_mimetype(SessionId, {guid, Guid}, RequestedMimeType),
            cdmi_metadata:update_user_metadata(SessionId, {guid, Guid}, RequestedUserMetadata, URIMetadataNames),

            % update cdmi metadata
            {ok, Attrs2} = ?run(lfm:stat(SessionId, {guid, Guid})),
            CdmiReq2 = CdmiReq#cdmi_req{file_attrs = Attrs2},
            Answer = get_file_info(?DEFAULT_PUT_FILE_OPTS, CdmiReq2),
            Req2 = cowboy_req:set_resp_body(json_utils:encode(Answer), Req0),
            {true, Req2, CdmiReq};
        none ->
            cdmi_metadata:update_encoding(SessionId, {guid, Guid}, RequestedValueTransferEncoding),
            cdmi_metadata:update_mimetype(SessionId, {guid, Guid}, RequestedMimeType),
            cdmi_metadata:update_user_metadata(SessionId, {guid, Guid}, RequestedUserMetadata, URIMetadataNames),
            case Range of
                {From, To} when is_binary(Value) andalso To - From + 1 == byte_size(RawValue) ->
                    {ok, FileHandler} = ?run(lfm:open(SessionId, {guid, Guid}, write)),
                    cdmi_metadata:update_cdmi_completion_status(SessionId, {guid, Guid}, <<"Processing">>),
                    {ok, _, RawValueSize} = ?run(lfm:write(FileHandler, From, RawValue)),
                    ?run(lfm:fsync(FileHandler)),
                    ?run(lfm:release(FileHandler)),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(SessionId, {guid, Guid}, CdmiPartialFlag),
                    {true, Req0, CdmiReq};
                undefined when is_binary(Value) ->
                    {ok, FileHandler} = ?run(lfm:open(SessionId, {guid, Guid}, write)),
                    cdmi_metadata:update_cdmi_completion_status(SessionId, {guid, Guid}, <<"Processing">>),
                    ?run(lfm:truncate(SessionId, {guid, Guid}, 0)),
                    {ok, _, RawValueSize} = ?run(lfm:write(FileHandler, 0, RawValue)),
                    ?run(lfm:fsync(FileHandler)),
                    ?run(lfm:release(FileHandler)),
                    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(SessionId, {guid, Guid}, CdmiPartialFlag),
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
    case lfm:unlink(SessionId, {guid, Guid}, false) of
        ok ->
            {true, Req, CdmiReq};
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Same as lists:keyfind/3, but returns Default when key is undefined
%% @end
%%--------------------------------------------------------------------
-spec get_range(Opts :: list()) -> {non_neg_integer(), non_neg_integer()} | undefined.
get_range(Opts) ->
    case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From_, To_} -> {From_, To_};
        false -> undefined
    end.


%% @private
-spec get_file_info([RequestedInfo :: binary()], cdmi_handler:cdmi_req()) ->
    map() | no_return().
get_file_info(RequestedInfo, #cdmi_req{
    client = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = #file_attr{guid = Guid} = Attrs
}) ->
    lists:foldl(
        fun
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
                        {ok, #file_attr{guid = ParentGuid}} = ?run(lfm:stat(
                            SessionId,
                            {path, filepath_utils:parent_dir(Path)}
                        )),
                        {ok, ParentObjectId} = file_id:guid_to_objectid(ParentGuid),
                        Acc#{<<"parentID">> => ParentObjectId}
                end;
            (<<"capabilitiesURI">>, Acc) ->
                Acc#{<<"capabilitiesURI">> => ?DATAOBJECT_CAPABILITY_PATH};
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
                            [<<"0-">>, integer_to_binary(Attrs#file_attr.size - 1)]
                        )} %todo fix 0--1 when file is empty
                end;
            (_, Acc) ->
                Acc
        end,
        #{},
        RequestedInfo
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reads request's body and writes it to file given by handler.
%% Returns updated request.
%% @end
%%--------------------------------------------------------------------
-spec write_body_to_file(cowboy_req:req(), integer(), lfm:handle()) ->
    {ok, cowboy_req:req()}.
write_body_to_file(Req0, Offset, FileHandle) ->
    {Status, Chunk, Req1} = cowboy_req:read_body(Req0),
    {ok, _NewHandle, Bytes} = ?run(lfm:write(FileHandle, Offset, Chunk)),
    case Status of
        more -> write_body_to_file(Req1, Offset + Bytes, FileHandle);
        ok -> {ok, Req1}
    end.


%% @private
-spec check_result(ok | {ok, term()} | {ok, term(), term()} | {error, term()}) ->
    ok | {ok, term()} | {ok, term(), term()} | no_return().
check_result(ok) -> ok;
check_result({ok, _} = Res) -> Res;
check_result({ok, _, _} = Res) -> Res;
check_result({error, Errno}) -> throw(?ERROR_POSIX(Errno)).
