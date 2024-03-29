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

-include("global_definitions.hrl").
-include("http/rest.hrl").
-include("http/cdmi.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").


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
    auth = ?USER(_UserId, SessionId),
    file_attrs = FileAttrs = #file_attr{guid = FileGuid}
} = CdmiReq) ->
    % prepare response
    MimeType = cdmi_metadata:get_mimetype(SessionId, ?FILE_REF(FileGuid)),
    Req1 = cowboy_req:set_resp_header(?HDR_CONTENT_TYPE, MimeType, Req),
    Req2 = file_content_download_utils:download_single_file(SessionId, FileAttrs, Req1),
    {stop, Req2, CdmiReq}.


%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {term(), cowboy_req:req(), cdmi_handler:cdmi_req()}.
get_cdmi(Req, #cdmi_req{
    auth = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = FileGuid},
    options = Options
} = CdmiReq) ->
    NonEmptyOpts = utils:ensure_defined(Options, [], ?DEFAULT_GET_FILE_OPTS),
    FileInfo = get_file_info(NonEmptyOpts, CdmiReq),

    case maps:take(<<"value">>, FileInfo) of
        {{range, Range}, FileInfo2} ->
            Encoding = cdmi_metadata:get_encoding(SessionId, ?FILE_REF(FileGuid)),
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
%% offset (?HDR_CONTENT_RANGE) or if it is not specified completely
%% overwriting it.
%% If file doesn't exist then creates it first.
%% @end
%%--------------------------------------------------------------------
-spec put_binary(cowboy_req:req(), cdmi_handler:cdmi_req()) ->
    {true, cowboy_req:req(), cdmi_handler:cdmi_req()} | no_return().
put_binary(Req, #cdmi_req{
    auth = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = Attrs
} = CdmiReq) ->
    CdmiPartialFlag = cowboy_req:header(<<"x-cdmi-partial">>, Req),
    {MimeType, Encoding} = cdmi_parser:parse_content_type_header(Req),
    {FileGuid, Truncate, Offset} = case Attrs of
        undefined ->
            DefaultMode = op_worker:get_env(default_file_mode),
            {ok, Guid} = cdmi_lfm:create_file(SessionId, Path, DefaultMode),
            {Guid, false, 0};
        #file_attr{guid = Guid, size = Size} ->
            Length = cowboy_req:body_length(Req),
            case cdmi_parser:parse_content_range_header(Req, Size) of
                undefined ->
                    {Guid, true, 0};
                {{From, To}, _ExpectedSize} when Length =:= undefined; Length =:= To - From + 1 ->
                    {Guid, false, From};
                _ ->
                    throw(?ERROR_BAD_DATA(?HDR_CONTENT_RANGE))
            end
    end,

    FileRef = ?FILE_REF(FileGuid),
    cdmi_metadata:update_mimetype(SessionId, FileRef, MimeType),
    cdmi_metadata:update_encoding(SessionId, FileRef, Encoding),
    Req2 = write_req_body_to_file(
        Req, SessionId, FileRef,
        Truncate, Offset, CdmiPartialFlag
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
    auth = ?USER(_UserId, SessionId),
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

    % create object using create/cp/mv
    {ok, OperationPerformed, Guid} = case {Attrs, CopyURI, MoveURI} of
        {undefined, undefined, undefined} ->
            DefaultMode = op_worker:get_env(default_file_mode),
            {ok, NewGuid} = cdmi_lfm:create_file(SessionId, Path, DefaultMode),
            write_binary_to_file(
                SessionId, ?FILE_REF(NewGuid),
                false, 0, RawValue,
                CdmiPartialFlag
            ),
            {ok, created, NewGuid};
        {#file_attr{guid = NewGuid}, undefined, undefined} ->
            {ok, none, NewGuid};
        {_, CopyURI, undefined} ->
            {ok, NewGuid} = cdmi_lfm:cp(SessionId, CopyURI, Path),
            {ok, copied, NewGuid};
        {_, undefined, MoveURI} ->
            {ok, NewGuid} = cdmi_lfm:mv(SessionId, MoveURI, Path),
            {ok, moved, NewGuid}
    end,

    % update value and metadata depending on creation type
    FileRef = ?FILE_REF(Guid),
    case OperationPerformed of
        created ->
            cdmi_metadata:update_encoding(SessionId, FileRef, utils:ensure_defined(
                Encoding, undefined, <<"utf-8">>
            )),
            cdmi_metadata:update_mimetype(SessionId, FileRef, MimeType),
            cdmi_metadata:update_user_metadata(SessionId, FileRef, UserMetadata),
            prepare_create_file_cdmi_response(Req, CdmiReq, FileRef);
        CopiedOrMoved when CopiedOrMoved =:= copied orelse CopiedOrMoved =:= moved ->
            cdmi_metadata:update_encoding(SessionId, FileRef, Encoding),
            cdmi_metadata:update_mimetype(SessionId, FileRef, MimeType),
            cdmi_metadata:update_user_metadata(SessionId, FileRef, UserMetadata, URIMetadataNames),
            prepare_create_file_cdmi_response(Req, CdmiReq, FileRef);
        none ->
            cdmi_metadata:update_encoding(SessionId, FileRef, Encoding),
            cdmi_metadata:update_mimetype(SessionId, FileRef, MimeType),
            cdmi_metadata:update_user_metadata(SessionId, FileRef, UserMetadata, URIMetadataNames),
            case Range of
                {From, To} when is_binary(Value) andalso To - From + 1 == byte_size(RawValue) ->
                    write_binary_to_file(SessionId, FileRef, false, From, RawValue, CdmiPartialFlag),
                    {true, Req0, CdmiReq};
                undefined when is_binary(Value) ->
                    write_binary_to_file(SessionId, FileRef, true, 0, RawValue, CdmiPartialFlag),
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
    auth = ?USER(_UserId, SessionId),
    file_attrs = #file_attr{guid = Guid}
} = CdmiReq) ->
    ?lfm_check(lfm:unlink(SessionId, ?FILE_REF(Guid), false)),
    {true, Req, CdmiReq}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% @private
-spec prepare_create_file_cdmi_response(cowboy_req:req(), cdmi_handler:cdmi_req(),
    lfm:file_ref()) -> {true, cowboy_req:req(), cdmi_handler:cdmi_req()}.
prepare_create_file_cdmi_response(Req1, #cdmi_req{
    auth = ?USER(_UserId, SessionId)
} = CdmiReq, FileRef) ->
    {ok, Attrs} = ?lfm_check(lfm:stat(SessionId, FileRef)),
    CdmiReq2 = CdmiReq#cdmi_req{file_attrs = Attrs},
    Answer = get_file_info(?DEFAULT_PUT_FILE_OPTS, CdmiReq2),
    Req2 = cowboy_req:set_resp_body(json_utils:encode(Answer), Req1),
    {true, Req2, CdmiReq2}.


%% @private
-spec get_file_info([RequestedInfo :: binary()], cdmi_handler:cdmi_req()) ->
    map() | no_return().
get_file_info(RequestedInfo, #cdmi_req{
    auth = ?USER(_UserId, SessionId),
    file_path = Path,
    file_attrs = #file_attr{
        guid = Guid,
        parent_guid = ParentGuid,
        size = FileSize
    } = Attrs
}) ->
    FileRef = ?FILE_REF(Guid),

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
                    {ok, ParentObjectId} = file_id:guid_to_objectid(ParentGuid),
                    Acc#{<<"parentID">> => ParentObjectId}
            end;
        (<<"capabilitiesURI">>, Acc) ->
            Acc#{<<"capabilitiesURI">> => <<?DATAOBJECT_CAPABILITY_PATH>>};
        (<<"completionStatus">>, Acc) ->
            CompletionStatus = cdmi_metadata:get_cdmi_completion_status(
                SessionId, FileRef
            ),
            Acc#{<<"completionStatus">> => CompletionStatus};
        (<<"mimetype">>, Acc) ->
            MimeType = cdmi_metadata:get_mimetype(SessionId, FileRef),
            Acc#{<<"mimetype">> => MimeType};
        (<<"metadata">>, Acc) ->
            Metadata = cdmi_metadata:prepare_metadata(
                SessionId, FileRef, <<>>, Attrs
            ),
            Acc#{<<"metadata">> => Metadata};
        ({<<"metadata">>, Prefix}, Acc) ->
            Metadata = cdmi_metadata:prepare_metadata(
                SessionId, FileRef, Prefix, Attrs
            ),
            Acc#{<<"metadata">> => Metadata};
        (<<"valuetransferencoding">>, Acc) ->
            Encoding = cdmi_metadata:get_encoding(SessionId, FileRef),
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
                    )} %TODO VFS-7289 fix 0--1 when file is empty
            end;
        (_, Acc) ->
            Acc
    end, #{}, RequestedInfo).


%% @private
-spec write_req_body_to_file(cowboy_req:req(), session:id(), lfm:file_ref(),
    Truncate :: boolean(), Offset :: non_neg_integer(),
    CdmiPartialFlag :: undefined | binary()) ->
    cowboy_req:req().
write_req_body_to_file(Req, SessId, FileRef, Truncate, Offset, CdmiPartialFlag) ->
    {ok, FileHandle} = ?lfm_check(lfm:monitored_open(SessId, FileRef, write)),
    cdmi_metadata:update_cdmi_completion_status(
        SessId,
        FileRef,
        <<"Processing">>
    ),
    Truncate andalso ?lfm_check(lfm:truncate(SessId, FileRef, 0)),

    {ok, Req2} = file_upload_utils:upload_file(
        FileHandle, Offset, Req,
        fun cowboy_req:read_body/2, #{}
    ),

    ?lfm_check(lfm:fsync(FileHandle)),
    ?lfm_check(lfm:monitored_release(FileHandle)),
    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
        SessId,
        FileRef,
        CdmiPartialFlag
    ),
    Req2.


%% @private
-spec write_binary_to_file(session:id(), lfm:file_ref(), Truncate :: boolean(),
    Offset :: non_neg_integer(), Data :: binary(),
    CdmiPartialFlag :: undefined | binary()) ->
    ok.
write_binary_to_file(SessionId, FileRef, Truncate, Offset, Data, CdmiPartialFlag) ->
    DataSize = byte_size(Data),
    {ok, FileHandle} = ?lfm_check(lfm:monitored_open(SessionId, FileRef, write)),
    cdmi_metadata:update_cdmi_completion_status(
        SessionId,
        FileRef,
        <<"Processing">>
    ),
    Truncate andalso ?lfm_check(lfm:truncate(SessionId, FileRef, 0)),

    {ok, _, DataSize} = ?lfm_check(lfm:write(FileHandle, Offset, Data)),
    ?lfm_check(lfm:fsync(FileHandle)),
    ?lfm_check(lfm:monitored_release(FileHandle)),
    cdmi_metadata:set_cdmi_completion_status_according_to_partial_flag(
        SessionId,
        FileRef,
        CdmiPartialFlag
    ).
