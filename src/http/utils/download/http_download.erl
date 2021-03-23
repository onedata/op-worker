%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions for handling file download using http and cowboy.
%%% @end
%%%--------------------------------------------------------------------
-module(http_download).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/rest.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    download_file/3, download_file/4,
    download_tarball/4
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec download_file(session:id(), lfm_attrs:file_attributes(), cowboy_req:req()) ->
    cowboy_req:req().
download_file(SessionId, FileAttrs, Req) ->
    download_file(SessionId, FileAttrs, fun() -> ok end, Req).


-spec download_file(
    session:id(),
    lfm_attrs:file_attributes(),
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_file(SessionId, #file_attr{
    guid = FileGuid,
    name = FileName,
    size = FileSize
}, OnSuccessCallback, Req0) ->
    case http_parser:parse_range_header(Req0, FileSize) of
        invalid ->
            cowboy_req:reply(
                ?HTTP_416_RANGE_NOT_SATISFIABLE,
                #{?HDR_CONTENT_RANGE => str_utils:format_bin("bytes */~B", [FileSize])},
                Req0
            );
        Ranges ->
            Req1 = ensure_content_type_header_set(FileName, Req0),

            case lfm:monitored_open(SessionId, {guid, FileGuid}, read) of
                {ok, FileHandle} ->
                    try

                        http_streamer:stream_file(
                            Ranges, FileHandle, FileSize, 
                            fun(Data) -> Data end, 
                            fun() -> execute_on_success_callback(FileGuid, OnSuccessCallback) end, 
                            Req1
                        )
                    catch Type:Reason ->
                        {ok, UserId} = session:get_user_id(SessionId),
                        ?error_stacktrace("Error while processing file (~p) download "
                                          "for user ~p - ~p:~p", [
                            FileGuid, UserId, Type, Reason
                        ]),
                        http_req:send_error(Reason, Req1)
                    after
                        lfm:monitored_release(FileHandle)
                    end;
                {error, Errno} ->
                    http_req:send_error(?ERROR_POSIX(Errno), Req1)
            end
    end.


-spec download_tarball(
    session:id(),
    [lfm_attrs:file_attributes()],
    OnSuccessCallback :: fun(() -> ok),
    cowboy_req:req()
) ->
    cowboy_req:req().
download_tarball(SessionId, FileAttrsList, OnSuccessCallback, Req0) ->
    Req1 = http_streamer:stream_reply(?HTTP_200_OK, Req0),
    ok = tarball_streaming_traverse:run(FileAttrsList, SessionId, Req1),
    execute_on_success_callback(<<>>, OnSuccessCallback),
    Req1.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec ensure_content_type_header_set(file_meta:name(), cowboy_req:req()) -> cowboy_req:req().
ensure_content_type_header_set(FileName, Req) ->
    case cowboy_req:resp_header(?HDR_CONTENT_TYPE, Req, undefined) of
        undefined ->
            {Type, Subtype, _} = cow_mimetypes:all(FileName),
            cowboy_req:set_resp_header(?HDR_CONTENT_TYPE, [Type, "/", Subtype], Req);
        _ ->
            Req
    end.


%% @private
-spec execute_on_success_callback(fslogic_worker:file_guid(), OnSuccessCallback :: fun(() -> ok)) -> ok.
execute_on_success_callback(Guid, OnSuccessCallback) ->
    try
        ok = OnSuccessCallback()
    catch Type:Reason ->
        ?warning("Failed to execute file download successfully finished callback for file (~p) "
                 "due to ~p:~p", [Guid, Type, Reason])
    end.
