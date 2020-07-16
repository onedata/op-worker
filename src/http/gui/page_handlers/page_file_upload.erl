%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when file upload page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_file_upload).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-behaviour(dynamic_page_behaviour).

-include("http/rest.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

%% Dynamic page behaviour API
-export([handle/2]).

%% For test purpose
-export([handle_multipart_req/3]).


%% ====================================================================
%% dynamic_page_behaviour API functions
%% ====================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"OPTIONS">>, Req) ->
    gui_cors:options_response(
        oneprovider:get_oz_url(),
        [<<"POST">>],
        [?HDR_X_AUTH_TOKEN, ?HDR_CONTENT_TYPE],
        Req
    );
handle(<<"POST">>, InitialReq) ->
    Req = gui_cors:allow_origin(oneprovider:get_oz_url(), InitialReq),
    case http_auth:authenticate(Req, graphsync, disallow_data_access_caveats) of
        {ok, ?USER(UserId) = Auth} ->
            try
                Req2 = handle_multipart_req(Req, Auth, #{}),
                cowboy_req:reply(?HTTP_200_OK, Req2)
            catch
                throw:upload_not_registered ->
                    reply_with_error(?ERROR_FORBIDDEN, Req);
                throw:Error ->
                    reply_with_error(Error, Req);
                Type:Message ->
                    ?error_stacktrace("Error while processing file upload "
                                      "from user ~p - ~p:~p", [
                        UserId, Type, Message
                    ]),
                    reply_with_error(?ERROR_INTERNAL_SERVER_ERROR, Req)
            end;
        {ok, ?GUEST} ->
            reply_with_error(?ERROR_UNAUTHORIZED, Req);
        {error, _} = Error ->
            reply_with_error(Error, Req)
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% @private
-spec handle_multipart_req(cowboy_req:req(), aai:auth(), map()) ->
    cowboy_req:req().
handle_multipart_req(Req, Auth, Params) ->
    case cowboy_req:read_part(Req) of
        {ok, Headers, Req2} ->
            case cow_multipart:form_data(Headers) of
                {data, FieldName} ->
                    {ok, FieldValue, Req3} = cowboy_req:read_part_body(Req2),
                    handle_multipart_req(Req3, Auth, Params#{
                        FieldName => FieldValue
                    });
                {file, _FieldName, _Filename, _CType} ->
                    {ok, Req3} = write_chunk(Req2, Auth, Params),
                    handle_multipart_req(Req3, Auth, Params)
            end;
        {done, Req2} ->
            Req2
    end.


%% @private
-spec write_chunk(cowboy_req:req(), aai:auth(), map()) ->
    {ok, cowboy_req:req()} | no_return().
write_chunk(Req, ?USER(UserId, SessionId), Params) ->
    SanitizedParams = middleware_sanitizer:sanitize_data(Params, #{
        required => #{
            <<"guid">> => {binary, non_empty},
            <<"resumableChunkNumber">> => {integer, {not_lower_than, 0}},
            <<"resumableChunkSize">> => {integer, {not_lower_than, 0}}
        }
    }),
    FileGuid = maps:get(<<"guid">>, SanitizedParams),
    ChunkSize = maps:get(<<"resumableChunkSize">>, SanitizedParams),
    ChunkNumber = maps:get(<<"resumableChunkNumber">>, SanitizedParams),

    assert_file_upload_registered(UserId, FileGuid),

    SpaceId = file_id:guid_to_space_id(FileGuid),
    Offset = ChunkSize * (ChunkNumber - 1),
    {ok, FileHandle} = ?check(lfm:open(SessionId, {guid, FileGuid}, write)),

    try
        file_upload_utils:upload_file(
            FileHandle, Offset, Req,
            fun cowboy_req:read_part_body/2, read_body_opts(SpaceId)
        )
    after
        lfm:release(FileHandle) % release if possible
    end.


%% @private
-spec assert_file_upload_registered(od_user:id(), file_id:file_guid()) ->
    ok | no_return().
assert_file_upload_registered(UserId, FileGuid) ->
    case file_upload_manager:is_upload_registered(UserId, FileGuid) of
        true -> ok;
        false -> throw(upload_not_registered)
    end.


%% @private
-spec read_body_opts(od_space:id()) -> cowboy_req:read_body_opts().
read_body_opts(SpaceId) ->
    WriteBlockSize = file_upload_utils:get_preferable_write_block_size(SpaceId),

    {ok, UploadWriteSize} = application:get_env(?APP_NAME, upload_write_size),
    {ok, UploadReadTimeout} = application:get_env(?APP_NAME, upload_read_timeout),
    {ok, UploadPeriod} = application:get_env(?APP_NAME, upload_read_period),

    #{
        % length is chunk size - how much the cowboy read
        % function returns at once.
        length => utils:ensure_defined(WriteBlockSize, UploadWriteSize),
        % Maximum timeout after which body read from request
        % is passed to upload handler process.
        % Note that the body is returned immediately
        % if its size reaches the buffer size (length above).
        period => UploadPeriod,
        % read timeout - the read will fail if read_length
        % is not satisfied within this time.
        timeout => UploadReadTimeout
    }.


%% @private
-spec reply_with_error(errors:error(), cowboy_req:req()) -> cowboy_req:req().
reply_with_error(Error, Req) ->
    ErrorResp = rest_translator:error_response(Error),
    cowboy_req:reply(
        ErrorResp#rest_resp.code,
        maps:merge(ErrorResp#rest_resp.headers, #{?HDR_CONNECTION => <<"close">>}),
        json_utils:encode(ErrorResp#rest_resp.body),
        Req
    ).
