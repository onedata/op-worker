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
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% Dynamic page behaviour API
-export([handle/2]).

%% For test purpose
-export([handle_multipart_req/3]).

-define(CONN_CLOSE_HEADERS, #{<<"connection">> => <<"close">>}).


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
        [<<"x-auth-token">>, <<"content-type">>],
        Req
    );
handle(<<"POST">>, InitialReq) ->
    Req = gui_cors:allow_origin(oneprovider:get_oz_url(), InitialReq),
    case op_gui_session:authenticate(Req) of
        ?ERROR_UNAUTHORIZED ->
            cowboy_req:reply(?HTTP_401_UNAUTHORIZED, ?CONN_CLOSE_HEADERS, Req);
        false ->
            cowboy_req:reply(?HTTP_401_UNAUTHORIZED, ?CONN_CLOSE_HEADERS, Req);
        {ok, Identity, Auth} ->
            Host = cowboy_req:host(Req),
            SessionId = op_gui_session:initialize(Identity, Auth, Host),
            try
                Req2 = handle_multipart_req(Req, SessionId, #{}),
                cowboy_req:reply(?HTTP_200_OK, Req2)
            catch
                throw:upload_not_registered ->
                    cowboy_req:reply(?HTTP_403_FORBIDDEN, ?CONN_CLOSE_HEADERS, Req);
                throw:Error ->
                    ErrorResp = rest_translator:error_response(Error),
                    cowboy_req:reply(
                        ErrorResp#rest_resp.code,
                        maps:merge(ErrorResp#rest_resp.headers, ?CONN_CLOSE_HEADERS),
                        json_utils:encode(ErrorResp#rest_resp.body),
                        Req
                    );
                Type:Message ->
                    UserId = op_gui_session:get_user_id(),
                    ?error_stacktrace("Error while processing file upload "
                                      "from user ~p - ~p:~p", [
                        UserId, Type, Message
                    ]),
                    cowboy_req:reply(
                        ?HTTP_500_INTERNAL_SERVER_ERROR,
                        ?CONN_CLOSE_HEADERS, Req
                    )
            end
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% @private
-spec handle_multipart_req(cowboy_req:req(), session:id(), map()) ->
    cowboy_req:req().
handle_multipart_req(Req, SessionId, Params) ->
    ReadBodyOpts = get_read_body_opts(),

    case cowboy_req:read_part(Req) of
        {ok, Headers, Req2} ->
            case cow_multipart:form_data(Headers) of
                {data, FieldName} ->
                    {ok, FieldValue, Req3} = cowboy_req:read_part_body(Req2),
                    handle_multipart_req(Req3, SessionId, Params#{
                        FieldName => FieldValue
                    });
                {file, _FieldName, _Filename, _CType} ->
                    Req3 = write_chunk(Req2, SessionId, Params, ReadBodyOpts),
                    handle_multipart_req(Req3, SessionId, Params)
            end;
        {done, Req2} ->
            Req2
    end.


%% @private
-spec get_read_body_opts() -> cowboy_req:read_body_opts().
get_read_body_opts() ->
    {ok, UploadWriteSize} = application:get_env(?APP_NAME, upload_write_size),
    {ok, UploadReadTimeout} = application:get_env(?APP_NAME, upload_read_timeout),
    {ok, UploadPeriod} = application:get_env(?APP_NAME, upload_read_period),

    #{
        % length is chunk size - how much the cowboy read
        % function returns at once.
        length => UploadWriteSize,
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
-spec write_chunk(cowboy_req:req(), session:id(), map(),
    cowboy_req:read_body_opts()) -> cowboy_req:req().
write_chunk(Req, SessionId, Params, ReadBodyOpts) ->
    SanitizedParams = op_sanitizer:sanitize_data(Params, #{
        required => #{
            <<"guid">> => {binary, non_empty},
            <<"resumableChunkNumber">> => {integer, {not_lower_than, 0}},
            <<"resumableChunkSize">> => {integer, {not_lower_than, 0}}
        }
    }),

    FileGuid = maps:get(<<"guid">>, SanitizedParams),
    assert_file_upload_registered(FileGuid),
    {ok, FileHandle} = ?check(lfm:open(SessionId, {guid, FileGuid}, write)),

    ChunkNumber = maps:get(<<"resumableChunkNumber">>, SanitizedParams),
    ChunkSize = maps:get(<<"resumableChunkSize">>, SanitizedParams),
    Offset = ChunkSize * (ChunkNumber - 1),

    try
        write_binary(Req, FileHandle, Offset, ReadBodyOpts)
    after
        lfm:release(FileHandle) % release if possible
    end.


%% @private
-spec write_binary(cowboy_req:req(), lfm:handle(), non_neg_integer(),
    cowboy_req:read_body_opts()) -> cowboy_req:req().
write_binary(Req, FileHandle, Offset, ReadBodyOpts) ->
    case cowboy_req:read_part_body(Req, ReadBodyOpts) of
        {ok, Body, Req2} ->
            ?check(lfm:write(FileHandle, Offset, Body)),
            Req2;
        {more, Body, Req2} ->
            {ok, NewHandle, Written} = ?check(lfm:write(
                FileHandle, Offset, Body
            )),
            write_binary(Req2, NewHandle, Offset + Written, ReadBodyOpts)
    end.


%% @private
-spec assert_file_upload_registered(file_id:file_guid()) -> ok | no_return().
assert_file_upload_registered(FileGuid) ->
    UserId = op_gui_session:get_user_id(),
    case file_upload_manager:is_upload_registered(UserId, FileGuid) of
        true -> ok;
        false -> throw(upload_not_registered)
    end.
