%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when file upload page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_file_upload_new).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

-define(CONN_CLOSE_HEADERS, #{<<"connection">> => <<"close">>}).

%% Cowboy API
-export([handle/2]).

%% For test purpose
-export([multipart/3]).


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
                Req2 = multipart(Req, SessionId, []),
                cowboy_req:reply(?HTTP_200_OK, Req2)
            catch
                throw:{missing_param, _} ->
                    cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, ?CONN_CLOSE_HEADERS, Req);
                throw:stream_file_error ->
                    cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, ?CONN_CLOSE_HEADERS, Req);
                Type:Message ->
                    UserId = op_gui_session:get_user_id(),
                    ?error_stacktrace("Error while processing file upload "
                    "from user ~p - ~p:~p",
                        [UserId, Type, Message]),
                    % @todo VFS-1815 for now return 500,
                    % because retries are not stable
%%                    % Return 204 - resumable will retry the upload
%%                    cowboy_req:reply(204, #{}, <<"">>)
                    cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, ?CONN_CLOSE_HEADERS, Req)
            end
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses a multipart HTTP form.
%% @end
%%--------------------------------------------------------------------
-spec multipart(cowboy_req:req(), session:id(), Params :: proplists:proplist()) ->
    cowboy_req:req().
multipart(Req, SessionId, Params) ->
    {ok, UploadWriteSize} = application:get_env(?APP_NAME, upload_write_size),
    {ok, UploadReadTimeout} = application:get_env(?APP_NAME, upload_read_timeout),
    {ok, UploadPeriod} = application:get_env(?APP_NAME, upload_read_period),

    case cowboy_req:read_part(Req) of
        {ok, Headers, Req2} ->
            case cow_multipart:form_data(Headers) of
                {data, FieldName} ->
                    {ok, FieldValue, Req3} = cowboy_req:read_part_body(Req2),
                    multipart(Req3, SessionId, [{FieldName, FieldValue} | Params]);
                {file, _FieldName, _Filename, _CType} ->
                    FileGuid = get_bin_param(<<"fileId">>, Params),
                    {ok, FileHandle} = lfm:open(SessionId, {guid, FileGuid}, write),
                    ChunkNumber = get_int_param(
                        <<"resumableChunkNumber">>, Params),
                    ChunkSize = get_int_param(
                        <<"resumableChunkSize">>, Params),
                    % First chunk number in resumable is 1
                    Offset = ChunkSize * (ChunkNumber - 1),
                    % Set options for reading from socket
                    Opts = #{
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
                    },
                    Req3 = try
                        stream_file(Req2, FileHandle, Offset, Opts)
                    catch Type:Message ->
                        {ok, UserId} = session:get_user_id(SessionId),
                        ?error_stacktrace("Error while streaming file upload "
                        "from user ~p - ~p:~p",
                            [UserId, Type, Message]),
                        lfm:release(FileHandle), % release if possible
                        lfm:unlink(SessionId, {guid, FileGuid}, false),
                        throw(stream_file_error)
                    end,
                    multipart(Req3, SessionId, Params)
            end;
        {done, Req2} ->
            Req2
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stream a file upload based on file handle and write offset.
%% @end
%%--------------------------------------------------------------------
-spec stream_file(Req :: cowboy_req:req(),
    FileHandle :: lfm:handle(), Offset :: non_neg_integer(),
    Opts :: cowboy_req:read_body_opts()) -> cowboy_req:req().
stream_file(Req, FileHandle, Offset, Opts) ->
    case cowboy_req:read_part_body(Req, Opts) of
        {ok, Body, Req2} ->
            % @todo VFS-1815 handle errors, such as {error, enospc}
            {ok, _, _} = lfm:write(FileHandle, Offset, Body),
            ok = lfm:release(FileHandle),
            % @todo VFS-1815 register_chunk?
            % or send a message from client that uuid has finished?
            Req2;
        {more, Body, Req2} ->
            {ok, NewHandle, Written} =
                lfm:write(FileHandle, Offset, Body),
            stream_file(Req2, NewHandle, Offset + Written, Opts)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves a value from a proplist, converting it to integer.
%% @end
%%--------------------------------------------------------------------
-spec get_int_param(Key :: term(), Params :: proplists:proplist()) -> integer().
get_int_param(Key, Params) ->
    binary_to_integer(get_bin_param(Key, Params)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves a value from a proplist. Assumes all values are binary.
%% Throws when given key is not found.
%% @end
%%--------------------------------------------------------------------
-spec get_bin_param(Key :: term(), Params :: proplists:proplist()) -> binary().
get_bin_param(Key, Params) ->
    case proplists:get_value(Key, Params) of
        undefined ->
            throw({missing_param, Key});
        Value ->
            Value
    end.
