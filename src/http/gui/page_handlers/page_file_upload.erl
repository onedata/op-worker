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
-module(page_file_upload).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

% Key of in-memory mapping of uploads kept in session.
-define(UPLOAD_MAP, upload_map).

% How many tries of creating a unique filename before failure.
-define(MAX_UNIQUE_FILENAME_COUNTER, 20).

% Maximum time a process will be waiting for chunk 1 to generate file handle.
-define(MAX_WAIT_FOR_FILE_HANDLE, 60000).

% Interval between retries to resolve file handle.
-define(INTERVAL_WAIT_FOR_FILE_HANDLE, 300).

%% Cowboy API
-export([handle/2]).
%% API
-export([upload_map_insert/3, upload_map_delete/2]).
-export([wait_for_file_new_file_id/2]).
-export([clean_upload_map/1]).

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
            cowboy_req:reply(401, #{<<"connection">> => <<"close">>}, Req);
        false ->
            cowboy_req:reply(401, #{<<"connection">> => <<"close">>}, Req);
        {ok, Identity, Auth} ->
            Host = cowboy_req:host(Req),
            SessionId = op_gui_session:initialize(Identity, Auth, Host),
            try
                Req2 = multipart(Req, SessionId, []),
                cowboy_req:reply(200, Req2)
            catch
                throw:{missing_param, _} ->
                    cowboy_req:reply(500, #{<<"connection">> => <<"close">>}, Req);
                throw:stream_file_error ->
                    cowboy_req:reply(500, #{<<"connection">> => <<"close">>}, Req);
                Type:Message ->
                    UserId = op_gui_session:get_user_id(),
                    ?error_stacktrace("Error while processing file upload "
                    "from user ~p - ~p:~p",
                        [UserId, Type, Message]),
                    % @todo VFS-1815 for now return 500,
                    % because retries are not stable
%%                    % Return 204 - resumable will retry the upload
%%                    cowboy_req:reply(204, #{}, <<"">>)
                    cowboy_req:reply(500, #{<<"connection">> => <<"close">>}, Req)
            end
    end.


%% ====================================================================
%% API functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Inserts a UploadId - FileId pair into upload map.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_insert(session:id(), UploadId :: term(), FileId :: term()) ->
    ok | {error, term()}.
upload_map_insert(_SessionId, _UploadId, undefined) ->
    error(badarg);

upload_map_insert(_SessionId, undefined, _FileId) ->
    error(badarg);

upload_map_insert(SessionId, UploadId, FileId) ->
    MapUpdateFun = fun(Map) ->
        maps:put(UploadId, FileId, Map)
    end,
    op_gui_session:update_value(SessionId, ?UPLOAD_MAP, MapUpdateFun, #{}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves a FileId from upload map by UploadId or returns a default value.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_lookup(session:id(), UploadId :: term()) ->
    FileId :: term() | undefined.
upload_map_lookup(SessionId, UploadId) ->
    {ok, Map} = op_gui_session:get_value(SessionId, ?UPLOAD_MAP, #{}),
    maps:get(UploadId, Map, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a UploadId - FileId pair from upload map.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_delete(session:id(), UploadId :: term()) -> ok | {error, term()}.
upload_map_delete(SessionId, UploadId) ->
    MapUpdateFun = fun(Map) ->
        maps:remove(UploadId, Map)
    end,
    op_gui_session:update_value(SessionId, ?UPLOAD_MAP, MapUpdateFun, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Wait for new file id for upload trying to read it from session memory,
%% retrying in intervals.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_file_new_file_id(session:id(), UploadId :: binary()) ->
    {fslogic_worker:file_guid(), logical_file_manager:handle()}.
wait_for_file_new_file_id(SessionId, UploadId) ->
    wait_for_file_new_file_id(SessionId, UploadId, ?MAX_WAIT_FOR_FILE_HANDLE).
wait_for_file_new_file_id(_SessionId, _, Timeout) when Timeout < 0 ->
    throw(cannot_resolve_new_file_id);
wait_for_file_new_file_id(SessionId, UploadId, Timeout) ->
    case upload_map_lookup(SessionId, UploadId) of
        undefined ->
            timer:sleep(?INTERVAL_WAIT_FOR_FILE_HANDLE),
            wait_for_file_new_file_id(
                SessionId, UploadId, Timeout - ?INTERVAL_WAIT_FOR_FILE_HANDLE
            );
        FileId ->
            {ok, FileHandle} = logical_file_manager:open(
                SessionId, {guid, FileId}, write),
            {FileId, FileHandle}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Cleans all upload-related mappings in session memory.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec clean_upload_map(session:id()) -> ok | {error, term()}.
clean_upload_map(SessionId) ->
    op_gui_session:put_value(SessionId, ?UPLOAD_MAP, #{}).


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
                    {FileId, FileHandle} = get_new_file_id(SessionId, Params),
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
                        logical_file_manager:release(FileHandle), % release if possible
                        logical_file_manager:unlink(SessionId, {guid, FileId}, false),
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
    FileHandle :: logical_file_manager:handle(), Offset :: non_neg_integer(),
    Opts :: cowboy_req:read_body_opts()) -> cowboy_req:req().
stream_file(Req, FileHandle, Offset, Opts) ->
    case cowboy_req:read_part_body(Req, Opts) of
        {ok, Body, Req2} ->
            % @todo VFS-1815 handle errors, such as {error, enospc}
            {ok, _, _} = logical_file_manager:write(FileHandle, Offset, Body),
            ok = logical_file_manager:release(FileHandle),
            % @todo VFS-1815 register_chunk?
            % or send a message from client that uuid has finished?
            Req2;
        {more, Body, Req2} ->
            {ok, NewHandle, Written} =
                logical_file_manager:write(FileHandle, Offset, Body),
            stream_file(Req2, NewHandle, Offset + Written, Opts)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieve a new file ID for upload. First chunk causes creation of new file
%% and puts it in session memory, other chunks wait for the creation and
%% resolve the ID from session memory.
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_id(session:id(), Params :: proplists:proplist()) ->
    {fslogic_worker:file_guid(), logical_file_manager:handle()}.
get_new_file_id(SessionId, Params) ->
    UploadId = get_bin_param(<<"resumableIdentifier">>, Params),
    ChunkNumber = get_int_param(<<"resumableChunkNumber">>, Params),
    % Create an upload handle for first chunk. Further chunks reuse the handle
    % or have to wait until it is created.
    case ChunkNumber of
        1 ->
            ParentId = get_bin_param(<<"parentId">>, Params),
            FileName = get_bin_param(<<"resumableFilename">>, Params),
            {ok, ParentPath} = logical_file_manager:get_file_path(
                SessionId, ParentId),
            ProposedPath = filename:join([ParentPath, FileName]),
            {FileId, FileHandle} = create_unique_file(SessionId, ProposedPath),
            upload_map_insert(SessionId, UploadId, FileId),
            {FileId, FileHandle};
        _ ->
            wait_for_file_new_file_id(SessionId, UploadId, ?MAX_WAIT_FOR_FILE_HANDLE)
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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a unique file. Starts with given path, but if such file exists,
%% tries to add a numerical suffix like file(4).txt. Tries up to
%% ?MAX_UNIQUE_FILENAME_COUNTER amount of times and then throws on failure.
%% @end
%%--------------------------------------------------------------------
-spec create_unique_file(SessionId :: session:id(),
    OriginalPath :: file_meta:path()) ->
    {fslogic_worker:file_guid(), logical_file_manager:handle()}.
create_unique_file(SessionId, OriginalPath) ->
    create_unique_file(SessionId, OriginalPath, 0).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a unique file. Starts with given path, but if such file exists,
%% tries to add a numerical suffix like file(4).txt. Tries up to
%% ?MAX_UNIQUE_FILENAME_COUNTER amount of times and then throws on failure.
%% @end
%%--------------------------------------------------------------------
-spec create_unique_file(SessionId :: session:id(),
    OriginalPath :: file_meta:path(), Tries :: integer()) ->
    {fslogic_worker:file_guid(), logical_file_manager:handle()}.
create_unique_file(_, _, ?MAX_UNIQUE_FILENAME_COUNTER) ->
    throw(filename_occupied);

create_unique_file(SessionId, OriginalPath, Counter) ->
    ProposedPath = case Counter of
        0 ->
            OriginalPath;
        _ ->
            RootNm = filename:rootname(OriginalPath),
            Ext = filename:extension(OriginalPath),
            str_utils:format_bin("~s(~B)~s", [RootNm, Counter, Ext])
    end,
    % @todo use exists when it is implemented
    case logical_file_manager:stat(SessionId, {path, ProposedPath}) of
        {error, _} ->
            {ok, {FileId, FileHandle}} = logical_file_manager:create_and_open(
                SessionId, ProposedPath, undefined, write),
            {FileId, FileHandle};
        {ok, _} ->
            create_unique_file(SessionId, OriginalPath, Counter + 1)
    end.


