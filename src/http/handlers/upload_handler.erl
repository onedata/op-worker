%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module processes file upload requests originating from GUI.
%%% Exposes an API required by lib ResumableJS that is used on client side.
%%% @end
%%%-------------------------------------------------------------------
-module(upload_handler).
-author("Lukasz Opiola").
-behaviour(cowboy_http_handler).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Key of in-memory mapping of uploads kept in session.
-define(UPLOAD_MAP, upload_map).

% How many tries of creating a unique filename before failure.
-define(MAX_UNIQUE_FILENAME_COUNTER, 20).

% Maximum time a process will be waiting for chunk 1 to generate file handle.
-define(MAX_WAIT_FOR_FILE_HANDLE, 30000).

% Interval between retries to resolve file handle.
-define(INTERVAL_WAIT_FOR_FILE_HANDLE, 300).

%% Cowboy API
-export([init/3, handle/2, terminate/3]).
%% API
-export([upload_map_insert/2, upload_map_delete/1]).
-export([wait_for_file_new_file_id/1]).
-export([clean_upload_map/0]).

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
-spec upload_map_insert(UploadId :: term(), FileId :: term()) -> ok.
upload_map_insert(_UploadId, undefined) ->
    error(badarg);

upload_map_insert(undefined, _FileId) ->
    error(badarg);

upload_map_insert(UploadId, FileId) ->
    MapUpdateFun = fun(Map) ->
        maps:put(UploadId, FileId, Map)
    end,
    gui_session:update_value(?UPLOAD_MAP, MapUpdateFun, #{}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves a FileId from upload map by UploadId or returns a default value.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_lookup(UploadId :: term()) -> FileId :: term() | undefined.
upload_map_lookup(UploadId) ->
    Map = gui_session:get_value(?UPLOAD_MAP, #{}),
    maps:get(UploadId, Map, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a UploadId - FileId pair from upload map.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_delete(UploadId :: term()) -> ok.
upload_map_delete(UploadId) ->
    MapUpdateFun = fun(Map) ->
        maps:remove(UploadId, Map)
    end,
    gui_session:update_value(?UPLOAD_MAP, MapUpdateFun, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Wait for new file id for upload trying to read it from session memory,
%% retrying in intervals.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_file_new_file_id(UploadId :: binary()) ->
    fslogic_worker:file_guid().
wait_for_file_new_file_id(UploadId) ->
    wait_for_file_new_file_id(UploadId, ?MAX_WAIT_FOR_FILE_HANDLE).
wait_for_file_new_file_id(_, Timeout) when Timeout < 0 ->
    throw(cannot_resolve_new_file_id);
wait_for_file_new_file_id(UploadId, Timeout) ->
    case upload_map_lookup(UploadId) of
        undefined ->
            timer:sleep(?INTERVAL_WAIT_FOR_FILE_HANDLE),
            wait_for_file_new_file_id(
                UploadId, Timeout - ?INTERVAL_WAIT_FOR_FILE_HANDLE);
        FileId ->
            FileId
    end.


%%--------------------------------------------------------------------
%% @doc
%% Cleans all upload-related mappings in session memory.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec clean_upload_map() -> ok.
clean_upload_map() ->
    gui_session:put_value(?UPLOAD_MAP, #{}).


%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback.
%% @end
%%--------------------------------------------------------------------
-spec init({TransportName :: atom(), ProtocolName :: http},
    Req :: cowboy_req:req(), Opts :: any()) ->
    {ok, cowboy_req:req(), []}.
init(_Type, Req, _Opts) ->
    {ok, Req, []}.


%%--------------------------------------------------------------------
%% @doc
%% Handles an upload request.
%% @end
%%--------------------------------------------------------------------
-spec handle(term(), term()) -> {ok, cowboy_req:req(), term()}.
handle(Req, State) ->
    NewReq = handle_http_upload(Req),
    {ok, NewReq, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no cleanup needed
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.


%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asserts the validity of multipart POST request and proceeds with
%% parsing or returns an error. Returns list of parsed filed values and
%% file body.
%% @end
%%--------------------------------------------------------------------
-spec handle_http_upload(Req :: cowboy_req:req()) -> cowboy_req:req().
handle_http_upload(Req) ->
    % Try to retrieve user's session
    InitSession =
        try
            gui_ctx:init(Req, false)
        catch _:_ ->
            % Error logging is done inside gui_ctx:init
            error
        end,
    case InitSession of
        error ->
            gui_ctx:reply(500, #{<<"connection">> => <<"close">>}, <<"">>);
        ok ->
            try
                NewReq = multipart(Req, []),
                gui_ctx:set_cowboy_req(NewReq),
                gui_ctx:reply(200, #{}, <<"">>)
            catch
                throw:{missing_param, _} ->
                    gui_ctx:reply(500, #{<<"connection">> => <<"close">>}, <<"">>);
                throw:stream_file_error ->
                    gui_ctx:reply(500, #{<<"connection">> => <<"close">>}, <<"">>);
                Type:Message ->
                    ?error_stacktrace("Error while processing file upload "
                    "from user ~p - ~p:~p",
                        [gui_session:get_user_id(), Type, Message]),
                    % @todo VFS-1815 for now return 500,
                    % because retries are not stable
%%                    % Return 204 - resumable will retry the upload
%%                    gui_ctx:reply(204, [], <<"">>)
                    gui_ctx:reply(500, #{<<"connection">> => <<"close">>}, <<"">>)
            end
    end,
    gui_ctx:finish().


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses a multipart HTTP form.
%% @end
%%--------------------------------------------------------------------
-spec multipart(Req :: cowboy_req:req(), Params :: proplists:proplist()) ->
    cowboy_req:req().
multipart(Req, Params) ->
    {ok, UploadWriteSize} = application:get_env(?APP_NAME, upload_write_size),
    {ok, UploadReadTimeout} = application:get_env(?APP_NAME, upload_read_timeout),
    {ok, UploadReadSize} = application:get_env(?APP_NAME, upload_read_size),

    case cowboy_req:part(Req) of
        {ok, Headers, Req2} ->
            case cow_multipart:form_data(Headers) of
                {data, FieldName} ->
                    {ok, FieldValue, Req3} = cowboy_req:part_body(Req2),
                    multipart(Req3, [{FieldName, FieldValue} | Params]);
                {file, _FieldName, _Filename, _CType, _CTransferEncoding} ->
                    FileId = get_new_file_id(Params),
                    SessionId = gui_session:get_session_id(),
                    {ok, FileHandle} = logical_file_manager:open(
                        SessionId, {guid, FileId}, write),
                    ChunkNumber = get_int_param(
                        <<"resumableChunkNumber">>, Params),
                    ChunkSize = get_int_param(
                        <<"resumableChunkSize">>, Params),
                    % First chunk number in resumable is 1
                    Offset = ChunkSize * (ChunkNumber - 1),
                    % Set options for reading from socket
                    Opts = [
                        % length is chunk size - how much the cowboy read
                        % function returns at once.
                        {length, UploadWriteSize},
                        % read_length means how much will be read from socket
                        % at once - cowboy will repeat reading until it
                        % accumulates a chunk, then it returns it.
                        {read_length, UploadReadSize},
                        % read timeout - the read will fail if read_length
                        % is not satisfied within this time.
                        {read_timeout, UploadReadTimeout}
                    ],
                    Req3 = try
                        stream_file(Req2, FileHandle, Offset, Opts)
                    catch Type:Message ->
                        ?error_stacktrace("Error while streaming file upload "
                        "from user ~p - ~p:~p",
                            [gui_session:get_user_id(), Type, Message]),
                        logical_file_manager:release(FileHandle), % release if possible
                        logical_file_manager:unlink(SessionId, {guid, FileId}, false),
                        throw(stream_file_error)
                    end,
                    multipart(Req3, Params)
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
    Opts :: proplists:proplist()) -> cowboy_req:req().
stream_file(Req, FileHandle, Offset, Opts) ->
    case cowboy_req:part_body(Req, Opts) of
        {ok, Body, Req2} ->
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
-spec get_new_file_id(Params :: proplists:proplist()) -> file_meta:uuid().
get_new_file_id(Params) ->
    UploadId = get_bin_param(<<"resumableIdentifier">>, Params),
    ChunkNumber = get_int_param(<<"resumableChunkNumber">>, Params),
    % Create an upload handle for first chunk. Further chunks reuse the handle
    % or have to wait until it is created.
    case ChunkNumber of
        1 ->
            SessionId = gui_session:get_session_id(),
            ParentId = get_bin_param(<<"parentId">>, Params),
            FileName = get_bin_param(<<"resumableFilename">>, Params),
            {ok, ParentPath} = logical_file_manager:get_file_path(
                SessionId, ParentId),
            ProposedPath = filename:join([ParentPath, FileName]),
            % TODO - use parentID
            FileId = create_unique_file(SessionId, ProposedPath),
            upload_map_insert(UploadId, FileId),
            FileId;
        _ ->
            wait_for_file_new_file_id(UploadId, ?MAX_WAIT_FOR_FILE_HANDLE)
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
    OriginalPath :: file_meta:path()) -> file_meta:uuid().
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
    OriginalPath :: file_meta:path(), Tries :: integer()) -> file_meta:uuid().
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
            {ok, FileId} = logical_file_manager:create(SessionId, ProposedPath),
            FileId;
        {ok, _} ->
            create_unique_file(SessionId, OriginalPath, Counter + 1)
    end.


