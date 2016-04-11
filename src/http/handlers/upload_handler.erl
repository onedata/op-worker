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

-include_lib("ctool/include/logging.hrl").

% Key of in-memory mapping of uploads kept in session.
-define(UPLOAD_MAP, upload_map).

% How many tries of creating a unique filename before failure.
-define(MAX_UNIQUE_FILENAME_COUNTER, 20).

% Maximum time a process will be waiting for chunk 1 to generate file handle.
-define(MAX_WAIT_FOR_FILE_HANDLE, 30000).

% Interval between retries to resolve file handle.
-define(INTERVAL_WAIT_FOR_FILE_HANDLE, 100).

%% Cowboy API
-export([init/3, handle/2, terminate/3]).
%% API
-export([upload_map_insert/2, upload_map_delete/1]).
-export([upload_map_lookup/1, upload_map_lookup/2]).
-export([clean_upload_map/0]).

%% ====================================================================
%% API functions
%% ====================================================================


%%--------------------------------------------------------------------
%% @doc
%% Inserts a Key - Value pair into upload map.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_insert(Key :: term(), Value :: term()) -> ok.
upload_map_insert(Key, Value) ->
    Map = g_session:get_value(?UPLOAD_MAP, #{}),
    NewMap = maps:put(Key, Value, Map),
    g_session:put_value(?UPLOAD_MAP, NewMap).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a Value from upload map by Key.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_lookup(Key :: term()) -> Value :: term().
upload_map_lookup(Key) ->
    upload_map_lookup(Key, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a Value from upload map by Key or returns a default value.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_lookup(Key :: term(), Default :: term()) -> Value :: term().
upload_map_lookup(Key, Default) ->
    Map = g_session:get_value(?UPLOAD_MAP, #{}),
    maps:get(Key, Map, Default).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a Key Value pair from upload map.
%% Upload map is a map in session memory dedicated for handling chunked
%% file uploads.
%% @todo Should be redesigned in VFS-1815.
%% @end
%%--------------------------------------------------------------------
-spec upload_map_delete(Key :: term()) -> ok.
upload_map_delete(Key) ->
    Map = g_session:get_value(?UPLOAD_MAP, #{}),
    NewMap = maps:remove(Key, Map),
    g_session:put_value(?UPLOAD_MAP, NewMap).


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
    g_session:put_value(?UPLOAD_MAP, #{}).


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
            g_ctx:init(Req, false)
        catch _:_ ->
            % Error logging is done inside g_ctx:init
            error
        end,
    case InitSession of
        error ->
            g_ctx:reply(500, [], <<"">>);
        ok ->
            try
                NewReq = multipart(Req, []),
                g_ctx:set_cowboy_req(NewReq),
                g_ctx:reply(200, [], <<"">>)
            catch
                throw:{missing_param, _} ->
                    g_ctx:reply(500, [], <<"">>);
                Type:Message ->
                    ?error_stacktrace("Error while processing file upload "
                    "from user ~p - ~p:~p",
                        [g_session:get_user_id(), Type, Message]),
                    % @todo VFS-1815 for now return 500,
                    % because retries are not stable
%%                    % Return 204 - resumable will retry the upload
%%                    g_ctx:reply(204, [], <<"">>)
                    g_ctx:reply(500, [], <<"">>)
            end
    end,
    g_ctx:finish().


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses a multipart HTTP form.
%% @end
%%--------------------------------------------------------------------
-spec multipart(Req :: cowboy_req:req(), Params :: proplists:proplist()) ->
    cowboy_req:req().
multipart(Req, Params) ->
    case cowboy_req:part(Req) of
        {ok, Headers, Req2} ->
            case cow_multipart:form_data(Headers) of
                {data, FieldName} ->
                    {ok, FieldValue, Req3} = cowboy_req:part_body(Req2),
                    multipart(Req3, [{FieldName, FieldValue} | Params]);
                {file, _FieldName, _Filename, _CType, _CTransferEncoding} ->
                    FileId = get_new_file_id(Params),
                    SessionId = g_session:get_session_id(),
                    {ok, FileHandle} = logical_file_manager:open(
                        SessionId, {uuid, FileId}, write),
                    ChunkNumber = get_int_param(
                        <<"resumableChunkNumber">>, Params),
                    ChunkSize = get_int_param(
                        <<"resumableChunkSize">>, Params),
                    % First chunk number in resumable is 1
                    Offset = ChunkSize * (ChunkNumber - 1),
                    Req3 = stream_file(Req2, FileHandle, Offset),
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
    FileHandle :: logical_file_manager:handle(), Offset :: non_neg_integer()) ->
    cowboy_req:req().
stream_file(Req, FileHandle, Offset) ->
    case cowboy_req:part_body(Req) of
        {ok, Body, Req2} ->
            {ok, _, _} = logical_file_manager:write(FileHandle, Offset, Body),
            % @todo VFS-1815 register_chunk?
            % or send a message from client that uuid has finished?
            Req2;
        {more, Body, Req2} ->
            {ok, NewHandle, Written} =
                logical_file_manager:write(FileHandle, Offset, Body),
            stream_file(Req2, NewHandle, Offset + Written)
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
    Identifier = get_bin_param(<<"resumableIdentifier">>, Params),
    ChunkNumber = get_int_param(<<"resumableChunkNumber">>, Params),
    % Create an upload handle for first chunk. Further chunks reuse the handle
    % or have to wait until it is created.
    case ChunkNumber of
        1 ->
            SessionId = g_session:get_session_id(),
            ParentId = get_bin_param(<<"parentId">>, Params),
            FileName = get_bin_param(<<"resumableFilename">>, Params),
            {ok, ParentPath} = logical_file_manager:get_file_path(
                SessionId, ParentId),
            ProposedPath = filename:join([ParentPath, FileName]),
            FileId = create_unique_file(SessionId, ProposedPath),
            upload_map_insert(Identifier, FileId),
            FileId;
        _ ->
            wait_for_file_new_file_id(Identifier, ?MAX_WAIT_FOR_FILE_HANDLE)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Wait for new file id for upload trying to read it from session memory,
%% retrying in intervals.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_file_new_file_id(Identifier :: binary(), Timeout :: integer()) ->
    file_meta:uuid().
wait_for_file_new_file_id(_, Timeout) when Timeout < 0 ->
    throw(cannot_resolve_new_file_id);

wait_for_file_new_file_id(Identifier, Timeout) ->
    case upload_map_lookup(Identifier, undefined) of
        undefined ->
            timer:sleep(?INTERVAL_WAIT_FOR_FILE_HANDLE),
            wait_for_file_new_file_id(
                Identifier, Timeout - ?INTERVAL_WAIT_FOR_FILE_HANDLE);
        FileId ->
            FileId
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


%%--------------------------------------------------------------------u
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


