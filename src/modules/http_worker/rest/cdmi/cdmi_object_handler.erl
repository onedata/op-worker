%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%%% It handles cdmi object PUT, GET and DELETE requests
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_object_handler).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/http_worker/http_common.hrl").
-include("modules/http_worker/rest/cdmi/cdmi.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include("modules/http_worker/rest/http_status.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2,
    resource_exists/2, is_authorized/2, content_types_provided/2,
    content_types_accepted/2, delete_resource/2]).

%% Content type routing functions
-export([get_cdmi/2, put_cdmi/2, get_binary/2, put_binary/2]).

-define(DEFAULT_FILE_PERMISSIONS, 8#664).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), #{}} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:malformed_request/2
%%--------------------------------------------------------------------
-spec malformed_request(req(), #{}) -> {boolean(), req(), #{}}.
malformed_request(Req, State) ->
    cdmi_arg_parser:malformed_request(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {boolean(), req(), #{}}.
is_authorized(Req, State) ->
    rest_auth:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(req(), #{}) -> {boolean(), req(), #{}}.
resource_exists(Req, State) ->
    cdmi_existence_checker:object_resource_exists(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
content_types_provided(Req, #{cdmi_version := undefined} = State) ->
    {[
        {<<"application/binary">>, get_binary}
    ], Req, State};
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-object">>, get_cdmi_object}
    ], Req, State}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
content_types_accepted(Req, #{cdmi_version := undefined} = State) ->
    {[
        {'*', put_binary}
    ], Req, State};
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-object">>, put_cdmi}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource(req(), #{}) -> {term(), req(), #{}}.
delete_resource(Req, #{path := Path, identity := Identity} = State) ->
    ok = onedata_file_api:unlink(Identity, {path, Path}),
    {true, Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Cowboy callback function
%% Handles GET requests for file, returning file content as response body.
%% @end
%%--------------------------------------------------------------------
-spec get_binary(req(), #{}) -> {term(), req(), #{}}.
get_binary(Req, #{path := Path, attributes := #file_attr{size = Size}} = State) ->

    % get optional 'Range' header
    {Ranges, Req1} = get_ranges(Req, State),

    % prepare response
    StreamSize = get_stream_size(Ranges, Size),
    StreamFun = stream(State, Ranges),
    Req2 = cowboy_req:set_resp_header(<<"content-type">>, get_mimetype(Path), Req1),

    HttpStatus =
        case Ranges of
            undefined -> ?HTTP_OK;
            _ -> ?PARTIAL_CONTENT
        end,

    % reply
    {ok, Req3} = apply(cowboy_req, reply, [HttpStatus, [], {StreamSize, StreamFun}, Req2]),
    {halt, Req3, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi(req(), #{}) -> {term(), req(), #{}}.
get_cdmi(Req, State) ->
    {<<"ok">>, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT without cdmi content-type
%% @end
%%--------------------------------------------------------------------
-spec put_binary(req(), #{}) -> {term(), req(), #{}}.
put_binary(ReqArg, State = #{identity := Identity, path := Path}) ->
    % prepare request data
    {Content, Req} = cowboy_req:header(<<"content-type">>, ReqArg, ?MIMETYPE_DEFAULT_VALUE),
    %%  TODO partial upload
    %%  {CdmiPartialFlag, Req1} = cowboy_req:header(<<"x-cdmi-partial">>, Req0),
    {Mimetype, Encoding} = parse_content(Content),
    case onedata_file_api:create(Identity, Path, ?DEFAULT_FILE_PERMISSIONS) of
        ok ->
            update_completion_status(Path, <<"Processing">>),
            update_mimetype(Path, Mimetype),
            update_encoding(Path, Encoding),
            Ans = write_body_to_file(Req, State, 0),
            %% set_completion_status_according_to_partial_flag(Path, CdmiPartialFlag),
            Ans;

        {error, ?ENOENT} ->
            case onedata_file_api:check_perms(Path, write) of
                true -> ok;
                false -> throw(?forbidden)
            end,
            update_completion_status(Path, <<"Processing">>),
            update_mimetype(Path, Mimetype),
            update_encoding(Path, Encoding),
            {RawRange, Req1} = cowboy_req:header(<<"content-range">>, Req),
            case RawRange of
                undefined ->
                    case onedata_file_api:truncate(Identity, {path, Path}, 0) of
                        ok -> ok;
                        {error, Err} when Err =:= ?EPERM orelse Err =:= ?EACCES ->
                        %% set_completion_status_according_to_partial_flag(Path, CdmiPartialFlag),
                            throw(?forbidden);
                        Error ->
                        %% set_completion_status_according_to_partial_flag(Path, CdmiPartialFlag),
                            throw({?put_object_unknown_error, Error})
                    end,
                    Ans = write_body_to_file(Req1, State, 0, false),
                    %% set_completion_status_according_to_partial_flag(Path, CdmiPartialFlag),
                    Ans;
                _ ->
                    {Length, Req2} = cowboy_req:body_length(Req1),
                    case parse_byte_range(State, RawRange) of
                        [{From, To}] when Length =:= undefined orelse Length =:= To - From + 1 ->
                            Ans = write_body_to_file(Req2, State, From, false),
                            %% set_completion_status_according_to_partial_flag(Path, CdmiPartialFlag),
                            Ans;
                        _ ->
                            %% set_completion_status_according_to_partial_flag(Path, CdmiPartialFlag),
                            throw(?invalid_range)
                    end
            end;
        {error, Err} when Err =:= ?EPERM orelse Err =:= ?EACCES ->
            throw(?forbidden);
        _ ->
            throw(?put_object_unknown_error)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec put_cdmi(req(), #{}) -> {term(), req(), #{}}.
put_cdmi(Req, State) ->
    {true, Req, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc @equiv write_body_to_file(Req, State, Offset, true)
%%--------------------------------------------------------------------
-spec write_body_to_file(req(), #{}, integer()) -> {boolean(), req(), #{}}.
write_body_to_file(Req, State, Offset) ->
    write_body_to_file(Req, State, Offset, true).

%%--------------------------------------------------------------------
%% @doc
%% Reads request's body and writes it to file obtained from state.
%% This callback return value is compatibile with put requests.
%% @end
%%--------------------------------------------------------------------
-spec write_body_to_file(req(), #{}, integer(), boolean()) -> {boolean(), req(), #{}}.
write_body_to_file(Req, #{path := Path, identity := Identity} = State, Offset, RemoveIfFails) ->
    {Status, Chunk, Req1} = cowboy_req:body(Req),
    {ok, FileHandle} = onedata_file_api:open(Identity, {path, Path}, write),
    case onedata_file_api:write(FileHandle, Offset, Chunk) of
        {ok, _NewHandle, Bytes} when is_integer(Bytes) ->
            case Status of
                more -> write_body_to_file(Req1, State, Offset + Bytes);
                ok -> {true, Req1, State}
            end;
        _ -> %todo handle write file forbidden
            case RemoveIfFails of
                true -> onedata_file_api:unlink(FileHandle);
                false -> ok
            end,
            throw(?write_object_unknown_error)
    end.

%%--------------------------------------------------------------------
%% @doc Parses byte ranges from 'Range' http header format to list of
%% erlang range tuples, i. e. <<"1-5,-3">> for a file with length 10
%% will produce -> [{1,5},{7,9}]
%% @end
%%--------------------------------------------------------------------
-spec parse_byte_range(#{}, binary() | list()) -> list(Range) | invalid when
    Range :: {From :: integer(), To :: integer()} | invalid.
parse_byte_range(State, Range) when is_binary(Range) ->
    Ranges = parse_byte_range(State, binary:split(Range, <<",">>, [global])),
    case lists:member(invalid, Ranges) of
        true -> invalid;
        false -> Ranges
    end;
parse_byte_range(_, []) ->
    [];
parse_byte_range(#{attributes := #file_attr{size = Size}} = State, [First | Rest]) ->
    Range = case binary:split(First, <<"-">>, [global]) of
                [<<>>, FromEnd] -> {max(0, Size - binary_to_integer(FromEnd)), Size - 1};
                [From, <<>>] -> {binary_to_integer(From), Size - 1};
                [From_, To] -> {binary_to_integer(From_), binary_to_integer(To)};
                _ -> invalid
            end,
    case Range of
        invalid -> [invalid];
        {Begin, End} when Begin > End -> [invalid];
        ValidRange -> [ValidRange | parse_byte_range(State, Rest)]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads given range of bytes (defaults to whole file) from file (obtained
%% from state path), result is encoded according to 'Encoding' argument
%% and streamed to given Socket
%% @end
%%--------------------------------------------------------------------
-spec stream_range(Socket :: term(), Transport :: atom(), State :: #{}, Range, Encoding :: binary(),
    BufferSize :: integer(), FileHandle :: onedata_file_api:file_handle()) -> Result when
    Range :: default | {From :: integer(), To :: integer()},
    Result :: ok | no_return().
stream_range(Socket, Transport, State, Range, Encoding, BufferSize, FileHandle) when (BufferSize rem 3) =/= 0 ->
    %buffer size is extended, so it's divisible by 3 to allow base64 on the fly conversion
    stream_range(Socket, Transport, State, Range, Encoding, BufferSize - (BufferSize rem 3), FileHandle);
stream_range(Socket, Transport, #{attributes := #file_attr{size = Size}} = State, default, Encoding, BufferSize, FileHandle) ->
    %default range should remain consistent with parse_object_ans/2 valuerange clause
    stream_range(Socket, Transport, State, {0, Size - 1}, Encoding, BufferSize, FileHandle);
stream_range(Socket, Transport, State, {From, To}, Encoding, BufferSize, FileHandle) ->
    ToRead = To - From + 1,
    case ToRead > BufferSize of
        true ->
            {ok, NewFileHandle,  Data} = onedata_file_api:read(FileHandle, From, BufferSize),
            Transport:send(Socket, encode(Data, Encoding)),
            stream_range(Socket, Transport, State, {From + BufferSize, To}, Encoding, BufferSize, NewFileHandle);
        false ->
            {ok, NewFileHandle ,Data} = onedata_file_api:read(FileHandle, From, ToRead),
            Transport:send(Socket, encode(Data, Encoding))
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns fun that reads given ranges of file and streams to given Socket
%% @end
%%--------------------------------------------------------------------
-spec stream(Stata ::#{}, Ranges :: [{non_neg_integer(), non_neg_integer()}] | undefined) -> any().
stream(#{attributes := #file_attr{size = Size}} = State, undefined) ->
    stream(State, [{0, Size -1}]);
stream(#{path := Path, identity := Identity} = State, Ranges) ->
    {ok, FileHandle} = onedata_file_api:open(Identity, {path, Path} ,read),
    fun(Socket, Transport) ->
        try
            {ok, BufferSize} = application:get_env(?APP_NAME, download_buffer_size),
            lists:foreach(fun(Rng) ->
            stream_range(Socket, Transport, State, Rng, <<"utf-8">>, BufferSize, FileHandle) end, Ranges)
        catch Type:Message ->
            % Any exceptions that occur during file streaming must be caught here for cowboy to close the connection cleanly
            ?error_stacktrace("Error while streaming file '~p' - ~p:~p", [Path, Type, Message])
        end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets mimetype associated with file, returns default value if no mimetype
%% could be found
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(string()) -> binary().
get_mimetype(Path) ->
    case onedata_file_api:get_xattr(Path, ?MIMETYPE_XATTR_KEY) of
        %%TODO lfm_attrs:get_xattr is not yet implemented and always returns {ok, <<"">>}
        {ok, <<"">>} -> ?MIMETYPE_DEFAULT_VALUE;
        {ok, Value} -> Value
        %%TODO uncomment when lfm_attrs:get_xattr is implemented
        %%{error, ?ENOATTR} -> ?MIMETYPE_DEFAULT_VALUE
    end.

%%--------------------------------------------------------------------
%% @doc Encodes data according to given ecoding
%%--------------------------------------------------------------------
-spec encode(Data :: binary(), Encoding :: binary()) -> binary().
encode(Data, Encoding) when Encoding =:= ?ENCODING_DEFAULT_VALUE ->
    base64:encode(Data);
encode(Data, _) ->
    Data.

%%--------------------------------------------------------------------
%% @doc Encodes data according to given ecoding
%%--------------------------------------------------------------------
-spec get_ranges(Req :: req(), #{}) ->
    {[{non_neg_integer(), non_neg_integer()}] | undefined, req()}.
get_ranges(Req, State) ->
    {RawRange, Req1} = cowboy_req:header(<<"range">>, Req),
    case RawRange of
        undefined -> {undefined, Req1};
        _ ->
            case parse_byte_range(State, RawRange) of
                invalid ->throw(?invalid_range);
                Ranges -> {Ranges, Req1}
            end
    end.

%%--------------------------------------------------------------------
%% @doc Gets size of a stream
%%--------------------------------------------------------------------
-spec get_stream_size(Ranges :: [{non_neg_integer(), non_neg_integer()}] | undefined,
    Size :: non_neg_integer()) -> non_neg_integer().
get_stream_size(undefined, Size) -> Size;
get_stream_size(Ranges, _Size) ->
    lists:foldl(fun
        ({From, To}, Acc) when To >= From -> max(0, Acc + To - From + 1);
        ({_, _}, Acc)  -> Acc
    end, 0, Ranges).

%%--------------------------------------------------------------------
%% @doc
%% Parses content-type header to mimetype and charset part, if charset
%% is other than utf-8, function returns undefined
%% @end
%%--------------------------------------------------------------------
-spec parse_content(binary()) -> {Mimetype :: binary(), Encoding :: binary() | undefined}.
parse_content(Content) ->
    case binary:split(Content,<<";">>) of
        [RawMimetype, RawEncoding] ->
            case binary:split(utils:trim_spaces(RawEncoding),<<"=">>) of
                [<<"charset">>, <<"utf-8">>] ->
                    {utils:trim_spaces(RawMimetype), <<"utf-8">>};
                _ ->
                    {utils:trim_spaces(RawMimetype), undefined}
            end;
        [RawMimetype] ->
            {utils:trim_spaces(RawMimetype), undefined}
    end.


%%--------------------------------------------------------------------
%% @doc Updates mimetype associated with file
%%--------------------------------------------------------------------
-spec update_mimetype(string(), binary()) -> ok | no_return().
update_mimetype(_Filepath, undefined) -> ok;
update_mimetype(Filepath, Mimetype) ->
    ok = onedata_file_api:set_xattr(Filepath, ?MIMETYPE_XATTR_KEY, Mimetype).

%%--------------------------------------------------------------------
%% @doc Updates valuetransferencoding associated with file
%%--------------------------------------------------------------------
-spec update_encoding(string(), binary()) -> ok | no_return().
update_encoding(_Filepath, undefined) -> ok;
update_encoding(Filepath, Encoding) ->
    ok = onedata_file_api:set_xattr(Filepath, ?ENCODING_XATTR_KEY, Encoding).

%%--------------------------------------------------------------------
%% @doc Updates completion status associated with file
%%--------------------------------------------------------------------
-spec update_completion_status(string(), binary()) -> ok | no_return().
update_completion_status(_Filepath, undefined) -> ok;
update_completion_status(Filepath, CompletionStatus)
    when CompletionStatus =:= <<"Complete">> orelse CompletionStatus =:= <<"Processing">> orelse CompletionStatus =:= <<"Error">> ->
    ok = onedata_file_api:set_xattr(Filepath, ?COMPLETION_STATUS_XATTR_KEY, CompletionStatus).
