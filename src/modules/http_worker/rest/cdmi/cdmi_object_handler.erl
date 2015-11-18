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
-include("modules/http_worker/rest/http_status.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2,
    resource_exists/2, is_authorized/2, content_types_provided/2,
    content_types_accepted/2, delete_resource/2]).

%% Content type routing functions
-export([get/2, put/2, get_binary/2]).

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
content_types_provided(Req, State) ->
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
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-object">>, put}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:delete_resource/2
%%--------------------------------------------------------------------
-spec delete_resource(req(), #{}) -> {term(), req(), #{}}.
delete_resource(Req, #{path := Path, identity := Identity} = State) ->
    case onedata_file_api:unlink(Identity, {path, utils:ensure_unicode_binary(Path)}) of
        ok -> {true, Req, State};
        {error, Code} -> cdmi_error:unlink_failed_reply(Req, State, Code)
    end.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Cowboy callback function
%% Handles GET requests for file, returning file content as response body.
%% @end
%%--------------------------------------------------------------------
-spec get_binary(req(), #{}) -> {term(), req(), #{}}.
get_binary(Req, #{path := Path} = State) ->

    Size = get_size(State),
    % get optional 'Range' header
    {RawRange, Req1} = cowboy_req:header(<<"range">>, Req),
    Ranges = case RawRange of
                 undefined -> [{0,Size-1}];
                 _ -> parse_byte_range(State, RawRange)
             end,

    % return bad request if Range is invalid
    case Ranges of
        invalid -> cdmi_error:invalid_range_reply(Req1, State);
        _ ->
            % prepare data size and stream function
            StreamSize = lists:foldl(fun
                ({From, To}, Acc) when To >= From -> max(0, Acc + To - From + 1);
                ({_, _}, Acc)  -> Acc
            end, 0, Ranges),
            StreamFun = fun(Socket, Transport) ->
                try
                    {ok, BufferSize} = application:get_env(?APP_NAME, download_buffer_size),
                    lists:foreach(fun(Rng) ->
                        stream_file(Socket, Transport, State, Rng, <<"utf-8">>, BufferSize) end, Ranges)
                catch Type:Message ->
                    % Any exceptions that occur during file streaming must be caught here for cowboy to close the connection cleanly
                    ?error_stacktrace("Error while streaming file '~p' - ~p:~p", [Path, Type, Message])
                end
            end,

            % set mimetype
            Req2 = cowboy_req:set_resp_header(<<"content-type">>, get_mimetype(Path), Req1),

            % reply with stream and adequate status
            {ok, Req3} = case RawRange of
                             undefined ->
                                 cowboy_req:reply(?HTTP_OK, [], {StreamSize, StreamFun}, Req2);
                             _ ->
                                 cowboy_req:reply(?PARTIAL_CONTENT, [], {StreamSize, StreamFun}, Req2)
                         end,
            {halt, Req3, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles GET with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec get(req(), #{}) -> {term(), req(), #{}}.
get(Req, State) ->
    {<<"ok">>, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles PUT with "application/cdmi-object" content-type
%% @end
%%--------------------------------------------------------------------
-spec put(req(), #{}) -> {term(), req(), #{}}.
put(Req, State) ->
    {true, Req, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================


%%--------------------------------------------------------------------
%% @doc Parses byte ranges from 'Range' http header format to list of
%% erlang range tuples, i. e. <<"1-5,-3">> for a file with length 10
%% will produce -> [{1,5},{7,9}]
%% @end
%%--------------------------------------------------------------------
-spec parse_byte_range(#{}, binary() | list()) -> list(Range) | invalid when
    Range :: {From :: integer(), To :: integer()}.
parse_byte_range(State, Range) when is_binary(Range) ->
    Ranges = parse_byte_range(State, binary:split(Range, <<",">>, [global])),
    case lists:member(invalid, Ranges) of
        true -> invalid;
        false -> Ranges
    end;
parse_byte_range(_, []) ->
    [];
parse_byte_range(State, [First | Rest]) ->
    Size = get_size(State),
    Range = case binary:split(First, <<"-">>, [global]) of
                [<<>>, FromEnd] -> {max(0, Size - binary_to_integer(FromEnd)), Size - 1};
                [From, <<>>] -> {binary_to_integer(From), Size - 1};
                [From_, To] -> {binary_to_integer(From_), binary_to_integer(To)};
                _ -> [invalid]
            end,
    case Range of
        [invalid] -> [invalid];
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
-spec stream_file(Socket :: term(), Transport :: atom(), State :: #{}, Range, Encoding :: binary(), BufferSize :: integer()) -> Result when
    Range :: default | {From :: integer(), To :: integer()},
    Result :: ok | no_return().
stream_file(Socket, Transport, State, Range, Encoding, BufferSize) when (BufferSize rem 3) =/= 0 ->
    %buffer size is extended, so it's divisible by 3 to allow base64 on the fly conversion
    stream_file(Socket, Transport, State, Range, Encoding, BufferSize - (BufferSize rem 3));
stream_file(Socket, Transport, State, default, Encoding, BufferSize) ->
    %default range should remain consistent with parse_object_ans/2 valuerange clause
    Size = get_size(State),
    stream_file(Socket, Transport, State, {0, Size - 1}, Encoding, BufferSize);
stream_file(Socket, Transport, #{path := Path} = State, {From, To}, Encoding, BufferSize) ->
    ToRead = To - From + 1,
    case ToRead > BufferSize of
        true ->
            {ok, Data} = onedata_file_api:read(Path, From, BufferSize),
            Transport:send(Socket, encode(Data, Encoding)),
            stream_file(Socket, Transport, State, {From + BufferSize, To}, Encoding, BufferSize);
        false ->
            {ok, Data} = onedata_file_api:read(Path, From, ToRead),
            Transport:send(Socket, encode(Data, Encoding))
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
        {ok, Value} -> Value;
        {error, ?ENOATTR} -> ?MIMETYPE_DEFAULT_VALUE
    end.

%%--------------------------------------------------------------------
%% @doc Encodes data according to given ecoding
%%--------------------------------------------------------------------
-spec encode(Data :: binary(), Encoding :: binary()) -> binary().
encode(Data, Encoding) when Encoding =:= <<"base64">> ->
    base64:encode(Data);
encode(Data, _) ->
    Data.

%%--------------------------------------------------------------------
%% @doc Gets size from map State
%%--------------------------------------------------------------------
-spec get_size(State :: #{}) -> non_neg_integer().
get_size(#{identity := Identity, path := Path} = _State) ->
    {ok, #file_attr{size = Size}} = onedata_file_api:stat(Identity, {path, Path}),
    Size.