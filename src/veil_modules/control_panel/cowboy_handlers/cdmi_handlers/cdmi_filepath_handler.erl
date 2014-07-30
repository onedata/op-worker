%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%% It handles cdmi object/container PUT, GET and DELETE requests
%% @end
%% ===================================================================
-module(cdmi_filepath_handler).
-author("Tomasz Lichon").

-include("err.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/control_panel/rest_messages.hrl").
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/control_panel/cdmi.hrl").

-define(default_get_dir_opts, [<<"objectType">>, <<"objectName">>, <<"parentURI">>, <<"completionStatus">>, <<"metadata">>, <<"children">>]). %todo add childrenrange
-define(default_get_file_opts, [<<"objectType">>, <<"objectName">>, <<"parentURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>, <<"valuetransferencoding">>, <<"valuerange">>, <<"value">>]).
-define(default_post_dir_opts, [<<"objectType">>, <<"objectName">>, <<"parentURI">>, <<"completionStatus">>, <<"metadata">>, <<"children">>]). %todo add childrenrange
-define(default_post_file_opts, [<<"objectType">>, <<"objectName">>, <<"parentURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>]).

-record(state, {
    method = <<"GET">> :: binary(),
    filepath = undefined :: binary(),
    filetype = undefined :: dir | reg,
    attributes = undefined :: #fileattributes{},
    cdmi_version = undefined :: undefined | binary(),
    opts = [] :: [binary()]
}).

%% Callbacks
-export([init/3, rest_init/2, resource_exists/2, allowed_methods/2, content_types_provided/2, content_types_accepted/2, delete_resource/2]).
-export([put_dir/2, put_cdmi_file/2, put_noncdmi_file/2, get_dir/2, get_cdmi_file/2, get_noncdmi_file/2]).

%% init/3
%% ====================================================================
%% @doc Cowboy callback function
%% Imposes a cowboy upgrade protocol to cowboy_rest - this module is
%% now treated as REST module by cowboy.
%% @end
-spec init(any(), any(), any()) -> {upgrade, protocol, cowboy_rest}.
%% ====================================================================
init(_, _, _) -> {upgrade, protocol, cowboy_rest}.

%% rest_init/2
%% ====================================================================
%% @doc Cowboy callback function
%% Called right after protocol upgrade to init the request context.
%% Will shut down the connection if the peer doesn't provide a valid
%% proxy certificate.
%% @end
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
%% ====================================================================
rest_init(Req, []) ->
    case binary:last(Req#http_req.path) =:= $/ of
        true -> rest_init(Req, [dir]);
        false -> rest_init(Req, [reg])
    end;
rest_init(Req, [Type]) ->
    {ok, DnString} = rest_utils:verify_peer_cert(Req),
    case rest_utils:prepare_context(DnString) of %todo check all required request header/body fields, and return badrequest error if something is missing
        ok ->
            {Method, _} = cowboy_req:method(Req),
            {PathInfo, _} = cowboy_req:path_info(Req),
            {RawOpts, _} = cowboy_req:qs(Req),
            {CdmiVersion, _} = cowboy_req:header(<<"x-cdmi-specification-version">>, Req),
            Path = case PathInfo == [] of
                       true -> "/";
                       false -> gui_str:binary_to_unicode_list(rest_utils:join_to_path(PathInfo))
                   end,
            {ok, Req, #state{method = Method, filepath = Path, filetype = Type, opts = parse_opts(RawOpts), cdmi_version = CdmiVersion}};
        Error -> {ok, Req, Error}
    end.

%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns methods that are allowed.
%% @end
-spec allowed_methods(req(), #state{} | {error, term()}) -> {[binary()], req(), #state{}}.
%% ====================================================================
% Some errors could have been detected in do_init/2. If so, State contains
% an {error, Type} tuple. These errors shall be handled here,
% because cowboy doesn't allow returning errors in rest_init.
allowed_methods(Req, {error, Type}) ->
    NewReq = case Type of
                 path_invalid -> rest_utils:reply_with_error(Req, warning, ?error_path_invalid, []);
                 {user_unknown, DnString} -> rest_utils:reply_with_error(Req, error, ?error_user_unknown, [DnString])
             end,
    {halt, NewReq, error};
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State}.

%% resource_exists/2
%% ====================================================================
%% @doc Cowboy callback function
%% Determines if resource identified by Filepath exists.
%% @end
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, #state{filepath = Filepath, filetype = Filetype} = State) ->
    case logical_files_manager:getfileattr(Filepath) of
        {ok, #fileattributes{type = "DIR"} = Attr} when Filetype =:= dir -> {true, Req, State#state{attributes = Attr}};
        {ok, #fileattributes{type = "REG"} = Attr} when Filetype =:= reg -> {true, Req, State#state{attributes = Attr}};
        _ -> {false, Req, State}
    end.

%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided.
%% @end
-spec content_types_provided(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
content_types_provided(Req, #state{filetype = dir} = State) -> %todo handle non-cdmi types
    {[
        {<<"application/cdmi-container">>, get_dir}
    ], Req, State};
content_types_provided(Req, #state{filetype = reg, cdmi_version = undefined} = State) ->
    {[
        {<<"application/binary">>, get_noncdmi_file}
    ], Req, State};
content_types_provided(Req, #state{filetype = reg} = State) ->
    {[
        {<<"application/cdmi-object">>, get_cdmi_file}
    ], Req, State}.

%% get_dir/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for file
%% @end
-spec get_dir(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_dir(Req, #state{opts = Opts} = State) ->
    DirCdmi = prepare_container_ans(case Opts of [] -> ?default_get_dir_opts; _ -> Opts end, State),
    Response = rest_utils:encode_to_json({struct, DirCdmi}),
    {Response, Req, State}.

%% get_noncdmi_file/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for file, returning file content as response body.
%% @end
-spec get_noncdmi_file(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_noncdmi_file(Req, #state{filepath = Filepath, attributes = #fileattributes{size = Size}} = State) ->
    StreamFun = file_download_handler:cowboy_file_stream_fun(Filepath, Size),
    NewReq = file_download_handler:content_disposition_attachment_headers(Req, filename:basename(Filepath)),
    {Type, Subtype, _} = cow_mimetypes:all(list_to_binary(Filepath)),
    ContentType = <<Type/binary, "/", Subtype/binary>>,
    Req2 = gui_utils:cowboy_ensure_header(<<"content-type">>, ContentType, NewReq),
    {{stream, Size, StreamFun}, Req2, State}.

%% get_cdmi_file/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for file, returning cdmi-object content type.
%% @end
-spec get_cdmi_file(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_cdmi_file(Req, #state{opts = Opts} = State) ->
    DirCdmi = prepare_object_ans(case Opts of [] -> ?default_get_file_opts; _ -> Opts end, State),
    case proplists:get_value(<<"value">>, DirCdmi) of
        {range, Range} ->
            Encoding = <<"base64">>, %todo send also utf-8 when possible
            case read_file(State, Range, Encoding) of
                {ok, Data} ->
                    DirCdmiWithValue = lists:append(proplists:delete(<<"value">>, DirCdmi), [{<<"value">>, Data}]),
                    Response = rest_utils:encode_to_json({struct, DirCdmiWithValue}),
                    {Response, Req, State};
                Error ->
                    ?error("Reading cdmi object end up with error: ~p", [Error]),
                    {ok, Req2} = cowboy_req:reply(?error_forbidden_code, Req),
                    {halt, Req2, State}
            end;
        undefined ->
            Response = rest_utils:encode_to_json({struct, DirCdmi}),
            {Response, Req, State}
    end.

%% content_types_accepted/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
-spec content_types_accepted(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
content_types_accepted(Req, #state{filetype = dir} = State) -> %todo handle noncdmi dir put
    {[
        {<<"application/cdmi-container">>, put_dir}
    ], Req, State};
content_types_accepted(Req, #state{filetype = reg, cdmi_version = undefined} = State) ->
    {[
        {<<"application/binary">>, put_noncdmi_file}
    ], Req, State};
content_types_accepted(Req, #state{filetype = reg} = State) ->
    {[
        {<<"application/cdmi-object">>, put_cdmi_file}
    ], Req, State}.

%% put_dir/2
%% ====================================================================
%% @doc Callback function for cdmi container PUT operation (create dir)
%% @end
-spec put_dir(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_dir(Req, #state{filepath = Filepath} = State) ->
    case logical_files_manager:mkdir(Filepath) of
        ok -> %todo check given body
            Response = rest_utils:encode_to_json({struct, prepare_container_ans(?default_post_dir_opts, State)}),
            Req2 = cowboy_req:set_resp_body(Response, Req),
            {true, Req2, State};
        {error, dir_exists} ->
            {ok, Req2} = cowboy_req:reply(?error_conflict_code, Req),
            {halt, Req2, State};
        {logical_file_system_error, "enoent"} ->
            {ok, Req2} = cowboy_req:reply(?error_not_found_code, Req),
            {halt, Req2, State};
        _ ->
            {ok, Req2} = cowboy_req:reply(?error_forbidden_code, Req),
            {halt, Req2, State}
    end.

%% put_cdmi_file/2
%% ====================================================================
%% @doc Callback function for cdmi data object PUT operation with cdmi body
%% content type. It parses body as JSON string and gets cdmi data to create file.
%% @end
-spec put_cdmi_file(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_cdmi_file(Req, #state{filepath = Filepath} = State) ->
    {ok, RawBody, _} = cowboy_req:body(Req),
    Body = parse_body(RawBody),
    ValueTransferEncoding = proplists:get_value(<<"valuetransferencoding">>, Body, <<"utf-8">>),  %todo check given body opts, store given mimetype
    Value = proplists:get_value(<<"value">>, Body, <<>>),
    RawValue = case ValueTransferEncoding of
                   <<"base64">> -> base64:decode(Value);
                   <<"utf-8">> -> Value
               end,
    case logical_files_manager:create(Filepath) of
        ok ->
            case logical_files_manager:write(Filepath, RawValue) of
                Bytes when is_integer(Bytes) andalso Bytes == byte_size(RawValue) ->
                    Response = rest_utils:encode_to_json({struct, prepare_object_ans(?default_post_file_opts, State)}),
                    Req2 = cowboy_req:set_resp_body(Response, Req),
                    {true, Req2, State};
                Error ->
                    ?error("Writing to cdmi object end up with error: ~p", [Error]),
                    logical_files_manager:delete(Filepath),
                    {ok, Req2} = cowboy_req:reply(?error_forbidden_code, Req),
                    {halt, Req2, State}
            end;
        {error, file_exists} ->
            {ok, Req2} = cowboy_req:reply(?error_conflict_code, Req),
            {halt, Req2, State};
        Error -> %todo handle common errors
            ?error("Creating cdmi object end up with error: ~p", [Error]),
            {ok, Req2} = cowboy_req:reply(?error_forbidden_code, Req),
            {halt, Req2, State}
    end.


%% put_noncdmi_file/2
%% ====================================================================
%% @doc Callback function for cdmi data object PUT operation with non-cdmi
%% body content-type. In that case we treat whole body as file content.
%% @end
-spec put_noncdmi_file(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_noncdmi_file(Req, #state{filepath = Filepath} = State) ->
    case logical_files_manager:create(Filepath) of %todo can be potentially refactored and combined with put_cdmi_file (some code is duplicated)
        ok ->
            write_body_to_file(Req, State, 0);
        {error, file_exists} ->
            {ok, Req2} = cowboy_req:reply(?error_conflict_code, Req),
            {halt, Req2, State};
        Error ->
            ?error("Creating cdmi object end up with error: ~p", [Error]),
            {ok, Req2} = cowboy_req:reply(?error_forbidden_code, Req),
            {halt, Req2, State}
    end.

%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests.
%% @end
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, #state{filepath = Filepath, filetype = dir} = State) ->
    case is_group_dir(Filepath) of
        false ->
            fs_remove_dir(Filepath),
            {true, Req, State};
        true ->
            {ok, Req2} = cowboy_req:reply(?error_forbidden_code, Req),
            {halt, Req2, State}
    end;
delete_resource(Req, #state{filepath = Filepath, filetype = reg} = State) ->
    case logical_files_manager:delete(Filepath) of
        ok ->
            {true, Req, State};
        _ ->
            {ok, Req2} = cowboy_req:reply(?error_forbidden_code, Req),
            {halt, Req2, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% prepare_container_ans/2
%% ====================================================================
%% @doc Prepares proplist formatted answer with field names from given list of binaries
%% @end
-spec prepare_container_ans([FieldName :: binary()], #state{}) -> [{FieldName :: binary(), Value :: term()}].
%% ====================================================================
prepare_container_ans([], _State) ->
    [];
prepare_container_ans([<<"objectType">> | Tail], State) ->
    [{<<"objectType">>, <<"application/cdmi-container">>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"objectName">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"objectName">>, list_to_binary([filename:basename(Filepath), "/"])} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentURI">> | Tail], #state{filepath = <<"/">>} = State) ->
    [{<<"parentURI">>, <<>>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"parentURI">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"parentURI">>, list_to_binary(fslogic_path:strip_path_leaf(Filepath))} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"completionStatus">> | Tail], State) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"metadata">> | Tail], State) -> %todo extract metadata
    [{<<"metadata">>, <<>>} | prepare_container_ans(Tail, State)];
prepare_container_ans([<<"children">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"children">>, [list_to_binary(Path) || Path <- rest_utils:list_dir(Filepath)]} | prepare_container_ans(Tail, State)];
prepare_container_ans([Other | Tail], State) ->
    [{Other, <<>>} | prepare_container_ans(Tail, State)].

%% prepare_object_ans/2
%% ====================================================================
%% @doc Prepares proplist formatted answer with field names from given list of binaries
%% @end
-spec prepare_object_ans([FieldName :: binary()], #state{}) -> [{FieldName :: binary(), Value :: term()}].
%% ====================================================================
prepare_object_ans([], _State) ->
    [];
prepare_object_ans([<<"objectType">> | Tail], State) ->
    [{<<"objectType">>, <<"application/cdmi-object">>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"objectName">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"objectName">>, list_to_binary(filename:basename(Filepath))} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentURI">> | Tail], #state{filepath = <<"/">>} = State) ->
    [{<<"parentURI">>, <<>>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"parentURI">> | Tail], #state{filepath = Filepath} = State) ->
    [{<<"parentURI">>, list_to_binary(fslogic_path:strip_path_leaf(Filepath))} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"completionStatus">> | Tail], State) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"mimetype">> | Tail], #state{filepath = Filepath} = State) ->
    {Type, Subtype, _} = cow_mimetypes:all(gui_str:to_binary(Filepath)),
    [{<<"mimetype">>, <<Type/binary, "/", Subtype/binary>>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"metadata">> | Tail], State) -> %todo extract metadata
    [{<<"metadata">>, <<>>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"valuetransferencoding">> | Tail], State) ->
    [{<<"valuetransferencoding">>, <<"base64">>} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"value">> | Tail], State) ->
    [{<<"value">>, {range, default}} | prepare_object_ans(Tail, State)];
prepare_object_ans([{<<"value">>, From, To} | Tail], State) ->
    [{<<"value">>, {range, {From, To}}} | prepare_object_ans(Tail, State)];
prepare_object_ans([<<"valuerange">> | Tail], #state{opts = Opts, attributes = Attrs} = State) ->
    case lists:keyfind(<<"value">>, 1, Opts) of
        {<<"value">>, From, To} ->
            [{<<"valuerange">>, iolist_to_binary([integer_to_binary(From), <<"-">>, integer_to_binary(To)])} | prepare_object_ans(Tail, State)];
        _ ->
            [{<<"valuerange">>, iolist_to_binary([<<"0-">>, integer_to_binary(Attrs#fileattributes.size - 1)])} | prepare_object_ans(Tail, State)]
    end;
prepare_object_ans([Other | Tail], State) ->
    [{Other, <<>>} | prepare_object_ans(Tail, State)].

%% parse_opts/1
%% ====================================================================
%% @doc Parses given cowboy 'qs' opts (all that appears after '?' in url), splitting
%% them by ';' separator and handling range values,
%% i. e. input: binary("aaa;bbb:1-2;ccc") will return [binary(aaa),{binary(bbb),1,2},binary(ccc)]
%% @end
-spec parse_opts(binary()) -> [binary() | {binary(), From :: integer(), To :: integer()}].
%% ====================================================================
parse_opts(<<>>) ->
    [];
parse_opts(RawOpts) ->
    Opts = binary:split(RawOpts, <<";">>, [global]),
    lists:map(
        fun(Opt) ->
            case binary:split(Opt, <<":">>) of
                [SimpleOpt] -> SimpleOpt;
                [SimpleOpt, Range] ->
                    [From, To] = binary:split(Range, <<"-">>),
                    {SimpleOpt, binary_to_integer(From), binary_to_integer(To)}
            end
        end,
        Opts
    ).

%% parse_body/1
%% ====================================================================
%% @doc Parses json request body to erlang proplist format.
%% @end
-spec parse_body(binary()) -> list().
%% ====================================================================
parse_body(RawBody) ->
    case gui_str:binary_to_unicode_list(RawBody) of
        "" -> [];
        NonEmptyBody ->
            {struct, Ans} = rest_utils:decode_from_json(gui_str:binary_to_unicode_list(NonEmptyBody)),
            Ans
    end.

%% write_body_to_file/3
%% ====================================================================
%% @doc Reads request's body and writes it to file obtained from state.
%% This callback return value is compatibile with put requests.
%% @end
-spec write_body_to_file(req(), #state{}, integer()) -> {boolean(), req(), #state{}}.
%% ====================================================================
write_body_to_file(Req, #state{filepath = Filepath} = State, Offset) ->
    case cowboy_req:stream_body(Req) of
        {ok, Chunk, Req2} ->
            case logical_files_manager:write(Filepath, Offset, Chunk) of
                Bytes when is_integer(Bytes) ->
                    write_body_to_file(Req2, State, Offset + Bytes);
                Error ->
                    ?error("Writing to cdmi object end up with error: ~p", [Error]),
                    logical_files_manager:delete(Filepath),
                    {ok, Req2} = cowboy_req:reply(?error_forbidden_code, Req),
                    {halt, Req2, State}
            end;
        {done, Req2} ->
            {true, Req2, State}
    end.

%% read_file/3
%% ====================================================================
%% @doc Reads given range of bytes (defaults to whole file) from file (obtained from state filepath), result is
%% encoded according to 'Encoding' argument
%% @end
-spec read_file(State :: #state{}, Range, Encoding) -> Result when
    Range :: default | {From :: integer(), To :: integer()},
    Encoding :: binary(),
    Result :: {ok, Bytes} | {ErrorGeneral, ErrorDetail},
    Bytes :: binary(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
read_file(#state{attributes = Attrs} = State, default, Encoding) ->
    read_file(State, {0, Attrs#fileattributes.size - 1}, Encoding); %default range shuold remain consistent with parse_object_ans/2 valuerange clause
read_file(State, Range, <<"base64">>) ->
    case read_file(State, Range, <<"utf-8">>) of
        {ok, Data} -> {ok, base64:encode(Data)};
        Error -> Error
    end;
read_file(#state{filepath = Path}, {From, To}, <<"utf-8">>) ->
    logical_files_manager:read(Path, From, To - From + 1).

%% fs_remove/1
%% ====================================================================
%% @doc Removes given file/dir from filesystem and db. In case of dir, it's
%% done recursively.
%% @end
-spec fs_remove(Path :: string()) -> Result when
    Result :: ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
fs_remove(Path) ->
    {ok, FA} = logical_files_manager:getfileattr(Path),
    case FA#fileattributes.type of
        "DIR" -> fs_remove_dir(Path);
        "REG" -> logical_files_manager:delete(Path)
    end.

%% fs_remove_dir/1
%% ====================================================================
%% @doc Removes given dir with all files and subdirectories.
%% @end
-spec fs_remove_dir(DirPath :: string()) -> Result when
    Result :: ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
fs_remove_dir(DirPath) ->
    case is_group_dir(DirPath) of
        true -> ok;
        false ->
            ItemList = fs_list_dir(DirPath),
            lists:foreach(fun(Item) -> fs_remove(filename:join(DirPath, Item)) end, ItemList),
            logical_files_manager:rmdir(DirPath)
    end.

%% fs_list_dir/1
%% ====================================================================
%% @doc @equiv fs_list_dir(Dir, 0, 10, [])
-spec fs_list_dir(Dir :: string()) -> [string()].
%% ====================================================================
fs_list_dir(Dir) ->
    fs_list_dir(Dir, 0, 10, []).

%% fs_list_dir/4
%% ====================================================================
%% @doc Lists all childrens of given dir, starting from offset and with initial
%% chunk size set to 'Count'
-spec fs_list_dir(Dir :: string(), Offset :: integer(), Count :: integer(), Result :: [string()]) -> [string()].
%% ====================================================================
fs_list_dir(Path, Offset, Count, Result) ->
    case logical_files_manager:ls(Path, Count, Offset) of
        {ok, FileList} ->
            case length(FileList) of
                Count -> fs_list_dir(Path, Offset + Count, Count * 10, Result ++ FileList);
                _ -> Result ++ FileList
            end;
        _ ->
            {error, not_a_dir}
    end.

%% is_group_dir/1
%% ====================================================================
%% @doc Returns true when Path points to group directory (or groups root directory)
-spec is_group_dir(Path :: string()) -> boolean().
%% ====================================================================
is_group_dir(Path) ->
    case string:tokens(Path,"/") of
        [?GROUPS_BASE_DIR_NAME] -> true;
        [?GROUPS_BASE_DIR_NAME , _GroupName] ->  true;
        _ -> false
    end.