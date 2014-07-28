%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cowboy handler module, implementing cowboy_rest interface.
%% It handles CDMI requests by routing them to proper cdmi module.
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
-define(default_get_file_opts, [<<"objectType">>, <<"objectName">>, <<"parentURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>,<<"valuetransferencoding">>,<<"valuerange">>,<<"value">>]).
-define(default_post_dir_opts, [<<"objectType">>, <<"objectName">>, <<"parentURI">>, <<"completionStatus">>, <<"metadata">>, <<"children">>]). %todo add childrenrange
-define(default_post_file_opts, [<<"objectType">>, <<"objectName">>, <<"parentURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>]). %todo add valuerange

-record(state, {
    method = <<"GET">> :: binary(),
    filepath = undefined :: binary(),
    attributes = undefined :: #fileattributes{},
    opts = [] :: [binary()],
    body = [] :: list()
}).

%% Callbacks
-export([init/3, rest_init/2, resource_exists/2, allowed_methods/2, content_types_provided/2,content_types_accepted/2, delete_resource/2]).
-export([put_dir/2, put_file/2, get_dir/2, get_file/2]).

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
rest_init(Req, _Opts) ->
    {ok,DnString} = rest_utils:verify_peer_cert(Req),
    case rest_utils:prepare_context(DnString) of %todo check all required request header/body fields
        ok ->
            {Method, _} = cowboy_req:method(Req),
            {PathInfo, _} = cowboy_req:path_info(Req),
            {RawOpts,_} = cowboy_req:qs(Req),
            {ok,RawBody,_} = cowboy_req:body(Req),
            Path = case PathInfo == [] of
                     true -> "/";
                     false -> gui_str:binary_to_unicode_list(rest_utils:join_to_path(PathInfo))
                 end,
            {ok, Req, #state{method = Method, filepath = Path, opts = parse_opts(RawOpts), body = parse_body(RawBody) }};
        Error -> {ok,Req,Error}
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
resource_exists(Req, #state{filepath = Filepath} = State) ->
    case logical_files_manager:getfileattr(Filepath) of
        {ok, Attr} -> {true, Req, State#state{attributes = Attr}};
        _ -> {false, Req, State}
    end.

%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided.
%% @end
-spec content_types_provided(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
content_types_provided(Req, State) -> %todo handle non-cdmi types
    {[
        {<<"application/cdmi-container">>, get_dir},
        {<<"application/cdmi-object">>,get_file}
    ], Req, State}.

%% get_dir/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for file
%% @end
-spec get_dir(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_dir(Req, #state{opts = Opts, attributes = #fileattributes{type = "DIR"}} = State) ->
    DirCdmi = prepare_container_ans(State,case Opts of [] -> ?default_get_dir_opts; _ -> Opts end),
    Response = rest_utils:encode_to_json({struct, DirCdmi}),
    {Response, Req, State};
get_dir(Req, State) -> %given uri points to file, return 404
    {ok,Req2} = cowboy_req:reply(?error_not_found_code,Req),
    {halt, Req2, State}.

%% get_file/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for directory.
%% @end
-spec get_file(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_file(Req, #state{opts = Opts, attributes = #fileattributes{type = "REG"}} = State) ->
    DirCdmi = prepare_object_ans(State,case Opts of [] -> ?default_get_file_opts; _ -> Opts end),
    case proplists:get_value(<<"value">>, DirCdmi) of
        {range, Range} ->
            Encoding = <<"base64">>, %todo send also utf-8 when possible
            case read_file(State,Range,Encoding) of
                {ok,Data} ->
                    DirCdmiWithValue = lists:append(proplists:delete(<<"value">>,DirCdmi), [{<<"value">>,Data}]),
                    Response = rest_utils:encode_to_json({struct, DirCdmiWithValue}),
                    {Response, Req, State};
                Error ->
                    ?error("Reading cdmi object end up with error: ~p", [Error]),
                    {ok,Req2} = cowboy_req:reply(?error_forbidden_code,Req),
                    {halt, Req2, State}
            end;
        undefined ->
            Response = rest_utils:encode_to_json({struct, DirCdmi}),
            {Response, Req, State}
    end;
get_file(Req, State) -> %given uri points to dir, return 404
    {ok,Req2} = cowboy_req:reply(?error_not_found_code,Req),
    {halt, Req2, State}.

%% content_types_accepted/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
-spec content_types_accepted(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
content_types_accepted(Req, State) ->
    {[
        {<<"application/cdmi-container">>, put_dir},
        {<<"application/cdmi-object">>,put_file}
    ], Req, State}.

%% put_dir/2
%% ====================================================================
%% @doc Callback function for cdmi container PUT operation (create dir)
%% @end
-spec put_dir(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_dir(Req, #state{filepath = Filepath, body = _Body} = State) ->
    case logical_files_manager:mkdir(Filepath) of
        ok -> %todo check given body
            Response = rest_utils:encode_to_json({struct, prepare_container_ans(State,?default_post_dir_opts)}),
            Req2 = cowboy_req:set_resp_body(Response,Req),
            {true,Req2,State};
        {error, dir_exists} ->
            {ok,Req2} = cowboy_req:reply(?error_conflict_code,Req),
            {halt, Req2, State};
        {logical_file_system_error,"enoent"} ->
            {ok,Req2} = cowboy_req:reply(?error_not_found_code,Req),
            {halt, Req2, State};
        _ ->
            {ok,Req2} = cowboy_req:reply(?error_forbidden_code,Req),
            {halt, Req2, State}
    end.

%% put_file/2
%% ====================================================================
%% @doc Callback function for cdmi data object PUT operation (create file)
%% @end
-spec put_file(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
put_file(Req,#state{filepath = Filepath, body = Body} = State) -> %todo handle writing in chunks
    ValueTransferEncoding = proplists:get_value(<<"valuetransferencoding">>,Body,<<"utf-8">>),  %todo check given body opts, store given mimetype
    Value = proplists:get_value(<<"value">>,Body,<<>>),
    case logical_files_manager:create(Filepath) of
        ok ->
            RawValue = case ValueTransferEncoding of
                <<"base64">> -> base64:decode(Value);
                <<"utf-8">> -> Value
            end,
            case logical_files_manager:write(Filepath,RawValue) of
                Bytes when is_integer(Bytes) andalso Bytes == byte_size(RawValue) ->
                    Response = rest_utils:encode_to_json({struct, prepare_object_ans(State,?default_post_file_opts)}),
                    Req2 = cowboy_req:set_resp_body(Response, Req),
                    {true,Req2,State};
                Error ->
                    ?error("Writing to cdmi object end up with error: ~p", [Error]),
                    logical_files_manager:delete(Filepath),
                    {ok,Req2} = cowboy_req:reply(?error_forbidden_code,Req),
                    {halt, Req2, State}
            end;
        {error, file_exists} ->
            {ok,Req2} = cowboy_req:reply(?error_conflict_code,Req),
            {halt, Req2, State};
        Error -> %todo handle common errors
            ?error("Creating cdmi object end up with error: ~p", [Error]),
            {ok,Req2} = cowboy_req:reply(?error_forbidden_code,Req),
            {halt, Req2, State}
    end.

%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests.
%% @end
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, #state{filepath = Filepath, attributes = #fileattributes{type = "DIR"}} = State) ->
    case is_group_dir(Filepath) of
        false ->
            fs_remove_dir(Filepath),
            {true, Req,State};
        true ->
            {ok,Req2} = cowboy_req:reply(?error_forbidden_code,Req),
            {halt, Req2, State}
    end;
delete_resource(Req, #state{filepath = Filepath, attributes = #fileattributes{type = "REG"}} = State) ->
    case logical_files_manager:delete(Filepath) of
       ok ->
           {true, Req, State};
       _ ->
           {ok,Req2} = cowboy_req:reply(?error_forbidden_code,Req),
           {halt, Req2 ,State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% prepare_container_ans/2
%% ====================================================================
%% @doc Prepares json formatted answer with field names from given list of binaries
%% @end
-spec prepare_container_ans(#state{}, [FieldName :: binary()]) -> [{FieldName :: binary(), Value :: term()}].
%% ====================================================================
prepare_container_ans(_State,[]) ->
    [];
prepare_container_ans(State,[<<"objectType">> | Tail]) ->
    [{<<"objectType">>, <<"application/cdmi-container">>} | prepare_container_ans(State, Tail)];
prepare_container_ans(#state{filepath = Filepath} = State,[<<"objectName">> | Tail]) ->
    [{<<"objectName">>, list_to_binary([filename:basename(Filepath),"/"])} | prepare_container_ans(State, Tail)];
prepare_container_ans(#state{filepath = <<"/">>} = State,[<<"parentURI">> | Tail]) ->
    [{<<"parentURI">>, <<>>} | prepare_container_ans(State, Tail)];
prepare_container_ans(#state{filepath = Filepath} = State,[<<"parentURI">> | Tail]) ->
    [{<<"parentURI">>, list_to_binary(fslogic_path:strip_path_leaf(Filepath))} | prepare_container_ans(State, Tail)];
prepare_container_ans(State,[<<"completionStatus">> | Tail]) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare_container_ans(State, Tail)];
prepare_container_ans(State,[<<"metadata">> | Tail]) -> %todo extract metadata
    [{<<"metadata">>, <<>>} | prepare_container_ans(State, Tail)];
prepare_container_ans(#state{filepath = Filepath} = State,[<<"children">> | Tail]) ->
    [{<<"children">>, [list_to_binary(Path) || Path <- rest_utils:list_dir(Filepath)]} | prepare_container_ans(State, Tail)];
prepare_container_ans(State,[Other | Tail]) ->
    [{Other, <<>>} | prepare_container_ans(State, Tail)].

%% prepare_object_ans/2
%% ====================================================================
%% @doc Prepares json formatted answer with field names from given list of binaries
%% @end
-spec prepare_object_ans(#state{}, [FieldName :: binary()]) -> [{FieldName :: binary(), Value :: term()}].
%% ====================================================================
prepare_object_ans(_State,[]) ->
    [];
prepare_object_ans(State,[<<"objectType">> | Tail]) ->
    [{<<"objectType">>, <<"application/cdmi-object">>} | prepare_object_ans(State, Tail)];
prepare_object_ans(#state{filepath = Filepath} = State,[<<"objectName">> | Tail]) ->
    [{<<"objectName">>, list_to_binary(filename:basename(Filepath))} | prepare_object_ans(State, Tail)];
prepare_object_ans(#state{filepath = <<"/">>} = State,[<<"parentURI">> | Tail]) ->
    [{<<"parentURI">>, <<>>} | prepare_object_ans(State, Tail)];
prepare_object_ans(#state{filepath = Filepath} = State,[<<"parentURI">> | Tail]) ->
    [{<<"parentURI">>, list_to_binary(fslogic_path:strip_path_leaf(Filepath))} | prepare_object_ans(State, Tail)];
prepare_object_ans(State,[<<"completionStatus">> | Tail]) ->
    [{<<"completionStatus">>, <<"Complete">>} | prepare_object_ans(State, Tail)];
prepare_object_ans(#state{filepath = Filepath} = State,[<<"mimetype">> | Tail]) ->
    {Type, Subtype, _} = cow_mimetypes:all(gui_str:to_binary(Filepath)),
    [{<<"mimetype">>, <<Type/binary, "/", Subtype/binary>>} | prepare_object_ans(State, Tail)];
prepare_object_ans(State,[<<"metadata">> | Tail]) -> %todo extract metadata
    [{<<"metadata">>, <<>>} | prepare_object_ans(State, Tail)];
prepare_object_ans(State,[<<"valuetransferencoding">> | Tail]) ->
    [{<<"valuetransferencoding">>, <<"base64">>} | prepare_object_ans(State, Tail)];
prepare_object_ans(State,[<<"value">> | Tail]) ->
    [{<<"value">>, {range, default}} | prepare_object_ans(State, Tail)];
prepare_object_ans(State,[{<<"value">>,From,To} | Tail]) ->
    [{<<"value">>, {range, {From,To}}} | prepare_object_ans(State, Tail)];
prepare_object_ans(#state{opts = Opts,attributes = Attrs} = State,[<<"valuerange">> | Tail]) ->
    case lists:keyfind(<<"value">>,1,Opts) of
        {<<"value">>,From,To} ->
            [{<<"valuerange">>, iolist_to_binary([integer_to_binary(From),<<"-">>,integer_to_binary(To)])} | prepare_object_ans(State, Tail)];
        _ ->
            [{<<"valuerange">>, iolist_to_binary([<<"0-">>,integer_to_binary(Attrs#fileattributes.size-1)])} | prepare_object_ans(State, Tail)]
    end;
prepare_object_ans(State,[Other | Tail]) ->
    [{Other, <<>>} | prepare_object_ans(State, Tail)].

%% parse_opts/1
%% ====================================================================
%% @doc Parses given cowboy 'qs' opts (all that appears after '?' in url), splitting
%% them by ';' separator ignoring additional values after ':',
%% i. e. input: binary("aaa;bbb:1-2;ccc") will return [binary(aaa),binary(bbb),binary(ccc)]
%% @end
-spec parse_opts(binary()) -> [binary()].
%% ====================================================================
parse_opts(<<>>) ->
    [];
parse_opts(RawOpts) ->
    Opts = binary:split(RawOpts,<<";">>,[global]),
    lists:map(
        fun(Opt) ->
            case binary:split(Opt,<<":">>) of
                [SimpleOpt] -> SimpleOpt;
                [SimpleOpt, Range] ->
                    [From,To] = binary:split(Range,<<"-">>),
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
            {struct,Ans} = rest_utils:decode_from_json(gui_str:binary_to_unicode_list(NonEmptyBody)),
            Ans
    end.


read_file(#state{attributes = Attrs} = State,default,Encoding) ->
    read_file(State,{0,Attrs#fileattributes.size-1},Encoding); %default range shuold remain consistent with parse_object_ans/2 valuerange clause
read_file(State,Range,<<"base64">>) ->
    case read_file(State,Range,<<"utf-8">>) of
        {ok,Data} -> {ok,base64:encode(Data)};
        Error -> Error
    end;
read_file(#state{filepath = Path},{From,To},<<"utf-8">>) ->
    logical_files_manager:read(Path,From, To-From+1).



%% fs_remove/1
%% ====================================================================
%% @doc Removes given file/dir from filesystem and db. In case of dir, it's
%% done recursively.
%% @end
-spec fs_remove(Path :: string()) -> Result when
Result :: ok | {ErrorGeneral :: atom(), ErrorDetail::term()}.
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
    Result :: ok | {ErrorGeneral :: atom(), ErrorDetail::term()}.
%% ====================================================================
fs_remove_dir(DirPath) ->
    case is_group_dir(DirPath) of
        true -> ok;
        false ->
            ItemList = fs_list_dir(DirPath),
            lists:foreach(fun(Item) -> fs_remove(filename:join(DirPath,Item)) end, ItemList),
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
    case Path of
        "/groups" -> true;
        [$/,$g,$r,$o,$u,$p,$s | Rest] -> case length(string:tokens(Rest, "/")) of
                                             1 -> true;
                                             _ -> false
                                         end;
        _ -> false
    end.