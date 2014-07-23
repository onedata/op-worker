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

-define(default_dir_opts, [<<"objectType">>, <<"objectName">>, <<"parentURI">>, <<"completionStatus">>, <<"metadata">>, <<"children">>]).

-record(state, {
    method = <<"GET">> :: binary(),
    filepath = undefined :: binary(),
    attributes = undefined :: #fileattributes{},
    opts = ?default_dir_opts :: [binary()]
}).

%% API
-export([init/3, rest_init/2, resource_exists/2, allowed_methods/2, content_types_provided/2, get_dir/2, get_file/2]).
-export([content_types_accepted/2, delete_resource/2]).

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
    case rest_utils:prepare_context(DnString) of
        ok ->
            {Method, _} = cowboy_req:method(Req),
            {PathInfo, _} = cowboy_req:path_info(Req),
            {RawOpts,_} = cowboy_req:qs(Req),
            Path = case PathInfo == [] of
                     true -> "/";
                     false -> gui_str:binary_to_unicode_list(rest_utils:join_to_path(PathInfo))
                 end,
            {ok, Req, #state{method = Method, filepath = Path, opts = parse_opts(RawOpts) }};
        Error -> {ok,Req,Error}
    end.


%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns methods that are allowed.
%% @end
-spec allowed_methods(req(), #state{} | {error, term()}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, State) ->
    {[<<"PUT">>, <<"GET">>, <<"DELETE">>], Req, State};
% Some errors could have been detected in do_init/2. If so, State contains
% an {error, Type} tuple. These errors shall be handled here,
% because cowboy doesn't allow returning errors in rest_init.
allowed_methods(Req, {error, Type}) ->
    NewReq = case Type of
                 path_invalid -> rest_utils:reply_with_error(Req, warning, ?error_path_invalid, []);
                 {user_unknown, DnString} -> rest_utils:reply_with_error(Req, error, ?error_user_unknown, [DnString])
             end,
    {halt, NewReq, error}.

%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided. "application/json" is default.
%% It can be changed later by gui_utils:cowboy_ensure_header/3.
%% @end
-spec content_types_provided(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-container">>, get_dir},
        {<<"application/cdmi-object">>,get_file}
    ], Req, State}.


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
        _ ->
            ErrorRec = ?report_warning(?error_not_found, [Filepath]),
            Req2 = cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec), Req),
            {false, Req2, State}
    end.


%% get_dir/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for file
%% @end
-spec get_dir(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_dir(Req, #state{opts = Opts, attributes = #fileattributes{type = "DIR"}} = State) ->
    DirCdmi = prepare_container_ans(State,Opts),
    Response = rest_utils:encode_to_json({struct, DirCdmi}),
    {Response, Req, State}.

%% get_file/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for directory.
%% @end
-spec get_file(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_file(Req, #state{filepath = Filepath, attributes = #fileattributes{type = "REG", size = Size}} = State) ->
    StreamFun = file_download_handler:cowboy_file_stream_fun(Filepath, Size),
    NewReq = file_download_handler:content_disposition_attachment_headers(Req, filename:basename(Filepath)),
    {Type, Subtype, _} = cow_mimetypes:all(gui_str:to_binary(Filepath)),
    Mimetype = <<Type/binary, "/", Subtype/binary>>,
    Req3 = gui_utils:cowboy_ensure_header(<<"content-type">>, Mimetype, NewReq),
    {{stream, Size, StreamFun}, Req3, State}. %todo return proper cdmi value


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
        {{<<"application">>, <<"json">>, '*'}, handle_json_data}
    ], Req, State}.

%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests.
%% Will call delete/2 from rest_module_behaviour.
%% @end
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, #state{attributes = #fileattributes{type = "DIR"}} = State) ->
    ErrorRec = ?report_warning(?error_dir_cannot_delete),
    {false, cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec),Req), State};
delete_resource(Req, #state{filepath = Filepath, attributes = #fileattributes{type = "REG"}} = State) ->
    case logical_files_manager:delete(Filepath) of
       ok ->
           {true, cowboy_req:set_resp_body(rest_utils:success_reply(?success_file_deleted),Req), State};
       _ ->
           ErrorRec = ?report_error(?error_reg_file_cannot_delete, [Filepath]),
           {false, cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec),Req),State}
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

prepare_container_ans(#state{filepath = Filepath} = State,[Other | Tail]) ->
    [{Other, <<>>} | prepare_container_ans(State, Tail)].

%% parse_opts/1
%% ====================================================================
%% @doc Parses given cowboy 'qs' opts (all that appears after '?' in url), splitting
%% them by ';' separator ignoring additional values after ':',
%% i. e. input: binary("aaa;bbb:1-2;ccc") will return [binary(aaa),binary(bbb),binary(ccc)]
%% @end
-spec parse_opts(binary()) -> [binary()].
%% ====================================================================
parse_opts(<<>>) ->
  ?default_dir_opts;
parse_opts(RawOpts) ->
  Opts = binary:split(RawOpts,<<";">>,[global]),
  [hd(binary:split(Opt,<<":">>)) || Opt <- Opts]. %todo handle 'value:something' format