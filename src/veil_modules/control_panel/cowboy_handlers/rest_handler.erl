%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a cowboy handler module, implementing cowboy_rest interface.
%% It handles REST requests by routing them to proper rest module.
%% @end
%% ===================================================================

-module(rest_handler).

-include_lib("public_key/include/public_key.hrl").
-include("veil_modules/control_panel/common.hrl").
-include("err.hrl").
-include("veil_modules/control_panel/global_registry_interfacing.hrl").

-record(state, {
    version = <<"latest">> :: binary(),
    method = <<"GET">> :: binary(),
    handler_module = undefined :: atom(),
    resource_id = undefined :: binary()
}).

-export([init/3, rest_init/2, resource_exists/2, allowed_methods/2, content_types_provided/2, get_resource/2]).
-export([content_types_accepted/2, delete_resource/2, handle_urlencoded_data/2, handle_json_data/2, handle_multipart_data/2]).


%% ====================================================================
%% API functions
%% ====================================================================

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
%% proxy certificate (unless it is a connection check where no auth is required).
%% @end
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
%% ====================================================================
rest_init(Req, _Opts) ->
    case cowboy_req:path_info(Req) of
        {[Endpoint], _} when Endpoint =:= ?connection_check_path orelse Endpoint =:= <<"token">> ->
            % when checking connection or generating token, continue without cert verification
            init_state(Req);
        _ ->
            {ok, Identity, Req1} = rest_utils:verify_peer_cert(Req),
            Req2 = gui_utils:cowboy_ensure_header(<<"content-type">>, <<"application/json">>, Req1),
            case rest_utils:prepare_context(Identity) of
                ok -> init_state(Req2);
                Error -> {ok, Req2, Error}
            end
    end.

%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns methods that are allowed, based on version specified in URI.
%% Will call methods_and_version_info/1 from rest_module_behaviour.
%% @end
-spec allowed_methods(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, #state{version = Version, handler_module = Mod} = State) ->
    {MethodsVersionInfo, Req2} = Mod:methods_and_versions_info(Req),
    RequestedVersion = case Version of
                           <<"latest">> ->
                               {Ver, _} = lists:last(MethodsVersionInfo),
                               Ver;
                           Ver ->
                               Ver
                       end,
    % Check if requested version is supported
    case proplists:get_value(RequestedVersion, MethodsVersionInfo, undefined) of
        undefined ->
            NewReq = rest_utils:reply_with_error(Req2, warning, ?error_version_unsupported, [binary_to_list(RequestedVersion)]),
            {halt, NewReq, State};
        AllowedMethods ->
            {Method, _} = cowboy_req:method(Req2),
            % Check if requested method is allowed
            case lists:member(Method, AllowedMethods) of
                false ->
                    ErrorRec = ?report_warning(?error_method_unsupported, [binary_to_list(Method)]),
                    NewReq = cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec), Req2),
                    {AllowedMethods, NewReq, State};
                true ->
                    % Check if content-type is acceptable (for PUT or POST)
                    case (Method =/= <<"POST">> andalso Method =/= <<"PUT">>) orelse (content_type_supported(Req2)) of
                        false ->
                            NewReq = rest_utils:reply_with_error(Req2, warning, ?error_media_type_unsupported, []),
                            {halt, NewReq, State};
                        true ->
                            {AllowedMethods, Req2, State#state{version = RequestedVersion}}
                    end
            end
    end;


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
    {[{<<"application/json">>, get_resource}], Req, State}.


%% resource_exists/2
%% ====================================================================
%% @doc Cowboy callback function
%% Determines if resource identified by URI exists.
%% Will call exists/3 from rest_module_behaviour.
%% @end
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, #state{handler_module = undefined} = State) ->
    {false, Req, State};

resource_exists(Req, #state{resource_id = undefined} = State) ->
    {true, Req, State};

resource_exists(Req, #state{version = Version, handler_module = Mod, resource_id = Id} = State) ->
    {Exists, NewReq} = Mod:exists(Req, Version, Id),
    case Exists of
        false ->
            ErrorRec = ?report_warning(?error_not_found, [binary_to_list(Id)]),
            Req2 = cowboy_req:set_resp_body(rest_utils:error_reply(ErrorRec), NewReq),
            {false, Req2, State};
        true ->
            {true, NewReq, State}
    end.


%% get_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests. 
%% Will call get/3 from rest_module_behaviour.
%% @end
-spec get_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_resource(Req, #state{version = Version, handler_module = Mod, resource_id = Id} = State) ->
    {Answer, Req2} = Mod:get(Req, Version, Id),
    % process_callback_answer/2 cannot be used here as cowboy expects other returned values
    {Resp, NewReq} = case Answer of
                         ok ->
                             {halt, Req2};
                         {body, ResponseBody} ->
                             {ResponseBody, Req2};
                         {stream, Size, Fun, ContentType} ->
                             Req3 = gui_utils:cowboy_ensure_header(<<"content-type">>, ContentType, Req2),
                             {{stream, Size, Fun}, Req3};
                         error ->
                             {ok, Req3} = veil_cowboy_bridge:apply(cowboy_req, reply, [500, Req2]),
                             {halt, Req3};
                         {error, ErrorDesc} ->
                             Req3 = cowboy_req:set_resp_body(ErrorDesc, Req2),
                             {ok, Req4} = veil_cowboy_bridge:apply(cowboy_req, reply, [500, Req3]),
                             {halt, Req4}
                     end,
    {Resp, NewReq, State}.


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
        {{<<"application">>, <<"x-www-form-urlencoded">>, '*'}, handle_urlencoded_data},
        {{<<"application">>, <<"json">>, '*'}, handle_json_data},
        {{<<"multipart">>, <<"form-data">>, '*'}, handle_multipart_data}
    ], Req, State}.


%% handle_urlencoded_data/2
%% ====================================================================
%% @doc Function handling "application/x-www-form-urlencoded" requests.
%% @end
-spec handle_urlencoded_data(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
handle_urlencoded_data(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) ->
    {ok, Data, Req2} = cowboy_req:body_qs(Req),
    {Result, NewReq} = handle_data(Req2, Mod, Version, Id, Data),
    {Result, NewReq, State}.


%% handle_json_data/2
%% ====================================================================
%% @doc Function handling "application/json" requests.
%% @end
-spec handle_json_data(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
handle_json_data(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) ->
    {ok, Binary, Req2} = veil_cowboy_bridge:apply(cowboy_req, body, [Req]),
    Data = case Binary of
               <<"">> ->
                   <<"">>;
               _ -> rest_utils:decode_from_json(Binary)
           end,
    {Result, NewReq} = handle_data(Req2, Mod, Version, Id, Data),
    {Result, NewReq, State}.


%% handle_multipart_data/2
%% ====================================================================
%% @doc Function handling "multipart/form-data" requests.
%% @end
-spec handle_multipart_data(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
handle_multipart_data(Req, #state{handler_module = Mod, version = Version, resource_id = Id, method = Method} = State) ->
    {Result, NewReq} = case erlang:function_exported(Mod, handle_multipart_data, 4) of
                           true ->
                               {Answer, Req2} = Mod:handle_multipart_data(Req, Version, Method, Id),
                               process_callback_answer(Answer, Req2);
                           false ->
                               {false, Req}
                       end,
    {Result, NewReq, State}.


%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests. 
%% Will call delete/2 from rest_module_behaviour.
%% @end
-spec delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
delete_resource(Req, #state{handler_module = Mod, version = Version, resource_id = Id} = State) ->
    {Answer, Req2} = Mod:delete(Req, Version, Id),
    {Result, Req3} = process_callback_answer(Answer, Req2),
    {Result, Req3, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================
init_state(Req) ->
    {Method, _} = cowboy_req:method(Req),
    {Version, _} = cowboy_req:binding(version, Req), % :version in cowboy router
    {PathInfo, _} = cowboy_req:path_info(Req),
    case rest_routes:route(PathInfo) of
        {error, Error} ->
            {ok, Req, {error, Error}};
        {Module, Id} ->
            {ok, Req, #state{version = Version, handler_module = Module, method = Method, resource_id = Id}}
    end.


%% handle_data/5
%% ====================================================================
%% Handles POST or PUT requests.
%% Will call post/2 or put/3 from rest_module_behaviour.
%% @end
-spec handle_data(req(), atom(), binary(), binary(), term()) -> {boolean(), req()}.
%% ====================================================================
handle_data(Req, Mod, Version, Id, Data) ->
    {Answer, Req2} = case cowboy_req:method(Req) of
                         {<<"POST">>, _} ->
                             Mod:post(Req, Version, Id, Data);
                         {<<"PUT">>, _} ->
                             Mod:put(Req, Version, Id, Data)
                     end,
    process_callback_answer(Answer, Req2).


%% process_callback_answer/2
%% ====================================================================
%% Unifies replying from PUT / POST / DELETE requests - first argument is response
%% from a callback, that should conform to rules specified in rest_module_behaviour
%% @end
-spec process_callback_answer(term(), req()) -> {boolean(), req()}.
%% ====================================================================
process_callback_answer(Answer, Req) ->
    case Answer of
        ok ->
            {true, Req};
        {body, ResponseBody} ->
            NewReq = cowboy_req:set_resp_body(ResponseBody, Req),
            {true, NewReq};
        {stream, Size, Fun, ContentType} ->
            Req2 = gui_utils:cowboy_ensure_header(<<"content-type">>, ContentType, Req),
            Req3 = cowboy_req:set_resp_body_fun(Size, Fun, Req2),
            {true, Req3};
        error ->
            {false, Req};
        {error, ErrorDesc} ->
            Req2 = cowboy_req:set_resp_body(ErrorDesc, Req),
            {false, Req2}
    end.

%% content_type_supported/1
%% ====================================================================
%% Checks if request content-type is supported by rest modules.
%% @end
-spec content_type_supported(req()) -> boolean().
%% ====================================================================
content_type_supported(Req) ->
    {CTA, _, _} = content_types_accepted(Req, []),
    {Ans, ContentType, _} = cowboy_req:parse_header(<<"content-type">>, Req),
    case Ans of
        ok ->
            lists:foldl(
                fun({{Type, Subtype, _}, _}, Acc) ->
                    case ContentType of
                        {Type, Subtype, _} -> Acc orelse true;
                        _ -> Acc orelse false
                    end
                end, false, CTA);
        _ ->
            false
    end.