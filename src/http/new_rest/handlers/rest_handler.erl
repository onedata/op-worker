%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The module handling the common RESTful logic. It implements
%%% Cowboy's rest pseudo-behavior, delegating specifics to submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(rest_handler).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-behaviour(cowboy_rest).

-include("op_logic.hrl").
-include("http/rest.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

-type method() :: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'.
-type parse_body() :: ignore | as_json_params | as_is.
-type binding() :: {binding, atom()} | {objectid_binding, atom()} | path_binding.
-type bound_gri() :: #b_gri{}.

-export_type([method/0, parse_body/0, binding/0, bound_gri/0]).

% State of REST handler
-record(state, {
    client = undefined :: undefined | op_logic:client(),
    rest_req = undefined :: undefined | #rest_req{},
    allowed_methods :: [method()]
}).
-type state() :: #state{}.
-type opts() :: #{method() => #rest_req{}}.

%% cowboy rest handler API
-export([
    init/2,
    allowed_methods/2,
    content_types_accepted/2,
    content_types_provided/2,
    is_authorized/2,
    accept_resource/2,
    provide_resource/2,
    delete_resource/2
]).
-export([
    rest_routes/0
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Initialize the state for this request.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), opts()) ->
    {cowboy_rest, cowboy_req:req(), state()}.
init(#{method := MethodBin} = Req, Opts) ->
    Method = binary_to_method(MethodBin),
    % If given method is not allowed, it is not in the map. Such request
    % will stop execution on allowed_methods/2 callback. Use undefined if
    % the method does not exist.
    {cowboy_rest, Req, #state{
        rest_req = maps:get(Method, Opts, undefined),
        allowed_methods = maps:keys(Opts)
    }}.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Return the list of allowed methods.
%% @end
%%--------------------------------------------------------------------
-spec allowed_methods(cowboy_req:req(), state()) ->
    {[binary()], cowboy_req:req(), state()}.
allowed_methods(Req, #state{allowed_methods = AllowedMethods} = State) ->
    {[method_to_binary(M) || M <- AllowedMethods], Req, State}.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Return the list of content-types the resource accepts.
%% @end
%%--------------------------------------------------------------------
-spec content_types_accepted(cowboy_req:req(), state()) ->
    {Value, cowboy_req:req(), state()} when
    Value :: [{binary() | {Type, SubType, Params}, AcceptResource}],
    Type :: binary(),
    SubType :: binary(),
    Params :: '*' | [{binary(), binary()}],
    AcceptResource :: atom().
content_types_accepted(Req, #state{rest_req = #rest_req{consumes = Consumes}} = State) ->
    case {cowboy_req:has_body(Req), length(Consumes)} of
        {false, 1} ->
            {[{'*', accept_resource}], Req, State};
        _ ->
            {[{MediaType, accept_resource} || MediaType <- Consumes], Req, State}
    end.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Return the list of content-types the resource provides.
%% @end
%%--------------------------------------------------------------------
-spec content_types_provided(cowboy_req:req(), state()) ->
    {Value, cowboy_req:req(), state()} when
    Value :: [{binary() | {Type, SubType, Params}, ProvideResource}],
    Type :: binary(),
    SubType :: binary(),
    Params :: '*' | [{binary(), binary()}],
    ProvideResource :: atom().
content_types_provided(Req, #state{rest_req = #rest_req{produces = Produces}} = State) ->
    {[{MediaType, provide_resource} || MediaType <- Produces], Req, State}.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Return whether the user is authorized to perform the action.
%% NOTE: The name and description of this function is actually misleading;
%% 401 Unauthorized is returned when there's been an *authentication* error,
%% and 403 Forbidden is returned when the already-authenticated client
%% is unauthorized to perform an operation.
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(cowboy_req:req(), state()) ->
    {true | {false, binary()}, cowboy_req:req(), state()}.
is_authorized(Req, State) ->
    % Check if the request carries any authorization
    Result = try
        http_auth:authenticate(Req)
    catch
        throw:Err ->
            Err;
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:is_authorized - ~p:~p", [
                ?MODULE, Type, Message
            ]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end,

    case Result of
        {ok, Client} ->
            % Always return true - authorization is checked by internal logic later.
            {true, Req, State#state{client = Client}};
        {error, _} = Error ->
            RestResp = rest_translator:error_response(Error),
            {stop, send_response(RestResp, Req), State}
    end.


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Process the request body of application/json content type.
%% @end
%%--------------------------------------------------------------------
-spec accept_resource(cowboy_req:req(), state()) ->
    {stop, cowboy_req:req(), state()}.
accept_resource(Req, State) ->
    process_request(Req, State).


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Process the request body.
%% @end
%%--------------------------------------------------------------------
-spec provide_resource(cowboy_req:req(), state()) ->
    {stop, cowboy_req:req(), state()}.
provide_resource(Req, State) ->
    process_request(Req, State).


%%--------------------------------------------------------------------
%% @doc Cowboy callback function.
%% Delete the resource.
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(cowboy_req:req(), state()) ->
    {stop, cowboy_req:req(), state()}.
delete_resource(Req, State) ->
    process_request(Req, State).


%%--------------------------------------------------------------------
%% @doc
%% Returns all REST routes in the cowboy router format.
%% @end
%%--------------------------------------------------------------------
-spec rest_routes() -> [{binary(), module(), maps:map()}].
rest_routes() ->
    AllRoutes = lists:flatten([
        file_routes:routes(),
        monitoring_routes:routes(),
        oneprovider_routes:routes(),
        replica_routes:routes(),
        share_routes:routes(),
        space_routes:routes(),
        transfer_routes:routes()
    ]),
    % Aggregate routes that share the same path
    AggregatedRoutes = lists:foldr(fun
        ({Path, Handler, #rest_req{method = Method} = RestReq}, [{Path, _, RoutesForPath} | Acc]) ->
            [{Path, Handler, RoutesForPath#{Method => RestReq}} | Acc];
        ({Path, Handler, #rest_req{method = Method} = RestReq}, Acc) ->
            [{Path, Handler, #{Method => RestReq}} | Acc]
    end, [], AllRoutes),
    % Convert all routes to cowboy-compliant routes
    % - prepend REST prefix to every route
    % - rest handler module must be added as second element to the tuples
    % - RoutesForPath will serve as Opts to rest handler init.
    {ok, PrefixStr} = application:get_env(?APP_NAME, op_rest_api_prefix),
    Prefix = str_utils:to_binary(PrefixStr),
    lists:map(fun({Path, Handler, RoutesForPath}) ->
        {<<Prefix/binary, Path/binary>>, Handler, RoutesForPath}
    end, AggregatedRoutes).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a REST request (of any type) by calling op logic.
%% Return new Req and State (after setting cowboy response).
%% @end
%%--------------------------------------------------------------------
-spec process_request(cowboy_req:req(), state()) ->
    {stop, cowboy_req:req(), state()}.
process_request(Req, State) ->
    try
        #state{client = Cl, rest_req = #rest_req{
            method = Method,
            parse_body = ParseBody,
            consumes = Consumes,
            b_gri = GriWithBindings
        }} = State,
        Operation = method_to_operation(Method),
        GRI = resolve_gri_bindings(Cl#client.session_id, GriWithBindings, Req),
        {Data, Req2} = get_data(Req, ParseBody, Consumes),
        OpReq = #op_req{
            operation = Operation,
            client = Cl,
            gri = GRI,
            data = Data
        },
        RestResp = handle_request(OpReq),
        {stop, send_response(RestResp, Req2), State}
    catch
        throw:Error ->
            ErrorResp = rest_translator:error_response(Error),
            {stop, send_response(ErrorResp, Req), State};
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:process_request - ~p:~p", [
                ?MODULE, Type, Message
            ]),
            NewReq = cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req),
            {stop, NewReq, State}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls op_logic and translates obtained response into REST response
%% using TranslatorModule.
%% @end
%%--------------------------------------------------------------------
-spec handle_request(#op_req{}) -> #rest_resp{}.
handle_request(#op_req{operation = Operation, gri = GRI} = ElReq) ->
    Result = op_logic:handle(ElReq),
    try
        rest_translator:response(ElReq, Result)
    catch
        Type:Message ->
            ?error_stacktrace("Cannot translate REST result for:~n"
            "Operation: ~p~n"
            "GRI: ~p~n"
            "Result: ~p~n"
            "---------~n"
            "Error was: ~p:~p", [
                Operation, GRI, Result, Type, Message
            ]),
            rest_translator:error_response(?ERROR_INTERNAL_SERVER_ERROR)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends given response (#rest_resp{}) and returns modified cowboy_req record.
%% @end
%%--------------------------------------------------------------------
-spec send_response(RestResp :: #rest_resp{}, cowboy_req:req()) ->
    cowboy_req:req().
send_response(#rest_resp{code = Code, headers = Headers, body = Body}, Req) ->
    RespBody = case Body of
        {binary, Bin} ->
            Bin;
        Map ->
            json_utils:encode(Map)
    end,
    cowboy_req:reply(Code, Headers, RespBody, Req).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transforms bindings included in a #b_gri{} record into actual data
%% that was sent with the request.
%% @end
%%--------------------------------------------------------------------
-spec resolve_gri_bindings(session:id(), bound_gri(), cowboy_req:req()) ->
    op_logic:gri().
resolve_gri_bindings(SessionId, #b_gri{type = Tp, id = Id, aspect = As, scope = Sc}, Req) ->
    IdBinding = resolve_bindings(SessionId, Id, Req),
    AspectBinding = case As of
        {Atom, Asp} -> {Atom, resolve_bindings(SessionId, Asp, Req)};
        Atom -> Atom
    end,
    #gri{type = Tp, id = IdBinding, aspect = AspectBinding, scope = Sc}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Transforms bindings as specified in rest routes into actual data that was
%% sent with the request.
%% @end
%%--------------------------------------------------------------------
-spec resolve_bindings(session:id(), binding() | {atom(), binding()} | term(),
    cowboy_req:req()) -> binary() | {atom(), binary()}.
resolve_bindings(_SessionId, ?BINDING(Key), Req) ->
    cowboy_req:binding(Key, Req);
resolve_bindings(_SessionId, ?OBJECTID_BINDING(Key), Req) ->
    case catch file_id:objectid_to_guid(cowboy_req:binding(Key, Req)) of
        {ok, Guid} ->
            Guid;
        _Error ->
            throw(?ERROR_BAD_VALUE_IDENTIFIER(Key))
    end;
resolve_bindings(SessionId, ?PATH_BINDING, Req) ->
    Path = filename:join([<<"/">> | cowboy_req:path_info(Req)]),
    case guid_utils:ensure_guid(SessionId, {path, Path}) of
        {guid, Guid} ->
            Guid;
        _Error ->
            throw(?ERROR_BAD_VALUE_IDENTIFIER(Path))
    end;
resolve_bindings(SessionId, {Atom, PossibleBinding}, Req) when is_atom(Atom) ->
    {Atom, resolve_bindings(SessionId, PossibleBinding, Req)};
resolve_bindings(_SessionId, Other, _Req) ->
    Other.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns data associated with given request. That includes parameters
%% from query string but may also, in case of some requests,
%% include data from request body.
%% Depending on specified ParseBody option request body will be saved as is
%% under content-type key or as json object containing additional parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_data(cowboy_req:req(), parse_body(), Consumes :: [term()]) ->
    {Data :: op_logic:data(), cowboy_req:req()}.
get_data(Req, ignore, _Consumes) ->
    {parse_query_string(Req), Req};
get_data(Req, as_json_params, _Consumes) ->
    QueryParams = parse_query_string(Req),
    {ok, Body, Req2} = cowboy_req:read_body(Req),
    ParsedBody = try
        case Body of
            <<"">> -> #{};
            _ -> json_utils:decode(Body)
        end
    catch _:_ ->
        throw(?ERROR_MALFORMED_DATA)
    end,
    is_map(ParsedBody) orelse throw(?ERROR_MALFORMED_DATA),
    {maps:merge(ParsedBody, QueryParams), Req2};
get_data(Req, as_is, Consumes) ->
    QueryParams = parse_query_string(Req),
    {ok, Body, Req2} = cowboy_req:read_body(Req),
    ContentType = case Consumes of
        [ConsumedType] ->
            ConsumedType;
        _ ->
            {Type, Subtype, _} = cowboy_req:parse_header(<<"content-type">>, Req2),
            <<Type/binary, "/", Subtype/binary>>
    end,
    ParsedBody = case ContentType of
        <<"application/json">> ->
            try
                json_utils:decode(Body)
            catch _:_ ->
                throw(?ERROR_BAD_VALUE_JSON(<<"request body">>))
            end;
        _ ->
            Body
    end,
    {QueryParams#{ContentType => ParsedBody}, Req2}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses and returns query string parameters. For every parameter
%% specified multiple times it's values are merged into list.
%% @end
%%--------------------------------------------------------------------
-spec parse_query_string(cowboy_req:req()) -> maps:map().
parse_query_string(Req) ->
    Params = lists:foldl(fun({Key, Val}, AccMap) ->
        maps:update_with(Key, fun(OldVal) ->
            [Val | ensure_list(OldVal)]
        end, Val, AccMap)
    end, #{}, cowboy_req:parse_qs(Req)),

    maps:fold(fun
        (K, V, AccIn) when is_list(V) ->
            AccIn#{K => lists:reverse(V)};
        (_K, _V, AccIn) ->
            AccIn
    end, Params, Params).


%% @private
-spec ensure_list(Val | [Val]) -> [Val] when Val :: true | binary().
ensure_list(Val) when is_list(Val) -> Val;
ensure_list(Val) -> [Val].


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts a binary representing a REST method to an atom representing
%% the method.
%% @end
%%--------------------------------------------------------------------
-spec binary_to_method(BinMethod :: binary()) -> method().
binary_to_method(<<"POST">>) -> 'POST';
binary_to_method(<<"PUT">>) -> 'PUT';
binary_to_method(<<"GET">>) -> 'GET';
binary_to_method(<<"PATCH">>) -> 'PATCH';
binary_to_method(<<"DELETE">>) -> 'DELETE'.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts an atom representing a REST method to a binary representing
%% the method.
%% @end
%%--------------------------------------------------------------------
-spec method_to_binary(Method :: method()) -> binary().
method_to_binary('POST') -> <<"POST">>;
method_to_binary('PUT') -> <<"PUT">>;
method_to_binary('GET') -> <<"GET">>;
method_to_binary('PATCH') -> <<"PATCH">>;
method_to_binary('DELETE') -> <<"DELETE">>.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts an atom representing a REST method into operation
%% that should be called to handle it.
%% @end
%%--------------------------------------------------------------------
-spec method_to_operation(method()) -> op_logic:operation().
method_to_operation('POST') -> create;
method_to_operation('PUT') -> create;
method_to_operation('GET') -> get;
method_to_operation('PATCH') -> update;
method_to_operation('DELETE') -> delete.
