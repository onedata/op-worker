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

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

-type method() :: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'.
-type parse_body() :: ignore | as_json_params | {as_is, KeyName :: binary()}.
-type binding() :: {binding, atom()} | {objectid_binding, atom()} | path_binding.
-type bound_gri() :: #b_gri{}.
-type rest_req() :: #rest_req{}.

-export_type([method/0, parse_body/0, binding/0, bound_gri/0]).

% State of REST handler
-record(state, {
    auth = undefined :: undefined | aai:auth(),
    rest_req = undefined :: undefined | rest_req(),
    allowed_methods :: [method()]
}).
-type state() :: #state{}.
-type opts() :: #{method() => rest_req()}.

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
    Method = http_utils:binary_to_method(MethodBin),
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
    {[http_utils:method_to_binary(M) || M <- AllowedMethods], Req, State}.


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
    % The data access caveats policy depends on requested resource,
    % which is not known yet - it is checked later in specific handler.
    case http_auth:authenticate(Req, rest, allow_data_access_caveats) of
        {ok, Auth} ->
            % Always return true - authorization is checked by internal logic later.
            {true, Req, State#state{auth = Auth}};
        ?ERROR_UNAUTHORIZED(?ERROR_USER_NOT_SUPPORTED) = Error ->
            % The user presented some authentication, but he is not supported
            % by this Oneprovider. Still, if the request concerned a shared
            % file, the user should be treated as a guest and served.
            case (catch resolve_gri_bindings(?GUEST_SESS_ID, State#state.rest_req#rest_req.b_gri, Req)) of
                #gri{scope = public} ->
                    {true, Req, State#state{auth = ?GUEST}};
                _ ->
                    {stop, http_req:send_error(Error, Req), State}
            end;
        {error, _} = Error ->
            {stop, http_req:send_error(Error, Req), State}
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a REST request (of any type) by calling middleware.
%% Return new Req and State (after setting cowboy response).
%% @end
%%--------------------------------------------------------------------
-spec process_request(cowboy_req:req(), state()) ->
    {stop, cowboy_req:req(), state()}.
process_request(Req, #state{auth = #auth{session_id = SessionId} = Auth, rest_req = #rest_req{
    method = Method,
    parse_body = ParseBody,
    consumes = Consumes,
    b_gri = GriWithBindings
}} = State) ->
    try
        {Data, Req2} = get_data(Req, ParseBody, Consumes),
        OpReq = #op_req{
            auth = Auth,
            operation = method_to_operation(Method),
            gri = resolve_gri_bindings(SessionId, GriWithBindings, Req),
            data = Data
        },
        {stop, route_to_proper_handler(OpReq, Req2), State}
    catch
        throw:Error ->
            {stop, http_req:send_error(Error, Req), State};
        Type:Message:Stacktrace ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Message
            ], Stacktrace),
            NewReq = cowboy_req:reply(?HTTP_500_INTERNAL_SERVER_ERROR, Req),
            {stop, NewReq, State}
    end.


%% @private
-spec method_to_operation(method()) -> middleware:operation().
method_to_operation('POST') -> create;
method_to_operation('PUT') -> create;
method_to_operation('GET') -> get;
method_to_operation('PATCH') -> update;
method_to_operation('DELETE') -> delete.


%% @private
-spec resolve_gri_bindings(session:id(), bound_gri(), cowboy_req:req()) ->
    gri:gri().
resolve_gri_bindings(SessionId, #b_gri{type = Tp, id = Id, aspect = As, scope = Sc}, Req) ->
    IdBinding = resolve_bindings(SessionId, Id, Req),
    AsBinding = case As of
        {Atom, Asp} -> {Atom, resolve_bindings(SessionId, Asp, Req)};
        Atom -> Atom
    end,
    ScBinding = case middleware_utils:is_shared_file_request(Tp, AsBinding, Sc, IdBinding) of
        true -> public;
        false -> Sc
    end,
    #gri{type = Tp, id = IdBinding, aspect = AsBinding, scope = ScBinding}.


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
    SpaceIdOrObjectId = cowboy_req:binding(Key, Req),
    try
        middleware_utils:decode_object_id(SpaceIdOrObjectId, Key)
    catch throw:?ERROR_BAD_VALUE_IDENTIFIER(Key) ->
        {ok, SupportedSpaceIds} = provider_logic:get_spaces(),
        case lists:member(SpaceIdOrObjectId, SupportedSpaceIds) of
            true -> fslogic_uuid:spaceid_to_space_dir_guid(SpaceIdOrObjectId);
            false -> throw(?ERROR_SPACE_NOT_SUPPORTED_LOCALLY(SpaceIdOrObjectId))
        end
    end;
resolve_bindings(SessionId, ?PATH_BINDING, Req) ->
    Path = filename:join([<<"/">> | cowboy_req:path_info(Req)]),
    {ok, Guid} = middleware_utils:resolve_file_path(SessionId, Path),
    Guid;
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
    {middleware:data(), cowboy_req:req()}.
get_data(Req, ignore, _Consumes) ->
    {http_parser:parse_query_string(Req), Req};
get_data(Req, as_json_params, _Consumes) ->
    QueryParams = http_parser:parse_query_string(Req),
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
get_data(Req, {as_is, KeyName}, Consumes) ->
    QueryParams = http_parser:parse_query_string(Req),
    {ok, Body, Req2} = cowboy_req:read_body(Req),
    ContentType = case Consumes of
        [ConsumedType] ->
            ConsumedType;
        _ ->
            {Type, Subtype, _} = cowboy_req:parse_header(?HDR_CONTENT_TYPE, Req2),
            <<Type/binary, "/", Subtype/binary>>
    end,
    ParsedBody = case ContentType of
        <<"application/json">> ->
            try
                json_utils:decode(Body)
            catch _:_ ->
                throw(?ERROR_BAD_VALUE_JSON(KeyName))
            end;
        _ ->
            Body
    end,
    {QueryParams#{KeyName => ParsedBody}, Req2}.


%% @private
-spec route_to_proper_handler(middleware:req(), cowboy_req:req()) -> cowboy_req:req().
route_to_proper_handler(#op_req{operation = Operation, gri = #gri{
    type = op_file,
    aspect = As
}} = OpReq, Req) when
    (Operation == create andalso As == child);
    (Operation == create andalso As == content);
    (Operation == get andalso As == content);
    (Operation == create andalso As == file_at_path);
    (Operation == get andalso As == file_at_path);
    (Operation == delete andalso As == file_at_path)
->
    file_content_rest_handler:handle_request(OpReq, Req);
route_to_proper_handler(OpReq, Req) ->
    middleware_rest_handler:handle_request(OpReq, Req).
